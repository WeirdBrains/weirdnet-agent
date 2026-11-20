//! The `pool` feature: the app side of remote inference over `weirdnet-net`.
//!
//! - [`NetDialer`]: a [`Dialer`] that opens agent protocol streams to peers by id.
//! - [`LinkedNodes`]: a [`NodeSource`] of the inference nodes among the user's linked devices
//!   (active certs in the `weirdnet-sync` roster whose node announces [`INFERENCE_APP`]),
//!   direct connections before relayed ones.
//! - [`connect`]: a [`Pool`] over both, presenting this device's certificate.
//!
//! The pool itself ([`crate::remote::pool`]) is transport-neutral.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::sync::watch;
use weirdnet_net::Event;
use weirdnet_net::node::Message;
use weirdnet_net::user::NodeCert;
use weirdnet_net::{Multiaddr, Node, Path, PeerId};
use weirdnet_sync::Roster;

use crate::net::open_error;
pub use crate::net::{INFERENCE_APP, channel, decode_cert, encode_cert, stream_limits};
pub use crate::remote::pool::{Answer, CatalogWatch, EventSink, Interrupted, Job, NoEvents, NodeSource, Pool, PoolEvent, PoolOptions, Stall};
use crate::remote::{Channel, Dialer, RemoteError};

/// Opens agent streams over a `weirdnet-net` node. Node ids are peer ids.
pub struct NetDialer<Q: Message, R: Message> {
    node: Arc<Node<Q, R>>,
    hints: Mutex<HashMap<PeerId, Vec<Multiaddr>>>,
}

impl<Q: Message, R: Message> NetDialer<Q, R> {
    pub fn new(node: Arc<Node<Q, R>>) -> Self {
        Self { node, hints: Mutex::new(HashMap::new()) }
    }

    /// Addresses to try for a peer (LAN, fixed or relay circuit addresses).
    pub fn hint(&self, peer: PeerId, addrs: Vec<Multiaddr>) {
        self.hints.lock().unwrap_or_else(|e| e.into_inner()).insert(peer, addrs);
    }
}

#[async_trait]
impl<Q: Message, R: Message> Dialer for NetDialer<Q, R> {
    async fn open(&self, node: &str, protocol: &str) -> Result<Channel, RemoteError> {
        let peer: PeerId = node.parse().map_err(|_| RemoteError::Unreachable { message: format!("{node} is not a peer id") })?;
        let addrs = self.hints.lock().unwrap_or_else(|e| e.into_inner()).get(&peer).cloned().unwrap_or_default();
        let stream = self.node.open_stream_async(peer, &addrs, protocol, stream_limits()).await.map_err(open_error)?;
        Ok(channel(stream))
    }
}

/// The roster as it is now (the app keeps it current from `weirdnet-sync`).
pub type RosterView = Arc<dyn Fn() -> Roster + Send + Sync>;

/// How long a probe's `connect` may take (the node's own limit).
const CONNECT_LIMIT: Duration = Duration::from_secs(30);
/// Once a probe finds an inference node, how long to keep waiting for the other probes.
const SETTLE: Duration = Duration::from_millis(250);

/// What the last look at a linked device found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Seen {
    /// A probe (connect, then wait for identify) is under way.
    Probing,
    /// Identified and announces [`INFERENCE_APP`]; `direct` if a direct connection is open.
    Inference { direct: bool },
    /// Identified, not an inference node.
    Other,
    /// Could not be connected to, or did not identify in time.
    Unreachable,
}

/// State shared by every call to [`LinkedNodes::nodes`] and its background probes.
struct Shared {
    seen: Mutex<HashMap<PeerId, (Seen, Instant)>>,
    /// Bumped whenever a probe finishes.
    changed: watch::Sender<u64>,
    watching: AtomicBool,
}

impl Shared {
    fn seen(&self) -> std::sync::MutexGuard<'_, HashMap<PeerId, (Seen, Instant)>> {
        self.seen.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn record(&self, peer: PeerId, seen: Seen) {
        self.seen().insert(peer, (seen, Instant::now()));
        self.changed.send_modify(|n| *n = n.wrapping_add(1));
    }

    /// Try unreachable devices again on the next call (`None`: all of them).
    fn forget_unreachable(&self, peer: Option<PeerId>) {
        self.seen().retain(|p, (s, _)| *s != Seen::Unreachable || peer.is_some_and(|q| q != *p));
    }
}

/// What `node` knows about `peer` without dialing (`None`: not connected or not identified).
fn identified<Q: Message, R: Message>(node: &Node<Q, R>, peer: PeerId) -> Option<Seen> {
    let apps = node.peer_apps(peer)?;
    if !apps.iter().any(|a| a == INFERENCE_APP) {
        return Some(Seen::Other);
    }
    let direct = node.paths(peer).map(|p| p.contains(&Path::Direct)).unwrap_or(false);
    Some(Seen::Inference { direct })
}

/// The inference nodes among the user's linked devices.
///
/// [`NodeSource::nodes`] answers at once with the devices that are connected and announce
/// [`INFERENCE_APP`]; devices not known yet are probed in the background. Only when none is
/// known does it wait, for all probes together, at most [`LinkedNodes::with_wait`]. A device
/// that can't be connected to (no addresses, dial refused) is given up on at once; one that
/// can't be reached or doesn't identify is skipped for [`LinkedNodes::with_retry_after`], or
/// until it connects, the network changes or the node resumes.
pub struct LinkedNodes<Q: Message, R: Message> {
    node: Arc<Node<Q, R>>,
    roster: RosterView,
    hints: HashMap<PeerId, Vec<Multiaddr>>,
    wait: Duration,
    retry_after: Duration,
    shared: Arc<Shared>,
}

impl<Q: Message, R: Message> LinkedNodes<Q, R> {
    pub fn new(node: Arc<Node<Q, R>>, roster: RosterView) -> Self {
        let shared = Arc::new(Shared { seen: Mutex::new(HashMap::new()), changed: watch::channel(0).0, watching: AtomicBool::new(false) });
        Self { node, roster, hints: HashMap::new(), wait: Duration::from_secs(3), retry_after: Duration::from_secs(30), shared }
    }

    /// Addresses to connect to a device through.
    pub fn with_hint(mut self, peer: PeerId, addrs: Vec<Multiaddr>) -> Self {
        self.hints.insert(peer, addrs);
        self
    }

    /// When no inference node is known yet, how long to wait for the devices to connect and
    /// identify (one deadline for all of them; default 3 s). Also how long each probe waits
    /// for identify once connected.
    pub fn with_wait(mut self, wait: Duration) -> Self {
        self.wait = wait;
        self
    }

    /// How long a device that couldn't be reached or identified is skipped before it is
    /// probed again (default 30 s).
    pub fn with_retry_after(mut self, retry_after: Duration) -> Self {
        self.retry_after = retry_after;
        self
    }

    /// Forget unreachable devices when one connects, the network changes or the node resumes.
    fn watch_events(&self) {
        if self.shared.watching.swap(true, Ordering::SeqCst) {
            return;
        }
        let Ok(rt) = tokio::runtime::Handle::try_current() else {
            self.shared.watching.store(false, Ordering::SeqCst);
            return;
        };
        let mut events = self.node.events();
        let shared = Arc::downgrade(&self.shared);
        rt.spawn(async move {
            use futures::StreamExt;
            while let Some(ev) = events.next().await {
                let Some(shared) = shared.upgrade() else { break };
                match ev {
                    Event::Connected { peer, .. } => shared.forget_unreachable(Some(peer)),
                    Event::NetworkChanged | Event::Resumed => shared.forget_unreachable(None),
                    _ => {}
                }
            }
        });
    }

    /// Connect to `peer` and wait (at most `wait`) for it to identify, in the background.
    fn probe(&self, peer: PeerId) {
        let node = self.node.clone();
        let addrs = self.hints.get(&peer).cloned().unwrap_or_default();
        let wait = self.wait;
        let shared = Arc::downgrade(&self.shared);
        tokio::task::spawn_blocking(move || {
            let t = Instant::now();
            let seen = match node.connect(peer, &addrs) {
                Err(e) => {
                    tracing::debug!(%peer, error = %e, "linked device unreachable");
                    Seen::Unreachable
                }
                Ok(()) => {
                    let until = Instant::now() + wait;
                    loop {
                        if let Some(s) = identified(&node, peer) {
                            break s;
                        }
                        if Instant::now() >= until {
                            tracing::debug!(%peer, "linked device connected but did not identify in time");
                            break Seen::Unreachable;
                        }
                        std::thread::sleep(Duration::from_millis(25));
                    }
                }
            };
            tracing::debug!(%peer, ?seen, took = ?t.elapsed(), "probed linked device");
            if let Some(shared) = shared.upgrade() {
                shared.record(peer, seen);
            }
        });
    }
}

#[async_trait]
impl<Q: Message, R: Message> NodeSource for LinkedNodes<Q, R> {
    async fn nodes(&self) -> Vec<String> {
        let started = Instant::now();
        self.watch_events();
        let me = self.node.peer_id();
        let roster = (self.roster)();
        let mut peers: Vec<PeerId> = Vec::new();
        for p in roster.active().filter_map(|c: &NodeCert| c.peer_id.parse::<PeerId>().ok()) {
            if p != me && !peers.contains(&p) {
                peers.push(p);
            }
        }
        // What the node knows now, without dialing (one blocking call per device, together:
        // each node query is a round trip to its event loop).
        let known: Vec<Option<Seen>> = futures::future::join_all(peers.iter().map(|p| {
            let (node, p) = (self.node.clone(), *p);
            tokio::task::spawn_blocking(move || identified(&node, p))
        }))
        .await
        .into_iter()
        .map(|r| r.ok().flatten())
        .collect();
        let mut changed = self.shared.changed.subscribe();
        let mut state: Vec<Option<Seen>> = vec![None; peers.len()];
        let mut probe = Vec::new();
        {
            let now = Instant::now();
            let mut seen = self.shared.seen();
            for (i, peer) in peers.iter().enumerate() {
                if let Some(s) = known[i] {
                    seen.insert(*peer, (s, now));
                    state[i] = Some(s);
                    continue;
                }
                match seen.get(peer) {
                    Some((Seen::Unreachable, at)) if now.duration_since(*at) < self.retry_after => state[i] = Some(Seen::Unreachable),
                    Some((Seen::Probing, at)) if now.duration_since(*at) < CONNECT_LIMIT + self.wait => {}
                    _ => {
                        seen.insert(*peer, (Seen::Probing, now));
                        probe.push(*peer);
                    }
                }
            }
        }
        for peer in &probe {
            self.probe(*peer);
        }
        let is_inference = |s: &Option<Seen>| matches!(s, Some(Seen::Inference { .. }));
        // Inference nodes already known: answer now; probes carry on in the background.
        if !state.iter().any(is_inference) {
            let deadline = started + self.wait;
            let mut first_found: Option<Instant> = None;
            loop {
                {
                    let seen = self.shared.seen();
                    for (i, peer) in peers.iter().enumerate() {
                        if state[i].is_none()
                            && let Some((s, _)) = seen.get(peer)
                            && *s != Seen::Probing
                        {
                            state[i] = Some(*s);
                        }
                    }
                }
                let now = Instant::now();
                if state.iter().all(Option::is_some) || now >= deadline {
                    break;
                }
                let until = if state.iter().any(is_inference) { (*first_found.get_or_insert(now) + SETTLE).min(deadline) } else { deadline };
                if now >= until {
                    break;
                }
                if tokio::time::timeout_at(until.into(), changed.changed()).await.is_ok_and(|r| r.is_err()) {
                    break;
                }
            }
        }
        let mut found: Vec<(bool, String)> = peers
            .iter()
            .zip(&state)
            .filter_map(|(p, s)| match s {
                Some(Seen::Inference { direct }) => Some((!direct, p.to_string())),
                _ => None,
            })
            .collect();
        found.sort_by_key(|(relayed, _)| *relayed);
        tracing::debug!(
            took = ?started.elapsed(),
            linked = peers.len(),
            found = found.len(),
            probing = state.iter().filter(|s| s.is_none()).count(),
            "LinkedNodes::nodes"
        );
        found.into_iter().map(|(_, p)| p).collect()
    }
}

/// A [`Pool`] over `node`: inference nodes among the roster's linked devices, presenting
/// `cert` (this device's certificate) with every request.
pub fn connect<Q: Message, R: Message>(node: Arc<Node<Q, R>>, roster: RosterView, cert: Option<&NodeCert>) -> Pool {
    let opts = PoolOptions::default().with_cert(cert.map(encode_cert));
    Pool::new(Arc::new(NetDialer::new(node.clone())), LinkedNodes::new(node, roster)).with_options(opts)
}
