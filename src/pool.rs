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
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
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

/// The inference nodes among the user's linked devices.
pub struct LinkedNodes<Q: Message, R: Message> {
    node: Arc<Node<Q, R>>,
    roster: RosterView,
    hints: HashMap<PeerId, Vec<Multiaddr>>,
    wait: Duration,
}

impl<Q: Message, R: Message> LinkedNodes<Q, R> {
    pub fn new(node: Arc<Node<Q, R>>, roster: RosterView) -> Self {
        Self { node, roster, hints: HashMap::new(), wait: Duration::from_secs(3) }
    }

    /// Addresses to connect to a device through.
    pub fn with_hint(mut self, peer: PeerId, addrs: Vec<Multiaddr>) -> Self {
        self.hints.insert(peer, addrs);
        self
    }

    /// How long to wait to connect to and identify each device (default 3 s).
    pub fn with_wait(mut self, wait: Duration) -> Self {
        self.wait = wait;
        self
    }
}

#[async_trait]
impl<Q: Message, R: Message> NodeSource for LinkedNodes<Q, R> {
    async fn nodes(&self) -> Vec<String> {
        let me = self.node.peer_id().to_string();
        let roster = (self.roster)();
        let peers: Vec<PeerId> = roster.active().filter(|c: &&NodeCert| c.peer_id != me).filter_map(|c| c.peer_id.parse().ok()).collect();
        let mut found = Vec::new();
        for peer in peers {
            let node = self.node.clone();
            let addrs = self.hints.get(&peer).cloned().unwrap_or_default();
            let wait = self.wait;
            let r = tokio::task::spawn_blocking(move || {
                let announced = |apps: Option<Vec<String>>| apps.is_some_and(|a| a.iter().any(|a| a == INFERENCE_APP));
                let mut apps = node.peer_apps(peer);
                if apps.is_none() {
                    let _ = node.connect(peer, &addrs);
                    let until = Instant::now() + wait;
                    while apps.is_none() && Instant::now() < until {
                        std::thread::sleep(Duration::from_millis(25));
                        apps = node.peer_apps(peer);
                    }
                }
                if !announced(apps) {
                    return None;
                }
                let direct = node.paths(peer).map(|p| p.contains(&Path::Direct)).unwrap_or(false);
                Some(direct)
            })
            .await;
            if let Ok(Some(direct)) = r {
                found.push((!direct, peer.to_string()));
            }
        }
        found.sort_by_key(|(relayed, _)| *relayed);
        found.into_iter().map(|(_, p)| p).collect()
    }
}

/// A [`Pool`] over `node`: inference nodes among the roster's linked devices, presenting
/// `cert` (this device's certificate) with every request.
pub fn connect<Q: Message, R: Message>(node: Arc<Node<Q, R>>, roster: RosterView, cert: Option<&NodeCert>) -> Pool {
    let opts = PoolOptions::default().with_cert(cert.map(encode_cert));
    Pool::new(Arc::new(NetDialer::new(node.clone())), LinkedNodes::new(node, roster)).with_options(opts)
}
