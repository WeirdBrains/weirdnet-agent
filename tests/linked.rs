//! `LinkedNodes` with linked devices that are offline: finding the inference nodes must not
//! cost a wait per offline device (0.4.2 waited the whole `with_wait` for each, on every call).
#![cfg(feature = "pool")]

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use weirdnet_agent::pool::{INFERENCE_APP, LinkedNodes, NodeSource};
use weirdnet_net::node::{Handler, Node, NodeConfig};
use weirdnet_net::{AppResponse, Keypair, Multiaddr, PeerId, WEIRDNET};
use weirdnet_sync::Roster;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Q;
#[derive(Debug, Clone, Serialize, Deserialize)]
struct R;
struct H;
impl Handler<Q, R> for H {
    fn request(&self, _: &PeerId, _: Q) -> R {
        R
    }
}
type N = Node<Q, R>;

fn start_with(key: Keypair) -> Arc<N> {
    let cfg = NodeConfig { listen: vec!["/memory/0".parse().unwrap()], memory: true, ..Default::default() };
    Arc::new(Node::start(&WEIRDNET, key, cfg, Arc::new(H)).unwrap())
}

fn addr(n: &N) -> Multiaddr {
    n.reachable_addrs().into_iter().find(|a| a.starts_with("/memory/") && !a.contains("p2p-circuit")).unwrap().parse().unwrap()
}

/// Announce INFERENCE_APP, as an inference node's `Server` does.
fn announce(n: &N) {
    n.serve_app(INFERENCE_APP, Arc::new(|_peer, _req| async move { Ok(AppResponse::new(200)) })).unwrap();
}

fn certify(user: &Keypair, peer: PeerId, label: &str) -> weirdnet_net::user::NodeCert {
    Roster::self_certify(&WEIRDNET, user, &peer.to_string(), label, weirdnet_net::now_ms()).unwrap()
}

async fn timed(nodes: &impl NodeSource) -> (Duration, Vec<String>) {
    let t = Instant::now();
    let found = nodes.nodes().await;
    (t.elapsed(), found)
}

const WAIT: Duration = Duration::from_secs(3);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn offline_devices_do_not_slow_down_nodes() {
    let user = Keypair::generate_ed25519();
    let app = start_with(Keypair::generate_ed25519());
    let inf = start_with(Keypair::generate_ed25519());
    announce(&inf);
    let mut roster = Roster::new();
    roster.upsert(certify(&user, app.peer_id(), "phone"));
    roster.upsert(certify(&user, inf.peer_id(), "inference"));
    // Three linked devices that never start; one has a stale address hint.
    let offline: Vec<PeerId> = (0..3).map(|_| Keypair::generate_ed25519().public().to_peer_id()).collect();
    for (i, p) in offline.iter().enumerate() {
        roster.upsert(certify(&user, *p, &format!("offline{i}")));
    }
    let view: Arc<dyn Fn() -> Roster + Send + Sync> = Arc::new(move || roster.clone());
    let nodes = LinkedNodes::new(app.clone(), view)
        .with_hint(inf.peer_id(), vec![addr(&inf)])
        .with_hint(offline[0], vec!["/memory/987654321".parse().unwrap()])
        .with_wait(WAIT);

    // Nothing known yet: the probes run together and the dead ones give up at once.
    let (took, found) = timed(&nodes).await;
    assert_eq!(found, [inf.peer_id().to_string()]);
    assert!(took < Duration::from_millis(1500), "first call took {took:?}");
    // The inference node is known now: answered without waiting.
    for _ in 0..3 {
        let (took, found) = timed(&nodes).await;
        assert_eq!(found, [inf.peer_id().to_string()]);
        assert!(took < Duration::from_millis(200), "known inference node, call took {took:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unreachable_devices_are_remembered_until_they_connect() {
    let user = Keypair::generate_ed25519();
    let app = start_with(Keypair::generate_ed25519());
    let late_key = Keypair::generate_ed25519();
    let late_peer = late_key.public().to_peer_id();
    let mut roster = Roster::new();
    roster.upsert(certify(&user, app.peer_id(), "phone"));
    roster.upsert(certify(&user, late_peer, "laptop"));
    for i in 0..3 {
        roster.upsert(certify(&user, Keypair::generate_ed25519().public().to_peer_id(), &format!("offline{i}")));
    }
    let view: Arc<dyn Fn() -> Roster + Send + Sync> = Arc::new(move || roster.clone());
    let nodes = LinkedNodes::new(app.clone(), view).with_wait(WAIT);

    // No inference node anywhere: still well under one wait, and then cached.
    let (took, found) = timed(&nodes).await;
    assert!(found.is_empty());
    assert!(took < WAIT, "first call took {took:?}");
    let (took, found) = timed(&nodes).await;
    assert!(found.is_empty());
    assert!(took < Duration::from_millis(200), "cached call took {took:?}");

    // The laptop comes online as an inference node and connects: found without waiting out
    // `with_retry_after`.
    let late = start_with(late_key);
    announce(&late);
    let to = (app.peer_id(), vec![addr(&app)]);
    tokio::task::spawn_blocking({
        let late = late.clone();
        move || late.connect(to.0, &to.1)
    })
    .await
    .unwrap()
    .unwrap();
    let until = Instant::now() + Duration::from_secs(2);
    loop {
        let (took, found) = timed(&nodes).await;
        assert!(took < Duration::from_millis(500), "call took {took:?}");
        if found == [late_peer.to_string()] {
            break;
        }
        assert!(Instant::now() < until, "the laptop was not found after it connected");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
