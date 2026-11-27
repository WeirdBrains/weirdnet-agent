//! Cross-identity pool sharing between in-process `weirdnet-net` nodes (memory transport): an
//! owner with two inference nodes, a guest of another Weirdnet user who has the share link, a
//! stranger, and a node that does not take guests.
#![cfg(all(feature = "pool", feature = "serve"))]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use weirdnet_agent::pool::{self, PoolShare, encode_cert, encode_credential};
use weirdnet_agent::remote::catalog::{self, CatalogFeed, CatalogModel, ModelState, Purpose};
use weirdnet_agent::remote::infer::v2::{self, ClientFrame, Finish, InferBackend, Output, Request, ServerFrame, TranscribeRequest, Transcript};
use weirdnet_agent::remote::infer::{Admission, InferHost};
use weirdnet_agent::remote::pool::NoEvents;
use weirdnet_agent::remote::{Caller, Dialer, RemoteError};
use weirdnet_agent::serve::{Guest, GuestAuth, GuestPolicy, GuestScope, RosterAuth, Server, Serving, SharedPoolAuth};
use weirdnet_agent::{CancelToken, Message, ModelBackend};
use weirdnet_net::node::{Handler, Node, NodeConfig};
use weirdnet_net::user::{NodeCert, user_id};
use weirdnet_net::{Keypair, PeerId, WEIRDNET};
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

fn start() -> Arc<N> {
    let cfg = NodeConfig { listen: vec!["/memory/0".parse().unwrap()], memory: true, ..Default::default() };
    Arc::new(Node::start(&WEIRDNET, Keypair::generate_ed25519(), cfg, Arc::new(H)).unwrap())
}

fn addr(n: &N) -> String {
    n.reachable_addrs().into_iter().find(|a| a.starts_with("/memory/") && !a.contains("p2p-circuit")).unwrap()
}

fn certify(user: &Keypair, n: &N, label: &str) -> NodeCert {
    Roster::self_certify(&WEIRDNET, user, &n.peer_id().to_string(), label, weirdnet_net::now_ms()).unwrap()
}

struct Words(&'static str);

#[async_trait]
impl InferBackend for Words {
    fn name(&self) -> String {
        format!("gemma@{}", self.0)
    }
    async fn infer(&self, _request: &Request, out: &Output, _cancel: &CancelToken) -> Result<Finish, RemoteError> {
        out.text("Hello guest");
        Ok(Finish::new("stop"))
    }
}

struct Ears(&'static str);

#[async_trait]
impl v2::TranscribeBackend for Ears {
    fn name(&self) -> String {
        format!("voxtral@{}", self.0)
    }
    async fn transcribe(&self, request: &TranscribeRequest, _cancel: &CancelToken) -> Result<Transcript, RemoteError> {
        Ok(Transcript::new(format!("{} bytes", request.audio.len())))
    }
}

/// An inference node of the owner: the roster for its own devices, guests by policy. Records
/// who it served (own or guest) so the test sees the identity reach `open_request`.
struct Host {
    name: &'static str,
    auth: SharedPoolAuth,
    served: Mutex<Vec<(String, bool)>>,
}

impl InferHost for Host {
    fn authorize(&self, caller: &Caller) -> Result<(), RemoteError> {
        self.auth.authorize_for(caller, GuestScope::Infer)
    }
    fn open(&self, _caller: &Caller, _model: Option<&str>) -> Result<Arc<dyn ModelBackend>, RemoteError> {
        Err(RemoteError::unavailable("infer/2 only"))
    }
    fn open_request(&self, caller: &Caller, request: &Request) -> Result<Arc<dyn InferBackend>, RemoteError> {
        let who = self.auth.identify(caller, GuestScope::Infer)?;
        // What Ymir does for guests: no server-side context (ymir.context) for them.
        if who.is_guest() && matches!(request.input, v2::Input::Context(_)) {
            return Err(RemoteError::refused("guest-context"));
        }
        self.served.lock().unwrap().push((who.peer().to_string(), who.is_guest()));
        Ok(Arc::new(Words(self.name)))
    }
    fn open_transcribe(&self, _caller: &Caller, _model: Option<&str>) -> Result<Arc<dyn v2::TranscribeBackend>, RemoteError> {
        Ok(Arc::new(Ears(self.name)))
    }
}

/// A node that serves only its own user (every host before 0.5.9).
struct RosterOnly(Arc<RosterAuth>);

impl InferHost for RosterOnly {
    fn authorize(&self, caller: &Caller) -> Result<(), RemoteError> {
        weirdnet_agent::Authorize::authorize(self.0.as_ref(), caller)
    }
    fn open(&self, _caller: &Caller, _model: Option<&str>) -> Result<Arc<dyn ModelBackend>, RemoteError> {
        Err(RemoteError::unavailable("infer/2 only"))
    }
    fn open_request(&self, _caller: &Caller, _request: &Request) -> Result<Arc<dyn InferBackend>, RemoteError> {
        Ok(Arc::new(Words("roster-only")))
    }
}

fn refused<T: std::fmt::Debug>(r: Result<T, RemoteError>) -> String {
    match r {
        Err(RemoteError::Refused { message }) => message,
        other => panic!("not refused: {other:?}"),
    }
}

#[test]
fn a_guest_uses_a_shared_pool_until_revoked() {
    let (owner, guest_user) = (Keypair::generate_ed25519(), Keypair::generate_ed25519());
    let (a, b, plain, guest, stranger) = (start(), start(), start(), start(), start());
    let mut roster = Roster::new();
    for (n, l) in [(&a, "node-a"), (&b, "node-b"), (&plain, "node-c")] {
        roster.upsert(certify(&owner, n, l));
    }
    let view: Arc<dyn Fn() -> Roster + Send + Sync> = Arc::new(move || roster.clone());
    let guest_cert = certify(&guest_user, &guest, "morph");
    let guest_uid = user_id(&guest_user.public());

    // The owner's synced guest list: the guest's user id with catalog + infer.
    let shared_with: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(vec![guest_uid.clone()]));
    let policy: GuestPolicy = {
        let list = shared_with.clone();
        Arc::new(move |g: &Guest, scope: GuestScope| {
            let listed = list.lock().unwrap().contains(&g.user_id);
            if listed && matches!(scope, GuestScope::Catalog | GuestScope::Infer) { Ok(()) } else { Err(RemoteError::refused("not-shared")) }
        })
    };

    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    rt.block_on(async {
        let roster_auth = Arc::new(RosterAuth::new(&WEIRDNET, owner.public(), view.clone()));
        let auth = SharedPoolAuth::new(roster_auth.clone()).with_guests(Arc::new(GuestAuth::new(&WEIRDNET, &owner.public(), policy)));
        let feed = CatalogFeed::new(auth.scoped(GuestScope::Catalog)).rechecking();
        feed.publish(vec![
            CatalogModel::new("gemma", Purpose::Chat, ModelState::Warm).with_replicas(2),
            CatalogModel::new("voxtral", Purpose::Transcribe, ModelState::Warm),
        ]);
        let mut servers = Vec::new();
        let mut hosts = Vec::new();
        for (n, name) in [(&a, "node-a"), (&b, "node-b")] {
            let host = Arc::new(Host { name, auth: auth.clone(), served: Mutex::new(Vec::new()) });
            hosts.push(host.clone());
            let serving = Serving { infer: Some((host, Admission::with_queue(2, 4))), catalog: Some(Arc::new(feed.clone())), announce: true };
            servers.push(Server::start(n, serving).unwrap());
        }
        let plain_feed = CatalogFeed::new(roster_auth.clone());
        let serving = Serving {
            infer: Some((Arc::new(RosterOnly(roster_auth.clone())), Admission::with_queue(1, 1))),
            catalog: Some(Arc::new(plain_feed)),
            announce: true,
        };
        servers.push(Server::start(&plain, serving).unwrap());

        // The owner shares the pool: a link with the inference nodes, through a string.
        let link = PoolShare::new(user_id(&owner.public()))
            .with_name("Ymir at home")
            .with_node(a.peer_id().to_string(), vec![addr(&a)])
            .with_node(b.peer_id().to_string(), vec![addr(&b)])
            .to_link()
            .unwrap();
        let share = PoolShare::parse(&link).unwrap();
        assert_eq!(share.owner, user_id(&owner.public()));

        // The guest connects with its own certificate and user key; no roster of the owner.
        let pool = pool::connect_shared(guest.clone(), &share, &guest_user.public(), &guest_cert).unwrap();
        assert_eq!(pool.nodes().await, [a.peer_id().to_string(), b.peer_id().to_string()]);
        let cat = pool.catalog().await.unwrap();
        assert_eq!(cat.find("gemma").unwrap().replicas, 2);
        assert!(cat.route_transcribe(None).is_ok(), "transcribe entries reach 0.5.9 guests");
        let answer = pool.infer(Request::messages(vec![Message::user("hi")]), &NoEvents, &CancelToken::new()).await.unwrap();
        assert_eq!((answer.text.as_str(), answer.node.clone()), ("Hello guest", a.peer_id().to_string()));
        assert_eq!(*hosts[0].served.lock().unwrap(), [(guest.peer_id().to_string(), true)], "the host saw a guest");
        let heard = pool.transcribe(TranscribeRequest::new(vec![0; 64], "wav"), &CancelToken::new()).await.unwrap();
        assert_eq!((heard.transcript.text.as_str(), heard.model.as_str()), ("64 bytes", "voxtral@node-a"));
        // Guests get no server-side context.
        let ctx = Request::context(v2::ContextRef::new("conv-1", None));
        assert_eq!(refused(pool.infer(ctx, &NoEvents, &CancelToken::new()).await.map_err(|i| i.error)), "guest-context");

        // A subscription, live until the guest is revoked.
        let mut watch = pool.subscribe();
        let (_, first) = watch.changed().await.unwrap();
        assert_eq!(first.rev, cat.rev);

        // Failover across the shared pool's nodes: node-a goes away, node-b answers.
        let gone = a.clone();
        tokio::task::spawn_blocking(move || gone.halt()).await.unwrap();
        let answer = pool.infer(Request::messages(vec![Message::user("hi")]), &NoEvents, &CancelToken::new()).await.unwrap();
        assert_eq!((answer.text.as_str(), answer.model.as_str()), ("Hello guest", "gemma@node-b"));
        // The owner's own devices still go by the roster through the same hosts.
        let own_app_cert = certify(&owner, &stranger, "owner-phone");
        let own_dialer = pool::NetDialer::new(stranger.clone());
        own_dialer.hint(b.peer_id(), vec![addr(&b).parse().unwrap()]);
        let mut ch = own_dialer.open(&b.peer_id().to_string(), catalog::PROTOCOL).await.unwrap();
        assert_eq!(refused(catalog::query(&mut ch, Some(encode_cert(&own_app_cert))).await), "unknown-device", "not in the roster");

        // Revocation: off the guest list, refused at once on every protocol, and the
        // subscription closes (within the re-check interval).
        shared_with.lock().unwrap().clear();
        assert_eq!(refused(pool.catalog().await), "not-shared");
        assert_eq!(
            refused(pool.infer(Request::messages(vec![Message::user("hi")]), &NoEvents, &CancelToken::new()).await.map_err(|i| i.error)),
            "not-shared"
        );
        assert_eq!(refused(pool.transcribe(TranscribeRequest::new(vec![0; 8], "wav"), &CancelToken::new()).await), "not-shared");
        let mut sd = pool::NetDialer::new(guest.clone());
        sd = {
            sd.hint(b.peer_id(), vec![addr(&b).parse().unwrap()]);
            sd
        };
        let cred = encode_credential(&guest_user.public(), &guest_cert);
        let mut ch = sd.open(&b.peer_id().to_string(), catalog::PROTOCOL).await.unwrap();
        let mut updates = 0;
        let ended = tokio::time::timeout(catalog::RECHECK_EVERY * 3, catalog::subscribe(&mut ch, Some(cred.clone()), |_| updates += 1)).await;
        assert_eq!(ended.expect("refused at once"), Err(RemoteError::refused("not-shared")));
        assert_eq!(updates, 0);
        // Re-shared: let in again.
        shared_with.lock().unwrap().push(guest_uid.clone());
        let mut ch = sd.open(&b.peer_id().to_string(), catalog::PROTOCOL).await.unwrap();
        let sub = catalog::subscribe(&mut ch, Some(cred.clone()), |_| updates += 1);
        let revoke = async {
            tokio::time::sleep(Duration::from_millis(300)).await;
            shared_with.lock().unwrap().clear();
        };
        let (r, ()) = tokio::time::timeout(catalog::RECHECK_EVERY * 3, async { tokio::join!(sub, revoke) }).await.expect("closed by the re-check");
        assert_eq!(r, Err(RemoteError::refused("not-shared")));
        assert_eq!(updates, 1, "one catalog, then closed on revocation without any update");
        shared_with.lock().unwrap().push(guest_uid.clone());

        // A forged credential over the network: the guest's id, signed by another key.
        let other = Keypair::generate_ed25519();
        let forged = WEIRDNET.certify(&other, &guest_uid, &guest.peer_id().to_string(), "x", weirdnet_net::now_ms()).unwrap();
        let mut ch = sd.open(&b.peer_id().to_string(), v2::PROTOCOL).await.unwrap();
        ch.send_cbor(&ClientFrame::Infer(Request::messages(vec![]).with_cert(encode_credential(&guest_user.public(), &forged)))).await.unwrap();
        let r = ch.recv_cbor::<ServerFrame>().await.unwrap();
        assert!(matches!(r, Some(ServerFrame::Failed { error: RemoteError::Refused { ref message }, .. }) if message == "guest-bad-cert"), "{r:?}");
        // The guest's real credential presented by another peer (the stranger).
        let stolen = pool::NetDialer::new(stranger.clone());
        stolen.hint(b.peer_id(), vec![addr(&b).parse().unwrap()]);
        let mut ch = stolen.open(&b.peer_id().to_string(), catalog::PROTOCOL).await.unwrap();
        assert_eq!(refused(catalog::query(&mut ch, Some(cred.clone())).await), "wrong-peer");
        // A stranger with a share link and a valid credential of its own user, not on the list.
        let stranger_user = Keypair::generate_ed25519();
        let stranger_cert = certify(&stranger_user, &stranger, "laptop");
        let theirs = pool::connect_shared(stranger.clone(), &share, &stranger_user.public(), &stranger_cert).unwrap();
        assert_eq!(refused(theirs.catalog().await), "not-shared");

        // Default deny: a node that does not take guests refuses the guest (as before 0.5.9).
        let to_plain = PoolShare::new(user_id(&owner.public())).with_node(plain.peer_id().to_string(), vec![addr(&plain)]);
        let p = pool::connect_shared(guest.clone(), &to_plain, &guest_user.public(), &guest_cert).unwrap();
        assert_eq!(refused(p.catalog().await), "wrong-user");
        assert_eq!(
            refused(p.infer(Request::messages(vec![Message::user("hi")]), &NoEvents, &CancelToken::new()).await.map_err(|i| i.error)),
            "wrong-user"
        );

        // connect_shared checks what it is given.
        let expired = share.clone().with_expires_at(1);
        assert!(matches!(pool::connect_shared(guest.clone(), &expired, &guest_user.public(), &guest_cert), Err(RemoteError::BadRequest { .. })));
        assert!(matches!(pool::connect_shared(stranger.clone(), &share, &guest_user.public(), &guest_cert), Err(RemoteError::BadRequest { .. })));
        assert!(matches!(pool::connect_shared(guest.clone(), &share, &other.public(), &guest_cert), Err(RemoteError::BadRequest { .. })));
        let wrong_addr = PoolShare::new(user_id(&owner.public())).with_node(plain.peer_id().to_string(), vec![addr(&b)]);
        assert!(matches!(pool::share_dialer(guest.clone(), &wrong_addr), Err(RemoteError::BadRequest { .. })));
        let not_a_peer = PoolShare::new(user_id(&owner.public())).with_node("nope", vec![]);
        assert!(matches!(pool::share_dialer(guest.clone(), &not_a_peer), Err(RemoteError::BadRequest { .. })));

        drop(watch);
        drop(servers);
    });
    drop(rt);
    for n in [b, plain, guest, stranger] {
        n.halt();
    }
}
