//! Infer/2, catalog/1, failover and resume between in-process `weirdnet-net` nodes (memory
//! transport): an app node, two inference nodes of the same user, and a stranger.
#![cfg(all(feature = "pool", feature = "serve"))]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use weirdnet_agent::pool::{LinkedNodes, NetDialer, Pool, PoolEvent, PoolOptions, Stall, encode_cert};
use weirdnet_agent::remote::catalog::{self, CatalogFeed, CatalogModel, ModelState, Purpose};
use weirdnet_agent::remote::infer::v2::{self, ClientFrame, Finish, InferBackend, Output, Prefix, Request, Resume, ServerFrame};
use weirdnet_agent::remote::infer::{self, Admission, ClientMsg, InferHost, InferRequest, ServerMsg};
use weirdnet_agent::remote::{Authorize, Caller, Dialer, RemoteError};
use weirdnet_agent::serve::{RosterAuth, Server, Serving};
use weirdnet_agent::{CancelToken, DeltaSink, Message, ModelBackend, ModelError, ModelTurn, ToolSpec};
use weirdnet_net::node::{Handler, Node, NodeConfig};
use weirdnet_net::user::NodeCert;
use weirdnet_net::{Keypair, Multiaddr, PeerId, WEIRDNET};
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

fn addr(n: &N) -> Multiaddr {
    n.reachable_addrs().into_iter().find(|a| a.starts_with("/memory/") && !a.contains("p2p-circuit")).unwrap().parse().unwrap()
}

/// Streams words; with model "die" it stops after two and waits (its node is then halted);
/// a resumed request continues from the prefix.
struct Words {
    node: &'static str,
    seen: Mutex<Vec<Request>>,
    cancelled: AtomicUsize,
}

#[async_trait]
impl InferBackend for Words {
    fn name(&self) -> String {
        format!("gemma@{}", self.node)
    }

    fn resumes(&self) -> bool {
        true
    }

    async fn infer(&self, request: &Request, out: &Output, cancel: &CancelToken) -> Result<Finish, RemoteError> {
        self.seen.lock().unwrap().push(request.clone());
        let words: &[&str] = match request.assistant_prefix.as_ref() {
            Some(p) if p.text == "Hello there " => &["world"],
            _ => &["Hello ", "there ", "world"],
        };
        for (i, w) in words.iter().enumerate() {
            if request.model.as_deref() == Some("die") && i == 2 {
                cancel.cancelled().await;
                self.cancelled.fetch_add(1, Ordering::SeqCst);
                return Err(RemoteError::Cancelled);
            }
            out.text(*w);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok(Finish::new("stop"))
    }
}

struct Host {
    auth: Arc<RosterAuth>,
    words: Arc<Words>,
}

struct Echo;

#[async_trait]
impl ModelBackend for Echo {
    fn name(&self) -> String {
        "echo".into()
    }
    async fn complete(&self, m: &[Message], _t: &[ToolSpec], deltas: &DeltaSink, _c: &CancelToken) -> Result<ModelTurn, ModelError> {
        let text = m.last().map(|m| m.content.clone()).unwrap_or_default();
        deltas.text(text.clone());
        Ok(ModelTurn { text, tool_calls: vec![] })
    }
}

impl InferHost for Host {
    fn authorize(&self, caller: &Caller) -> Result<(), RemoteError> {
        self.auth.authorize(caller)
    }
    fn open(&self, _caller: &Caller, _model: Option<&str>) -> Result<Arc<dyn ModelBackend>, RemoteError> {
        Ok(Arc::new(Echo))
    }
    fn open_request(&self, _caller: &Caller, _request: &Request) -> Result<Arc<dyn InferBackend>, RemoteError> {
        Ok(self.words.clone())
    }
}

fn certify(user: &Keypair, n: &N, label: &str) -> NodeCert {
    Roster::self_certify(&WEIRDNET, user, &n.peer_id().to_string(), label, weirdnet_net::now_ms()).unwrap()
}

#[test]
fn infer2_catalog_failover_and_resume_over_weirdnet() {
    let user = Keypair::generate_ed25519();
    let (app, b, c, stranger) = (start(), start(), start(), start());
    let app_cert = certify(&user, &app, "phone");
    let mut roster = Roster::new();
    roster.upsert(app_cert.clone());
    roster.upsert(certify(&user, &b, "node-a"));
    roster.upsert(certify(&user, &c, "node-b"));
    let roster = Arc::new(roster);
    let view: Arc<dyn Fn() -> Roster + Send + Sync> = {
        let r = roster.clone();
        Arc::new(move || (*r).clone())
    };

    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    rt.block_on(async {
        let auth = Arc::new(RosterAuth::new(&WEIRDNET, user.public(), view.clone()));
        let feed = CatalogFeed::new(auth.clone());
        feed.publish(vec![CatalogModel::new("gemma", Purpose::Chat, ModelState::Warm).with_replicas(2)]);
        let mut servers = Vec::new();
        let mut words = Vec::new();
        for (n, name) in [(&b, "node-a"), (&c, "node-b")] {
            let w = Arc::new(Words { node: name, seen: Mutex::new(Vec::new()), cancelled: AtomicUsize::new(0) });
            words.push(w.clone());
            let host: Arc<dyn InferHost> = Arc::new(Host { auth: auth.clone(), words: w });
            let serving = Serving { infer: Some((host, Admission::with_queue(2, 4))), catalog: Some(Arc::new(feed.clone())), announce: true };
            servers.push(Server::start(n, serving).unwrap());
        }

        let dialer = NetDialer::new(app.clone());
        dialer.hint(b.peer_id(), vec![addr(&b)]);
        dialer.hint(c.peer_id(), vec![addr(&c)]);
        let linked = LinkedNodes::new(app.clone(), view.clone()).with_hint(b.peer_id(), vec![addr(&b)]).with_hint(c.peer_id(), vec![addr(&c)]);
        let opts = PoolOptions::default().with_cert(Some(encode_cert(&app_cert))).with_stall(Stall::new(
            Duration::from_millis(800),
            3,
            Duration::from_secs(2),
        ));
        let pool = Pool::new(Arc::new(dialer), linked).with_options(opts);

        // Discovery: both inference nodes, in roster order; not the app itself.
        let nodes = pool.nodes().await;
        assert_eq!(nodes, [b.peer_id().to_string(), c.peer_id().to_string()]);

        // Catalog: query, then subscribe and see a change.
        let cat = pool.catalog().await.unwrap();
        assert_eq!(cat.find("gemma").unwrap().replicas, 2);
        let mut watch = pool.subscribe();
        let (_, first) = watch.changed().await.unwrap();
        assert_eq!(first.rev, cat.rev);
        feed.publish(vec![CatalogModel::new("gemma", Purpose::Chat, ModelState::Busy)]);
        let (from, next) = watch.changed().await.unwrap();
        assert_eq!((next.find("gemma").unwrap().state, from), (ModelState::Busy, b.peer_id().to_string()));

        // A whole answer over infer/2 from the first node.
        let a = pool.infer(Request::messages(vec![Message::user("hi")]), &weirdnet_agent::remote::pool::NoEvents, &CancelToken::new()).await.unwrap();
        assert_eq!((a.text.as_str(), a.model.as_str()), ("Hello there world", "gemma@node-a"));

        // Failover mid-answer: node-a goes away after two words; the pool cancels on the stall and
        // resumes on node-b from the twelve characters already shown.
        let (tx, mut rx) = mpsc::unbounded_channel();
        let job = {
            let pool = pool.clone();
            tokio::spawn(async move {
                pool.infer(Request::messages(vec![Message::user("hi")]).with_model("die").with_job_id("j-1"), &tx, &CancelToken::new()).await
            })
        };
        let mut shown = String::new();
        while shown != "Hello there " {
            if let Some(PoolEvent::Text(t)) = rx.recv().await {
                shown.push_str(&t);
            }
        }
        let gone = b.clone();
        tokio::task::spawn_blocking(move || gone.halt()).await.unwrap();
        let answer = job.await.unwrap().unwrap();
        assert_eq!(answer.text, "Hello there world");
        assert_eq!(answer.node, c.peer_id().to_string());
        assert_eq!(answer.meta.resumes, [Resume::new(12, c.peer_id().to_string())]);
        while let Ok(ev) = rx.try_recv() {
            if let PoolEvent::Text(t) = ev {
                shown.push_str(&t);
            }
        }
        assert_eq!(shown, "Hello there world", "nothing shown twice");
        let resumed = words[1].seen.lock().unwrap().last().cloned().unwrap();
        assert_eq!((resumed.job_id.as_str(), resumed.attempt), ("j-1", 1));
        assert_eq!(resumed.assistant_prefix, Some(Prefix::new("Hello there ", "")));
        assert_eq!(resumed.cert.as_deref(), None, "the host took the presented cert off the request");

        // infer/1 is still served, with the caller checked.
        let mut ch = Arc::new({
            let d = NetDialer::new(app.clone());
            d.hint(c.peer_id(), vec![addr(&c)]);
            d
        })
        .open(&c.peer_id().to_string(), infer::PROTOCOL)
        .await
        .unwrap();
        ch.send(&ClientMsg::Infer { request: InferRequest { id: "1".into(), model: None, messages: vec![Message::user("echo me")], tools: vec![] } })
            .await
            .unwrap();
        let mut got = Vec::new();
        while let Some(m) = ch.recv::<ServerMsg>().await.unwrap() {
            let done = matches!(m, ServerMsg::Done { .. });
            got.push(m);
            if done {
                break;
            }
        }
        assert!(matches!(got.last(), Some(ServerMsg::Done { turn, .. }) if turn.text == "echo me"), "{got:?}");

        // A device outside the roster is refused on every protocol, even with a cert of its own.
        let foreign = Keypair::generate_ed25519();
        let fake = Roster::self_certify(&WEIRDNET, &foreign, &stranger.peer_id().to_string(), "x", weirdnet_net::now_ms()).unwrap();
        let sd = NetDialer::new(stranger.clone());
        sd.hint(c.peer_id(), vec![addr(&c)]);
        let mut ch = sd.open(&c.peer_id().to_string(), v2::PROTOCOL).await.unwrap();
        let req = Request::messages(vec![]).with_cert(encode_cert(&fake));
        ch.send_cbor(&ClientFrame::Infer(req)).await.unwrap();
        let refused = ch.recv_cbor::<ServerFrame>().await.unwrap();
        assert!(
            matches!(refused, Some(ServerFrame::Failed { error: RemoteError::Refused { ref message }, .. }) if message == "wrong-user"),
            "{refused:?}"
        );
        let mut ch = sd.open(&c.peer_id().to_string(), catalog::PROTOCOL).await.unwrap();
        assert_eq!(catalog::query(&mut ch, None).await.unwrap_err(), RemoteError::refused("unknown-device"));
        // A protocol the node does not serve.
        assert!(matches!(sd.open(&c.peer_id().to_string(), "/weirdnet-agent/nothing/1").await.err(), Some(RemoteError::Unsupported { .. })));
        drop(watch);
        drop(servers);
    });
    drop(rt);
}

struct Tasks {
    seen: Mutex<Vec<Option<Vec<String>>>>,
}

impl weirdnet_agent::remote::task::TaskHost for Tasks {
    fn session(
        &self,
        _r: &weirdnet_agent::remote::task::TaskRequest,
        _a: Arc<dyn weirdnet_agent::Approver>,
    ) -> Result<weirdnet_agent::AgentSession, RemoteError> {
        Err(RemoteError::refused("session_for is used"))
    }

    fn session_for(
        &self,
        caller: &weirdnet_agent::Admitted,
        _r: &weirdnet_agent::remote::task::TaskRequest,
        approver: Arc<dyn weirdnet_agent::Approver>,
    ) -> Result<weirdnet_agent::AgentSession, RemoteError> {
        self.seen.lock().unwrap().push(caller.roles.clone());
        let identity = weirdnet_agent::AgentIdentity::new("atlas", "Atlas", weirdnet_agent::AgentKind::Node).with_node("node-a");
        let tools = Arc::new(weirdnet_agent::tool::ToolRegistry::new());
        let policy = weirdnet_agent::policy::Policy::new().shared();
        Ok(weirdnet_agent::AgentSession::new(weirdnet_agent::AgentConfig::new(identity), Arc::new(Echo), tools, policy, approver))
    }
}

struct Deny;

#[async_trait]
impl weirdnet_agent::Approver for Deny {
    async fn decide(&self, _p: &weirdnet_agent::policy::ApprovalPrompt, _t: &ToolSpec) -> weirdnet_agent::ApprovalAnswer {
        weirdnet_agent::ApprovalAnswer::Deny
    }
}

#[test]
fn tasks_are_admitted_by_roles_over_weirdnet() {
    use weirdnet_agent::remote::task::{self, TaskRequest};
    use weirdnet_agent::serve::{RoleView, ServingExtras};
    let user = Keypair::generate_ed25519();
    let (app, node, stranger) = (start(), start(), start());
    let mut roster = Roster::new();
    roster.upsert(certify(&user, &app, "phone"));
    roster.upsert(certify(&user, &node, "node-a"));
    let view: Arc<dyn Fn() -> Roster + Send + Sync> = Arc::new(move || roster.clone());
    // The app device's grant: `ymir.app`, later `ymir.inference` too.
    let granted = Arc::new(Mutex::new(vec!["ymir.app".to_string()]));
    let roles: RoleView = {
        let (g, app_peer) = (granted.clone(), app.peer_id().to_string());
        Arc::new(move |peer: &str| (peer == app_peer).then(|| g.lock().unwrap().clone()))
    };
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    rt.block_on(async {
        let auth = Arc::new(RosterAuth::new(&WEIRDNET, user.public(), view).require_roles(&["ymir.inference"], roles));
        let host = Arc::new(Tasks { seen: Mutex::new(Vec::new()) });
        let serving = Serving { infer: None, catalog: None, announce: true };
        let _server = Server::start_with(&node, serving, ServingExtras::default().tasks(host.clone(), auth.clone())).unwrap();

        let ask = |from: &Arc<N>| {
            let dialer = NetDialer::new(from.clone());
            dialer.hint(node.peer_id(), vec![addr(&node)]);
            let peer = node.peer_id().to_string();
            async move {
                let req = TaskRequest::new(weirdnet_agent::AgentId::new("atlas"), "ping", vec![]);
                task::run(&dialer, &peer, req, None, &Deny, &CancelToken::new()).await
            }
        };
        let err = ask(&app).await.unwrap_err();
        assert!(matches!(&err, RemoteError::Refused { message } if message.contains("role")), "{err:?}");
        let err = ask(&stranger).await.unwrap_err();
        assert!(matches!(&err, RemoteError::Refused { message } if message.contains("unknown-device")), "{err:?}");
        assert!(host.seen.lock().unwrap().is_empty(), "refused before any session");

        granted.lock().unwrap().push("ymir.inference".into());
        let got = ask(&app).await.unwrap();
        assert_eq!(got.outcome.text, "ping");
        assert_eq!(*host.seen.lock().unwrap(), [Some(vec!["ymir.app".to_string(), "ymir.inference".to_string()])]);

        // The default `admit` (a closure authorizer) knows no roles.
        let open = |_c: &Caller| -> Result<(), RemoteError> { Ok(()) };
        let who = open.admit(&Caller::new("p")).unwrap();
        assert_eq!((who.peer.as_str(), who.roles.clone()), ("p", None));
        assert!(!who.has_role("ymir.inference"));
    });
    for n in [app, node, stranger] {
        n.halt();
    }
}
