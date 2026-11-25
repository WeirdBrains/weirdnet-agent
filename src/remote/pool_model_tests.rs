//! PoolModel and PoolWatch over in-memory channels.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::json;
use tokio::sync::mpsc;

use super::*;
use crate::remote::catalog::{CatalogFeed, CatalogModel, ModelState, Purpose};
use crate::remote::infer::v2::{self, Finish, InferBackend, Output};
use crate::remote::infer::{Admission, InferHost};
use crate::remote::pool::{PoolOptions, Stall};
use crate::remote::{Caller, Channel, FrameTx, MemoryDialer};
use crate::session::AgentEvent;
use crate::testing::ScriptedModel;
use crate::{ContentPart, ToolCall};

const PNG: &str = "data:image/png;base64,iVBORw0KGgo=";

#[derive(Clone)]
enum Step {
    Text(&'static str),
    Reasoning(&'static str),
    Call(&'static str),
    Sleep(u64),
    Hang,
}

struct Script {
    resumes: bool,
    steps: Vec<Step>,
    seen: Mutex<Vec<v2::Request>>,
    cancelled: AtomicUsize,
}

impl Script {
    fn new(resumes: bool, steps: Vec<Step>) -> Arc<Self> {
        Arc::new(Self { resumes, steps, seen: Mutex::new(Vec::new()), cancelled: AtomicUsize::new(0) })
    }
}

#[async_trait]
impl InferBackend for Script {
    fn name(&self) -> String {
        "script".into()
    }

    fn resumes(&self) -> bool {
        self.resumes
    }

    async fn infer(&self, request: &v2::Request, out: &Output, cancel: &CancelToken) -> Result<Finish, RemoteError> {
        self.seen.lock().unwrap().push(request.clone());
        for step in &self.steps {
            if cancel.is_cancelled() {
                break;
            }
            match step {
                Step::Text(t) => out.text(*t),
                Step::Reasoning(t) => out.reasoning(*t),
                Step::Call(name) => out.tool_call(ToolCall { id: format!("c-{name}"), name: (*name).into(), arguments: json!({"q": name}) }),
                Step::Sleep(ms) => tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(*ms)) => {}
                    _ = cancel.cancelled() => {}
                },
                Step::Hang => cancel.cancelled().await,
            }
        }
        if cancel.is_cancelled() {
            self.cancelled.fetch_add(1, Ordering::SeqCst);
            return Err(RemoteError::Cancelled);
        }
        Ok(Finish::new("stop"))
    }
}

struct Host {
    backend: Arc<dyn InferBackend>,
    busy: bool,
}

impl InferHost for Host {
    fn authorize(&self, caller: &Caller) -> Result<(), RemoteError> {
        if caller.peer == "stranger" { Err(RemoteError::refused("unknown-device")) } else { Ok(()) }
    }

    fn open(&self, _caller: &Caller, _model: Option<&str>) -> Result<Arc<dyn ModelBackend>, RemoteError> {
        Err(RemoteError::unavailable("infer/2 only"))
    }

    fn open_request(&self, _caller: &Caller, _request: &v2::Request) -> Result<Arc<dyn InferBackend>, RemoteError> {
        if self.busy { Err(RemoteError::Busy) } else { Ok(self.backend.clone()) }
    }
}

/// Sends `left` frames, then the node "dies".
struct Cut {
    inner: Option<Box<dyn FrameTx>>,
    left: usize,
}

#[async_trait]
impl FrameTx for Cut {
    async fn send(&mut self, message: Vec<u8>) -> Result<(), RemoteError> {
        if self.left == 0 {
            self.inner = None;
            return Err(RemoteError::Closed);
        }
        self.left -= 1;
        let r = match &mut self.inner {
            Some(tx) => tx.send(message).await,
            None => Err(RemoteError::Closed),
        };
        if self.left == 0 {
            self.inner = None;
        }
        r
    }
}

/// An inference node: infer/2 over `backend` and catalog/1 over `feed`.
fn node(dialer: &MemoryDialer, name: &str, backend: Arc<dyn InferBackend>, feed: CatalogFeed, cut: Option<usize>, busy: bool) {
    let host = Arc::new(Host { backend, busy });
    let admission = Admission::new(2);
    dialer.route_only(name, &[v2::PROTOCOL, catalog::PROTOCOL], move |ch, protocol| {
        let (host, admission, feed) = (host.clone(), admission.clone(), feed.clone());
        tokio::spawn(async move {
            if protocol == catalog::PROTOCOL {
                let _ = catalog::serve(ch, Caller::new("phone"), &feed).await;
            } else {
                let ch = match cut {
                    Some(left) => Channel::new(Box::new(Cut { inner: Some(ch.tx), left }), ch.rx),
                    None => ch,
                };
                let _ = v2::serve(ch, Caller::new("phone"), host.as_ref(), &admission).await;
            }
        });
    });
}

fn feed(models: Vec<CatalogModel>) -> CatalogFeed {
    let f = CatalogFeed::new(Arc::new(|_: &Caller| Ok(())));
    f.publish(models);
    f
}

fn gemma() -> CatalogModel {
    CatalogModel::new("gemma", Purpose::Chat, ModelState::Warm).with_speed(40.0)
}

fn pool(dialer: &MemoryDialer, nodes: impl NodeSource + 'static) -> Pool {
    let opts = PoolOptions::default()
        .with_accept_timeout(Duration::from_secs(2))
        .with_stall(Stall::new(Duration::from_secs(1), 3, Duration::from_secs(2)))
        .with_infer_v1_fallback(false);
    Pool::new(Arc::new(dialer.clone()), nodes).with_options(opts)
}

fn names(nodes: &[&str]) -> Vec<String> {
    nodes.iter().map(|n| n.to_string()).collect()
}

fn sink() -> (DeltaSink, mpsc::UnboundedReceiver<AgentEvent>) {
    let (tx, rx) = mpsc::unbounded_channel();
    (DeltaSink::new(Some(tx)), rx)
}

fn drain(rx: &mut mpsc::UnboundedReceiver<AgentEvent>) -> (String, String, Vec<ModelNotice>) {
    let (mut text, mut reasoning, mut notices) = (String::new(), String::new(), Vec::new());
    while let Ok(e) = rx.try_recv() {
        match e {
            AgentEvent::TextDelta(t) => text.push_str(&t),
            AgentEvent::ReasoningDelta(t) => reasoning.push_str(&t),
            AgentEvent::ModelNotice(n) => notices.push(n),
            _ => {}
        }
    }
    (text, reasoning, notices)
}

fn local() -> Arc<dyn ModelBackend> {
    Arc::new(ScriptedModel::new(vec![ModelTurn { text: "from the local model".into(), tool_calls: Vec::new() }]))
}

#[tokio::test]
async fn a_turn_streams_text_reasoning_and_tool_calls_from_the_pool() {
    let dialer = MemoryDialer::new();
    let script = Script::new(false, vec![Step::Reasoning("hm"), Step::Text("Hel"), Step::Text("lo"), Step::Call("search")]);
    node(&dialer, "inference", script.clone(), feed(vec![gemma()]), None, false);
    let model = PoolModel::new(pool(&dialer, names(&["inference"]))).with_model("gemma");
    let (deltas, mut rx) = sink();
    let tools = vec![ToolSpec::new("search", "Search the web")];
    let turn = model.complete(&[Message::user("hi")], &tools, &deltas, &CancelToken::new()).await.unwrap();
    assert_eq!(turn.text, "Hello");
    assert_eq!(turn.tool_calls.len(), 1);
    assert_eq!(turn.tool_calls[0].name, "search");
    assert_eq!(drain(&mut rx), ("Hello".into(), "hm".into(), vec![]));
    let seen = script.seen.lock().unwrap()[0].clone();
    assert_eq!(seen.model.as_deref(), Some("gemma"));
    assert_eq!(seen.tools.len(), 1, "tools go to the node");
    assert_eq!(model.last_route(), Some(Route::Node { node: "inference".into(), model: "script".into() }));
    assert_eq!(model.name(), "script@inference");
}

#[tokio::test]
async fn a_node_that_dies_mid_answer_continues_on_another_with_a_notice() {
    let dialer = MemoryDialer::new();
    let a = Script::new(true, vec![Step::Text("Hello "), Step::Text("there "), Step::Sleep(50), Step::Text("never"), Step::Hang]);
    let b = Script::new(true, vec![Step::Text("world")]);
    // Accepted, Delta, Delta, then the channel goes.
    node(&dialer, "a", a, feed(vec![gemma()]), Some(3), false);
    node(&dialer, "b", b.clone(), feed(vec![gemma()]), None, false);
    let model = PoolModel::new(pool(&dialer, names(&["a", "b"]))).with_fallback(local());
    let (deltas, mut rx) = sink();
    let turn = model.complete(&[Message::user("q")], &[], &deltas, &CancelToken::new()).await.unwrap();
    assert_eq!(turn.text, "Hello there world");
    let (text, _, notices) = drain(&mut rx);
    assert_eq!(text, "Hello there world", "no text twice");
    assert_eq!(notices, [ModelNotice::Continued { node: "b".into(), offset: 12 }]);
    assert_eq!(b.seen.lock().unwrap()[0].attempt, 1);
}

#[tokio::test]
async fn an_unreachable_pool_is_pool_unreachable_or_falls_back_with_a_notice() {
    let dialer = MemoryDialer::new();
    let err = PoolModel::new(pool(&dialer, names(&["gone"]))).complete(&[Message::user("q")], &[], &DeltaSink::discard(), &CancelToken::new()).await;
    assert!(matches!(&err, Err(ModelError::PoolUnreachable(why)) if why.contains("gone")), "{err:?}");
    let none = PoolModel::new(pool(&dialer, Vec::<String>::new())).complete(&[], &[], &DeltaSink::discard(), &CancelToken::new()).await;
    assert!(matches!(none, Err(ModelError::PoolUnreachable(_))), "no node at all: {none:?}");

    let model = PoolModel::new(pool(&dialer, names(&["gone"]))).with_fallback(local());
    let (deltas, mut rx) = sink();
    let turn = model.complete(&[Message::user("q")], &[], &deltas, &CancelToken::new()).await.unwrap();
    assert_eq!(turn.text, "from the local model");
    let (_, _, notices) = drain(&mut rx);
    assert!(matches!(&notices[..], [ModelNotice::FellBack { reason, .. }] if reason.starts_with("model pool unreachable")), "{notices:?}");
    assert_eq!(model.last_route(), Some(Route::Fallback));
}

#[tokio::test]
async fn a_busy_pool_before_any_text_falls_back_too_but_says_unavailable() {
    let dialer = MemoryDialer::new();
    node(&dialer, "full", Script::new(false, vec![Step::Text("no")]), feed(vec![gemma()]), None, true);
    let err = PoolModel::new(pool(&dialer, names(&["full"]))).complete(&[], &[], &DeltaSink::discard(), &CancelToken::new()).await;
    assert!(matches!(&err, Err(ModelError::Unavailable(why)) if why.contains("busy")), "a node's answer is not unreachable: {err:?}");
    let model = PoolModel::new(pool(&dialer, names(&["full"]))).with_fallback(local());
    let (deltas, mut rx) = sink();
    assert_eq!(model.complete(&[], &[], &deltas, &CancelToken::new()).await.unwrap().text, "from the local model");
    assert!(matches!(&drain(&mut rx).2[..], [ModelNotice::FellBack { .. }]));
}

#[tokio::test]
async fn text_already_shown_is_not_redone_on_the_fallback() {
    let dialer = MemoryDialer::new();
    let a = Script::new(false, vec![Step::Text("partial "), Step::Sleep(50), Step::Hang]);
    node(&dialer, "a", a, feed(vec![gemma()]), Some(2), false);
    let model = PoolModel::new(pool(&dialer, names(&["a"]))).with_fallback(local());
    let (deltas, mut rx) = sink();
    let err = model.complete(&[], &[], &deltas, &CancelToken::new()).await.unwrap_err();
    assert!(matches!(err, ModelError::Failed(_)), "{err:?}");
    let (text, _, notices) = drain(&mut rx);
    assert_eq!(text, "partial ");
    assert!(notices.is_empty(), "no fallback after text: {notices:?}");
}

#[tokio::test]
async fn cancel_stops_the_node_and_skips_the_fallback() {
    let dialer = MemoryDialer::new();
    let a = Script::new(false, vec![Step::Text("x"), Step::Hang]);
    node(&dialer, "a", a.clone(), feed(vec![gemma()]), None, false);
    let model = Arc::new(PoolModel::new(pool(&dialer, names(&["a"]))).with_fallback(local()));
    let cancel = CancelToken::new();
    let (m, c) = (model.clone(), cancel.clone());
    let task = tokio::spawn(async move { m.complete(&[], &[], &DeltaSink::discard(), &c).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    cancel.cancel();
    assert_eq!(task.await.unwrap(), Err(ModelError::Cancelled));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(a.cancelled.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_text_only_choice_is_rerouted_for_images() {
    let dialer = MemoryDialer::new();
    let script = Script::new(false, vec![Step::Text("a cat")]);
    let models = vec![gemma(), CatalogModel::new("qwen-vl", Purpose::Chat, ModelState::Loadable).with_inputs(Modalities::VISION)];
    node(&dialer, "inference", script.clone(), feed(models), None, false);
    let model = PoolModel::new(pool(&dialer, names(&["inference"]))).with_model("gemma");
    let (deltas, mut rx) = sink();
    let msg = Message::user(vec![ContentPart::text("what is it?"), ContentPart::image(PNG)]);
    assert_eq!(model.complete(std::slice::from_ref(&msg), &[], &deltas, &CancelToken::new()).await.unwrap().text, "a cat");
    assert_eq!(script.seen.lock().unwrap()[0].model.as_deref(), Some("qwen-vl"), "the pool picked a model that reads images");
    assert_eq!(drain(&mut rx).2, [ModelNotice::Rerouted { from: "gemma".into(), needs: "images".into() }]);
    let strict = PoolModel::new(pool(&dialer, names(&["inference"]))).with_model("gemma").with_reroute(false);
    let err = strict.complete(&[msg], &[], &DeltaSink::discard(), &CancelToken::new()).await.unwrap_err();
    assert!(matches!(err, ModelError::Unavailable(_)), "without reroute the pool refuses: {err:?}");
}

async fn status_until(watch: &mut PoolWatch, f: impl Fn(&PoolStatus) -> bool) -> PoolStatus {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let s = watch.status();
            if f(&s) {
                return s;
            }
            watch.changed().await.expect("the watch runs");
        }
    })
    .await
    .expect("status in time")
}

#[tokio::test]
async fn the_watch_follows_the_catalog_and_names_the_nodes_that_answered() {
    let dialer = MemoryDialer::new();
    // A linked device that is not an inference node (a Morph desktop): it does not serve catalog/1.
    dialer.route_only("desktop", &[v2::PROTOCOL], |_, _| {});
    let f = feed(vec![
        CatalogModel::new("small", Purpose::Chat, ModelState::Loadable),
        CatalogModel::new("embed", Purpose::Embedding, ModelState::Warm),
        gemma(),
        CatalogModel::new("vision", Purpose::Chat, ModelState::Busy).with_inputs(Modalities::VISION),
    ]);
    node(&dialer, "inference", Script::new(false, vec![Step::Text("ok")]), f.clone(), None, false);
    let mut watch = PoolWatch::spawn(pool(&dialer, names(&["gone", "desktop", "inference"])));
    let live = status_until(&mut watch, PoolStatus::is_live).await;
    assert!(matches!(&live, PoolStatus::Live { node, .. } if node == "inference"));
    let ids = |s: &PoolStatus, n: Modalities| s.chat_models(n).iter().map(|m| m.id.clone()).collect::<Vec<_>>();
    assert_eq!(ids(&live, Modalities::TEXT), ["gemma", "small", "vision"], "chat models, ready first");
    assert_eq!(ids(&live, Modalities::VISION), ["vision"], "models that can't read the page's images are left out");
    assert_eq!(watch.nodes().get(), ["inference"], "only nodes that answered run turns");

    f.publish(vec![gemma(), CatalogModel::new("new", Purpose::Chat, ModelState::Loading)]);
    let changed = status_until(&mut watch, |s| s.catalog().is_some_and(|c| c.models.len() == 2)).await;
    assert!(changed.is_live());

    // Turns use the watch's nodes.
    let model = PoolModel::new(pool(&dialer, watch.nodes()));
    assert_eq!(model.complete(&[], &[], &DeltaSink::discard(), &CancelToken::new()).await.unwrap().text, "ok");
}

#[tokio::test]
async fn the_watch_says_unreachable_and_keeps_the_last_catalog() {
    let dialer = MemoryDialer::new();
    let mut watch = PoolWatch::spawn(pool(&dialer, names(&["gone"])));
    let down = status_until(&mut watch, |s| matches!(s, PoolStatus::Unreachable { .. })).await;
    assert!(matches!(down, PoolStatus::Unreachable { error: RemoteError::Unreachable { .. }, last: None }));
    let mut empty = PoolWatch::spawn(pool(&dialer, Vec::<String>::new()));
    let none = status_until(&mut empty, |s| matches!(s, PoolStatus::Unreachable { .. })).await;
    assert!(matches!(none, PoolStatus::Unreachable { error: RemoteError::Unavailable { .. }, .. }), "{none:?}");

    // A node that answers and then goes away.
    dialer.route("inference", |mut ch, _| {
        tokio::spawn(async move {
            let _ = ch.recv_cbor::<catalog::ClientMsg>().await;
            let _ = ch.send_cbor(&catalog::ServerMsg::Catalog(Catalog::new(1, vec![gemma()]))).await;
            tokio::time::sleep(Duration::from_millis(200)).await;
        });
    });
    let mut watch = PoolWatch::spawn(pool(&dialer, names(&["inference"])));
    status_until(&mut watch, PoolStatus::is_live).await;
    assert_eq!(watch.nodes().get(), ["inference"]);
    dialer.unroute("inference");
    let gone = status_until(&mut watch, |s| matches!(s, PoolStatus::Unreachable { .. })).await;
    assert_eq!(gone.catalog().map(|c| c.models[0].id.as_str()), Some("gemma"), "the last catalog stays for the picker");
    // On the next round it can't be reached, so turns stop going to it.
    let forgotten = tokio::time::timeout(Duration::from_secs(5), async {
        while !watch.nodes().get().is_empty() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });
    assert!(forgotten.await.is_ok(), "the node is forgotten once it fails to answer");
}

#[tokio::test]
async fn retry_asks_again_without_waiting_out_the_backoff() {
    let dialer = MemoryDialer::new();
    let mut watch = PoolWatch::spawn(pool(&dialer, names(&["inference"])));
    // Three failed rounds: the next retry would be 4 s away.
    for _ in 0..3 {
        status_until(&mut watch, |s| matches!(s, PoolStatus::Unreachable { .. })).await;
        watch.changed().await;
    }
    node(&dialer, "inference", Script::new(false, vec![Step::Text("ok")]), feed(vec![gemma()]), None, false);
    watch.retry();
    let live = tokio::time::timeout(Duration::from_millis(1500), status_until(&mut watch, PoolStatus::is_live)).await;
    assert!(live.is_ok(), "the retry found the node before the backoff ran out");
}
