//! Infer/2 and the pool over in-memory channels.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::json;
use tokio::sync::mpsc;

use super::v2::{
    self, ClientFrame, ContextRef, EmbedBackend, EmbedRequest, Finish, InferBackend, Output, Prefix, Priority, Request, ResponseFormat, Resume,
    ServerFrame, Usage, bytes_f32, f32_bytes,
};
use super::{Admission, ClientMsg, InferHost, InferRequest, PROTOCOL as V1, RemoteModel, Route, serve as serve_v1};
use crate::cancel::CancelToken;
use crate::model::{ContentPart, DeltaSink, Message, Modalities, ModelBackend, ModelError, ModelTurn, ToolCall};
use crate::remote::catalog::{self, CatalogFeed, CatalogModel, ModelState, Purpose};
use crate::remote::pool::{NoEvents, Pool, PoolEvent, PoolOptions, Stall};
use crate::remote::{Caller, Channel, RemoteError};
use crate::remote::{FrameTx, MemoryDialer, from_cbor, memory_pair, to_cbor};
use crate::testing::ScriptedModel;
use crate::tool::ToolSpec;

#[derive(Clone)]
enum Step {
    Text(&'static str),
    Reasoning(&'static str),
    Call(&'static str),
    Usage(u64, u64),
    /// Early metadata: a cache hit.
    CacheHit,
    Sleep(u64),
    /// Wait until cancelled.
    Hang,
}

/// An infer/2 backend that plays a script, records what it saw and whether it was cancelled.
struct Script {
    name: &'static str,
    resumes: bool,
    steps: Vec<Step>,
    seen: Mutex<Vec<Request>>,
    cancelled: AtomicUsize,
}

impl Script {
    fn new(name: &'static str, resumes: bool, steps: Vec<Step>) -> Arc<Self> {
        Arc::new(Self { name, resumes, steps, seen: Mutex::new(Vec::new()), cancelled: AtomicUsize::new(0) })
    }

    fn seen(&self) -> Vec<Request> {
        self.seen.lock().unwrap().clone()
    }
}

#[async_trait]
impl InferBackend for Script {
    fn name(&self) -> String {
        self.name.into()
    }

    fn resumes(&self) -> bool {
        self.resumes
    }

    async fn infer(&self, request: &Request, out: &Output, cancel: &CancelToken) -> Result<Finish, RemoteError> {
        self.seen.lock().unwrap().push(request.clone());
        for step in &self.steps {
            if cancel.is_cancelled() {
                break;
            }
            match step {
                Step::Text(t) => out.text(*t),
                Step::Reasoning(t) => out.reasoning(*t),
                Step::Call(name) => out.tool_call(ToolCall { id: format!("c-{name}"), name: (*name).into(), arguments: json!({"q": name}) }),
                Step::Usage(p, c) => out.usage(Usage::new(*p, *c)),
                Step::CacheHit => out.meta(v2::Meta { cache_hit: true, ..Default::default() }),
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
        Ok(Finish::new("stop").with_usage(Usage::new(3, 4)))
    }
}

struct Embedder;

#[async_trait]
impl EmbedBackend for Embedder {
    fn name(&self) -> String {
        "embedder".into()
    }

    async fn embed(&self, input: &str, _cancel: &CancelToken) -> Result<Vec<f32>, RemoteError> {
        Ok(vec![input.len() as f32, 0.5, -1.25])
    }
}

struct Host {
    backend: Arc<dyn InferBackend>,
}

impl InferHost for Host {
    fn authorize(&self, caller: &Caller) -> Result<(), RemoteError> {
        if caller.peer == "stranger" { Err(RemoteError::refused("unknown-device")) } else { Ok(()) }
    }

    fn open(&self, _caller: &Caller, _model: Option<&str>) -> Result<Arc<dyn ModelBackend>, RemoteError> {
        Err(RemoteError::unavailable("infer/2 only"))
    }

    fn open_request(&self, _caller: &Caller, _request: &Request) -> Result<Arc<dyn InferBackend>, RemoteError> {
        Ok(self.backend.clone())
    }

    fn open_embed(&self, _caller: &Caller, _model: Option<&str>) -> Result<Arc<dyn EmbedBackend>, RemoteError> {
        Ok(Arc::new(Embedder))
    }
}

/// Sends `limit` frames, then the node "dies": its side of the channel goes away.
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

fn serving(dialer: &MemoryDialer, node: &str, backend: Arc<dyn InferBackend>, admission: Admission, cut: Option<usize>, peer: &'static str) {
    let host = Arc::new(Host { backend });
    dialer.route_only(node, &[v2::PROTOCOL], move |ch, _| {
        let (host, admission) = (host.clone(), admission.clone());
        let ch = match cut {
            Some(left) => Channel::new(Box::new(Cut { inner: Some(ch.tx), left }), ch.rx),
            None => ch,
        };
        tokio::spawn(async move {
            let _ = v2::serve(ch, Caller::new(peer), host.as_ref(), &admission).await;
        });
    });
}

fn pool(dialer: MemoryDialer, nodes: &[&str]) -> Pool {
    let opts =
        PoolOptions::default().with_accept_timeout(Duration::from_secs(5)).with_stall(Stall::new(Duration::from_secs(2), 3, Duration::from_secs(4)));
    Pool::new(Arc::new(dialer), nodes.iter().map(|n| n.to_string()).collect::<Vec<_>>()).with_options(opts)
}

fn events() -> (mpsc::UnboundedSender<PoolEvent>, mpsc::UnboundedReceiver<PoolEvent>) {
    mpsc::unbounded_channel()
}

fn drain(rx: &mut mpsc::UnboundedReceiver<PoolEvent>) -> Vec<PoolEvent> {
    let mut v = Vec::new();
    while let Ok(e) = rx.try_recv() {
        v.push(e);
    }
    v
}

fn shown(evs: &[PoolEvent]) -> String {
    evs.iter().filter_map(|e| if let PoolEvent::Text(t) = e { Some(t.as_str()) } else { None }).collect()
}

#[tokio::test]
async fn text_reasoning_tool_calls_usage_and_metadata_stream_in_order() {
    let dialer = MemoryDialer::new();
    let script = Script::new(
        "gemma@node-b",
        false,
        vec![
            Step::Reasoning("let me "),
            Step::Reasoning("think"),
            Step::Text("Héllo "),
            Step::Text("wörld"),
            Step::Usage(3, 2),
            Step::Call("search"),
        ],
    );
    serving(&dialer, "inference", script.clone(), Admission::new(1), None, "phone");
    let (tx, mut rx) = events();
    let request = Request::messages(vec![Message::user("hi")]).with_model("gemma").with_seed(7).with_response_format(ResponseFormat::Json);
    let answer = pool(dialer, &["inference"]).infer(request.clone(), &tx, &CancelToken::new()).await.unwrap();
    assert_eq!(answer.text, "Héllo wörld");
    assert_eq!(answer.reasoning, "let me think");
    assert_eq!(answer.tool_calls.len(), 1);
    assert_eq!(answer.tool_calls[0].name, "search");
    assert_eq!(answer.finish_reason.as_deref(), Some("stop"));
    assert_eq!(answer.usage, Some(Usage::new(3, 4)));
    assert_eq!((answer.node.as_str(), answer.model.as_str()), ("inference", "gemma@node-b"));
    assert_eq!(answer.meta.served_by.as_deref(), Some("gemma@node-b"));
    assert!(answer.meta.resumes.is_empty() && answer.meta.resumed_from.is_none());
    let seen = script.seen();
    assert_eq!((seen[0].seed, seen[0].response_format.clone(), seen[0].attempt), (Some(7), Some(ResponseFormat::Json), 0));
    let evs = drain(&mut rx);
    assert!(matches!(&evs[0], PoolEvent::Accepted { node, model, resume: false } if node == "inference" && model == "gemma@node-b"));
    assert_eq!(shown(&evs), "Héllo wörld");
    assert!(evs.contains(&PoolEvent::Usage(Usage::new(3, 2))));
}

#[tokio::test]
async fn frames_carry_character_offsets() {
    let script = Script::new(
        "m",
        false,
        vec![Step::Text("añb"), Step::Reasoning("r1"), Step::Text("c"), Step::Reasoning("r2"), Step::Call("a"), Step::Call("b")],
    );
    let host = Host { backend: script };
    let (mut ours, theirs) = memory_pair();
    let server = tokio::spawn(async move { v2::serve(theirs, Caller::new("phone"), &host, &Admission::new(1)).await });
    let req = Request::messages(vec![]);
    let id = req.id.clone();
    ours.send_cbor(&ClientFrame::Infer(req)).await.unwrap();
    let mut frames = Vec::new();
    while let Some(f) = ours.recv_cbor::<ServerFrame>().await.unwrap() {
        let done = matches!(f, ServerFrame::Done { .. });
        frames.push(f);
        if done {
            break;
        }
    }
    server.await.unwrap().unwrap();
    let pick = |f: &ServerFrame| match f {
        ServerFrame::Delta { offset, text, .. } => Some(format!("d{offset}:{text}")),
        ServerFrame::Reasoning { offset, text, .. } => Some(format!("r{offset}:{text}")),
        ServerFrame::ToolCall { index, call, .. } => Some(format!("t{index}:{}", call.name)),
        _ => None,
    };
    let got: Vec<String> = frames.iter().filter_map(pick).collect();
    assert_eq!(got, ["d0:añb", "r0:r1", "d3:c", "r2:r2", "t0:a", "t1:b"]);
    assert!(matches!(&frames[0], ServerFrame::Accepted { id: i, attempt: 0, resume: false, .. } if *i == id));
    let ServerFrame::Done { finish, .. } = frames.last().unwrap() else { panic!() };
    assert!(finish.meta.timings.first_token_ms.is_some());
}

#[tokio::test(start_paused = true)]
async fn a_queued_request_waits_with_keepalives_longer_than_the_accept_timeout() {
    let dialer = MemoryDialer::new();
    // Holds the only slot for ~20 s, streaming a token a second.
    let mut steps = vec![Step::Text("first")];
    steps.extend((0..20).flat_map(|_| [Step::Sleep(1_000), Step::Text(".")]));
    let slow = Script::new("m", false, steps);
    let admission = Admission::with_queue(1, 4);
    serving(&dialer, "inference", slow, admission.clone(), None, "phone");
    let p = pool(dialer, &["inference"]);
    let first = {
        let p = p.clone();
        tokio::spawn(async move { p.infer(Request::messages(vec![]).with_priority(Priority::Batch), &NoEvents, &CancelToken::new()).await })
    };
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(admission.free(), 0);
    let (tx, mut rx) = events();
    // Waits ~20 s: four times the 5 s accept timeout, kept alive by Queued every 3 s.
    let second = p.infer(Request::messages(vec![]), &tx, &CancelToken::new()).await.unwrap();
    assert!(second.text.starts_with("first."), "{}", second.text);
    assert!(first.await.unwrap().unwrap().text.starts_with("first."));
    let queued = drain(&mut rx).into_iter().filter(|e| matches!(e, PoolEvent::Queued { position: 1, .. })).count();
    assert!(queued >= 6, "{queued} Queued frames");
    assert!(second.meta.timings.queued_ms >= 19_000, "{:?}", second.meta.timings);
}

#[tokio::test]
async fn a_full_queue_is_busy_and_the_next_node_takes_it() {
    let dialer = MemoryDialer::new();
    let full = Admission::new(1);
    let _held = full.try_take().unwrap();
    serving(&dialer, "full", Script::new("a", false, vec![Step::Text("no")]), full, None, "phone");
    serving(&dialer, "free", Script::new("b", false, vec![Step::Text("yes")]), Admission::new(1), None, "phone");
    let answer = pool(dialer, &["full", "free"]).infer(Request::messages(vec![]), &NoEvents, &CancelToken::new()).await.unwrap();
    assert_eq!((answer.text.as_str(), answer.node.as_str()), ("yes", "free"));
}

#[tokio::test]
async fn a_node_that_dies_mid_answer_is_resumed_on_the_next_without_repeating_text() {
    let dialer = MemoryDialer::new();
    let a = Script::new(
        "a",
        true,
        vec![Step::Reasoning("hm"), Step::Text("Hello "), Step::Text("there "), Step::Sleep(50), Step::Text("never"), Step::Hang],
    );
    let b = Script::new("b", true, vec![Step::Text("world")]);
    // Accepted, Reasoning, Delta, Delta, then the channel goes.
    serving(&dialer, "a", a.clone(), Admission::new(1), Some(4), "phone");
    serving(&dialer, "b", b.clone(), Admission::new(1), None, "phone");
    let (tx, mut rx) = events();
    let request = Request::messages(vec![Message::user("q")]).with_job_id("job-1").with_seed(42);
    let answer = pool(dialer, &["a", "b"]).infer(request, &tx, &CancelToken::new()).await.unwrap();
    assert_eq!(answer.text, "Hello there world");
    assert_eq!(answer.reasoning, "hm");
    assert_eq!(answer.node, "b");
    assert_eq!(answer.meta.resumes, [Resume::new(12, "b")]);
    assert_eq!(answer.meta.resumed_from, Some(12));
    let resumed = &b.seen()[0];
    assert_eq!((resumed.job_id.as_str(), resumed.attempt, resumed.seed), ("job-1", 1, Some(42)));
    assert_eq!(resumed.assistant_prefix, Some(Prefix::new("Hello there ", "hm")));
    assert_eq!(resumed.inline_messages(), Some(&[Message::user("q")][..]));
    let evs = drain(&mut rx);
    assert_eq!(shown(&evs), "Hello there world", "no text twice");
    assert!(evs.contains(&PoolEvent::Resumed { node: "b".into(), offset: 12 }));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(a.cancelled.load(Ordering::SeqCst), 1, "the dead node's run was cancelled");
}

#[tokio::test]
async fn resume_goes_only_to_nodes_that_say_so() {
    let dialer = MemoryDialer::new();
    serving(&dialer, "a", Script::new("a", true, vec![Step::Text("Hel"), Step::Hang]), Admission::new(1), Some(2), "phone");
    let old = Script::new("old", false, vec![Step::Text("Hello from scratch")]);
    serving(&dialer, "old", old.clone(), Admission::new(1), None, "phone");
    serving(&dialer, "c", Script::new("c", true, vec![Step::Text("lo")]), Admission::new(1), None, "phone");
    let answer = pool(dialer.clone(), &["a", "old", "c"]).infer(Request::messages(vec![]), &NoEvents, &CancelToken::new()).await.unwrap();
    assert_eq!((answer.text.as_str(), answer.node.as_str()), ("Hello", "c"));
    // "old" said resume: false, so the pool cancelled before it produced anything.
    assert!(old.seen().len() <= 1);

    // Nothing that resumes: the partial answer comes back.
    let err = pool(dialer, &["a", "old"]).infer(Request::messages(vec![]), &NoEvents, &CancelToken::new()).await.unwrap_err();
    assert_eq!(err.partial.text, "Hel");
    assert!(err.streamed());
    assert_eq!(err.tried.len(), 2);
}

#[tokio::test]
async fn resume_off_returns_the_partial_answer() {
    let dialer = MemoryDialer::new();
    serving(&dialer, "a", Script::new("a", true, vec![Step::Text("Hel"), Step::Hang]), Admission::new(1), Some(2), "phone");
    serving(&dialer, "b", Script::new("b", true, vec![Step::Text("lo")]), Admission::new(1), None, "phone");
    let opts = PoolOptions::default().with_resume(false);
    let p = Pool::new(Arc::new(dialer), vec!["a".to_string(), "b".into()]).with_options(opts);
    let err = p.infer(Request::messages(vec![]), &NoEvents, &CancelToken::new()).await.unwrap_err();
    assert_eq!((err.partial.text.as_str(), err.error.clone()), ("Hel", RemoteError::Closed));
}

#[tokio::test(start_paused = true)]
async fn a_stalled_stream_is_cancelled_and_resumed() {
    let dialer = MemoryDialer::new();
    let a = Script::new("a", true, vec![Step::Text("one "), Step::Hang]);
    serving(&dialer, "a", a.clone(), Admission::new(1), None, "phone");
    serving(&dialer, "b", Script::new("b", true, vec![Step::Text("two")]), Admission::new(1), None, "phone");
    let answer = pool(dialer, &["a", "b"]).infer(Request::messages(vec![]), &NoEvents, &CancelToken::new()).await.unwrap();
    assert_eq!(answer.text, "one two");
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(a.cancelled.load(Ordering::SeqCst), 1, "the stalled node got Cancel");
}

#[tokio::test]
async fn at_most_max_resumes_per_answer() {
    let dialer = MemoryDialer::new();
    for (n, t) in [("a", "1"), ("b", "2"), ("c", "3"), ("d", "4")] {
        let steps = vec![Step::Text(t), Step::Hang];
        serving(&dialer, n, Script::new("m", true, steps), Admission::new(1), Some(2), "phone");
    }
    let err = pool(dialer, &["a", "b", "c", "d"]).infer(Request::messages(vec![]), &NoEvents, &CancelToken::new()).await.unwrap_err();
    assert_eq!(err.partial.text, "123");
    assert_eq!(err.tried.len(), 3, "d was never asked");
}

/// A hand-written server that repeats and skips text.
fn odd_server(dialer: &MemoryDialer, node: &str, frames: Vec<(u64, &'static str)>) {
    let frames = Arc::new(frames);
    dialer.route(node, move |mut ch, _| {
        let frames = frames.clone();
        tokio::spawn(async move {
            let Ok(Some(ClientFrame::Infer(r))) = ch.recv_cbor::<ClientFrame>().await else { return };
            let id = r.id;
            let _ = ch.send_cbor(&ServerFrame::Accepted { id: id.clone(), model: "m".into(), attempt: 0, resume: false }).await;
            for (offset, text) in frames.iter() {
                let _ = ch.send_cbor(&ServerFrame::Delta { id: id.clone(), offset: *offset, text: (*text).into() }).await;
            }
            let _ = ch.send_cbor(&ServerFrame::Delta { id: "old-attempt".into(), offset: 0, text: "stale".into() }).await;
            let _ = ch.send_cbor(&ServerFrame::Done { id, finish: Finish::new("stop") }).await;
        });
    });
}

#[tokio::test]
async fn overlapping_frames_are_trimmed_and_a_gap_fails_the_attempt() {
    let dialer = MemoryDialer::new();
    odd_server(&dialer, "dup", vec![(0, "abc"), (0, "abc"), (2, "cde"), (5, "f")]);
    odd_server(&dialer, "gap", vec![(0, "ab"), (5, "x")]);
    let (tx, mut rx) = events();
    let answer = pool(dialer.clone(), &["dup"]).infer(Request::messages(vec![]), &tx, &CancelToken::new()).await.unwrap();
    assert_eq!(answer.text, "abcdef");
    assert_eq!(shown(&drain(&mut rx)), "abcdef");
    let err = pool(dialer, &["gap"]).infer(Request::messages(vec![]), &NoEvents, &CancelToken::new()).await.unwrap_err();
    assert!(matches!(err.error, RemoteError::Protocol { ref message } if message.contains("missing")), "{:?}", err.error);
    assert_eq!(err.partial.text, "ab");
}

#[tokio::test]
async fn cancel_reaches_the_node() {
    let dialer = MemoryDialer::new();
    let s = Script::new("m", true, vec![Step::Text("x"), Step::Hang]);
    serving(&dialer, "a", s.clone(), Admission::new(1), None, "phone");
    let p = pool(dialer, &["a"]);
    let cancel = CancelToken::new();
    let (tx, mut rx) = events();
    let task = {
        let c = cancel.clone();
        tokio::spawn(async move { p.infer(Request::messages(vec![]), &tx, &c).await })
    };
    while !matches!(rx.recv().await, Some(PoolEvent::Text(_))) {}
    cancel.cancel();
    let err = task.await.unwrap().unwrap_err();
    assert_eq!((err.error, err.partial.text.as_str()), (RemoteError::Cancelled, "x"));
    for _ in 0..100 {
        if s.cancelled.load(Ordering::SeqCst) == 1 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("the node never saw the cancel");
}

#[tokio::test]
async fn strangers_bad_requests_and_inconsistent_prefixes_are_refused() {
    let dialer = MemoryDialer::new();
    serving(&dialer, "for-stranger", Script::new("m", true, vec![Step::Text("x")]), Admission::new(1), None, "stranger");
    serving(&dialer, "a", Script::new("m", true, vec![Step::Text("x")]), Admission::new(1), None, "phone");
    let err = pool(dialer.clone(), &["for-stranger"]).infer(Request::messages(vec![]), &NoEvents, &CancelToken::new()).await.unwrap_err();
    assert_eq!(err.error, RemoteError::refused("unknown-device"));

    let raw = Request::context(ContextRef::new("conv-1", None)).raw();
    let err = pool(dialer.clone(), &["a"]).infer(raw, &NoEvents, &CancelToken::new()).await.unwrap_err();
    assert!(matches!(err.error, RemoteError::BadRequest { .. }), "{:?}", err.error);

    let mut bad = Prefix::new("abc", "");
    bad.len = 2;
    let err = pool(dialer.clone(), &["a"]).infer(Request::messages(vec![]).with_prefix(bad), &NoEvents, &CancelToken::new()).await.unwrap_err();
    assert!(matches!(err.error, RemoteError::BadRequest { .. }), "{:?}", err.error);

    // A context_ref on a node with no conversations (the ModelBackend adapter) is unavailable.
    let host = Arc::new(super::tests_support::ModelHost(Arc::new(ScriptedModel::new(vec![]))));
    dialer.route("plain", move |ch, _| {
        let host = host.clone();
        tokio::spawn(async move { v2::serve(ch, Caller::new("phone"), host.as_ref(), &Admission::new(1)).await });
    });
    let err = pool(dialer, &["plain"]).infer(Request::context(ContextRef::new("c", None)), &NoEvents, &CancelToken::new()).await.unwrap_err();
    assert!(matches!(err.error, RemoteError::Unavailable { .. }), "{:?}", err.error);
}

#[tokio::test]
async fn a_model_backend_serves_infer2_with_reasoning_and_tool_calls() {
    struct Thinker;
    #[async_trait]
    impl ModelBackend for Thinker {
        fn name(&self) -> String {
            "thinker".into()
        }
        async fn complete(&self, _m: &[Message], tools: &[ToolSpec], deltas: &DeltaSink, _c: &CancelToken) -> Result<ModelTurn, ModelError> {
            deltas.reasoning("pondering");
            deltas.text("call it");
            let calls = tools.iter().map(|t| ToolCall { id: "1".into(), name: t.name.clone(), arguments: json!({}) }).collect();
            Ok(ModelTurn { text: "call it".into(), tool_calls: calls })
        }
    }
    let dialer = MemoryDialer::new();
    let host = Arc::new(super::tests_support::ModelHost(Arc::new(Thinker)));
    dialer.route("plain", move |ch, protocol| {
        let host = host.clone();
        tokio::spawn(async move {
            if protocol == v2::PROTOCOL {
                let _ = v2::serve(ch, Caller::new("phone"), host.as_ref(), &Admission::new(1)).await;
            }
        });
    });
    let req = Request::messages(vec![]).with_tools(vec![ToolSpec::new("search", "find")]);
    let a = pool(dialer, &["plain"]).infer(req, &NoEvents, &CancelToken::new()).await.unwrap();
    assert_eq!((a.text.as_str(), a.reasoning.as_str(), a.finish_reason.as_deref()), ("call it", "pondering", Some("tool_calls")));
    assert_eq!(a.tool_calls[0].name, "search");
}

#[tokio::test]
async fn embeddings_come_back_as_f32_in_order() {
    let dialer = MemoryDialer::new();
    serving(&dialer, "a", Script::new("m", false, vec![]), Admission::new(1), None, "phone");
    let e = pool(dialer, &["a"]).embed(EmbedRequest::new(vec!["a".into(), "abcd".into()]), &CancelToken::new()).await.unwrap();
    assert_eq!(e.vectors, [vec![1.0, 0.5, -1.25], vec![4.0, 0.5, -1.25]]);
    assert_eq!(e.model, "embedder");
}

#[tokio::test]
async fn jobs_run_in_the_background() {
    let dialer = MemoryDialer::new();
    let s = Script::new("m", false, vec![Step::Text("batch "), Step::Text("done")]);
    serving(&dialer, "a", s.clone(), Admission::new(1), None, "phone");
    let mut job = pool(dialer, &["a"]).submit(Request::messages(vec![]).with_priority(Priority::Batch).with_job_id("nightly-1"));
    assert_eq!(job.job_id(), "nightly-1");
    let mut text = String::new();
    while let Some(ev) = job.events().recv().await {
        if let PoolEvent::Text(t) = ev {
            text.push_str(&t);
        }
    }
    assert_eq!(text, "batch done");
    assert_eq!(job.wait().await.unwrap().text, "batch done");
    assert_eq!(s.seen()[0].priority, Priority::Batch);
}

#[tokio::test]
async fn remote_model_uses_infer2_and_falls_back_to_infer1_on_older_nodes() {
    let dialer = MemoryDialer::new();
    let host =
        Arc::new(super::tests_support::ModelHost(Arc::new(ScriptedModel::new(vec![ModelTurn { text: "from 0.3".into(), tool_calls: vec![] }]))));
    dialer.route_only("old", &[V1], move |ch, _| {
        let host = host.clone();
        tokio::spawn(async move { serve_v1(ch, &Caller::new("phone"), host.as_ref(), &Admission::new(1)).await });
    });
    serving(&dialer, "new", Script::new("v2", false, vec![Step::Text("from 0.4")]), Admission::new(1), None, "phone");
    let dialer = Arc::new(dialer);
    let (deltas, _rx) = (DeltaSink::discard(), ());
    let old = RemoteModel::new(dialer.clone(), vec!["old".into()]);
    assert_eq!(old.complete(&[], &[], &deltas, &CancelToken::new()).await.unwrap().text, "from 0.3");
    assert_eq!(old.last_route(), Some(Route::Node { node: "old".into(), model: "scripted".into() }));
    let new = RemoteModel::new(dialer, vec!["new".into()]);
    assert_eq!(new.complete(&[], &[], &deltas, &CancelToken::new()).await.unwrap().text, "from 0.4");
}

#[tokio::test]
async fn remote_model_resume_is_opt_in() {
    let dialer = MemoryDialer::new();
    serving(&dialer, "a", Script::new("a", true, vec![Step::Text("Hel"), Step::Hang]), Admission::new(1), Some(2), "phone");
    serving(&dialer, "b", Script::new("b", true, vec![Step::Text("lo")]), Admission::new(1), None, "phone");
    let dialer = Arc::new(dialer);
    let nodes = vec!["a".to_string(), "b".into()];
    let err = RemoteModel::new(dialer.clone(), nodes.clone()).complete(&[], &[], &DeltaSink::discard(), &CancelToken::new()).await.unwrap_err();
    assert_eq!(err, ModelError::Failed("a: connection closed".into()));
    let turn = RemoteModel::new(dialer, nodes).with_resume(true).complete(&[], &[], &DeltaSink::discard(), &CancelToken::new()).await.unwrap();
    assert_eq!(turn.text, "Hello");
}

#[test]
fn wire_is_cbor_with_byte_string_vectors() {
    let v = [1.5f32, -2.0];
    let f = ServerFrame::Embedding { id: "e".into(), index: 0, vector: f32_bytes(&v) };
    let b = to_cbor(&f).unwrap();
    // bytes(8): 0x48 then the little-endian floats.
    let mut want = vec![0x48];
    want.extend(f32_bytes(&v));
    assert!(b.windows(9).any(|w| w == want.as_slice()), "{b:x?}");
    let back: ServerFrame = from_cbor(&b).unwrap();
    let ServerFrame::Embedding { vector, .. } = &back else { panic!() };
    assert_eq!(bytes_f32(vector).unwrap(), v);
    assert!(bytes_f32(&[0, 1, 2]).is_err());

    let req = Request::messages(vec![Message::user("q")]).with_prefix(Prefix::new("ab", "")).with_cert(vec![9, 9]);
    let m = ClientFrame::Infer(req);
    assert_eq!(from_cbor::<ClientFrame>(&to_cbor(&m).unwrap()).unwrap(), m);
    let e = ServerFrame::Failed { id: "x".into(), error: RemoteError::Stalled { ms: 15_000 } };
    assert_eq!(from_cbor::<ServerFrame>(&to_cbor(&e).unwrap()).unwrap(), e);
}

#[tokio::test]
async fn infer1_keeps_its_json_and_old_error_kinds() {
    let host = super::tests_support::ModelHost(Arc::new(ScriptedModel::new(vec![])));
    let (mut ours, theirs) = memory_pair();
    let server = tokio::spawn(async move { serve_v1(theirs, &Caller::new("stranger"), &host, &Admission::new(1)).await });
    ours.send(&ClientMsg::Infer { request: InferRequest { id: "1".into(), model: None, messages: vec![], tools: vec![] } }).await.unwrap();
    let raw = ours.rx.recv().await.unwrap().unwrap();
    assert_eq!(String::from_utf8(raw).unwrap(), r#"{"msg":"failed","id":"1","error":{"kind":"refused","message":"unknown-device"}}"#);
    server.await.unwrap().unwrap();
    assert_eq!(RemoteError::Stalled { ms: 1 }.for_v1(), RemoteError::Failed { message: "stalled: no frame for 1 ms".into() });
}

#[tokio::test]
async fn raw_prompts_reach_backends_and_model_adapters_refuse_them() {
    let dialer = MemoryDialer::new();
    let script = Script::new("base", false, vec![Step::Text(" there was")]);
    serving(&dialer, "a", script.clone(), Admission::new(1), None, "phone");
    let answer = pool(dialer.clone(), &["a"]).infer(Request::prompt("Once upon a time"), &NoEvents, &CancelToken::new()).await.unwrap();
    assert_eq!(answer.text, " there was");
    let seen = &script.seen()[0];
    assert_eq!((seen.raw_prompt(), seen.inline_messages()), (Some("Once upon a time"), None));

    let with_tools = Request::prompt("x").with_tools(vec![ToolSpec::new("f", "a tool")]);
    let err = pool(dialer.clone(), &["a"]).infer(with_tools, &NoEvents, &CancelToken::new()).await.unwrap_err();
    assert!(matches!(err.error, RemoteError::BadRequest { .. }), "{:?}", err.error);

    let host = Arc::new(super::tests_support::ModelHost(Arc::new(ScriptedModel::new(vec![]))));
    dialer.route("plain", move |ch, _| {
        let host = host.clone();
        tokio::spawn(async move { v2::serve(ch, Caller::new("phone"), host.as_ref(), &Admission::new(1)).await });
    });
    let err = pool(dialer, &["plain"]).infer(Request::prompt("x"), &NoEvents, &CancelToken::new()).await.unwrap_err();
    assert!(matches!(err.error, RemoteError::Unsupported { .. }), "{:?}", err.error);

    let m = ClientFrame::Infer(Request::prompt("p"));
    assert_eq!(from_cbor::<ClientFrame>(&to_cbor(&m).unwrap()).unwrap(), m);
}

#[tokio::test]
async fn early_meta_reaches_the_pool_before_the_text() {
    let dialer = MemoryDialer::new();
    serving(&dialer, "a", Script::new("m", false, vec![Step::CacheHit, Step::Text("cached")]), Admission::new(1), None, "phone");
    let (tx, mut rx) = events();
    let answer = pool(dialer, &["a"]).infer(Request::messages(vec![Message::user("q")]), &tx, &CancelToken::new()).await.unwrap();
    assert!(answer.meta.cache_hit);
    let evs = drain(&mut rx);
    let meta_at = evs.iter().position(|e| matches!(e, PoolEvent::Meta(m) if m.cache_hit)).expect("a Meta event");
    let text_at = evs.iter().position(|e| matches!(e, PoolEvent::Text(_))).unwrap();
    assert!(meta_at < text_at, "{evs:?}");
}

#[tokio::test]
async fn askers_that_do_not_read_meta_get_it_only_in_done() {
    let host = Host { backend: Script::new("m", false, vec![Step::CacheHit, Step::Text("cached")]) };
    let (mut ours, theirs) = memory_pair();
    let server = tokio::spawn(async move { v2::serve(theirs, Caller::new("phone"), &host, &Admission::new(1)).await });
    let req = Request::messages(vec![]);
    assert!(!to_cbor(&req).unwrap().windows(10).any(|w| w == b"early_meta"), "the flag is not on the wire when unset");
    ours.send_cbor(&ClientFrame::Infer(req)).await.unwrap();
    let mut frames = Vec::new();
    while let Some(f) = ours.recv_cbor::<ServerFrame>().await.unwrap() {
        let done = matches!(f, ServerFrame::Done { .. });
        frames.push(f);
        if done {
            break;
        }
    }
    server.await.unwrap().unwrap();
    assert!(!frames.iter().any(|f| matches!(f, ServerFrame::Meta { .. })), "{frames:?}");
    let ServerFrame::Done { finish, .. } = frames.last().unwrap() else { panic!() };
    assert!(finish.meta.cache_hit);
}

#[tokio::test]
async fn embedding_token_counts_flow_to_the_pool() {
    struct Counting;
    #[async_trait]
    impl EmbedBackend for Counting {
        fn name(&self) -> String {
            "counting".into()
        }
        async fn embed(&self, _input: &str, _cancel: &CancelToken) -> Result<Vec<f32>, RemoteError> {
            Ok(vec![1.0])
        }
        async fn embed_counted(&self, input: &str, cancel: &CancelToken) -> Result<(Vec<f32>, Option<u64>), RemoteError> {
            Ok((self.embed(input, cancel).await?, Some(input.split_whitespace().count() as u64)))
        }
    }
    struct CountingHost;
    impl InferHost for CountingHost {
        fn authorize(&self, _caller: &Caller) -> Result<(), RemoteError> {
            Ok(())
        }
        fn open(&self, _caller: &Caller, _model: Option<&str>) -> Result<Arc<dyn ModelBackend>, RemoteError> {
            Err(RemoteError::unavailable("embeddings only"))
        }
        fn open_embed(&self, _caller: &Caller, _model: Option<&str>) -> Result<Arc<dyn EmbedBackend>, RemoteError> {
            Ok(Arc::new(Counting))
        }
    }
    let dialer = MemoryDialer::new();
    dialer.route("counting", |ch, _| {
        tokio::spawn(async move { v2::serve(ch, Caller::new("phone"), &CountingHost, &Admission::new(1)).await });
    });
    serving(&dialer, "plain", Script::new("m", false, vec![]), Admission::new(1), None, "phone");
    let e = pool(dialer.clone(), &["counting"]).embed(EmbedRequest::new(vec!["a b".into(), "c d e".into()]), &CancelToken::new()).await.unwrap();
    assert_eq!(e.usage, Some(Usage::new(5, 0)));
    let e = pool(dialer, &["plain"]).embed(EmbedRequest::new(vec!["a".into()]), &CancelToken::new()).await.unwrap();
    assert_eq!(e.usage, None, "a backend that does not count reports nothing");
}

/// Refuses every request with `error` and counts how often it was asked.
struct Refusing {
    error: RemoteError,
    asked: Arc<AtomicUsize>,
}

impl InferHost for Refusing {
    fn authorize(&self, _caller: &Caller) -> Result<(), RemoteError> {
        Ok(())
    }
    fn open(&self, _caller: &Caller, _model: Option<&str>) -> Result<Arc<dyn ModelBackend>, RemoteError> {
        Err(RemoteError::unavailable("infer/2 only"))
    }
    fn open_request(&self, _caller: &Caller, _request: &Request) -> Result<Arc<dyn InferBackend>, RemoteError> {
        self.asked.fetch_add(1, Ordering::SeqCst);
        Err(self.error.clone())
    }
    fn open_embed(&self, _caller: &Caller, _model: Option<&str>) -> Result<Arc<dyn EmbedBackend>, RemoteError> {
        self.asked.fetch_add(1, Ordering::SeqCst);
        Err(self.error.clone())
    }
}

fn refusing(dialer: &MemoryDialer, node: &str, error: RemoteError) -> Arc<AtomicUsize> {
    let asked = Arc::new(AtomicUsize::new(0));
    let host = Arc::new(Refusing { error, asked: asked.clone() });
    dialer.route_only(node, &[v2::PROTOCOL], move |ch, _| {
        let host = host.clone();
        tokio::spawn(async move { v2::serve(ch, Caller::new("phone"), host.as_ref(), &Admission::new(1)).await });
    });
    asked
}

#[tokio::test]
async fn bad_requests_are_returned_intact_without_failover() {
    let dialer = MemoryDialer::new();
    let bad = RemoteError::BadRequest { message: "no model named and no default".into() };
    let first = refusing(&dialer, "a", bad.clone());
    let second = refusing(&dialer, "b", bad.clone());
    let p = pool(dialer, &["a", "b"]);

    let err = p.embed(EmbedRequest::new(vec!["x".into()]), &CancelToken::new()).await.unwrap_err();
    assert_eq!(err, bad);
    let err = p.infer(Request::messages(vec![]), &NoEvents, &CancelToken::new()).await.unwrap_err();
    assert_eq!(err.error, bad);
    assert_eq!(err.tried.len(), 1);
    assert_eq!((first.load(Ordering::SeqCst), second.load(Ordering::SeqCst)), (2, 0), "the second node is never asked");
}

#[tokio::test]
async fn node_errors_fail_over_and_the_node_answer_is_reported_over_transport_errors() {
    let dialer = MemoryDialer::new();
    let unknown = RemoteError::unavailable("unknown model `x`");
    refusing(&dialer, "a", unknown.clone());
    // "gone" is not routed at all: a transport error, tried last.
    let p = pool(dialer.clone(), &["a", "gone"]);
    let err = p.embed(EmbedRequest::new(vec!["x".into()]).with_model("x"), &CancelToken::new()).await.unwrap_err();
    assert_eq!(err, unknown, "kind and message intact, not folded into one Unavailable");
    let err = p.infer(Request::messages(vec![]).with_model("x"), &NoEvents, &CancelToken::new()).await.unwrap_err();
    assert_eq!((err.error, err.tried.len()), (unknown, 2));

    // A node that does not serve it, then one that does: failover still works.
    refusing(&dialer, "old", RemoteError::Unsupported { message: "no embeddings here".into() });
    let e = pool(dialer.clone(), &["old", "plain"]);
    serving(&dialer, "plain", Script::new("m", false, vec![Step::Text("ok")]), Admission::new(1), None, "phone");
    assert_eq!(e.embed(EmbedRequest::new(vec!["x".into()]), &CancelToken::new()).await.unwrap().node, "plain");
    let only_old = pool(dialer, &["old"]);
    let err = only_old.embed(EmbedRequest::new(vec!["x".into()]), &CancelToken::new()).await.unwrap_err();
    assert!(matches!(err, RemoteError::Unsupported { .. }), "{err:?}");
}

#[test]
fn request_errors_and_node_answers() {
    assert!(RemoteError::BadRequest { message: String::new() }.is_request_error());
    assert!(RemoteError::Cancelled.is_request_error());
    assert!(!RemoteError::Busy.is_request_error() && RemoteError::Busy.is_node_answer());
    assert!(!RemoteError::Timeout.is_node_answer() && !RemoteError::Closed.is_node_answer());
}

#[tokio::test]
async fn continue_starts_from_the_callers_prefix_and_skips_nodes_that_cannot() {
    let dialer = MemoryDialer::new();
    let old = Script::new("old", false, vec![Step::Text("Hello from scratch")]);
    serving(&dialer, "old", old.clone(), Admission::new(1), None, "phone");
    let b = Script::new("b", true, vec![Step::Text(" world")]);
    serving(&dialer, "b", b.clone(), Admission::new(1), None, "phone");
    let (tx, mut rx) = events();
    let request = Request::messages(vec![Message::user("q")]).with_prefix(Prefix::new("Hello", "hm"));
    // Resume off: a Continue is the request itself, not a resume, so it still moves on.
    let p = pool(dialer.clone(), &["old", "b"]);
    let p = p.clone().with_options(p.options().clone().with_resume(false));
    let answer = p.infer(request, &tx, &CancelToken::new()).await.unwrap();
    assert_eq!((answer.text.as_str(), answer.reasoning.as_str(), answer.node.as_str()), ("Hello world", "hm", "b"));
    assert_eq!(answer.continuation(), " world");
    assert_eq!(answer.prefix, Prefix::new("Hello", "hm"));
    assert!(answer.meta.resumes.is_empty(), "a continue is not a resume");
    assert_eq!(b.seen()[0].assistant_prefix, Some(Prefix::new("Hello", "hm")));
    assert_eq!(shown(&drain(&mut rx)), " world", "events carry only the new text");

    // Nothing that can continue: nothing streamed, the prefix comes back as it was.
    let err =
        pool(dialer, &["old"]).infer(Request::messages(vec![]).with_prefix(Prefix::new("Hi", "")), &NoEvents, &CancelToken::new()).await.unwrap_err();
    assert!(!err.streamed());
    assert_eq!(err.partial.text, "Hi");
    assert!(matches!(err.error, RemoteError::Unsupported { .. }), "{:?}", err.error);
}

#[tokio::test]
async fn a_continued_answer_cut_off_mid_way_resumes_with_the_whole_prefix() {
    let dialer = MemoryDialer::new();
    let a = Script::new("a", true, vec![Step::Text(" upon"), Step::Sleep(50), Step::Text("never"), Step::Hang]);
    let b = Script::new("b", true, vec![Step::Text(" a time")]);
    // Accepted, Delta, then the channel goes.
    serving(&dialer, "a", a, Admission::new(1), Some(2), "phone");
    serving(&dialer, "b", b.clone(), Admission::new(1), None, "phone");
    let request = Request::prompt("Tell me a story.").with_prefix(Prefix::new("Once", ""));
    let answer = pool(dialer, &["a", "b"]).infer(request, &NoEvents, &CancelToken::new()).await.unwrap();
    assert_eq!(answer.text, "Once upon a time");
    assert_eq!(answer.continuation(), " upon a time");
    assert_eq!(answer.meta.resumes, [Resume::new(9, "b")]);
    assert_eq!(b.seen()[0].assistant_prefix, Some(Prefix::new("Once upon", "")));
}

#[test]
fn continuing_moves_a_trailing_assistant_message_into_the_prefix() {
    let r = Request::messages(vec![Message::user("q"), Message::assistant("Once upon", vec![])]).continuing();
    assert_eq!(r.inline_messages().unwrap(), [Message::user("q")]);
    assert_eq!(r.assistant_prefix, Some(Prefix::new("Once upon", "")));

    let call = ToolCall { id: "c".into(), name: "t".into(), arguments: json!({}) };
    let with_call = Request::messages(vec![Message::user("q"), Message::assistant("", vec![call])]);
    assert_eq!(with_call.clone().continuing(), with_call);
    let ends_with_user = Request::messages(vec![Message::user("q")]);
    assert_eq!(ends_with_user.clone().continuing(), ends_with_user);
}

const PNG: &str = "data:image/png;base64,iVBORw0KGgo=";

fn image_request() -> Request {
    Request::messages(vec![Message::user(vec![ContentPart::text("what is this?"), ContentPart::image(PNG)])])
}

/// An inference node serving infer/2 and a catalog/1 with a text model and a vision model.
fn serving_with_catalog(dialer: &MemoryDialer, node: &str, backend: Arc<dyn InferBackend>) {
    let host = Arc::new(Host { backend });
    let feed = CatalogFeed::new(Arc::new(|_: &Caller| Ok(())));
    feed.publish(vec![
        CatalogModel::new("qwen", Purpose::Chat, ModelState::Warm),
        CatalogModel::new("gemma", Purpose::Chat, ModelState::Loadable).with_inputs(Modalities::new(true, true)),
    ]);
    let admission = Admission::new(1);
    dialer.route(node, move |ch, protocol| {
        let (host, admission, feed) = (host.clone(), admission.clone(), feed.clone());
        tokio::spawn(async move {
            if protocol == catalog::PROTOCOL {
                let _ = catalog::serve(ch, Caller::new("phone"), &feed).await;
            } else if protocol == v2::PROTOCOL {
                let _ = v2::serve(ch, Caller::new("phone"), host.as_ref(), &admission).await;
            }
        });
    });
}

#[tokio::test]
async fn image_requests_go_to_a_model_that_takes_images() {
    let dialer = MemoryDialer::new();
    let script = Script::new("gemma@node-b", false, vec![Step::Text("a cat")]);
    serving_with_catalog(&dialer, "inference", script.clone());
    let answer = pool(dialer, &["inference"]).infer(image_request(), &NoEvents, &CancelToken::new()).await.unwrap();
    assert_eq!(answer.text, "a cat");
    let seen = script.seen();
    assert_eq!(seen[0].model.as_deref(), Some("gemma"), "picked from the catalog, not the text-only default");
    let parts = seen[0].inline_messages().unwrap()[0].content.parts();
    assert_eq!(parts, vec![ContentPart::text("what is this?"), ContentPart::image(PNG)], "parts arrive unchanged");
}

#[tokio::test]
async fn image_requests_never_reach_a_text_only_model() {
    let dialer = MemoryDialer::new();
    let script = Script::new("qwen@node-b", false, vec![Step::Text("no")]);
    serving_with_catalog(&dialer, "inference", script.clone());
    let p = pool(dialer, &["inference"]);
    let e = p.infer(image_request().with_model("qwen"), &NoEvents, &CancelToken::new()).await.unwrap_err();
    assert!(matches!(&e.error, RemoteError::BadRequest { message } if message.contains("`qwen` does not take images")), "{:?}", e.error);
    let remote = Request::messages(vec![Message::user(vec![ContentPart::image("https://example.com/cat.png")])]);
    let e = p.infer(remote, &NoEvents, &CancelToken::new()).await.unwrap_err();
    assert!(matches!(&e.error, RemoteError::BadRequest { message } if message.contains("data:image")), "{:?}", e.error);
    assert!(script.seen().is_empty());
    // Text still goes to whatever the node picks.
    p.infer(Request::messages(vec![Message::user("hi")]), &NoEvents, &CancelToken::new()).await.unwrap();
    assert_eq!(script.seen()[0].model, None);
}

#[tokio::test]
async fn without_a_catalog_the_node_decides_and_infer1_is_skipped() {
    let dialer = MemoryDialer::new();
    let script = Script::new("gemma@node-b", false, vec![Step::Text("a cat")]);
    serving(&dialer, "inference", script.clone(), Admission::new(1), None, "phone");
    let v1_hits = Arc::new(AtomicUsize::new(0));
    let hits = v1_hits.clone();
    dialer.route_only("old", &[V1], move |_ch, _| {
        hits.fetch_add(1, Ordering::SeqCst);
    });
    let answer = pool(dialer, &["old", "inference"]).infer(image_request(), &NoEvents, &CancelToken::new()).await.unwrap();
    assert_eq!(answer.text, "a cat");
    assert_eq!(script.seen()[0].model, None);
    assert_eq!(v1_hits.load(Ordering::SeqCst), 0, "infer/1 nodes predate content parts");
}
