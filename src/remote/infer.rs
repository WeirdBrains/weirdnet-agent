//! Remote inference: one model turn on another of the user's nodes.
//!
//! ```text
//! asking node                      serving node
//!   Infer { id, model, messages, tools } ->
//!                                  <- Accepted { model }      (or Failed { Busy | Unavailable | .. })
//!                                  <- Delta { text } ...
//!   Cancel { id } ->               (optional, any time)
//!                                  <- Done { turn } | Failed { error } | Cancelled
//! ```
//!
//! [`RemoteModel`] is a [`ModelBackend`] over this protocol. It tries its nodes in order and
//! then its local fallback, moving on whenever a node fails before any token has streamed
//! (unreachable, busy, no model, an error, or no `Accepted` within the accept timeout). Once
//! tokens have streamed a failure is returned as is: the text already shown cannot be taken back.

use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

use super::{Channel, Dialer, RemoteError, request_id};
use crate::cancel::CancelToken;
use crate::model::{DeltaSink, Message, ModelBackend, ModelError, ModelTurn};
use crate::session::AgentEvent;
use crate::tool::ToolSpec;

/// Stream protocol id for remote inference.
pub const PROTOCOL: &str = "/weirdnet-agent/infer/1";

/// How long a node has to accept a turn before the next one is tried.
pub const DEFAULT_ACCEPT_TIMEOUT: Duration = Duration::from_secs(10);

/// One model turn to run elsewhere.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InferRequest {
    pub id: String,
    /// The model wanted, by the serving app's id (`None`: the node's default).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    pub messages: Vec<Message>,
    #[serde(default)]
    pub tools: Vec<ToolSpec>,
}

/// Asking node to serving node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "msg")]
pub enum ClientMsg {
    Infer { request: InferRequest },
    Cancel { id: String },
}

/// Serving node to asking node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "msg")]
pub enum ServerMsg {
    Accepted { id: String, model: String },
    Delta { id: String, text: String },
    Done { id: String, turn: ModelTurn },
    Failed { id: String, error: RemoteError },
    Cancelled { id: String },
}

/// What the serving app offers: a backend for a requested model.
pub trait InferHost: Send + Sync {
    fn open(&self, model: Option<&str>) -> Result<Arc<dyn ModelBackend>, RemoteError>;
}

/// How many turns a node serves at once; more are answered `Busy` so the asker moves on.
#[derive(Clone)]
pub struct Admission {
    slots: Arc<Semaphore>,
}

impl Admission {
    pub fn new(max_concurrent: usize) -> Self {
        Self { slots: Arc::new(Semaphore::new(max_concurrent)) }
    }

    fn try_take(&self) -> Option<OwnedSemaphorePermit> {
        self.slots.clone().try_acquire_owned().ok()
    }
}

fn model_error(e: ModelError) -> RemoteError {
    match e {
        ModelError::Unavailable(m) => RemoteError::Unavailable { message: m },
        ModelError::Failed(m) => RemoteError::Failed { message: m },
        ModelError::Cancelled => RemoteError::Failed { message: "cancelled".into() },
    }
}

/// Serve one inference request on an accepted channel. The caller has already checked that the
/// far end is a linked device. Returns when the turn is done, failed or cancelled; if the asker
/// goes away the turn is cancelled.
pub async fn serve(mut ch: Channel, host: &dyn InferHost, admission: &Admission) -> Result<(), RemoteError> {
    let request = match ch.recv::<ClientMsg>().await? {
        Some(ClientMsg::Infer { request }) => request,
        Some(ClientMsg::Cancel { .. }) | None => return Ok(()),
    };
    let id = request.id.clone();
    let Some(_permit) = admission.try_take() else {
        return ch.send(&ServerMsg::Failed { id, error: RemoteError::Busy }).await;
    };
    let backend = match host.open(request.model.as_deref()) {
        Ok(b) => b,
        Err(error) => return ch.send(&ServerMsg::Failed { id, error }).await,
    };
    ch.send(&ServerMsg::Accepted { id: id.clone(), model: backend.name() }).await?;

    let (ev_tx, mut ev_rx) = mpsc::unbounded_channel();
    let sink = DeltaSink::new(Some(ev_tx));
    let cancel = CancelToken::new();
    let mut run = pin!(backend.complete(&request.messages, &request.tools, &sink, &cancel));
    let mut asker_open = true;
    let mut send_failed = false;
    let result = loop {
        tokio::select! {
            r = &mut run => break r,
            Some(ev) = ev_rx.recv() => {
                if let AgentEvent::TextDelta(text) = ev
                    && !send_failed
                    && ch.send(&ServerMsg::Delta { id: id.clone(), text }).await.is_err()
                {
                    send_failed = true;
                    cancel.cancel();
                }
            }
            m = ch.rx.recv(), if asker_open => match m {
                Ok(Some(bytes)) => {
                    if let Ok(ClientMsg::Cancel { id: cid }) = serde_json::from_slice::<ClientMsg>(&bytes)
                        && cid == id
                    {
                        cancel.cancel();
                    }
                }
                Ok(None) | Err(_) => {
                    asker_open = false;
                    cancel.cancel();
                }
            },
        }
    };
    if send_failed || !asker_open {
        return Ok(());
    }
    while let Ok(ev) = ev_rx.try_recv() {
        if let AgentEvent::TextDelta(text) = ev {
            ch.send(&ServerMsg::Delta { id: id.clone(), text }).await?;
        }
    }
    let reply = match result {
        Ok(turn) => ServerMsg::Done { id, turn },
        Err(_) if cancel.is_cancelled() => ServerMsg::Cancelled { id },
        Err(e) => ServerMsg::Failed { id, error: model_error(e) },
    };
    ch.send(&reply).await
}

/// Where the last turn of a [`RemoteModel`] ran.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Route {
    /// On this node, by the model name it reported.
    Node { node: String, model: String },
    /// On the local fallback.
    Fallback,
}

enum Attempt {
    Cancelled,
    /// Failed before any token streamed: try the next node.
    Before(RemoteError),
    /// Failed after tokens streamed.
    Streamed(RemoteError),
}

/// A [`ModelBackend`] that runs turns on the user's other nodes, with fallback.
pub struct RemoteModel {
    dialer: Arc<dyn Dialer>,
    nodes: Vec<String>,
    model: Option<String>,
    fallback: Option<Arc<dyn ModelBackend>>,
    accept_timeout: Duration,
    last: Mutex<Option<Route>>,
}

impl RemoteModel {
    /// Tries `nodes` in order.
    pub fn new(dialer: Arc<dyn Dialer>, nodes: Vec<String>) -> Self {
        Self { dialer, nodes, model: None, fallback: None, accept_timeout: DEFAULT_ACCEPT_TIMEOUT, last: Mutex::new(None) }
    }

    /// Ask the nodes for this model id instead of their default.
    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = Some(model.into());
        self
    }

    /// Run here when no node can take the turn.
    pub fn with_fallback(mut self, fallback: Arc<dyn ModelBackend>) -> Self {
        self.fallback = Some(fallback);
        self
    }

    pub fn with_accept_timeout(mut self, timeout: Duration) -> Self {
        self.accept_timeout = timeout;
        self
    }

    pub fn nodes(&self) -> &[String] {
        &self.nodes
    }

    /// Where the last completed turn ran.
    pub fn last_route(&self) -> Option<Route> {
        self.last.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    fn set_last(&self, route: Route) {
        *self.last.lock().unwrap_or_else(|e| e.into_inner()) = Some(route);
    }

    async fn attempt(
        &self,
        node: &str,
        messages: &[Message],
        tools: &[ToolSpec],
        deltas: &DeltaSink,
        cancel: &CancelToken,
    ) -> Result<ModelTurn, Attempt> {
        let open = tokio::select! {
            r = tokio::time::timeout(self.accept_timeout, self.dialer.open(node, PROTOCOL)) => r,
            _ = cancel.cancelled() => return Err(Attempt::Cancelled),
        };
        let mut ch = match open {
            Ok(Ok(ch)) => ch,
            Ok(Err(e)) => return Err(Attempt::Before(e)),
            Err(_) => return Err(Attempt::Before(RemoteError::Timeout)),
        };
        let id = request_id();
        let request = InferRequest { id: id.clone(), model: self.model.clone(), messages: messages.to_vec(), tools: tools.to_vec() };
        if let Err(e) = ch.send(&ClientMsg::Infer { request }).await {
            return Err(Attempt::Before(e));
        }
        let mut accepted: Option<String> = None;
        let mut streamed = false;
        let fail = |streamed: bool, e: RemoteError| if streamed { Attempt::Streamed(e) } else { Attempt::Before(e) };
        loop {
            let next = tokio::select! {
                _ = cancel.cancelled() => {
                    let _ = ch.send(&ClientMsg::Cancel { id: id.clone() }).await;
                    return Err(Attempt::Cancelled);
                }
                m = async {
                    if accepted.is_some() {
                        Ok(ch.recv::<ServerMsg>().await)
                    } else {
                        tokio::time::timeout(self.accept_timeout, ch.recv::<ServerMsg>()).await
                    }
                } => m,
            };
            let msg = match next {
                Err(_) => return Err(fail(streamed, RemoteError::Timeout)),
                Ok(Err(e)) => return Err(fail(streamed, e)),
                Ok(Ok(None)) => return Err(fail(streamed, RemoteError::Closed)),
                Ok(Ok(Some(m))) => m,
            };
            match msg {
                ServerMsg::Accepted { id: rid, model } if rid == id => accepted = Some(model),
                ServerMsg::Delta { id: rid, text } if rid == id && accepted.is_some() => {
                    streamed = true;
                    deltas.text(text);
                }
                ServerMsg::Done { id: rid, turn } if rid == id && accepted.is_some() => {
                    self.set_last(Route::Node { node: node.to_string(), model: accepted.unwrap_or_default() });
                    return Ok(turn);
                }
                ServerMsg::Failed { id: rid, error } if rid == id => return Err(fail(streamed, error)),
                ServerMsg::Cancelled { id: rid } if rid == id => {
                    return Err(fail(streamed, RemoteError::Failed { message: "cancelled by the node".into() }));
                }
                other => return Err(fail(streamed, RemoteError::protocol(format!("unexpected message {other:?}")))),
            }
        }
    }
}

#[async_trait]
impl ModelBackend for RemoteModel {
    /// The model and node of the last turn (traces record it after each turn).
    fn name(&self) -> String {
        match self.last_route() {
            Some(Route::Node { node, model }) => format!("{model}@{node}"),
            Some(Route::Fallback) => self.fallback.as_ref().map(|f| f.name()).unwrap_or_default(),
            None => format!("{}@{}", self.model.as_deref().unwrap_or("model"), self.nodes.first().map(String::as_str).unwrap_or("remote")),
        }
    }

    async fn complete(&self, messages: &[Message], tools: &[ToolSpec], deltas: &DeltaSink, cancel: &CancelToken) -> Result<ModelTurn, ModelError> {
        let mut tried = Vec::new();
        for node in &self.nodes {
            if cancel.is_cancelled() {
                return Err(ModelError::Cancelled);
            }
            match self.attempt(node, messages, tools, deltas, cancel).await {
                Ok(turn) => return Ok(turn),
                Err(Attempt::Cancelled) => return Err(ModelError::Cancelled),
                Err(Attempt::Streamed(e)) => return Err(ModelError::Failed(format!("{node}: {e}"))),
                Err(Attempt::Before(e)) => tried.push(format!("{node}: {e}")),
            }
        }
        if let Some(fallback) = &self.fallback {
            let r = fallback.complete(messages, tools, deltas, cancel).await;
            if r.is_ok() {
                self.set_last(Route::Fallback);
            }
            return r;
        }
        if tried.is_empty() {
            return Err(ModelError::Unavailable("no node to run on".into()));
        }
        Err(ModelError::Unavailable(tried.join("; ")))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::remote::{MemoryDialer, memory_pair};
    use crate::testing::ScriptedModel;

    struct Host {
        model: Option<Arc<dyn ModelBackend>>,
    }

    impl InferHost for Host {
        fn open(&self, model: Option<&str>) -> Result<Arc<dyn ModelBackend>, RemoteError> {
            match (&self.model, model) {
                (_, Some(m)) if m != "gemma" => Err(RemoteError::Unavailable { message: format!("no {m} here") }),
                (Some(b), _) => Ok(b.clone()),
                (None, _) => Err(RemoteError::Unavailable { message: "no model".into() }),
            }
        }
    }

    fn turn(text: &str) -> ModelTurn {
        ModelTurn { text: text.into(), tool_calls: Vec::new() }
    }

    fn serving(dialer: &MemoryDialer, node: &str, model: Option<Arc<dyn ModelBackend>>, admission: Admission) {
        let host = Arc::new(Host { model });
        dialer.route(node, move |ch, protocol| {
            assert_eq!(protocol, PROTOCOL);
            let host = host.clone();
            let admission = admission.clone();
            tokio::spawn(async move {
                let _ = serve(ch, host.as_ref(), &admission).await;
            });
        });
    }

    fn sink() -> (DeltaSink, mpsc::UnboundedReceiver<AgentEvent>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (DeltaSink::new(Some(tx)), rx)
    }

    fn text_of(rx: &mut mpsc::UnboundedReceiver<AgentEvent>) -> String {
        let mut s = String::new();
        while let Ok(AgentEvent::TextDelta(t)) = rx.try_recv() {
            s.push_str(&t);
        }
        s
    }

    #[tokio::test]
    async fn a_turn_runs_on_the_node_and_streams_its_tokens() {
        let dialer = MemoryDialer::new();
        let scripted = Arc::new(ScriptedModel::new(vec![turn("hello from the desktop")]));
        serving(&dialer, "desktop", Some(scripted.clone()), Admission::new(1));
        let remote = RemoteModel::new(Arc::new(dialer), vec!["desktop".into()]).with_model("gemma");
        let (deltas, mut rx) = sink();
        let got = remote.complete(&[Message::user("hi")], &[], &deltas, &CancelToken::new()).await.unwrap();
        assert_eq!(got.text, "hello from the desktop");
        assert_eq!(text_of(&mut rx), "hello from the desktop");
        assert_eq!(scripted.seen(), [vec![Message::user("hi")]], "the node saw the messages");
        assert_eq!(remote.last_route(), Some(Route::Node { node: "desktop".into(), model: "scripted".into() }));
        assert_eq!(remote.name(), "scripted@desktop");
    }

    #[tokio::test]
    async fn unreachable_busy_and_modelless_nodes_fall_through_to_the_next_then_local() {
        let dialer = MemoryDialer::new();
        // "busy" has one slot and it is held.
        let busy = Admission::new(1);
        let _held = busy.try_take().unwrap();
        serving(&dialer, "busy", Some(Arc::new(ScriptedModel::new(vec![turn("no")]))), busy);
        serving(&dialer, "empty", None, Admission::new(1));
        serving(&dialer, "good", Some(Arc::new(ScriptedModel::new(vec![turn("from good")]))), Admission::new(1));
        let dialer = Arc::new(dialer);
        let nodes = vec!["gone".to_string(), "busy".into(), "empty".into(), "good".into()];
        let remote = RemoteModel::new(dialer.clone(), nodes);
        let got = remote.complete(&[Message::user("q")], &[], &DeltaSink::discard(), &CancelToken::new()).await.unwrap();
        assert_eq!(got.text, "from good");

        // No node can take it: the local fallback runs.
        let remote = RemoteModel::new(dialer.clone(), vec!["gone".into(), "empty".into()])
            .with_fallback(Arc::new(ScriptedModel::new(vec![turn("local answer")])));
        let (deltas, mut rx) = sink();
        let got = remote.complete(&[Message::user("q")], &[], &deltas, &CancelToken::new()).await.unwrap();
        assert_eq!(got.text, "local answer");
        assert_eq!(text_of(&mut rx), "local answer");
        assert_eq!(remote.last_route(), Some(Route::Fallback));
        assert_eq!(remote.name(), "scripted");

        // No fallback: the error names every node tried.
        let remote = RemoteModel::new(dialer, vec!["gone".into(), "empty".into()]);
        let err = remote.complete(&[Message::user("q")], &[], &DeltaSink::discard(), &CancelToken::new()).await.unwrap_err();
        let ModelError::Unavailable(why) = err else { panic!("{err:?}") };
        assert!(why.contains("gone: node unreachable") && why.contains("empty: model unavailable"), "{why}");
    }

    #[tokio::test]
    async fn a_wrong_model_id_is_unavailable_and_falls_back() {
        let dialer = MemoryDialer::new();
        serving(&dialer, "desktop", Some(Arc::new(ScriptedModel::new(vec![turn("x")]))), Admission::new(1));
        let remote = RemoteModel::new(Arc::new(dialer), vec!["desktop".into()])
            .with_model("llama-9000")
            .with_fallback(Arc::new(ScriptedModel::new(vec![turn("phone model")])));
        let got = remote.complete(&[], &[], &DeltaSink::discard(), &CancelToken::new()).await.unwrap();
        assert_eq!(got.text, "phone model");
    }

    /// A model that streams one token, then waits until cancelled.
    struct Slow {
        cancelled: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl ModelBackend for Slow {
        fn name(&self) -> String {
            "slow".into()
        }
        async fn complete(&self, _m: &[Message], _t: &[ToolSpec], deltas: &DeltaSink, cancel: &CancelToken) -> Result<ModelTurn, ModelError> {
            deltas.text("thinking");
            cancel.cancelled().await;
            self.cancelled.fetch_add(1, Ordering::SeqCst);
            Err(ModelError::Cancelled)
        }
    }

    #[tokio::test]
    async fn cancel_reaches_the_node() {
        let dialer = MemoryDialer::new();
        let cancelled = Arc::new(AtomicUsize::new(0));
        serving(&dialer, "desktop", Some(Arc::new(Slow { cancelled: cancelled.clone() })), Admission::new(1));
        let remote = Arc::new(RemoteModel::new(Arc::new(dialer), vec!["desktop".into()]));
        let (deltas, mut rx) = sink();
        let cancel = CancelToken::new();
        let task = {
            let (remote, cancel) = (remote.clone(), cancel.clone());
            tokio::spawn(async move { remote.complete(&[], &[], &deltas, &cancel).await })
        };
        // Wait for the first token, then cancel.
        assert_eq!(rx.recv().await, Some(AgentEvent::TextDelta("thinking".into())));
        cancel.cancel();
        assert_eq!(task.await.unwrap(), Err(ModelError::Cancelled));
        for _ in 0..100 {
            if cancelled.load(Ordering::SeqCst) == 1 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("the node never saw the cancel");
    }

    #[tokio::test]
    async fn a_failure_after_tokens_streamed_does_not_fall_back() {
        // A node that accepts, streams a token, then drops the channel.
        let dialer = MemoryDialer::new();
        dialer.route("flaky", |mut ch, _| {
            tokio::spawn(async move {
                let Ok(Some(ClientMsg::Infer { request })) = ch.recv::<ClientMsg>().await else { return };
                let _ = ch.send(&ServerMsg::Accepted { id: request.id.clone(), model: "m".into() }).await;
                let _ = ch.send(&ServerMsg::Delta { id: request.id, text: "part".into() }).await;
            });
        });
        let remote = RemoteModel::new(Arc::new(dialer), vec!["flaky".into()]).with_fallback(Arc::new(ScriptedModel::new(vec![turn("local")])));
        let err = remote.complete(&[], &[], &DeltaSink::discard(), &CancelToken::new()).await.unwrap_err();
        assert_eq!(err, ModelError::Failed("flaky: connection closed".into()));
    }

    #[tokio::test(start_paused = true)]
    async fn a_node_that_never_accepts_times_out_and_the_next_is_tried() {
        let dialer = MemoryDialer::new();
        let parked = Arc::new(Mutex::new(Vec::new()));
        let keep = parked.clone();
        dialer.route("silent", move |ch, _| keep.lock().unwrap().push(ch));
        serving(&dialer, "good", Some(Arc::new(ScriptedModel::new(vec![turn("ok")]))), Admission::new(1));
        let remote = RemoteModel::new(Arc::new(dialer), vec!["silent".into(), "good".into()]).with_accept_timeout(Duration::from_millis(50));
        let got = remote.complete(&[], &[], &DeltaSink::discard(), &CancelToken::new()).await.unwrap();
        assert_eq!(got.text, "ok");
        assert_eq!(parked.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn the_server_cancels_when_the_asker_goes_away() {
        let cancelled = Arc::new(AtomicUsize::new(0));
        let host = Host { model: Some(Arc::new(Slow { cancelled: cancelled.clone() })) };
        let (mut ours, theirs) = memory_pair();
        let server = tokio::spawn(async move { serve(theirs, &host, &Admission::new(1)).await });
        ours.send(&ClientMsg::Infer { request: InferRequest { id: "1".into(), model: None, messages: vec![], tools: vec![] } }).await.unwrap();
        assert!(matches!(ours.recv::<ServerMsg>().await.unwrap(), Some(ServerMsg::Accepted { .. })));
        assert!(matches!(ours.recv::<ServerMsg>().await.unwrap(), Some(ServerMsg::Delta { .. })));
        drop(ours);
        server.await.unwrap().unwrap();
        assert_eq!(cancelled.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn wire_messages_round_trip() {
        let m = ClientMsg::Infer {
            request: InferRequest { id: "a".into(), model: Some("gemma".into()), messages: vec![Message::user("q")], tools: vec![] },
        };
        let back: ClientMsg = serde_json::from_str(&serde_json::to_string(&m).unwrap()).unwrap();
        assert_eq!(m, back);
        let s = ServerMsg::Failed { id: "a".into(), error: RemoteError::Busy };
        let json = serde_json::to_string(&s).unwrap();
        assert_eq!(json, r#"{"msg":"failed","id":"a","error":{"kind":"busy"}}"#);
    }
}
