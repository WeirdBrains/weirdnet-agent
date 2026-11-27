//! Remote inference: one model turn on another of the user's nodes.
//!
//! Two protocol versions, served side by side:
//!
//! - **`/weirdnet-agent/infer/1`** ([`PROTOCOL`], JSON), unchanged on the wire:
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
//! - **`/weirdnet-agent/infer/2`** ([`v2`], CBOR): a priority queue with `Queued`, jobs and
//!   attempts, resume from an assistant prefix, offsets, reasoning and tool-call frames, usage,
//!   metadata, raw mode, embeddings, transcription and speech out.
//!
//! Serving: the app accepts streams of each protocol and hands them, with the transport peer as
//! a [`Caller`], to [`serve`] or [`v2::serve`]; both ask [`InferHost::authorize`] first and pass
//! the caller to [`InferHost::open`]. The `serve` cargo feature does this over `weirdnet-net`.
//!
//! [`RemoteModel`] is a [`ModelBackend`] over a [`Pool`]: it tries its nodes in order (infer/2,
//! or infer/1 on older nodes) and then its local fallback, moving on whenever a node fails
//! before any token has streamed. Once tokens have streamed a failure is returned as is (the
//! text already shown cannot be taken back) unless resume is turned on with
//! [`RemoteModel::with_resume`].

mod queue;
pub mod v2;

use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

pub use queue::{Admission, Permit, Ticket, Turn};

use super::pool::{Pool, PoolOptions};
use super::{Caller, Channel, Dialer, RemoteError};
use crate::cancel::CancelToken;
use crate::model::{DeltaSink, Message, ModelBackend, ModelError, ModelTurn};
use crate::session::AgentEvent;
use crate::tool::ToolSpec;

/// Stream protocol id of infer/1.
pub const PROTOCOL: &str = "/weirdnet-agent/infer/1";

/// How long a node has to accept a turn before the next one is tried. On infer/2 every
/// `Queued` frame starts it again.
pub const DEFAULT_ACCEPT_TIMEOUT: Duration = Duration::from_secs(10);

/// One model turn to run elsewhere (infer/1).
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

/// Asking node to serving node (infer/1).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "msg")]
#[non_exhaustive]
pub enum ClientMsg {
    Infer { request: InferRequest },
    Cancel { id: String },
}

/// Serving node to asking node (infer/1).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "msg")]
#[non_exhaustive]
pub enum ServerMsg {
    Accepted { id: String, model: String },
    Delta { id: String, text: String },
    Done { id: String, turn: ModelTurn },
    Failed { id: String, error: RemoteError },
    Cancelled { id: String },
}

/// What the serving app offers.
pub trait InferHost: Send + Sync {
    /// Whether `caller` may use this node at all (for Weirdnet: a linked device, see
    /// `serve::RosterAuth`). Called before anything else on every request.
    fn authorize(&self, caller: &Caller) -> Result<(), RemoteError>;

    /// A backend for a requested model (infer/1, and infer/2 through the default
    /// [`open_request`](Self::open_request)).
    fn open(&self, caller: &Caller, model: Option<&str>) -> Result<Arc<dyn ModelBackend>, RemoteError>;

    /// The backend for one infer/2 request. The default wraps [`open`](Self::open) in a
    /// [`v2::ModelAdapter`]: inline messages and tools, no resume. Inference nodes route here.
    fn open_request(&self, caller: &Caller, request: &v2::Request) -> Result<Arc<dyn v2::InferBackend>, RemoteError> {
        Ok(Arc::new(v2::ModelAdapter::new(self.open(caller, request.model.as_deref())?)))
    }

    /// The backend for embeddings (infer/2). Default: none here.
    fn open_embed(&self, caller: &Caller, model: Option<&str>) -> Result<Arc<dyn v2::EmbedBackend>, RemoteError> {
        let _ = (caller, model);
        Err(RemoteError::unavailable("no embedding model on this node"))
    }

    /// The backend for transcription (infer/2 `Transcribe`, since 0.5.8), for a named
    /// speech-to-text model or, with `None`, the node's own pick (inference nodes route with
    /// [`Catalog::route_transcribe`](crate::remote::catalog::Catalog::route_transcribe)). The
    /// request has been checked ([`v2::TranscribeRequest::check`]). Default: none here.
    fn open_transcribe(&self, caller: &Caller, model: Option<&str>) -> Result<Arc<dyn v2::TranscribeBackend>, RemoteError> {
        let _ = (caller, model);
        Err(RemoteError::unavailable("no transcription model on this node"))
    }

    /// The backend for speech out (infer/2 `Speak`, since 0.5.11). The request has been
    /// authorized and checked ([`v2::SpeakRequest::check`]); its `model` names a speaking model
    /// or, with `None`, leaves the pick to the node (inference nodes route with
    /// [`Catalog::route_speak`](crate::remote::catalog::Catalog::route_speak) and
    /// [`v2::SpeakRequest::needs`]). The whole request is here so a host can also refuse what a
    /// caller may not do (a guest cloning a voice, say). Default: none here.
    fn open_speak(&self, caller: &Caller, request: &v2::SpeakRequest) -> Result<Arc<dyn v2::SpeakBackend>, RemoteError> {
        let _ = (caller, request);
        Err(RemoteError::unavailable("no speech model on this node"))
    }
}

pub(crate) fn model_error(e: ModelError) -> RemoteError {
    match e {
        ModelError::Unavailable(m) => RemoteError::Unavailable { message: m },
        ModelError::Failed(m) => RemoteError::Failed { message: m },
        ModelError::Cancelled => RemoteError::Cancelled,
        ModelError::PoolUnreachable(m) => RemoteError::Unavailable { message: m },
    }
}

/// Serve one infer/1 request on an accepted channel. `caller` is the transport-authenticated
/// peer; the host authorizes it first and gets it in [`InferHost::open`]. Returns when the turn
/// is done, failed or cancelled; if the asker goes away the turn is cancelled. Infer/1 never
/// queues: with no free slot the answer is `Busy`.
pub async fn serve(mut ch: Channel, caller: &Caller, host: &dyn InferHost, admission: &Admission) -> Result<(), RemoteError> {
    let request = match ch.recv::<ClientMsg>().await? {
        Some(ClientMsg::Infer { request }) => request,
        Some(ClientMsg::Cancel { .. }) | None => return Ok(()),
    };
    let id = request.id.clone();
    if let Err(error) = host.authorize(caller) {
        return ch.send(&ServerMsg::Failed { id, error: error.for_v1() }).await;
    }
    let Some(_permit) = admission.try_take() else {
        return ch.send(&ServerMsg::Failed { id, error: RemoteError::Busy }).await;
    };
    let backend = match host.open(caller, request.model.as_deref()) {
        Ok(b) => b,
        Err(error) => return ch.send(&ServerMsg::Failed { id, error: error.for_v1() }).await,
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
        Err(e) => ServerMsg::Failed { id, error: model_error(e).for_v1() },
    };
    ch.send(&reply).await
}

/// Where the last turn of a [`RemoteModel`] ran.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Route {
    /// On this node, by the model name it reported.
    Node { node: String, model: String },
    /// On the local fallback.
    Fallback,
}

/// A [`ModelBackend`] that runs turns on the user's other nodes, with fallback: a thin layer
/// over a [`Pool`] with resume off by default.
pub struct RemoteModel {
    pool: Pool,
    nodes: Vec<String>,
    model: Option<String>,
    fallback: Option<Arc<dyn ModelBackend>>,
    last: Mutex<Option<Route>>,
}

impl RemoteModel {
    /// Tries `nodes` in order.
    pub fn new(dialer: Arc<dyn Dialer>, nodes: Vec<String>) -> Self {
        let opts = PoolOptions::default().with_resume(false);
        Self::over(Pool::new(dialer, nodes.clone()).with_options(opts), nodes)
    }

    /// Over a pool the app has set up (for example with discovery from the `pool` feature).
    /// `nodes` is only used for [`nodes`](Self::nodes) and the name before the first turn.
    pub fn over(pool: Pool, nodes: Vec<String>) -> Self {
        Self { pool, nodes, model: None, fallback: None, last: Mutex::new(None) }
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
        self.pool.options_mut().accept_timeout = timeout;
        self
    }

    /// Continue an answer on another node when one fails mid-answer (infer/2 nodes that can
    /// resume). Off by default.
    pub fn with_resume(mut self, on: bool) -> Self {
        self.pool.options_mut().resume = on;
        self
    }

    pub fn pool(&self) -> &Pool {
        &self.pool
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
        let mut request = v2::Request::messages(messages.to_vec()).with_tools(tools.to_vec());
        request.model.clone_from(&self.model);
        let err = match self.pool.infer(request, deltas, cancel).await {
            Ok(answer) => {
                self.set_last(Route::Node { node: answer.node.clone(), model: answer.model.clone() });
                return Ok(answer.turn());
            }
            Err(e) => e,
        };
        if err.error == RemoteError::Cancelled {
            return Err(ModelError::Cancelled);
        }
        if err.streamed() {
            let node = err.tried.last().map(|(n, _)| n.as_str()).unwrap_or("remote");
            return Err(ModelError::Failed(format!("{node}: {}", err.error)));
        }
        if let Some(fallback) = &self.fallback {
            let r = fallback.complete(messages, tools, deltas, cancel).await;
            if r.is_ok() {
                self.set_last(Route::Fallback);
            }
            return r;
        }
        if err.tried.is_empty() {
            return Err(ModelError::Unavailable("no node to run on".into()));
        }
        Err(ModelError::Unavailable(err.tried.iter().map(|(n, e)| format!("{n}: {e}")).collect::<Vec<_>>().join("; ")))
    }
}

#[cfg(test)]
mod v2_tests;

#[cfg(test)]
mod speak_tests;
#[cfg(test)]
mod transcribe_tests;

#[cfg(test)]
mod tests_support {
    use super::*;

    /// A host over one model backend that admits everyone but "stranger".
    pub(super) struct ModelHost(pub Arc<dyn ModelBackend>);

    impl InferHost for ModelHost {
        fn authorize(&self, caller: &Caller) -> Result<(), RemoteError> {
            if caller.peer == "stranger" { Err(RemoteError::refused("unknown-device")) } else { Ok(()) }
        }

        fn open(&self, _caller: &Caller, _model: Option<&str>) -> Result<Arc<dyn ModelBackend>, RemoteError> {
            Ok(self.0.clone())
        }
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
        fn authorize(&self, caller: &Caller) -> Result<(), RemoteError> {
            if caller.peer == "stranger" { Err(RemoteError::refused("unknown-device")) } else { Ok(()) }
        }

        fn open(&self, _caller: &Caller, model: Option<&str>) -> Result<Arc<dyn ModelBackend>, RemoteError> {
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
            let host = host.clone();
            let admission = admission.clone();
            tokio::spawn(async move {
                let caller = Caller::new("phone");
                let _ = match protocol.as_str() {
                    PROTOCOL => serve(ch, &caller, host.as_ref(), &admission).await,
                    v2::PROTOCOL => v2::serve(ch, caller, host.as_ref(), &admission).await,
                    other => panic!("{other}"),
                };
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
        dialer.route_only("flaky", &[PROTOCOL], |mut ch, _| {
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
        let server = tokio::spawn(async move { serve(theirs, &Caller::new("phone"), &host, &Admission::new(1)).await });
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
