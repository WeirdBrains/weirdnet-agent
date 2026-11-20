//! The asking side of remote inference: a pool of inference nodes.
//!
//! [`Pool`] sends a [`Request`] to the first node of its [`NodeSource`] that takes it
//! (infer/2, falling back to infer/1 on nodes that only serve that), streams the answer to an
//! [`EventSink`], and moves on to the next node when one fails:
//!
//! - **Before any text** (unreachable, busy, no such model, no `Accepted` in time): the next
//!   node is tried, as `RemoteModel` always did. `Queued` frames reset the accept timeout.
//! - **During the answer** (the stream drops, the node fails, or no frame arrives within the
//!   [`Stall`] timeout, after which the pool sends `Cancel` itself rather than wait out
//!   `weirdnet-net`'s 30 s stream resume): with [`PoolOptions::resume`] on, the request goes to
//!   the next node as the same job with `attempt + 1` and the text shown so far as
//!   `assistant_prefix`, but only to a node whose `Accepted` says `resume: true`. Frames are
//!   placed by offset, so nothing is shown twice. At most [`PoolOptions::max_resumes`] resumes
//!   per answer; each is recorded in the answer's [`Meta::resumes`]. With resume off, or once
//!   nothing more can take it, the partial answer comes back in [`Interrupted`].
//! - **Continue:** a request that already carries an `assistant_prefix` (the user asked to
//!   continue a stopped or cut-off answer) continues that text: the answer starts from the
//!   prefix ([`Answer::prefix`]; `text` and `reasoning` include it, events carry only what is
//!   new), and only nodes whose `Accepted` says `resume: true` take it. Failover keeps the
//!   prefix whether or not resume is on; infer/1 nodes are skipped.
//!
//! - **Images and audio:** a request whose messages carry image or audio parts
//!   ([`Request::needs`]) is routed by the catalog first ([`Catalog::route`]): a named model must
//!   list those inputs, and with none named the pool picks one that does. When the catalog says
//!   no model takes them, the request fails with `BadRequest` before any node runs it. With no
//!   catalog to ask, the nodes decide (model nodes refuse inputs their model can't read). Such
//!   requests never go to infer/1 nodes, which predate content parts.
//!
//! The pool also queries and subscribes to the [`catalog`], sends embeddings,
//! and runs [`Job`]s in the background. It has no networking of its own: a [`Dialer`] opens
//! channels and a [`NodeSource`] says which nodes to try, in order. The `pool` cargo feature
//! provides both over `weirdnet-net` (`crate::pool`).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use async_trait::async_trait;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use super::catalog::{self, Catalog};
use super::infer::v2::{self, ClientFrame, EmbedRequest, Finish, Meta, Prefix, Request, Resume, ServerFrame, Usage, chars};
use super::infer::{self as v1, DEFAULT_ACCEPT_TIMEOUT};
use super::{Channel, Dialer, RemoteError, request_id};
use crate::cancel::CancelToken;
use crate::model::{DeltaSink, ModelTurn, ToolCall};

/// Resumes per answer when [`PoolOptions::resume`] is on.
pub const DEFAULT_MAX_RESUMES: u32 = 2;

/// When an accepted stream counts as stalled: no frame for `max(min, factor × the recent gap
/// between frames)`, capped at `max`. Before the first frame after `Accepted` (prompt
/// processing) the wait is `max`. The defaults (15 s, 3×, 25 s) stay under `weirdnet-net`'s
/// 30 s stream resume window, so the pool cancels and moves on before the transport would give
/// up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct Stall {
    pub min: Duration,
    pub factor: u32,
    pub max: Duration,
}

impl Default for Stall {
    fn default() -> Self {
        Self { min: Duration::from_secs(15), factor: 3, max: Duration::from_secs(25) }
    }
}

impl Stall {
    pub fn new(min: Duration, factor: u32, max: Duration) -> Self {
        Self { min, factor, max }
    }

    /// The wait for the first frame after `Accepted`.
    pub fn first(&self) -> Duration {
        self.max.max(self.min)
    }

    /// The timeout given the recent gap between frames.
    pub fn timeout(&self, recent_gap: Option<Duration>) -> Duration {
        let scaled = recent_gap.map(|g| g * self.factor).unwrap_or_default();
        scaled.max(self.min).min(self.max.max(self.min))
    }
}

/// The recent gap between frames: a moving average.
#[derive(Debug, Default)]
struct Gaps {
    last: Option<Instant>,
    recent: Option<Duration>,
}

impl Gaps {
    fn wait(&self, stall: &Stall) -> Duration {
        if self.last.is_none() { stall.first() } else { stall.timeout(self.recent) }
    }

    fn tick(&mut self) {
        let now = Instant::now();
        if let Some(last) = self.last {
            let gap = now - last;
            self.recent = Some(match self.recent {
                Some(r) => (r * 3 + gap) / 4,
                None => gap,
            });
        }
        self.last = Some(now);
    }
}

/// How a [`Pool`] behaves.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct PoolOptions {
    /// How long a node has to accept (reset by each `Queued`), and to open a channel.
    pub accept_timeout: Duration,
    pub stall: Stall,
    /// Continue an interrupted answer on another node. On by default; `RemoteModel` turns it off.
    pub resume: bool,
    pub max_resumes: u32,
    /// Try infer/1 on nodes that do not serve infer/2 (only before any text has streamed).
    pub infer_v1_fallback: bool,
    /// The certificate presented with every request (for Weirdnet a CBOR `NodeCert`).
    pub cert: Option<Vec<u8>>,
}

impl Default for PoolOptions {
    fn default() -> Self {
        Self {
            accept_timeout: DEFAULT_ACCEPT_TIMEOUT,
            stall: Stall::default(),
            resume: true,
            max_resumes: DEFAULT_MAX_RESUMES,
            infer_v1_fallback: true,
            cert: None,
        }
    }
}

impl PoolOptions {
    pub fn with_accept_timeout(mut self, timeout: Duration) -> Self {
        self.accept_timeout = timeout;
        self
    }

    pub fn with_stall(mut self, stall: Stall) -> Self {
        self.stall = stall;
        self
    }

    pub fn with_resume(mut self, resume: bool) -> Self {
        self.resume = resume;
        self
    }

    pub fn with_max_resumes(mut self, max: u32) -> Self {
        self.max_resumes = max;
        self
    }

    pub fn with_infer_v1_fallback(mut self, on: bool) -> Self {
        self.infer_v1_fallback = on;
        self
    }

    pub fn with_cert(mut self, cert: Option<Vec<u8>>) -> Self {
        self.cert = cert;
        self
    }
}

/// Which inference nodes to try, best first (for example the local one, then the closest).
#[async_trait]
pub trait NodeSource: Send + Sync {
    async fn nodes(&self) -> Vec<String>;
}

#[async_trait]
impl NodeSource for Vec<String> {
    async fn nodes(&self) -> Vec<String> {
        self.clone()
    }
}

/// What the pool reports while an answer streams.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum PoolEvent {
    /// Waiting on `node` at this place.
    Queued {
        node: String,
        position: u32,
    },
    Accepted {
        node: String,
        model: String,
        resume: bool,
    },
    Text(String),
    Reasoning(String),
    ToolCall(ToolCall),
    Usage(Usage),
    /// The answer continues on `node` from `offset` characters.
    Resumed {
        node: String,
        offset: u64,
    },
    /// Response metadata the node knew before the answer, such as a cache hit (0.4.1 servers
    /// and later; the final [`Answer::meta`] has it too).
    Meta(Meta),
}

/// Where a [`Pool`] streams [`PoolEvent`]s. A [`DeltaSink`] takes the text and reasoning.
pub trait EventSink: Send + Sync {
    fn event(&self, event: PoolEvent);
}

impl EventSink for DeltaSink {
    fn event(&self, event: PoolEvent) {
        match event {
            PoolEvent::Text(t) => self.text(t),
            PoolEvent::Reasoning(t) => self.reasoning(t),
            _ => {}
        }
    }
}

impl EventSink for mpsc::UnboundedSender<PoolEvent> {
    fn event(&self, event: PoolEvent) {
        let _ = self.send(event);
    }
}

/// An [`EventSink`] that drops everything.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoEvents;

impl EventSink for NoEvents {
    fn event(&self, _: PoolEvent) {}
}

/// A whole answer.
#[derive(Debug, Clone, PartialEq, Default)]
#[non_exhaustive]
pub struct Answer {
    pub text: String,
    pub reasoning: String,
    pub tool_calls: Vec<ToolCall>,
    pub finish_reason: Option<String>,
    pub usage: Option<Usage>,
    pub meta: Meta,
    /// The inference node that finished it.
    pub node: String,
    /// The model it reported.
    pub model: String,
    /// The `assistant_prefix` the request asked to continue (empty if none). `text` and
    /// `reasoning` start with it; [`Answer::continuation`] is the text after it.
    pub prefix: Prefix,
}

impl Answer {
    /// As a model turn (text and tool calls).
    pub fn turn(&self) -> ModelTurn {
        ModelTurn { text: self.text.clone(), tool_calls: self.tool_calls.clone() }
    }

    /// The text generated after [`Answer::prefix`] (all of it when there is none).
    pub fn continuation(&self) -> &str {
        let at = self.text.char_indices().nth(self.prefix.len as usize).map_or(self.text.len(), |(i, _)| i);
        &self.text[at..]
    }

    /// Something was generated beyond the prefix.
    fn has_output(&self) -> bool {
        chars(&self.text) > self.prefix.len || chars(&self.reasoning) > self.prefix.reasoning_len || !self.tool_calls.is_empty()
    }

    /// The text and reasoning so far, as the prefix of a next attempt.
    fn shown(&self) -> Option<Prefix> {
        (!self.text.is_empty() || !self.reasoning.is_empty()).then(|| Prefix::new(self.text.clone(), self.reasoning.clone()))
    }
}

/// An answer that could not be finished: why, what was shown, and every node tried.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[error("{error}")]
#[non_exhaustive]
pub struct Interrupted {
    pub error: RemoteError,
    /// What streamed before it stopped (empty if nothing did).
    pub partial: Box<Answer>,
    /// Each node tried and why it did not finish.
    pub tried: Vec<(String, RemoteError)>,
}

impl Interrupted {
    /// Some of the answer was shown.
    pub fn streamed(&self) -> bool {
        self.partial.has_output()
    }
}

/// Embeddings for each input, in order.
#[derive(Debug, Clone, PartialEq, Default)]
#[non_exhaustive]
pub struct Embeddings {
    pub vectors: Vec<Vec<f32>>,
    pub model: String,
    pub node: String,
    pub meta: Meta,
    /// Prompt tokens, when the serving node reports them (0.4.1 servers and later).
    pub usage: Option<Usage>,
}

enum Fail {
    Cancelled,
    /// The node does not serve the protocol.
    Unsupported(RemoteError),
    /// The node would not resume.
    NoResume,
    Error(RemoteError),
}

/// The error to report when every node failed, kind intact: the last node answer
/// ([`RemoteError::is_node_answer`]), else the last error; `Unavailable(none)` with no nodes.
fn reported<'a>(errors: impl DoubleEndedIterator<Item = &'a RemoteError> + Clone, none: &str) -> RemoteError {
    let mut last = errors.clone().rev();
    errors.rev().find(|e| e.is_node_answer()).or_else(|| last.next()).cloned().unwrap_or_else(|| RemoteError::unavailable(none))
}

/// Places `text` at `offset` in `buf`: `Ok(Some(new))` with the part that was not there yet,
/// `Ok(None)` if it was all there, `Err` on a gap.
fn splice(buf: &mut String, offset: u64, text: &str) -> Result<Option<String>, u64> {
    let have = chars(buf);
    if offset > have {
        return Err(offset - have);
    }
    let skip = (have - offset) as usize;
    let new: String = text.chars().skip(skip).collect();
    if new.is_empty() {
        return Ok(None);
    }
    buf.push_str(&new);
    Ok(Some(new))
}

/// A pool of inference nodes. Cheap to clone.
#[derive(Clone)]
pub struct Pool {
    dialer: Arc<dyn Dialer>,
    nodes: Arc<dyn NodeSource>,
    opts: PoolOptions,
}

impl Pool {
    pub fn new(dialer: Arc<dyn Dialer>, nodes: impl NodeSource + 'static) -> Self {
        Self { dialer, nodes: Arc::new(nodes), opts: PoolOptions::default() }
    }

    pub fn with_options(mut self, opts: PoolOptions) -> Self {
        self.opts = opts;
        self
    }

    pub fn options(&self) -> &PoolOptions {
        &self.opts
    }

    pub(crate) fn options_mut(&mut self) -> &mut PoolOptions {
        &mut self.opts
    }

    /// The nodes it would try now, in order.
    pub async fn nodes(&self) -> Vec<String> {
        self.nodes.nodes().await
    }

    async fn open(&self, node: &str, protocol: &str, cancel: &CancelToken) -> Result<Channel, Fail> {
        let open = tokio::select! {
            r = tokio::time::timeout(self.opts.accept_timeout, self.dialer.open(node, protocol)) => r,
            _ = cancel.cancelled() => return Err(Fail::Cancelled),
        };
        match open {
            Ok(Ok(ch)) => Ok(ch),
            Ok(Err(e @ RemoteError::Unsupported { .. })) => Err(Fail::Unsupported(e)),
            Ok(Err(e)) => Err(Fail::Error(e)),
            Err(_) => Err(Fail::Error(RemoteError::Timeout)),
        }
    }

    /// Runs one answer to the end, failing over (and, with resume on, resuming) between nodes.
    pub async fn infer(&self, mut request: Request, events: &dyn EventSink, cancel: &CancelToken) -> Result<Answer, Interrupted> {
        if request.cert.is_none() {
            request.cert.clone_from(&self.opts.cert);
        }
        let needed = request.needs();
        if !needed.is_text() {
            let refuse = |error| Err(Interrupted { error, partial: Box::default(), tried: Vec::new() });
            if let Some(Err(message)) = request.inline_messages().unwrap_or_default().iter().map(|m| m.content.check()).find(Result::is_err) {
                return refuse(RemoteError::BadRequest { message });
            }
            match self.catalog().await {
                Ok(catalog) => match catalog.route(request.model.as_deref(), needed) {
                    Ok(model) => request.model = model,
                    Err(e) => return refuse(e),
                },
                Err(e) if e.is_request_error() => return refuse(e),
                // No catalog to ask: the nodes decide.
                Err(_) => {}
            }
        }
        let mut answer = Answer::default();
        // Continue: the answer starts from the caller's prefix, so the node's frames (which
        // start at its length) line up, and failover keeps it.
        if let Some(p) = request.assistant_prefix.clone().filter(|p| !p.is_empty() && p.is_consistent()) {
            answer.text.clone_from(&p.text);
            answer.reasoning.clone_from(&p.reasoning);
            answer.prefix = p;
        }
        let mut tried: Vec<(String, RemoteError)> = Vec::new();
        let mut resumes: Vec<Resume> = Vec::new();
        let mut next = request;
        let mut first = true;
        let interrupted = |error, partial, tried| Interrupted { error, partial: Box::new(partial), tried };
        for node in self.nodes.nodes().await {
            if cancel.is_cancelled() {
                return Err(interrupted(RemoteError::Cancelled, answer, tried));
            }
            let resuming = answer.has_output();
            if !first {
                next = next.next_attempt();
                next.assistant_prefix = answer.shown();
            }
            first = false;
            let pending = resuming.then(|| chars(&answer.text));
            let outcome = match self.attempt(&node, &next, &mut answer, pending, &mut resumes, events, cancel).await {
                Err(Fail::Unsupported(e)) if self.opts.infer_v1_fallback && next.assistant_prefix.is_none() && needed.is_text() => {
                    match self.attempt_v1(&node, &next, &mut answer, events, cancel).await {
                        Err(Fail::Unsupported(_)) => Err(Fail::Unsupported(e)),
                        other => other,
                    }
                }
                other => other,
            };
            match outcome {
                Ok(finish) => {
                    answer.finish_reason = finish.finish_reason;
                    if finish.usage.is_some() {
                        answer.usage = finish.usage;
                    }
                    answer.meta = finish.meta;
                    answer.meta.resumes.extend(resumes);
                    answer.node = node;
                    return Ok(answer);
                }
                Err(Fail::Cancelled) => return Err(interrupted(RemoteError::Cancelled, answer, tried)),
                Err(Fail::NoResume) => {
                    tried.push((node, RemoteError::Unsupported { message: "the node cannot resume or continue an answer".into() }))
                }
                Err(Fail::Unsupported(e)) => tried.push((node, e)),
                Err(Fail::Error(e)) if e.is_request_error() => {
                    tried.push((node, e.clone()));
                    return Err(interrupted(e, answer, tried));
                }
                Err(Fail::Error(e)) => {
                    tried.push((node, e.clone()));
                    if answer.has_output() && (!self.opts.resume || resumes.len() as u32 >= self.opts.max_resumes) {
                        return Err(interrupted(e, answer, tried));
                    }
                }
            }
        }
        let error = reported(tried.iter().map(|(_, e)| e), "no inference node to run on");
        Err(interrupted(error, answer, tried))
    }

    #[allow(clippy::too_many_arguments)]
    async fn attempt(
        &self,
        node: &str,
        request: &Request,
        answer: &mut Answer,
        resume_from: Option<u64>,
        resumes: &mut Vec<Resume>,
        events: &dyn EventSink,
        cancel: &CancelToken,
    ) -> Result<Finish, Fail> {
        let mut ch = self.open(node, v2::PROTOCOL, cancel).await?;
        let id = request.id.clone();
        let mut sent = request.clone();
        sent.early_meta = true;
        ch.send_cbor(&ClientFrame::Infer(sent)).await.map_err(Fail::Error)?;
        let wants_resume = request.assistant_prefix.as_ref().is_some_and(|p| !p.is_empty());
        let mut accepted = false;
        let mut gaps = Gaps::default();
        let calls_before = answer.tool_calls.len();
        loop {
            let wait = if accepted { gaps.wait(&self.opts.stall) } else { self.opts.accept_timeout };
            let next = tokio::select! {
                _ = cancel.cancelled() => {
                    let _ = ch.send_cbor(&ClientFrame::Cancel { id }).await;
                    return Err(Fail::Cancelled);
                }
                r = tokio::time::timeout(wait, ch.recv_cbor::<ServerFrame>()) => r,
            };
            let frame = match next {
                Err(_) => {
                    let _ = ch.send_cbor(&ClientFrame::Cancel { id }).await;
                    let e = if accepted { RemoteError::Stalled { ms: wait.as_millis() as u64 } } else { RemoteError::Timeout };
                    return Err(Fail::Error(e));
                }
                Ok(Err(e)) => return Err(Fail::Error(e)),
                Ok(Ok(None)) => return Err(Fail::Error(RemoteError::Closed)),
                Ok(Ok(Some(f))) => f,
            };
            if frame.id() != id {
                continue;
            }
            match frame {
                ServerFrame::Queued { position, .. } if !accepted => events.event(PoolEvent::Queued { node: node.into(), position }),
                ServerFrame::Accepted { model, resume, .. } if !accepted => {
                    if wants_resume && !resume {
                        let _ = ch.send_cbor(&ClientFrame::Cancel { id }).await;
                        return Err(Fail::NoResume);
                    }
                    accepted = true;
                    answer.model.clone_from(&model);
                    events.event(PoolEvent::Accepted { node: node.into(), model, resume });
                    if let Some(offset) = resume_from {
                        resumes.push(Resume::new(offset, node));
                        events.event(PoolEvent::Resumed { node: node.into(), offset });
                    }
                }
                ServerFrame::Delta { offset, text, .. } if accepted => match splice(&mut answer.text, offset, &text) {
                    Ok(Some(new)) => {
                        gaps.tick();
                        events.event(PoolEvent::Text(new));
                    }
                    Ok(None) => gaps.tick(),
                    Err(gap) => {
                        let _ = ch.send_cbor(&ClientFrame::Cancel { id }).await;
                        return Err(Fail::Error(RemoteError::protocol(format!("{gap} characters of the answer are missing"))));
                    }
                },
                ServerFrame::Reasoning { offset, text, .. } if accepted => match splice(&mut answer.reasoning, offset, &text) {
                    Ok(Some(new)) => {
                        gaps.tick();
                        events.event(PoolEvent::Reasoning(new));
                    }
                    Ok(None) => gaps.tick(),
                    Err(gap) => {
                        let _ = ch.send_cbor(&ClientFrame::Cancel { id }).await;
                        return Err(Fail::Error(RemoteError::protocol(format!("{gap} characters of the reasoning are missing"))));
                    }
                },
                ServerFrame::ToolCall { index, call, .. } if accepted => {
                    gaps.tick();
                    // Calls are whole; after a resume one already held may come again.
                    let slot = calls_before + index as usize;
                    let seen = answer.tool_calls.iter().any(|c| c.name == call.name && c.arguments == call.arguments);
                    if slot >= answer.tool_calls.len() && !seen {
                        answer.tool_calls.push(call.clone());
                        events.event(PoolEvent::ToolCall(call));
                    }
                }
                ServerFrame::Usage { usage, .. } if accepted => {
                    gaps.tick();
                    answer.usage = Some(usage);
                    events.event(PoolEvent::Usage(usage));
                }
                ServerFrame::Meta { meta, .. } if accepted => {
                    gaps.tick();
                    events.event(PoolEvent::Meta(meta));
                }
                ServerFrame::Done { finish, .. } if accepted => return Ok(finish),
                ServerFrame::Failed { error, .. } => {
                    return Err(match error {
                        e @ RemoteError::Unsupported { .. } if !accepted => Fail::Unsupported(e),
                        e => Fail::Error(e),
                    });
                }
                ServerFrame::Cancelled { .. } => return Err(Fail::Error(RemoteError::Failed { message: "cancelled by the node".into() })),
                other => return Err(Fail::Error(RemoteError::protocol(format!("unexpected frame {other:?}")))),
            }
        }
    }

    /// One try on an infer/1 node (0.3 and older): inline messages and tools only, no resume.
    async fn attempt_v1(
        &self,
        node: &str,
        request: &Request,
        answer: &mut Answer,
        events: &dyn EventSink,
        cancel: &CancelToken,
    ) -> Result<Finish, Fail> {
        let Some(messages) = request.inline_messages() else {
            return Err(Fail::Unsupported(RemoteError::Unsupported { message: "infer/1 takes inline messages only".into() }));
        };
        let mut ch = self.open(node, v1::PROTOCOL, cancel).await?;
        let id = request_id();
        let req = v1::InferRequest { id: id.clone(), model: request.model.clone(), messages: messages.to_vec(), tools: request.tools.clone() };
        ch.send(&v1::ClientMsg::Infer { request: req }).await.map_err(Fail::Error)?;
        let mut accepted = false;
        loop {
            let next = tokio::select! {
                _ = cancel.cancelled() => {
                    let _ = ch.send(&v1::ClientMsg::Cancel { id }).await;
                    return Err(Fail::Cancelled);
                }
                m = async {
                    if accepted {
                        Ok(ch.recv::<v1::ServerMsg>().await)
                    } else {
                        tokio::time::timeout(self.opts.accept_timeout, ch.recv::<v1::ServerMsg>()).await
                    }
                } => m,
            };
            let msg = match next {
                Err(_) => return Err(Fail::Error(RemoteError::Timeout)),
                Ok(Err(e)) => return Err(Fail::Error(e)),
                Ok(Ok(None)) => return Err(Fail::Error(RemoteError::Closed)),
                Ok(Ok(Some(m))) => m,
            };
            match msg {
                v1::ServerMsg::Accepted { id: rid, model } if rid == id => {
                    accepted = true;
                    answer.model.clone_from(&model);
                    events.event(PoolEvent::Accepted { node: node.into(), model, resume: false });
                }
                v1::ServerMsg::Delta { id: rid, text } if rid == id && accepted => {
                    answer.text.push_str(&text);
                    events.event(PoolEvent::Text(text));
                }
                v1::ServerMsg::Done { id: rid, turn } if rid == id && accepted => {
                    answer.text = turn.text;
                    let calls = !turn.tool_calls.is_empty();
                    for call in turn.tool_calls {
                        answer.tool_calls.push(call.clone());
                        events.event(PoolEvent::ToolCall(call));
                    }
                    let mut finish = Finish::new(if calls { "tool_calls" } else { "stop" });
                    finish.meta.served_by = Some(node.to_string());
                    return Ok(finish);
                }
                v1::ServerMsg::Failed { id: rid, error } if rid == id => return Err(Fail::Error(error)),
                v1::ServerMsg::Cancelled { id: rid } if rid == id => {
                    return Err(Fail::Error(RemoteError::Failed { message: "cancelled by the node".into() }));
                }
                other => return Err(Fail::Error(RemoteError::protocol(format!("unexpected message {other:?}")))),
            }
        }
    }

    /// Embeddings for every input, from the first node that serves them all (infer/2 only).
    pub async fn embed(&self, mut request: EmbedRequest, cancel: &CancelToken) -> Result<Embeddings, RemoteError> {
        if request.cert.is_none() {
            request.cert.clone_from(&self.opts.cert);
        }
        let mut tried = Vec::new();
        for node in self.nodes.nodes().await {
            match self.embed_on(&node, &request, cancel).await {
                Ok(e) => return Ok(e),
                Err(Fail::Cancelled) => return Err(RemoteError::Cancelled),
                Err(Fail::Error(e) | Fail::Unsupported(e)) if e.is_request_error() => return Err(e),
                Err(Fail::Error(e) | Fail::Unsupported(e)) => tried.push(e),
                Err(Fail::NoResume) => {}
            }
            request.id = request_id();
        }
        Err(reported(tried.iter(), "no inference node to run on"))
    }

    async fn embed_on(&self, node: &str, request: &EmbedRequest, cancel: &CancelToken) -> Result<Embeddings, Fail> {
        let mut ch = self.open(node, v2::PROTOCOL, cancel).await?;
        let id = request.id.clone();
        ch.send_cbor(&ClientFrame::Embed(request.clone())).await.map_err(Fail::Error)?;
        let mut out = Embeddings { node: node.to_string(), ..Default::default() };
        let mut got: BTreeMap<u32, Vec<f32>> = BTreeMap::new();
        let mut accepted = false;
        let mut gaps = Gaps::default();
        loop {
            let wait = if accepted { gaps.wait(&self.opts.stall) } else { self.opts.accept_timeout };
            let next = tokio::select! {
                _ = cancel.cancelled() => {
                    let _ = ch.send_cbor(&ClientFrame::Cancel { id }).await;
                    return Err(Fail::Cancelled);
                }
                r = tokio::time::timeout(wait, ch.recv_cbor::<ServerFrame>()) => r,
            };
            let frame = match next {
                Err(_) => {
                    let _ = ch.send_cbor(&ClientFrame::Cancel { id }).await;
                    return Err(Fail::Error(if accepted { RemoteError::Stalled { ms: wait.as_millis() as u64 } } else { RemoteError::Timeout }));
                }
                Ok(Err(e)) => return Err(Fail::Error(e)),
                Ok(Ok(None)) => return Err(Fail::Error(RemoteError::Closed)),
                Ok(Ok(Some(f))) => f,
            };
            if frame.id() != id {
                continue;
            }
            match frame {
                ServerFrame::Queued { .. } if !accepted => {}
                ServerFrame::Accepted { model, .. } if !accepted => {
                    accepted = true;
                    out.model = model;
                }
                ServerFrame::Embedding { index, vector, .. } if accepted => {
                    gaps.tick();
                    got.insert(index, v2::bytes_f32(&vector).map_err(Fail::Error)?);
                }
                ServerFrame::Done { finish, .. } if accepted => {
                    if got.len() != request.inputs.len() || got.keys().enumerate().any(|(i, k)| *k as usize != i) {
                        return Err(Fail::Error(RemoteError::protocol(format!("{} of {} embeddings arrived", got.len(), request.inputs.len()))));
                    }
                    out.vectors = got.into_values().collect();
                    out.meta = finish.meta;
                    out.usage = finish.usage;
                    return Ok(out);
                }
                ServerFrame::Failed { error, .. } => return Err(Fail::Error(error)),
                ServerFrame::Cancelled { .. } => return Err(Fail::Error(RemoteError::Failed { message: "cancelled by the node".into() })),
                other => return Err(Fail::Error(RemoteError::protocol(format!("unexpected frame {other:?}")))),
            }
        }
    }

    /// The catalog of the first node that answers.
    pub async fn catalog(&self) -> Result<Catalog, RemoteError> {
        let mut tried = Vec::new();
        for node in self.nodes.nodes().await {
            let r = tokio::time::timeout(self.opts.accept_timeout, async {
                let mut ch = self.dialer.open(&node, catalog::PROTOCOL).await?;
                catalog::query(&mut ch, self.opts.cert.clone()).await
            })
            .await
            .unwrap_or(Err(RemoteError::Timeout));
            match r {
                Ok(c) => return Ok(c),
                Err(e) if e.is_request_error() => return Err(e),
                Err(e) => tried.push(e),
            }
        }
        Err(reported(tried.iter(), "no inference node to ask"))
    }

    /// Follows the catalog: the first node that answers streams its changes; when it goes away
    /// the next one takes over. Stops when the [`CatalogWatch`] is dropped.
    pub fn subscribe(&self) -> CatalogWatch {
        let (tx, rx) = watch::channel(None);
        let pool = self.clone();
        let task = tokio::spawn(async move {
            let mut backoff = Duration::from_millis(200);
            loop {
                for node in pool.nodes.nodes().await {
                    let opened = tokio::time::timeout(pool.opts.accept_timeout, pool.dialer.open(&node, catalog::PROTOCOL)).await;
                    let Ok(Ok(mut ch)) = opened else { continue };
                    let sent = catalog::subscribe(&mut ch, pool.opts.cert.clone(), |c| {
                        tx.send_replace(Some((node.clone(), c)));
                    });
                    tokio::select! {
                        r = sent => if r.is_ok() { backoff = Duration::from_millis(200) },
                        _ = tx.closed() => return,
                    }
                }
                tokio::select! {
                    _ = tokio::time::sleep(backoff) => {}
                    _ = tx.closed() => return,
                }
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
        });
        CatalogWatch { rx, task }
    }

    /// Runs a job in the background: the same failover and resume as [`Pool::infer`], with its
    /// events on [`Job::events`]. Batch work usually sets `Priority::Batch`.
    pub fn submit(&self, request: Request) -> Job {
        let (tx, events) = mpsc::unbounded_channel();
        let cancel = CancelToken::new();
        let job_id = request.job_id.clone();
        let pool = self.clone();
        let c = cancel.clone();
        let handle = tokio::spawn(async move { pool.infer(request, &tx, &c).await });
        Job { job_id, cancel, handle, events }
    }
}

/// A followed catalog ([`Pool::subscribe`]).
pub struct CatalogWatch {
    rx: watch::Receiver<Option<(String, Catalog)>>,
    task: JoinHandle<()>,
}

impl CatalogWatch {
    /// The latest catalog and the node it came from, if one has arrived.
    pub fn current(&self) -> Option<(String, Catalog)> {
        self.rx.borrow().clone()
    }

    /// Waits for the next catalog.
    pub async fn changed(&mut self) -> Option<(String, Catalog)> {
        self.rx.changed().await.ok()?;
        self.rx.borrow_and_update().clone()
    }
}

impl Drop for CatalogWatch {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// A job running in the background ([`Pool::submit`]).
pub struct Job {
    job_id: String,
    cancel: CancelToken,
    handle: JoinHandle<Result<Answer, Interrupted>>,
    events: mpsc::UnboundedReceiver<PoolEvent>,
}

impl Job {
    pub fn job_id(&self) -> &str {
        &self.job_id
    }

    pub fn cancel(&self) {
        self.cancel.cancel();
    }

    /// The job's events as they happen.
    pub fn events(&mut self) -> &mut mpsc::UnboundedReceiver<PoolEvent> {
        &mut self.events
    }

    /// Waits for the job to end.
    pub async fn wait(self) -> Result<Answer, Interrupted> {
        match self.handle.await {
            Ok(r) => r,
            Err(e) => Err(Interrupted { error: RemoteError::Failed { message: e.to_string() }, partial: Box::default(), tried: Vec::new() }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splice_places_by_offset() {
        let mut b = String::new();
        assert_eq!(splice(&mut b, 0, "Hel"), Ok(Some("Hel".into())));
        assert_eq!(splice(&mut b, 3, "lo"), Ok(Some("lo".into())));
        assert_eq!(splice(&mut b, 0, "Hell"), Ok(None));
        assert_eq!(splice(&mut b, 4, "o wö"), Ok(Some(" wö".into())));
        assert_eq!(b, "Hello wö");
        assert_eq!(splice(&mut b, 10, "x"), Err(2));
    }

    #[test]
    fn the_stall_timeout_follows_the_token_gap_within_bounds() {
        let s = Stall::default();
        assert_eq!(s.timeout(None), Duration::from_secs(15));
        assert_eq!(s.timeout(Some(Duration::from_secs(1))), Duration::from_secs(15));
        assert_eq!(s.timeout(Some(Duration::from_secs(7))), Duration::from_secs(21));
        assert_eq!(s.timeout(Some(Duration::from_secs(20))), Duration::from_secs(25), "stays under net's 30 s resume window");
    }
}
