//! `/weirdnet-agent/infer/2`: remote inference with a queue, jobs, resume and richer frames.
//!
//! ```text
//! asking node                               serving node
//!   Infer(Request) | Embed(EmbedRequest) ->
//!                                           <- Queued { position } ...   (while waiting for a slot)
//!                                           <- Accepted { model, attempt, resume }
//!                                           <- Delta { offset, text } | Reasoning { offset, text }
//!                                           <- Meta { meta }             (early_meta askers)
//!                                           <- ToolCall { index, call } | Usage | Embedding ...
//!   Cancel { id } ->                        (any time)
//!                                           <- Done { finish } | Failed { error } | Cancelled
//! ```
//!
//! Messages are CBOR. Infer/1 stays JSON and is served beside this protocol; the two can't
//! share a stream id because a `/1` server would ignore `assistant_prefix` (and repeat the
//! prefix) and a `/1` client fails on `Queued`.
//!
//! - **Queue:** requests wait in the node's [`Admission`] priority queue. Every change of place,
//!   and every [`QUEUE_KEEPALIVE`] without one, sends `Queued`; each resets the asker's accept
//!   timeout, so a queued request is not mistaken for a dead node.
//! - **Offsets:** every `Delta` carries the number of characters (Unicode scalar values) of the
//!   answer before it, and every `Reasoning` the same for the reasoning text. A receiver appends
//!   a frame only where it continues its buffer, so a resumed stream never repeats text.
//! - **Resume:** a request may carry the `assistant_prefix` already shown. A server whose
//!   backend honours it ([`InferBackend::resumes`]) says `resume: true` and streams only what
//!   comes after it, starting at offset = prefix length. Others say `resume: false` and start
//!   over at 0; askers that need a resume move on.
//! - **Tool calls** are sent whole, never half-built.
//! - **Raw prompts** ([`Input::Prompt`]) are for completions: the model continues the text, with
//!   no chat template and no tools.
//! - **Early metadata:** an asker that sets [`Request::early_meta`] may get a `Meta` frame before
//!   the answer (a cache hit, say); `Done` carries the same metadata for everyone.
//! - **Raw mode** is for inference node → model node calls: assembled messages only, no context
//!   lookups, caches or stand-ins.
//! - **Embeddings:** one `Embedding` frame per input, the vector as a CBOR byte string of
//!   little-endian `f32`.

use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc;

use super::queue::{Admission, Turn};
use super::{InferHost, model_error};
use crate::cancel::CancelToken;
use crate::model::{DeltaSink, Message, ModelBackend, ToolCall};
use crate::remote::{Caller, Channel, RemoteError, bytes, request_id};
use crate::session::AgentEvent;
use crate::tool::ToolSpec;

/// Stream protocol id.
pub const PROTOCOL: &str = "/weirdnet-agent/infer/2";

/// How often a waiting request hears its place again when it has not changed.
pub const QUEUE_KEEPALIVE: Duration = Duration::from_secs(3);

/// Scheduling class: `interactive` before `batch` before `background` (prefix warming,
/// re-placement). Classes never preempt a running request.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Priority {
    #[default]
    Interactive,
    Batch,
    Background,
}

/// A conversation kept by the inference node (synced context), instead of inline messages.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ContextRef {
    pub conversation: String,
    /// The conversation head the answer follows; `None`: the latest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head: Option<String>,
}

impl ContextRef {
    pub fn new(conversation: impl Into<String>, head: Option<String>) -> Self {
        Self { conversation: conversation.into(), head }
    }
}

/// What the model answers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Input {
    /// Fully assembled messages.
    Messages(Vec<Message>),
    /// A conversation the serving node holds.
    Context(ContextRef),
    /// A raw text prompt the model continues (OpenAI `/v1/completions`); no chat template,
    /// no tools. Servers older than 0.4.1 can't read it and close the stream.
    Prompt(String),
}

impl Default for Input {
    fn default() -> Self {
        Input::Messages(Vec::new())
    }
}

/// The part of the answer already shown, for resume. Lengths are in characters.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Prefix {
    pub text: String,
    pub len: u64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reasoning: String,
    #[serde(default)]
    pub reasoning_len: u64,
}

impl Prefix {
    pub fn new(text: impl Into<String>, reasoning: impl Into<String>) -> Self {
        let (text, reasoning) = (text.into(), reasoning.into());
        Self { len: chars(&text), reasoning_len: chars(&reasoning), text, reasoning }
    }

    /// The lengths match the texts.
    pub fn is_consistent(&self) -> bool {
        self.len == chars(&self.text) && self.reasoning_len == chars(&self.reasoning)
    }

    pub fn is_empty(&self) -> bool {
        self.text.is_empty() && self.reasoning.is_empty()
    }
}

/// Characters (Unicode scalar values) in `s`, the unit of offsets.
pub fn chars(s: &str) -> u64 {
    s.chars().count() as u64
}

/// Sampling settings; `None` leaves the backend's default.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Sampling {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_k: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_p: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub stop: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub presence_penalty: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frequency_penalty: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repeat_penalty: Option<f32>,
}

/// The shape of the answer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ResponseFormat {
    Text,
    /// Any JSON object.
    Json,
    /// JSON matching a schema.
    JsonSchema {
        name: String,
        schema: Value,
        #[serde(default)]
        strict: bool,
    },
}

/// One model turn to run on an infer/2 node.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Request {
    /// This attempt's id on the stream.
    pub id: String,
    /// Stable across attempts and nodes (the idempotency key of a job).
    pub job_id: String,
    /// 0 for the first try, then one more per retry or resume.
    #[serde(default)]
    pub attempt: u32,
    /// The model wanted, by the serving app's id (`None`: the node's default).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default)]
    pub priority: Priority,
    pub input: Input,
    /// Text already shown, to continue from (see [`Prefix`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assistant_prefix: Option<Prefix>,
    #[serde(default)]
    pub sampling: Sampling,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seed: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_format: Option<ResponseFormat>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<ToolSpec>,
    /// Inference node → model node: assembled messages only, no context lookups, caches or
    /// stand-ins.
    #[serde(default)]
    pub raw: bool,
    /// Wait for the requested model to load rather than take a stand-in.
    #[serde(default)]
    pub wait_for_model: bool,
    /// The certificate the asker presents (for Weirdnet a CBOR `NodeCert`), checked by the host
    /// against the transport peer.
    #[serde(default, skip_serializing_if = "Option::is_none", with = "bytes::opt")]
    pub cert: Option<Vec<u8>>,
    /// The asker reads [`ServerFrame::Meta`] (0.4.1 and later), so the server may send response
    /// metadata it knows before the answer, such as a cache hit. Servers that predate it ignore
    /// the flag; askers that don't set it never get the frame.
    #[serde(default, skip_serializing_if = "is_false")]
    pub early_meta: bool,
}

fn is_false(b: &bool) -> bool {
    !*b
}

impl Request {
    /// A request over assembled messages, with a fresh id and job id.
    pub fn messages(messages: Vec<Message>) -> Self {
        Self::with_input(Input::Messages(messages))
    }

    /// A request over a conversation the serving node holds.
    pub fn context(context: ContextRef) -> Self {
        Self::with_input(Input::Context(context))
    }

    /// A request over a raw text prompt (see [`Input::Prompt`]).
    pub fn prompt(prompt: impl Into<String>) -> Self {
        Self::with_input(Input::Prompt(prompt.into()))
    }

    fn with_input(input: Input) -> Self {
        let id = request_id();
        Self { job_id: id.clone(), id, input, ..Default::default() }
    }

    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = Some(model.into());
        self
    }

    pub fn with_priority(mut self, priority: Priority) -> Self {
        self.priority = priority;
        self
    }

    pub fn with_tools(mut self, tools: Vec<ToolSpec>) -> Self {
        self.tools = tools;
        self
    }

    pub fn with_sampling(mut self, sampling: Sampling) -> Self {
        self.sampling = sampling;
        self
    }

    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = Some(seed);
        self
    }

    pub fn with_response_format(mut self, format: ResponseFormat) -> Self {
        self.response_format = Some(format);
        self
    }

    pub fn with_job_id(mut self, job_id: impl Into<String>) -> Self {
        self.job_id = job_id.into();
        self
    }

    pub fn with_prefix(mut self, prefix: Prefix) -> Self {
        self.assistant_prefix = Some(prefix);
        self
    }

    pub fn with_cert(mut self, cert: Vec<u8>) -> Self {
        self.cert = Some(cert);
        self
    }

    /// Raw mode (inference node → model node).
    pub fn raw(mut self) -> Self {
        self.raw = true;
        self
    }

    /// Inline messages, if the input is not a context reference.
    pub fn inline_messages(&self) -> Option<&[Message]> {
        match &self.input {
            Input::Messages(m) => Some(m),
            _ => None,
        }
    }

    /// The raw prompt, if the input is one.
    pub fn raw_prompt(&self) -> Option<&str> {
        match &self.input {
            Input::Prompt(p) => Some(p),
            _ => None,
        }
    }

    /// The next attempt: same job, a new id, `attempt + 1`.
    pub fn next_attempt(&self) -> Self {
        Self { id: request_id(), attempt: self.attempt + 1, ..self.clone() }
    }
}

/// Embeddings for a list of inputs.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct EmbedRequest {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    pub inputs: Vec<String>,
    #[serde(default)]
    pub priority: Priority,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "bytes::opt")]
    pub cert: Option<Vec<u8>>,
}

impl EmbedRequest {
    pub fn new(inputs: Vec<String>) -> Self {
        Self { id: request_id(), inputs, ..Default::default() }
    }

    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = Some(model.into());
        self
    }

    pub fn with_priority(mut self, priority: Priority) -> Self {
        self.priority = priority;
        self
    }
}

/// Token counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Usage {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    #[serde(default)]
    pub reasoning_tokens: u64,
    /// Prompt tokens served from a prefix cache.
    #[serde(default)]
    pub cached_tokens: u64,
}

impl Usage {
    pub fn new(prompt_tokens: u64, completion_tokens: u64) -> Self {
        Self { prompt_tokens, completion_tokens, ..Default::default() }
    }
}

/// Where time went, in milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Timings {
    /// Waiting in the queue.
    pub queued_ms: u64,
    /// From acceptance to the first text or reasoning.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_token_ms: Option<u64>,
    /// From the request to the end.
    pub total_ms: u64,
}

/// An answer that continued elsewhere: from which offset, on which node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Resume {
    pub offset: u64,
    pub node: String,
}

impl Resume {
    pub fn new(offset: u64, node: impl Into<String>) -> Self {
        Self { offset, node: node.into() }
    }
}

/// Response metadata.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Meta {
    /// The model node (or backend) that produced the answer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub served_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
    #[serde(default)]
    pub cache_hit: bool,
    /// Set when a stand-in answered for the requested model.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub standin: Option<String>,
    /// The prefix length this attempt continued from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resumed_from: Option<u64>,
    /// Resumes behind this answer (the inference node's, then the pool's).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resumes: Vec<Resume>,
    #[serde(default)]
    pub timings: Timings,
}

/// How a turn ended.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Finish {
    /// `stop`, `length`, `tool_calls`, ...
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finish_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<Usage>,
    #[serde(default)]
    pub meta: Meta,
}

impl Finish {
    pub fn new(finish_reason: impl Into<String>) -> Self {
        Self { finish_reason: Some(finish_reason.into()), ..Default::default() }
    }

    pub fn with_usage(mut self, usage: Usage) -> Self {
        self.usage = Some(usage);
        self
    }
}

/// Asking node to serving node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
#[allow(clippy::large_enum_variant)]
pub enum ClientFrame {
    Infer(Request),
    Embed(EmbedRequest),
    Cancel { id: String },
}

/// Serving node to asking node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ServerFrame {
    Queued {
        id: String,
        position: u32,
    },
    Accepted {
        id: String,
        model: String,
        attempt: u32,
        resume: bool,
    },
    Delta {
        id: String,
        offset: u64,
        text: String,
    },
    Reasoning {
        id: String,
        offset: u64,
        text: String,
    },
    ToolCall {
        id: String,
        index: u32,
        call: ToolCall,
    },
    Embedding {
        id: String,
        index: u32,
        #[serde(with = "bytes")]
        vector: Vec<u8>,
    },
    Usage {
        id: String,
        usage: Usage,
    },
    Done {
        id: String,
        finish: Finish,
    },
    Failed {
        id: String,
        error: RemoteError,
    },
    Cancelled {
        id: String,
    },
    /// Response metadata known before the answer (a cache hit, a stand-in, the revision). Sent
    /// only to askers that set [`Request::early_meta`]; `Done` carries it again.
    Meta {
        id: String,
        meta: Meta,
    },
}

impl ServerFrame {
    /// The request id the frame belongs to.
    pub fn id(&self) -> &str {
        match self {
            ServerFrame::Queued { id, .. }
            | ServerFrame::Accepted { id, .. }
            | ServerFrame::Delta { id, .. }
            | ServerFrame::Reasoning { id, .. }
            | ServerFrame::ToolCall { id, .. }
            | ServerFrame::Embedding { id, .. }
            | ServerFrame::Usage { id, .. }
            | ServerFrame::Meta { id, .. }
            | ServerFrame::Done { id, .. }
            | ServerFrame::Failed { id, .. }
            | ServerFrame::Cancelled { id } => id,
        }
    }
}

/// A vector as little-endian `f32` bytes.
pub fn f32_bytes(v: &[f32]) -> Vec<u8> {
    v.iter().flat_map(|x| x.to_le_bytes()).collect()
}

/// Little-endian `f32` bytes back to a vector.
pub fn bytes_f32(b: &[u8]) -> Result<Vec<f32>, RemoteError> {
    if !b.len().is_multiple_of(4) {
        return Err(RemoteError::protocol(format!("an embedding of {} bytes is not whole f32s", b.len())));
    }
    Ok(b.as_chunks::<4>().0.iter().map(|c| f32::from_le_bytes(*c)).collect())
}

/// One piece of a backend's output.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Piece {
    Text(String),
    Reasoning(String),
    ToolCall(ToolCall),
    Usage(Usage),
    Meta(Meta),
}

/// Where an [`InferBackend`] streams its output. The server turns it into frames with offsets.
#[derive(Debug, Clone)]
pub struct Output {
    tx: mpsc::UnboundedSender<Piece>,
}

impl Output {
    pub(crate) fn channel() -> (Self, mpsc::UnboundedReceiver<Piece>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Self { tx }, rx)
    }

    /// Answer text.
    pub fn text(&self, delta: impl Into<String>) {
        let d = delta.into();
        if !d.is_empty() {
            let _ = self.tx.send(Piece::Text(d));
        }
    }

    /// Reasoning text.
    pub fn reasoning(&self, delta: impl Into<String>) {
        let d = delta.into();
        if !d.is_empty() {
            let _ = self.tx.send(Piece::Reasoning(d));
        }
    }

    /// A complete tool call.
    pub fn tool_call(&self, call: ToolCall) {
        let _ = self.tx.send(Piece::ToolCall(call));
    }

    /// Token counts so far.
    pub fn usage(&self, usage: Usage) {
        let _ = self.tx.send(Piece::Usage(usage));
    }

    /// Response metadata known before the answer, such as [`Meta::cache_hit`] before replaying a
    /// cached answer. Call it before the first text. The server passes it on to askers that read
    /// it and folds it into the final [`Finish::meta`].
    pub fn meta(&self, meta: Meta) {
        let _ = self.tx.send(Piece::Meta(meta));
    }
}

/// What serves an infer/2 request: a model, or an inference node's routing to model nodes.
/// Implementations poll `cancel` and stream through `out`.
#[async_trait]
pub trait InferBackend: Send + Sync {
    fn name(&self) -> String;

    /// Honours `assistant_prefix`, generating only what comes after it (vLLM
    /// `continue_final_message`, llama.cpp assistant prefill). Default: no.
    fn resumes(&self) -> bool {
        false
    }

    async fn infer(&self, request: &Request, out: &Output, cancel: &CancelToken) -> Result<Finish, RemoteError>;
}

/// What serves embeddings.
#[async_trait]
pub trait EmbedBackend: Send + Sync {
    fn name(&self) -> String;

    async fn embed(&self, input: &str, cancel: &CancelToken) -> Result<Vec<f32>, RemoteError>;

    /// The vector and the prompt tokens it took, if the backend knows them. The server sums the
    /// counts into the reply's [`Finish::usage`]. Default: [`embed`](Self::embed), no count.
    async fn embed_counted(&self, input: &str, cancel: &CancelToken) -> Result<(Vec<f32>, Option<u64>), RemoteError> {
        Ok((self.embed(input, cancel).await?, None))
    }
}

/// An [`InferBackend`] over a [`ModelBackend`]: inline messages and tools only, no resume;
/// sampling settings, seed and response format are left to the model's defaults.
pub struct ModelAdapter {
    backend: Arc<dyn ModelBackend>,
}

impl ModelAdapter {
    pub fn new(backend: Arc<dyn ModelBackend>) -> Self {
        Self { backend }
    }
}

#[async_trait]
impl InferBackend for ModelAdapter {
    fn name(&self) -> String {
        self.backend.name()
    }

    async fn infer(&self, request: &Request, out: &Output, cancel: &CancelToken) -> Result<Finish, RemoteError> {
        let messages = match &request.input {
            Input::Messages(m) => m,
            Input::Prompt(_) => return Err(RemoteError::Unsupported { message: "this backend takes messages, not a raw prompt".into() }),
            _ => return Err(RemoteError::unavailable("this node keeps no conversations; send the messages")),
        };
        let (tx, mut rx) = mpsc::unbounded_channel();
        let sink = DeltaSink::new(Some(tx));
        let forward = |ev: AgentEvent| match ev {
            AgentEvent::TextDelta(t) => out.text(t),
            AgentEvent::ReasoningDelta(t) => out.reasoning(t),
            _ => {}
        };
        let mut run = pin!(self.backend.complete(messages, &request.tools, &sink, cancel));
        let turn = loop {
            tokio::select! {
                r = &mut run => break r,
                Some(ev) = rx.recv() => forward(ev),
            }
        };
        while let Ok(ev) = rx.try_recv() {
            forward(ev);
        }
        let turn = turn.map_err(model_error)?;
        let calls = !turn.tool_calls.is_empty();
        for call in turn.tool_calls {
            out.tool_call(call);
        }
        Ok(Finish::new(if calls { "tool_calls" } else { "stop" }))
    }
}

fn is_cancel(bytes: &[u8], id: &str) -> bool {
    matches!(crate::remote::from_cbor::<ClientFrame>(bytes), Ok(ClientFrame::Cancel { id: c }) if c == id)
}

fn millis(d: Duration) -> u64 {
    d.as_millis().min(u64::MAX as u128) as u64
}

enum Waited {
    Go(super::queue::Permit),
    /// The asker cancelled or went away while waiting; the reply (if any) has been sent.
    Gone,
}

/// Waits for a slot, telling the asker its place.
async fn wait_turn(ch: &mut Channel, id: &str, admission: &Admission, priority: Priority) -> Result<Waited, RemoteError> {
    let mut ticket = match admission.enter(priority) {
        Ok(t) => t,
        Err(error) => {
            ch.send_cbor(&ServerFrame::Failed { id: id.to_string(), error }).await?;
            return Ok(Waited::Gone);
        }
    };
    let mut last = None;
    loop {
        tokio::select! {
            turn = ticket.turn(last, QUEUE_KEEPALIVE) => match turn {
                Turn::Go(permit) => return Ok(Waited::Go(permit)),
                Turn::Waiting(position) => {
                    last = Some(position);
                    ch.send_cbor(&ServerFrame::Queued { id: id.to_string(), position }).await?;
                }
            },
            m = ch.rx.recv() => match m {
                Ok(Some(bytes)) if is_cancel(&bytes, id) => {
                    ch.send_cbor(&ServerFrame::Cancelled { id: id.to_string() }).await?;
                    return Ok(Waited::Gone);
                }
                Ok(Some(_)) => {}
                Ok(None) | Err(_) => return Ok(Waited::Gone),
            },
        }
    }
}

/// Serve one infer/2 stream. `caller` is the transport-authenticated peer (plus a certificate
/// if the transport carries one; otherwise the one the request presents is attached). The host
/// authorizes the caller before anything else. Returns when the request is done, failed or
/// cancelled; if the asker goes away it is cancelled.
pub async fn serve(mut ch: Channel, caller: Caller, host: &dyn InferHost, admission: &Admission) -> Result<(), RemoteError> {
    match ch.recv_cbor::<ClientFrame>().await? {
        Some(ClientFrame::Infer(request)) => serve_infer(ch, caller, host, admission, request).await,
        Some(ClientFrame::Embed(request)) => serve_embed(ch, caller, host, admission, request).await,
        Some(ClientFrame::Cancel { .. }) | None => Ok(()),
    }
}

async fn serve_infer(mut ch: Channel, caller: Caller, host: &dyn InferHost, admission: &Admission, mut request: Request) -> Result<(), RemoteError> {
    let started = Instant::now();
    let id = request.id.clone();
    let caller = caller.presenting(request.cert.take());
    let checked = host.authorize(&caller).and_then(|()| {
        if matches!(request.input, Input::Prompt(_)) && !request.tools.is_empty() {
            return Err(RemoteError::BadRequest { message: "a raw prompt takes no tools".into() });
        }
        if request.raw && matches!(request.input, Input::Context(_)) {
            return Err(RemoteError::BadRequest { message: "raw mode takes assembled messages, not a context_ref".into() });
        }
        if request.assistant_prefix.as_ref().is_some_and(|p| !p.is_consistent()) {
            return Err(RemoteError::BadRequest { message: "assistant_prefix lengths do not match its text".into() });
        }
        host.open_request(&caller, &request)
    });
    let backend = match checked {
        Ok(b) => b,
        Err(error) => return ch.send_cbor(&ServerFrame::Failed { id, error }).await,
    };
    let _permit = match wait_turn(&mut ch, &id, admission, request.priority).await? {
        Waited::Go(p) => p,
        Waited::Gone => return Ok(()),
    };
    let queued = started.elapsed();
    let resume = backend.resumes();
    let prefix = request.assistant_prefix.clone().filter(|_| resume);
    if !resume {
        request.assistant_prefix = None;
    }
    let (text_at, reasoning_at) = prefix.as_ref().map(|p| (p.len, p.reasoning_len)).unwrap_or((0, 0));
    ch.send_cbor(&ServerFrame::Accepted { id: id.clone(), model: backend.name(), attempt: request.attempt, resume }).await?;
    let accepted = Instant::now();

    let (out, mut pieces) = Output::channel();
    let cancel = CancelToken::new();
    let mut framer =
        Framer { id: id.clone(), text_at, reasoning_at, calls: 0, accepted, first_token: None, early_meta: request.early_meta, meta: None };
    let mut run = pin!(backend.infer(&request, &out, &cancel));
    let mut asker_open = true;
    let mut send_failed = false;
    let result = loop {
        tokio::select! {
            r = &mut run => break r,
            Some(p) = pieces.recv() => {
                let Some(f) = framer.frame(p) else { continue };
                if !send_failed && ch.send_cbor(&f).await.is_err() {
                    send_failed = true;
                    cancel.cancel();
                }
            }
            m = ch.rx.recv(), if asker_open => match m {
                Ok(Some(bytes)) => {
                    if is_cancel(&bytes, &id) {
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
    while let Ok(p) = pieces.try_recv() {
        if let Some(f) = framer.frame(p) {
            ch.send_cbor(&f).await?;
        }
    }
    let reply = match result {
        Ok(mut finish) => {
            let meta = &mut finish.meta;
            if let Some(early) = framer.meta.take() {
                meta.cache_hit |= early.cache_hit;
                if meta.served_by.is_none() {
                    meta.served_by = early.served_by;
                }
                if meta.revision.is_none() {
                    meta.revision = early.revision;
                }
                if meta.standin.is_none() {
                    meta.standin = early.standin;
                }
            }
            meta.served_by.get_or_insert_with(|| backend.name());
            if let Some(p) = &prefix {
                meta.resumed_from = Some(p.len);
            }
            meta.timings.queued_ms = millis(queued);
            meta.timings.first_token_ms = framer.first_token.map(millis);
            meta.timings.total_ms = millis(started.elapsed());
            ServerFrame::Done { id, finish }
        }
        Err(_) if cancel.is_cancelled() => ServerFrame::Cancelled { id },
        Err(error) => ServerFrame::Failed { id, error },
    };
    ch.send_cbor(&reply).await
}

/// Turns backend output into frames with offsets and tool-call indexes.
struct Framer {
    id: String,
    text_at: u64,
    reasoning_at: u64,
    calls: u32,
    accepted: Instant,
    first_token: Option<Duration>,
    /// The asker reads `Meta` frames.
    early_meta: bool,
    /// Early metadata from the backend, folded into the final `Done`.
    meta: Option<Meta>,
}

impl Framer {
    fn frame(&mut self, p: Piece) -> Option<ServerFrame> {
        let id = self.id.clone();
        Some(match p {
            Piece::Text(text) => {
                self.first_token.get_or_insert_with(|| self.accepted.elapsed());
                let offset = self.text_at;
                self.text_at += chars(&text);
                ServerFrame::Delta { id, offset, text }
            }
            Piece::Reasoning(text) => {
                self.first_token.get_or_insert_with(|| self.accepted.elapsed());
                let offset = self.reasoning_at;
                self.reasoning_at += chars(&text);
                ServerFrame::Reasoning { id, offset, text }
            }
            Piece::ToolCall(call) => {
                self.calls += 1;
                ServerFrame::ToolCall { id, index: self.calls - 1, call }
            }
            Piece::Usage(usage) => ServerFrame::Usage { id, usage },
            Piece::Meta(meta) => {
                self.meta = Some(meta.clone());
                if !self.early_meta {
                    return None;
                }
                ServerFrame::Meta { id, meta }
            }
        })
    }
}

async fn serve_embed(
    mut ch: Channel,
    caller: Caller,
    host: &dyn InferHost,
    admission: &Admission,
    mut request: EmbedRequest,
) -> Result<(), RemoteError> {
    let started = Instant::now();
    let id = request.id.clone();
    let caller = caller.presenting(request.cert.take());
    let backend = match host.authorize(&caller).and_then(|()| host.open_embed(&caller, request.model.as_deref())) {
        Ok(b) => b,
        Err(error) => return ch.send_cbor(&ServerFrame::Failed { id, error }).await,
    };
    let _permit = match wait_turn(&mut ch, &id, admission, request.priority).await? {
        Waited::Go(p) => p,
        Waited::Gone => return Ok(()),
    };
    let queued = started.elapsed();
    ch.send_cbor(&ServerFrame::Accepted { id: id.clone(), model: backend.name(), attempt: 0, resume: false }).await?;
    let cancel = CancelToken::new();
    let mut tokens: Option<u64> = None;
    for (index, input) in request.inputs.iter().enumerate() {
        let vector = tokio::select! {
            v = backend.embed_counted(input, &cancel) => v,
            m = async { loop {
                match ch.rx.recv().await {
                    Ok(Some(bytes)) if is_cancel(&bytes, &id) => return true,
                    Ok(Some(_)) => {}
                    Ok(None) | Err(_) => return false,
                }
            } } => {
                cancel.cancel();
                return if m { ch.send_cbor(&ServerFrame::Cancelled { id }).await } else { Ok(()) };
            }
        };
        match vector {
            Ok((v, n)) => {
                if let Some(n) = n {
                    *tokens.get_or_insert(0) += n;
                }
                ch.send_cbor(&ServerFrame::Embedding { id: id.clone(), index: index as u32, vector: f32_bytes(&v) }).await?
            }
            Err(error) => return ch.send_cbor(&ServerFrame::Failed { id, error }).await,
        }
    }
    let mut finish = Finish::new("stop");
    finish.usage = tokens.map(|n| Usage::new(n, 0));
    finish.meta.served_by = Some(backend.name());
    finish.meta.timings.queued_ms = millis(queued);
    finish.meta.timings.total_ms = millis(started.elapsed());
    ch.send_cbor(&ServerFrame::Done { id, finish }).await
}
