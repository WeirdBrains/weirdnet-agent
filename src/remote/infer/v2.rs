//! `/weirdnet-agent/infer/2`: remote inference with a queue, jobs, resume and richer frames.
//!
//! ```text
//! asking node                               serving node
//!   Infer(Request) | Embed(EmbedRequest) | Transcribe(TranscribeRequest) | Speak(SpeakRequest) ->
//!                                           <- Queued { position } ...   (while waiting for a slot)
//!                                           <- Accepted { model, attempt, resume }
//!                                           <- Delta { offset, text } | Reasoning { offset, text }
//!                                           <- Meta { meta }             (early_meta askers)
//!                                           <- ToolCall { index, call } | Usage | Embedding | Transcript ...
//!                                           <- Working ... | SpeechStart { format, .. } | Audio { offset, data } ...
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
//! - **Continue:** the same prefix continues an answer the user stopped or that was cut off
//!   (assistant prefill; [`Request::continuing`] makes one from a trailing assistant message).
//!   The wire format is unchanged, so any 0.4 server whose backend resumes can serve it.
//! - **Tool calls** are sent whole, never half-built.
//! - **Raw prompts** ([`Input::Prompt`]) are for completions: the model continues the text, with
//!   no chat template and no tools.
//! - **Early metadata:** an asker that sets [`Request::early_meta`] may get a `Meta` frame before
//!   the answer (a cache hit, say); `Done` carries the same metadata for everyone.
//! - **Raw mode** is for inference node → model node calls: assembled messages only, no context
//!   lookups, caches or stand-ins.
//! - **Answer cache:** [`Request::cache`] ([`CacheMode`]) lets an asker skip a node's answer
//!   cache for one request without raw mode: `bypass` neither reads nor stores, `refresh` skips
//!   the read and stores the fresh answer. The field is left off the wire when it is `use`, so
//!   older nodes read the request as before; they don't know the field and may still answer from
//!   their cache ([`Meta::cache_hit`] says so).
//! - **Embeddings:** one `Embedding` frame per input, the vector as a CBOR byte string of
//!   little-endian `f32`.
//! - **Transcription** (since 0.5.8): a [`TranscribeRequest`] carries one audio clip as a CBOR
//!   byte string with its format (`wav`, `mp3`, ... as in an `input_audio` part), optional
//!   language and prompt hints and whether timed segments are wanted. It queues like any request;
//!   the node answers `Accepted`, `Working` every [`QUEUE_KEEPALIVE`] while the backend runs, one
//!   `Transcript` frame ([`Transcript`]: the text, the language,
//!   the audio's length and timed [`Segment`]s when the backend gives them), then `Done` with
//!   usage and timings. The host serves it through [`InferHost::open_transcribe`] and a
//!   [`TranscribeBackend`]; an inference node routes it with
//!   [`Catalog::route_transcribe`](crate::remote::catalog::Catalog::route_transcribe). Nodes
//!   before 0.5.8 can't read the request and close the stream; the pool reports that as
//!   `Unsupported` and tries the next node. The whole request must fit in one stream message
//!   (32 MiB on Weirdnet), so long soundtracks are sent in pieces.
//! - **Video** (since 0.5.10): a message may carry a whole clip as a
//!   [`ContentPart::VideoUrl`](crate::model::ContentPart::VideoUrl) (a `data:video/...` URL, the
//!   `video_url` part in CBOR as in JSON). It needs [`Modalities::VIDEO`]; the pool routes it with
//!   [`Catalog::route`](crate::remote::catalog::Catalog::route) only to models whose catalog
//!   entry says `inputs.video`. A node before 0.5.10 can't read such a request and closes the
//!   stream, which the pool treats like any dropped node. The clip travels inside the request, so
//!   it must fit in one stream message with everything else.
//! - **Speech out** (since 0.5.11): a [`SpeakRequest`] carries text ([`SpeakInput::Text`]), a
//!   multi-speaker script ([`SpeakInput::Script`]) or raw engine input ([`SpeakInput::Raw`]),
//!   an optional [`Voice`] (a preset, a designed voice, or a voice cloned from a reference clip
//!   sent as a CBOR byte string, with `consent`), per-speaker voices, instructions, language,
//!   speed (per mille), seed, format ([`AudioFormat`]), sample rate and whether to stream. The
//!   node answers `Accepted`, `Working` every [`QUEUE_KEEPALIVE`] until the audio starts, one
//!   `SpeechStart` (format, and sample rate and channels when known), `Audio` frames of at most
//!   [`MAX_AUDIO_CHUNK`] bytes in order (each with its byte offset), then `Done` with usage
//!   ([`Usage::input_chars`], [`Usage::audio_ms`]) and metadata ([`Meta::ignored`] names fields
//!   the model doesn't use). [`SpeakRequest::check`] refuses empty input, a clone without
//!   consent and a reference over [`MAX_REFERENCE_BYTES`] before the host opens a backend
//!   ([`InferHost::open_speak`], a [`SpeakBackend`] returning an [`AudioStream`]); an inference
//!   node routes with [`Catalog::route_speak`](crate::remote::catalog::Catalog::route_speak).
//!   The pool fails over to another node only before `SpeechStart`: once audio has left, a
//!   failure is final (another node would speak with another voice draw). Nodes before 0.5.11
//!   can't read the request and close the stream, which the pool treats as `Unsupported`.

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
use crate::model::{DeltaSink, Message, Modalities, ModelBackend, Role, ToolCall};
use crate::remote::{Caller, Channel, RemoteError, bytes, request_id};
use crate::session::AgentEvent;
use crate::speech::{AudioFormat, SpeechNeeds};
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

/// Whether a node's answer cache may serve or keep this request's answer.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum CacheMode {
    /// Read and store as the cache sees fit.
    #[default]
    Use,
    /// Neither read nor store: always run the model (evals, fresh weights).
    Bypass,
    /// Skip the read, run the model and store the fresh answer over the old one.
    Refresh,
}

impl CacheMode {
    pub fn is_use(&self) -> bool {
        *self == CacheMode::Use
    }

    /// A stored answer may stand in for the model.
    pub fn may_read(&self) -> bool {
        *self == CacheMode::Use
    }

    /// The finished answer may be kept.
    pub fn may_store(&self) -> bool {
        matches!(self, CacheMode::Use | CacheMode::Refresh)
    }
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
    /// Answer cache use (0.5.1 and later; see [`CacheMode`]). Not sent when `use`; nodes that
    /// predate it ignore it.
    #[serde(default, skip_serializing_if = "CacheMode::is_use")]
    pub cache: CacheMode,
    /// How strongly a LoRA adapter applies for this request, when `model` names one on an engine
    /// that scales adapters per request (llama.cpp). `None`: the adapter's configured scale.
    /// Ignored for base models. Not sent when `None`; nodes that predate it (0.5.1 and earlier)
    /// ignore it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub adapter_scale: Option<f32>,
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

    /// Continue the last assistant message: a trailing assistant message with no tool calls
    /// moves into `assistant_prefix` (prefill), so the model carries on from its text. Only
    /// nodes that say `resume: true` take such a request; others answer `Unsupported`. A
    /// request that ends any other way is returned unchanged.
    pub fn continuing(mut self) -> Self {
        if let Input::Messages(m) = &mut self.input
            && m.last().is_some_and(|last| last.role == Role::Assistant && last.tool_calls.is_empty() && last.content.as_text().is_some())
            && let Some(last) = m.pop()
        {
            self.assistant_prefix = Some(Prefix::new(last.content.into_text(), ""));
        }
        self
    }

    pub fn with_cert(mut self, cert: Vec<u8>) -> Self {
        self.cert = Some(cert);
        self
    }

    /// How the serving node's answer cache treats this request (see [`CacheMode`]).
    pub fn with_cache(mut self, cache: CacheMode) -> Self {
        self.cache = cache;
        self
    }

    /// Applies the requested LoRA adapter at `scale` for this request (see
    /// [`Request::adapter_scale`]).
    pub fn with_adapter_scale(mut self, scale: f32) -> Self {
        self.adapter_scale = Some(scale);
        self
    }

    /// Wait for the requested model to load rather than take a stand-in.
    pub fn with_wait_for_model(mut self, wait: bool) -> Self {
        self.wait_for_model = wait;
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

    /// What a model needs to read the inline messages: [`Modalities::TEXT`] unless some carry
    /// images, audio or video. Context references and raw prompts are text.
    pub fn needs(&self) -> Modalities {
        self.inline_messages().map(crate::model::needs).unwrap_or(Modalities::TEXT)
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

/// Speech to text for one audio clip (since 0.5.8). The audio travels as raw bytes (a CBOR byte
/// string), not base64.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct TranscribeRequest {
    pub id: String,
    /// A speech-to-text model by the serving app's id (`None`: the node picks its transcriber).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// The encoded audio.
    #[serde(with = "bytes")]
    pub audio: Vec<u8>,
    /// Its format, as in an `input_audio` part: `wav`, `mp3`, `flac`, `ogg`, ...
    pub format: String,
    /// The spoken language (ISO 639-1, such as `en`), if the asker knows it; otherwise the model
    /// detects it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    /// Text to guide the transcript (names, terms, the previous piece's ending), as OpenAI's
    /// `prompt`. Backends that take none ignore it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt: Option<String>,
    /// Ask for timed [`Segment`]s. Backends that can't time the text return none.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub timestamps: bool,
    #[serde(default)]
    pub priority: Priority,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "bytes::opt")]
    pub cert: Option<Vec<u8>>,
}

impl TranscribeRequest {
    /// `audio` encoded as `format` (`wav`, `mp3`, ...).
    pub fn new(audio: Vec<u8>, format: impl Into<String>) -> Self {
        Self { id: request_id(), audio, format: format.into(), ..Default::default() }
    }

    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = Some(model.into());
        self
    }

    /// The spoken language (ISO 639-1, such as `en`).
    pub fn with_language(mut self, language: impl Into<String>) -> Self {
        self.language = Some(language.into());
        self
    }

    /// Text to guide the transcript.
    pub fn with_prompt(mut self, prompt: impl Into<String>) -> Self {
        self.prompt = Some(prompt.into());
        self
    }

    /// Ask for timed segments.
    pub fn with_timestamps(mut self, timestamps: bool) -> Self {
        self.timestamps = timestamps;
        self
    }

    pub fn with_priority(mut self, priority: Priority) -> Self {
        self.priority = priority;
        self
    }

    /// `BadRequest` unless there is audio and a format. The server checks this before the host
    /// opens a backend.
    pub fn check(&self) -> Result<(), RemoteError> {
        if self.audio.is_empty() {
            return Err(RemoteError::BadRequest { message: "a transcription request needs audio".into() });
        }
        if self.format.trim().is_empty() {
            return Err(RemoteError::BadRequest { message: "a transcription request needs the audio format (wav, mp3, ...)".into() });
        }
        Ok(())
    }
}

/// A transcript: the text, and what the backend knows about it.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Transcript {
    /// The whole text.
    pub text: String,
    /// The spoken language (ISO 639-1), when the backend says.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    /// The audio's length in milliseconds, when the backend says.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    /// Timed pieces of the text, in order, when asked for and the backend can time them.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub segments: Vec<Segment>,
}

impl Transcript {
    pub fn new(text: impl Into<String>) -> Self {
        Self { text: text.into(), ..Default::default() }
    }

    pub fn with_language(mut self, language: impl Into<String>) -> Self {
        self.language = Some(language.into());
        self
    }

    pub fn with_duration_ms(mut self, duration_ms: u64) -> Self {
        self.duration_ms = Some(duration_ms);
        self
    }

    pub fn with_segments(mut self, segments: Vec<Segment>) -> Self {
        self.segments = segments;
        self
    }
}

/// A timed piece of a [`Transcript`], in milliseconds from the start of the audio.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Segment {
    pub start_ms: u64,
    pub end_ms: u64,
    pub text: String,
}

impl Segment {
    pub fn new(start_ms: u64, end_ms: u64, text: impl Into<String>) -> Self {
        Self { start_ms, end_ms, text: text.into() }
    }
}

/// Largest reference clip a [`SpeakRequest`] may carry (30 MiB), checked before any backend
/// opens. A model's [`SpeechProfile::max_reference_bytes`](crate::speech::SpeechProfile::max_reference_bytes) may be smaller. Since 0.5.11.
pub const MAX_REFERENCE_BYTES: usize = 30 << 20;

/// Largest [`ServerFrame::Audio`] chunk (256 KiB); the server splits bigger pieces. Since 0.5.11.
pub const MAX_AUDIO_CHUNK: usize = 256 << 10;

/// What a [`SpeakRequest`] speaks (since 0.5.11).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum SpeakInput {
    /// Plain text in the model's chat-free speech template.
    Text(String),
    /// A multi-speaker script, turn by turn (needs [`SpeechProfile::script_speakers`](crate::speech::SpeechProfile::script_speakers) above the
    /// highest speaker number).
    Script(Vec<ScriptTurn>),
    /// Passed to the engine as is, with no template (phonemes for Kokoro, say; needs
    /// [`SpeechProfile::raw`](crate::speech::SpeechProfile::raw)).
    Raw(String),
}

impl Default for SpeakInput {
    fn default() -> Self {
        SpeakInput::Text(String::new())
    }
}

impl SpeakInput {
    /// Characters (Unicode scalar values) of input: the text, or the turns' text.
    pub fn chars(&self) -> u64 {
        match self {
            SpeakInput::Text(t) | SpeakInput::Raw(t) => chars(t),
            SpeakInput::Script(turns) => turns.iter().map(|t| chars(&t.text)).sum(),
        }
    }

    /// The highest speaker number of a script; `None` for text or raw input.
    pub fn highest_speaker(&self) -> Option<u8> {
        match self {
            SpeakInput::Script(turns) => turns.iter().map(|t| t.speaker).max(),
            _ => None,
        }
    }
}

/// One turn of a multi-speaker script (since 0.5.11).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ScriptTurn {
    /// The speaker number, from 0.
    pub speaker: u8,
    pub text: String,
}

impl ScriptTurn {
    pub fn new(speaker: u8, text: impl Into<String>) -> Self {
        Self { speaker, text: text.into() }
    }
}

/// The voice to speak with (since 0.5.11). Absent from a request: the model's default voice.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Voice {
    /// One of the model's preset voices ([`SpeechProfile::presets`](crate::speech::SpeechProfile::presets)); any other name is refused
    /// with the list, never mapped to another voice.
    Preset { name: String },
    /// A voice designed from a description (needs [`SpeechProfile::design`](crate::speech::SpeechProfile::design)).
    Design { description: String },
    /// A voice cloned from a reference clip (needs [`SpeechProfile::clone`](crate::speech::SpeechProfile::clone)). The clip travels
    /// as a CBOR byte string, lives in memory for this request only and is never stored or
    /// logged. `consent` must be true: the asker confirms the voice is theirs or they have
    /// permission to use it.
    Clone {
        #[serde(with = "bytes")]
        audio: Vec<u8>,
        /// The clip's format, as in an `input_audio` part: `wav`, `mp3`, `flac`, ...
        format: String,
        /// What the clip says, for models that use it ([`CloneMode`](crate::speech::CloneMode)).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        text: Option<String>,
        #[serde(default)]
        consent: bool,
    },
}

impl Voice {
    pub fn preset(name: impl Into<String>) -> Self {
        Voice::Preset { name: name.into() }
    }

    pub fn design(description: impl Into<String>) -> Self {
        Voice::Design { description: description.into() }
    }

    /// A cloned voice from `audio` in `format`, with `consent` as the asker gave it.
    pub fn clone_from(audio: Vec<u8>, format: impl Into<String>, text: Option<String>, consent: bool) -> Self {
        Voice::Clone { audio, format: format.into(), text, consent }
    }
}

/// The voice for one speaker of a script (since 0.5.11).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SpeakerVoice {
    pub speaker: u8,
    pub voice: Voice,
}

impl SpeakerVoice {
    pub fn new(speaker: u8, voice: Voice) -> Self {
        Self { speaker, voice }
    }
}

/// Text to speech (infer/2 `Speak`, since 0.5.11): text or a script, the voice, and how the audio
/// should come back. Nothing in it is cached, logged or stored by a node.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SpeakRequest {
    pub id: String,
    /// A speaking model by the serving app's id (`None`: the node picks a speaker that covers
    /// the request).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    pub input: SpeakInput,
    /// The voice (for a script: the voice of speakers without their own). `None`: the model's
    /// default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub voice: Option<Voice>,
    /// Per-speaker voices for a script, at most one per speaker.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub voices: Vec<SpeakerVoice>,
    /// Style or emotion for the chosen voice (needs [`SpeechProfile::instructions`](crate::speech::SpeechProfile::instructions)).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
    /// The language (ISO 639-1 or one of the engine's codes in [`SpeechProfile::languages`](crate::speech::SpeechProfile::languages)).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    /// Speaking speed in per mille of the model's pace (1000 = as is, 1200 = 20 % faster; needs
    /// [`SpeechProfile::speed`](crate::speech::SpeechProfile::speed)). Per mille so the request keeps `Eq`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub speed: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seed: Option<u64>,
    /// The output format (`None`: the model's default, the first of
    /// [`SpeechProfile::formats`](crate::speech::SpeechProfile::formats)).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<AudioFormat>,
    /// The output sample rate in Hz (`None`: the model's own).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sample_rate: Option<u32>,
    /// Send audio as the engine makes it (when it streams); otherwise the finished file comes in
    /// chunks.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub stream: bool,
    #[serde(default)]
    pub priority: Priority,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "bytes::opt")]
    pub cert: Option<Vec<u8>>,
}

impl SpeakRequest {
    /// Speak `text` in the model's default voice.
    pub fn text(text: impl Into<String>) -> Self {
        Self::with_input(SpeakInput::Text(text.into()))
    }

    /// Speak a multi-speaker script.
    pub fn script(turns: Vec<ScriptTurn>) -> Self {
        Self::with_input(SpeakInput::Script(turns))
    }

    /// Pass `input` to the engine with no template ([`SpeakInput::Raw`]).
    pub fn raw(input: impl Into<String>) -> Self {
        Self::with_input(SpeakInput::Raw(input.into()))
    }

    /// A request for `input` with a fresh id.
    pub fn with_input(input: SpeakInput) -> Self {
        Self { id: request_id(), input, ..Default::default() }
    }

    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = Some(model.into());
        self
    }

    pub fn with_voice(mut self, voice: Voice) -> Self {
        self.voice = Some(voice);
        self
    }

    /// The voice of one speaker of a script (replacing one set before for that speaker).
    pub fn with_speaker_voice(mut self, speaker: u8, voice: Voice) -> Self {
        self.voices.retain(|v| v.speaker != speaker);
        self.voices.push(SpeakerVoice::new(speaker, voice));
        self
    }

    pub fn with_instructions(mut self, instructions: impl Into<String>) -> Self {
        self.instructions = Some(instructions.into());
        self
    }

    pub fn with_language(mut self, language: impl Into<String>) -> Self {
        self.language = Some(language.into());
        self
    }

    /// Speed in per mille of the model's pace (1000 = as is).
    pub fn with_speed(mut self, per_mille: u32) -> Self {
        self.speed = Some(per_mille);
        self
    }

    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = Some(seed);
        self
    }

    pub fn with_format(mut self, format: AudioFormat) -> Self {
        self.format = Some(format);
        self
    }

    pub fn with_sample_rate(mut self, hz: u32) -> Self {
        self.sample_rate = Some(hz);
        self
    }

    pub fn with_stream(mut self, stream: bool) -> Self {
        self.stream = stream;
        self
    }

    pub fn with_priority(mut self, priority: Priority) -> Self {
        self.priority = priority;
        self
    }

    pub fn with_cert(mut self, cert: Vec<u8>) -> Self {
        self.cert = Some(cert);
        self
    }

    /// Every voice in the request: the voice, then the per-speaker voices.
    pub fn all_voices(&self) -> impl Iterator<Item = &Voice> {
        self.voice.iter().chain(self.voices.iter().map(|v| &v.voice))
    }

    /// Whether any voice is cloned from a reference clip.
    pub fn clones(&self) -> bool {
        self.all_voices().any(|v| matches!(v, Voice::Clone { .. }))
    }

    /// `BadRequest` for a request no model could serve: empty input (or an empty script turn),
    /// per-speaker voices without a script or twice for one speaker, an empty preset name,
    /// description or reference format, a reference clip that is empty, over
    /// [`MAX_REFERENCE_BYTES`] or without `consent`, or a speed of 0. The server checks this
    /// before the host opens a backend; the backend then checks the request against its profile.
    pub fn check(&self) -> Result<(), RemoteError> {
        let bad = |m: &str| Err(RemoteError::BadRequest { message: m.to_string() });
        match &self.input {
            SpeakInput::Text(t) | SpeakInput::Raw(t) if t.trim().is_empty() => return bad("a speech request needs text to speak"),
            SpeakInput::Script(turns) if turns.is_empty() => return bad("a speech script needs at least one turn"),
            SpeakInput::Script(turns) if turns.iter().any(|t| t.text.trim().is_empty()) => return bad("every turn of a speech script needs text"),
            _ => {}
        }
        if !self.voices.is_empty() && !matches!(self.input, SpeakInput::Script(_)) {
            return bad("per-speaker voices need a script input");
        }
        let mut seen = std::collections::BTreeSet::new();
        if let Some(v) = self.voices.iter().find(|v| !seen.insert(v.speaker)) {
            return Err(RemoteError::BadRequest { message: format!("speaker {} has more than one voice", v.speaker) });
        }
        for voice in self.all_voices() {
            match voice {
                Voice::Preset { name } if name.trim().is_empty() => return bad("a preset voice needs a name"),
                Voice::Design { description } if description.trim().is_empty() => return bad("a designed voice needs a description"),
                Voice::Clone { audio, format, consent, .. } => {
                    if !consent {
                        return bad("cloning a voice needs consent: confirm the voice is yours or you have permission to use it");
                    }
                    if audio.is_empty() {
                        return bad("cloning a voice needs a reference clip");
                    }
                    if audio.len() > MAX_REFERENCE_BYTES {
                        return Err(RemoteError::BadRequest {
                            message: format!("the reference clip is {} bytes, over the {MAX_REFERENCE_BYTES}-byte limit", audio.len()),
                        });
                    }
                    if format.trim().is_empty() {
                        return bad("a reference clip needs its format (wav, mp3, ...)");
                    }
                }
                _ => {}
            }
        }
        if self.speed == Some(0) {
            return bad("a speaking speed of 0 is not a speed (1000 is the model's pace)");
        }
        if self.sample_rate == Some(0) {
            return bad("a sample rate of 0 Hz is not a rate");
        }
        Ok(())
    }

    /// What the request needs from a model's [`SpeechProfile`](crate::speech::SpeechProfile), for
    /// [`SpeechProfile::covers`](crate::speech::SpeechProfile::covers) and
    /// [`Catalog::route_speak`](crate::remote::catalog::Catalog::route_speak). A reference
    /// clip's length is read from WAV headers; other formats leave it to the engine.
    pub fn needs(&self) -> SpeechNeeds {
        let mut n = SpeechNeeds::new().with_input_chars(self.input.chars().min(u64::from(u32::MAX)) as u32);
        for voice in self.all_voices() {
            n = match voice {
                Voice::Preset { name } => n.with_preset(name.clone()),
                Voice::Design { .. } => n.with_design(),
                Voice::Clone { audio, format, text, .. } => {
                    let ms = if format.eq_ignore_ascii_case("wav") { crate::speech::wav_duration_ms(audio) } else { None };
                    n.with_clone(text.as_deref().is_some_and(|t| !t.trim().is_empty())).with_reference(audio.len() as u64, ms)
                }
            };
        }
        if let Some(h) = self.input.highest_speaker() {
            n = n.with_script(h);
        }
        if matches!(self.input, SpeakInput::Raw(_)) {
            n = n.with_raw();
        }
        if self.instructions.as_deref().is_some_and(|i| !i.trim().is_empty()) {
            n = n.with_instructions();
        }
        if self.speed.is_some_and(|s| s != 1000) {
            n = n.with_speed();
        }
        if let Some(f) = self.format {
            n = n.with_format(f);
        }
        n
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
    /// Characters of input spoken ([`SpeakRequest`]). Since 0.5.11; left off the wire when 0.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub input_chars: u64,
    /// Milliseconds of audio produced ([`SpeakRequest`]). Since 0.5.11; left off the wire when 0.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub audio_ms: u64,
}

fn is_zero(n: &u64) -> bool {
    *n == 0
}

impl Usage {
    pub fn new(prompt_tokens: u64, completion_tokens: u64) -> Self {
        Self { prompt_tokens, completion_tokens, ..Default::default() }
    }

    /// Speech usage: characters spoken and milliseconds of audio made (since 0.5.11).
    pub fn speech(input_chars: u64, audio_ms: u64) -> Self {
        Self { input_chars, audio_ms, ..Default::default() }
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
    /// Request fields the model received but does not use (such as `reference_text` for a model
    /// that clones from the clip alone), named rather than refused because sending them is
    /// harmless. Since 0.5.11; left off the wire when empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ignored: Vec<String>,
}

impl Meta {
    /// Names a request field the model received but does not use ([`Meta::ignored`]), once.
    /// Since 0.5.11.
    pub fn with_ignored(mut self, field: impl Into<String>) -> Self {
        let field = field.into();
        if !self.ignored.contains(&field) {
            self.ignored.push(field);
        }
        self
    }
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
    Cancel {
        id: String,
    },
    /// Speech to text (since 0.5.8).
    Transcribe(TranscribeRequest),
    /// Text to speech (since 0.5.11).
    Speak(SpeakRequest),
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
    /// The answer to a [`TranscribeRequest`], before `Done` (since 0.5.8).
    Transcript {
        id: String,
        transcript: Transcript,
    },
    /// Still at work on an accepted request that streams nothing until it is done
    /// (transcription): sent every [`QUEUE_KEEPALIVE`] so the asker doesn't take the quiet for a
    /// stall (since 0.5.8).
    Working {
        id: String,
    },
    /// The audio of a [`SpeakRequest`] begins (since 0.5.11): sent once, before the first
    /// `Audio`. The sample rate and channel count are absent when the engine did not say.
    SpeechStart {
        id: String,
        format: AudioFormat,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sample_rate: Option<u32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        channels: Option<u16>,
    },
    /// A piece of the audio (since 0.5.11): at most [`MAX_AUDIO_CHUNK`] bytes, at byte `offset`
    /// of the whole file, in order.
    Audio {
        id: String,
        offset: u64,
        #[serde(with = "bytes")]
        data: Vec<u8>,
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
            | ServerFrame::Transcript { id, .. }
            | ServerFrame::Working { id }
            | ServerFrame::SpeechStart { id, .. }
            | ServerFrame::Audio { id, .. }
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

/// What serves transcription (since 0.5.8): a speech-to-text model on this node, or an inference
/// node's routing to one.
#[async_trait]
pub trait TranscribeBackend: Send + Sync {
    fn name(&self) -> String;

    /// The transcript of `request.audio`. Poll `cancel`.
    async fn transcribe(&self, request: &TranscribeRequest, cancel: &CancelToken) -> Result<Transcript, RemoteError>;

    /// The transcript and token counts, if the backend knows them; they go into the reply's
    /// [`Finish::usage`]. Default: [`transcribe`](Self::transcribe), no counts.
    async fn transcribe_counted(&self, request: &TranscribeRequest, cancel: &CancelToken) -> Result<(Transcript, Option<Usage>), RemoteError> {
        Ok((self.transcribe(request, cancel).await?, None))
    }
}

/// How a speech answer's audio is encoded: the format, and the sample rate and channel count when
/// the engine says (since 0.5.11). Becomes [`ServerFrame::SpeechStart`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct AudioHeader {
    pub format: AudioFormat,
    pub sample_rate: Option<u32>,
    pub channels: Option<u16>,
}

impl AudioHeader {
    pub fn new(format: AudioFormat) -> Self {
        Self { format, ..Default::default() }
    }

    pub fn with_sample_rate(mut self, hz: u32) -> Self {
        self.sample_rate = Some(hz);
        self
    }

    pub fn with_channels(mut self, channels: u16) -> Self {
        self.channels = Some(channels);
        self
    }
}

/// One piece of an [`AudioStream`] (since 0.5.11): the header once, then audio, with usage and
/// metadata whenever the backend knows them.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum SpeechPiece {
    /// The audio begins; must come once, before any `Audio`.
    Start(AudioHeader),
    /// The next bytes of the file, any size (the server splits them into frames of at most
    /// [`MAX_AUDIO_CHUNK`]).
    Audio(Vec<u8>),
    /// Counts so far (the last one wins); the server fills in
    /// [`Usage::input_chars`] when the backend leaves it at 0.
    Usage(Usage),
    /// Metadata for `Done`: [`Meta::ignored`] fields are added, `served_by` and `revision` kept
    /// when set.
    Meta(Meta),
}

/// What a [`SpeakBackend`] returns (since 0.5.11): the speech as it is made. A backend either
/// hands over a finished file ([`AudioStream::from_file`]) or streams through the
/// [`AudioSink`] of [`AudioStream::channel`] from a task it spawns (which should stop when
/// `cancel` fires or [`AudioSink::is_closed`]). An `Err` piece fails the request.
#[derive(Debug)]
pub struct AudioStream {
    rx: mpsc::Receiver<Result<SpeechPiece, RemoteError>>,
}

/// The sending end of an [`AudioStream`] (since 0.5.11). Every send fails with `Cancelled` once
/// the stream is gone (the asker cancelled or went away).
#[derive(Debug, Clone)]
pub struct AudioSink {
    tx: mpsc::Sender<Result<SpeechPiece, RemoteError>>,
}

impl AudioStream {
    /// A stream and its sink, holding up to `buffer` pieces (at least 1) before a send waits.
    pub fn channel(buffer: usize) -> (AudioSink, AudioStream) {
        let (tx, rx) = mpsc::channel(buffer.max(1));
        (AudioSink { tx }, AudioStream { rx })
    }

    /// A finished file: the header, the audio and, if known, usage.
    pub fn from_file(header: AudioHeader, audio: Vec<u8>, usage: Option<Usage>) -> Self {
        let mut pieces = vec![Ok(SpeechPiece::Start(header)), Ok(SpeechPiece::Audio(audio))];
        pieces.extend(usage.map(|u| Ok(SpeechPiece::Usage(u))));
        Self::from_pieces(pieces)
    }

    /// A stream of these pieces, in order.
    pub fn from_pieces(pieces: Vec<Result<SpeechPiece, RemoteError>>) -> Self {
        let (tx, rx) = mpsc::channel(pieces.len().max(1));
        for p in pieces {
            let _ = tx.try_send(p);
        }
        Self { rx }
    }

    /// The next piece; `None` at the end.
    pub async fn next(&mut self) -> Option<Result<SpeechPiece, RemoteError>> {
        self.rx.recv().await
    }
}

impl AudioSink {
    async fn send(&self, piece: Result<SpeechPiece, RemoteError>) -> Result<(), RemoteError> {
        self.tx.send(piece).await.map_err(|_| RemoteError::Cancelled)
    }

    /// The audio begins (once, first).
    pub async fn start(&self, header: AudioHeader) -> Result<(), RemoteError> {
        self.send(Ok(SpeechPiece::Start(header))).await
    }

    /// More audio; empty pieces are skipped.
    pub async fn audio(&self, data: Vec<u8>) -> Result<(), RemoteError> {
        if data.is_empty() {
            return Ok(());
        }
        self.send(Ok(SpeechPiece::Audio(data))).await
    }

    pub async fn usage(&self, usage: Usage) -> Result<(), RemoteError> {
        self.send(Ok(SpeechPiece::Usage(usage))).await
    }

    pub async fn meta(&self, meta: Meta) -> Result<(), RemoteError> {
        self.send(Ok(SpeechPiece::Meta(meta))).await
    }

    /// Fails the request with `error` (after any audio already sent).
    pub async fn fail(&self, error: RemoteError) -> Result<(), RemoteError> {
        self.send(Err(error)).await
    }

    /// The stream is gone: stop making audio.
    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }
}

/// What serves speech out (since 0.5.11): a speaking model on this node, or an inference node's
/// routing to one.
#[async_trait]
pub trait SpeakBackend: Send + Sync {
    fn name(&self) -> String;

    /// The speech for `request` (already [`check`](SpeakRequest::check)ed). Return
    /// `Unsupported` when this model can't serve the request but another node's copy might (a
    /// different engine), `BadRequest` naming what the model lacks when none could
    /// ([`SpeechProfile::covers`](crate::speech::SpeechProfile::covers)). Poll `cancel`; the
    /// server keeps the asker's stream alive with `Working` until the stream starts.
    async fn speak(&self, request: &SpeakRequest, cancel: &CancelToken) -> Result<AudioStream, RemoteError>;
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
        Some(ClientFrame::Transcribe(request)) => serve_transcribe(ch, caller, host, admission, request).await,
        Some(ClientFrame::Speak(request)) => serve_speak(ch, caller, host, admission, request).await,
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

async fn serve_transcribe(
    mut ch: Channel,
    caller: Caller,
    host: &dyn InferHost,
    admission: &Admission,
    mut request: TranscribeRequest,
) -> Result<(), RemoteError> {
    let started = Instant::now();
    let id = request.id.clone();
    let caller = caller.presenting(request.cert.take());
    let opened = host.authorize(&caller).and_then(|()| request.check()).and_then(|()| host.open_transcribe(&caller, request.model.as_deref()));
    let backend = match opened {
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
    let mut run = pin!(backend.transcribe_counted(&request, &cancel));
    let mut keepalive = tokio::time::interval_at(Instant::now() + QUEUE_KEEPALIVE, QUEUE_KEEPALIVE);
    let result = loop {
        tokio::select! {
            r = &mut run => break r,
            _ = keepalive.tick() => ch.send_cbor(&ServerFrame::Working { id: id.clone() }).await?,
            m = ch.rx.recv() => match m {
                Ok(Some(bytes)) if is_cancel(&bytes, &id) => {
                    cancel.cancel();
                    return ch.send_cbor(&ServerFrame::Cancelled { id }).await;
                }
                Ok(Some(_)) => {}
                Ok(None) | Err(_) => {
                    cancel.cancel();
                    return Ok(());
                }
            },
        }
    };
    let (transcript, usage) = match result {
        Ok(r) => r,
        Err(error) => return ch.send_cbor(&ServerFrame::Failed { id, error }).await,
    };
    ch.send_cbor(&ServerFrame::Transcript { id: id.clone(), transcript }).await?;
    let mut finish = Finish::new("stop");
    finish.usage = usage;
    finish.meta.served_by = Some(backend.name());
    finish.meta.timings.queued_ms = millis(queued);
    finish.meta.timings.total_ms = millis(started.elapsed());
    ch.send_cbor(&ServerFrame::Done { id, finish }).await
}

/// Cancels the token when dropped, so a backend task stops however the request ends.
struct CancelOnDrop(CancelToken);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

async fn serve_speak(
    mut ch: Channel,
    caller: Caller,
    host: &dyn InferHost,
    admission: &Admission,
    mut request: SpeakRequest,
) -> Result<(), RemoteError> {
    let started = Instant::now();
    let id = request.id.clone();
    let caller = caller.presenting(request.cert.take());
    let opened = host.authorize(&caller).and_then(|()| request.check()).and_then(|()| host.open_speak(&caller, &request));
    let backend = match opened {
        Ok(b) => b,
        Err(error) => return ch.send_cbor(&ServerFrame::Failed { id, error }).await,
    };
    let _permit = match wait_turn(&mut ch, &id, admission, request.priority).await? {
        Waited::Go(p) => p,
        Waited::Gone => return Ok(()),
    };
    let queued = started.elapsed();
    ch.send_cbor(&ServerFrame::Accepted { id: id.clone(), model: backend.name(), attempt: 0, resume: false }).await?;
    let accepted = Instant::now();
    let cancel = CancelToken::new();
    let _stop = CancelOnDrop(cancel.clone());
    let mut keepalive = tokio::time::interval_at(Instant::now() + QUEUE_KEEPALIVE, QUEUE_KEEPALIVE);
    let opened = {
        let mut run = pin!(backend.speak(&request, &cancel));
        loop {
            tokio::select! {
                r = &mut run => break r,
                _ = keepalive.tick() => ch.send_cbor(&ServerFrame::Working { id: id.clone() }).await?,
                m = ch.rx.recv() => match m {
                    Ok(Some(bytes)) if is_cancel(&bytes, &id) => {
                        cancel.cancel();
                        return ch.send_cbor(&ServerFrame::Cancelled { id }).await;
                    }
                    Ok(Some(_)) => {}
                    Ok(None) | Err(_) => return Ok(()),
                },
            }
        }
    };
    let mut stream = match opened {
        Ok(s) => s,
        Err(error) => return ch.send_cbor(&ServerFrame::Failed { id, error }).await,
    };
    let mut first_audio = None;
    let mut offset = 0u64;
    let mut usage: Option<Usage> = None;
    let mut meta = Meta::default();
    loop {
        tokio::select! {
            p = stream.next() => match p {
                None => break,
                Some(Err(error)) => return ch.send_cbor(&ServerFrame::Failed { id, error }).await,
                Some(Ok(SpeechPiece::Start(h))) if first_audio.is_none() => {
                    first_audio = Some(accepted.elapsed());
                    ch.send_cbor(&ServerFrame::SpeechStart { id: id.clone(), format: h.format, sample_rate: h.sample_rate, channels: h.channels }).await?;
                }
                Some(Ok(SpeechPiece::Start(_))) => {
                    let error = RemoteError::protocol("the speech backend started the audio twice");
                    return ch.send_cbor(&ServerFrame::Failed { id, error }).await;
                }
                Some(Ok(SpeechPiece::Audio(data))) if first_audio.is_some() => {
                    for chunk in data.chunks(MAX_AUDIO_CHUNK) {
                        ch.send_cbor(&ServerFrame::Audio { id: id.clone(), offset, data: chunk.to_vec() }).await?;
                        offset += chunk.len() as u64;
                    }
                }
                Some(Ok(SpeechPiece::Audio(_))) => {
                    let error = RemoteError::protocol("the speech backend sent audio before its format");
                    return ch.send_cbor(&ServerFrame::Failed { id, error }).await;
                }
                Some(Ok(SpeechPiece::Usage(u))) => usage = Some(u),
                Some(Ok(SpeechPiece::Meta(m))) => {
                    for field in m.ignored {
                        if !meta.ignored.contains(&field) {
                            meta.ignored.push(field);
                        }
                    }
                    if m.served_by.is_some() {
                        meta.served_by = m.served_by;
                    }
                    if m.revision.is_some() {
                        meta.revision = m.revision;
                    }
                }
            },
            _ = keepalive.tick(), if first_audio.is_none() => ch.send_cbor(&ServerFrame::Working { id: id.clone() }).await?,
            m = ch.rx.recv() => match m {
                Ok(Some(bytes)) if is_cancel(&bytes, &id) => {
                    cancel.cancel();
                    return ch.send_cbor(&ServerFrame::Cancelled { id }).await;
                }
                Ok(Some(_)) => {}
                Ok(None) | Err(_) => return Ok(()),
            },
        }
    }
    if first_audio.is_none() {
        let error = RemoteError::Failed { message: "the speech backend finished without audio".into() };
        return ch.send_cbor(&ServerFrame::Failed { id, error }).await;
    }
    let mut usage = usage.unwrap_or_default();
    if usage.input_chars == 0 {
        usage.input_chars = request.input.chars();
    }
    let mut finish = Finish::new("stop").with_usage(usage);
    meta.served_by.get_or_insert_with(|| backend.name());
    meta.timings.queued_ms = millis(queued);
    meta.timings.first_token_ms = first_audio.map(millis);
    meta.timings.total_ms = millis(started.elapsed());
    finish.meta = meta;
    ch.send_cbor(&ServerFrame::Done { id, finish }).await
}
