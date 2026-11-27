//! The model backend trait and chat messages. Inference lives elsewhere
//! (`weirdnet-model`, remote nodes); this crate only drives it.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc::UnboundedSender;

use crate::cancel::CancelToken;
use crate::session::AgentEvent;
use crate::tool::ToolSpec;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Role {
    System,
    User,
    Assistant,
    Tool,
}

/// A tool call the model asked for.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub arguments: Value,
}

/// What a message says: plain text, or OpenAI-style parts (text, images, audio, video).
///
/// Text serializes as a plain string, exactly as `content` did before 0.5, so text-only messages
/// stay readable by older peers. Parts serialize as a list of `{"type": ...}` objects; peers older
/// than 0.5 can't read them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Content {
    Text(String),
    Parts(Vec<ContentPart>),
}

impl Default for Content {
    fn default() -> Self {
        Content::Text(String::new())
    }
}

/// One part of a [`Content::Parts`] message, in the OpenAI chat shape.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
pub enum ContentPart {
    Text {
        text: String,
    },
    /// An image as a `data:` URL (`data:image/png;base64,...`).
    ImageUrl {
        image_url: ImageUrl,
    },
    /// Base64 audio and its format (`wav`, `mp3`).
    InputAudio {
        input_audio: InputAudio,
    },
    /// A whole video clip as a `data:` URL (`data:video/mp4;base64,...`), for models that read
    /// video natively ([`Modalities::video`]); `{"type": "video_url", "video_url": {"url": ...}}`
    /// on the wire, the part vLLM and mistral.rs take. Since 0.5.10: peers before it can't read a
    /// message carrying one, and [`Catalog::route`](crate::remote::catalog::Catalog::route) only
    /// sends it to models whose catalog entry says `inputs.video`, which older nodes never do.
    VideoUrl {
        video_url: VideoUrl,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ImageUrl {
    pub url: String,
    /// OpenAI's `detail` hint (`auto`, `low`, `high`), passed on as is.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// A video clip, inline. Small clips only for now: the whole request must fit in one stream
/// message (32 MiB on Weirdnet, less behind a bridge's `max_request_bytes`). A reference to a
/// clip the node fetches over the link (a `weirdnet-net` content root) may be added later as
/// another optional field, so this stays the place to look.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct VideoUrl {
    /// A `data:video/...;base64,` URL (see [`Content::check`]); remote URLs are never fetched.
    pub url: String,
}

impl VideoUrl {
    /// The clip's MIME type (`video/mp4`) when the URL is a `data:` URL.
    pub fn mime_type(&self) -> Option<&str> {
        let rest = self.url.strip_prefix("data:")?;
        let end = rest.find([';', ','])?;
        Some(&rest[..end]).filter(|m| !m.is_empty())
    }

    /// The clip's size in bytes once decoded, from the base64 length of a `data:...;base64,` URL
    /// (nothing is decoded), to compare with
    /// [`MediaLimits::video_max_bytes`](crate::MediaLimits::video_max_bytes). `None` for any
    /// other URL.
    pub fn data_len(&self) -> Option<u64> {
        let (head, data) = self.url.strip_prefix("data:")?.split_once(',')?;
        if !head.ends_with(";base64") {
            return None;
        }
        let data = data.trim_end();
        let pad = data.bytes().rev().take_while(|b| *b == b'=').count().min(2);
        Some((data.len() as u64 * 3 / 4).saturating_sub(pad as u64))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct InputAudio {
    /// Base64, no `data:` prefix.
    pub data: String,
    pub format: String,
}

impl ContentPart {
    pub fn text(text: impl Into<String>) -> Self {
        ContentPart::Text { text: text.into() }
    }

    /// An image part; `url` should be a `data:image/...;base64,` URL (see [`Content::check`]).
    pub fn image(url: impl Into<String>) -> Self {
        ContentPart::ImageUrl { image_url: ImageUrl { url: url.into(), detail: None } }
    }

    pub fn audio(data: impl Into<String>, format: impl Into<String>) -> Self {
        ContentPart::InputAudio { input_audio: InputAudio { data: data.into(), format: format.into() } }
    }

    /// A video part; `url` should be a `data:video/...;base64,` URL (see [`Content::check`]).
    /// Since 0.5.10.
    pub fn video(url: impl Into<String>) -> Self {
        ContentPart::VideoUrl { video_url: VideoUrl { url: url.into() } }
    }

    /// A video part from the clip's bytes and MIME type (`video/mp4`, `video/webm`), as a
    /// `data:` URL. Since 0.5.10.
    pub fn video_bytes(bytes: &[u8], mime_type: &str) -> Self {
        use base64::Engine as _;
        Self::video(format!("data:{mime_type};base64,{}", base64::engine::general_purpose::STANDARD.encode(bytes)))
    }

    /// What a model needs to read this part.
    pub fn needs(&self) -> Modalities {
        match self {
            ContentPart::Text { .. } => Modalities::TEXT,
            ContentPart::ImageUrl { .. } => Modalities::VISION,
            ContentPart::InputAudio { .. } => Modalities::AUDIO,
            ContentPart::VideoUrl { .. } => Modalities::VIDEO,
        }
    }
}

impl Content {
    /// The text: the string, or the text parts joined (images, audio and video left out).
    pub fn text(&self) -> std::borrow::Cow<'_, str> {
        match self {
            Content::Text(t) => std::borrow::Cow::Borrowed(t),
            Content::Parts(parts) => parts
                .iter()
                .filter_map(|p| match p {
                    ContentPart::Text { text } => Some(text.as_str()),
                    _ => None,
                })
                .collect::<String>()
                .into(),
        }
    }

    /// The string, if this is plain text.
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Content::Text(t) => Some(t),
            Content::Parts(_) => None,
        }
    }

    /// The text, consuming the content (see [`text`](Self::text)).
    pub fn into_text(self) -> String {
        match self {
            Content::Text(t) => t,
            other => other.text().into_owned(),
        }
    }

    /// The parts; plain text is one text part.
    pub fn parts(&self) -> Vec<ContentPart> {
        match self {
            Content::Text(t) => vec![ContentPart::text(t.clone())],
            Content::Parts(p) => p.clone(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Content::Text(t) => t.is_empty(),
            Content::Parts(p) => p.is_empty(),
        }
    }

    /// What a model needs to read this content.
    pub fn needs(&self) -> Modalities {
        match self {
            Content::Text(_) => Modalities::TEXT,
            Content::Parts(p) => p.iter().fold(Modalities::TEXT, |m, part| m.union(part.needs())),
        }
    }

    /// Text-only parts become plain text, the form older peers read. Content with images, audio
    /// or video is unchanged.
    pub fn normalized(self) -> Self {
        match self {
            Content::Parts(p) if p.iter().all(|x| matches!(x, ContentPart::Text { .. })) => Content::Parts(p).into_text().into(),
            other => other,
        }
    }

    /// Images must be `data:image/...` URLs and videos `data:video/...` URLs (nodes never fetch
    /// remote URLs), and audio needs a format. `Err` says what is wrong, for a 400.
    pub fn check(&self) -> Result<(), String> {
        let Content::Parts(parts) = self else { return Ok(()) };
        for p in parts {
            match p {
                ContentPart::ImageUrl { image_url } if !image_url.url.starts_with("data:image/") => {
                    return Err("image_url must be a data:image/... URL; remote image URLs are not fetched".into());
                }
                ContentPart::InputAudio { input_audio } if input_audio.format.is_empty() || input_audio.data.is_empty() => {
                    return Err("input_audio needs data and a format".into());
                }
                ContentPart::VideoUrl { video_url } if !video_url.url.starts_with("data:video/") => {
                    return Err("video_url must be a data:video/... URL; remote video URLs are not fetched".into());
                }
                _ => {}
            }
        }
        Ok(())
    }
}

impl From<String> for Content {
    fn from(s: String) -> Self {
        Content::Text(s)
    }
}

impl From<&str> for Content {
    fn from(s: &str) -> Self {
        Content::Text(s.to_string())
    }
}

impl From<&String> for Content {
    fn from(s: &String) -> Self {
        Content::Text(s.clone())
    }
}

impl From<Vec<ContentPart>> for Content {
    fn from(p: Vec<ContentPart>) -> Self {
        Content::Parts(p)
    }
}

impl PartialEq<str> for Content {
    fn eq(&self, other: &str) -> bool {
        self.as_text() == Some(other)
    }
}

impl PartialEq<&str> for Content {
    fn eq(&self, other: &&str) -> bool {
        self.as_text() == Some(*other)
    }
}

impl std::fmt::Display for Content {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.text())
    }
}

/// Input kinds beyond text: what a request needs, or what a model accepts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Modalities {
    /// Images (`image_url` parts).
    #[serde(default, skip_serializing_if = "is_false")]
    pub vision: bool,
    /// Audio (`input_audio` parts).
    #[serde(default, skip_serializing_if = "is_false")]
    pub audio: bool,
    /// Whole video clips (`video_url` parts), read natively rather than as frames. Since 0.5.10;
    /// absent (`false`) from older peers, so their models count as not reading video.
    #[serde(default, skip_serializing_if = "is_false")]
    pub video: bool,
}

fn is_false(b: &bool) -> bool {
    !*b
}

impl Modalities {
    /// Text only.
    pub const TEXT: Self = Self { vision: false, audio: false, video: false };
    pub const VISION: Self = Self { vision: true, audio: false, video: false };
    pub const AUDIO: Self = Self { vision: false, audio: true, video: false };
    /// Whole video clips (`video_url` parts). Since 0.5.10.
    pub const VIDEO: Self = Self { vision: false, audio: false, video: true };

    /// Images and audio; add video with [`with_video`](Self::with_video).
    pub fn new(vision: bool, audio: bool) -> Self {
        Self { vision, audio, video: false }
    }

    /// The same with video set to `video`. Since 0.5.10.
    pub fn with_video(mut self, video: bool) -> Self {
        self.video = video;
        self
    }

    pub fn is_text(&self) -> bool {
        !self.vision && !self.audio && !self.video
    }

    pub fn union(self, other: Self) -> Self {
        Self { vision: self.vision || other.vision, audio: self.audio || other.audio, video: self.video || other.video }
    }

    /// A model accepting `self` can read input that needs `needed`.
    pub fn covers(&self, needed: Modalities) -> bool {
        (self.vision || !needed.vision) && (self.audio || !needed.audio) && (self.video || !needed.video)
    }

    /// What `self` lacks of `needed`, for errors: `"images"`, `"audio"`, `"video"`,
    /// `"images and audio"`, `"images and video"`, `"audio and video"` or
    /// `"images, audio and video"`.
    pub fn lacking(&self, needed: Modalities) -> Option<&'static str> {
        match (needed.vision && !self.vision, needed.audio && !self.audio, needed.video && !self.video) {
            (true, true, true) => Some("images, audio and video"),
            (true, true, false) => Some("images and audio"),
            (true, false, true) => Some("images and video"),
            (false, true, true) => Some("audio and video"),
            (true, false, false) => Some("images"),
            (false, true, false) => Some("audio"),
            (false, false, true) => Some("video"),
            (false, false, false) => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Message {
    pub role: Role,
    pub content: Content,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<ToolCall>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
}

impl Message {
    pub fn system(content: impl Into<Content>) -> Self {
        Self { role: Role::System, content: content.into(), tool_calls: Vec::new(), tool_call_id: None }
    }

    /// A user message: text (`Message::user("hi")`) or parts
    /// (`Message::user(vec![ContentPart::text("what is this?"), ContentPart::image(url)])`).
    pub fn user(content: impl Into<Content>) -> Self {
        Self { role: Role::User, content: content.into(), tool_calls: Vec::new(), tool_call_id: None }
    }

    pub fn assistant(content: impl Into<Content>, tool_calls: Vec<ToolCall>) -> Self {
        Self { role: Role::Assistant, content: content.into(), tool_calls, tool_call_id: None }
    }

    pub fn tool(call_id: impl Into<String>, content: impl Into<Content>) -> Self {
        Self { role: Role::Tool, content: content.into(), tool_calls: Vec::new(), tool_call_id: Some(call_id.into()) }
    }

    /// The text of the message (text parts joined; see [`Content::text`]).
    pub fn text(&self) -> std::borrow::Cow<'_, str> {
        self.content.text()
    }

    /// What a model needs to read this message.
    pub fn needs(&self) -> Modalities {
        self.content.needs()
    }
}

/// What a model needs to read all of `messages`.
pub fn needs(messages: &[Message]) -> Modalities {
    messages.iter().fold(Modalities::TEXT, |m, msg| m.union(msg.needs()))
}

/// One model turn: final text and any tool calls.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct ModelTurn {
    pub text: String,
    #[serde(default)]
    pub tool_calls: Vec<ToolCall>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ModelError {
    #[error("model unavailable: {0}")]
    Unavailable(String),
    #[error("model failed: {0}")]
    Failed(String),
    #[error("cancelled")]
    Cancelled,
    /// No inference node of a model pool could be reached (none found, or every one was
    /// unreachable, timed out or closed before answering). Apps usually fall back to a local
    /// model on it; [`remote::pool_model::PoolModel`](crate::remote::pool_model::PoolModel) can do that itself.
    #[error("model pool unreachable: {0}")]
    PoolUnreachable(String),
}

/// Where a turn's model runs, when that changes during the turn: shown to the user as a short
/// note ([`AgentEvent::ModelNotice`]), never part of the turn's text.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ModelNotice {
    /// A node failed mid-answer and the answer continues on `node` from `offset` characters,
    /// with no text repeated.
    Continued { node: String, offset: u64 },
    /// The pool could not take the turn before any text streamed (`reason`), so `to` (usually
    /// the local model) runs it instead.
    FellBack { reason: String, to: String },
    /// The chosen model `from` can't read this turn's `needs` (for example "images"), so the
    /// pool picks a model that can.
    Rerouted { from: String, needs: String },
}

/// Where a backend streams text as it is sampled.
#[derive(Debug, Clone)]
pub struct DeltaSink {
    tx: Option<UnboundedSender<AgentEvent>>,
}

impl DeltaSink {
    pub fn new(tx: Option<UnboundedSender<AgentEvent>>) -> Self {
        Self { tx }
    }

    /// A sink that drops everything.
    pub fn discard() -> Self {
        Self { tx: None }
    }

    pub fn text(&self, delta: impl Into<String>) {
        if let Some(tx) = &self.tx {
            let _ = tx.send(AgentEvent::TextDelta(delta.into()));
        }
    }

    /// Reasoning ("thinking") text, streamed apart from the answer. It is shown but never part
    /// of the turn's text.
    pub fn reasoning(&self, delta: impl Into<String>) {
        if let Some(tx) = &self.tx {
            let _ = tx.send(AgentEvent::ReasoningDelta(delta.into()));
        }
    }

    /// A note about where the model runs ([`ModelNotice`]).
    pub fn notice(&self, notice: ModelNotice) {
        if let Some(tx) = &self.tx {
            let _ = tx.send(AgentEvent::ModelNotice(notice));
        }
    }
}

/// A chat model with tool calling. Implementations should poll `cancel` while
/// sampling and stream text through `deltas`.
#[async_trait]
pub trait ModelBackend: Send + Sync {
    /// Shown in traces, for example `gemma-4-e4b@node-a`.
    fn name(&self) -> String;

    async fn complete(&self, messages: &[Message], tools: &[ToolSpec], deltas: &DeltaSink, cancel: &CancelToken) -> Result<ModelTurn, ModelError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    const PNG: &str = "data:image/png;base64,iVBORw0KGgo=";

    #[test]
    fn text_content_keeps_the_plain_string_wire_form() {
        let m = Message::user("hi");
        assert_eq!(serde_json::to_value(&m).unwrap(), serde_json::json!({"role": "user", "content": "hi"}));
        let cbor = crate::remote::to_cbor(&m).unwrap();
        #[derive(Deserialize)]
        struct Old {
            content: String,
        }
        assert_eq!(crate::remote::from_cbor::<Old>(&cbor).unwrap().content, "hi", "a 0.4 peer reads it");
        let old = serde_json::json!({"role": "user", "content": "from 0.4"});
        assert_eq!(serde_json::from_value::<Message>(old).unwrap().content, "from 0.4");
    }

    #[test]
    fn parts_round_trip_in_json_and_cbor() {
        let m = Message::user(vec![ContentPart::text("what is it?"), ContentPart::image(PNG), ContentPart::audio("UklGRg==", "wav")]);
        let j = serde_json::to_value(&m).unwrap();
        assert_eq!(
            j["content"],
            serde_json::json!([
                {"type": "text", "text": "what is it?"},
                {"type": "image_url", "image_url": {"url": PNG}},
                {"type": "input_audio", "input_audio": {"data": "UklGRg==", "format": "wav"}}
            ])
        );
        assert_eq!(serde_json::from_value::<Message>(j).unwrap(), m);
        let back: Message = crate::remote::from_cbor(&crate::remote::to_cbor(&m).unwrap()).unwrap();
        assert_eq!(back, m);
        assert_eq!(m.needs(), Modalities::new(true, true));
        assert_eq!(m.text(), "what is it?");
    }

    #[test]
    fn needs_covers_and_lacking() {
        let msgs = [Message::system("s"), Message::user(vec![ContentPart::image(PNG)])];
        let n = needs(&msgs);
        assert_eq!(n, Modalities::VISION);
        assert!(Modalities::VISION.covers(n) && !Modalities::TEXT.covers(n) && !Modalities::AUDIO.covers(n));
        assert_eq!(Modalities::TEXT.lacking(Modalities::new(true, true)), Some("images and audio"));
        assert_eq!(Modalities::VISION.lacking(n), None);
        assert!(needs(&[Message::user("x")]).is_text());
    }

    #[test]
    fn check_refuses_remote_images_and_empty_audio() {
        assert!(Content::from(vec![ContentPart::image(PNG)]).check().is_ok());
        assert!(Content::from(vec![ContentPart::image("https://example.com/a.png")]).check().is_err());
        assert!(Content::from(vec![ContentPart::audio("", "wav")]).check().is_err());
        assert!(Content::from("text").check().is_ok());
    }

    const MP4: &str = "data:video/mp4;base64,AAAAIGZ0eXBpc29t";

    #[test]
    fn video_parts_round_trip_in_json_and_cbor() {
        let m = Message::user(vec![ContentPart::text("what happens?"), ContentPart::video(MP4)]);
        let j = serde_json::to_value(&m).unwrap();
        assert_eq!(j["content"], serde_json::json!([{"type": "text", "text": "what happens?"}, {"type": "video_url", "video_url": {"url": MP4}}]));
        assert_eq!(serde_json::from_value::<Message>(j).unwrap(), m);
        let cbor = crate::remote::to_cbor(&m).unwrap();
        assert_eq!(crate::remote::from_cbor::<Message>(&cbor).unwrap(), m);
        assert_eq!(m.needs(), Modalities::VIDEO);
        assert_eq!(m.text(), "what happens?");
        let mixed = Message::user(vec![ContentPart::image(PNG), ContentPart::video(MP4), ContentPart::audio("UklGRg==", "wav")]);
        assert_eq!(mixed.needs(), Modalities::new(true, true).with_video(true));
        // A part this version doesn't know (a newer one) fails the message, as a video part does
        // for peers before 0.5.10; routing keeps it away from them.
        let newer = serde_json::json!({"role": "user", "content": [{"type": "hologram_url", "hologram_url": {"url": "x"}}]});
        assert!(serde_json::from_value::<Message>(newer).is_err());
    }

    #[test]
    fn video_bytes_mime_and_size() {
        let clip = b"\x00\x00\x00\x20ftypisom-a-tiny-clip";
        let ContentPart::VideoUrl { video_url } = ContentPart::video_bytes(clip, "video/mp4") else { panic!("a video part") };
        assert!(video_url.url.starts_with("data:video/mp4;base64,"));
        assert_eq!(video_url.mime_type(), Some("video/mp4"));
        assert_eq!(video_url.data_len(), Some(clip.len() as u64));
        for n in 0..8usize {
            let ContentPart::VideoUrl { video_url } = ContentPart::video_bytes(&vec![7u8; n], "video/webm") else { unreachable!() };
            assert_eq!(video_url.data_len(), Some(n as u64), "{n} bytes");
        }
        let remote = VideoUrl { url: "https://example.com/a.mp4".into() };
        assert_eq!((remote.mime_type(), remote.data_len()), (None, None));
        assert_eq!(VideoUrl { url: "data:video/mp4,raw".into() }.data_len(), None, "not base64");
        assert!(Content::from(vec![ContentPart::video(MP4)]).check().is_ok());
        assert!(Content::from(vec![ContentPart::video("https://example.com/a.mp4")]).check().is_err());
        assert!(Content::from(vec![ContentPart::video("data:image/png;base64,iVBORw0KGgo=")]).check().is_err());
        let v = Content::from(vec![ContentPart::video(MP4)]);
        assert_eq!(v.clone().normalized(), v, "video stays parts");
    }

    #[test]
    fn video_modality() {
        let v = Modalities::VIDEO;
        assert!(!v.is_text() && v.video && !v.vision && !v.audio);
        assert_eq!(Modalities::new(true, false).with_video(true), Modalities::VISION.union(Modalities::VIDEO));
        assert!(Modalities::VIDEO.covers(Modalities::VIDEO) && !Modalities::new(true, true).covers(Modalities::VIDEO));
        assert!(Modalities::VIDEO.covers(Modalities::TEXT) && !Modalities::VIDEO.covers(Modalities::VISION));
        let all = Modalities::new(true, true).with_video(true);
        assert_eq!(Modalities::TEXT.lacking(all), Some("images, audio and video"));
        assert_eq!(Modalities::AUDIO.lacking(all), Some("images and video"));
        assert_eq!(Modalities::VISION.lacking(all), Some("audio and video"));
        assert_eq!(Modalities::VIDEO.lacking(all), Some("images and audio"));
        assert_eq!(Modalities::new(true, true).lacking(all), Some("video"));
        assert_eq!(all.lacking(all), None);
        // On the wire: absent unless set, and older peers' records read as no video.
        assert_eq!(serde_json::to_value(Modalities::new(true, true)).unwrap(), serde_json::json!({"vision": true, "audio": true}));
        assert_eq!(serde_json::to_value(Modalities::VIDEO).unwrap(), serde_json::json!({"video": true}));
        assert_eq!(serde_json::from_str::<Modalities>(r#"{"vision":true}"#).unwrap(), Modalities::VISION);
        let back: Modalities = crate::remote::from_cbor(&crate::remote::to_cbor(&all).unwrap()).unwrap();
        assert_eq!(back, all);
        #[derive(Deserialize, PartialEq, Debug)]
        struct Old {
            #[serde(default)]
            vision: bool,
            #[serde(default)]
            audio: bool,
        }
        let old: Old = crate::remote::from_cbor(&crate::remote::to_cbor(&all).unwrap()).unwrap();
        assert_eq!(old, Old { vision: true, audio: true }, "a 0.5.9 peer reads the rest and ignores video");
    }

    #[test]
    fn text_parts_normalize_to_a_string() {
        let c = Content::from(vec![ContentPart::text("a"), ContentPart::text("b")]).normalized();
        assert_eq!(c, Content::Text("ab".into()));
        let img = Content::from(vec![ContentPart::image(PNG)]);
        assert_eq!(img.clone().normalized(), img);
    }
}
