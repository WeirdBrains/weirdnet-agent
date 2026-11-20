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

/// What a message says: plain text, or OpenAI-style parts (text, images, audio).
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ImageUrl {
    pub url: String,
    /// OpenAI's `detail` hint (`auto`, `low`, `high`), passed on as is.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
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

    /// What a model needs to read this part.
    pub fn needs(&self) -> Modalities {
        match self {
            ContentPart::Text { .. } => Modalities::TEXT,
            ContentPart::ImageUrl { .. } => Modalities::VISION,
            ContentPart::InputAudio { .. } => Modalities::AUDIO,
        }
    }
}

impl Content {
    /// The text: the string, or the text parts joined (images and audio left out).
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

    /// Text-only parts become plain text, the form older peers read. Content with images or
    /// audio is unchanged.
    pub fn normalized(self) -> Self {
        match self {
            Content::Parts(p) if p.iter().all(|x| matches!(x, ContentPart::Text { .. })) => Content::Parts(p).into_text().into(),
            other => other,
        }
    }

    /// Images must be `data:image/...` URLs (nodes never fetch remote URLs) and audio needs a
    /// format. `Err` says what is wrong, for a 400.
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
}

fn is_false(b: &bool) -> bool {
    !*b
}

impl Modalities {
    /// Text only.
    pub const TEXT: Self = Self { vision: false, audio: false };
    pub const VISION: Self = Self { vision: true, audio: false };
    pub const AUDIO: Self = Self { vision: false, audio: true };

    pub fn new(vision: bool, audio: bool) -> Self {
        Self { vision, audio }
    }

    pub fn is_text(&self) -> bool {
        !self.vision && !self.audio
    }

    pub fn union(self, other: Self) -> Self {
        Self { vision: self.vision || other.vision, audio: self.audio || other.audio }
    }

    /// A model accepting `self` can read input that needs `needed`.
    pub fn covers(&self, needed: Modalities) -> bool {
        (self.vision || !needed.vision) && (self.audio || !needed.audio)
    }

    /// What `self` lacks of `needed`, for errors: `"images"`, `"audio"` or `"images and audio"`.
    pub fn lacking(&self, needed: Modalities) -> Option<&'static str> {
        match (needed.vision && !self.vision, needed.audio && !self.audio) {
            (true, true) => Some("images and audio"),
            (true, false) => Some("images"),
            (false, true) => Some("audio"),
            (false, false) => None,
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

    #[test]
    fn text_parts_normalize_to_a_string() {
        let c = Content::from(vec![ContentPart::text("a"), ContentPart::text("b")]).normalized();
        assert_eq!(c, Content::Text("ab".into()));
        let img = Content::from(vec![ContentPart::image(PNG)]);
        assert_eq!(img.clone().normalized(), img);
    }
}
