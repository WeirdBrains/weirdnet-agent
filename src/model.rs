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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Message {
    pub role: Role,
    pub content: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<ToolCall>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
}

impl Message {
    pub fn system(content: impl Into<String>) -> Self {
        Self { role: Role::System, content: content.into(), tool_calls: Vec::new(), tool_call_id: None }
    }

    pub fn user(content: impl Into<String>) -> Self {
        Self { role: Role::User, content: content.into(), tool_calls: Vec::new(), tool_call_id: None }
    }

    pub fn assistant(content: impl Into<String>, tool_calls: Vec<ToolCall>) -> Self {
        Self { role: Role::Assistant, content: content.into(), tool_calls, tool_call_id: None }
    }

    pub fn tool(call_id: impl Into<String>, content: impl Into<String>) -> Self {
        Self { role: Role::Tool, content: content.into(), tool_calls: Vec::new(), tool_call_id: Some(call_id.into()) }
    }
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
