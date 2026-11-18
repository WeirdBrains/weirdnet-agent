//! Test doubles: a scripted model, a scripted approver and closure-backed tools.
//! Enabled in this crate's tests and, for downstream crates, with the `testing` feature.

use std::collections::VecDeque;
use std::sync::Mutex;

use async_trait::async_trait;
use serde_json::Value;

use crate::cancel::CancelToken;
use crate::model::{DeltaSink, Message, ModelBackend, ModelError, ModelTurn, ToolCall};
use crate::policy::ApprovalPrompt;
use crate::session::{ApprovalAnswer, Approver};
use crate::tool::{CallTarget, Tool, ToolContext, ToolError, ToolOutput, ToolSpec};

/// Shorthand for a model tool call.
pub fn call(id: &str, name: &str, arguments: Value) -> ToolCall {
    ToolCall { id: id.into(), name: name.into(), arguments }
}

/// Returns pre-written turns in order, streaming each turn's text word by word.
/// Once the script runs out it returns empty turns.
pub struct ScriptedModel {
    turns: Mutex<VecDeque<ModelTurn>>,
    seen: Mutex<Vec<Vec<Message>>>,
}

impl ScriptedModel {
    pub fn new(turns: Vec<ModelTurn>) -> Self {
        Self { turns: Mutex::new(turns.into()), seen: Mutex::new(Vec::new()) }
    }

    /// The message history passed on each call.
    pub fn seen(&self) -> Vec<Vec<Message>> {
        self.seen.lock().unwrap().clone()
    }
}

#[async_trait]
impl ModelBackend for ScriptedModel {
    fn name(&self) -> String {
        "scripted".into()
    }

    async fn complete(&self, messages: &[Message], _tools: &[ToolSpec], deltas: &DeltaSink, cancel: &CancelToken) -> Result<ModelTurn, ModelError> {
        if cancel.is_cancelled() {
            return Err(ModelError::Cancelled);
        }
        self.seen.lock().unwrap().push(messages.to_vec());
        let turn = self.turns.lock().unwrap().pop_front().unwrap_or_default();
        for piece in turn.text.split_inclusive(' ') {
            deltas.text(piece);
        }
        Ok(turn)
    }
}

/// Answers approval cards from a queue (then denies) and records every prompt.
#[derive(Default)]
pub struct ScriptedApprover {
    answers: Mutex<VecDeque<ApprovalAnswer>>,
    asked: Mutex<Vec<ApprovalPrompt>>,
}

impl ScriptedApprover {
    pub fn new(answers: Vec<ApprovalAnswer>) -> Self {
        Self { answers: Mutex::new(answers.into()), asked: Mutex::new(Vec::new()) }
    }

    pub fn asked(&self) -> Vec<ApprovalPrompt> {
        self.asked.lock().unwrap().clone()
    }
}

#[async_trait]
impl Approver for ScriptedApprover {
    async fn decide(&self, prompt: &ApprovalPrompt, _tool: &ToolSpec) -> ApprovalAnswer {
        self.asked.lock().unwrap().push(prompt.clone());
        self.answers.lock().unwrap().pop_front().unwrap_or(ApprovalAnswer::Deny)
    }
}

type CallFn = Box<dyn Fn(Value) -> Result<ToolOutput, ToolError> + Send + Sync>;
type TargetFn = Box<dyn Fn(&Value) -> CallTarget + Send + Sync>;

/// A tool backed by closures.
pub struct FnTool {
    spec: ToolSpec,
    call: CallFn,
    target: Option<TargetFn>,
}

impl FnTool {
    pub fn new(spec: ToolSpec, call: impl Fn(Value) -> Result<ToolOutput, ToolError> + Send + Sync + 'static) -> Self {
        Self { spec, call: Box::new(call), target: None }
    }

    pub fn with_target(mut self, target: impl Fn(&Value) -> CallTarget + Send + Sync + 'static) -> Self {
        self.target = Some(Box::new(target));
        self
    }
}

#[async_trait]
impl Tool for FnTool {
    fn spec(&self) -> ToolSpec {
        self.spec.clone()
    }

    fn target(&self, args: &Value) -> CallTarget {
        self.target.as_ref().map(|t| t(args)).unwrap_or_default()
    }

    async fn call(&self, _ctx: &ToolContext, args: Value) -> Result<ToolOutput, ToolError> {
        (self.call)(args)
    }
}
