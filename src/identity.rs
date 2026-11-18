//! Who an agent (a "companion" in Morph) is.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Stable agent id, unique per user.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AgentId(pub String);

impl AgentId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AgentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Where an agent runs relative to the app hosting the session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentKind {
    /// Runs on this machine.
    Local,
    /// Runs on another of the user's linked nodes.
    Node,
    /// An outside agent driving the app (for example over MCP).
    External,
}

/// A named agent: every step, approval and citation is attributed to one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentIdentity {
    pub id: AgentId,
    pub name: String,
    pub kind: AgentKind,
    /// Node the agent runs on, when known (for example a host name).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
}

impl AgentIdentity {
    pub fn new(id: impl Into<String>, name: impl Into<String>, kind: AgentKind) -> Self {
        Self { id: AgentId::new(id), name: name.into(), kind, node: None }
    }

    pub fn with_node(mut self, node: impl Into<String>) -> Self {
        self.node = Some(node.into());
        self
    }
}
