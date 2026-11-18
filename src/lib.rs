//! Weirdnet agent runtime.
//!
//! A model-agnostic agent loop shared by WeirdBrains apps. Apps supply a
//! [`ModelBackend`], tool sources and an [`Approver`]; this crate runs the turn
//! loop, enforces the approval [`Policy`] and research [`Budget`], and records a
//! [`Trace`] whose [`Citation`]s are checked against the steps that fetched them.
//!
//! - [`session`]: [`AgentSession`], the turn loop with streaming [`AgentEvent`]s.
//! - [`cancel`]: [`CancelToken`], checked between model and tool steps.
//! - [`tool`]: [`Tool`], [`ToolSource`] (local now, MCP and remote nodes later) and [`ToolRegistry`].
//! - [`policy`]: tiers, per-companion per-site grants, task approvals and standing allows.
//! - [`budget`]: searches, pages, concurrency and time limits.
//! - [`trace`]: steps, citations and the cited-step check.
//! - [`model`]: the model backend trait and chat messages.
//!
//! Nothing here is app-specific: browser tools, panels and storage live in the app.

pub mod budget;
pub mod cancel;
pub mod identity;
pub mod model;
pub mod policy;
pub mod session;
#[cfg(any(test, feature = "testing"))]
pub mod testing;
pub mod tool;
pub mod trace;

pub use budget::{Budget, BudgetExceeded, BudgetTracker, BudgetUsage};
pub use cancel::CancelToken;
pub use identity::{AgentId, AgentIdentity, AgentKind};
pub use model::{DeltaSink, Message, ModelBackend, ModelError, ModelTurn, Role, ToolCall};
pub use policy::{
    ActionRequest, AllowRule, ApprovalPrompt, CompanionScope, Decision, DenyReason, Policy, PolicyError, Scope, SharedPolicy, SiteLevel, SkipNotice,
    StandingAllow, TaskApprovals, Tier, site_of,
};
pub use session::{AgentConfig, AgentEvent, AgentSession, ApprovalAnswer, Approver, Outcome, StopReason};
pub use tool::{
    BudgetCharge, CallTarget, LocalTools, RegistryError, SourceKind, Tool, ToolAnnotations, ToolContext, ToolError, ToolOutput, ToolRegistry,
    ToolSource, ToolSpec,
};
pub use trace::{
    ApprovalOutcome, Citation, CitationError, CitationTarget, SessionMark, StepId, StepKind, Trace, TraceStep, retain_valid, validate_citations,
};
