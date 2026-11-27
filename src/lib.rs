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
//! - [`model`]: the model backend trait and chat messages, whose [`Content`] is text or parts
//!   (text, images, audio, video) with the [`Modalities`] a model needs to read them.
//! - [`remote`]: protocols between one user's nodes over an app-supplied channel: inference
//!   (`infer/2` with priorities, queueing, reasoning, tool calls, embeddings, transcription and resume; `infer/1`
//!   kept for 0.3 nodes), the model catalog, remote tasks (Node companions), and the
//!   transport-neutral [`remote::pool::Pool`] that fails over and resumes answers across nodes,
//!   and [`remote::share`] links for pools shared with another Weirdnet identity.
//! - `pool` (feature): the pool over Weirdnet: inference nodes among the user's linked devices,
//!   or (`pool::connect_shared`) the nodes of a pool another identity shared.
//! - `jobs` (feature): synced jobs over the weirdnet-sync job log, claimed under majority-vote
//!   leases and resumed on another device from their last checkpoint.
//! - `serve` (feature): serve inference and the catalog on a Weirdnet node, checking callers
//!   against the device roster and, opt-in, guests of other identities (`serve::GuestAuth`).
//!
//! Nothing here is app-specific: browser tools, panels and storage live in the app.
//!
//! Public enums are `#[non_exhaustive]`: match them with a wildcard arm.

pub mod budget;
pub mod cancel;
pub mod identity;
#[cfg(feature = "jobs")]
pub mod jobs;
pub mod limits;
pub mod model;
#[cfg(any(feature = "pool", feature = "serve"))]
mod net;
pub mod policy;
#[cfg(feature = "pool")]
pub mod pool;
pub mod remote;
#[cfg(feature = "serve")]
pub mod serve;
pub mod session;
#[cfg(any(test, feature = "testing"))]
pub mod testing;
pub mod tool;
pub mod trace;

pub use budget::{Budget, BudgetExceeded, BudgetTracker, BudgetUsage, ToolTimer};
pub use cancel::CancelToken;
pub use identity::{AgentId, AgentIdentity, AgentKind};
pub use limits::MediaLimits;
pub use model::{
    Content, ContentPart, DeltaSink, ImageUrl, InputAudio, Message, Modalities, ModelBackend, ModelError, ModelNotice, ModelTurn, Role, ToolCall,
    VideoUrl,
};
pub use policy::{
    ActionRequest, AllowRule, ApprovalPrompt, CompanionScope, Decision, DenyReason, Policy, PolicyError, Scope, SharedPolicy, SiteLevel, SkipNotice,
    StandingAllow, TaskApprovals, Tier, site_of,
};
pub use remote::{Admitted, Authorize, Caller, RemoteError};
pub use session::{AgentConfig, AgentEvent, AgentSession, ApprovalAnswer, Approver, Outcome, PagesPrompt, StopReason};
pub use tool::{
    BudgetCharge, CallTarget, LocalTools, RegistryError, SourceKind, Tool, ToolAnnotations, ToolContext, ToolError, ToolOutput, ToolRegistry,
    ToolSource, ToolSpec,
};
pub use trace::{
    ApprovalOutcome, Citation, CitationError, CitationTarget, SessionMark, StepId, StepKind, Trace, TraceStep, retain_valid, validate_citations,
};
