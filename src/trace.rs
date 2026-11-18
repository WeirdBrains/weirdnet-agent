//! Traces, steps and citations.
//!
//! Every search, fetch, tool call, approval and notice in a task becomes a
//! [`TraceStep`]. Answers cite sources with [`Citation`]s that name the steps
//! behind them; [`validate_citations`] rejects any cited URL that no listed
//! fetch step retrieved, and [`retain_valid`] drops such citations before display.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::identity::AgentId;
use crate::policy::{DenyReason, Scope, Tier};

/// Index of a step in its trace, starting at 1 (as shown in footnotes).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StepId(pub u32);

/// Which browsing session a step used.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "session")]
pub enum SessionMark {
    #[default]
    LoggedOut,
    SignedIn {
        profile: String,
    },
}

/// How an approval was resolved.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "outcome")]
pub enum ApprovalOutcome {
    Approved {
        scope: Scope,
    },
    /// Skipped because of a site grant or task approval (`allow_id` is `None`) or a standing allow.
    AutoAllowed {
        allow_id: Option<u64>,
    },
    Denied,
    Blocked {
        reason: DenyReason,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum StepKind {
    Model { model: String, tool_calls: u32 },
    Search { provider: String, query: String, results: u32 },
    Fetch { url: String, status: Option<u16>, bytes: u64 },
    Extract { url: String, chars: u64 },
    Tool { source: String, tool: String, args_summary: String, result_bytes: u64, is_error: bool },
    Approval { tool: String, tier: Tier, outcome: ApprovalOutcome },
    Skipped { reason: String, url: Option<String> },
    Notice { text: String },
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceStep {
    pub id: StepId,
    pub at_unix_ms: u64,
    pub agent: AgentId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    #[serde(default)]
    pub session: SessionMark,
    #[serde(flatten)]
    pub kind: StepKind,
}

/// The ordered record of one task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Trace {
    pub task_id: String,
    pub agent: AgentId,
    pub steps: Vec<TraceStep>,
}

impl Trace {
    pub fn new(task_id: impl Into<String>, agent: AgentId) -> Self {
        Self { task_id: task_id.into(), agent, steps: Vec::new() }
    }

    /// Appends a step and returns its id.
    pub fn push(&mut self, kind: StepKind, session: SessionMark, node: Option<String>, at_unix_ms: u64) -> StepId {
        let id = StepId(self.steps.len() as u32 + 1);
        self.steps.push(TraceStep { id, at_unix_ms, agent: self.agent.clone(), node, session, kind });
        id
    }

    pub fn step(&self, id: StepId) -> Option<&TraceStep> {
        id.0.checked_sub(1).and_then(|i| self.steps.get(i as usize))
    }

    /// Fetched URLs and the steps that fetched them.
    pub fn fetched_urls(&self) -> BTreeMap<String, Vec<StepId>> {
        let mut out: BTreeMap<String, Vec<StepId>> = BTreeMap::new();
        for s in &self.steps {
            if let StepKind::Fetch { url, .. } = &s.kind {
                out.entry(normalize_url(url)).or_default().push(s.id);
            }
        }
        out
    }

    /// Counts for the trace summary line ("9 steps · 1 search · 1 MCP call · 4 pages").
    pub fn summary(&self) -> TraceSummary {
        let mut s = TraceSummary { steps: self.steps.len() as u32, ..Default::default() };
        for step in &self.steps {
            match step.kind {
                StepKind::Search { .. } => s.searches += 1,
                StepKind::Fetch { .. } => s.pages += 1,
                StepKind::Tool { .. } => s.tool_calls += 1,
                _ => {}
            }
        }
        s
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TraceSummary {
    pub steps: u32,
    pub searches: u32,
    pub pages: u32,
    pub tool_calls: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum CitationTarget {
    Url { url: String, quote: Option<String> },
    ToolResult { source: String, tool: String },
}

/// A footnote `[n]` in an answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Citation {
    pub n: u32,
    pub target: CitationTarget,
    pub steps: Vec<StepId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(rename_all = "snake_case", tag = "error")]
pub enum CitationError {
    #[error("citation [{n}] names no steps")]
    NoSteps { n: u32 },
    #[error("citation [{n}] names step {step:?}, which is not in the trace")]
    UnknownStep { n: u32, step: StepId },
    #[error("citation [{n}] cites {url}, which none of its steps fetched")]
    UrlNotFetched { n: u32, url: String },
    #[error("citation [{n}] cites {tool_source}.{tool}, which none of its steps called")]
    ToolNotCalled { n: u32, tool_source: String, tool: String },
}

fn normalize_url(url: &str) -> String {
    let no_frag = url.split('#').next().unwrap_or_default();
    let trimmed = no_frag.strip_suffix('/').unwrap_or(no_frag);
    match trimmed.split_once("://") {
        Some((scheme, rest)) => {
            let (host, path) = rest.split_at(rest.find('/').unwrap_or(rest.len()));
            format!("{}://{}{}", scheme.to_ascii_lowercase(), host.to_ascii_lowercase(), path)
        }
        None => trimmed.to_string(),
    }
}

fn check(trace: &Trace, c: &Citation) -> Option<CitationError> {
    if c.steps.is_empty() {
        return Some(CitationError::NoSteps { n: c.n });
    }
    let mut steps = Vec::with_capacity(c.steps.len());
    for id in &c.steps {
        match trace.step(*id) {
            Some(s) => steps.push(s),
            None => return Some(CitationError::UnknownStep { n: c.n, step: *id }),
        }
    }
    match &c.target {
        CitationTarget::Url { url, .. } => {
            let want = normalize_url(url);
            let ok = steps.iter().any(|s| matches!(&s.kind, StepKind::Fetch { url, .. } if normalize_url(url) == want));
            (!ok).then(|| CitationError::UrlNotFetched { n: c.n, url: url.clone() })
        }
        CitationTarget::ToolResult { source, tool } => {
            let ok =
                steps.iter().any(|s| matches!(&s.kind, StepKind::Tool { source: s2, tool: t2, is_error: false, .. } if s2 == source && t2 == tool));
            (!ok).then(|| CitationError::ToolNotCalled { n: c.n, tool_source: source.clone(), tool: tool.clone() })
        }
    }
}

/// Checks that every citation maps to steps in `trace`: each cited URL must be
/// fetched by one of its listed steps, and each cited tool result must come from
/// one of its listed (successful) tool steps.
pub fn validate_citations(trace: &Trace, citations: &[Citation]) -> Result<(), Vec<CitationError>> {
    let errors: Vec<_> = citations.iter().filter_map(|c| check(trace, c)).collect();
    if errors.is_empty() { Ok(()) } else { Err(errors) }
}

/// Keeps only valid citations: a citation with no backing step is never shown.
pub fn retain_valid(trace: &Trace, citations: Vec<Citation>) -> Vec<Citation> {
    citations.into_iter().filter(|c| check(trace, c).is_none()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Trace {
        let mut t = Trace::new("7f3a", AgentId::new("atlas"));
        let lo = SessionMark::LoggedOut;
        t.push(StepKind::Search { provider: "duckduckgo".into(), query: "tokio cancellation".into(), results: 10 }, lo.clone(), None, 1);
        t.push(StepKind::Fetch { url: "https://Tokio.rs/tokio/topics/shutdown/".into(), status: Some(200), bytes: 9000 }, lo.clone(), None, 2);
        t.push(StepKind::Extract { url: "https://tokio.rs/tokio/topics/shutdown".into(), chars: 610 }, lo.clone(), None, 3);
        t.push(
            StepKind::Tool {
                source: "mouser-tools".into(),
                tool: "search_issues".into(),
                args_summary: "q=cancel".into(),
                result_bytes: 300,
                is_error: false,
            },
            lo,
            Some("desktop".into()),
            4,
        );
        t.push(
            StepKind::Fetch { url: "https://docs.rs/tokio".into(), status: Some(200), bytes: 1 },
            SessionMark::SignedIn { profile: "default".into() },
            None,
            5,
        );
        t
    }

    fn url(n: u32, u: &str, steps: &[u32]) -> Citation {
        Citation { n, target: CitationTarget::Url { url: u.into(), quote: None }, steps: steps.iter().map(|s| StepId(*s)).collect() }
    }

    #[test]
    fn valid_citations_pass() {
        let t = sample();
        let cites = vec![
            url(1, "https://tokio.rs/tokio/topics/shutdown#graceful", &[2, 3]),
            Citation {
                n: 2,
                target: CitationTarget::ToolResult { source: "mouser-tools".into(), tool: "search_issues".into() },
                steps: vec![StepId(4)],
            },
            url(3, "https://docs.rs/tokio/", &[5]),
        ];
        assert_eq!(validate_citations(&t, &cites), Ok(()));
    }

    #[test]
    fn invalid_citations_are_reported_and_dropped() {
        let t = sample();
        let cites = vec![
            url(1, "https://tokio.rs/tokio/topics/shutdown", &[3]), // extract only, no fetch
            url(2, "https://evil.example/", &[2]),
            url(3, "https://docs.rs/tokio", &[]),
            url(4, "https://docs.rs/tokio", &[99]),
            Citation { n: 5, target: CitationTarget::ToolResult { source: "x".into(), tool: "y".into() }, steps: vec![StepId(4)] },
            url(6, "https://docs.rs/tokio", &[5]),
        ];
        let errs = validate_citations(&t, &cites).unwrap_err();
        assert_eq!(errs.len(), 5);
        assert!(matches!(errs[0], CitationError::UrlNotFetched { n: 1, .. }));
        assert!(matches!(errs[1], CitationError::UrlNotFetched { n: 2, .. }));
        assert_eq!(errs[2], CitationError::NoSteps { n: 3 });
        assert_eq!(errs[3], CitationError::UnknownStep { n: 4, step: StepId(99) });
        assert!(matches!(errs[4], CitationError::ToolNotCalled { n: 5, .. }));
        let kept = retain_valid(&t, cites);
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].n, 6);
    }

    #[test]
    fn step_lookup_summary_and_fetched_urls() {
        let t = sample();
        assert!(t.step(StepId(0)).is_none());
        assert_eq!(t.step(StepId(4)).unwrap().node.as_deref(), Some("desktop"));
        let s = t.summary();
        assert_eq!((s.steps, s.searches, s.pages, s.tool_calls), (5, 1, 2, 1));
        let urls = t.fetched_urls();
        assert_eq!(urls["https://tokio.rs/tokio/topics/shutdown"], vec![StepId(2)]);
    }

    #[test]
    fn serde_roundtrip_keeps_session_marks() {
        let t = sample();
        let json = serde_json::to_string(&t).unwrap();
        assert!(json.contains("\"kind\":\"fetch\""));
        assert!(json.contains("\"session\":\"signed_in\""));
        let back: Trace = serde_json::from_str(&json).unwrap();
        assert_eq!(back, t);
    }
}
