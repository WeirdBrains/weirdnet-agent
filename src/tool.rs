//! Tools, tool sources and the registry.
//!
//! A [`ToolSource`] is anything that lists and runs tools: [`LocalTools`] for
//! in-process tools today, MCP servers and remote nodes later. The
//! [`ToolRegistry`] merges sources, applies per-source and per-tool switches and
//! stricter tier overrides, and resolves a tool name to its source.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::cancel::CancelToken;
use crate::identity::AgentIdentity;
use crate::policy::{Tier, site_of};
use crate::trace::{SessionMark, StepKind};

/// Which budget counter a call draws from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BudgetCharge {
    #[default]
    None,
    Search,
    Page,
}

/// MCP-style hints used to derive the default tier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ToolAnnotations {
    pub read_only: bool,
    pub destructive: bool,
    pub open_world: bool,
}

/// What the model sees about a tool, plus how the runtime treats it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolSpec {
    pub name: String,
    pub description: String,
    /// JSON Schema for the arguments.
    pub input_schema: Value,
    pub annotations: ToolAnnotations,
    pub tier: Tier,
    #[serde(default)]
    pub charge: BudgetCharge,
}

impl ToolSpec {
    /// A tool with an empty object schema and the conservative default tier (Navigate).
    pub fn new(name: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            input_schema: serde_json::json!({ "type": "object", "properties": {} }),
            annotations: ToolAnnotations::default(),
            tier: Tier::Navigate,
            charge: BudgetCharge::None,
        }
    }

    pub fn schema(mut self, schema: Value) -> Self {
        self.input_schema = schema;
        self
    }

    /// Sets annotations and derives the tier from them.
    pub fn annotations(mut self, a: ToolAnnotations) -> Self {
        self.annotations = a;
        self.tier = Tier::from_annotations(a.read_only, a.destructive, a.open_world);
        self
    }

    /// Sets the tier directly (for first-party tools that know their tier).
    pub fn tier(mut self, tier: Tier) -> Self {
        self.tier = tier;
        self
    }

    pub fn charge(mut self, charge: BudgetCharge) -> Self {
        self.charge = charge;
        self
    }
}

/// What a call touches, so the policy can check site grants and providers.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct CallTarget {
    pub url: Option<String>,
    pub site: Option<String>,
    pub provider: Option<String>,
}

impl CallTarget {
    pub fn url(url: impl Into<String>) -> Self {
        let url = url.into();
        Self { site: site_of(&url), url: Some(url), provider: None }
    }

    pub fn provider(provider: impl Into<String>) -> Self {
        Self { provider: Some(provider.into()), ..Self::default() }
    }

    /// Site from `site`, else from `url`.
    pub fn resolved_site(&self) -> Option<String> {
        self.site.clone().or_else(|| self.url.as_deref().and_then(site_of))
    }
}

/// Passed to every call.
#[derive(Debug, Clone)]
pub struct ToolContext {
    pub agent: AgentIdentity,
    pub task_id: String,
    pub session: SessionMark,
    pub cancel: CancelToken,
}

/// A tool's result. `steps` are recorded in the trace in order; when empty the
/// runtime records one generic tool step.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct ToolOutput {
    pub content: String,
    #[serde(default)]
    pub is_error: bool,
    #[serde(default)]
    pub steps: Vec<StepKind>,
}

impl ToolOutput {
    pub fn text(content: impl Into<String>) -> Self {
        Self { content: content.into(), ..Self::default() }
    }

    pub fn error(content: impl Into<String>) -> Self {
        Self { content: content.into(), is_error: true, steps: Vec::new() }
    }

    pub fn with_step(mut self, step: StepKind) -> Self {
        self.steps.push(step);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(rename_all = "snake_case", tag = "error", content = "detail")]
pub enum ToolError {
    #[error("unknown tool {0}")]
    NotFound(String),
    #[error("tool {0} is turned off")]
    Disabled(String),
    #[error("invalid arguments: {0}")]
    InvalidArgs(String),
    #[error("{0}")]
    Failed(String),
    #[error("timed out")]
    Timeout,
    #[error("cancelled")]
    Cancelled,
}

/// One in-process tool.
#[async_trait]
pub trait Tool: Send + Sync {
    fn spec(&self) -> ToolSpec;

    /// What a call with these arguments touches. Defaults to nothing.
    fn target(&self, _args: &Value) -> CallTarget {
        CallTarget::default()
    }

    async fn call(&self, ctx: &ToolContext, args: Value) -> Result<ToolOutput, ToolError>;
}

/// Where tools come from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceKind {
    Local,
    Mcp,
    Node,
}

/// A set of tools behind one connection (in-process, an MCP server, a remote node).
#[async_trait]
pub trait ToolSource: Send + Sync {
    fn id(&self) -> &str;
    fn kind(&self) -> SourceKind;
    async fn list(&self) -> Result<Vec<ToolSpec>, ToolError>;
    fn target(&self, _tool: &str, _args: &Value) -> CallTarget {
        CallTarget::default()
    }
    async fn call(&self, tool: &str, ctx: &ToolContext, args: Value) -> Result<ToolOutput, ToolError>;
}

/// In-process tools.
pub struct LocalTools {
    id: String,
    tools: BTreeMap<String, Arc<dyn Tool>>,
}

impl LocalTools {
    pub fn new(id: impl Into<String>) -> Self {
        Self { id: id.into(), tools: BTreeMap::new() }
    }

    pub fn with(mut self, tool: impl Tool + 'static) -> Self {
        self.add(Arc::new(tool));
        self
    }

    pub fn add(&mut self, tool: Arc<dyn Tool>) {
        self.tools.insert(tool.spec().name, tool);
    }
}

#[async_trait]
impl ToolSource for LocalTools {
    fn id(&self) -> &str {
        &self.id
    }

    fn kind(&self) -> SourceKind {
        SourceKind::Local
    }

    async fn list(&self) -> Result<Vec<ToolSpec>, ToolError> {
        Ok(self.tools.values().map(|t| t.spec()).collect())
    }

    fn target(&self, tool: &str, args: &Value) -> CallTarget {
        self.tools.get(tool).map(|t| t.target(args)).unwrap_or_default()
    }

    async fn call(&self, tool: &str, ctx: &ToolContext, args: Value) -> Result<ToolOutput, ToolError> {
        let t = self.tools.get(tool).ok_or_else(|| ToolError::NotFound(tool.to_string()))?;
        t.call(ctx, args).await
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RegistryError {
    #[error("tool {tool} is offered by both {first} and {second}")]
    Duplicate { tool: String, first: String, second: String },
    #[error("source {source_id} failed to list tools: {error}")]
    List { source_id: String, error: ToolError },
    #[error("tier override for {tool} would loosen it")]
    LooserTier { tool: String },
}

/// Merged view of all sources for one session.
#[derive(Default)]
pub struct ToolRegistry {
    sources: Vec<Arc<dyn ToolSource>>,
    index: BTreeMap<String, (usize, ToolSpec)>,
    disabled_sources: BTreeSet<String>,
    disabled_tools: BTreeSet<String>,
    tier_overrides: BTreeMap<String, Tier>,
}

impl ToolRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a source. Call [`refresh`](Self::refresh) afterwards.
    pub fn add_source(&mut self, source: Arc<dyn ToolSource>) {
        self.sources.push(source);
    }

    pub fn source_ids(&self) -> impl Iterator<Item = &str> {
        self.sources.iter().map(|s| s.id())
    }

    /// Re-lists every source. Tool names must be unique across sources.
    pub async fn refresh(&mut self) -> Result<(), RegistryError> {
        let mut index = BTreeMap::new();
        for (i, src) in self.sources.iter().enumerate() {
            let specs = src.list().await.map_err(|error| RegistryError::List { source_id: src.id().to_string(), error })?;
            for spec in specs {
                if let Some((j, _)) = index.get(&spec.name) {
                    let first: &Arc<dyn ToolSource> = &self.sources[*j];
                    return Err(RegistryError::Duplicate { tool: spec.name, first: first.id().to_string(), second: src.id().to_string() });
                }
                index.insert(spec.name.clone(), (i, spec));
            }
        }
        self.index = index;
        Ok(())
    }

    pub fn set_source_enabled(&mut self, source_id: &str, on: bool) {
        if on {
            self.disabled_sources.remove(source_id);
        } else {
            self.disabled_sources.insert(source_id.to_string());
        }
    }

    pub fn set_tool_enabled(&mut self, tool: &str, on: bool) {
        if on {
            self.disabled_tools.remove(tool);
        } else {
            self.disabled_tools.insert(tool.to_string());
        }
    }

    /// Makes a tool stricter. Loosening (including below Interact for destructive tools) is refused.
    pub fn set_tier_override(&mut self, tool: &str, tier: Tier) -> Result<(), RegistryError> {
        if let Some((_, spec)) = self.index.get(tool)
            && !spec.tier.allows_override_to(tier)
        {
            return Err(RegistryError::LooserTier { tool: tool.to_string() });
        }
        self.tier_overrides.insert(tool.to_string(), tier);
        Ok(())
    }

    fn effective(&self, idx: usize, spec: &ToolSpec) -> Option<ToolSpec> {
        let src = &self.sources[idx];
        if self.disabled_sources.contains(src.id()) || self.disabled_tools.contains(&spec.name) {
            return None;
        }
        let mut spec = spec.clone();
        if let Some(t) = self.tier_overrides.get(&spec.name)
            && spec.tier.allows_override_to(*t)
        {
            spec.tier = *t;
        }
        Some(spec)
    }

    /// Enabled tools offered to the model. Tier `Never` tools are never offered.
    pub fn specs(&self) -> Vec<ToolSpec> {
        self.index.values().filter_map(|(i, s)| self.effective(*i, s)).filter(|s| s.tier != Tier::Never).collect()
    }

    /// Resolves an enabled tool to its source and effective spec.
    pub fn lookup(&self, tool: &str) -> Result<(Arc<dyn ToolSource>, ToolSpec), ToolError> {
        let (i, spec) = self.index.get(tool).ok_or_else(|| ToolError::NotFound(tool.to_string()))?;
        let spec = self.effective(*i, spec).ok_or_else(|| ToolError::Disabled(tool.to_string()))?;
        Ok((self.sources[*i].clone(), spec))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::FnTool;

    fn src(id: &str, names: &[&str]) -> Arc<dyn ToolSource> {
        let mut l = LocalTools::new(id);
        for n in names {
            l.add(Arc::new(FnTool::new(ToolSpec::new(*n, "t"), |_| Ok(ToolOutput::text("ok")))));
        }
        Arc::new(l)
    }

    #[tokio::test]
    async fn registry_merges_switches_and_rejects_duplicates() {
        let mut r = ToolRegistry::new();
        r.add_source(src("browser", &["tabs.open", "page.click"]));
        r.add_source(src("mcp:mouser", &["search_issues"]));
        r.refresh().await.unwrap();
        assert_eq!(r.specs().len(), 3);
        r.set_source_enabled("mcp:mouser", false);
        assert_eq!(r.lookup("search_issues").err(), Some(ToolError::Disabled("search_issues".into())));
        r.set_source_enabled("mcp:mouser", true);
        r.set_tool_enabled("page.click", false);
        assert_eq!(r.specs().len(), 2);
        assert_eq!(r.lookup("nope").err(), Some(ToolError::NotFound("nope".into())));
        assert_eq!(r.source_ids().collect::<Vec<_>>(), vec!["browser", "mcp:mouser"]);

        r.add_source(src("other", &["tabs.open"]));
        assert!(matches!(r.refresh().await, Err(RegistryError::Duplicate { .. })));
    }

    #[tokio::test]
    async fn tier_overrides_only_tighten() {
        let mut l = LocalTools::new("mcp:x");
        l.add(Arc::new(FnTool::new(ToolSpec::new("delete", "d").annotations(ToolAnnotations { destructive: true, ..Default::default() }), |_| {
            Ok(ToolOutput::text(""))
        })));
        l.add(Arc::new(FnTool::new(ToolSpec::new("read", "r").annotations(ToolAnnotations { read_only: true, ..Default::default() }), |_| {
            Ok(ToolOutput::text(""))
        })));
        let mut r = ToolRegistry::new();
        r.add_source(Arc::new(l));
        r.refresh().await.unwrap();
        assert_eq!(r.lookup("delete").unwrap().1.tier, Tier::Interact);
        assert!(r.set_tier_override("delete", Tier::Navigate).is_err());
        r.set_tier_override("read", Tier::Interact).unwrap();
        assert_eq!(r.lookup("read").unwrap().1.tier, Tier::Interact);
        r.set_tier_override("delete", Tier::Never).unwrap();
        assert!(r.specs().iter().all(|s| s.name != "delete"));
    }

    #[test]
    fn call_target_resolves_site() {
        let t = CallTarget::url("https://www.Docs.rs/tokio");
        assert_eq!(t.resolved_site().as_deref(), Some("docs.rs"));
        assert_eq!(CallTarget::provider("kagi").resolved_site(), None);
    }
}
