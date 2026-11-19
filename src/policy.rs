//! Approval policy: tiers, per-companion per-site grants, task approvals and
//! standing "Always allow" rules.
//!
//! Rules (Companions design v1):
//! - Tier `Read` (attached content) always runs; the app decides what is attached.
//! - Tier `Never` is never offered.
//! - A site set to [`SiteLevel::Off`] blocks every navigate, research and interact
//!   step there; nothing overrides it.
//! - [`SiteLevel::Act`] lets navigate and interact run without asking on that site,
//!   for that companion only.
//! - Navigate and research ask once per task ([`Scope::Task`]) or can be covered by a
//!   standing allow ([`AllowRule`]); a skipped approval is reported as a [`SkipNotice`].
//! - Interact asks every time unless approved for this task on this site or the site is Act.
//! - Signed-in research is never covered by a standing allow; it needs a task approval
//!   given while signed-in was on.
//! - An all-companions allow covers an [`AgentKind::External`] agent only after the user
//!   approved one of its requests.
//! - Revoking takes effect at the next evaluation.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

use crate::identity::{AgentId, AgentIdentity, AgentKind};

/// How much a tool can do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Tier {
    /// Tier 0: reads only what the user attached.
    Read,
    /// Tier 1: navigates (open, switch, scroll, find, bookmark).
    Navigate,
    /// Tier R: background research and open-world reads.
    Research,
    /// Tier 2: changes page or world state (click, fill, submit, destructive tools).
    Interact,
    /// Tier 3: never available to agents.
    Never,
}

impl Tier {
    /// Default tier from MCP-style annotations: destructive → Interact,
    /// read-only closed-world → Read, read-only open-world → Research, otherwise Navigate.
    pub fn from_annotations(read_only: bool, destructive: bool, open_world: bool) -> Self {
        if destructive {
            Tier::Interact
        } else if read_only && !open_world {
            Tier::Read
        } else if read_only {
            Tier::Research
        } else {
            Tier::Navigate
        }
    }

    fn rank(self) -> u8 {
        match self {
            Tier::Read => 0,
            Tier::Navigate | Tier::Research => 1,
            Tier::Interact => 2,
            Tier::Never => 3,
        }
    }

    /// Whether `other` is the same as or strictly stricter than `self`.
    /// Users may make a tool stricter, never looser.
    pub fn allows_override_to(self, other: Tier) -> bool {
        other == self || other.rank() > self.rank()
    }
}

/// Per-companion, per-site permission level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum SiteLevel {
    Off,
    #[default]
    Ask,
    Act,
}

/// Which companions a standing allow covers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum CompanionScope {
    One(AgentId),
    All,
}

/// What a standing allow skips approval for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "tier")]
#[non_exhaustive]
pub enum AllowRule {
    /// Navigate on one site, or everywhere when `site` is `None`.
    Navigate { site: Option<String> },
    /// Research everywhere, optionally limited to one search provider.
    Research { provider: Option<String> },
}

/// A revocable "Always allow".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StandingAllow {
    pub id: u64,
    pub companion: CompanionScope,
    pub rule: AllowRule,
    pub created_unix_ms: u64,
}

/// What the user picked on an approval card.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "scope")]
#[non_exhaustive]
pub enum Scope {
    /// Just this step.
    Once,
    /// The rest of this task (for interact: on this site).
    Task,
    /// Navigate: Always allow on this site. Interact: set this site to Act for this companion.
    AlwaysSite { all_companions: bool },
    /// Navigate: Always allow everywhere.
    AlwaysEverywhere { all_companions: bool },
    /// Research: Always allow, optionally only with the request's provider.
    AlwaysResearch { all_companions: bool, provider_only: bool },
}

/// One step the agent wants to take, as the policy sees it.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ActionRequest {
    pub tier: Option<Tier>,
    /// Normalised host the step touches, if any (see [`site_of`]).
    pub site: Option<String>,
    /// Search provider for research steps.
    pub provider: Option<String>,
    /// Research that would use the user's signed-in session.
    pub signed_in: bool,
}

impl ActionRequest {
    pub fn new(tier: Tier) -> Self {
        Self { tier: Some(tier), ..Self::default() }
    }

    pub fn site(mut self, site: impl Into<String>) -> Self {
        self.site = Some(site.into());
        self
    }

    pub fn provider(mut self, provider: impl Into<String>) -> Self {
        self.provider = Some(provider.into());
        self
    }

    pub fn signed_in(mut self, on: bool) -> Self {
        self.signed_in = on;
        self
    }

    fn tier(&self) -> Tier {
        self.tier.unwrap_or(Tier::Never)
    }
}

/// Approvals that last until the task ends.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TaskApprovals {
    pub navigate: bool,
    pub research: bool,
    pub research_signed_in: bool,
    pub interact_sites: BTreeSet<String>,
}

/// An approval skipped because of a standing allow; shown as a notice with Stop.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkipNotice {
    pub allow_id: u64,
    pub tier: Tier,
    pub site: Option<String>,
    /// For research, the provider the query goes to.
    pub provider: Option<String>,
}

/// Why a step can't run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(rename_all = "snake_case", tag = "reason")]
#[non_exhaustive]
pub enum DenyReason {
    #[error("this kind of action is never available to agents")]
    Never,
    #[error("site {site} is set to Off")]
    SiteOff { site: String },
    #[error("the user declined")]
    Declined,
}

/// What the user is asked, with the scopes the card may offer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalPrompt {
    pub agent: AgentId,
    pub request: ActionRequest,
    pub offers: Vec<Scope>,
}

/// Result of evaluating one request.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Decision {
    Allow,
    AllowWithNotice(SkipNotice),
    Ask(ApprovalPrompt),
    Deny(DenyReason),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum PolicyError {
    #[error("scope {0:?} is not offered for this request")]
    ScopeNotOffered(Scope),
}

/// Persistent policy state for one user: grants, standing allows and which
/// External agents have been approved once. Serialize it to store or sync.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Policy {
    #[serde(default)]
    grants: BTreeMap<AgentId, BTreeMap<String, SiteLevel>>,
    #[serde(default)]
    allows: Vec<StandingAllow>,
    #[serde(default)]
    next_allow_id: u64,
    #[serde(default)]
    approved_external: BTreeSet<AgentId>,
}

/// Policy shared between running sessions and settings; a revoke is seen at the next step.
pub type SharedPolicy = Arc<RwLock<Policy>>;

impl Policy {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn shared(self) -> SharedPolicy {
        Arc::new(RwLock::new(self))
    }

    pub fn site_level(&self, agent: &AgentId, site: &str) -> SiteLevel {
        self.grants.get(agent).and_then(|m| m.get(&normalize_site(site))).copied().unwrap_or_default()
    }

    pub fn set_site_level(&mut self, agent: &AgentId, site: &str, level: SiteLevel) {
        let site = normalize_site(site);
        let map = self.grants.entry(agent.clone()).or_default();
        if level == SiteLevel::Ask {
            map.remove(&site);
        } else {
            map.insert(site, level);
        }
    }

    /// Per-site grants for one companion (non-default levels only).
    pub fn grants_for(&self, agent: &AgentId) -> impl Iterator<Item = (&str, SiteLevel)> {
        self.grants.get(agent).into_iter().flat_map(|m| m.iter().map(|(s, l)| (s.as_str(), *l)))
    }

    pub fn add_allow(&mut self, companion: CompanionScope, rule: AllowRule, now_unix_ms: u64) -> u64 {
        let rule = match rule {
            AllowRule::Navigate { site } => AllowRule::Navigate { site: site.map(|s| normalize_site(&s)) },
            r => r,
        };
        if let Some(existing) = self.allows.iter().find(|a| a.companion == companion && a.rule == rule) {
            return existing.id;
        }
        self.next_allow_id += 1;
        let id = self.next_allow_id;
        self.allows.push(StandingAllow { id, companion, rule, created_unix_ms: now_unix_ms });
        id
    }

    /// Every standing allow, for the settings list.
    pub fn allows(&self) -> &[StandingAllow] {
        &self.allows
    }

    /// Standing allows that name this companion directly or cover all companions.
    pub fn allows_for<'a>(&'a self, agent: &'a AgentId) -> impl Iterator<Item = &'a StandingAllow> {
        self.allows.iter().filter(move |a| match &a.companion {
            CompanionScope::One(id) => id == agent,
            CompanionScope::All => true,
        })
    }

    pub fn revoke(&mut self, allow_id: u64) -> bool {
        let before = self.allows.len();
        self.allows.retain(|a| a.id != allow_id);
        before != self.allows.len()
    }

    pub fn revoke_all(&mut self) {
        self.allows.clear();
    }

    pub fn is_external_approved(&self, agent: &AgentId) -> bool {
        self.approved_external.contains(agent)
    }

    /// Forgets everything about one companion (for example when it is removed).
    pub fn forget_agent(&mut self, agent: &AgentId) {
        self.grants.remove(agent);
        self.approved_external.remove(agent);
        self.allows.retain(|a| a.companion != CompanionScope::One(agent.clone()));
    }

    fn matching_allow(&self, who: &AgentIdentity, req: &ActionRequest) -> Option<&StandingAllow> {
        self.allows.iter().find(|a| {
            let companion_ok = match &a.companion {
                CompanionScope::One(id) => *id == who.id,
                CompanionScope::All => who.kind != AgentKind::External || self.approved_external.contains(&who.id),
            };
            companion_ok
                && match (&a.rule, req.tier()) {
                    (AllowRule::Navigate { site: None }, Tier::Navigate) => true,
                    (AllowRule::Navigate { site: Some(s) }, Tier::Navigate) => req.site.as_deref().map(normalize_site).as_deref() == Some(s),
                    (AllowRule::Research { provider }, Tier::Research) if !req.signed_in => {
                        provider.is_none() || provider.as_deref() == req.provider.as_deref()
                    }
                    _ => false,
                }
        })
    }

    /// Scopes an approval card may offer for this request.
    pub fn offers(&self, req: &ActionRequest) -> Vec<Scope> {
        let has_site = req.site.is_some();
        match req.tier() {
            Tier::Navigate => {
                let mut v = vec![Scope::Once, Scope::Task];
                if has_site {
                    v.push(Scope::AlwaysSite { all_companions: false });
                    v.push(Scope::AlwaysSite { all_companions: true });
                }
                v.push(Scope::AlwaysEverywhere { all_companions: false });
                v.push(Scope::AlwaysEverywhere { all_companions: true });
                v
            }
            Tier::Research if req.signed_in => vec![Scope::Once, Scope::Task],
            Tier::Research => {
                let mut v = vec![Scope::Once, Scope::Task];
                for all in [false, true] {
                    v.push(Scope::AlwaysResearch { all_companions: all, provider_only: false });
                    if req.provider.is_some() {
                        v.push(Scope::AlwaysResearch { all_companions: all, provider_only: true });
                    }
                }
                v
            }
            Tier::Interact if has_site => vec![Scope::Once, Scope::Task, Scope::AlwaysSite { all_companions: false }],
            Tier::Interact => vec![Scope::Once],
            Tier::Read | Tier::Never => Vec::new(),
        }
    }

    /// Decides whether `who` may take the step now.
    pub fn evaluate(&self, who: &AgentIdentity, req: &ActionRequest, task: &TaskApprovals) -> Decision {
        let tier = req.tier();
        match tier {
            Tier::Never => return Decision::Deny(DenyReason::Never),
            Tier::Read => return Decision::Allow,
            _ => {}
        }
        let level = req.site.as_deref().map(|s| self.site_level(&who.id, s));
        if level == Some(SiteLevel::Off) {
            return Decision::Deny(DenyReason::SiteOff { site: normalize_site(req.site.as_deref().unwrap_or_default()) });
        }
        let ask = || Decision::Ask(ApprovalPrompt { agent: who.id.clone(), request: req.clone(), offers: self.offers(req) });
        let notice = |a: &StandingAllow| {
            Decision::AllowWithNotice(SkipNotice {
                allow_id: a.id,
                tier,
                site: req.site.as_deref().map(normalize_site),
                provider: req.provider.clone(),
            })
        };
        match tier {
            Tier::Navigate => {
                if level == Some(SiteLevel::Act) || task.navigate {
                    Decision::Allow
                } else if let Some(a) = self.matching_allow(who, req) {
                    notice(a)
                } else {
                    ask()
                }
            }
            Tier::Research => {
                if req.signed_in {
                    if task.research_signed_in { Decision::Allow } else { ask() }
                } else if task.research {
                    Decision::Allow
                } else if let Some(a) = self.matching_allow(who, req) {
                    notice(a)
                } else {
                    ask()
                }
            }
            Tier::Interact => {
                let site_ok = req.site.as_deref().is_some_and(|s| task.interact_sites.contains(&normalize_site(s)));
                if level == Some(SiteLevel::Act) || site_ok { Decision::Allow } else { ask() }
            }
            Tier::Read | Tier::Never => unreachable!(),
        }
    }

    /// Records the user's answer to an approval card.
    pub fn apply(
        &mut self,
        who: &AgentIdentity,
        req: &ActionRequest,
        scope: &Scope,
        task: &mut TaskApprovals,
        now_unix_ms: u64,
    ) -> Result<(), PolicyError> {
        if !self.offers(req).contains(scope) {
            return Err(PolicyError::ScopeNotOffered(scope.clone()));
        }
        if who.kind == AgentKind::External {
            self.approved_external.insert(who.id.clone());
        }
        let companion = |all: bool| if all { CompanionScope::All } else { CompanionScope::One(who.id.clone()) };
        let site = req.site.as_deref().map(normalize_site);
        match (req.tier(), scope) {
            (_, Scope::Once) => {}
            (Tier::Navigate, Scope::Task) => task.navigate = true,
            (Tier::Research, Scope::Task) => {
                task.research = true;
                if req.signed_in {
                    task.research_signed_in = true;
                }
            }
            (Tier::Interact, Scope::Task) => {
                if let Some(s) = site {
                    task.interact_sites.insert(s);
                }
            }
            (Tier::Navigate, Scope::AlwaysSite { all_companions }) => {
                self.add_allow(companion(*all_companions), AllowRule::Navigate { site }, now_unix_ms);
            }
            (Tier::Interact, Scope::AlwaysSite { .. }) => {
                if let Some(s) = site {
                    self.set_site_level(&who.id, &s, SiteLevel::Act);
                }
            }
            (Tier::Navigate, Scope::AlwaysEverywhere { all_companions }) => {
                self.add_allow(companion(*all_companions), AllowRule::Navigate { site: None }, now_unix_ms);
            }
            (Tier::Research, Scope::AlwaysResearch { all_companions, provider_only }) => {
                let provider = if *provider_only { req.provider.clone() } else { None };
                self.add_allow(companion(*all_companions), AllowRule::Research { provider }, now_unix_ms);
            }
            _ => return Err(PolicyError::ScopeNotOffered(scope.clone())),
        }
        Ok(())
    }
}

fn normalize_site(site: &str) -> String {
    let s = site.trim().trim_end_matches('.').to_ascii_lowercase();
    s.strip_prefix("www.").map(str::to_string).unwrap_or(s)
}

/// Host part of a URL, lower-cased and without `www.`, used as the site key.
/// Returns `None` for URLs without a host (for example `about:blank`).
pub fn site_of(url: &str) -> Option<String> {
    let rest = url.split_once("://")?.1;
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    let host_port = authority.rsplit('@').next().unwrap_or_default();
    let host = if let Some(stripped) = host_port.strip_prefix('[') {
        stripped.split(']').next().unwrap_or_default()
    } else {
        host_port.split(':').next().unwrap_or_default()
    };
    if host.is_empty() { None } else { Some(normalize_site(host)) }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn atlas() -> AgentIdentity {
        AgentIdentity::new("atlas", "Atlas", AgentKind::Local)
    }
    fn scout() -> AgentIdentity {
        AgentIdentity::new("scout", "Scout", AgentKind::Node)
    }
    fn ext() -> AgentIdentity {
        AgentIdentity::new("mouser", "Mouser", AgentKind::External)
    }
    fn nav(site: &str) -> ActionRequest {
        ActionRequest::new(Tier::Navigate).site(site)
    }
    fn research() -> ActionRequest {
        ActionRequest::new(Tier::Research).provider("duckduckgo")
    }
    fn is_ask(d: &Decision) -> bool {
        matches!(d, Decision::Ask(_))
    }

    #[test]
    fn tiers_from_annotations_and_overrides() {
        assert_eq!(Tier::from_annotations(true, false, false), Tier::Read);
        assert_eq!(Tier::from_annotations(true, false, true), Tier::Research);
        assert_eq!(Tier::from_annotations(false, true, true), Tier::Interact);
        assert_eq!(Tier::from_annotations(false, false, false), Tier::Navigate);
        assert!(Tier::Read.allows_override_to(Tier::Interact));
        assert!(Tier::Interact.allows_override_to(Tier::Never));
        assert!(!Tier::Interact.allows_override_to(Tier::Navigate));
        assert!(!Tier::Navigate.allows_override_to(Tier::Research));
    }

    #[test]
    fn site_of_parses_hosts() {
        assert_eq!(site_of("https://www.Tokio.rs/tokio/tutorial?x#y").as_deref(), Some("tokio.rs"));
        assert_eq!(site_of("http://user:pw@example.com:8080/a").as_deref(), Some("example.com"));
        assert_eq!(site_of("http://[::1]:3000/").as_deref(), Some("::1"));
        assert_eq!(site_of("about:blank"), None);
        assert_eq!(site_of("file:///etc/hosts"), None);
    }

    #[test]
    fn read_always_never_never() {
        let p = Policy::new();
        let t = TaskApprovals::default();
        assert_eq!(p.evaluate(&atlas(), &ActionRequest::new(Tier::Read), &t), Decision::Allow);
        assert_eq!(p.evaluate(&atlas(), &ActionRequest::new(Tier::Never), &t), Decision::Deny(DenyReason::Never));
        assert!(p.offers(&ActionRequest::new(Tier::Never)).is_empty());
    }

    #[test]
    fn navigate_asks_then_task_approval_covers_rest_of_task() {
        let mut p = Policy::new();
        let mut t = TaskApprovals::default();
        let d = p.evaluate(&atlas(), &nav("tokio.rs"), &t);
        let Decision::Ask(prompt) = d else { panic!("expected ask") };
        assert!(prompt.offers.contains(&Scope::AlwaysSite { all_companions: false }));
        p.apply(&atlas(), &nav("tokio.rs"), &Scope::Task, &mut t, 0).unwrap();
        assert_eq!(p.evaluate(&atlas(), &nav("docs.rs"), &t), Decision::Allow);
        // A new task asks again.
        assert!(is_ask(&p.evaluate(&atlas(), &nav("docs.rs"), &TaskApprovals::default())));
    }

    #[test]
    fn once_does_not_persist() {
        let mut p = Policy::new();
        let mut t = TaskApprovals::default();
        p.apply(&atlas(), &nav("tokio.rs"), &Scope::Once, &mut t, 0).unwrap();
        assert!(is_ask(&p.evaluate(&atlas(), &nav("tokio.rs"), &t)));
    }

    #[test]
    fn always_allow_navigate_site_is_per_companion_and_gives_notice() {
        let mut p = Policy::new();
        let mut t = TaskApprovals::default();
        p.apply(&atlas(), &nav("www.tokio.rs"), &Scope::AlwaysSite { all_companions: false }, &mut t, 5).unwrap();
        let fresh = TaskApprovals::default();
        match p.evaluate(&atlas(), &nav("tokio.rs"), &fresh) {
            Decision::AllowWithNotice(n) => {
                assert_eq!(n.tier, Tier::Navigate);
                assert_eq!(n.site.as_deref(), Some("tokio.rs"));
            }
            d => panic!("{d:?}"),
        }
        assert!(is_ask(&p.evaluate(&atlas(), &nav("docs.rs"), &fresh)));
        assert!(is_ask(&p.evaluate(&scout(), &nav("tokio.rs"), &fresh)));
        assert_eq!(p.allows()[0].created_unix_ms, 5);
    }

    #[test]
    fn always_allow_everywhere_for_all_companions() {
        let mut p = Policy::new();
        let mut t = TaskApprovals::default();
        p.apply(&atlas(), &nav("a.com"), &Scope::AlwaysEverywhere { all_companions: true }, &mut t, 0).unwrap();
        let fresh = TaskApprovals::default();
        assert!(matches!(p.evaluate(&scout(), &nav("b.com"), &fresh), Decision::AllowWithNotice(_)));
        // Duplicate allows are merged.
        p.add_allow(CompanionScope::All, AllowRule::Navigate { site: None }, 1);
        assert_eq!(p.allows().len(), 1);
    }

    #[test]
    fn all_companions_allow_covers_external_only_after_first_approval() {
        let mut p = Policy::new();
        p.add_allow(CompanionScope::All, AllowRule::Navigate { site: None }, 0);
        let mut t = TaskApprovals::default();
        assert!(is_ask(&p.evaluate(&ext(), &nav("a.com"), &t)));
        p.apply(&ext(), &nav("a.com"), &Scope::Once, &mut t, 0).unwrap();
        assert!(p.is_external_approved(&ext().id));
        assert!(matches!(p.evaluate(&ext(), &nav("b.com"), &TaskApprovals::default()), Decision::AllowWithNotice(_)));
    }

    #[test]
    fn off_site_beats_everything() {
        let mut p = Policy::new();
        p.add_allow(CompanionScope::All, AllowRule::Navigate { site: None }, 0);
        p.add_allow(CompanionScope::All, AllowRule::Research { provider: None }, 0);
        p.set_site_level(&atlas().id, "evil.com", SiteLevel::Off);
        let t = TaskApprovals { navigate: true, research: true, research_signed_in: true, interact_sites: ["evil.com".into()].into() };
        for tier in [Tier::Navigate, Tier::Research, Tier::Interact] {
            assert_eq!(
                p.evaluate(&atlas(), &ActionRequest::new(tier).site("evil.com"), &t),
                Decision::Deny(DenyReason::SiteOff { site: "evil.com".into() })
            );
        }
        // Off is per companion.
        assert_eq!(p.evaluate(&scout(), &nav("evil.com"), &t), Decision::Allow);
    }

    #[test]
    fn act_site_runs_navigate_and_interact_without_asking() {
        let mut p = Policy::new();
        p.set_site_level(&atlas().id, "github.com", SiteLevel::Act);
        let t = TaskApprovals::default();
        assert_eq!(p.evaluate(&atlas(), &nav("github.com"), &t), Decision::Allow);
        assert_eq!(p.evaluate(&atlas(), &ActionRequest::new(Tier::Interact).site("github.com"), &t), Decision::Allow);
        assert!(is_ask(&p.evaluate(&scout(), &ActionRequest::new(Tier::Interact).site("github.com"), &t)));
        // Resetting to Ask removes the grant.
        p.set_site_level(&atlas().id, "github.com", SiteLevel::Ask);
        assert_eq!(p.grants_for(&atlas().id).count(), 0);
    }

    #[test]
    fn interact_scopes() {
        let mut p = Policy::new();
        let mut t = TaskApprovals::default();
        let req = ActionRequest::new(Tier::Interact).site("github.com");
        let Decision::Ask(prompt) = p.evaluate(&atlas(), &req, &t) else { panic!() };
        assert_eq!(prompt.offers, vec![Scope::Once, Scope::Task, Scope::AlwaysSite { all_companions: false }]);
        // Always allow everywhere is never offered for interact.
        assert_eq!(
            p.apply(&atlas(), &req, &Scope::AlwaysEverywhere { all_companions: false }, &mut t, 0),
            Err(PolicyError::ScopeNotOffered(Scope::AlwaysEverywhere { all_companions: false }))
        );
        p.apply(&atlas(), &req, &Scope::Task, &mut t, 0).unwrap();
        assert_eq!(p.evaluate(&atlas(), &req, &t), Decision::Allow);
        assert!(is_ask(&p.evaluate(&atlas(), &ActionRequest::new(Tier::Interact).site("gitlab.com"), &t)));
        p.apply(&atlas(), &req, &Scope::AlwaysSite { all_companions: false }, &mut t, 0).unwrap();
        assert_eq!(p.site_level(&atlas().id, "github.com"), SiteLevel::Act);
        // Interact without a site only offers Once.
        assert_eq!(p.offers(&ActionRequest::new(Tier::Interact)), vec![Scope::Once]);
    }

    #[test]
    fn research_always_allow_with_provider_limit() {
        let mut p = Policy::new();
        let mut t = TaskApprovals::default();
        p.apply(&atlas(), &research(), &Scope::AlwaysResearch { all_companions: false, provider_only: true }, &mut t, 0).unwrap();
        let fresh = TaskApprovals::default();
        match p.evaluate(&atlas(), &research(), &fresh) {
            Decision::AllowWithNotice(n) => assert_eq!(n.provider.as_deref(), Some("duckduckgo")),
            d => panic!("{d:?}"),
        }
        assert!(is_ask(&p.evaluate(&atlas(), &ActionRequest::new(Tier::Research).provider("google"), &fresh)));
    }

    #[test]
    fn signed_in_research_is_never_covered_by_always_allow() {
        let mut p = Policy::new();
        p.add_allow(CompanionScope::All, AllowRule::Research { provider: None }, 0);
        let mut t = TaskApprovals { research: true, ..Default::default() };
        let req = research().signed_in(true);
        let Decision::Ask(prompt) = p.evaluate(&atlas(), &req, &t) else { panic!() };
        assert_eq!(prompt.offers, vec![Scope::Once, Scope::Task]);
        assert!(p.apply(&atlas(), &req, &Scope::AlwaysResearch { all_companions: false, provider_only: false }, &mut t, 0).is_err());
        p.apply(&atlas(), &req, &Scope::Task, &mut t, 0).unwrap();
        assert_eq!(p.evaluate(&atlas(), &req, &t), Decision::Allow);
    }

    #[test]
    fn revoke_takes_effect_on_next_evaluation() {
        let mut p = Policy::new();
        let id = p.add_allow(CompanionScope::One(atlas().id), AllowRule::Navigate { site: None }, 0);
        let t = TaskApprovals::default();
        assert!(matches!(p.evaluate(&atlas(), &nav("a.com"), &t), Decision::AllowWithNotice(_)));
        assert!(p.revoke(id));
        assert!(!p.revoke(id));
        assert!(is_ask(&p.evaluate(&atlas(), &nav("a.com"), &t)));
        p.add_allow(CompanionScope::All, AllowRule::Navigate { site: None }, 0);
        p.revoke_all();
        assert!(p.allows().is_empty());
    }

    #[test]
    fn forget_agent_and_serde_roundtrip() {
        let mut p = Policy::new();
        p.set_site_level(&atlas().id, "a.com", SiteLevel::Act);
        p.add_allow(CompanionScope::One(atlas().id), AllowRule::Navigate { site: Some("A.com".into()) }, 7);
        p.add_allow(CompanionScope::All, AllowRule::Research { provider: Some("kagi".into()) }, 8);
        let json = serde_json::to_string(&p).unwrap();
        let back: Policy = serde_json::from_str(&json).unwrap();
        assert_eq!(back, p);
        assert_eq!(back.allows_for(&atlas().id).count(), 2);
        let mut p = back;
        p.forget_agent(&atlas().id);
        assert_eq!(p.allows().len(), 1);
        assert_eq!(p.site_level(&atlas().id, "a.com"), SiteLevel::Ask);
    }
}
