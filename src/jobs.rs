//! Synced jobs (feature `jobs`): a typed job board over the weirdnet-sync job log, so a job one
//! device started can be picked up by another from its last checkpoint.
//!
//! [`JobBoard<P>`] is a thin, typed layer over `weirdnet_sync::Jobs` and `Leases::claim_job`,
//! not a second log: every job is signed [`JobEvent`]s in the op log, sealed and synced like
//! any other record type, and every device folds them into the same [`JobRecord<P>`]. Apps
//! add only their payload type `P` (for Ymir, a training job).
//!
//! - **Claims** are majority-vote leases (`<kind>/<job id>`) among the devices with the job's
//!   role. Their term is the fencing token ([`Claim::term`], [`Claim::fence`]). A [`Claim`]
//!   renews in the background; if its holder disappears, voters wait out
//!   [`JobPolicy::takeover_after_ms`] after the lease lapsed before another device may claim,
//!   so a network drop doesn't move a long job. The holder itself can renew back inside that
//!   wait.
//! - **Who may claim**: devices with [`JobPolicy::role`], narrowed by
//!   [`JobPolicy::candidates`], plus the app's own check passed to [`JobBoard::claim`].
//!   Claims from anyone else are ignored when folding and refused by voters.
//! - **Checkpoints** are pointers ([`CheckpointRef`]): a content hash, its size and the devices
//!   holding a copy. The app moves the bytes itself.
//! - **Retries**: [`Claim::fail`] with `retry: true` reopens the job after
//!   [`JobPolicy::retry_backoff_ms`] until [`JobPolicy::max_attempts`] claims were used
//!   (lapsed claims count); `retry: false` is final. [`Claim::pause`] and the owner's
//!   [`JobBoard::pause`] never count as an attempt.
//!
//! Voters: every device with the role runs a `weirdnet_sync::Voter` for it, with limits that
//! allow the job's claim time ([`JobPolicy::configure_voter`]). The `jobs` feature needs
//! weirdnet-sync 0.8.1 or newer.
use std::collections::{BTreeMap, VecDeque};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use weirdnet_net::Node;
use weirdnet_sync::{
    FenceGuard, Hlc, JobEvent, JobRules, JobSpec, JobStatus, JobView, Jobs, LeaseConfig, LeaseError, LeaseHandle, LeaseName, Leases, Stale,
    SyncState, Term, Voter, Voters, job_lease,
};

/// A device's peer id, as in the weirdnet-sync roster.
pub type PeerId = String;
/// Largest encoded payload.
pub const MAX_PAYLOAD: usize = 64 * 1024;
/// Finished and cancelled jobs older than this are left out of [`JobBoard::list`] unless
/// [`JobFilter::include_old`] is set.
pub const RETENTION: Duration = Duration::from_secs(30 * 24 * 3600);
/// Checkpoints [`JobBoard::checkpoints`] returns, newest last.
pub const KEEP_CHECKPOINTS: usize = 8;
/// [`Claim::progress`] writes at most once per this interval.
pub const PROGRESS_EVERY: Duration = Duration::from_secs(30);
const NOTE_PROGRESS: &str = "progress";
const NOTE_HOLDERS: &str = "holders";

type SyncNode = Node<Vec<u8>, Vec<u8>>;

/// A SHA-256 digest; shown and parsed as `sha256:<64 hex>`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default, Serialize, Deserialize)]
pub struct ContentHash(pub [u8; 32]);

impl ContentHash {
    /// The first 16 hex digits with the `sha256:` prefix, as Ymir writes base revisions.
    pub fn short(&self) -> String {
        format!("sha256:{}", &hex(&self.0)[..16])
    }
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

impl std::fmt::Display for ContentHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "sha256:{}", hex(&self.0))
    }
}

impl std::fmt::Debug for ContentHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl std::str::FromStr for ContentHash {
    type Err = JobError;
    fn from_str(s: &str) -> Result<Self, JobError> {
        let h = s.strip_prefix("sha256:").unwrap_or(s);
        if h.len() != 64 || !h.bytes().all(|c| c.is_ascii_hexdigit()) {
            return Err(JobError::Codec(format!("not a sha256 digest: {s:?}")));
        }
        let mut out = [0u8; 32];
        for (i, o) in out.iter_mut().enumerate() {
            *o = u8::from_str_radix(&h[2 * i..2 * i + 2], 16).map_err(|e| JobError::Codec(e.to_string()))?;
        }
        Ok(Self(out))
    }
}

/// Where a job stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum JobState {
    Queued,
    Running,
    Paused,
    Failed,
    Done,
    Cancelled,
}

impl From<JobStatus> for JobState {
    fn from(s: JobStatus) -> Self {
        match s {
            JobStatus::Open => JobState::Queued,
            JobStatus::Claimed => JobState::Running,
            JobStatus::Paused => JobState::Paused,
            JobStatus::Done => JobState::Done,
            JobStatus::Cancelled => JobState::Cancelled,
            _ => JobState::Failed,
        }
    }
}

/// The claim with the highest term, as recorded in the log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct LeaseInfo {
    pub holder: PeerId,
    /// The fencing term.
    pub term: u64,
    /// The holder's newest event (its claim, a checkpoint or a note) plus the claim time, by
    /// the writer's clock. Informational: the voters decide whether the lease is alive.
    pub expires_ms: u64,
}

/// A pointer to a checkpoint the app stores itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct CheckpointRef {
    /// Increases per checkpoint within a claim.
    pub seq: u64,
    /// The app's step (for training, the optimizer step).
    pub step: u64,
    /// Hash of the checkpoint's manifest.
    pub root: ContentHash,
    pub bytes: u64,
    /// Devices holding a copy, the writer first.
    pub holders: Vec<PeerId>,
}

impl CheckpointRef {
    pub fn new(seq: u64, step: u64, root: ContentHash, bytes: u64, holders: Vec<PeerId>) -> Self {
        Self { seq, step, root, bytes, holders }
    }
}

/// How a job is claimed, renewed, taken over and retried.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct JobPolicy {
    /// How long a claim lasts at the voters (ms).
    pub claim_ms: u64,
    /// How often the holder renews (ms).
    pub renew_ms: u64,
    /// After a claim lapses, how long voters wait before another device may claim (ms).
    pub takeover_after_ms: u64,
    /// Claims (lapsed ones included) before a retryable failure is final.
    pub max_attempts: u32,
    /// Wait after a retryable failure before the next claim (ms).
    pub retry_backoff_ms: u64,
    /// Devices that may claim; empty means any device with `role`.
    pub candidates: Vec<PeerId>,
    /// The role grant claimers need; the voters are the devices with it.
    pub role: String,
}

impl Default for JobPolicy {
    /// 60 s claims renewed every 20 s, a 5 min takeover wait, 3 attempts, 60 s backoff, no role.
    fn default() -> Self {
        Self {
            claim_ms: 60_000,
            renew_ms: 20_000,
            takeover_after_ms: 300_000,
            max_attempts: 3,
            retry_backoff_ms: 60_000,
            candidates: Vec::new(),
            role: String::new(),
        }
    }
}

impl JobPolicy {
    /// The defaults with claimers and voters taken from `role`.
    pub fn for_role(role: impl Into<String>) -> Self {
        Self { role: role.into(), ..Self::default() }
    }

    /// Sets the claim time and renews every third of it.
    pub fn with_claim(mut self, claim: Duration) -> Self {
        self.claim_ms = ms(claim);
        self.renew_ms = self.claim_ms / 3;
        self
    }

    pub fn with_renew(mut self, every: Duration) -> Self {
        self.renew_ms = ms(every);
        self
    }

    pub fn with_takeover_after(mut self, wait: Duration) -> Self {
        self.takeover_after_ms = ms(wait);
        self
    }

    pub fn with_max_attempts(mut self, n: u32) -> Self {
        self.max_attempts = n.max(1);
        self
    }

    pub fn with_retry_backoff(mut self, wait: Duration) -> Self {
        self.retry_backoff_ms = ms(wait);
        self
    }

    pub fn with_candidates<S: Into<PeerId>>(mut self, peers: impl IntoIterator<Item = S>) -> Self {
        self.candidates = peers.into_iter().map(Into::into).collect();
        self
    }

    pub fn claim(&self) -> Duration {
        Duration::from_millis(self.claim_ms)
    }

    pub fn takeover_after(&self) -> Duration {
        Duration::from_millis(self.takeover_after_ms)
    }

    fn voters(&self) -> Voters {
        if self.role.is_empty() { Voters::Peers(self.candidates.clone()) } else { Voters::Role(self.role.clone()) }
    }

    /// `voter` with limits for `kind`'s claim leases: grants up to this policy's claim time
    /// and the takeover wait. Voters also apply each job's own rules once they have synced it.
    pub fn configure_voter(&self, voter: Voter, kind: &str) -> Voter {
        voter.with_limits(format!("{kind}/"), self.claim(), self.takeover_after())
    }

    /// A fresh voter for this policy's role (or candidates) with [`configure_voter`]'s limits.
    ///
    /// [`configure_voter`]: Self::configure_voter
    pub fn voter(&self, kind: &str) -> Voter {
        self.configure_voter(Voter::new(self.voters()), kind)
    }

    fn rules(&self) -> JobRules {
        let mut r = JobRules::new()
            .with_takeover_after(self.takeover_after())
            .with_retry_backoff(Duration::from_millis(self.retry_backoff_ms))
            .with_candidates(self.candidates.clone());
        if !self.role.is_empty() {
            r = r.with_role(self.role.clone());
        }
        r
    }
}

fn ms(d: Duration) -> u64 {
    d.as_millis().min(u64::MAX as u128) as u64
}

/// A job folded from the log.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct JobRecord<P> {
    pub id: String,
    pub kind: String,
    /// The device that posted it; it (or a full device of the user) may pause, resume and
    /// cancel.
    pub owner: PeerId,
    pub base_revision: String,
    pub dataset: ContentHash,
    pub payload: P,
    pub state: JobState,
    /// Set while running.
    pub lease: Option<LeaseInfo>,
    /// The newest checkpoint of the highest term.
    pub checkpoint: Option<CheckpointRef>,
    pub attempts: u32,
    pub policy: JobPolicy,
    pub failure: Option<String>,
    pub result: Option<Vec<u8>>,
    /// The holder's newest [`Claim::progress`].
    pub progress: Option<serde_json::Value>,
    pub priority: u8,
    pub created_ms: u64,
    /// Not claimable before this time after a retryable failure (ms).
    pub not_before_ms: Option<u64>,
    /// The newest event's clock.
    pub updated: Hlc,
}

/// A job to post.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct NewJob<P> {
    pub id: String,
    pub base_revision: String,
    pub dataset: ContentHash,
    pub payload: P,
    pub policy: JobPolicy,
    pub priority: u8,
}

impl<P> NewJob<P> {
    /// A job with the default policy (set a role with [`with_policy`](Self::with_policy)).
    pub fn new(id: impl Into<String>, base_revision: impl Into<String>, dataset: ContentHash, payload: P) -> Self {
        Self { id: id.into(), base_revision: base_revision.into(), dataset, payload, policy: JobPolicy::default(), priority: 0 }
    }

    pub fn with_policy(mut self, policy: JobPolicy) -> Self {
        self.policy = policy;
        self
    }

    pub fn with_priority(mut self, priority: u8) -> Self {
        self.priority = priority;
        self
    }
}

/// What [`JobBoard::list`] returns.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct JobFilter {
    pub state: Option<JobState>,
    pub owner: Option<PeerId>,
    /// Include finished and cancelled jobs older than [`RETENTION`].
    pub include_old: bool,
}

impl JobFilter {
    pub fn all() -> Self {
        Self::default()
    }

    pub fn state(state: JobState) -> Self {
        Self { state: Some(state), ..Self::default() }
    }

    pub fn with_owner(mut self, owner: impl Into<PeerId>) -> Self {
        self.owner = Some(owner.into());
        self
    }

    pub fn with_old(mut self) -> Self {
        self.include_old = true;
        self
    }
}

/// A job that is new or changed, from [`JobBoard::watch`].
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct JobChange<P> {
    pub id: String,
    pub record: JobRecord<P>,
}

/// Why a [`Claim`] ended without the holder finishing, failing or pausing it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Lost {
    /// The lease ran out and could not be renewed within the takeover wait.
    Lease,
    /// Another device holds a higher term.
    Superseded,
    /// The owner paused the job; the claim paused itself (no attempt used) and let go.
    PausedByOwner,
    /// The job was cancelled; the claim let go.
    Cancelled,
    /// The holder finished, failed, paused or dropped the claim.
    Released,
}

/// Job board and claim errors.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum JobError {
    #[error("no job {0}")]
    NotFound(String),
    #[error("job is {0:?}")]
    NotClaimable(JobState),
    /// The app's check, the job's candidates or its role leave this device out.
    #[error("this device may not claim the job")]
    Ineligible,
    /// In the retry backoff.
    #[error("not claimable yet, retry in {retry_in:?}")]
    NotYet { retry_in: Duration },
    /// Another device holds it, or voters wait out a lapsed claim.
    #[error("held by {holder}, retry in {retry_in:?}")]
    Held { holder: PeerId, retry_in: Duration },
    #[error("no quorum: {reached} of {needed} grants")]
    NoQuorum { reached: usize, needed: usize },
    /// The claim's lease is not valid now.
    #[error("not holding the claim")]
    NotHolding,
    #[error("payload is {0} bytes encoded, over the limit")]
    PayloadTooLarge(usize),
    #[error("a job needs a role or candidates")]
    NoVoters,
    #[error("codec: {0}")]
    Codec(String),
    #[error("sync: {0}")]
    Sync(String),
}

/// The spec's name for claim errors.
pub type ClaimError = JobError;

impl From<LeaseError> for JobError {
    fn from(e: LeaseError) -> Self {
        match e {
            LeaseError::Held { holder, retry_in, .. } => JobError::Held { holder, retry_in },
            LeaseError::NoQuorum { reached, needed } => JobError::NoQuorum { reached, needed },
            LeaseError::NotEligible => JobError::Ineligible,
            LeaseError::NotYet { retry_in } => JobError::NotYet { retry_in },
            LeaseError::Lost => JobError::NotHolding,
            other => JobError::Sync(other.to_string()),
        }
    }
}

impl From<weirdnet_sync::SyncError> for JobError {
    fn from(e: weirdnet_sync::SyncError) -> Self {
        JobError::Sync(e.to_string())
    }
}

fn enc<T: Serialize>(v: &T) -> Result<Vec<u8>, JobError> {
    cbor4ii::serde::to_vec(Vec::new(), v).map_err(|e| JobError::Codec(e.to_string()))
}

fn dec<T: DeserializeOwned>(b: &[u8]) -> Result<T, JobError> {
    cbor4ii::serde::from_slice(b).map_err(|e| JobError::Codec(e.to_string()))
}

fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

/// What a job's sync spec body holds beside the sync fields.
#[derive(Serialize, Deserialize)]
struct Body<P> {
    base_revision: String,
    dataset: ContentHash,
    payload: P,
    renew_ms: u64,
    created_ms: u64,
}

/// The sync state and node the jobs use (the same ones the app syncs with).
#[derive(Clone)]
pub struct SyncHandle {
    leases: Leases,
}

impl SyncHandle {
    pub fn new(node: Arc<SyncNode>, state: Arc<Mutex<SyncState>>) -> Self {
        Self { leases: Leases::new(node, state) }
    }

    /// From leases already set up (for example with an address book, `Leases::with_addrs`).
    pub fn from_leases(leases: Leases) -> Self {
        Self { leases }
    }

    pub fn leases(&self) -> &Leases {
        &self.leases
    }

    pub fn state(&self) -> &Arc<Mutex<SyncState>> {
        self.leases.state()
    }
}

fn lock(st: &Mutex<SyncState>) -> Result<std::sync::MutexGuard<'_, SyncState>, JobError> {
    st.lock().map_err(|_| JobError::Sync("sync state lock poisoned".into()))
}

/// The jobs of one kind (also their sync record type and lease prefix), with payload `P`.
pub struct JobBoard<P> {
    sync: SyncHandle,
    kind: String,
    _p: PhantomData<fn() -> P>,
}

impl<P> Clone for JobBoard<P> {
    fn clone(&self) -> Self {
        Self { sync: self.sync.clone(), kind: self.kind.clone(), _p: PhantomData }
    }
}

impl<P: Serialize + DeserializeOwned + Clone + Send + Sync + 'static> JobBoard<P> {
    /// The board for `kind` (for example `ymir.train`), which must be one of the state's
    /// synced record types for jobs to reach other devices.
    pub fn open(sync: &SyncHandle, kind: impl Into<String>) -> Result<Self, JobError> {
        let kind = kind.into();
        job_lease(&kind, "x")?;
        Ok(Self { sync: sync.clone(), kind, _p: PhantomData })
    }

    pub fn kind(&self) -> &str {
        &self.kind
    }

    /// Post a job; this device is its owner. Posting an id again keeps the first job.
    pub fn post(&self, job: NewJob<P>) -> Result<JobRecord<P>, JobError> {
        job_lease(&self.kind, &job.id)?;
        if job.policy.role.is_empty() && job.policy.candidates.is_empty() {
            return Err(JobError::NoVoters);
        }
        let size = enc(&job.payload)?.len();
        if size > MAX_PAYLOAD {
            return Err(JobError::PayloadTooLarge(size));
        }
        let body = Body {
            base_revision: job.base_revision,
            dataset: job.dataset,
            payload: job.payload,
            renew_ms: job.policy.renew_ms,
            created_ms: now_ms(),
        };
        let spec = JobSpec::new(job.id.clone(), self.kind.clone(), enc(&body)?)
            .with_priority(job.priority)
            .with_max_attempts(job.policy.max_attempts)
            .with_claim(job.policy.claim())
            .with_rules(job.policy.rules());
        let mut st = lock(self.sync.state())?;
        Jobs::new(&mut st, self.kind.clone()).post(now_ms(), spec)?;
        let view = Jobs::new(&mut st, self.kind.clone()).view(&job.id).ok_or_else(|| JobError::NotFound(job.id.clone()))?;
        self.record(&st, view)
    }

    fn record(&self, st: &SyncState, v: JobView) -> Result<JobRecord<P>, JobError> {
        let body: Body<P> = dec(&v.spec.body)?;
        let rules = v.spec.rules.clone().unwrap_or_default();
        let policy = JobPolicy {
            claim_ms: v.spec.claim_ms,
            renew_ms: body.renew_ms,
            takeover_after_ms: rules.takeover_after_ms,
            max_attempts: v.spec.max_attempts,
            retry_backoff_ms: rules.retry_backoff_ms,
            candidates: rules.candidates.clone(),
            role: rules.role.clone().unwrap_or_default(),
        };
        let state = JobState::from(v.status);
        let lease = v.claim.as_ref().filter(|_| state == JobState::Running).map(|(holder, term)| LeaseInfo {
            holder: holder.clone(),
            term: term.0,
            expires_ms: v.last_seen_ms.unwrap_or(v.changed_ms).saturating_add(v.spec.claim_ms),
        });
        let mut checkpoint = latest_checkpoint(&v);
        if let (Some(cp), Some((_, data))) = (checkpoint.as_mut(), v.notes.get(NOTE_HOLDERS))
            && let Ok((seq, holders)) = dec::<(u64, Vec<PeerId>)>(data)
            && seq == cp.seq
        {
            cp.holders = holders;
        }
        let progress = v.notes.get(NOTE_PROGRESS).and_then(|(_, d)| serde_json::from_slice(d).ok());
        let updated = st
            .log
            .iter()
            .filter(|e| e.typ == self.kind && e.record_id == v.spec.id)
            .map(|e| e.hlc.clone())
            .max_by(|a, b| a.cmp_total(b))
            .ok_or_else(|| JobError::NotFound(v.spec.id.clone()))?;
        Ok(JobRecord {
            id: v.spec.id.clone(),
            kind: v.spec.kind.clone(),
            owner: v.owner.clone(),
            base_revision: body.base_revision,
            dataset: body.dataset,
            payload: body.payload,
            state,
            lease,
            checkpoint,
            attempts: v.attempts,
            policy,
            failure: v.failure.clone(),
            result: v.result.clone(),
            progress,
            priority: v.spec.priority,
            created_ms: body.created_ms,
            not_before_ms: v.not_before_ms,
            updated,
        })
    }

    fn view(&self, st: &mut SyncState, id: &str) -> Option<JobView> {
        Jobs::new(st, self.kind.clone()).view(id)
    }

    pub fn get(&self, id: &str) -> Result<Option<JobRecord<P>>, JobError> {
        let mut st = lock(self.sync.state())?;
        match self.view(&mut st, id) {
            Some(v) => self.record(&st, v).map(Some),
            None => Ok(None),
        }
    }

    /// Jobs matching `filter`, highest priority first, then by id. Jobs whose payload doesn't
    /// decode as `P` are skipped.
    pub fn list(&self, filter: &JobFilter) -> Result<Vec<JobRecord<P>>, JobError> {
        let mut st = lock(self.sync.state())?;
        let ids = Jobs::new(&mut st, self.kind.clone()).ids();
        let cutoff = now_ms().saturating_sub(ms(RETENTION));
        let mut out = Vec::new();
        for id in ids {
            let Some(v) = self.view(&mut st, &id) else { continue };
            let old = matches!(v.status, JobStatus::Done | JobStatus::Cancelled) && v.changed_ms < cutoff;
            if old && !filter.include_old {
                continue;
            }
            let Ok(r) = self.record(&st, v) else { continue };
            if filter.state.is_some_and(|s| s != r.state) || filter.owner.as_ref().is_some_and(|o| *o != r.owner) {
                continue;
            }
            out.push(r);
        }
        out.sort_by(|a, b| b.priority.cmp(&a.priority).then_with(|| a.id.cmp(&b.id)));
        Ok(out)
    }

    /// The newest [`KEEP_CHECKPOINTS`] checkpoints, oldest first, ordered by (term, seq).
    pub fn checkpoints(&self, id: &str) -> Result<Vec<CheckpointRef>, JobError> {
        let mut st = lock(self.sync.state())?;
        let v = self.view(&mut st, id).ok_or_else(|| JobError::NotFound(id.to_string()))?;
        let mut cps: Vec<(Term, u64, CheckpointRef)> =
            v.checkpoints.iter().filter_map(|(t, s, d)| dec::<CheckpointRef>(d).ok().map(|c| (*t, *s, c))).collect();
        cps.sort_by_key(|c| (c.0, c.1));
        let skip = cps.len().saturating_sub(KEEP_CHECKPOINTS);
        Ok(cps.into_iter().skip(skip).map(|c| c.2).collect())
    }

    /// New and changed jobs, polled once a second (the first poll reports every job).
    pub fn watch(&self) -> impl futures::Stream<Item = JobChange<P>> + Send + 'static {
        self.watch_every(Duration::from_secs(1))
    }

    /// [`watch`](Self::watch) polling every `every`.
    pub fn watch_every(&self, every: Duration) -> impl futures::Stream<Item = JobChange<P>> + Send + 'static {
        let seen: BTreeMap<String, (JobState, Hlc)> = BTreeMap::new();
        futures::stream::unfold((self.clone(), seen, VecDeque::new()), move |(board, mut seen, mut queue)| async move {
            loop {
                if let Some(c) = queue.pop_front() {
                    return Some((c, (board, seen, queue)));
                }
                if let Ok(all) = board.list(&JobFilter::all().with_old()) {
                    for r in all {
                        let key = (r.state, r.updated.clone());
                        if seen.get(&r.id) != Some(&key) {
                            seen.insert(r.id.clone(), key);
                            queue.push_back(JobChange { id: r.id.clone(), record: r });
                        }
                    }
                }
                if queue.is_empty() {
                    tokio::time::sleep(every).await;
                }
            }
        })
    }

    /// Claim job `id`: it must be queued (or running with a lease that lapsed; the voters
    /// decide), pass `eligible`, the job's candidates and role, and be past any retry backoff.
    /// The returned [`Claim`] renews in the background until finished, failed, paused, lost
    /// or dropped.
    pub async fn claim(&self, id: &str, eligible: impl Fn(&JobRecord<P>) -> bool) -> Result<Claim<P>, JobError> {
        let record = self.get(id)?.ok_or_else(|| JobError::NotFound(id.to_string()))?;
        if !matches!(record.state, JobState::Queued | JobState::Running) {
            return Err(JobError::NotClaimable(record.state));
        }
        if !eligible(&record) {
            return Err(JobError::Ineligible);
        }
        let policy = record.policy.clone();
        let mut cfg = LeaseConfig::new(policy.voters()).with_duration(policy.claim());
        if policy.renew_ms > 0 {
            cfg.renew_every = Duration::from_millis(policy.renew_ms);
        }
        let (leases, kind, jid, c) = (self.sync.leases.clone(), self.kind.clone(), id.to_string(), cfg.clone());
        let handle = tokio::task::spawn_blocking(move || leases.claim_job(&kind, &jid, &c)).await.map_err(|e| JobError::Sync(e.to_string()))??;
        let record = self.get(id)?.ok_or_else(|| JobError::NotFound(id.to_string()))?;
        let (lost_tx, _) = watch::channel(None);
        let inner = Arc::new(ClaimInner {
            leases: self.sync.leases.clone(),
            cfg,
            kind: self.kind.clone(),
            id: id.to_string(),
            name: handle.name().clone(),
            term: handle.term(),
            handle: Mutex::new(handle),
            stop: AtomicBool::new(false),
            lost: lost_tx,
            progress: Mutex::new((None, None)),
            takeover_after: policy.takeover_after(),
            thread: Mutex::new(None),
        });
        let worker = inner.clone();
        let t = std::thread::Builder::new()
            .name(format!("job claim {id}"))
            .spawn(move || run_claim(worker))
            .map_err(|e| JobError::Sync(format!("starting the renew thread: {e}")))?;
        if let Ok(mut slot) = inner.thread.lock() {
            *slot = Some(t);
        }
        Ok(Claim { inner, record })
    }

    fn owner_op(&self, id: &str, op: impl FnOnce(&mut Jobs<'_>, u64, &str) -> weirdnet_sync::Result<()>) -> Result<(), JobError> {
        let mut st = lock(self.sync.state())?;
        let mut jobs = Jobs::new(&mut st, self.kind.clone());
        if jobs.view(id).is_none() {
            return Err(JobError::NotFound(id.to_string()));
        }
        op(&mut jobs, now_ms(), id).map_err(Into::into)
    }

    /// Pause a job as its owner (or a full device). A running holder pauses itself and lets
    /// go once it has synced this; no attempt is used.
    pub fn pause(&self, id: &str) -> Result<(), JobError> {
        self.owner_op(id, |j, now, id| j.pause(now, id))
    }

    /// Put a paused job back in the queue.
    pub fn resume(&self, id: &str) -> Result<(), JobError> {
        self.owner_op(id, |j, now, id| j.resume(now, id))
    }

    pub fn cancel(&self, id: &str) -> Result<(), JobError> {
        self.owner_op(id, |j, now, id| j.cancel(now, id))
    }
}

fn latest_checkpoint(v: &JobView) -> Option<CheckpointRef> {
    v.checkpoints.iter().filter_map(|(t, s, d)| dec::<CheckpointRef>(d).ok().map(|c| ((*t, *s), c))).max_by(|a, b| a.0.cmp(&b.0)).map(|x| x.1)
}

struct ClaimInner {
    leases: Leases,
    cfg: LeaseConfig,
    kind: String,
    id: String,
    name: LeaseName,
    term: Term,
    handle: Mutex<LeaseHandle>,
    stop: AtomicBool,
    lost: watch::Sender<Option<Lost>>,
    /// When progress was last written, and progress waiting for its turn.
    progress: Mutex<(Option<Instant>, Option<serde_json::Value>)>,
    takeover_after: Duration,
    thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl ClaimInner {
    fn handle(&self) -> Option<LeaseHandle> {
        self.handle.lock().ok().map(|h| h.clone())
    }

    fn write(&self, ev: JobEvent) -> Result<(), JobError> {
        let h = self.handle().ok_or(JobError::NotHolding)?;
        if !h.is_valid() {
            return Err(JobError::NotHolding);
        }
        let mut st = lock(self.leases.state())?;
        Jobs::new(&mut st, self.kind.clone()).record(now_ms(), &self.id, &h, ev).map_err(|e| match e {
            weirdnet_sync::SyncError::NotPermitted(_) => JobError::NotHolding,
            e => e.into(),
        })
    }

    fn end(&self, why: Lost) {
        self.lost.send_if_modified(|v| {
            if v.is_none() {
                *v = Some(why);
                true
            } else {
                false
            }
        });
    }

    fn release(&self) {
        if let Some(h) = self.handle() {
            self.leases.release(h, &self.cfg);
        }
    }

    fn flush_progress(&self, force: bool) {
        let pending = {
            let Ok(mut p) = self.progress.lock() else { return };
            let due = force || p.0.is_none_or(|t| t.elapsed() >= PROGRESS_EVERY);
            if !due || p.1.is_none() {
                return;
            }
            p.0 = Some(Instant::now());
            p.1.take()
        };
        if let Some(v) = pending
            && let Ok(data) = serde_json::to_vec(&v)
        {
            let _ = self.write(JobEvent::Note { term: self.term, key: NOTE_PROGRESS.into(), data });
        }
    }

    fn sleep(&self, d: Duration) -> bool {
        let end = Instant::now() + d;
        while Instant::now() < end {
            if self.stop.load(Ordering::SeqCst) {
                return false;
            }
            std::thread::sleep((end - Instant::now()).min(Duration::from_millis(25)));
        }
        !self.stop.load(Ordering::SeqCst)
    }
}

fn run_claim(c: Arc<ClaimInner>) {
    let mut lapsed: Option<Instant> = None;
    loop {
        let wait = match (lapsed, c.handle()) {
            (Some(_), _) => Duration::from_millis(200).min(c.cfg.renew_every),
            (None, Some(h)) => c.cfg.renew_every.min(h.remaining() / 2).max(Duration::from_millis(20)),
            (None, None) => return,
        };
        if !c.sleep(wait) {
            return;
        }
        // What the log says: a higher term, the owner's pause or a cancel ends this claim.
        let view = c.leases.state().lock().ok().and_then(|mut st| Jobs::new(&mut st, c.kind.clone()).view(&c.id));
        if let Some(v) = view {
            if v.claim.as_ref().is_some_and(|(_, t)| *t > c.term) {
                c.end(Lost::Superseded);
                return;
            }
            match v.status {
                JobStatus::Paused => {
                    let _ = c.write(JobEvent::Paused { term: Some(c.term) });
                    c.release();
                    c.end(Lost::PausedByOwner);
                    return;
                }
                JobStatus::Cancelled => {
                    c.release();
                    c.end(Lost::Cancelled);
                    return;
                }
                _ => {}
            }
        }
        c.flush_progress(false);
        let Some(mut h) = c.handle() else { return };
        match c.leases.renew(&mut h, &c.cfg) {
            Ok(()) => {
                if let Ok(mut slot) = c.handle.lock() {
                    *slot = h;
                }
                lapsed = None;
            }
            Err(LeaseError::Lost) => {
                let since = *lapsed.get_or_insert_with(Instant::now);
                if c.leases.term_seen(&c.name) > c.term {
                    c.end(Lost::Superseded);
                    return;
                }
                if since.elapsed() >= c.takeover_after {
                    c.end(Lost::Lease);
                    return;
                }
            }
            Err(LeaseError::NotEligible) => {
                c.end(Lost::Lease);
                return;
            }
            Err(_) => {}
        }
    }
}

/// A held claim on a job. Carry [`term`](Self::term) (or a [`fence`](Self::fence)) into every
/// effect that must happen once. Dropping it stops renewing and lets go of the lease (the job
/// stays running in the log until another device claims it).
pub struct Claim<P> {
    inner: Arc<ClaimInner>,
    record: JobRecord<P>,
}

/// A claim's fencing token for a resource's `FenceGuard`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fence {
    pub name: LeaseName,
    pub term: Term,
}

impl Fence {
    /// Accepts this term at `guard` unless it has seen a newer one for the same job.
    pub fn check(&self, guard: &mut FenceGuard) -> Result<(), Stale> {
        guard.check(&self.name, self.term)
    }
}

impl<P: Clone + Send + 'static> Claim<P> {
    pub fn id(&self) -> &str {
        &self.inner.id
    }

    /// The fencing term.
    pub fn term(&self) -> u64 {
        self.inner.term.0
    }

    /// The job as it was when claimed (with the checkpoint to resume from).
    pub fn record(&self) -> &JobRecord<P> {
        &self.record
    }

    /// The checkpoint to resume from.
    pub fn resume_from(&self) -> Option<&CheckpointRef> {
        self.record.checkpoint.as_ref()
    }

    /// Whether the lease is valid now. Stop acting while it isn't; it may come back after a
    /// network drop shorter than the takeover wait.
    pub fn is_valid(&self) -> bool {
        self.inner.handle().is_some_and(|h| h.is_valid()) && self.inner.lost.borrow().is_none()
    }

    /// Resolves once the claim has ended without the holder finishing, failing or pausing it.
    pub async fn lost(&self) -> Lost {
        let mut rx = self.inner.lost.subscribe();
        loop {
            if let Some(l) = *rx.borrow_and_update() {
                return l;
            }
            if rx.changed().await.is_err() {
                return Lost::Released;
            }
        }
    }

    /// Record a checkpoint the app has stored.
    pub fn checkpoint(&self, cp: CheckpointRef) -> Result<(), JobError> {
        let data = enc(&cp)?;
        self.inner.write(JobEvent::Checkpoint { term: self.inner.term, seq: cp.seq, data })
    }

    /// Replace the holders of checkpoint `seq` without writing a new checkpoint.
    pub fn set_holders(&self, seq: u64, holders: Vec<PeerId>) -> Result<(), JobError> {
        let data = enc(&(seq, holders))?;
        self.inner.write(JobEvent::Note { term: self.inner.term, key: NOTE_HOLDERS.into(), data })
    }

    /// Report progress (last writer wins). Written at most once per [`PROGRESS_EVERY`]; a
    /// newer value given sooner replaces the waiting one and is written in its turn.
    pub fn progress(&self, value: serde_json::Value) -> Result<(), JobError> {
        if let Ok(mut p) = self.inner.progress.lock() {
            p.1 = Some(value);
        }
        self.inner.flush_progress(false);
        Ok(())
    }

    /// A fencing token for once-only effects (for Ymir, installing the trained adapter).
    pub fn fence(&self) -> Fence {
        Fence { name: self.inner.name.clone(), term: self.inner.term }
    }

    async fn close(&self, ev: Option<JobEvent>) -> Result<(), JobError> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            inner.stop.store(true, Ordering::SeqCst);
            if let Some(t) = inner.thread.lock().ok().and_then(|mut t| t.take()) {
                let _ = t.join();
            }
            inner.flush_progress(true);
            let r = ev.map(|e| inner.write(e)).unwrap_or(Ok(()));
            inner.release();
            inner.end(Lost::Released);
            r
        })
        .await
        .map_err(|e| JobError::Sync(e.to_string()))?
    }

    /// The job is done with `result`.
    pub async fn finish(self, result: Vec<u8>) -> Result<(), JobError> {
        self.close(Some(JobEvent::Finished { term: self.inner.term, result })).await
    }

    /// The attempt failed. `retry: true` reopens the job after the backoff while attempts
    /// remain; `retry: false` is final.
    pub async fn fail(self, reason: impl Into<String>, retry: bool) -> Result<(), JobError> {
        self.close(Some(JobEvent::Failed { term: self.inner.term, reason: reason.into(), retry })).await
    }

    /// Stop and let go without failing; the job is paused until its owner resumes it, and
    /// this claim is not counted as an attempt.
    pub async fn pause(self) -> Result<(), JobError> {
        self.close(Some(JobEvent::Paused { term: Some(self.inner.term) })).await
    }
}

impl<P> Drop for Claim<P> {
    fn drop(&mut self) {
        if self.inner.stop.swap(true, Ordering::SeqCst) {
            return;
        }
        let inner = self.inner.clone();
        // Let go off the async runtime: a release waits for the voters.
        let _ = std::thread::Builder::new().name("job claim release".into()).spawn(move || {
            if let Some(t) = inner.thread.lock().ok().and_then(|mut t| t.take()) {
                let _ = t.join();
            }
            if inner.lost.borrow().is_none() {
                inner.release();
                inner.end(Lost::Released);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_hashes_print_and_parse() {
        let h = ContentHash([0xab; 32]);
        let s = h.to_string();
        assert_eq!(s.len(), 7 + 64);
        assert_eq!(s.parse::<ContentHash>().unwrap(), h);
        assert_eq!(s[7..].parse::<ContentHash>().unwrap(), h);
        assert_eq!(h.short(), "sha256:abababababababab");
        assert!("sha256:xyz".parse::<ContentHash>().is_err());
    }

    #[test]
    fn the_default_policy_matches_training_jobs() {
        let p = JobPolicy::for_role("ymir.model");
        assert_eq!((p.claim_ms, p.renew_ms, p.takeover_after_ms, p.max_attempts, p.retry_backoff_ms), (60_000, 20_000, 300_000, 3, 60_000));
        let v = p.voter("ymir.train");
        let n = LeaseName::new("ymir.train/j1").unwrap();
        assert_eq!(v.limits_for(&n).map(|l| (l.max_duration, l.takeover_after)), Some((Duration::from_secs(60), Duration::from_secs(300))));
        let r = p.rules();
        assert_eq!((r.role.as_deref(), r.takeover_after_ms, r.retry_backoff_ms), (Some("ymir.model"), 300_000, 60_000));
    }
}
