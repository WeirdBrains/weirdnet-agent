//! Synced training-style jobs between service devices over weirdnet-net (memory transport):
//! claims, renewals, lapses and takeovers, resuming from checkpoints, retries, pause and
//! resume, cancel, eligibility, a split pool and fencing.
#![cfg(feature = "jobs")]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use weirdnet_agent::jobs::{CheckpointRef, ContentHash, JobBoard, JobError, JobFilter, JobPolicy, JobState, Lost, NewJob, SyncHandle};
use weirdnet_net::user::{self, NodeCert};
use weirdnet_net::{Keypair, Multiaddr, Node, WEIRDNET};
use weirdnet_sync::{FenceGuard, LeaseName, Leases, LinkBundle, SyncState, Voter, Voters, node_config, pull_from_shared, start_node};

const KIND: &str = "ymir.train";
const ROLE: &str = "ymir.model";
const SEED: [u8; 32] = *b"JJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJ";
/// Short claims keep the tests quick.
const CLAIM: Duration = Duration::from_millis(1_500);
const TAKEOVER: Duration = Duration::from_millis(1_500);

type N = Node<Vec<u8>, Vec<u8>>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct TrainJob {
    base: String,
    steps: u64,
}

fn user() -> Keypair {
    let mut s = SEED;
    Keypair::ed25519_from_bytes(&mut s).unwrap()
}

fn peer(k: &Keypair) -> String {
    k.public().to_peer_id().to_string()
}

fn cert(user: &Keypair, k: &Keypair, label: &str) -> NodeCert {
    WEIRDNET.certify(user, &user::user_id(&user.public()), &peer(k), label, 10).unwrap()
}

fn addrs(n: &N) -> Vec<Multiaddr> {
    n.reachable_addrs().into_iter().map(|s| s.parse().unwrap()).collect()
}

fn policy() -> JobPolicy {
    JobPolicy::for_role(ROLE).with_claim(CLAIM).with_takeover_after(TAKEOVER).with_retry_backoff(Duration::from_millis(800)).with_max_attempts(3)
}

struct Dev {
    k: Keypair,
    st: Arc<Mutex<SyncState>>,
    node: Arc<N>,
    board: JobBoard<TrainJob>,
}

impl Dev {
    fn id(&self) -> String {
        peer(&self.k)
    }
}

struct Pool {
    devs: Vec<Dev>,
    book: Arc<Mutex<HashMap<String, Vec<Multiaddr>>>>,
}

/// Service devices linked by a phone; `roles[i]` says whether device i holds the role (and
/// votes).
fn pool(roles: &[bool]) -> Pool {
    let u = user();
    let kp = Keypair::generate_ed25519();
    let mut phone = SyncState::new(SEED.to_vec(), u.public(), cert(&u, &kp, "phone"), Some(&kp), vec![KIND.into()]).unwrap();
    phone.auto_first_rotation = false;
    let mut linked = Vec::new();
    for (i, has) in roles.iter().enumerate() {
        let k = Keypair::generate_ed25519();
        let r: Vec<String> = if *has { vec![ROLE.into()] } else { vec!["ymir.inference".into()] };
        let bundle = phone.service_bundle(&peer(&k), &r, &[KIND.to_string()], 10 + i as i64).unwrap();
        let bytes = bundle.to_bytes().unwrap();
        let c = cert(&u, &k, &format!("node{i}"));
        phone.approve(c.clone(), 10).unwrap();
        linked.push((k, c, bytes.to_vec(), *has));
    }
    let book: Arc<Mutex<HashMap<String, Vec<Multiaddr>>>> = Arc::default();
    let mut devs = Vec::new();
    for (k, c, bytes, has) in linked {
        let mut st = SyncState::new_service(u.public(), c, &k, vec![KIND.into()], &LinkBundle::from_bytes(&bytes).unwrap()).unwrap();
        st.roster = phone.roster.clone();
        if has {
            // Short default maximum so the start-up grace is the claim time, not 15 s.
            let v = policy().configure_voter(Voter::new(Voters::Role(ROLE.into())).with_max_duration(Duration::from_millis(100)), KIND);
            st.set_voter(Some(v));
        }
        let st = Arc::new(Mutex::new(st));
        let node = Arc::new(start_node(k.clone(), node_config(true), st.clone()).unwrap());
        book.lock().unwrap().insert(peer(&k), addrs(&node));
        let b = book.clone();
        let leases = Leases::new(node.clone(), st.clone()).with_addrs(move |p| b.lock().unwrap().get(p).cloned().unwrap_or_default());
        let board = JobBoard::open(&SyncHandle::from_leases(leases), KIND).unwrap();
        devs.push(Dev { k, st, node, board });
    }
    for (i, a) in devs.iter().enumerate() {
        for b in &devs[i + 1..] {
            let _ = a.node.connect(b.node.peer_id(), &addrs(&b.node));
        }
    }
    std::thread::sleep(CLAIM + Duration::from_millis(100));
    Pool { devs, book }
}

/// Everyone pulls device `from`'s jobs.
fn spread(p: &Pool, from: usize) {
    let (pid, a) = (p.devs[from].node.peer_id(), addrs(&p.devs[from].node));
    for (i, d) in p.devs.iter().enumerate() {
        if i != from {
            let _ = pull_from_shared(&d.st, &d.node, pid, &a, KIND);
        }
    }
}

/// A crash or a cut cable: the device leaves the network.
fn cut(p: &Pool, i: usize) {
    p.book.lock().unwrap().remove(&p.devs[i].id());
    p.devs[i].node.halt();
}

fn job(id: &str) -> NewJob<TrainJob> {
    NewJob::new(id, "sha256:0123456789abcdef", ContentHash([7; 32]), TrainJob { base: "base-1b".into(), steps: 1_000 }).with_policy(policy())
}

fn cp(seq: u64, step: u64, holder: &str) -> CheckpointRef {
    CheckpointRef::new(seq, step, ContentHash([seq as u8; 32]), 1 << 30, vec![holder.to_string()])
}

/// Claim, retrying only while another holder's lease or a takeover wait is in the way.
async fn claim_when_free(d: &Dev, id: &str, within: Duration) -> weirdnet_agent::jobs::Claim<TrainJob> {
    let end = Instant::now() + within;
    loop {
        match d.board.claim(id, |_| true).await {
            Ok(c) => return c,
            Err(JobError::Held { .. } | JobError::NoQuorum { .. }) if Instant::now() < end => tokio::time::sleep(Duration::from_millis(50)).await,
            Err(e) => panic!("expected a claim, got {e:?}"),
        }
    }
}

fn state(d: &Dev, id: &str) -> JobState {
    d.board.get(id).unwrap().unwrap().state
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn claim_renew_checkpoint_progress_and_finish() {
    use futures::StreamExt;
    let p = pool(&[true, true, true]);
    let posted = p.devs[0].board.post(job("t1")).unwrap();
    assert_eq!((posted.state, posted.owner.clone(), posted.attempts), (JobState::Queued, p.devs[0].id(), 0));
    assert_eq!(posted.payload.steps, 1_000);
    let mut changes = Box::pin(p.devs[0].board.watch_every(Duration::from_millis(50)));
    assert_eq!(changes.next().await.unwrap().record.state, JobState::Queued);
    spread(&p, 0);
    let c = claim_when_free(&p.devs[1], "t1", CLAIM * 4).await;
    // It keeps the claim across several claim times by renewing in the background.
    tokio::time::sleep(CLAIM * 3).await;
    assert!(c.is_valid());
    assert!(matches!(p.devs[2].board.claim("t1", |_| true).await, Err(JobError::Held { .. })));
    c.checkpoint(cp(1, 200, &p.devs[1].id())).unwrap();
    c.set_holders(1, vec![p.devs[1].id(), p.devs[2].id()]).unwrap();
    c.progress(serde_json::json!({"step": 200})).unwrap();
    c.progress(serde_json::json!({"step": 250})).unwrap(); // waits for its turn
    let r = p.devs[1].board.get("t1").unwrap().unwrap();
    assert_eq!(r.state, JobState::Running);
    assert_eq!(r.lease.as_ref().map(|l| (l.holder.clone(), l.term)), Some((p.devs[1].id(), c.term())));
    assert_eq!(r.checkpoint.as_ref().map(|c| (c.step, c.holders.len())), Some((200, 2)));
    assert_eq!(r.progress, Some(serde_json::json!({"step": 200})));
    c.finish(b"adapter:abc".to_vec()).await.unwrap();
    spread(&p, 1);
    let r = p.devs[0].board.get("t1").unwrap().unwrap();
    assert_eq!((r.state, r.result.as_deref(), r.attempts), (JobState::Done, Some(&b"adapter:abc"[..]), 1));
    assert_eq!(r.progress, Some(serde_json::json!({"step": 250})), "the waiting progress is written when the claim ends");
    let seen = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if changes.next().await.unwrap().record.state == JobState::Done {
                return;
            }
        }
    })
    .await;
    assert!(seen.is_ok(), "watch reports the change");
    assert!(matches!(p.devs[2].board.claim("t1", |_| true).await, Err(JobError::NotClaimable(JobState::Done))));
    assert_eq!(p.devs[0].board.list(&JobFilter::state(JobState::Done)).unwrap().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dead_holder_lapses_and_another_resumes_after_the_takeover_wait() {
    let p = pool(&[true, true, true]);
    p.devs[0].board.post(job("t2")).unwrap();
    let c = claim_when_free(&p.devs[0], "t2", CLAIM * 4).await;
    c.checkpoint(cp(1, 400, &p.devs[0].id())).unwrap();
    spread(&p, 0);
    // The holder dies without letting go (no drop, no release).
    let t1 = c.term();
    std::mem::forget(c);
    cut(&p, 0);
    let at = Instant::now();
    let c2 = claim_when_free(&p.devs[1], "t2", (CLAIM + TAKEOVER) * 4).await;
    assert!(at.elapsed() >= TAKEOVER, "not before the lease lapsed plus the takeover wait ({:?})", at.elapsed());
    assert!(c2.term() > t1);
    let from = c2.resume_from().expect("resumes from the checkpoint");
    assert_eq!((from.step, from.holders[0].clone()), (400, p.devs[0].id()));
    assert_eq!(c2.record().attempts, 2, "the lapsed claim counts");
    c2.finish(vec![]).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_split_pool_never_has_two_holders_and_the_old_term_is_fenced() {
    let p = pool(&[true, true, true]);
    p.devs[0].board.post(job("t3")).unwrap();
    spread(&p, 0);
    let old = claim_when_free(&p.devs[0], "t3", CLAIM * 4).await;
    let mut adapter = FenceGuard::new();
    old.fence().check(&mut adapter).unwrap();
    // Device 0 is cut off from the other two but keeps running.
    cut(&p, 0);
    let new = claim_when_free(&p.devs[1], "t3", (CLAIM + TAKEOVER) * 4).await;
    assert!(!old.is_valid(), "the cut-off holder's lease ran out before anyone else got one");
    assert!(new.is_valid() && new.term() > old.term());
    new.fence().check(&mut adapter).unwrap();
    assert!(old.fence().check(&mut adapter).is_err(), "the old term is fenced off");
    let lost = tokio::time::timeout(TAKEOVER * 4, old.lost()).await.expect("the old claim ends");
    assert!(matches!(lost, Lost::Lease | Lost::Superseded));
    assert!(matches!(old.checkpoint(cp(9, 9, "x")), Err(JobError::NotHolding)));
    let lease = LeaseName::new(format!("{KIND}/t3")).unwrap();
    assert_eq!(adapter.seen(&lease).map(|t| t.0), Some(new.term()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retries_wait_out_the_backoff_until_the_attempts_run_out() {
    let p = pool(&[true, true, true]);
    p.devs[0].board.post(job("t4").with_policy(policy().with_max_attempts(2))).unwrap();
    let c = claim_when_free(&p.devs[0], "t4", CLAIM * 4).await;
    c.fail("oom", true).await.unwrap();
    spread(&p, 0);
    assert_eq!(state(&p.devs[1], "t4"), JobState::Queued);
    assert!(matches!(p.devs[1].board.claim("t4", |_| true).await, Err(JobError::NotYet { .. })));
    tokio::time::sleep(Duration::from_millis(900)).await;
    let c = claim_when_free(&p.devs[1], "t4", CLAIM * 4).await;
    c.fail("oom again", true).await.unwrap();
    let r = p.devs[1].board.get("t4").unwrap().unwrap();
    assert_eq!((r.state, r.attempts, r.failure.as_deref()), (JobState::Failed, 2, Some("oom again")));
    assert!(matches!(p.devs[1].board.claim("t4", |_| true).await, Err(JobError::NotClaimable(JobState::Failed))));
    // A final failure ends it at once.
    p.devs[0].board.post(job("t5")).unwrap();
    let c = claim_when_free(&p.devs[0], "t5", CLAIM * 4).await;
    c.fail("license mismatch", false).await.unwrap();
    assert_eq!(state(&p.devs[0], "t5"), JobState::Failed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pause_resume_and_cancel_use_no_attempts() {
    let p = pool(&[true, true, true]);
    p.devs[0].board.post(job("t6")).unwrap();
    let c = claim_when_free(&p.devs[0], "t6", CLAIM * 4).await;
    c.checkpoint(cp(1, 10, &p.devs[0].id())).unwrap();
    c.pause().await.unwrap();
    spread(&p, 0);
    let r = p.devs[1].board.get("t6").unwrap().unwrap();
    assert_eq!((r.state, r.attempts), (JobState::Paused, 0));
    assert!(matches!(p.devs[1].board.claim("t6", |_| true).await, Err(JobError::NotClaimable(JobState::Paused))));
    // Device 1 is a service device and not the owner: it can't resume.
    assert!(p.devs[1].board.resume("t6").is_err());
    p.devs[0].board.resume("t6").unwrap();
    spread(&p, 0);
    let c = claim_when_free(&p.devs[1], "t6", CLAIM * 4).await;
    assert_eq!(c.resume_from().map(|c| c.step), Some(10));
    // The owner pauses while device 1 runs it: the claim pauses itself and lets go.
    p.devs[0].board.pause("t6").unwrap();
    spread(&p, 0);
    let lost = tokio::time::timeout(CLAIM * 4, c.lost()).await.expect("paused");
    assert_eq!(lost, Lost::PausedByOwner);
    spread(&p, 1);
    let r = p.devs[0].board.get("t6").unwrap().unwrap();
    assert_eq!((r.state, r.attempts), (JobState::Paused, 0));
    p.devs[0].board.cancel("t6").unwrap();
    assert_eq!(state(&p.devs[0], "t6"), JobState::Cancelled);
    assert!(matches!(p.devs[0].board.claim("t6", |_| true).await, Err(JobError::NotClaimable(JobState::Cancelled))));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_eligible_devices_claim() {
    // Device 2 has no `ymir.model` grant.
    let p = pool(&[true, true, false]);
    let only0 = policy().with_candidates([p.devs[0].id()]);
    p.devs[0].board.post(job("t7").with_policy(only0)).unwrap();
    p.devs[0].board.post(job("t8")).unwrap();
    spread(&p, 0);
    assert!(matches!(p.devs[1].board.claim("t7", |_| true).await, Err(JobError::Ineligible)), "not a candidate");
    assert!(matches!(p.devs[2].board.claim("t8", |_| true).await, Err(JobError::Ineligible)), "no role grant");
    assert!(matches!(p.devs[1].board.claim("t8", |r| r.payload.base == "other").await, Err(JobError::Ineligible)), "the app's own check");
    let c = claim_when_free(&p.devs[1], "t8", CLAIM * 4).await;
    c.finish(vec![]).await.unwrap();
    let c = claim_when_free(&p.devs[0], "t7", CLAIM * 4).await;
    c.finish(vec![]).await.unwrap();
}
