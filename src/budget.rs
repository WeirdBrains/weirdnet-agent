//! Research budgets: searches, pages, concurrency and time.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

/// Limits for one task. Defaults follow Companions design decision 21.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Budget {
    pub max_searches: u32,
    pub max_pages: u32,
    /// Pages added by one user-approved extension ("+5 pages").
    pub page_extension: u32,
    /// Hidden views or fetches at a time.
    pub max_concurrent: u32,
    pub per_page: Duration,
    /// Time the task's tools may run, in total. Only time inside tool calls counts (see
    /// [`BudgetTracker::time_tool`]): model turns and waiting for the user to answer an
    /// approval card do not.
    pub per_task: Duration,
    /// Tokens of extracted text kept per page.
    pub tokens_per_page: u32,
    /// Model turns per task.
    pub max_steps: u32,
}

impl Default for Budget {
    fn default() -> Self {
        Self {
            max_searches: 3,
            max_pages: 12,
            page_extension: 5,
            max_concurrent: 2,
            per_page: Duration::from_secs(15),
            per_task: Duration::from_secs(90),
            tokens_per_page: 4_000,
            max_steps: 16,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum BudgetExceeded {
    #[error("search budget used ({limit} searches)")]
    Searches { limit: u32 },
    #[error("page budget used ({limit} pages)")]
    Pages { limit: u32 },
    #[error("task time budget used ({limit_ms} ms)")]
    Time { limit_ms: u64 },
    #[error("step budget used ({limit} steps)")]
    Steps { limit: u32 },
}

/// What a task has used so far.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct BudgetUsage {
    pub searches: u32,
    pub pages: u32,
    pub page_limit: u32,
    /// Tool time used, the time counted against [`Budget::per_task`].
    pub elapsed_ms: u64,
}

/// Live counters for one task. Cheap to share by reference across tool calls.
#[derive(Debug)]
pub struct BudgetTracker {
    budget: Budget,
    searches: AtomicU32,
    pages: AtomicU32,
    extensions: AtomicU32,
    started: Instant,
    tools: Mutex<ToolClock>,
    slots: Arc<Semaphore>,
}

/// Time spent in tool calls. Overlapping calls count once (wall time while any runs).
#[derive(Debug, Default)]
struct ToolClock {
    spent: Duration,
    running: u32,
    since: Option<Instant>,
}

impl ToolClock {
    fn now(&self) -> Duration {
        self.spent + self.since.map(|t| t.elapsed()).unwrap_or_default()
    }
}

/// Counts tool time against the task budget until dropped (see [`BudgetTracker::time_tool`]).
#[derive(Debug)]
#[must_use = "tool time counts only while the timer is held"]
pub struct ToolTimer<'a> {
    tracker: &'a BudgetTracker,
}

impl Drop for ToolTimer<'_> {
    fn drop(&mut self) {
        let mut c = self.tracker.tools.lock().unwrap_or_else(|e| e.into_inner());
        c.running = c.running.saturating_sub(1);
        if c.running == 0
            && let Some(t) = c.since.take()
        {
            c.spent += t.elapsed();
        }
    }
}

impl BudgetTracker {
    pub fn start(budget: Budget) -> Self {
        Self {
            budget,
            searches: AtomicU32::new(0),
            pages: AtomicU32::new(0),
            extensions: AtomicU32::new(0),
            started: Instant::now(),
            tools: Mutex::new(ToolClock::default()),
            slots: Arc::new(Semaphore::new(budget.max_concurrent.max(1) as usize)),
        }
    }

    pub fn budget(&self) -> &Budget {
        &self.budget
    }

    fn take(counter: &AtomicU32, limit: u32) -> bool {
        counter.try_update(Ordering::SeqCst, Ordering::SeqCst, |n| (n < limit).then_some(n + 1)).is_ok()
    }

    pub fn charge_search(&self) -> Result<(), BudgetExceeded> {
        self.check_time()?;
        let limit = self.budget.max_searches;
        if Self::take(&self.searches, limit) { Ok(()) } else { Err(BudgetExceeded::Searches { limit }) }
    }

    pub fn page_limit(&self) -> u32 {
        self.budget.max_pages + self.extensions.load(Ordering::SeqCst) * self.budget.page_extension
    }

    pub fn charge_page(&self) -> Result<(), BudgetExceeded> {
        self.check_time()?;
        let limit = self.page_limit();
        if Self::take(&self.pages, limit) { Ok(()) } else { Err(BudgetExceeded::Pages { limit }) }
    }

    /// Adds one page extension. Call only after the user asked for more pages.
    pub fn extend_pages(&self) -> u32 {
        self.extensions.fetch_add(1, Ordering::SeqCst);
        self.page_limit()
    }

    /// Tool time used so far: what [`Budget::per_task`] limits.
    pub fn elapsed(&self) -> Duration {
        self.tools.lock().unwrap_or_else(|e| e.into_inner()).now()
    }

    /// Wall time since the task started, model turns and approval waits included (not limited;
    /// for display and logs).
    pub fn wall_elapsed(&self) -> Duration {
        self.started.elapsed()
    }

    /// Starts counting tool time; it counts until the timer is dropped. The session holds one
    /// around every tool call. Timers that overlap count the shared time once.
    pub fn time_tool(&self) -> ToolTimer<'_> {
        let mut c = self.tools.lock().unwrap_or_else(|e| e.into_inner());
        if c.running == 0 {
            c.since = Some(Instant::now());
        }
        c.running += 1;
        ToolTimer { tracker: self }
    }

    /// Tool time left.
    pub fn remaining(&self) -> Duration {
        self.budget.per_task.saturating_sub(self.elapsed())
    }

    pub fn check_time(&self) -> Result<(), BudgetExceeded> {
        if self.remaining().is_zero() { Err(BudgetExceeded::Time { limit_ms: self.budget.per_task.as_millis() as u64 }) } else { Ok(()) }
    }

    /// Time a page fetch may take: the per-page limit or what's left of the task.
    pub fn page_timeout(&self) -> Duration {
        self.budget.per_page.min(self.remaining())
    }

    /// Waits for a concurrency slot (hidden view or fetch).
    pub async fn acquire_slot(&self) -> OwnedSemaphorePermit {
        self.slots.clone().acquire_owned().await.expect("budget semaphore is never closed")
    }

    pub fn usage(&self) -> BudgetUsage {
        BudgetUsage {
            searches: self.searches.load(Ordering::SeqCst),
            pages: self.pages.load(Ordering::SeqCst),
            page_limit: self.page_limit(),
            elapsed_ms: self.elapsed().as_millis() as u64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn searches_and_pages_are_capped_and_extendable() {
        let t = BudgetTracker::start(Budget::default());
        for _ in 0..3 {
            t.charge_search().unwrap();
        }
        assert_eq!(t.charge_search(), Err(BudgetExceeded::Searches { limit: 3 }));
        for _ in 0..12 {
            t.charge_page().unwrap();
        }
        assert_eq!(t.charge_page(), Err(BudgetExceeded::Pages { limit: 12 }));
        assert_eq!(t.extend_pages(), 17);
        for _ in 0..5 {
            t.charge_page().unwrap();
        }
        assert!(t.charge_page().is_err());
        let u = t.usage();
        assert_eq!((u.searches, u.pages, u.page_limit), (3, 17, 17));
    }

    #[tokio::test(start_paused = true)]
    async fn time_budget_runs_out_on_tool_time() {
        let t = BudgetTracker::start(Budget::default());
        assert_eq!(t.page_timeout(), Duration::from_secs(15));
        let timer = t.time_tool();
        tokio::time::advance(Duration::from_secs(80)).await;
        assert_eq!(t.page_timeout(), Duration::from_secs(10));
        tokio::time::advance(Duration::from_secs(10)).await;
        drop(timer);
        assert_eq!(t.check_time(), Err(BudgetExceeded::Time { limit_ms: 90_000 }));
        assert!(matches!(t.charge_page(), Err(BudgetExceeded::Time { .. })));
        assert_eq!(t.usage().elapsed_ms, 90_000);
    }

    #[tokio::test(start_paused = true)]
    async fn only_tool_time_counts() {
        let t = BudgetTracker::start(Budget::default());
        // A model turn or an approval card: no timer, nothing counted.
        tokio::time::advance(Duration::from_secs(600)).await;
        assert_eq!(t.elapsed(), Duration::ZERO);
        assert!(t.check_time().is_ok());
        {
            let _call = t.time_tool();
            tokio::time::advance(Duration::from_secs(5)).await;
            assert_eq!(t.elapsed(), Duration::from_secs(5), "counts while the call runs");
        }
        tokio::time::advance(Duration::from_secs(60)).await;
        assert_eq!(t.elapsed(), Duration::from_secs(5));
        assert_eq!(t.wall_elapsed(), Duration::from_secs(665));
        assert_eq!(t.remaining(), Duration::from_secs(85));
    }

    #[tokio::test(start_paused = true)]
    async fn overlapping_tool_calls_count_once() {
        let t = BudgetTracker::start(Budget::default());
        let a = t.time_tool();
        tokio::time::advance(Duration::from_secs(2)).await;
        let b = t.time_tool();
        tokio::time::advance(Duration::from_secs(3)).await;
        drop(a);
        tokio::time::advance(Duration::from_secs(1)).await;
        drop(b);
        tokio::time::advance(Duration::from_secs(10)).await;
        assert_eq!(t.elapsed(), Duration::from_secs(6));
    }

    #[tokio::test(start_paused = true)]
    async fn concurrency_slots() {
        let t = BudgetTracker::start(Budget::default());
        let a = t.acquire_slot().await;
        let _b = t.acquire_slot().await;
        let third = tokio::time::timeout(Duration::from_millis(10), t.acquire_slot()).await;
        assert!(third.is_err());
        drop(a);
        let _c = tokio::time::timeout(Duration::from_millis(10), t.acquire_slot()).await.unwrap();
    }

    #[test]
    fn serde_roundtrip() {
        let b = Budget::default();
        let back: Budget = serde_json::from_str(&serde_json::to_string(&b).unwrap()).unwrap();
        assert_eq!(b, back);
    }
}
