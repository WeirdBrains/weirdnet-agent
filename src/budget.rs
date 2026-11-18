//! Research budgets: searches, pages, concurrency and time.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
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
    slots: Arc<Semaphore>,
}

impl BudgetTracker {
    pub fn start(budget: Budget) -> Self {
        Self {
            budget,
            searches: AtomicU32::new(0),
            pages: AtomicU32::new(0),
            extensions: AtomicU32::new(0),
            started: Instant::now(),
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

    pub fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }

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
    async fn time_budget_runs_out() {
        let t = BudgetTracker::start(Budget::default());
        assert_eq!(t.page_timeout(), Duration::from_secs(15));
        tokio::time::advance(Duration::from_secs(80)).await;
        assert_eq!(t.page_timeout(), Duration::from_secs(10));
        tokio::time::advance(Duration::from_secs(10)).await;
        assert_eq!(t.check_time(), Err(BudgetExceeded::Time { limit_ms: 90_000 }));
        assert!(matches!(t.charge_page(), Err(BudgetExceeded::Time { .. })));
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
