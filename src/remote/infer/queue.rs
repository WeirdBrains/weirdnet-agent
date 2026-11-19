//! Admission: how many requests a node runs at once, and a priority queue for the rest.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use tokio::sync::watch;

use super::v2::Priority;
use crate::remote::RemoteError;

/// How many requests a node serves at once, and how many more may wait their turn.
///
/// Infer/1 never waits: with every slot taken it answers `Busy` so the asker moves on. Infer/2
/// requests wait in a priority queue (`interactive` before `batch` before `background`, first
/// come first served within a class) and are told their place with `Queued` frames; only when
/// the queue is full too is the answer `Busy`. Both protocols share the same slots.
#[derive(Clone)]
pub struct Admission {
    inner: Arc<Inner>,
}

struct Inner {
    state: Mutex<State>,
    changed: watch::Sender<u64>,
}

struct State {
    free: usize,
    max_waiting: usize,
    waiting: BTreeSet<(u8, u64)>,
    next_seq: u64,
    generation: u64,
}

fn rank(p: Priority) -> u8 {
    match p {
        Priority::Interactive => 0,
        Priority::Batch => 1,
        Priority::Background => 2,
    }
}

impl Admission {
    /// `max_concurrent` slots and no queue.
    pub fn new(max_concurrent: usize) -> Self {
        Self::with_queue(max_concurrent, 0)
    }

    /// `max_concurrent` slots and up to `max_waiting` infer/2 requests waiting for one.
    pub fn with_queue(max_concurrent: usize, max_waiting: usize) -> Self {
        let state = State { free: max_concurrent, max_waiting, waiting: BTreeSet::new(), next_seq: 0, generation: 0 };
        Self { inner: Arc::new(Inner { state: Mutex::new(state), changed: watch::channel(0).0 }) }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.inner.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Infer/2 requests waiting for a slot now.
    pub fn waiting(&self) -> usize {
        self.lock().waiting.len()
    }

    /// Free slots now.
    pub fn free(&self) -> usize {
        self.lock().free
    }

    /// A slot now, without queueing (infer/1). Never jumps ahead of waiting requests.
    pub(crate) fn try_take(&self) -> Option<Permit> {
        let mut s = self.lock();
        if s.free == 0 || !s.waiting.is_empty() {
            return None;
        }
        s.free -= 1;
        Some(Permit { inner: self.inner.clone() })
    }

    /// Joins the queue (infer/2). `Busy` when every slot is taken and the queue is full.
    pub fn enter(&self, priority: Priority) -> Result<Ticket, RemoteError> {
        let mut s = self.lock();
        if s.free == 0 && s.waiting.len() >= s.max_waiting {
            return Err(RemoteError::Busy);
        }
        let key = (rank(priority), s.next_seq);
        s.next_seq += 1;
        s.waiting.insert(key);
        s.generation += 1;
        drop(s);
        self.inner.changed.send_replace(0);
        Ok(Ticket { inner: self.inner.clone(), key, done: false })
    }
}

impl Inner {
    fn bump(&self, s: &mut State) {
        s.generation += 1;
        let g = s.generation;
        self.changed.send_replace(g);
    }
}

/// A running request's slot; dropping it frees the slot for the next one in the queue.
pub struct Permit {
    inner: Arc<Inner>,
}

impl Drop for Permit {
    fn drop(&mut self) {
        let mut s = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        s.free += 1;
        self.inner.bump(&mut s);
    }
}

/// A place in the queue. Dropping it leaves the queue.
pub struct Ticket {
    inner: Arc<Inner>,
    key: (u8, u64),
    done: bool,
}

/// What [`Ticket::turn`] found.
pub enum Turn {
    /// A slot is ours.
    Go(Permit),
    /// Still waiting, at this place (1 = next).
    Waiting(u32),
}

impl Ticket {
    /// The place in the queue (1 = next).
    pub fn position(&self) -> u32 {
        let s = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        s.waiting.range(..self.key).count() as u32 + 1
    }

    /// Waits until a slot is ours or the place changes from `last`; after `keepalive` with no
    /// change it reports the same place again (each `Queued` frame resets the asker's timer).
    pub async fn turn(&mut self, last: Option<u32>, keepalive: Duration) -> Turn {
        loop {
            let mut rx = {
                let mut s = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
                if s.free > 0 && s.waiting.first() == Some(&self.key) {
                    s.waiting.remove(&self.key);
                    s.free -= 1;
                    self.done = true;
                    self.inner.bump(&mut s);
                    return Turn::Go(Permit { inner: self.inner.clone() });
                }
                let pos = s.waiting.range(..self.key).count() as u32 + 1;
                if last != Some(pos) {
                    return Turn::Waiting(pos);
                }
                self.inner.changed.subscribe()
            };
            tokio::select! {
                _ = rx.changed() => {}
                _ = tokio::time::sleep(keepalive) => return Turn::Waiting(self.position()),
            }
        }
    }
}

impl Drop for Ticket {
    fn drop(&mut self) {
        if !self.done {
            let mut s = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
            s.waiting.remove(&self.key);
            self.inner.bump(&mut s);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const K: Duration = Duration::from_secs(60);

    #[tokio::test]
    async fn interactive_goes_before_batch_and_places_move_up() {
        let a = Admission::with_queue(1, 8);
        let mut first = a.enter(Priority::Batch).unwrap();
        let Turn::Go(running) = first.turn(None, K).await else { panic!("a free slot") };
        let mut batch = a.enter(Priority::Batch).unwrap();
        let mut background = a.enter(Priority::Background).unwrap();
        let mut chat = a.enter(Priority::Interactive).unwrap();
        assert_eq!((chat.position(), batch.position(), background.position()), (1, 2, 3));
        assert!(matches!(batch.turn(None, K).await, Turn::Waiting(2)));
        drop(running);
        let Turn::Go(p) = chat.turn(None, K).await else { panic!("chat is next") };
        assert!(matches!(batch.turn(Some(2), K).await, Turn::Waiting(1)));
        drop(p);
        assert!(matches!(batch.turn(Some(1), K).await, Turn::Go(_)));
        assert!(matches!(background.turn(None, K).await, Turn::Go(_)));
    }

    #[tokio::test]
    async fn a_full_queue_is_busy_and_leaving_frees_the_place() {
        let a = Admission::with_queue(1, 1);
        let _running = a.try_take().unwrap();
        let waiter = a.enter(Priority::Interactive).unwrap();
        assert!(matches!(a.enter(Priority::Interactive), Err(RemoteError::Busy)));
        assert!(a.try_take().is_none());
        drop(waiter);
        assert_eq!(a.waiting(), 0);
        assert!(a.enter(Priority::Interactive).is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn the_same_place_is_repeated_after_the_keepalive() {
        let a = Admission::with_queue(1, 4);
        let _running = a.try_take().unwrap();
        let mut t = a.enter(Priority::Batch).unwrap();
        assert!(matches!(t.turn(None, Duration::from_secs(3)).await, Turn::Waiting(1)));
        assert!(matches!(t.turn(Some(1), Duration::from_secs(3)).await, Turn::Waiting(1)));
    }
}
