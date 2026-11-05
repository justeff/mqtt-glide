//! Acknowledgement timeouts of a client's publishes, kept by one task per driver.
//!
//! A `tokio::time::timeout` per publish registers with, and on completion deregisters from,
//! the runtime's timer wheel under its lock. With many callers on many worker threads that
//! lock dominates a QoS 0 publish. Here a waiting publish pushes its deadline onto a lock-free
//! queue and forgets it when answered; the sweeper drops answered deadlines when it gets to
//! them and wakes only the publishes whose deadline passed.

use std::cmp::{Ordering as CmpOrdering, Reverse};
use std::collections::BinaryHeap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering, fence};
use std::sync::{Arc, Mutex, mpsc};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::task::AtomicWaker;
use tokio::sync::Notify;
use tokio::time::Instant;

/// How often a busy sweeper takes in new deadlines; also how late a timeout may fire.
const TICK: Duration = Duration::from_millis(1);

/// Deadlines kept before the sweeper first sheds answered ones.
const MIN_COMPACT: usize = 1024;

const WAITING: u8 = 0;
const ANSWERED: u8 = 1;
const EXPIRED: u8 = 2;

/// Shared by the client (registers deadlines) and the driver (runs the sweeper).
#[derive(Debug, Clone)]
pub(crate) struct AckDeadlines(Arc<Shared>);

#[derive(Debug)]
struct Shared {
    intake: mpsc::Sender<Entry>,
    /// Taken by the running sweeper.
    intake_rx: Mutex<Option<mpsc::Receiver<Entry>>>,
    /// A sweeper is running; without one, waits fall back to a runtime timer.
    active: AtomicBool,
    /// The sweeper is asleep until its earliest deadline and must be told about new ones.
    idle: AtomicBool,
    wake: Notify,
    /// Deadlines the sweeper keeps, for tests.
    #[cfg(test)]
    kept: std::sync::atomic::AtomicUsize,
}

#[derive(Debug, Default)]
struct Slot {
    state: AtomicU8,
    waker: AtomicWaker,
}

#[derive(Debug)]
struct Entry {
    deadline: Instant,
    slot: Arc<Slot>,
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.deadline == other.deadline
    }
}

impl Eq for Entry {}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for Entry {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.deadline.cmp(&other.deadline)
    }
}

impl Entry {
    fn answered(&self) -> bool {
        self.slot.state.load(Ordering::Acquire) == ANSWERED
    }

    fn expire(&self) {
        if self
            .slot
            .state
            .compare_exchange(WAITING, EXPIRED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.slot.waker.wake();
        }
    }
}

impl Default for AckDeadlines {
    fn default() -> Self {
        let (intake, intake_rx) = mpsc::channel();
        Self(Arc::new(Shared {
            intake,
            intake_rx: Mutex::new(Some(intake_rx)),
            active: AtomicBool::new(false),
            idle: AtomicBool::new(false),
            wake: Notify::new(),
            #[cfg(test)]
            kept: std::sync::atomic::AtomicUsize::new(0),
        }))
    }
}

impl AckDeadlines {
    /// Starts timing out `timeout` from now; `None` when no sweeper runs (use a runtime timer).
    #[inline]
    pub(crate) fn start(&self, timeout: Duration) -> Option<AckDeadline> {
        if !self.0.active.load(Ordering::Acquire) {
            return None;
        }
        let deadline = Instant::now().checked_add(timeout)?;
        let slot = Arc::new(Slot::default());
        let entry = Entry {
            deadline,
            slot: Arc::clone(&slot),
        };
        // Fails only once the sweeper's receiver is gone, together with the driver: the
        // driver dropped the publish too, so its answer (closed) arrives without a timeout.
        let _ = self.0.intake.send(entry);
        // Pairs with the fence in `Sweeper::sleep`: either it sees this entry, or this sees idle.
        fence(Ordering::SeqCst);
        if self.0.idle.load(Ordering::Relaxed) && self.0.idle.swap(false, Ordering::AcqRel) {
            self.0.wake.notify_one();
        }
        Some(AckDeadline { slot })
    }

    /// Times out deadlines until the returned future is dropped (with the driver).
    pub(crate) fn sweep(&self) -> impl Future<Output = ()> + Send + 'static {
        let shared = Arc::clone(&self.0);
        async move {
            let Some(intake) = shared.intake_rx.lock().ok().and_then(|mut rx| rx.take()) else {
                return;
            };
            let mut sweeper = Sweeper {
                shared,
                intake,
                pending: BinaryHeap::new(),
                compact_at: MIN_COMPACT,
            };
            sweeper.shared.active.store(true, Ordering::Release);
            sweeper.run().await;
        }
    }
}

/// One publish's deadline; forgets it on drop.
#[derive(Debug)]
pub(crate) struct AckDeadline {
    slot: Arc<Slot>,
}

impl AckDeadline {
    /// Waits for `answer` or the deadline, whichever comes first; `Err(())` on the deadline.
    pub(crate) fn race<'a, F>(&'a self, answer: &'a mut F) -> Race<'a, F>
    where
        F: Future + Unpin,
    {
        Race { deadline: self, answer }
    }

    fn expired(&self) -> bool {
        self.slot.state.load(Ordering::Acquire) == EXPIRED
    }
}

impl Drop for AckDeadline {
    fn drop(&mut self) {
        let _ = self
            .slot
            .state
            .compare_exchange(WAITING, ANSWERED, Ordering::AcqRel, Ordering::Relaxed);
        // The sweeper keeps the slot until its deadline or the next compaction; the caller's
        // waker in it would keep the caller's task allocation alive that long.
        drop(self.slot.waker.take());
    }
}

/// Future of [`AckDeadline::race`].
#[must_use = "futures do nothing unless polled"]
pub(crate) struct Race<'a, F> {
    deadline: &'a AckDeadline,
    answer: &'a mut F,
}

impl<F: Future + Unpin> Future for Race<'_, F> {
    type Output = Result<F::Output, ()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Poll::Ready(answer) = Pin::new(&mut *this.answer).poll(cx) {
            return Poll::Ready(Ok(answer));
        }
        if this.deadline.expired() {
            return Poll::Ready(Err(()));
        }
        this.deadline.slot.waker.register(cx.waker());
        // An expiry between the check and the registration woke no one.
        if this.deadline.expired() {
            Poll::Ready(Err(()))
        } else {
            Poll::Pending
        }
    }
}

struct Sweeper {
    shared: Arc<Shared>,
    intake: mpsc::Receiver<Entry>,
    pending: BinaryHeap<Reverse<Entry>>,
    compact_at: usize,
}

impl Drop for Sweeper {
    fn drop(&mut self) {
        self.shared.active.store(false, Ordering::Release);
    }
}

impl Sweeper {
    async fn run(&mut self) {
        let tick = tokio::time::sleep(TICK);
        tokio::pin!(tick);
        loop {
            let took = self.take_in();
            self.expire(Instant::now());
            self.compact();
            #[cfg(test)]
            self.shared.kept.store(self.pending.len(), Ordering::Relaxed);
            if took {
                // Busy: come back for new deadlines soon, without anyone having to wake us.
                tick.as_mut().reset(Instant::now() + TICK);
                tick.as_mut().await;
                continue;
            }
            self.sleep(tick.as_mut()).await;
        }
    }

    /// Moves new deadlines into `pending`, dropping the ones answered already.
    fn take_in(&mut self) -> bool {
        let mut took = false;
        while let Ok(entry) = self.intake.try_recv() {
            took = true;
            if !entry.answered() {
                self.pending.push(Reverse(entry));
            }
        }
        took
    }

    fn expire(&mut self, now: Instant) {
        while let Some(Reverse(entry)) = self.pending.peek()
            && entry.deadline <= now
        {
            if let Some(Reverse(entry)) = self.pending.pop() {
                entry.expire();
            }
        }
    }

    /// Sheds answered deadlines once `pending` doubled, so it stays proportional to the
    /// publishes still waiting, at constant cost per deadline.
    fn compact(&mut self) {
        if self.pending.len() >= self.compact_at {
            self.pending.retain(|Reverse(entry)| !entry.answered());
            self.compact_at = (self.pending.len() * 2).max(MIN_COMPACT);
        }
    }

    /// Idle: sleeps until the earliest deadline or until a new one is pushed.
    async fn sleep(&mut self, mut tick: Pin<&mut tokio::time::Sleep>) {
        self.shared.idle.store(true, Ordering::Relaxed);
        // Pairs with the fence in `AckDeadlines::start`.
        fence(Ordering::SeqCst);
        if self.take_in() {
            self.shared.idle.store(false, Ordering::Relaxed);
            return;
        }
        let woken = self.shared.wake.notified();
        match self.pending.peek() {
            Some(Reverse(entry)) => {
                tick.as_mut().reset(entry.deadline);
                tokio::select! {
                    () = woken => {}
                    () = tick => {}
                }
            }
            None => woken.await,
        }
        self.shared.idle.store(false, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tokio::sync::oneshot;

    fn running() -> (AckDeadlines, tokio::task::JoinHandle<()>) {
        let deadlines = AckDeadlines::default();
        let sweeper = tokio::spawn(deadlines.sweep());
        (deadlines, sweeper)
    }

    async fn until_active(deadlines: &AckDeadlines) {
        while !deadlines.0.active.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn without_a_sweeper_no_deadline_is_kept() {
        let deadlines = AckDeadlines::default();
        assert!(deadlines.start(Duration::from_secs(1)).is_none());
    }

    #[tokio::test]
    async fn an_unanswered_wait_expires_at_its_deadline() {
        let (deadlines, _sweeper) = running();
        until_active(&deadlines).await;
        let (_tx, mut rx) = oneshot::channel::<()>();
        let started = Instant::now();
        let deadline = deadlines.start(Duration::from_millis(50)).expect("sweeper runs");
        assert_eq!(deadline.race(&mut rx).await, Err(()));
        let waited = started.elapsed();
        assert!(waited >= Duration::from_millis(50), "{waited:?}");
        assert!(waited < Duration::from_millis(500), "{waited:?}");
    }

    #[tokio::test]
    async fn an_answer_wins_and_the_sweeper_goes_idle() {
        let (deadlines, _sweeper) = running();
        until_active(&deadlines).await;
        let (tx, mut rx) = oneshot::channel::<u8>();
        let deadline = deadlines.start(Duration::from_secs(30)).expect("sweeper runs");
        tx.send(7).expect("send");
        assert_eq!(deadline.race(&mut rx).await, Ok(Ok(7)));
        drop(deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(deadlines.0.idle.load(Ordering::Acquire), "nothing left to time out");
    }

    /// A shorter deadline pushed while the sweeper sleeps until a later one still fires on time.
    #[tokio::test]
    async fn a_shorter_deadline_wakes_an_idle_sweeper() {
        let (deadlines, _sweeper) = running();
        until_active(&deadlines).await;
        let (_long_tx, mut long_rx) = oneshot::channel::<()>();
        let long = deadlines.start(Duration::from_secs(30)).expect("sweeper runs");
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(deadlines.0.idle.load(Ordering::Acquire));

        let (_tx, mut rx) = oneshot::channel::<()>();
        let started = Instant::now();
        let short = deadlines.start(Duration::from_millis(20)).expect("sweeper runs");
        assert_eq!(short.race(&mut rx).await, Err(()));
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(!long.expired());
        assert!(long_rx.try_recv().is_err());
    }

    /// Deadlines answered after the sweeper took them in do not pile up until they expire.
    #[tokio::test]
    async fn answered_deadlines_are_shed() {
        let (deadlines, _sweeper) = running();
        until_active(&deadlines).await;
        let _stuck = deadlines.start(Duration::from_secs(30)).expect("sweeper runs");
        let mut most = 0;
        for _ in 0..50 {
            let batch: Vec<_> = (0..1_000)
                .map(|_| deadlines.start(Duration::from_secs(30)).expect("sweeper runs"))
                .collect();
            tokio::time::sleep(TICK).await;
            drop(batch);
            most = most.max(deadlines.0.kept.load(Ordering::Relaxed));
        }
        assert!(most < 4 * MIN_COMPACT, "kept {most} of 50001 deadlines");
    }

    #[tokio::test]
    async fn stopping_the_sweeper_falls_back_to_runtime_timers() {
        let (deadlines, sweeper) = running();
        until_active(&deadlines).await;
        sweeper.abort();
        let _ = sweeper.await;
        assert!(deadlines.start(Duration::from_secs(1)).is_none());
    }
}
