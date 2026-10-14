//! One-way stop signal between a session's tasks.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use futures_util::task::AtomicWaker;

/// Stops one session task (read or write); cloned to whoever may stop it.
///
/// Used instead of a `CancellationToken` on the I/O hot loops, which check and poll their
/// stop signals on every frame and every loop turn: a token takes its mutex for each check
/// and its inner `Notify`'s mutex for each poll. Here a check is one atomic load and a poll
/// one [`AtomicWaker`] registration.
///
/// Only the stopped task may await [`Self::cancelled`] (any number of futures within that
/// task): the waker slot holds one waker.
#[derive(Debug, Clone, Default)]
pub(crate) struct StopSignal(Arc<Inner>);

#[derive(Debug, Default)]
struct Inner {
    stopped: AtomicBool,
    waker: AtomicWaker,
}

impl StopSignal {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Signals the stop; idempotent.
    pub(crate) fn cancel(&self) {
        self.0.stopped.store(true, Ordering::Release);
        self.0.waker.wake();
    }

    #[inline]
    pub(crate) fn is_cancelled(&self) -> bool {
        self.0.stopped.load(Ordering::Acquire)
    }

    /// Resolves once [`Self::cancel`] was called.
    pub(crate) fn cancelled(&self) -> Cancelled<'_> {
        Cancelled(self)
    }
}

/// Future returned by [`StopSignal::cancelled`].
#[derive(Debug)]
#[must_use = "futures do nothing unless polled"]
pub(crate) struct Cancelled<'a>(&'a StopSignal);

impl Future for Cancelled<'_> {
    type Output = ();

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0.is_cancelled() {
            return Poll::Ready(());
        }
        self.0.0.waker.register(cx.waker());
        // A cancel between the first check and the registration woke no one.
        if self.0.is_cancelled() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn cancel_wakes_the_waiting_task() {
        let stop = StopSignal::new();
        let waiter = tokio::spawn({
            let stop = stop.clone();
            async move { stop.cancelled().await }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!waiter.is_finished());
        stop.cancel();
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("woken")
            .expect("joined");
        assert!(stop.is_cancelled());
    }

    #[tokio::test]
    async fn cancel_before_await_resolves_at_once() {
        let stop = StopSignal::new();
        stop.cancel();
        stop.cancel();
        stop.cancelled().await;
    }
}
