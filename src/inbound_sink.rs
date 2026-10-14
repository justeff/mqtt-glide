//! Injectable inbound PUBLISH sink (enqueue-before-ack).
//!
//! The read task delivers each inbound [`InboundMessage`] through a [`InboundSink`]
//! **before** QoS 1 PUBACK. Built-ins: [`flume::Sender`], [`tokio::sync::mpsc::Sender`],
//! and [`OnInboundSink`] (inline handler via [`crate::ClientBuilder::on_inbound`]).
//!
//! # Cancel safety
//!
//! On shutdown/session cancel the read task may drop a pending [`InboundSink::send`]
//! future. That **MUST** mean the message was **not** accepted into the sink (so the
//! client correctly skips PUBACK). flume `send_async`, Tokio `mpsc::Sender::send`, and
//! [`OnInboundSink`] satisfy this. Custom sinks **MUST** uphold the same contract: do
//! not deliver to the consumer before the `send` future completes with `Ok(())`.

use std::future::Future;
use std::sync::Arc;

use crate::codec::InboundMessage;

/// The publish-sink consumer is gone; further pushes fail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InboundSinkClosed;

/// Failure of a non-blocking [`InboundSink::try_send`].
///
/// Carries the message by value (same shape as flume / Tokio `TrySendError`) so the
/// full-buffer path can await [`InboundSink::send`] without an extra heap allocation.
/// Clippy `result_large_err` is allowed: `Ok(())` is the hot path; `Err` is stall/close.
#[derive(Debug)]
#[allow(clippy::result_large_err)] // message must round-trip by value on Full/Closed
pub enum InboundTrySendError {
    /// Channel is at capacity; message is returned for a blocking [`InboundSink::send`].
    Full(InboundMessage),
    /// No receivers remain; message is returned.
    Closed(InboundMessage),
}

/// Injectable sink for inbound [`InboundMessage`] delivery.
///
/// Implementors **MUST** be cheap to [`Clone`] so the connection driver can keep an
/// anchor handle across read-task respawn without closing the channel.
///
/// [`Self::send`] **MUST** be cancel-safe: dropping the returned future before
/// `Ready(Ok(()))` means the message was not accepted (see module-level cancel safety).
pub trait InboundSink: Clone + Send + Sync + 'static {
    /// Whether further sends will fail because the consumer side is gone.
    fn is_closed(&self) -> bool;

    /// Non-blocking push. On [`InboundTrySendError::Full`], call [`Self::send`].
    #[allow(clippy::result_large_err)] // see [`InboundTrySendError`]
    fn try_send(&self, message: InboundMessage) -> Result<(), InboundTrySendError>;

    /// Async push with backpressure until capacity is available or the sink closes.
    ///
    /// Returns a [`Send`] future so the read task can run on a multi-thread runtime.
    /// See cancel safety in the [module docs](self).
    fn send(&self, message: InboundMessage) -> impl Future<Output = Result<(), InboundSinkClosed>> + Send;
}

impl InboundSink for flume::Sender<InboundMessage> {
    #[inline]
    fn is_closed(&self) -> bool {
        self.is_disconnected()
    }

    #[inline]
    fn try_send(&self, message: InboundMessage) -> Result<(), InboundTrySendError> {
        match flume::Sender::try_send(self, message) {
            Ok(()) => Ok(()),
            Err(flume::TrySendError::Full(message)) => Err(InboundTrySendError::Full(message)),
            Err(flume::TrySendError::Disconnected(message)) => Err(InboundTrySendError::Closed(message)),
        }
    }

    fn send(&self, message: InboundMessage) -> impl Future<Output = Result<(), InboundSinkClosed>> + Send {
        let sender = self.clone();
        async move { sender.send_async(message).await.map_err(|_| InboundSinkClosed) }
    }
}

impl InboundSink for tokio::sync::mpsc::Sender<InboundMessage> {
    #[inline]
    fn is_closed(&self) -> bool {
        tokio::sync::mpsc::Sender::is_closed(self)
    }

    #[inline]
    fn try_send(&self, message: InboundMessage) -> Result<(), InboundTrySendError> {
        match tokio::sync::mpsc::Sender::try_send(self, message) {
            Ok(()) => Ok(()),
            Err(tokio::sync::mpsc::error::TrySendError::Full(message)) => Err(InboundTrySendError::Full(message)),
            Err(tokio::sync::mpsc::error::TrySendError::Closed(message)) => Err(InboundTrySendError::Closed(message)),
        }
    }

    fn send(&self, message: InboundMessage) -> impl Future<Output = Result<(), InboundSinkClosed>> + Send {
        let sender = self.clone();
        async move {
            tokio::sync::mpsc::Sender::send(&sender, message)
                .await
                .map_err(|_| InboundSinkClosed)
        }
    }
}

/// Inline inbound handoff: runs `handler` on the connection **read task** (no app queue).
///
/// Prefer a channel [`InboundSink`] (`flume` / `tokio::mpsc`) for non-trivial work —
/// this path stalls decode and keepalive while `handler` runs. QoS 1 `PUBACK` is sent
/// only after `handler` returns. Built for [`crate::ClientBuilder::on_inbound`]; also
/// usable with [`crate::connect`].
///
/// # Cancel safety
///
/// [`InboundSink::send`] invokes `handler` synchronously on the first poll and then
/// returns `Ok(())`. Dropping the future before that poll means the message was not
/// delivered.
pub struct OnInboundSink<F> {
    handler: Arc<F>,
}

impl<F> OnInboundSink<F>
where
    F: Fn(InboundMessage) + Send + Sync + 'static,
{
    /// Wraps `handler` for use as a [`InboundSink`].
    pub fn new(handler: F) -> Self {
        Self {
            handler: Arc::new(handler),
        }
    }
}

impl<F> Clone for OnInboundSink<F> {
    fn clone(&self) -> Self {
        Self {
            handler: Arc::clone(&self.handler),
        }
    }
}

impl<F> InboundSink for OnInboundSink<F>
where
    F: Fn(InboundMessage) + Send + Sync + 'static,
{
    #[inline]
    fn is_closed(&self) -> bool {
        false
    }

    #[inline]
    fn try_send(&self, message: InboundMessage) -> Result<(), InboundTrySendError> {
        (self.handler)(message);
        Ok(())
    }

    fn send(&self, message: InboundMessage) -> impl Future<Output = Result<(), InboundSinkClosed>> + Send {
        let handler = Arc::clone(&self.handler);
        async move {
            handler(message);
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use bytestring::ByteString;

    use super::*;
    use crate::codec::QoS;

    fn sample(payload: &'static [u8]) -> InboundMessage {
        InboundMessage {
            topic: ByteString::from("t"),
            payload: Bytes::from_static(payload),
            properties: Default::default(),
            qos: QoS::AtMostOnce,
            retain: false,
            dup: false,
            subscription_ids: Vec::new(),
        }
    }

    #[test]
    fn flume_try_send_full_and_closed() {
        let (tx, rx) = flume::bounded::<InboundMessage>(1);
        tx.try_send(sample(b"a")).expect("fill");
        match InboundSink::try_send(&tx, sample(b"b")) {
            Err(InboundTrySendError::Full(_)) => {}
            other => panic!("expected Full, got {other:?}"),
        }
        drop(rx);
        match InboundSink::try_send(&tx, sample(b"c")) {
            Err(InboundTrySendError::Closed(_)) => {}
            other => panic!("expected Closed, got {other:?}"),
        }
        assert!(InboundSink::is_closed(&tx));
    }

    #[tokio::test]
    async fn flume_send_awaits_capacity() {
        let (tx, rx) = flume::bounded::<InboundMessage>(1);
        InboundSink::try_send(&tx, sample(b"fill")).expect("fill");
        let push = tokio::spawn({
            let tx = tx.clone();
            async move { InboundSink::send(&tx, sample(b"next")).await }
        });
        tokio::task::yield_now().await;
        assert_eq!(&*rx.recv_async().await.expect("drain").payload, b"fill");
        push.await.expect("join").expect("send");
        assert_eq!(&*rx.recv_async().await.expect("next").payload, b"next");
    }

    #[test]
    fn mpsc_try_send_full_and_closed() {
        let (tx, rx) = tokio::sync::mpsc::channel::<InboundMessage>(1);
        tx.try_send(sample(b"a")).expect("fill");
        match InboundSink::try_send(&tx, sample(b"b")) {
            Err(InboundTrySendError::Full(_)) => {}
            other => panic!("expected Full, got {other:?}"),
        }
        drop(rx);
        match InboundSink::try_send(&tx, sample(b"c")) {
            Err(InboundTrySendError::Closed(_)) => {}
            other => panic!("expected Closed, got {other:?}"),
        }
        assert!(InboundSink::is_closed(&tx));
    }

    #[tokio::test]
    async fn mpsc_send_awaits_capacity() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<InboundMessage>(1);
        InboundSink::try_send(&tx, sample(b"fill")).expect("fill");
        let push = tokio::spawn({
            let tx = tx.clone();
            async move { InboundSink::send(&tx, sample(b"next")).await }
        });
        tokio::task::yield_now().await;
        assert_eq!(&*rx.recv().await.expect("drain").payload, b"fill");
        push.await.expect("join").expect("send");
        assert_eq!(&*rx.recv().await.expect("next").payload, b"next");
    }

    #[test]
    fn on_inbound_try_send_invokes_handler() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let seen = Arc::new(AtomicUsize::new(0));
        let sink = OnInboundSink::new({
            let seen = Arc::clone(&seen);
            move |msg: InboundMessage| {
                assert_eq!(&*msg.payload, b"inline");
                seen.fetch_add(1, Ordering::SeqCst);
            }
        });
        InboundSink::try_send(&sink, sample(b"inline")).expect("try_send");
        assert_eq!(seen.load(Ordering::SeqCst), 1);
        assert!(!InboundSink::is_closed(&sink));
    }

    #[tokio::test]
    async fn on_inbound_send_invokes_handler() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let seen = Arc::new(AtomicUsize::new(0));
        let sink = OnInboundSink::new({
            let seen = Arc::clone(&seen);
            move |_msg: InboundMessage| {
                seen.fetch_add(1, Ordering::SeqCst);
            }
        });
        InboundSink::send(&sink, sample(b"x")).await.expect("send");
        assert_eq!(seen.load(Ordering::SeqCst), 1);
    }
}
