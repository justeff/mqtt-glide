//! Read task: decode frames; control inline; PUBLISH → injectable handoff.

use std::num::NonZeroU16;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use futures_util::{FutureExt, SinkExt, Stream, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio_util::codec::FramedRead;

use crate::codec::v5::ToReasonCode;
use crate::codec::{DecodeError, DecodedFrame, DisconnectReasonCode, InboundMessage, Packet, PublishPacket, V5Codec};
use crate::connection::reauth::{self, Reauth};
use crate::connection::write::{SharedWriter, feed_inbound_publish_ack};
use crate::connection::{ControlMessage, Generation, StopSignal, inbound_publish_policy, io_batch};
use crate::error::{ProtocolError, TransportError};
use crate::inbound_sink::{InboundSink, InboundTrySendError};

/// Outcome of the read task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReadOutcome {
    /// Cancelled by the coordinator.
    Cancelled,
    /// Test/skeleton forced exit (supervision hook).
    #[cfg(test)]
    StubExit,
    /// Handoff channel closed.
    InboundSinkClosed,
    /// Peer closed the TCP stream.
    PeerClosed,
    /// Socket / decode I/O failure.
    Transport(TransportError),
    /// Broker sent DISCONNECT with a reason code (classified in the coordinator).
    BrokerDisconnect(Box<crate::codec::Disconnect>),
    /// Protocol violation or decode failure.
    Protocol(ProtocolError),
    /// A re-authentication failed on the client's side ([`crate::Authenticator`] error).
    Auth(crate::error::AuthError),
}

/// Session tokens and generation for the read task.
#[derive(Debug, Clone)]
pub(crate) struct ReadSession {
    /// Session cancel (peer close / supervisor).
    pub cancel: StopSignal,
    /// Graceful stop: cancelled by the write task once a shutdown drain is done; cuts off
    /// further `framed.next()`.
    pub stop: StopSignal,
    /// Connection generation for stale PUBACK discard.
    pub generation: Generation,
    /// Set while the read task is blocked on a full handoff — keepalive watchdog must not fire.
    pub handoff_stalled: Arc<AtomicBool>,
    /// Set on any inbound bytes and on every decoded frame (clears the ping watchdog).
    pub inbound_activity: Arc<AtomicBool>,
    /// Inbound Topic Alias bindings of this connection (continued from the resubscribe wait).
    pub topic_aliases: crate::connection::topic_alias::InboundTopicAliases,
    /// Request/response routing (responses bypass the inbound sink).
    pub response_router: Arc<crate::request::ResponseRouter>,
    /// Counters behind [`crate::MqttClient::stats`].
    pub stats: crate::stats::Stats,
    /// Re-authentication requests and the authenticator, when one is configured.
    pub reauth: Option<super::reauth::ReauthLink>,
    /// Bench-only count of PUBACKs written (see `ConnectOptions`); shared with the write task.
    pub inbound_publish_ack_counter: Option<Arc<AtomicUsize>>,
    /// The Receive Maximum the client announced in CONNECT.
    pub receive_maximum: NonZeroU16,
    /// The client's graceful shutdown (`MqttClient::shutdown`): its drain holds the writer.
    pub shutdown_notify: tokio_util::sync::CancellationToken,
}

/// Drop-guard: signals the write task that the read task has fully exited.
///
/// Used so graceful disconnect can drain inbound PUBACKs deterministically after read
/// cut-off — no idle timer. Fired from [`Drop`] so every exit path is covered.
#[derive(Debug)]
pub(crate) struct ReadDoneSignal {
    tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl ReadDoneSignal {
    /// Creates a signal that completes `rx` when this guard is dropped.
    pub(crate) fn new(tx: tokio::sync::oneshot::Sender<()>) -> Self {
        Self { tx: Some(tx) }
    }
}

impl Drop for ReadDoneSignal {
    fn drop(&mut self) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(());
        }
    }
}

/// Runs the read task until cancelled, peer close, or signalled to exit.
///
/// Control frames are dispatched inline; only PUBLISH waits on the handoff. Each publish
/// awaits handoff accept before enqueueing PUBACK, while the write task continues independently.
///
/// When the framed read buffer already holds further complete packets, they are handled in a
/// short burst (`now_or_never`) before awaiting the next socket read — fewer task wakeups under
/// pipelined load.
///
/// On graceful shutdown, [`ReadSession::stop`] cuts off further `framed.next()`;
/// packets not yet accepted into the handoff are discarded and never PUBACKed.
///
/// `read_done` is dropped when this task returns so the write task can finish PUBACK drain
/// before sending DISCONNECT.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run<H: InboundSink, R, W>(
    session: ReadSession,
    #[cfg(test)] stub_exit_rx: mpsc::Receiver<()>,
    framed: FramedRead<R, V5Codec>,
    handoff: H,
    control_tx: mpsc::Sender<ControlMessage>,
    writer: SharedWriter<W>,
    read_done: ReadDoneSignal,
) -> ReadOutcome
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    // Hold until every exit path so write observes a completed read-join.
    let _read_done = read_done;
    let mut acks = AcksOut {
        ids: Vec::new(),
        writer,
        counter: session.inbound_publish_ack_counter.clone(),
        queue_limit: super::control_channel_capacity(session.receive_maximum),
        cancel: session.cancel.clone(),
        shutdown_notify: session.shutdown_notify.clone(),
    };
    let mut reauth = session.reauth.as_ref().and_then(Reauth::new);
    let outcome = read_frames(
        session,
        #[cfg(test)]
        stub_exit_rx,
        framed,
        handoff,
        &control_tx,
        &mut acks,
        &mut reauth,
    )
    .await;
    // Before `read_done`: a graceful drain must see every PUBACK of an accepted message.
    acks.send_now(&control_tx);
    if let Some(reauth) = reauth.as_mut() {
        reauth.conclude(&outcome);
    }
    outcome
}

/// The read loop of [`run`]; returns with PUBACKs possibly still held in `acks`.
async fn read_frames<H: InboundSink, R, W>(
    session: ReadSession,
    #[cfg(test)] mut stub_exit_rx: mpsc::Receiver<()>,
    mut framed: FramedRead<R, V5Codec>,
    handoff: H,
    control_tx: &mpsc::Sender<ControlMessage>,
    acks: &mut AcksOut<W>,
    reauth: &mut Option<Reauth>,
) -> ReadOutcome
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let cancel = session.cancel;
    let stop = session.stop;
    let generation = session.generation;
    let read_burst = io_batch(false);
    let handoff_stalled = session.handoff_stalled;
    let inbound_activity = session.inbound_activity;
    let mut topic_aliases = session.topic_aliases;
    let response_router = session.response_router;
    let stats = session.stats;
    // Created once, not per iteration (see the write task): fresh `cancelled()` futures
    // register with the token under its mutex.
    let cancel_wait = cancel.cancelled();
    let stop_wait = stop.cancelled();
    tokio::pin!(cancel_wait, stop_wait);
    // A frame read ahead of a re-authentication timeout, handled before anything else.
    let mut read_ahead = None;

    loop {
        // Do not put `#[cfg(test)]` on a `tokio::select!` arm — the macro rejects it.
        #[cfg(test)]
        let stub_exit = stub_exit_rx.recv();
        #[cfg(not(test))]
        let stub_exit = std::future::pending::<Option<()>>();

        tokio::select! {
            // Session cancel or graceful-shutdown notify — stop consuming the read half.
            () = &mut cancel_wait => return ReadOutcome::Cancelled,
            () = &mut stop_wait => return ReadOutcome::Cancelled,
            event = next_reauth_event(reauth), if reauth.is_some() && read_ahead.is_none() => {
                if let Some(state) = reauth.as_mut() {
                    // The broker's answer may have arrived while the timer ran out: read what
                    // is there first, so the answer is not lost to the timeout.
                    if matches!(event, reauth::Event::TimedOut)
                        && state.defer_timeout()
                        && let Some(frame) = framed.next().now_or_never()
                    {
                        read_ahead = Some(frame);
                        continue;
                    }
                    if let Err(failure) = state.handle(event, control_tx).await {
                        return reauth_failure(control_tx, failure).await;
                    }
                }
            }
            msg = stub_exit => {
                #[cfg(test)]
                {
                    if msg.is_some() {
                        return ReadOutcome::StubExit;
                    }
                    return ReadOutcome::PeerClosed;
                }
                #[cfg(not(test))]
                {
                    let _ = msg;
                    unreachable!("stub_exit is pending in release builds");
                }
            }
            frame = next_frame(&mut read_ahead, &mut framed) => {
                let mut pending = frame;
                for burst_idx in 0..read_burst {
                    match pending {
                        None => return ReadOutcome::PeerClosed,
                        Some(Err(error)) => return decode_error_disconnect(control_tx, error).await,
                        Some(Ok((DecodedFrame::Publish(mut packet), _wire_len))) => {
                            inbound_activity.store(true, Ordering::Release);
                            if let Err(error) = topic_aliases.resolve(&mut packet) {
                                let reason = super::topic_alias::disconnect_reason(&error);
                                return protocol_error_disconnect(control_tx, error, reason).await;
                            }
                            stats.publish_received(packet.meta.qos, packet.meta.dup);
                            let reauth_waiting_since = reauth
                                .as_ref()
                                .is_some_and(Reauth::busy)
                                .then(tokio::time::Instant::now);
                            if let Some(outcome) = handle_inbound_publish(
                                packet,
                                &stats,
                                &response_router,
                                &handoff,
                                control_tx,
                                acks,
                                &cancel,
                                &stop,
                                &handoff_stalled,
                                &inbound_activity,
                            )
                            .await
                            {
                                return outcome;
                            }
                            // Time spent on a full inbound sink is not the broker's delay.
                            if let (Some(since), Some(state)) = (reauth_waiting_since, reauth.as_mut()) {
                                state.extend_deadline(since.elapsed());
                            }
                        }
                        Some(Ok((DecodedFrame::Control(Packet::Auth(auth)), _wire_len))) if reauth.is_some() => {
                            inbound_activity.store(true, Ordering::Release);
                            if let Some(state) = reauth.as_mut()
                                && let Err(failure) = state.on_broker_auth(auth)
                            {
                                return reauth_failure(control_tx, failure).await;
                            }
                        }
                        Some(Ok((DecodedFrame::Control(packet), _wire_len))) => {
                            inbound_activity.store(true, Ordering::Release);
                            if let Some(outcome) = dispatch_control(packet, generation, control_tx).await {
                                return outcome;
                            }
                        }
                    }

                    if cancel.is_cancelled() || stop.is_cancelled() {
                        return ReadOutcome::Cancelled;
                    }
                    if burst_idx + 1 == read_burst {
                        break;
                    }
                    // Drain already-buffered frames without awaiting another socket read.
                    match framed.next().now_or_never() {
                        Some(next) => pending = next,
                        None => break,
                    }
                }
                // Only now: nothing more is buffered, so a broker that waits for these PUBACKs
                // before it sends more (its window is full) gets them without delay.
                if acks.send(control_tx).await.is_err() {
                    return ReadOutcome::PeerClosed;
                }
            }
        }
    }
}

/// PUBACKs of accepted messages, written at the end of each read burst.
///
/// Never held past the burst (until more data arrives, or for a timer): a broker that keeps
/// fewer deliveries in flight than the client's Receive Maximum allows (EMQX's `max_inflight`
/// defaults to 32) would stop and wait for them, and both sides would wait.
///
/// Handing each one to the write task woke it once per message; with the consumer woken for
/// the same message, two runnable tasks made Tokio wake a second worker thread. When messages
/// arrive one at a time (a broker writing each delivery on its own) that was the largest cost
/// of inbound QoS 1. When the shared writer is free, the read task now writes the burst's
/// PUBACKs itself; when the write task holds it, they wait in the writer's
/// [`PendingAcks`](crate::connection::write::PendingAcks) queue, and the write task gets one
/// [`ControlMessage::AcksPending`] when the queue was empty (it is awake then, so the hand-over
/// costs no extra wake-up). Whoever holds the writer feeds the queue before newer PUBACKs, so
/// they leave in order ([MQTT-4.6.0-2]).
pub(crate) struct AcksOut<W> {
    ids: Vec<NonZeroU16>,
    writer: SharedWriter<W>,
    counter: Option<Arc<AtomicUsize>>,
    /// PUBACKs past which this task waits for the writer instead of queueing more: the
    /// client's Receive Maximum, at least the 128 the control channel held before.
    queue_limit: usize,
    cancel: StopSignal,
    shutdown_notify: tokio_util::sync::CancellationToken,
}

impl<W: AsyncWrite + Unpin + Send + 'static> AcksOut<W> {
    fn hold(&mut self, packet_id: NonZeroU16) {
        self.ids.push(packet_id);
    }

    /// Whether more PUBACKs wait than [`Self::queue_limit`], which only a broker sending more
    /// than the client's Receive Maximum ([MQTT-3.3.4-9]) brings about. PUBACKs handed to the
    /// writer count as sent: the broker can only know of fewer.
    fn over_limit(&self) -> bool {
        self.ids.len() + self.writer.pending_acks().len() > self.queue_limit
    }

    /// Writes the held PUBACKs, or queues them for the write task if it holds the writer.
    async fn send(&mut self, control_tx: &mpsc::Sender<ControlMessage>) -> Result<(), ()> {
        let written = match self.write_now() {
            // The client does not end the session over a broker past its Receive Maximum, but
            // the queue must not grow with it: wait for the writer, as the bounded control
            // channel once made this task wait.
            Written::WriterBusy if self.over_limit() => self.write_waiting().await,
            written => written,
        };
        match written {
            Written::All => Ok(()),
            Written::Buffered => control_tx.send(ControlMessage::FlushWrites).await.map_err(|_| ()),
            Written::WriterBusy => {
                if self.writer.pending_acks().push(&mut self.ids) {
                    control_tx.send(ControlMessage::AcksPending).await.map_err(|_| ())?;
                }
                Ok(())
            }
            Written::Refused => {
                self.writer.pending_acks().push(&mut self.ids);
                control_tx.send(ControlMessage::AcksPending).await.map_err(|_| ())
            }
        }
    }

    /// On exit: like [`Self::send`] without waiting. The write task's drain feeds the queue
    /// once this task has ended, even without [`ControlMessage::AcksPending`].
    fn send_now(&mut self, control_tx: &mpsc::Sender<ControlMessage>) {
        match self.write_now() {
            Written::All => {}
            Written::Buffered => {
                let _ = control_tx.try_send(ControlMessage::FlushWrites);
            }
            Written::WriterBusy | Written::Refused => {
                self.writer.pending_acks().push(&mut self.ids);
                let _ = control_tx.try_send(ControlMessage::AcksPending);
            }
        }
    }

    /// Encodes the queued and then the held PUBACKs into the shared writer and flushes once,
    /// without waiting.
    fn write_now(&mut self) -> Written {
        if self.ids.is_empty() {
            return Written::All;
        }
        let Ok(mut writer) = self.writer.try_lock() else {
            return Written::WriterBusy;
        };
        write_held(
            &mut self.ids,
            &mut writer,
            self.writer.pending_acks(),
            self.counter.as_ref(),
        )
    }

    /// Like [`Self::write_now`], waiting for the write task to let go of the writer. Not
    /// during a shutdown: the graceful drain holds the writer until this task has handed on
    /// the broker's PUBACKs, so the held ones are queued for the drain instead.
    async fn write_waiting(&mut self) -> Written {
        let mut writer = tokio::select! {
            biased;
            () = self.shutdown_notify.cancelled() => return Written::WriterBusy,
            () = self.cancel.cancelled() => return Written::WriterBusy,
            writer = self.writer.lock() => writer,
        };
        write_held(
            &mut self.ids,
            &mut writer,
            self.writer.pending_acks(),
            self.counter.as_ref(),
        )
    }
}

/// Encodes the queued and then the held PUBACKs into the writer and flushes once, without
/// waiting.
fn write_held<W: AsyncWrite + Unpin + Send + 'static>(
    ids: &mut Vec<NonZeroU16>,
    writer: &mut tokio_util::codec::FramedWrite<W, V5Codec>,
    pending: &crate::connection::write::PendingAcks,
    counter: Option<&Arc<AtomicUsize>>,
) -> Written {
    if pending.is_closed() {
        // The write task sent its last DISCONNECT: nothing may follow it, and the broker
        // redelivers what goes unacknowledged.
        ids.clear();
        return Written::All;
    }
    // Older PUBACKs queued while the write task held the writer go first.
    if pending.feed(writer, counter).is_err() {
        return Written::Refused;
    }
    let mut fed = 0;
    for &packet_id in ids.iter() {
        if feed_inbound_publish_ack(writer, packet_id, counter).is_err() {
            break;
        }
        fed += 1;
    }
    ids.drain(..fed);
    if !ids.is_empty() {
        // The encoder refused a PUBACK (a broker Maximum Packet Size below 4 bytes). The write
        // task, which owns the session's outcome, meets the same error and ends the connection.
        return Written::Refused;
    }
    match writer.flush().now_or_never() {
        Some(Ok(())) => Written::All,
        // A failed flush leaves the unsent bytes in the buffer: the write task's flush reports
        // the error and ends the connection.
        Some(Err(_)) | None => Written::Buffered,
    }
}

/// The broker's half of the connection, marking `activity` whenever bytes arrive.
///
/// The keep-alive watchdog takes any inbound traffic as proof the broker is alive. Marking
/// only decoded packets missed a PUBLISH larger than the link carries in a Keep Alive: the
/// PINGRESP queued behind it came too late, and the client closed a connection that was busy
/// delivering, again on every redelivery.
pub(crate) struct MarkActivity<R> {
    inner: R,
    activity: Arc<AtomicBool>,
}

impl<R> MarkActivity<R> {
    pub(crate) fn new(inner: R, activity: Arc<AtomicBool>) -> Self {
        Self { inner, activity }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for MarkActivity<R> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let filled = buf.filled().len();
        let polled = std::pin::Pin::new(&mut self.inner).poll_read(cx, buf);
        // Load first: the write task clears the flag once per Keep Alive tick, so a store per
        // read would only keep moving the cache line between the two tasks.
        if buf.filled().len() > filled && !self.activity.load(Ordering::Relaxed) {
            // Release pairs with the write task's AcqRel swap.
            self.activity.store(true, Ordering::Release);
        }
        polled
    }
}

/// Writes pass through: the WebSocket transports mark activity on the stream below the
/// WebSocket layer, which reads and writes the same stream.
impl<W: AsyncWrite + Unpin> AsyncWrite for MarkActivity<W> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// What [`AcksOut::write_now`] managed.
enum Written {
    /// Written (or nothing was held).
    All,
    /// Encoded, but the socket is full or the flush failed: the write task has to flush.
    Buffered,
    /// The write task holds the writer: the held ones are queued for it.
    WriterBusy,
    /// The encoder refused a PUBACK: the rest are queued, and the write task, which meets the
    /// same error, is told even when the queue was not empty.
    Refused,
}

/// `packet` must already have its Topic Alias resolved.
#[allow(clippy::too_many_arguments)]
async fn handle_inbound_publish<H: InboundSink, W: AsyncWrite + Unpin + Send + 'static>(
    packet: PublishPacket,
    stats: &crate::stats::Stats,
    response_router: &crate::request::ResponseRouter,
    handoff: &H,
    control_tx: &mpsc::Sender<ControlMessage>,
    acks: &mut AcksOut<W>,
    cancel: &StopSignal,
    shutdown_notify: &StopSignal,
    handoff_stalled: &AtomicBool,
    inbound_activity: &AtomicBool,
) -> Option<ReadOutcome> {
    let puback_id = match inbound_publish_policy(&packet) {
        Ok(id) => id,
        Err(error) => {
            return Some(protocol_error_disconnect(control_tx, error, DisconnectReasonCode::ProtocolError).await);
        }
    };
    // A response to a pending request goes to that request; everything else awaits handoff
    // accept before PUBACK, while the write task stays independent.
    if let Some(message) = response_router.route(InboundMessage::from(packet)) {
        match accept_publish_preferring_completed_push(
            handoff,
            message,
            control_tx,
            acks,
            stats,
            cancel,
            shutdown_notify,
            handoff_stalled,
            inbound_activity,
        )
        .await
        {
            PublishAccept::Accepted => {}
            PublishAccept::CutOff => return Some(ReadOutcome::Cancelled),
            PublishAccept::Closed => return Some(ReadOutcome::InboundSinkClosed),
            PublishAccept::WriteTaskGone => return Some(ReadOutcome::PeerClosed),
        }
    }
    // PUBACK only after successful handoff accept (enqueue-before-ack); written with the burst.
    if let Some(packet_id) = puback_id {
        acks.hold(packet_id);
    }
    None
}

/// Result of pushing one publish into the injectable handoff under shutdown cut-off.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublishAccept {
    /// Message is in the handoff — caller **MUST** still PUBACK for QoS 1 (§7.2 step 3).
    Accepted,
    /// Cut off before accept completed (message never entered the handoff).
    CutOff,
    /// Handoff channel closed.
    Closed,
    /// The write task has ended, so the held PUBACKs could not go to it: the connection is
    /// going down, which is no reason to stop the driver the way a closed sink is.
    WriteTaskGone,
}

/// Clears [`AtomicBool`] stall flag on drop (including cancel/cutoff paths).
struct HandoffStallGuard<'a> {
    flag: &'a AtomicBool,
}

impl Drop for HandoffStallGuard<'_> {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::Release);
    }
}

/// Awaits handoff push, preferring a *completed* accept over cut-off when both are ready.
///
/// `biased` + [`InboundSink::send`] first: if the sink already accepted and cancel/shutdown is
/// also ready in the same poll, we still report [`PublishAccept::Accepted`] so the caller can
/// enqueue PUBACK. Cut-off still wins while `send` is pending on a full handoff.
///
/// When the handoff is full, marks `handoff_stalled` so the write-task keepalive watchdog
/// does not treat unread broker replies as a dead peer.
/// Uses `Ordering::Release` so the write task's `Acquire` load observes the stall promptly
/// (do not weaken to `Relaxed` — delayed visibility can false-trigger PINGRESP timeout).
///
/// After a stalled push finally accepts, sets `inbound_activity` once as catch-up grace:
/// broker replies may already sit in the TCP/codec buffer while `handoff_stalled` drops —
/// without grace the next ping tick could spuriously time out before read catches up.
#[allow(clippy::too_many_arguments)]
async fn accept_publish_preferring_completed_push<H: InboundSink, W: AsyncWrite + Unpin + Send + 'static>(
    handoff: &H,
    message: InboundMessage,
    control_tx: &mpsc::Sender<ControlMessage>,
    acks: &mut AcksOut<W>,
    stats: &crate::stats::Stats,
    cancel: &StopSignal,
    shutdown_notify: &StopSignal,
    handoff_stalled: &AtomicBool,
    inbound_activity: &AtomicBool,
) -> PublishAccept {
    match handoff.try_send(message) {
        Ok(()) => PublishAccept::Accepted,
        Err(InboundTrySendError::Closed(_)) => PublishAccept::Closed,
        Err(InboundTrySendError::Full(message)) => {
            // The wait can be long: let the broker have the PUBACKs it is owed meanwhile.
            if acks.send(control_tx).await.is_err() {
                return PublishAccept::WriteTaskGone;
            }
            handoff_stalled.store(true, Ordering::Release);
            let _stall_guard = HandoffStallGuard { flag: handoff_stalled };
            let _stall_timer = stats.handoff_stall();
            let outcome = tokio::select! {
                biased;
                result = handoff.send(message) => match result {
                    Ok(()) => PublishAccept::Accepted,
                    Err(_) => PublishAccept::Closed,
                },
                _ = cancel.cancelled() => PublishAccept::CutOff,
                _ = shutdown_notify.cancelled() => PublishAccept::CutOff,
            };
            if matches!(outcome, PublishAccept::Accepted) {
                inbound_activity.store(true, Ordering::Release);
            }
            outcome
        }
    }
}

/// Hands an acknowledgement to the write task, waiting only while the channel is full.
///
/// `Sender::send` takes a unit of Tokio's cooperative budget even with room in the channel, and
/// tasks woken through the LIFO slot (write task, then the publisher awaiting the PUBACK) share
/// this task's budget. A burst of PUBACKs left the publisher too little to finish its window;
/// resuming it woke a second worker, which often moved the connection to another core.
async fn send_ack(control_tx: &mpsc::Sender<ControlMessage>, message: ControlMessage) -> Option<ReadOutcome> {
    let sent = match control_tx.try_send(message) {
        Ok(()) => true,
        Err(mpsc::error::TrySendError::Full(message)) => control_tx.send(message).await.is_ok(),
        Err(mpsc::error::TrySendError::Closed(_)) => false,
    };
    (!sent).then_some(ReadOutcome::PeerClosed)
}

async fn dispatch_control(
    packet: Packet,
    generation: Generation,
    control_tx: &mpsc::Sender<ControlMessage>,
) -> Option<ReadOutcome> {
    match packet {
        Packet::PingResponse => None,
        Packet::Disconnect(disconnect) => {
            // [MQTT-3.14.2-2] forbids it from a server; the connection ends anyway, so the
            // value is only ignored and named for whoever debugs the broker.
            if let Some(seconds) = disconnect.session_expiry_interval {
                tracing::debug!(seconds, "ignoring Session Expiry Interval in broker DISCONNECT");
            }
            Some(ReadOutcome::BrokerDisconnect(Box::new(disconnect)))
        }
        Packet::SubscribeAck(ack) => send_ack(control_tx, ControlMessage::SubscribeAck { ack, generation }).await,
        Packet::UnsubscribeAck(ack) => send_ack(control_tx, ControlMessage::UnsubscribeAck { ack, generation }).await,
        Packet::PublishAck(ack) => send_ack(control_tx, ControlMessage::PublishAck { ack, generation }).await,
        // QoS 2 handshake packets are out of scope: this client never sends or grants QoS 2,
        // so they are a protocol violation, answered with DISCONNECT 0x82 like any other.
        Packet::PublishReceived(_) | Packet::PublishRelease(_) | Packet::PublishComplete(_) => Some(
            protocol_error_disconnect(
                control_tx,
                ProtocolError::Violation("QoS 2 control packets are not supported".into()),
                DisconnectReasonCode::ProtocolError,
            )
            .await,
        ),
        // Without an Authentication Method in CONNECT the broker must not send AUTH
        // ([MQTT-4.12.0-6], §3.15): a Protocol Error, as before the CONNACK.
        Packet::Auth(_) => Some(
            protocol_error_disconnect(
                control_tx,
                ProtocolError::Violation("AUTH without an Authentication Method in CONNECT".into()),
                DisconnectReasonCode::ProtocolError,
            )
            .await,
        ),
        other => {
            let error = ProtocolError::Violation(format!(
                "unexpected control packet on connected session: {:?}",
                other.packet_type()
            ));
            Some(protocol_error_disconnect(control_tx, error, DisconnectReasonCode::ProtocolError).await)
        }
    }
}

/// The frame read ahead, or the next one from the socket.
async fn next_frame<S: Stream + Unpin>(read_ahead: &mut Option<Option<S::Item>>, framed: &mut S) -> Option<S::Item> {
    match read_ahead.take() {
        Some(frame) => frame,
        None => framed.next().await,
    }
}

/// The next re-authentication event; never, without an authenticator.
async fn next_reauth_event(reauth: &mut Option<Reauth>) -> reauth::Event {
    match reauth {
        Some(state) => state.next_event().await,
        None => std::future::pending().await,
    }
}

/// A failed re-authentication ends the connection, with a DISCONNECT when the write side is
/// still there ([MQTT-4.12.1-2]).
async fn reauth_failure(control_tx: &mpsc::Sender<ControlMessage>, failure: reauth::Failure) -> ReadOutcome {
    if let Some(reason) = failure.reason {
        let _ = control_tx.send(ControlMessage::ProtocolDisconnect(reason)).await;
    }
    failure.outcome
}

/// Queues a protocol-error DISCONNECT on the control path, then surfaces the typed error.
async fn protocol_error_disconnect(
    control_tx: &mpsc::Sender<ControlMessage>,
    error: ProtocolError,
    reason: DisconnectReasonCode,
) -> ReadOutcome {
    let _ = control_tx.send(ControlMessage::ProtocolDisconnect(reason)).await;
    ReadOutcome::Protocol(error)
}

/// A packet the codec rejected: tell the broker why (Malformed Packet, Packet too large, …)
/// before closing ([MQTT-3.14.1-1], §4.13). A broken socket gets no DISCONNECT.
async fn decode_error_disconnect(control_tx: &mpsc::Sender<ControlMessage>, error: DecodeError) -> ReadOutcome {
    if !matches!(error, DecodeError::Io(_)) {
        let _ = control_tx
            .send(ControlMessage::ProtocolDisconnect(error.to_reason_code()))
            .await;
    }
    map_decode_error(error)
}

fn map_decode_error(error: DecodeError) -> ReadOutcome {
    match error {
        DecodeError::Io(message) => ReadOutcome::Transport(TransportError::Connection(message.to_string())),
        other => ReadOutcome::Protocol(ProtocolError::Decode(other.to_string())),
    }
}

#[cfg(test)]
mod tests {

    use bytes::Bytes;
    use bytestring::ByteString;
    use std::sync::atomic::AtomicBool;

    use tokio::sync::mpsc;

    use super::{AcksOut, PublishAccept, StopSignal, accept_publish_preferring_completed_push};
    use crate::codec::{InboundMessage, QoS};

    fn sample_publish(payload: &'static [u8]) -> InboundMessage {
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

    /// Nothing held, so the writer is never touched.
    fn no_acks() -> AcksOut<tokio::io::Sink> {
        AcksOut {
            ids: Vec::new(),
            writer: crate::connection::write::SharedWriter::new(tokio_util::codec::FramedWrite::new(
                tokio::io::sink(),
                crate::codec::V5Codec::with_max_packet_size(0),
            )),
            counter: None,
            queue_limit: usize::MAX,
            cancel: StopSignal::new(),
            shutdown_notify: tokio_util::sync::CancellationToken::new(),
        }
    }

    /// When shutdown is already cancelled and handoff has capacity, prefer a completed
    /// accept (so the caller can still PUBACK) over CutOff in the same poll.
    #[tokio::test]
    async fn completed_handoff_wins_over_precancelled_shutdown_notify() {
        let (tx, rx) = flume::bounded::<InboundMessage>(1);
        let cancel = StopSignal::new();
        let shutdown_notify = StopSignal::new();
        shutdown_notify.cancel();
        let stalled = AtomicBool::new(false);
        let activity = AtomicBool::new(false);

        let outcome = accept_publish_preferring_completed_push(
            &tx,
            sample_publish(b"in"),
            &mpsc::channel(8).0,
            &mut no_acks(),
            &crate::stats::Stats::default(),
            &cancel,
            &shutdown_notify,
            &stalled,
            &activity,
        )
        .await;
        assert_eq!(outcome, PublishAccept::Accepted);
        assert_eq!(&*rx.try_recv().expect("in flume").payload, b"in");
        assert!(!stalled.load(std::sync::atomic::Ordering::Acquire));
        // Fast-path accept (not stalled) must not invent catch-up activity.
        assert!(!activity.load(std::sync::atomic::Ordering::Acquire));
    }

    /// stalled full handoff still allows cut-off (message never accepted).
    #[tokio::test]
    async fn cutoff_wins_while_handoff_send_is_pending() {
        let (tx, _rx) = flume::bounded::<InboundMessage>(1);
        tx.try_send(sample_publish(b"fill")).expect("fill");
        let cancel = StopSignal::new();
        let shutdown_notify = StopSignal::new();
        let stalled = std::sync::Arc::new(AtomicBool::new(false));
        let activity = std::sync::Arc::new(AtomicBool::new(false));

        let push = tokio::spawn({
            let tx = tx.clone();
            let cancel = cancel.clone();
            let shutdown_notify = shutdown_notify.clone();
            let stalled = std::sync::Arc::clone(&stalled);
            let activity = std::sync::Arc::clone(&activity);
            async move {
                accept_publish_preferring_completed_push(
                    &tx,
                    sample_publish(b"stall"),
                    &mpsc::channel(8).0,
                    &mut no_acks(),
                    &crate::stats::Stats::default(),
                    &cancel,
                    &shutdown_notify,
                    &stalled,
                    &activity,
                )
                .await
            }
        });
        // Allow the send to park on the full handoff before we cut off.
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert!(stalled.load(std::sync::atomic::Ordering::Acquire));
        shutdown_notify.cancel();

        let outcome = tokio::time::timeout(std::time::Duration::from_secs(2), push)
            .await
            .expect("cutoff timeout")
            .expect("join");
        assert_eq!(outcome, PublishAccept::CutOff);
        assert!(!stalled.load(std::sync::atomic::Ordering::Acquire));
        assert!(!activity.load(std::sync::atomic::Ordering::Acquire));
    }

    /// After a stalled accept, catch-up grace marks inbound activity so the ping watchdog
    /// does not race the unread socket buffer.
    #[tokio::test]
    async fn stalled_accept_sets_inbound_activity_catchup_grace() {
        let (tx, rx) = flume::bounded::<InboundMessage>(1);
        tx.try_send(sample_publish(b"fill")).expect("fill");
        let cancel = StopSignal::new();
        let shutdown_notify = StopSignal::new();
        let stalled = std::sync::Arc::new(AtomicBool::new(false));
        let activity = std::sync::Arc::new(AtomicBool::new(false));

        let push = tokio::spawn({
            let tx = tx.clone();
            let cancel = cancel.clone();
            let shutdown_notify = shutdown_notify.clone();
            let stalled = std::sync::Arc::clone(&stalled);
            let activity = std::sync::Arc::clone(&activity);
            async move {
                accept_publish_preferring_completed_push(
                    &tx,
                    sample_publish(b"stall"),
                    &mpsc::channel(8).0,
                    &mut no_acks(),
                    &crate::stats::Stats::default(),
                    &cancel,
                    &shutdown_notify,
                    &stalled,
                    &activity,
                )
                .await
            }
        });
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert!(stalled.load(std::sync::atomic::Ordering::Acquire));

        let _ = rx.try_recv().expect("drain fill");
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(2), push)
            .await
            .expect("accept timeout")
            .expect("join");
        assert_eq!(outcome, PublishAccept::Accepted);
        assert!(!stalled.load(std::sync::atomic::Ordering::Acquire));
        assert!(activity.load(std::sync::atomic::Ordering::Acquire));
    }

    /// A socket that refuses every write.
    struct BrokenSocket;

    impl tokio::io::AsyncWrite for BrokenSocket {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
            _: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            std::task::Poll::Ready(Err(std::io::ErrorKind::BrokenPipe.into()))
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    fn acks_on<W: tokio::io::AsyncWrite>(socket: W, max_packet_size: u32) -> AcksOut<W> {
        let mut codec = crate::codec::V5Codec::with_max_packet_size(0);
        codec.set_max_outbound_size(max_packet_size);
        AcksOut {
            ids: vec![
                std::num::NonZeroU16::new(1).unwrap(),
                std::num::NonZeroU16::new(2).unwrap(),
            ],
            writer: crate::connection::write::SharedWriter::new(tokio_util::codec::FramedWrite::new(socket, codec)),
            counter: None,
            queue_limit: usize::MAX,
            cancel: StopSignal::new(),
            shutdown_notify: tokio_util::sync::CancellationToken::new(),
        }
    }

    /// A write task that ended while the sink is full (the connection broke under a slow
    /// consumer) is a lost connection, not a closed sink: the driver reconnects instead of
    /// stopping with `InboundSinkClosed`.
    #[tokio::test]
    async fn write_task_gone_while_the_sink_is_full_is_no_closed_sink() {
        let (tx, _rx) = flume::bounded::<InboundMessage>(1);
        tx.try_send(sample_publish(b"fill")).expect("fill");
        let mut acks = acks_on(tokio::io::sink(), 0);
        let writer = acks.writer.clone();
        let _held = writer.lock().await; // the PUBACKs must go to the write task
        let (control_tx, control_rx) = mpsc::channel(8);
        drop(control_rx); // the write task has ended

        let outcome = accept_publish_preferring_completed_push(
            &tx,
            sample_publish(b"next"),
            &control_tx,
            &mut acks,
            &crate::stats::Stats::default(),
            &StopSignal::new(),
            &StopSignal::new(),
            &AtomicBool::new(false),
            &AtomicBool::new(false),
        )
        .await;
        assert_eq!(outcome, PublishAccept::WriteTaskGone);
        assert!(!tx.is_disconnected(), "the application's sink is open");
    }

    /// Past the client's Receive Maximum (a broker breaking [MQTT-3.3.4-9]) the read task waits
    /// for the writer instead of queueing more PUBACKs for the write task; within it, it queues.
    #[tokio::test]
    async fn pubacks_past_the_receive_maximum_wait_for_the_writer() {
        let mut acks = acks_on(tokio::io::sink(), 0);
        acks.queue_limit = 3;
        let writer = acks.writer.clone();
        let (control_tx, _control_rx) = mpsc::channel(8);
        let held = writer.lock().await;
        acks.send(&control_tx).await.expect("queued while the writer is taken");
        assert_eq!(writer.pending_acks().len(), 2, "within the window: queued");

        for id in 3..=4 {
            acks.hold(std::num::NonZeroU16::new(id).unwrap());
        }
        let mut send = std::pin::pin!(acks.send(&control_tx));
        assert!(
            futures_util::poll!(send.as_mut()).is_pending(),
            "past the window: waits for the writer"
        );
        drop(held);
        send.await.expect("written");
        assert_eq!(writer.pending_acks().len(), 0);
        assert_eq!(
            writer.lock().await.write_buffer().len(),
            0,
            "all four PUBACKs written and flushed"
        );
    }

    /// A failed flush is not a written PUBACK: the write task flushes again and meets the error.
    #[tokio::test]
    async fn failed_flush_goes_to_the_write_task() {
        let mut acks = acks_on(BrokenSocket, 0);
        let (control_tx, mut control_rx) = mpsc::channel(8);
        acks.send(&control_tx).await.expect("control channel open");
        assert!(matches!(control_rx.try_recv(), Ok(super::ControlMessage::FlushWrites)));
        assert_eq!(
            acks.writer.lock().await.write_buffer().len(),
            8,
            "both PUBACKs still buffered"
        );
    }

    /// Once the write task sent its last DISCONNECT, the read task writes no PUBACK after it
    /// ([MQTT-3.14.4-1]); the broker redelivers what goes unacknowledged.
    #[tokio::test]
    async fn no_puback_after_the_last_disconnect() {
        let mut acks = acks_on(tokio::io::sink(), 0);
        acks.writer.pending_acks().close();
        let (control_tx, mut control_rx) = mpsc::channel(8);
        acks.send(&control_tx).await.expect("control channel open");
        assert!(control_rx.try_recv().is_err(), "nothing for the write task");
        assert!(acks.ids.is_empty());
        assert!(acks.writer.lock().await.write_buffer().is_empty(), "nothing written");
    }

    /// A PUBACK the encoder refuses goes to the write task, which ends the connection with that
    /// error instead of the broker never hearing of the message.
    #[tokio::test]
    async fn refused_puback_goes_to_the_write_task() {
        let mut acks = acks_on(tokio::io::sink(), 3);
        let (control_tx, mut control_rx) = mpsc::channel(8);
        acks.send(&control_tx).await.expect("control channel open");
        assert!(matches!(control_rx.try_recv(), Ok(super::ControlMessage::AcksPending)));
        let queued: Vec<u16> = acks
            .writer
            .pending_acks()
            .snapshot()
            .iter()
            .map(|id| id.get())
            .collect();
        assert_eq!(queued, [1, 2]);
        assert!(acks.ids.is_empty());
    }

    /// A burst of acknowledgements leaves the cooperative budget (128 per poll) to the tasks
    /// that run after the read task; `Sender::send` would stop at the 129th.
    #[tokio::test]
    async fn acknowledgements_do_not_use_the_cooperative_budget() {
        use futures_util::FutureExt;

        let (control_tx, control_rx) = mpsc::channel(256);
        // A spawned task: only those are polled with a budget on every runtime.
        tokio::spawn(async move {
            for sent in 0..200 {
                let outcome = super::send_ack(&control_tx, super::ControlMessage::FlushWrites).now_or_never();
                assert_eq!(outcome, Some(None), "acknowledgement {sent} waited");
            }
        })
        .await
        .expect("burst");
        assert_eq!(control_rx.len(), 200);
    }

    /// A full channel still holds the read task back, and a closed one ends it.
    #[tokio::test]
    async fn acknowledgement_waits_for_room_in_a_full_channel() {
        let (control_tx, mut control_rx) = mpsc::channel(1);
        control_tx.try_send(super::ControlMessage::FlushWrites).expect("fill");

        let waiting = super::send_ack(&control_tx, super::ControlMessage::AcksPending);
        tokio::pin!(waiting);
        assert!(futures_util::poll!(waiting.as_mut()).is_pending());
        assert!(matches!(
            control_rx.recv().await,
            Some(super::ControlMessage::FlushWrites)
        ));
        assert_eq!(waiting.await, None);
        assert!(matches!(
            control_rx.recv().await,
            Some(super::ControlMessage::AcksPending)
        ));

        drop(control_rx);
        assert_eq!(
            super::send_ack(&control_tx, super::ControlMessage::FlushWrites).await,
            Some(super::ReadOutcome::PeerClosed)
        );
    }
}
