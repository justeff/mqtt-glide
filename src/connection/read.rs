//! Read task: decode frames; control inline; PUBLISH → injectable handoff.

use std::num::NonZeroU16;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use futures_util::{FutureExt, StreamExt};
use tokio::io::AsyncRead;
use tokio::sync::mpsc;
use tokio_util::codec::FramedRead;

use crate::codec::v5::ToReasonCode;
use crate::codec::{DecodeError, DecodedFrame, DisconnectReasonCode, InboundMessage, Packet, PublishPacket, V5Codec};
use crate::connection::{ControlMessage, Generation, StopSignal, effective_io_batch, inbound_publish_policy};
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
    /// Client Receive Maximum from CONNECT (adaptive buffered-frame burst).
    pub receive_maximum: NonZeroU16,
    /// Set while the read task is blocked on a full handoff — keepalive watchdog must not fire.
    pub handoff_stalled: Arc<AtomicBool>,
    /// Set on any successfully decoded inbound frame (clears the ping watchdog).
    pub inbound_activity: Arc<AtomicBool>,
    /// Inbound Topic Alias bindings of this connection (continued from the resubscribe wait).
    pub topic_aliases: crate::connection::topic_alias::InboundTopicAliases,
    /// Request/response routing (responses bypass the inbound sink).
    pub response_router: Arc<crate::request::ResponseRouter>,
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
/// On graceful shutdown, [`ReadSession::shutdown_notify`] cuts off further `framed.next()`;
/// packets not yet accepted into the handoff are discarded and never PUBACKed.
///
/// `read_done` is dropped when this task returns so the write task can finish PUBACK drain
/// before sending DISCONNECT.
pub(crate) async fn run<H: InboundSink, R>(
    session: ReadSession,
    #[cfg(test)] mut stub_exit_rx: mpsc::Receiver<()>,
    mut framed: FramedRead<R, V5Codec>,
    handoff: H,
    control_tx: mpsc::Sender<ControlMessage>,
    read_done: ReadDoneSignal,
) -> ReadOutcome
where
    R: AsyncRead + Unpin + Send + 'static,
{
    // Hold until every exit path so write observes a completed read-join.
    let _read_done = read_done;
    let cancel = session.cancel;
    let shutdown_notify = session.stop;
    let generation = session.generation;
    let read_burst = effective_io_batch(session.receive_maximum, false);
    let handoff_stalled = session.handoff_stalled;
    let inbound_activity = session.inbound_activity;
    let mut topic_aliases = session.topic_aliases;
    let response_router = session.response_router;
    // Created once, not per iteration (see the write task): fresh `cancelled()` futures
    // register with the token under its mutex.
    let cancel_wait = cancel.cancelled();
    let stop_wait = shutdown_notify.cancelled();
    tokio::pin!(cancel_wait, stop_wait);

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
            frame = framed.next() => {
                let mut pending = frame;
                for burst_idx in 0..read_burst {
                    match pending {
                        None => return ReadOutcome::PeerClosed,
                        Some(Err(error)) => return decode_error_disconnect(&control_tx, error).await,
                        Some(Ok((DecodedFrame::Publish(mut packet), _wire_len))) => {
                            inbound_activity.store(true, Ordering::Release);
                            if let Err(error) = topic_aliases.resolve(&mut packet) {
                                // §3.3.2.3.4: an unknown or out-of-range alias is answered with 0x94.
                                return protocol_error_disconnect(&control_tx, error, DisconnectReasonCode::TopicAliasInvalid)
                                    .await;
                            }
                            if let Some(outcome) = handle_inbound_publish(
                                packet,
                                &response_router,
                                &handoff,
                                &control_tx,
                                &cancel,
                                &shutdown_notify,
                                &handoff_stalled,
                                &inbound_activity,
                            )
                            .await
                            {
                                return outcome;
                            }
                        }
                        Some(Ok((DecodedFrame::Control(packet), _wire_len))) => {
                            inbound_activity.store(true, Ordering::Release);
                            if let Some(outcome) = dispatch_control(packet, generation, &control_tx).await {
                                return outcome;
                            }
                        }
                    }

                    if cancel.is_cancelled() || shutdown_notify.is_cancelled() {
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
            }
        }
    }
}

/// `packet` must already have its Topic Alias resolved.
#[allow(clippy::too_many_arguments)]
async fn handle_inbound_publish<H: InboundSink>(
    packet: PublishPacket,
    response_router: &crate::request::ResponseRouter,
    handoff: &H,
    control_tx: &mpsc::Sender<ControlMessage>,
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
        }
    }
    // PUBACK only after successful handoff accept (enqueue-before-ack).
    if let Some(packet_id) = puback_id
        && control_tx
            .send(ControlMessage::AckInboundPublish { packet_id })
            .await
            .is_err()
    {
        return Some(ReadOutcome::PeerClosed);
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
async fn accept_publish_preferring_completed_push<H: InboundSink>(
    handoff: &H,
    message: InboundMessage,
    cancel: &StopSignal,
    shutdown_notify: &StopSignal,
    handoff_stalled: &AtomicBool,
    inbound_activity: &AtomicBool,
) -> PublishAccept {
    match handoff.try_send(message) {
        Ok(()) => PublishAccept::Accepted,
        Err(InboundTrySendError::Closed(_)) => PublishAccept::Closed,
        Err(InboundTrySendError::Full(message)) => {
            handoff_stalled.store(true, Ordering::Release);
            let _stall_guard = HandoffStallGuard { flag: handoff_stalled };
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

async fn dispatch_control(
    packet: Packet,
    generation: Generation,
    control_tx: &mpsc::Sender<ControlMessage>,
) -> Option<ReadOutcome> {
    match packet {
        Packet::PingResponse => None,
        Packet::Disconnect(disconnect) => Some(ReadOutcome::BrokerDisconnect(Box::new(disconnect))),
        Packet::SubscribeAck(ack) => {
            if control_tx
                .send(ControlMessage::SubscribeAck { ack, generation })
                .await
                .is_err()
            {
                return Some(ReadOutcome::PeerClosed);
            }
            None
        }
        Packet::UnsubscribeAck(ack) => {
            if control_tx
                .send(ControlMessage::UnsubscribeAck { ack, generation })
                .await
                .is_err()
            {
                return Some(ReadOutcome::PeerClosed);
            }
            None
        }
        Packet::PublishAck(ack) => {
            if control_tx
                .send(ControlMessage::PublishAck { ack, generation })
                .await
                .is_err()
            {
                return Some(ReadOutcome::PeerClosed);
            }
            None
        }
        // QoS 2 handshake packets are out of scope — treat as protocol violation.
        Packet::PublishReceived(_) | Packet::PublishRelease(_) | Packet::PublishComplete(_) => {
            Some(ReadOutcome::Protocol(ProtocolError::Violation(
                "QoS 2 control packets are not supported".into(),
            )))
        }
        // Extended AUTH (re-auth) is out of scope — ignore until authenticated sessions land.
        Packet::Auth(_) => None,
        other => Some(ReadOutcome::Protocol(ProtocolError::Violation(format!(
            "unexpected control packet on connected session: {:?}",
            other.packet_type()
        )))),
    }
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

    use super::{PublishAccept, StopSignal, accept_publish_preferring_completed_push};
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
}
