//! Write-half I/O helpers: encode/flush, inbound PUBACK feed, control coalesce, graceful disconnect.

use std::num::NonZeroU16;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use futures_util::{FutureExt, Sink, SinkExt};
use tokio::io::AsyncWrite;
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::FramedWrite;
use tokio_util::sync::CancellationToken;

use crate::codec::{
    Disconnect, DisconnectReasonCode, EncodeError, Packet, PublishDelivery, PublishMessage, V5Codec, encode_publish,
    encode_publish_ack, encode_publish_with_alias,
};
use crate::connection::packet_id::InflightPots;
use crate::connection::subscriptions::SubscriptionRegistry;
use crate::connection::{
    ClientCommand, CommandReceiver, ControlMessage, ControlReceiver, Generation, StopSignal, io_batch,
};
use crate::error::{AppError, ProtocolError, TransportError};

use super::acks::{complete_publish_ack, complete_subscribe_ack, complete_unsubscribe_ack, fail_all_waiters};
use super::{
    BrokerPublishLimits, MAX_READY_DRAIN, PendingAcks, PendingOutboundPublish, PublishWaiters, SubUnsubWaiters,
    WRITE_BUFFER_SOFT_FLUSH_BYTES, WriteOutcome,
};

/// How blocking socket flushes interact with cancellation.
#[derive(Clone, Copy)]
pub(super) enum FlushAbort<'a> {
    /// Hot path: race flush with session cancel and app `shutdown_notify`.
    Hot {
        cancel: &'a StopSignal,
        shutdown_notify: &'a CancellationToken,
        /// A flush that has not finished after this long means the broker stopped reading:
        /// the connection is dead even though it is open (`None`: Keep Alive off).
        stall_limit: Option<Duration>,
        /// Set when bytes leave the socket below TLS or WebSocket, which drain their own
        /// buffers inside the flush; `None` on plain TCP.
        socket_written: Option<&'a AtomicBool>,
    },
    /// Graceful DISCONNECT path: do not race already-cancelled tokens; caller bounds with timeout.
    Graceful,
}

/// Mutable state for [`graceful_disconnect`] (keeps the helper under the arg limit).
pub(super) struct GracefulDisconnectCtx<'a> {
    pub(super) control_rx: &'a mut ControlReceiver,
    pub(super) waiters: &'a mut SubUnsubWaiters,
    pub(super) publish_waiters: &'a mut PublishWaiters,
    pub(super) inflight: &'a mut InflightPots,
    pub(super) flush_budget: Duration,
    pub(super) command_rx: &'a mut CommandReceiver,
    pub(super) publish_limits: BrokerPublishLimits,
    pub(super) subscription_registry: &'a Arc<RwLock<SubscriptionRegistry>>,
    pub(super) generation: Generation,
    pub(super) read_stop: &'a StopSignal,
    pub(super) pending_acks: &'a PendingAcks,
}

/// Graceful shutdown, bounded by `flush_budget`:
///
/// 1. Send the parked publishes the window has room for, then close the command channel
///    and send what is already queued (publishes are encoded or parked; subscribe /
///    unsubscribe fail with [`AppError::ShuttingDown`]).
/// 2. Keep processing control while outbound QoS-1 publishes (in flight or parked) await
///    their PUBACKs. The read task is still running, so inbound messages keep being
///    delivered and acknowledged instead of being dropped in the socket buffer.
/// 3. Stop the read task, flush its last inbound PUBACKs, send DISCONNECT.
///
/// Order on the wire **MUST** be `[…, PubAck*, Disconnect]`. Whatever is still
/// unacknowledged when the budget runs out fails with [`AppError::ConnectionLost`].
///
/// Socket flushes here **MUST NOT** race `shutdown_notify` or session `cancel`: both are
/// typically already cancelled when this path runs, so racing either would abort every flush
/// without writing DISCONNECT. A stuck socket is bounded by `flush_budget` (and a short final
/// DISCONNECT attempt) instead.
pub(super) async fn graceful_disconnect<W>(
    framed: &mut FramedWrite<W, V5Codec>,
    ctx: GracefulDisconnectCtx<'_>,
    mut read_done: oneshot::Receiver<()>,
) -> WriteOutcome
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    // Test hook: pause inside Shutdown so a late AckInboundPublish can be
    // queued on control before DISCONNECT is sent (drain-before-close).
    #[cfg(test)]
    if let Some(gate) = super::shutdown_test_hook::take() {
        gate.entered.notify_waiters();
        gate.release.notified().await;
    }

    let flush_abort = FlushAbort::Graceful;
    let GracefulDisconnectCtx {
        control_rx,
        waiters,
        publish_waiters,
        inflight,
        flush_budget,
        command_rx,
        publish_limits,
        subscription_registry,
        generation,
        read_stop,
        pending_acks,
    } = ctx;

    // Normal Disconnection, unless the read task reports a packet it could not accept.
    let mut disconnect_reason = DisconnectReasonCode::NormalDisconnection;
    let drain = async {
        // A PUBACK handled just before shutdown may have freed slots; the publishes parked
        // behind it are older than anything queued and no later PUBACK would send them.
        super::commands::send_parked_publishes(framed, inflight, publish_waiters)?;
        drain_queued_commands(framed, command_rx, inflight, publish_waiters, publish_limits).await?;
        flush_writes(framed, flush_abort).await?;

        let mut read_finished = false;
        while !publish_waiters.by_id.is_empty() || publish_waiters.waiting_for_window() > 0 {
            tokio::select! {
                biased;
                msg = control_rx.recv() => match msg {
                    Some(ControlMessage::PublishAck { ack, generation: ack_generation }) => {
                        complete_publish_ack(publish_waiters, inflight, generation, ack_generation, ack);
                        super::commands::send_parked_publishes(framed, inflight, publish_waiters)?;
                        flush_writes(framed, flush_abort).await?;
                    }
                    Some(ControlMessage::AcksPending) => {
                        pending_acks.feed(framed, None)?;
                        flush_writes(framed, flush_abort).await?;
                    }
                    Some(ControlMessage::SubscribeAck { ack, generation: ack_generation }) => {
                        if complete_subscribe_ack(waiters, inflight, subscription_registry, generation, ack_generation, ack).is_err() {
                            disconnect_reason = DisconnectReasonCode::ProtocolError;
                            break;
                        }
                    }
                    Some(ControlMessage::UnsubscribeAck { ack, generation: ack_generation }) => {
                        if complete_unsubscribe_ack(waiters, inflight, subscription_registry, generation, ack_generation, ack).is_err() {
                            disconnect_reason = DisconnectReasonCode::ProtocolError;
                            break;
                        }
                    }
                    // A re-authentication still running on the read task: its answer goes out,
                    // or the broker's next challenge would never come and the exchange time out.
                    Some(ControlMessage::Auth(auth)) => {
                        send_packet(framed, Packet::Auth(*auth), flush_abort).await?;
                    }
                    // The read task saw a protocol violation: nothing more will be acknowledged.
                    Some(ControlMessage::ProtocolDisconnect(reason)) => {
                        disconnect_reason = reason;
                        break;
                    }
                    None => break,
                    #[allow(unreachable_patterns)] // test-only control variants
                    Some(_) => {}
                },
                result = &mut read_done, if !read_finished => {
                    // Peer closed / read failed: no PUBACK can arrive any more.
                    let _ = result;
                    read_finished = true;
                    break;
                }
            }
        }

        read_stop.cancel();
        let read_done = (!read_finished).then_some(&mut read_done);
        drain_inbound_publish_acks_until_read_done(
            framed,
            control_rx,
            pending_acks,
            read_done,
            flush_abort,
            &mut disconnect_reason,
        )
        .await?;
        // Best-effort DISCONNECT so the broker sees a clean close after PUBACKs.
        send_packet(
            framed,
            Packet::Disconnect(Disconnect::new(disconnect_reason)),
            flush_abort,
        )
        .await?;
        // Then close the transport too, within the same budget: TLS sends its close_notify and
        // WebSocket its Close frame, which brokers and gateways otherwise log as an abrupt end.
        // Nothing follows the DISCONNECT on the MQTT level, so a failure here changes nothing.
        let _ = SinkExt::<Packet>::close(framed).await;
        Ok(())
    };

    let drained = tokio::time::timeout(flush_budget, drain).await;
    read_stop.cancel();
    fail_all_waiters(waiters, publish_waiters, inflight, &AppError::ConnectionLost);
    // What the budget left in the channel was never taken up.
    crate::connection::channels::fail_queued_commands(command_rx, &AppError::ShuttingDown);
    match drained {
        Ok(Ok(())) => WriteOutcome::Shutdown,
        Ok(Err(outcome)) => outcome,
        Err(_elapsed) => {
            // Budget exhausted: still attempt DISCONNECT so the peer sees a clean reason.
            // Bound the attempt so a stuck socket cannot hang past the graceful path.
            let _ = tokio::time::timeout(
                Duration::from_millis(50),
                send_packet(framed, Packet::Disconnect(Disconnect::default()), flush_abort),
            )
            .await;
            // The read task may still run: `read_done` was not awaited.
            pending_acks.close();
            WriteOutcome::Shutdown
        }
    }
}

/// Graceful step 1: closes the command channel and handles everything already queued.
async fn drain_queued_commands<W>(
    framed: &mut FramedWrite<W, V5Codec>,
    command_rx: &mut CommandReceiver,
    inflight: &mut InflightPots,
    publish_waiters: &mut PublishWaiters,
    limits: BrokerPublishLimits,
) -> Result<(), WriteOutcome>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    command_rx.close();
    while let Ok(command) = command_rx.try_recv() {
        match command {
            ClientCommand::Publish {
                message,
                qos,
                retain,
                reply,
            } => {
                // The parked queue may exceed its usual cap here: nothing new can arrive, so it
                // stays bounded by the command channel capacity.
                super::commands::handle_publish(
                    framed,
                    inflight,
                    publish_waiters,
                    PendingOutboundPublish {
                        message,
                        qos,
                        retain,
                        reply,
                    },
                    limits,
                )?;
                flush_if_buffer_large(framed, FlushAbort::Graceful).await?;
            }
            ClientCommand::Subscribe { reply, .. } => {
                let _ = reply.send(Err(AppError::ShuttingDown));
            }
            ClientCommand::Unsubscribe { reply, .. } => {
                let _ = reply.send(Err(AppError::ShuttingDown));
            }
            #[cfg(test)]
            ClientCommand::InsertPublishWaiter { packet_id, reply } if !reply.is_closed() => {
                publish_waiters.insert(packet_id, reply);
            }
            #[allow(unreachable_patterns)] // Shutdown + test-only commands
            _ => {}
        }
    }
    Ok(())
}

/// Feeds the read task's queued PUBACKs ([`ControlMessage::AcksPending`]) until it has exited,
/// then whatever it queued last.
/// A [`ControlMessage::ProtocolDisconnect`] met on the way sets `disconnect_reason`.
pub(super) async fn drain_inbound_publish_acks_until_read_done<W>(
    framed: &mut FramedWrite<W, V5Codec>,
    control_rx: &mut ControlReceiver,
    pending_acks: &PendingAcks,
    mut read_done: Option<&mut oneshot::Receiver<()>>,
    flush_abort: FlushAbort<'_>,
    disconnect_reason: &mut DisconnectReasonCode,
) -> Result<(), WriteOutcome>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    let mut dirty = false;
    // Recv Acks concurrently with waiting for read-join (read may still be sending Acks).
    // `None`: the read task has already exited.
    while let Some(read_done) = read_done.as_deref_mut() {
        tokio::select! {
            biased;
            msg = control_rx.recv() => {
                match msg {
                    Some(ControlMessage::AcksPending) => {
                        dirty |= pending_acks.feed(framed, None)? > 0;
                        flush_if_buffer_large(framed, flush_abort).await?;
                    }
                    Some(ControlMessage::ProtocolDisconnect(reason)) => *disconnect_reason = reason,
                    Some(_other) => {
                        // Waiters already failed; ignore non-ack control during drain.
                    }
                    None => {
                        dirty |= pending_acks.feed(framed, None)? > 0;
                        if dirty {
                            flush_writes(framed, flush_abort).await?;
                        }
                        return Ok(());
                    }
                }
            }
            result = &mut *read_done => {
                let _ = result;
                break;
            }
        }
    }

    // Read has exited: every PUBACK it queued is in `pending_acks` now, told or not (its exit
    // path does not wait for room in the channel).
    loop {
        match control_rx.try_recv() {
            Ok(ControlMessage::ProtocolDisconnect(reason)) => *disconnect_reason = reason,
            Ok(_other) => {}
            Err(mpsc::error::TryRecvError::Empty) | Err(mpsc::error::TryRecvError::Disconnected) => {
                dirty |= pending_acks.feed(framed, None)? > 0;
                if dirty {
                    flush_writes(framed, flush_abort).await?;
                }
                return Ok(());
            }
        }
    }
}

/// Time allowed for a DISCONNECT the read task asked for when the session is already being
/// torn down; the supervisor is waiting for this task.
const PENDING_DISCONNECT_BUDGET: Duration = Duration::from_secs(1);

/// Best-effort DISCONNECT with `reason` at the end of a session: 0x82 for a
/// [`WriteOutcome::BrokerViolation`] (§4.13), or the reason the read task queued for a packet it
/// could not accept ([MQTT-3.14.1-1]) when the cancel won the race against that message.
pub(super) async fn send_disconnect_best_effort<W>(framed: &mut FramedWrite<W, V5Codec>, reason: DisconnectReasonCode)
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    let disconnect = send_packet(
        framed,
        Packet::Disconnect(Disconnect::new(reason)),
        FlushAbort::Graceful,
    );
    let _ = tokio::time::timeout(PENDING_DISCONNECT_BUDGET, disconnect).await;
}

/// At the end of a session: completes the requests whose acknowledgements are already queued,
/// and returns the reason of a DISCONNECT the read task asked for, if any.
pub(super) fn apply_queued_acks(
    control_rx: &mut ControlReceiver,
    waiters: &mut SubUnsubWaiters,
    publish_waiters: &mut PublishWaiters,
    inflight: &mut InflightPots,
    subscription_registry: &Arc<RwLock<SubscriptionRegistry>>,
    generation: Generation,
) -> Result<Option<DisconnectReasonCode>, WriteOutcome> {
    let mut disconnect = None;
    while let Ok(message) = control_rx.try_recv() {
        match message {
            ControlMessage::PublishAck {
                ack,
                generation: ack_generation,
            } => complete_publish_ack(publish_waiters, inflight, generation, ack_generation, ack),
            ControlMessage::SubscribeAck {
                ack,
                generation: ack_generation,
            } => complete_subscribe_ack(
                waiters,
                inflight,
                subscription_registry,
                generation,
                ack_generation,
                ack,
            )?,
            ControlMessage::UnsubscribeAck {
                ack,
                generation: ack_generation,
            } => complete_unsubscribe_ack(
                waiters,
                inflight,
                subscription_registry,
                generation,
                ack_generation,
                ack,
            )?,
            // A DISCONNECT the read task asked for is still to be sent.
            ControlMessage::ProtocolDisconnect(reason) => {
                disconnect.get_or_insert(reason);
            }
            _ => {}
        }
    }
    Ok(disconnect)
}

/// Feeds a success PUBACK without flushing (caller coalesces + flushes).
///
/// Uses [`encode_publish_ack`] into the write buffer — same pattern as [`feed_publish`] /
/// [`encode_publish`] (no `Packet` / `FramedWrite::feed`).
pub(crate) fn feed_inbound_publish_ack<W>(
    framed: &mut FramedWrite<W, V5Codec>,
    packet_id: NonZeroU16,
    counter: Option<&Arc<AtomicUsize>>,
) -> Result<(), WriteOutcome>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    let max_out = framed.encoder().max_outbound_size();
    encode_publish_ack(packet_id, max_out, framed.write_buffer_mut()).map_err(map_encode_error)?;
    if let Some(counter) = counter {
        counter.fetch_add(1, Ordering::Relaxed);
    }
    Ok(())
}

/// Mutable borrow bundle for [`drain_ready_control_coalesce`].
pub(super) struct ControlCoalesceCtx<'a, W>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    pub(super) framed: &'a mut FramedWrite<W, V5Codec>,
    pub(super) control_rx: &'a mut ControlReceiver,
    pub(super) command_rx: &'a CommandReceiver,
    pub(super) waiters: &'a mut SubUnsubWaiters,
    pub(super) publish_waiters: &'a mut PublishWaiters,
    pub(super) inflight: &'a mut InflightPots,
    pub(super) subscription_registry: &'a Arc<RwLock<SubscriptionRegistry>>,
    pub(super) generation: Generation,
    pub(super) inbound_publish_ack_counter: Option<&'a Arc<AtomicUsize>>,
    pub(super) pending_acks: &'a PendingAcks,
    pub(super) flush_abort: FlushAbort<'a>,
}

/// Drains ready coalesceable control up to the adaptive I/O batch, then yields.
///
/// Returns a deferred non-coalesceable message if one was dequeued past the batch.
pub(super) async fn drain_ready_control_coalesce<W>(
    ctx: &mut ControlCoalesceCtx<'_, W>,
    mut fed_publish_acks: usize,
) -> Result<Option<ControlMessage>, WriteOutcome>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    let batch = io_batch(!ctx.command_rx.is_empty());
    for _ in 0..MAX_READY_DRAIN {
        if fed_publish_acks >= batch {
            break;
        }
        match ctx.control_rx.try_recv() {
            Ok(ControlMessage::AcksPending) => {
                fed_publish_acks += ctx.pending_acks.feed(ctx.framed, ctx.inbound_publish_ack_counter)?;
                flush_if_buffer_large(ctx.framed, ctx.flush_abort).await?;
            }
            Ok(ControlMessage::PublishAck {
                ack,
                generation: ack_generation,
            }) => {
                complete_publish_ack(ctx.publish_waiters, ctx.inflight, ctx.generation, ack_generation, ack);
            }
            // The flush after this drain covers it.
            Ok(ControlMessage::FlushWrites) => {}
            Ok(ControlMessage::SubscribeAck {
                ack,
                generation: ack_generation,
            }) => {
                complete_subscribe_ack(
                    ctx.waiters,
                    ctx.inflight,
                    ctx.subscription_registry,
                    ctx.generation,
                    ack_generation,
                    ack,
                )?;
            }
            Ok(ControlMessage::UnsubscribeAck {
                ack,
                generation: ack_generation,
            }) => {
                complete_unsubscribe_ack(
                    ctx.waiters,
                    ctx.inflight,
                    ctx.subscription_registry,
                    ctx.generation,
                    ack_generation,
                    ack,
                )?;
            }
            #[cfg(test)]
            Ok(ControlMessage::StubExitRead) => {}
            Ok(other) => return Ok(Some(other)),
            Err(mpsc::error::TryRecvError::Empty) => break,
            Err(mpsc::error::TryRecvError::Disconnected) => {
                return Err(WriteOutcome::ControlChannelClosed);
            }
        }
    }
    Ok(None)
}

/// [`drain_ready_control_coalesce`], then one flush for everything it fed, then the
/// non-coalesceable message it ran into, if any.
pub(super) async fn coalesce_ready_control_and_flush<W>(
    ctx: &mut ControlCoalesceCtx<'_, W>,
    fed_publish_acks: usize,
) -> Result<(), WriteOutcome>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    let deferred = drain_ready_control_coalesce(ctx, fed_publish_acks).await?;
    flush_writes(ctx.framed, ctx.flush_abort).await?;
    match deferred {
        // Deferred non-coalesceable control (PING / DISCONNECT / ...).
        Some(deferred) => {
            match handle_deferred_control(
                deferred,
                ctx.framed,
                ctx.flush_abort,
                ctx.pending_acks,
                ctx.inbound_publish_ack_counter,
            )
            .await
            {
                Some(outcome) => Err(outcome),
                None => Ok(()),
            }
        }
        None => Ok(()),
    }
}

/// Handles a control message that could not be coalesced with inbound PUBACKs.
pub(super) async fn handle_deferred_control<W>(
    control: ControlMessage,
    framed: &mut FramedWrite<W, V5Codec>,
    flush_abort: FlushAbort<'_>,
    pending_acks: &PendingAcks,
    inbound_publish_ack_counter: Option<&Arc<AtomicUsize>>,
) -> Option<WriteOutcome>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    match control {
        #[cfg(test)]
        ControlMessage::StubExitWrite => Some(WriteOutcome::StubExit),
        #[cfg(test)]
        ControlMessage::PingReq => send_packet(framed, Packet::PingRequest, flush_abort).await.err(),
        #[cfg(test)]
        ControlMessage::Disconnect => {
            match send_packet(framed, Packet::Disconnect(Disconnect::default()), flush_abort).await {
                Ok(()) => Some(WriteOutcome::Shutdown),
                Err(outcome) => Some(outcome),
            }
        }
        ControlMessage::ProtocolDisconnect(reason) => {
            pending_acks.close();
            if let Err(outcome) = send_packet(framed, Packet::Disconnect(Disconnect::new(reason)), flush_abort).await {
                return Some(outcome);
            }
            let _ = framed.close().await;
            if let FlushAbort::Hot { cancel, .. } = flush_abort {
                cancel.cancelled().await;
            }
            Some(WriteOutcome::Cancelled)
        }
        ControlMessage::AcksPending => {
            if let Err(outcome) = pending_acks.feed(framed, inbound_publish_ack_counter) {
                return Some(outcome);
            }
            flush_writes(framed, flush_abort).await.err()
        }
        ControlMessage::Auth(auth) => send_packet(framed, Packet::Auth(*auth), flush_abort).await.err(),
        ControlMessage::FlushWrites => flush_writes(framed, flush_abort).await.err(),
        // Coalesce already completed these ACKs; deferred path is a defensive vacuum.
        ControlMessage::SubscribeAck { .. }
        | ControlMessage::UnsubscribeAck { .. }
        | ControlMessage::PublishAck { .. } => None,
        #[cfg(test)]
        ControlMessage::StubExitRead => None,
    }
}

pub(super) async fn feed_packet<W>(framed: &mut FramedWrite<W, V5Codec>, packet: Packet) -> Result<(), WriteOutcome>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    framed.feed(packet).await.map_err(map_encode_error)
}

pub(super) async fn flush_writes<W>(
    framed: &mut FramedWrite<W, V5Codec>,
    flush_abort: FlushAbort<'_>,
) -> Result<(), WriteOutcome>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    match flush_abort {
        FlushAbort::Hot {
            cancel,
            shutdown_notify,
            stall_limit,
            socket_written,
        } => {
            // Notes whether the flush got any bytes out since the stall timer last looked.
            let progressed = AtomicBool::new(false);
            let flush = std::future::poll_fn(|cx| {
                let buffered = framed.write_buffer().len();
                let polled = Sink::<Packet>::poll_flush(std::pin::Pin::new(&mut *framed), cx);
                if framed.write_buffer().len() < buffered {
                    progressed.store(true, Ordering::Relaxed);
                }
                polled
            });
            tokio::pin!(flush);
            // Flush first: it is almost always ready at once, and then neither the token
            // futures (polling one locks the token's mutex) nor a stall timer are touched.
            if let Some(result) = flush.as_mut().now_or_never() {
                return result.map_err(map_encode_error);
            }
            // The socket is full. Keep-alive pings come from this task, so a broker that stops
            // reading but keeps the connection open is only noticed here. The limit is for a
            // flush that gets nothing out: a large publish on a slow link takes longer than it
            // and is no dead broker.
            let stalled = async {
                match stall_limit {
                    Some(limit) => {
                        progressed.store(false, Ordering::Relaxed);
                        let socket_moved =
                            || socket_written.is_some_and(|written| written.swap(false, Ordering::Relaxed));
                        socket_moved();
                        loop {
                            tokio::time::sleep(limit).await;
                            // Both: bytes leaving this task's buffer, or the socket under TLS
                            // or WebSocket.
                            if !(progressed.swap(false, Ordering::Relaxed) | socket_moved()) {
                                break;
                            }
                        }
                    }
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                biased;
                result = flush => result.map_err(map_encode_error),
                _ = shutdown_notify.cancelled() => Err(WriteOutcome::Shutdown),
                _ = cancel.cancelled() => Err(WriteOutcome::Cancelled),
                () = stalled => Err(WriteOutcome::Transport(TransportError::Connection(format!(
                    "broker stopped reading: write blocked for {:?}",
                    stall_limit.unwrap_or_default()
                )))),
            }
        }
        FlushAbort::Graceful => framed.flush().await.map_err(map_encode_error),
    }
}

pub(super) async fn flush_if_buffer_large<W>(
    framed: &mut FramedWrite<W, V5Codec>,
    flush_abort: FlushAbort<'_>,
) -> Result<(), WriteOutcome>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    if framed.write_buffer().len() >= WRITE_BUFFER_SOFT_FLUSH_BYTES {
        flush_writes(framed, flush_abort).await?;
    }
    Ok(())
}

/// Encodes and immediately flushes (PING / DISCONNECT / SUBSCRIBE / UNSUBSCRIBE).
pub(super) async fn send_packet<W>(
    framed: &mut FramedWrite<W, V5Codec>,
    packet: Packet,
    flush_abort: FlushAbort<'_>,
) -> Result<(), WriteOutcome>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    flush_before_feed(framed, flush_abort).await?;
    feed_packet(framed, packet).await?;
    flush_writes(framed, flush_abort).await
}

/// Flushes what is still buffered before a `feed`: `feed` flushes inside `poll_ready` once the
/// buffer reaches the backpressure boundary, where no cancel, shutdown or stall limit reaches
/// it. PUBACKs the read task wrote but could not flush can fill the buffer that far.
pub(super) async fn flush_before_feed<W>(
    framed: &mut FramedWrite<W, V5Codec>,
    flush_abort: FlushAbort<'_>,
) -> Result<(), WriteOutcome>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    if framed.write_buffer().is_empty() {
        return Ok(());
    }
    flush_writes(framed, flush_abort).await
}

/// Encodes a PUBLISH into the write buffer without flushing (caller coalesces + flushes).
///
/// Topic / delivery checks live in [`encode_publish`]; map failures with
/// [`finish_publish_encode_error`] so bad topics stay `InvalidRequest` (session alive).
/// Encodes a PUBLISH into the write buffer, using a Topic Alias when this connection has
/// aliasing enabled. The alias binding is recorded only after a successful encode.
pub(super) fn feed_publish<W>(
    framed: &mut FramedWrite<W, V5Codec>,
    aliases: &mut crate::connection::topic_alias::OutboundTopicAliases,
    message: &PublishMessage,
    delivery: PublishDelivery,
) -> Result<(), EncodeError>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    let max_out = framed.encoder().max_outbound_size();
    if !aliases.enabled() {
        return encode_publish(message, delivery, max_out, framed.write_buffer_mut());
    }
    let alias = aliases.choose(&message.topic);
    match encode_publish_with_alias(message, delivery, alias, max_out, framed.write_buffer_mut()) {
        Ok(()) => {
            if let Some(alias) = alias {
                aliases.commit(&message.topic, alias);
            }
            Ok(())
        }
        // Binding an alias adds its property next to the full topic, a few bytes that can tip a
        // publish just under the broker's Maximum Packet Size over it; it fits without one.
        // The encoder left nothing behind, and an alias not committed was never bound.
        Err(EncodeError::OverMaxPacketSize { .. })
            if matches!(alias, Some(crate::codec::OutboundTopicAlias::Assign(_))) =>
        {
            encode_publish(message, delivery, max_out, framed.write_buffer_mut())
        }
        Err(error) => Err(error),
    }
}

pub(super) fn map_encode_error(error: EncodeError) -> WriteOutcome {
    match error {
        EncodeError::Io(message) => WriteOutcome::Transport(TransportError::Connection(message.to_string())),
        other => WriteOutcome::Protocol(ProtocolError::Violation(other.to_string())),
    }
}
