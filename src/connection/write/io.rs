//! Write-half I/O helpers: encode/flush, inbound PUBACK feed, control coalesce, graceful disconnect.

use std::num::NonZeroU16;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use futures_util::{FutureExt, SinkExt};
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
    ClientCommand, CommandReceiver, ControlMessage, ControlReceiver, Generation, StopSignal, effective_io_batch,
};
use crate::error::{AppError, ProtocolError, TransportError};

use super::acks::{complete_publish_ack, complete_subscribe_ack, complete_unsubscribe_ack, fail_all_waiters};
use super::{
    BrokerPublishLimits, MAX_READY_DRAIN, PendingOutboundPublish, PublishWaiters, SubUnsubWaiters,
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
}

/// Graceful shutdown, bounded by `flush_budget`:
///
/// 1. Close the command channel and send what is already queued (publishes are encoded or
///    parked; subscribe / unsubscribe fail with [`AppError::ShuttingDown`]).
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
    } = ctx;

    // Normal Disconnection, unless the read task reports a packet it could not accept.
    let mut disconnect_reason = DisconnectReasonCode::NormalDisconnection;
    let drain = async {
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
                    Some(ControlMessage::AckInboundPublish { packet_id }) => {
                        feed_inbound_publish_ack(framed, packet_id, None)?;
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
        drain_inbound_publish_acks_until_read_done(framed, control_rx, read_done, flush_abort, &mut disconnect_reason)
            .await?;
        // Best-effort DISCONNECT so the broker sees a clean close after PUBACKs.
        send_packet(
            framed,
            Packet::Disconnect(Disconnect::new(disconnect_reason)),
            flush_abort,
        )
        .await
    };

    let drained = tokio::time::timeout(flush_budget, drain).await;
    read_stop.cancel();
    fail_all_waiters(waiters, publish_waiters, inflight, &AppError::ConnectionLost);
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
                )
                .await?;
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

/// Drains [`ControlMessage::AckInboundPublish`] until the read task has exited, then vacuums.
/// A [`ControlMessage::ProtocolDisconnect`] met on the way sets `disconnect_reason`.
pub(super) async fn drain_inbound_publish_acks_until_read_done<W>(
    framed: &mut FramedWrite<W, V5Codec>,
    control_rx: &mut ControlReceiver,
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
                    Some(ControlMessage::AckInboundPublish { packet_id }) => {
                        feed_inbound_publish_ack(framed, packet_id, None)?;
                        dirty = true;
                        flush_if_buffer_large(framed, flush_abort).await?;
                    }
                    Some(ControlMessage::ProtocolDisconnect(reason)) => *disconnect_reason = reason,
                    Some(_other) => {
                        // Waiters already failed; ignore non-ack control during drain.
                    }
                    None => {
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

    // Read has exited: any remaining Acks are already in the mpsc (or none left).
    loop {
        match control_rx.try_recv() {
            Ok(ControlMessage::AckInboundPublish { packet_id }) => {
                feed_inbound_publish_ack(framed, packet_id, None)?;
                dirty = true;
                flush_if_buffer_large(framed, flush_abort).await?;
            }
            Ok(ControlMessage::ProtocolDisconnect(reason)) => *disconnect_reason = reason,
            Ok(_other) => {}
            Err(mpsc::error::TryRecvError::Empty) | Err(mpsc::error::TryRecvError::Disconnected) => {
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

/// Best-effort DISCONNECT 0x82 for a [`WriteOutcome::BrokerViolation`] (§4.13).
pub(super) async fn send_broker_violation_disconnect<W>(framed: &mut FramedWrite<W, V5Codec>)
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    let disconnect = send_packet(
        framed,
        Packet::Disconnect(Disconnect::new(DisconnectReasonCode::ProtocolError)),
        FlushAbort::Graceful,
    );
    let _ = tokio::time::timeout(PENDING_DISCONNECT_BUDGET, disconnect).await;
}

/// The supervisor cancelled the session after the read task ended. If that task queued a
/// DISCONNECT reason first (a packet it could not accept, [MQTT-3.14.1-1]), send it now: the
/// cancel can win the race against the queued message.
pub(super) async fn send_pending_protocol_disconnect<W>(
    framed: &mut FramedWrite<W, V5Codec>,
    control_rx: &mut ControlReceiver,
) where
    W: AsyncWrite + Unpin + Send + 'static,
{
    while let Ok(message) = control_rx.try_recv() {
        if let ControlMessage::ProtocolDisconnect(reason) = message {
            let disconnect = send_packet(
                framed,
                Packet::Disconnect(Disconnect::new(reason)),
                FlushAbort::Graceful,
            );
            let _ = tokio::time::timeout(PENDING_DISCONNECT_BUDGET, disconnect).await;
            return;
        }
    }
}

/// Feeds a success PUBACK without flushing (caller coalesces + flushes).
///
/// Uses [`encode_publish_ack`] into the write buffer — same pattern as [`feed_publish`] /
/// [`encode_publish`] (no `Packet` / `FramedWrite::feed`).
pub(super) fn feed_inbound_publish_ack<W>(
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
    pub(super) receive_maximum: NonZeroU16,
    pub(super) inbound_publish_ack_counter: Option<&'a Arc<AtomicUsize>>,
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
    let batch = effective_io_batch(ctx.receive_maximum, !ctx.command_rx.is_empty());
    for _ in 0..MAX_READY_DRAIN {
        if fed_publish_acks >= batch {
            break;
        }
        match ctx.control_rx.try_recv() {
            Ok(ControlMessage::AckInboundPublish { packet_id }) => {
                feed_inbound_publish_ack(ctx.framed, packet_id, ctx.inbound_publish_ack_counter)?;
                fed_publish_acks += 1;
                flush_if_buffer_large(ctx.framed, ctx.flush_abort).await?;
            }
            Ok(ControlMessage::PublishAck {
                ack,
                generation: ack_generation,
            }) => {
                complete_publish_ack(ctx.publish_waiters, ctx.inflight, ctx.generation, ack_generation, ack);
            }
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

/// Handles a control message that could not be coalesced with inbound PUBACKs.
pub(super) async fn handle_deferred_control<W>(
    control: ControlMessage,
    framed: &mut FramedWrite<W, V5Codec>,
    flush_abort: FlushAbort<'_>,
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
            if let Err(outcome) = send_packet(framed, Packet::Disconnect(Disconnect::new(reason)), flush_abort).await {
                return Some(outcome);
            }
            let _ = framed.close().await;
            if let FlushAbort::Hot { cancel, .. } = flush_abort {
                cancel.cancelled().await;
            }
            Some(WriteOutcome::Cancelled)
        }
        ControlMessage::AckInboundPublish { packet_id } => {
            if let Err(outcome) = feed_inbound_publish_ack(framed, packet_id, inbound_publish_ack_counter) {
                return Some(outcome);
            }
            flush_writes(framed, flush_abort).await.err()
        }
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
        } => {
            // Flush first: it is almost always ready at once, and then neither the token
            // futures (polling one locks the token's mutex) nor a stall timer are touched.
            let flush = framed.flush();
            tokio::pin!(flush);
            if let Some(result) = flush.as_mut().now_or_never() {
                return result.map_err(map_encode_error);
            }
            // The socket is full. Keep-alive pings come from this task, so a broker that stops
            // reading but keeps the connection open is only noticed here.
            let stalled = async {
                match stall_limit {
                    Some(limit) => tokio::time::sleep(limit).await,
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
    feed_packet(framed, packet).await?;
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
    encode_publish_with_alias(message, delivery, alias, max_out, framed.write_buffer_mut())?;
    if let Some(alias) = alias {
        aliases.commit(&message.topic, alias);
    }
    Ok(())
}

pub(super) fn map_encode_error(error: EncodeError) -> WriteOutcome {
    match error {
        EncodeError::Io(message) => WriteOutcome::Transport(TransportError::Connection(message.to_string())),
        other => WriteOutcome::Protocol(ProtocolError::Violation(other.to_string())),
    }
}
