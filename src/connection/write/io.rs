//! Write-half I/O helpers: encode/flush, inbound PUBACK feed, control coalesce, graceful disconnect.

use std::num::NonZeroU16;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use futures_util::SinkExt;
use tokio::io::AsyncWrite;
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::FramedWrite;
use tokio_util::sync::CancellationToken;

use crate::codec::{
    Disconnect, DisconnectReasonCode, EncodeError, Packet, PublishDelivery, PublishMessage, V5Codec, encode_publish,
    encode_publish_ack,
};
use crate::connection::packet_id::InflightPots;
use crate::connection::subscriptions::SubscriptionRegistry;
use crate::connection::{CommandReceiver, ControlMessage, ControlReceiver, Generation, effective_io_batch};
use crate::error::{AppError, ProtocolError, TransportError};

use super::acks::{complete_publish_ack, complete_subscribe_ack, complete_unsubscribe_ack, fail_all_waiters};
use super::{MAX_READY_DRAIN, PublishWaiters, SubUnsubWaiters, WRITE_BUFFER_SOFT_FLUSH_BYTES, WriteOutcome};

/// How blocking socket flushes interact with cancellation.
#[derive(Clone, Copy)]
pub(super) enum FlushAbort<'a> {
    /// Hot path: race flush with session cancel and app `shutdown_notify`.
    Hot {
        cancel: &'a CancellationToken,
        shutdown_notify: &'a CancellationToken,
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
}

/// Best-effort graceful shutdown: wait for read exit, flush pending inbound PUBACKs, then
/// send DISCONNECT.
///
/// While waiting for `read_done`, control Acks are still received (avoids deadlock if read is
/// blocked on `AckInboundPublish` send). After read exits, the control queue is vacuumed with
/// `try_recv` — no idle timer. Order on the wire **MUST** be `[PubAck*, Disconnect]`.
///
/// Socket flushes here **MUST NOT** race `shutdown_notify` or session `cancel`: both are
/// typically already cancelled when this path runs (`client.shutdown` cancels notify then the
/// driver token), so racing either would abort every flush without writing DISCONNECT. A stuck
/// socket is bounded by `flush_budget` (and a short final DISCONNECT attempt) instead.
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
    fail_all_waiters(
        ctx.waiters,
        ctx.publish_waiters,
        ctx.inflight,
        &AppError::ConnectionLost,
    );

    let flush_abort = FlushAbort::Graceful;

    let flush = async {
        if let Err(outcome) =
            drain_inbound_publish_acks_until_read_done(framed, ctx.control_rx, &mut read_done, flush_abort).await
        {
            return outcome;
        }
        // Best-effort DISCONNECT so the broker sees a clean close after PUBACKs.
        if let Err(outcome) = send_packet(framed, Packet::Disconnect(Disconnect::default()), flush_abort).await {
            return outcome;
        }
        WriteOutcome::Shutdown
    };

    match tokio::time::timeout(ctx.flush_budget, flush).await {
        Ok(outcome) => outcome,
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

/// Drains [`ControlMessage::AckInboundPublish`] until the read task has exited, then vacuums.
pub(super) async fn drain_inbound_publish_acks_until_read_done<W>(
    framed: &mut FramedWrite<W, V5Codec>,
    control_rx: &mut ControlReceiver,
    read_done: &mut oneshot::Receiver<()>,
    flush_abort: FlushAbort<'_>,
) -> Result<(), WriteOutcome>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    let mut dirty = false;
    // Recv Acks concurrently with waiting for read-join (read may still be sending Acks).
    loop {
        tokio::select! {
            biased;
            msg = control_rx.recv() => {
                match msg {
                    Some(ControlMessage::AckInboundPublish { packet_id }) => {
                        feed_inbound_publish_ack(framed, packet_id, None)?;
                        dirty = true;
                        flush_if_buffer_large(framed, flush_abort).await?;
                    }
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
                );
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
                );
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
        ControlMessage::ProtocolDisconnect => {
            if let Err(outcome) = send_packet(
                framed,
                Packet::Disconnect(Disconnect::new(DisconnectReasonCode::ProtocolError)),
                flush_abort,
            )
            .await
            {
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
        } => {
            tokio::select! {
                biased;
                _ = shutdown_notify.cancelled() => Err(WriteOutcome::Shutdown),
                _ = cancel.cancelled() => Err(WriteOutcome::Cancelled),
                result = framed.flush() => result.map_err(map_encode_error),
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
pub(super) fn feed_publish<W>(
    framed: &mut FramedWrite<W, V5Codec>,
    message: &PublishMessage,
    delivery: PublishDelivery,
) -> Result<(), EncodeError>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    let max_out = framed.encoder().max_outbound_size();
    encode_publish(message, delivery, max_out, framed.write_buffer_mut())
}

pub(super) fn map_encode_error(error: EncodeError) -> WriteOutcome {
    match error {
        EncodeError::Io(message) => WriteOutcome::Transport(TransportError::Connection(message.to_string())),
        other => WriteOutcome::Protocol(ProtocolError::Violation(other.to_string())),
    }
}
