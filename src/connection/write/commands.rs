//! Application command dispatch: publish / subscribe / unsubscribe.

use std::num::NonZeroU16;

use bytestring::ByteString;
use tokio::io::AsyncWrite;
use tokio::sync::mpsc;
use tokio_util::codec::FramedWrite;

use crate::codec::{
    EncodeError, Packet, PublishDelivery, PublishMessage, QoS, Subscribe, SubscriptionOptions, Unsubscribe, V5Codec,
    topic_filter_bytes_invalid,
};
use crate::connection::packet_id::InflightPots;
use crate::connection::{
    ClientCommand, CommandReceiver, PublishReply, SubscribeReply, UnsubscribeReply, effective_io_batch,
    validate_publish_against_broker_limits,
};
use crate::error::AppError;
use crate::util::{is_shared_subscription_filter, validate_share_name};

use super::acks::app_error_from_write_outcome;
use super::io::{FlushAbort, feed_publish, flush_if_buffer_large, flush_writes, map_encode_error, send_packet};
use super::{
    BrokerPublishLimits, MAX_READY_DRAIN, PendingOutboundPublish, PendingSubscribe, PendingUnsubscribe, PublishWaiters,
    SubUnsubWaiters, WriteOutcome,
};

/// Mutable state needed to dispatch one [`ClientCommand`].
pub(super) struct CommandDispatchCtx<'a, W>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    pub(super) framed: &'a mut FramedWrite<W, V5Codec>,
    pub(super) command_rx: &'a mut CommandReceiver,
    pub(super) waiters: &'a mut SubUnsubWaiters,
    pub(super) publish_waiters: &'a mut PublishWaiters,
    pub(super) inflight: &'a mut InflightPots,
    pub(super) publish_limits: BrokerPublishLimits,
    pub(super) receive_maximum: NonZeroU16,
    pub(super) flush_abort: FlushAbort<'a>,
}

/// Result of [`dispatch_client_command`].
pub(super) enum CommandDispatch {
    /// Keep running the write loop.
    Continue,
    /// Caller should run [`graceful_disconnect`] (keeps shutdown ownership in `run`).
    #[cfg(test)]
    GracefulShutdown,
}

/// Dispatches one application command.
///
/// Used by the main `command_rx.recv()` path and for deferred commands after publish coalesce.
/// When `coalesce_publishes` is true, a leading [`ClientCommand::Publish`] drains further ready
/// PUBLISHes into the same TCP write; any deferred non-coalesceable command is handled with
/// `coalesce_publishes = false` (including defensive Publish → [`handle_publish`]).
pub(super) async fn dispatch_client_command<W>(
    cmd: ClientCommand,
    ctx: &mut CommandDispatchCtx<'_, W>,
    coalesce_publishes: bool,
) -> Result<CommandDispatch, WriteOutcome>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    let cmd = if let ClientCommand::Publish {
        message,
        qos,
        retain,
        reply,
    } = cmd
    {
        handle_publish(
            ctx.framed,
            ctx.inflight,
            ctx.publish_waiters,
            PendingOutboundPublish {
                message,
                qos,
                retain,
                reply,
            },
            ctx.publish_limits,
        )
        .await?;
        if !coalesce_publishes {
            return Ok(CommandDispatch::Continue);
        }
        // Coalesce further ready PUBLISHes into the same TCP write.
        let deferred = drain_ready_publishes(ctx, 1).await?;
        flush_writes(ctx.framed, ctx.flush_abort).await?;
        match deferred {
            Some(deferred_cmd) => deferred_cmd,
            None => return Ok(CommandDispatch::Continue),
        }
    } else {
        cmd
    };

    dispatch_deferred_or_direct_command(cmd, ctx).await
}

/// Handles a command that is not mid-publish-coalesce (direct recv or deferred after drain).
pub(super) async fn dispatch_deferred_or_direct_command<W>(
    cmd: ClientCommand,
    ctx: &mut CommandDispatchCtx<'_, W>,
) -> Result<CommandDispatch, WriteOutcome>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    match cmd {
        #[cfg(test)]
        ClientCommand::Shutdown => Ok(CommandDispatch::GracefulShutdown),
        #[cfg(test)]
        ClientCommand::Noop => Ok(CommandDispatch::Continue),
        ClientCommand::Publish {
            message,
            qos,
            retain,
            reply,
        } => {
            // Invariant: drain_ready_publishes coalesces Publish and never returns it deferred.
            // Handle defensively so the oneshot is not silently dropped.
            handle_publish(
                ctx.framed,
                ctx.inflight,
                ctx.publish_waiters,
                PendingOutboundPublish {
                    message,
                    qos,
                    retain,
                    reply,
                },
                ctx.publish_limits,
            )
            .await?;
            Ok(CommandDispatch::Continue)
        }
        #[cfg(test)]
        // Test-only: no inflight pot alloc — not the production packet-ID ownership model.
        ClientCommand::InsertPublishWaiter { packet_id, reply } => {
            if !reply.is_closed() {
                ctx.publish_waiters.insert(packet_id, reply);
            }
            Ok(CommandDispatch::Continue)
        }
        #[cfg(test)]
        // Test-only: no inflight pot alloc — not the production packet-ID ownership model.
        ClientCommand::InsertSubscribeWaiter { packet_id, reply } => {
            if !reply.is_closed() {
                ctx.waiters.subscribe.insert(
                    packet_id,
                    PendingSubscribe {
                        reply,
                        filters: Vec::new(),
                    },
                );
            }
            Ok(CommandDispatch::Continue)
        }
        #[cfg(test)]
        // Test-only: no inflight pot alloc — not the production packet-ID ownership model.
        ClientCommand::InsertUnsubscribeWaiter { packet_id, reply } => {
            if !reply.is_closed() {
                ctx.waiters.unsubscribe.insert(
                    packet_id,
                    PendingUnsubscribe {
                        reply,
                        filters: Vec::new(),
                    },
                );
            }
            Ok(CommandDispatch::Continue)
        }
        ClientCommand::Subscribe { filters, reply } => {
            handle_subscribe(ctx.framed, ctx.inflight, ctx.waiters, filters, reply, ctx.flush_abort).await?;
            Ok(CommandDispatch::Continue)
        }
        ClientCommand::Unsubscribe { filters, reply } => {
            handle_unsubscribe(ctx.framed, ctx.inflight, ctx.waiters, filters, reply, ctx.flush_abort).await?;
            Ok(CommandDispatch::Continue)
        }
    }
}

/// Drains ready outbound PUBLISH commands up to the adaptive I/O batch.
pub(super) async fn drain_ready_publishes<W>(
    ctx: &mut CommandDispatchCtx<'_, W>,
    mut fed_publishes: usize,
) -> Result<Option<ClientCommand>, WriteOutcome>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    // Competing work for outbound coalesce: subscribe waiters / control backlog are less
    // relevant here — use false so publish bursts can fill the adaptive window.
    let batch = effective_io_batch(ctx.receive_maximum, false);
    for _ in 0..MAX_READY_DRAIN {
        // Full parked queue: leave further commands in the bounded channel.
        if fed_publishes >= batch || ctx.publish_waiters.parked_full() {
            break;
        }
        match ctx.command_rx.try_recv() {
            Ok(ClientCommand::Publish {
                message,
                qos,
                retain,
                reply,
            }) => {
                handle_publish(
                    ctx.framed,
                    ctx.inflight,
                    ctx.publish_waiters,
                    PendingOutboundPublish {
                        message,
                        qos,
                        retain,
                        reply,
                    },
                    ctx.publish_limits,
                )
                .await?;
                fed_publishes += 1;
                flush_if_buffer_large(ctx.framed, ctx.flush_abort).await?;
            }
            #[cfg(test)]
            Ok(ClientCommand::Noop) => {}
            #[cfg(test)]
            // Test-only: no inflight pot alloc — not the production packet-ID ownership model.
            Ok(ClientCommand::InsertPublishWaiter { packet_id, reply }) => {
                if !reply.is_closed() {
                    ctx.publish_waiters.insert(packet_id, reply);
                }
            }
            Ok(other) => return Ok(Some(other)),
            Err(mpsc::error::TryRecvError::Empty) => break,
            Err(mpsc::error::TryRecvError::Disconnected) => {
                return Err(WriteOutcome::CommandChannelClosed);
            }
        }
    }
    Ok(None)
}

pub(super) async fn handle_publish<W>(
    framed: &mut FramedWrite<W, V5Codec>,
    inflight: &mut InflightPots,
    publish_waiters: &mut PublishWaiters,
    publish: PendingOutboundPublish,
    limits: BrokerPublishLimits,
) -> Result<(), WriteOutcome>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    if publish.reply.is_closed() {
        return Ok(());
    }
    if publish.qos == QoS::ExactlyOnce {
        let _ = publish
            .reply
            .send(Err(AppError::InvalidRequest("QoS 2 publish is not supported".into())));
        return Ok(());
    }
    if let Err(error) =
        validate_publish_against_broker_limits(publish.qos, publish.retain, limits.max_qos, limits.retain_available)
    {
        let _ = publish.reply.send(Err(error));
        return Ok(());
    }

    if publish.qos == QoS::AtMostOnce {
        let delivery = PublishDelivery {
            dup: false,
            retain: publish.retain,
            qos: publish.qos,
            packet_id: None,
        };
        if let Err(error) = feed_publish(framed, &publish.message, delivery) {
            return finish_publish_encode_error(Some(publish.reply), &publish.message, error);
        }
        let _ = publish.reply.send(Ok(()));
        return Ok(());
    }

    // QoS 1: wait for a free slot in the broker's Receive Maximum window rather than
    // failing the publish. Queue behind already-parked publishes to keep their order;
    // the write loop sends them as PUBACKs free slots.
    if !inflight.has_publish_capacity() || !publish_waiters.parked.is_empty() {
        publish_waiters.parked.push_back(publish);
        return Ok(());
    }

    encode_qos1_publish(framed, inflight, publish_waiters, publish)
}

/// Feeds parked QoS-1 publishes (FIFO) while the publish window has room. Caller flushes.
pub(super) fn send_parked_publishes<W>(
    framed: &mut FramedWrite<W, V5Codec>,
    inflight: &mut InflightPots,
    publish_waiters: &mut PublishWaiters,
) -> Result<(), WriteOutcome>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    while inflight.has_publish_capacity() {
        let Some(publish) = publish_waiters.parked.pop_front() else {
            break;
        };
        // Caller gave up (ack timeout / drop) while parked: never put it on the wire.
        if publish.reply.is_closed() {
            continue;
        }
        encode_qos1_publish(framed, inflight, publish_waiters, publish)?;
    }
    Ok(())
}

/// Allocates a publish packet id, feeds the QoS-1 PUBLISH, and registers its waiter.
fn encode_qos1_publish<W>(
    framed: &mut FramedWrite<W, V5Codec>,
    inflight: &mut InflightPots,
    publish_waiters: &mut PublishWaiters,
    publish: PendingOutboundPublish,
) -> Result<(), WriteOutcome>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    let packet_id = match inflight.alloc_publish() {
        Ok(id) => id,
        Err(error) => {
            let _ = publish.reply.send(Err(error));
            return Ok(());
        }
    };
    let delivery = PublishDelivery {
        dup: false,
        retain: publish.retain,
        qos: publish.qos,
        packet_id: Some(packet_id),
    };
    if let Err(error) = feed_publish(framed, &publish.message, delivery) {
        inflight.release_publish(packet_id);
        return finish_publish_encode_error(Some(publish.reply), &publish.message, error);
    }
    publish_waiters.insert_sent(packet_id, publish);
    Ok(())
}

/// Sends what the previous connection still owed, then flushes. Called once at write-task
/// start, before any new command:
///
/// 1. Unacknowledged QoS-1 PUBLISHes of a resumed session: DUP = 1, original ids, original
///    order. Only as many as the new Receive Maximum allows go out ([MQTT-4.9.0-2]); the rest
///    fail with [`AppError::ConnectionLost`] rather than overrun the broker window.
/// 2. Publishes that were never sent, in their original order, like new ones (closed
///    replies — callers that gave up — are skipped).
pub(super) async fn resend_carried<W>(
    framed: &mut FramedWrite<W, V5Codec>,
    inflight: &mut InflightPots,
    publish_waiters: &mut PublishWaiters,
    limits: BrokerPublishLimits,
    flush_abort: FlushAbort<'_>,
) -> Result<(), WriteOutcome>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    let Some(carry) = publish_waiters.carry.clone() else {
        return Ok(());
    };
    let (mut carried, unsent) = carry
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take_all();
    if carried.is_empty() && unsent.is_empty() {
        return Ok(());
    }
    // Resend in the original send order ([MQTT-4.6.0-1]); ids are reused round-robin,
    // so they do not encode it.
    carried.sort_by_key(|entry| entry.seq);
    for entry in carried {
        if !inflight.has_publish_capacity() || !inflight.reserve_publish(entry.packet_id) {
            let _ = entry.reply.send(Err(AppError::ConnectionLost));
            continue;
        }
        let delivery = PublishDelivery {
            dup: true,
            retain: entry.retain,
            qos: QoS::AtLeastOnce,
            packet_id: Some(entry.packet_id),
        };
        if let Err(error) = feed_publish(framed, &entry.message, delivery) {
            inflight.release_publish(entry.packet_id);
            finish_publish_encode_error(Some(entry.reply), &entry.message, error)?;
            continue;
        }
        publish_waiters.insert_sent(
            entry.packet_id,
            PendingOutboundPublish {
                message: entry.message,
                qos: QoS::AtLeastOnce,
                retain: entry.retain,
                reply: entry.reply,
            },
        );
    }
    for publish in unsent {
        handle_publish(framed, inflight, publish_waiters, publish, limits).await?;
    }
    flush_writes(framed, flush_abort).await
}

pub(super) async fn handle_subscribe<W>(
    framed: &mut FramedWrite<W, V5Codec>,
    inflight: &mut InflightPots,
    waiters: &mut SubUnsubWaiters,
    filters: Vec<(ByteString, SubscriptionOptions)>,
    reply: SubscribeReply,
    flush_abort: FlushAbort<'_>,
) -> Result<(), WriteOutcome>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    if reply.is_closed() {
        return Ok(());
    }
    if let Err(error) = validate_subscribe_request(&filters) {
        let _ = reply.send(Err(error));
        return Ok(());
    }

    let packet_id = match inflight.alloc_subscribe() {
        Ok(id) => id,
        Err(error) => {
            let _ = reply.send(Err(error));
            return Ok(());
        }
    };

    waiters.subscribe.insert(
        packet_id,
        PendingSubscribe {
            reply,
            filters: filters.clone(),
        },
    );

    let packet = Packet::Subscribe(Subscribe {
        packet_id,
        id: None,
        user_properties: Vec::new(),
        topic_filters: filters,
    });

    if let Err(outcome) = send_packet(framed, packet, flush_abort).await {
        if let Some(pending) = waiters.subscribe.remove(&packet_id) {
            inflight.release_subscribe(packet_id);
            let _ = pending.reply.send(Err(app_error_from_write_outcome(&outcome)));
        }
        return Err(outcome);
    }

    Ok(())
}

/// Validates subscribe filters before SUBSCRIBE encode
/// ([MQTT-3.8.3-4], [MQTT-4.7.1-1/2], [MQTT-4.7.3-1/2], [MQTT-4.8.2-1/2]).
pub(super) fn validate_subscribe_request(filters: &[(ByteString, SubscriptionOptions)]) -> Result<(), AppError> {
    if filters.is_empty() {
        return Err(AppError::InvalidRequest(
            "subscribe requires at least one filter".into(),
        ));
    }
    for (filter, options) in filters {
        if filter.is_empty() {
            return Err(AppError::InvalidRequest(
                "topic filter must be at least one character".into(),
            ));
        }
        if topic_filter_bytes_invalid(filter.as_bytes()) {
            return Err(AppError::InvalidRequest(
                "topic filter does not meet MQTT topic filter rules".into(),
            ));
        }
        // The broker delivers at most the granted subscription QoS; inbound QoS 2 is not
        // supported and would kill the session on the first matching message.
        if options.qos == QoS::ExactlyOnce {
            return Err(AppError::InvalidRequest(
                "QoS 2 subscriptions are not supported (use QoS 0 or 1)".into(),
            ));
        }
        if is_shared_subscription_filter(filter) {
            validate_shared_subscription_filter(filter)?;
            if options.no_local {
                return Err(AppError::InvalidRequest(
                    "no_local must not be set on shared subscription filters".into(),
                ));
            }
        }
    }
    Ok(())
}

/// Validates unsubscribe filters before UNSUBSCRIBE encode
/// ([MQTT-4.7.1-1/2], [MQTT-4.7.3-1/2]).
pub(super) fn validate_unsubscribe_request(filters: &[ByteString]) -> Result<(), AppError> {
    if filters.is_empty() {
        return Err(AppError::InvalidRequest(
            "unsubscribe requires at least one filter".into(),
        ));
    }
    for filter in filters {
        if filter.is_empty() {
            return Err(AppError::InvalidRequest(
                "topic filter must be at least one character".into(),
            ));
        }
        if topic_filter_bytes_invalid(filter.as_bytes()) {
            return Err(AppError::InvalidRequest(
                "topic filter does not meet MQTT topic filter rules".into(),
            ));
        }
    }
    Ok(())
}

/// Parses `$share/{ShareName}/{filter}` and validates ShareName + non-empty filter rest.
pub(super) fn validate_shared_subscription_filter(filter: &str) -> Result<(), AppError> {
    const PREFIX: &str = "$share/";
    let rest = filter
        .strip_prefix(PREFIX)
        .ok_or_else(|| AppError::InvalidRequest("shared filter must start with $share/".into()))?;
    let (share_name, topic) = rest
        .split_once('/')
        .ok_or_else(|| AppError::InvalidRequest("shared filter must be $share/{ShareName}/{TopicFilter}".into()))?;
    validate_share_name(share_name).map_err(|error| AppError::InvalidRequest(error.to_string()))?;
    if topic.is_empty() {
        return Err(AppError::InvalidRequest(
            "shared filter topic segment must not be empty".into(),
        ));
    }
    Ok(())
}

pub(super) async fn handle_unsubscribe<W>(
    framed: &mut FramedWrite<W, V5Codec>,
    inflight: &mut InflightPots,
    waiters: &mut SubUnsubWaiters,
    filters: Vec<ByteString>,
    reply: UnsubscribeReply,
    flush_abort: FlushAbort<'_>,
) -> Result<(), WriteOutcome>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    if reply.is_closed() {
        return Ok(());
    }
    if let Err(error) = validate_unsubscribe_request(&filters) {
        let _ = reply.send(Err(error));
        return Ok(());
    }

    let packet_id = match inflight.alloc_subscribe() {
        Ok(id) => id,
        Err(error) => {
            let _ = reply.send(Err(error));
            return Ok(());
        }
    };

    waiters.unsubscribe.insert(
        packet_id,
        PendingUnsubscribe {
            reply,
            filters: filters.clone(),
        },
    );

    let packet = Packet::Unsubscribe(Unsubscribe {
        packet_id,
        user_properties: Vec::new(),
        topic_filters: filters,
    });

    if let Err(outcome) = send_packet(framed, packet, flush_abort).await {
        if let Some(pending) = waiters.unsubscribe.remove(&packet_id) {
            inflight.release_subscribe(packet_id);
            let _ = pending.reply.send(Err(app_error_from_write_outcome(&outcome)));
        }
        return Err(outcome);
    }

    Ok(())
}

/// Replies to the publisher: app mistakes → `InvalidRequest` + keep session; I/O → fatal.
///
/// `reply` may be `None` when the caller already dropped the oneshot (QoS 1); packet-id
/// release is the caller's responsibility before invoking this helper.
pub(super) fn finish_publish_encode_error(
    reply: Option<PublishReply>,
    message: &PublishMessage,
    error: EncodeError,
) -> Result<(), WriteOutcome> {
    match error {
        EncodeError::MalformedPacket => {
            let detail =
                publish_topic_invalid_reason(message.topic.as_ref()).unwrap_or("invalid publish packet fields");
            if let Some(reply) = reply {
                let _ = reply.send(Err(AppError::InvalidRequest(detail.into())));
            }
            Ok(())
        }
        EncodeError::PacketIdRequired => {
            if let Some(reply) = reply {
                let _ = reply.send(Err(AppError::InvalidRequest(
                    "publish packet identifier is required for QoS > 0".into(),
                )));
            }
            Ok(())
        }
        EncodeError::OverMaxPacketSize { size, max } => {
            if let Some(reply) = reply {
                let _ = reply.send(Err(AppError::InvalidRequest(format!(
                    "publish exceeds maximum packet size ({size} > {max})"
                ))));
            }
            Ok(())
        }
        other => {
            let outcome = map_encode_error(other);
            if let Some(reply) = reply {
                let _ = reply.send(Err(app_error_from_write_outcome(&outcome)));
            }
            Err(outcome)
        }
    }
}

/// Specific InvalidRequest text when [`EncodeError::MalformedPacket`] is due to Topic Name rules.
pub(super) fn publish_topic_invalid_reason(topic: &str) -> Option<&'static str> {
    if topic.is_empty() {
        Some("publish topic must not be empty")
    } else if topic.as_bytes().contains(&0) {
        Some("publish topic must not contain U+0000")
    } else if topic.as_bytes().iter().any(|&b| matches!(b, b'+' | b'#')) {
        Some("publish topic must not contain wildcard characters")
    } else {
        None
    }
}
