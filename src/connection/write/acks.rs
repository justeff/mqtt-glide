//! ACK completion, generation filtering, and waiter failure helpers.

use std::sync::{Arc, RwLock};

use crate::codec::{PublishAck, PublishAckReason, SubscribeAck, UnsubscribeAck};
use crate::connection::Generation;
use crate::connection::packet_id::InflightPots;
use crate::connection::subscriptions::{
    SubscriptionRegistry, apply_subscribe_ack_to_registry, apply_unsubscribe_ack_to_registry,
};
use crate::error::AppError;

use super::{PublishWaiters, SubUnsubWaiters, WriteOutcome};

/// Returns whether an ACK bound to `ack_generation` should complete waiters for `current`.
///
/// Shared by PUBACK, SUBACK, and UNSUBACK. Late ACKs from a prior connection generation are
/// discarded.
pub(crate) fn ack_generation_applies(current: Generation, ack_generation: Generation) -> bool {
    current.accepts(ack_generation)
}

/// Completes an outbound publish waiter when `ack_generation` matches the write task generation.
///
/// Stale-generation ACKs are ignored (waiter left pending; packet id not released).
/// Unknown / duplicate packet ids (no waiter) are ignored.
/// A closed oneshot (caller cancelled) still consumes the PUBACK and frees the packet id.
pub(crate) fn complete_publish_ack(
    waiters: &mut PublishWaiters,
    inflight: &mut InflightPots,
    write_generation: Generation,
    ack_generation: Generation,
    ack: PublishAck,
) {
    if !ack_generation_applies(write_generation, ack_generation) {
        return;
    }
    let Some(reply) = waiters.by_id.remove(&ack.packet_id) else {
        return;
    };
    inflight.release_publish(ack.packet_id);
    let _ = reply.send(publish_ack_to_result(ack.reason_code));
}

pub(super) fn publish_ack_to_result(reason: PublishAckReason) -> Result<(), AppError> {
    match reason {
        PublishAckReason::Success | PublishAckReason::NoMatchingSubscribers => Ok(()),
        other => Err(AppError::PublishRejected(format!("{other:?}"))),
    }
}

/// Completes an outbound subscribe waiter when `ack_generation` matches the write task generation.
///
/// Stale-generation SUBACKs are ignored (waiter left pending; packet id not released; registry untouched).
/// Unknown / duplicate packet ids (no waiter) are ignored.
/// A closed oneshot (app ACK timeout / cancel) still applies the registry update and frees the id.
pub(super) fn complete_subscribe_ack(
    waiters: &mut SubUnsubWaiters,
    inflight: &mut InflightPots,
    subscription_registry: &Arc<RwLock<SubscriptionRegistry>>,
    write_generation: Generation,
    ack_generation: Generation,
    ack: SubscribeAck,
) {
    if !ack_generation_applies(write_generation, ack_generation) {
        return;
    }
    let packet_id = ack.packet_id;
    let Some(pending) = waiters.subscribe.remove(&packet_id) else {
        // Unknown / duplicate SUBACK (wrong pot or already completed) — ignore.
        return;
    };
    inflight.release_subscribe(packet_id);
    if let Ok(mut registry) = subscription_registry.write() {
        apply_subscribe_ack_to_registry(&mut registry, &pending.filters, &ack.status);
    }
    let _ = pending.reply.send(Ok(ack.status));
}

/// Completes an outbound unsubscribe waiter when `ack_generation` matches the write task generation.
///
/// Stale-generation UNSUBACKs are ignored (waiter left pending; packet id not released; registry untouched).
/// Unknown / duplicate packet ids (no waiter) are ignored.
/// A closed oneshot (app timeout / cancel) still applies the registry update and frees the id.
pub(super) fn complete_unsubscribe_ack(
    waiters: &mut SubUnsubWaiters,
    inflight: &mut InflightPots,
    subscription_registry: &Arc<RwLock<SubscriptionRegistry>>,
    write_generation: Generation,
    ack_generation: Generation,
    ack: UnsubscribeAck,
) {
    if !ack_generation_applies(write_generation, ack_generation) {
        return;
    }
    let packet_id = ack.packet_id;
    let Some(pending) = waiters.unsubscribe.remove(&packet_id) else {
        // Unknown / duplicate UNSUBACK (wrong pot or already completed) — ignore.
        return;
    };
    inflight.release_subscribe(packet_id);
    if let Ok(mut registry) = subscription_registry.write() {
        apply_unsubscribe_ack_to_registry(&mut registry, &pending.filters, &ack.status);
    }
    let _ = pending.reply.send(Ok(ack.status));
}

pub(super) fn fail_all_waiters(
    waiters: &mut SubUnsubWaiters,
    publish_waiters: &mut PublishWaiters,
    inflight: &mut InflightPots,
    error: &AppError,
) {
    for (id, pending) in waiters.subscribe.drain() {
        inflight.release_subscribe(id);
        let _ = pending.reply.send(Err(error.clone()));
    }
    for (id, pending) in waiters.unsubscribe.drain() {
        inflight.release_subscribe(id);
        let _ = pending.reply.send(Err(error.clone()));
    }
    for (id, reply) in publish_waiters.by_id.drain() {
        inflight.release_publish(id);
        let _ = reply.send(Err(error.clone()));
    }
}

/// Fail outstanding waiters with the [`AppError`] class that matches `outcome`.
pub(super) fn fail_waiters_for_outcome(
    waiters: &mut SubUnsubWaiters,
    publish_waiters: &mut PublishWaiters,
    inflight: &mut InflightPots,
    outcome: &WriteOutcome,
) {
    let error = app_error_from_write_outcome(outcome);
    fail_all_waiters(waiters, publish_waiters, inflight, &error);
}

pub(super) fn app_error_from_write_outcome(outcome: &WriteOutcome) -> AppError {
    match outcome {
        WriteOutcome::Transport(error) => AppError::SendFailed(error.to_string()),
        WriteOutcome::Protocol(error) => AppError::SendFailed(error.to_string()),
        WriteOutcome::CommandChannelClosed | WriteOutcome::ControlChannelClosed => AppError::CommandChannelClosed,
        // Peer loss / graceful protocol close: waiters must not see "channel closed".
        WriteOutcome::Cancelled | WriteOutcome::Shutdown => AppError::ConnectionLost,
        #[cfg(test)]
        WriteOutcome::StubExit => AppError::CommandChannelClosed,
    }
}
