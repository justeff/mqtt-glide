//! ACK completion, generation filtering, and waiter failure helpers.

use std::sync::{Arc, RwLock};

use crate::codec::{PublishAck, PublishAckReason, SubscribeAck, UnsubscribeAck};
use crate::connection::Generation;
use crate::connection::packet_id::InflightPots;
use crate::connection::subscriptions::{
    SubscriptionRegistry, apply_subscribe_ack_to_registry, apply_unsubscribe_ack_to_registry,
};
use crate::error::{AppError, ProtocolError};

use super::{CarriedPublish, PublishWaiters, SubUnsubWaiters, WriteOutcome};

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
    let Some(entry) = waiters.by_id.remove(&ack.packet_id) else {
        return;
    };
    inflight.release_publish(ack.packet_id);
    waiters.stats.publish_ack(ack.reason_code);
    let _ = entry.reply.send(publish_ack_to_result(ack));
}

pub(super) fn publish_ack_to_result(ack: PublishAck) -> Result<(), AppError> {
    match ack.reason_code {
        PublishAckReason::Success | PublishAckReason::NoMatchingSubscribers => Ok(()),
        reason => Err(AppError::PublishRejected(Box::new(crate::ack::PublishRejection {
            reason,
            reason_string: ack.reason_string,
            user_properties: ack.properties,
        }))),
    }
}

/// A SUBACK or UNSUBACK must answer every topic filter of its request ([MQTT-3.9.3-2],
/// [MQTT-3.11.3-2]); one that does not leaves the client unable to tell which ones the
/// broker holds, so the connection ends with DISCONNECT 0x82.
fn reason_count_violation(packet: &str, filters: usize, reasons: usize) -> WriteOutcome {
    WriteOutcome::BrokerViolation(ProtocolError::Violation(format!(
        "{packet} carries {reasons} reason codes for {filters} topic filters"
    )))
}

/// Completes an outbound subscribe waiter when `ack_generation` matches the write task generation.
///
/// Stale-generation SUBACKs are ignored (waiter left pending; packet id not released; registry untouched).
/// Unknown / duplicate packet ids (no waiter) are ignored.
/// A closed oneshot (app ACK timeout / cancel) still applies the registry update and frees the id.
/// A SUBACK with the wrong number of reason codes ends the connection.
pub(super) fn complete_subscribe_ack(
    waiters: &mut SubUnsubWaiters,
    inflight: &mut InflightPots,
    subscription_registry: &Arc<RwLock<SubscriptionRegistry>>,
    write_generation: Generation,
    ack_generation: Generation,
    ack: SubscribeAck,
) -> Result<(), WriteOutcome> {
    if !ack_generation_applies(write_generation, ack_generation) {
        return Ok(());
    }
    let packet_id = ack.packet_id;
    let Some(pending) = waiters.subscribe.remove(&packet_id) else {
        // Unknown / duplicate SUBACK (wrong pot or already completed) — ignore.
        return Ok(());
    };
    inflight.release_subscribe(packet_id);
    if ack.status.len() != pending.filters.len() {
        if let Some(reply) = pending.reply {
            let _ = reply.send(Err(AppError::ConnectionLost));
        }
        return Err(reason_count_violation(
            "SUBACK",
            pending.filters.len(),
            ack.status.len(),
        ));
    }
    let Some(reply) = pending.reply else {
        // The driver's retry of a refused restore: no caller, only the registry to update.
        if let Ok(mut registry) = subscription_registry.write() {
            registry.apply_restore_ack(&pending.filters, &ack.status, ack.reason_string.as_ref(), true);
        }
        return Ok(());
    };
    if let Ok(mut registry) = subscription_registry.write() {
        apply_subscribe_ack_to_registry(&mut registry, &pending.filters, pending.id, &ack.status);
    }
    let _ = reply.send(Ok(crate::ack::SubscribeResponse {
        reasons: ack.status,
        reason_string: ack.reason_string,
        user_properties: ack.properties,
    }));
    Ok(())
}

/// Completes an outbound unsubscribe waiter when `ack_generation` matches the write task generation.
///
/// Stale-generation UNSUBACKs are ignored (waiter left pending; packet id not released; registry untouched).
/// Unknown / duplicate packet ids (no waiter) are ignored.
/// A closed oneshot (app timeout / cancel) still applies the registry update and frees the id.
/// An UNSUBACK with the wrong number of reason codes ends the connection.
pub(super) fn complete_unsubscribe_ack(
    waiters: &mut SubUnsubWaiters,
    inflight: &mut InflightPots,
    subscription_registry: &Arc<RwLock<SubscriptionRegistry>>,
    write_generation: Generation,
    ack_generation: Generation,
    ack: UnsubscribeAck,
) -> Result<(), WriteOutcome> {
    if !ack_generation_applies(write_generation, ack_generation) {
        return Ok(());
    }
    let packet_id = ack.packet_id;
    let Some(pending) = waiters.unsubscribe.remove(&packet_id) else {
        // Unknown / duplicate UNSUBACK (wrong pot or already completed) — ignore.
        return Ok(());
    };
    inflight.release_subscribe(packet_id);
    if ack.status.len() != pending.filters.len() {
        let _ = pending.reply.send(Err(AppError::ConnectionLost));
        return Err(reason_count_violation(
            "UNSUBACK",
            pending.filters.len(),
            ack.status.len(),
        ));
    }
    if let Ok(mut registry) = subscription_registry.write() {
        apply_unsubscribe_ack_to_registry(&mut registry, &pending.filters, &ack.status);
    }
    let _ = pending.reply.send(Ok(crate::ack::UnsubscribeResponse {
        reasons: ack.status,
        reason_string: ack.reason_string,
        user_properties: ack.properties,
    }));
    Ok(())
}

pub(super) fn fail_all_waiters(
    waiters: &mut SubUnsubWaiters,
    publish_waiters: &mut PublishWaiters,
    inflight: &mut InflightPots,
    error: &AppError,
) {
    for (id, pending) in waiters.subscribe.drain() {
        inflight.release_subscribe(id);
        if let Some(reply) = pending.reply {
            let _ = reply.send(Err(error.clone()));
        }
    }
    for (id, pending) in waiters.unsubscribe.drain() {
        inflight.release_subscribe(id);
        let _ = pending.reply.send(Err(error.clone()));
    }
    for (id, entry) in publish_waiters.by_id.drain() {
        inflight.release_publish(id);
        let _ = entry.reply.send(Err(error.clone()));
    }
    for parked in publish_waiters.parked.drain(..) {
        let _ = parked.reply.send(Err(error.clone()));
    }
    for carried in publish_waiters.resend_parked.drain(..) {
        let _ = carried.reply.send(Err(error.clone()));
    }
}

/// Fail outstanding waiters with the [`AppError`] class that matches `outcome`.
pub(super) fn fail_waiters_for_outcome(
    waiters: &mut SubUnsubWaiters,
    publish_waiters: &mut PublishWaiters,
    inflight: &mut InflightPots,
    outcome: &WriteOutcome,
) {
    // Connection loss: hand over what is still owed to the next connection. Unacknowledged
    // QoS-1 publishes are session state only when the session is resumable ([MQTT-4.1]);
    // never-sent publishes always go (like ones issued during the reconnect backoff).
    if matches!(
        outcome,
        WriteOutcome::Transport(_)
            | WriteOutcome::Protocol(_)
            | WriteOutcome::BrokerViolation(_)
            | WriteOutcome::Cancelled
    ) && let Some(carry) = publish_waiters.carry.clone()
    {
        let mut slot = carry.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let ids: Vec<_> = publish_waiters
            .by_id
            .iter()
            .filter(|(_, entry)| entry.resend.is_some())
            .map(|(id, _)| *id)
            .collect();
        for id in ids {
            let Some(entry) = publish_waiters.by_id.remove(&id) else {
                continue;
            };
            inflight.release_publish(id);
            let (message, retain) = entry.resend.expect("filtered on resend");
            slot.unacked.push(CarriedPublish {
                packet_id: id,
                message,
                retain,
                reply: entry.reply,
                seq: entry.seq,
            });
        }
        slot.unacked.extend(publish_waiters.resend_parked.drain(..));
        slot.unsent.extend(publish_waiters.parked.drain(..));
    }
    let error = waiter_error_for_outcome(outcome);
    fail_all_waiters(waiters, publish_waiters, inflight, &error);
}

/// Error for requests that were *waiting* (in flight, parked) when the session ended.
///
/// Whether the read or the write side noticed a dying connection first is timing, so a
/// transport or protocol loss is always [`AppError::ConnectionLost`] here: the request may
/// or may not have reached the broker. [`app_error_from_write_outcome`] stays for the one
/// command whose own write failed.
pub(super) fn waiter_error_for_outcome(outcome: &WriteOutcome) -> AppError {
    match outcome {
        WriteOutcome::CommandChannelClosed | WriteOutcome::ControlChannelClosed => AppError::CommandChannelClosed,
        WriteOutcome::Transport(_)
        | WriteOutcome::Protocol(_)
        | WriteOutcome::BrokerViolation(_)
        | WriteOutcome::Cancelled
        | WriteOutcome::Shutdown => AppError::ConnectionLost,
        #[cfg(test)]
        WriteOutcome::StubExit => AppError::CommandChannelClosed,
    }
}

pub(super) fn app_error_from_write_outcome(outcome: &WriteOutcome) -> AppError {
    match outcome {
        WriteOutcome::Transport(error) => AppError::SendFailed(error.to_string()),
        WriteOutcome::Protocol(error) | WriteOutcome::BrokerViolation(error) => AppError::SendFailed(error.to_string()),
        WriteOutcome::CommandChannelClosed | WriteOutcome::ControlChannelClosed => AppError::CommandChannelClosed,
        // Peer loss / graceful protocol close: waiters must not see "channel closed".
        WriteOutcome::Cancelled | WriteOutcome::Shutdown => AppError::ConnectionLost,
        #[cfg(test)]
        WriteOutcome::StubExit => AppError::CommandChannelClosed,
    }
}
