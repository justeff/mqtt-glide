//! Per-connection subscription registry and Clean Start resubscribe.

use std::collections::HashMap;
use std::num::NonZeroU16;
use std::sync::RwLock;
use std::time::Duration;

use bytestring::ByteString;
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::timeout;
use tokio_util::codec::{FramedRead, FramedWrite};
use tokio_util::sync::CancellationToken;

use super::packet_id::InflightPots;
use crate::codec::{
    DecodedFrame, Packet, Subscribe, SubscribeAck, SubscribeAckReason, SubscriptionOptions, UnsubscribeAckReason,
    V5Codec, encode_publish_ack,
};
use crate::connection::session_policy::inbound_publish_policy;
use crate::error::{AppError, ConnectionOutcome, ProtocolError, ShutdownError, TransportError};
use crate::inbound_sink::InboundSink;

/// Active topic filters + options remembered across reconnects (lives on [`super::Connection`]).
#[derive(Debug, Default)]
pub(crate) struct SubscriptionRegistry {
    entries: HashMap<ByteString, SubscriptionOptions>,
}

impl SubscriptionRegistry {
    /// Registers or replaces filters granted by the broker.
    pub(crate) fn upsert(&mut self, filters: impl IntoIterator<Item = (ByteString, SubscriptionOptions)>) {
        for (filter, options) in filters {
            self.entries.insert(filter, options);
        }
    }

    /// Removes filters after a successful UNSUBSCRIBE.
    pub(crate) fn remove(&mut self, filters: impl IntoIterator<Item = ByteString>) {
        for filter in filters {
            self.entries.remove(&filter);
        }
    }

    /// Snapshot for Clean Start resubscribe after reconnect.
    pub(crate) fn snapshot(&self) -> Vec<(ByteString, SubscriptionOptions)> {
        self.entries
            .iter()
            .map(|(filter, options)| (filter.clone(), *options))
            .collect()
    }
}

/// Returns whether a SUBACK reason code grants a subscription.
///
/// Connection QoS 2 is out of scope — only QoS 0/1 count as granted for the registry.
pub(crate) fn subscribe_reason_granted(reason: SubscribeAckReason) -> bool {
    matches!(
        reason,
        SubscribeAckReason::GrantedQos0 | SubscribeAckReason::GrantedQos1
    )
}

/// Snapshot filters for Clean Start resubscribe.
///
/// RwLock poison must surface as an error — never as an empty subscription list.
pub(crate) fn snapshot_for_resubscribe(
    registry: &RwLock<SubscriptionRegistry>,
) -> Result<Vec<(ByteString, SubscriptionOptions)>, ConnectionOutcome> {
    registry
        .read()
        .map(|guard| guard.snapshot())
        .map_err(|_| ConnectionOutcome::Application(AppError::Internal("subscription registry lock poisoned".into())))
}

/// Applies per-filter SUBACK results to the registry (app-initiated subscribe only).
pub(crate) fn apply_subscribe_ack_to_registry(
    registry: &mut SubscriptionRegistry,
    filters: &[(ByteString, SubscriptionOptions)],
    reasons: &[SubscribeAckReason],
) {
    for ((filter, options), reason) in filters.iter().zip(reasons.iter()) {
        if subscribe_reason_granted(*reason) {
            registry.upsert([(filter.clone(), *options)]);
        }
    }
}

/// Removes filters that received UNSUBACK Success.
pub(crate) fn apply_unsubscribe_ack_to_registry(
    registry: &mut SubscriptionRegistry,
    filters: &[ByteString],
    reasons: &[UnsubscribeAckReason],
) {
    let to_remove: Vec<_> = filters
        .iter()
        .zip(reasons.iter())
        .filter(|(_, reason)| **reason == UnsubscribeAckReason::Success)
        .map(|(filter, _)| filter.clone())
        .collect();
    registry.remove(to_remove);
}

/// Re-issues prior SUBSCRIBE filters on a fresh session before read/write tasks start.
///
/// `cancel` must abort the wait. Inbound PUBLISH during the wait is delivered to `handoff`
/// (enqueue-before-ack PUBACK on the wire for QoS 1). SUBACK reason codes must all be granted (QoS 0/1).
pub(crate) async fn resubscribe_pending<H: InboundSink, R, W>(
    framed_read: &mut FramedRead<R, V5Codec>,
    framed_write: &mut FramedWrite<W, V5Codec>,
    inflight: &mut InflightPots,
    filters: Vec<(ByteString, SubscriptionOptions)>,
    deadline: Duration,
    cancel: &CancellationToken,
    handoff: &H,
) -> Result<(), ConnectionOutcome>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    if filters.is_empty() {
        return Ok(());
    }

    let packet_id = inflight
        .alloc_subscribe()
        .map_err(|_| ConnectionOutcome::Protocol(ProtocolError::Violation("resubscribe packet id exhausted".into())))?;

    let filter_count = filters.len();
    let packet = Packet::Subscribe(Subscribe {
        packet_id,
        id: None,
        user_properties: Vec::new(),
        topic_filters: filters,
    });

    framed_write.send(packet).await.map_err(map_resubscribe_send_error)?;

    tokio::select! {
        () = cancel.cancelled() => {
            inflight.release_subscribe(packet_id);
            Err(ConnectionOutcome::Shutdown(ShutdownError::Cancelled))
        }
        result = timeout(
            deadline,
            read_suback_for_id(framed_read, framed_write, packet_id, handoff),
        ) => {
            match result {
                Ok(Ok(ack)) => {
                    inflight.release_subscribe(packet_id);
                    validate_resubscribe_ack(&ack, filter_count)
                }
                Ok(Err(outcome)) => {
                    inflight.release_subscribe(packet_id);
                    Err(outcome)
                }
                Err(_elapsed) => {
                    inflight.release_subscribe(packet_id);
                    Err(ConnectionOutcome::Protocol(ProtocolError::Violation(
                        "resubscribe SUBACK timeout".into(),
                    )))
                }
            }
        }
    }
}

/// Whether prior filters must be re-issued on a fresh session.
///
/// Clean Start discards any broker-side session ([MQTT-3.1.2-4] N/A applies), and a broker
/// answering `session_present = false` to a resume attempt proves it has no stored
/// subscriptions — in both cases the app-visible subscriptions only exist again after
/// re-SUBSCRIBE.
pub(crate) fn needs_resubscribe(clean_start: bool, session_present: bool) -> bool {
    clean_start || !session_present
}

/// Rejects auto-resubscribe when the SUBACK does not grant every filter at QoS 0/1.
///
/// [MQTT-3.9.3-1]: reason code count and order must match the filters we sent; an empty
/// or short status list is a protocol violation, not an implicit success.
fn validate_resubscribe_ack(ack: &SubscribeAck, expected_filters: usize) -> Result<(), ConnectionOutcome> {
    if ack.status.len() != expected_filters {
        return Err(ConnectionOutcome::Protocol(ProtocolError::Violation(
            "auto-resubscribe SUBACK reason count did not match filters".into(),
        )));
    }
    if ack.status.iter().all(|reason| subscribe_reason_granted(*reason)) {
        Ok(())
    } else {
        Err(ConnectionOutcome::Protocol(ProtocolError::Violation(
            "auto-resubscribe SUBACK contained non-granted reason".into(),
        )))
    }
}

async fn read_suback_for_id<H: InboundSink, R, W>(
    framed_read: &mut FramedRead<R, V5Codec>,
    framed_write: &mut FramedWrite<W, V5Codec>,
    expected_id: NonZeroU16,
    handoff: &H,
) -> Result<SubscribeAck, ConnectionOutcome>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    loop {
        match framed_read.next().await {
            Some(Ok((DecodedFrame::Control(Packet::SubscribeAck(ack)), _))) if ack.packet_id == expected_id => {
                return Ok(ack);
            }
            Some(Ok((DecodedFrame::Control(Packet::SubscribeAck(_)), _))) => {}
            Some(Ok((DecodedFrame::Publish(packet), _))) => {
                deliver_publish_during_resubscribe(framed_write, handoff, packet).await?;
            }
            Some(Ok((DecodedFrame::Control(other), _))) => {
                return Err(ConnectionOutcome::Protocol(ProtocolError::Violation(format!(
                    "expected SUBACK during resubscribe, got {:?}",
                    other.packet_type()
                ))));
            }
            Some(Err(error)) => {
                return Err(ConnectionOutcome::Protocol(ProtocolError::Decode(error.to_string())));
            }
            None => {
                return Err(ConnectionOutcome::Transport(TransportError::Connection(
                    "connection closed before resubscribe SUBACK".into(),
                )));
            }
        }
    }
}

/// Delivers an inbound PUBLISH that arrives while waiting for auto-resubscribe SUBACK.
///
/// Shares [`inbound_publish_policy`] with the read task (alias / QoS 2 rejection). PUBACK is
/// written inline on the wire because `resubscribe_pending` runs before read/write tasks
/// start — there is no write task to consume `AckInboundPublish`.
async fn deliver_publish_during_resubscribe<H: InboundSink, W>(
    framed_write: &mut FramedWrite<W, V5Codec>,
    handoff: &H,
    packet: crate::codec::PublishPacket,
) -> Result<(), ConnectionOutcome>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    let puback_id = inbound_publish_policy(&packet).map_err(ConnectionOutcome::Protocol)?;
    if handoff.send(packet.message).await.is_err() {
        return Err(ConnectionOutcome::Application(AppError::InboundSinkClosed));
    }
    // PUBACK only after successful handoff accept (enqueue-before-ack).
    if let Some(packet_id) = puback_id {
        let max_out = framed_write.encoder().max_outbound_size();
        encode_publish_ack(packet_id, max_out, framed_write.write_buffer_mut()).map_err(map_resubscribe_send_error)?;
        framed_write.flush().await.map_err(map_resubscribe_send_error)?;
    }
    Ok(())
}

fn map_resubscribe_send_error(error: crate::codec::EncodeError) -> ConnectionOutcome {
    match error {
        crate::codec::EncodeError::Io(message) => {
            ConnectionOutcome::Transport(TransportError::Connection(message.to_string()))
        }
        other => ConnectionOutcome::Protocol(ProtocolError::Violation(other.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use rstest::rstest;

    use super::*;
    use crate::codec::QoS;

    #[test]
    fn registry_upsert_remove_and_snapshot() {
        let mut registry = SubscriptionRegistry::default();
        let opts = SubscriptionOptions {
            qos: QoS::AtLeastOnce,
            ..SubscriptionOptions::default()
        };
        registry.upsert([(ByteString::from_static("a/b"), opts)]);
        let snap = registry.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(&*snap[0].0, "a/b");

        registry.remove([ByteString::from_static("a/b")]);
        assert!(registry.snapshot().is_empty());
    }

    #[test]
    fn subscribe_ack_registry_only_records_granted_filters() {
        let mut registry = SubscriptionRegistry::default();
        let opts = SubscriptionOptions::default();
        apply_subscribe_ack_to_registry(
            &mut registry,
            &[
                (ByteString::from_static("ok"), opts),
                (ByteString::from_static("denied"), opts),
            ],
            &[SubscribeAckReason::GrantedQos1, SubscribeAckReason::NotAuthorized],
        );
        let snap = registry.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(&*snap[0].0, "ok");
    }

    /// Connection does not support QoS 2 — GrantedQos2 must not count as granted.
    #[test]
    fn subscribe_reason_granted_rejects_qos2() {
        assert!(
            subscribe_reason_granted(SubscribeAckReason::GrantedQos0),
            "GrantedQos0 must count as granted"
        );
        assert!(
            subscribe_reason_granted(SubscribeAckReason::GrantedQos1),
            "GrantedQos1 must count as granted"
        );
        assert!(
            !subscribe_reason_granted(SubscribeAckReason::GrantedQos2),
            "GrantedQos2 must not count as granted (no QoS2 Connection)"
        );
        assert!(!subscribe_reason_granted(SubscribeAckReason::NotAuthorized));
    }

    /// [MQTT-4.1.0-1] In-connection subscription state is retained until explicit remove.
    #[test]
    fn session_subscription_state_retained_until_explicit_remove() {
        let mut registry = SubscriptionRegistry::default();
        let opts = SubscriptionOptions {
            qos: QoS::AtLeastOnce,
            ..SubscriptionOptions::default()
        };
        registry.upsert([(ByteString::from_static("active/topic"), opts)]);
        assert_eq!(
            registry.snapshot().len(),
            1,
            "registry must retain subs while connection is open"
        );
        registry.remove([ByteString::from_static("active/topic")]);
        assert!(registry.snapshot().is_empty());
    }

    /// Auto-resubscribe SUBACK must carry one granted reason per sent filter
    /// ([MQTT-3.9.3-1]); empty or short status lists are protocol violations.
    #[rstest]
    #[case::all_granted(vec![SubscribeAckReason::GrantedQos1], 1, true)]
    #[case::empty_status(vec![], 1, false)]
    #[case::short_status(vec![SubscribeAckReason::GrantedQos0], 2, false)]
    #[case::extra_reasons(vec![SubscribeAckReason::GrantedQos0, SubscribeAckReason::GrantedQos1], 1, false)]
    #[case::not_authorized(vec![SubscribeAckReason::NotAuthorized], 1, false)]
    fn resubscribe_ack_validation(
        #[case] status: Vec<SubscribeAckReason>,
        #[case] expected_filters: usize,
        #[case] expect_ok: bool,
    ) {
        use crate::codec::SubscribeAck;
        use std::num::NonZeroU16;

        let ack = SubscribeAck {
            packet_id: NonZeroU16::new(1).unwrap(),
            properties: Vec::new(),
            reason_string: None,
            status,
        };
        assert_eq!(validate_resubscribe_ack(&ack, expected_filters).is_ok(), expect_ok);
    }

    /// `clean_start = false` with `session_present = false` must resubscribe — the broker
    /// proved it holds no session, so skipping the re-SUBSCRIBE would silently lose the
    /// app-visible subscriptions.
    #[rstest]
    #[case::clean_start_discards(true, false, true)]
    #[case::clean_start_even_if_present(true, true, true)]
    #[case::resumed_session_kept(false, true, false)]
    #[case::lost_session_rebuilt(false, false, true)]
    fn resubscribe_decision(#[case] clean_start: bool, #[case] session_present: bool, #[case] expected: bool) {
        assert_eq!(needs_resubscribe(clean_start, session_present), expected);
    }

    /// registry poison must not silently become "no subscriptions".
    #[test]
    fn registry_poison_does_not_yield_empty_snapshot() {
        let registry = RwLock::new(SubscriptionRegistry::default());
        {
            let mut guard = registry.write().expect("write");
            guard.upsert([(
                ByteString::from_static("poison/topic"),
                SubscriptionOptions {
                    qos: QoS::AtLeastOnce,
                    ..SubscriptionOptions::default()
                },
            )]);
        }
        let _ = catch_unwind(AssertUnwindSafe(|| {
            let _guard = registry.write().expect("write for poison");
            panic!("poison registry");
        }));
        assert!(registry.read().is_err(), "precondition: registry lock is poisoned");

        let result = snapshot_for_resubscribe(&registry);
        assert!(
            matches!(
                result,
                Err(ConnectionOutcome::Application(AppError::Internal(ref message)))
                    if message.contains("poisoned")
            ),
            "poisoned registry must surface as AppError::Internal, not TaskJoin/empty; got {result:?}"
        );
    }
}
