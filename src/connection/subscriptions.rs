//! Per-connection subscription registry and Clean Start resubscribe.

use std::collections::HashMap;
use std::num::{NonZeroU16, NonZeroU32};
use std::sync::RwLock;
use std::time::Duration;

use bytestring::ByteString;
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::watch;
use tokio::time::timeout;
use tokio_util::codec::{FramedRead, FramedWrite};
use tokio_util::sync::CancellationToken;

use super::packet_id::InflightPots;
use crate::ack::RefusedSubscription;
use crate::codec::{
    DecodedFrame, Packet, Subscribe, SubscribeAck, SubscribeAckReason, SubscriptionOptions, UnsubscribeAckReason,
    V5Codec, encode_publish_ack,
};
use crate::connection::session_policy::inbound_publish_policy;
use crate::error::{AppError, ConnectionOutcome, ProtocolError, ShutdownError, TransportError};
use crate::inbound_sink::InboundSink;

/// Active topic filters + options remembered across reconnects (lives on [`super::Connection`]).
#[derive(Debug)]
pub(crate) struct SubscriptionRegistry {
    /// Filter → options, the Subscription Identifier it was subscribed with, and whether the
    /// broker refused to restore it.
    entries: HashMap<ByteString, Entry>,
    /// Behind [`crate::MqttClient::refused_subscriptions`]; republished on every change.
    refused_tx: watch::Sender<Vec<RefusedSubscription>>,
    /// Numbers the SUBSCRIBEs whose grants are recorded, in order.
    next_batch: u64,
}

#[derive(Debug)]
struct Entry {
    options: SubscriptionOptions,
    id: Option<NonZeroU32>,
    /// The SUBSCRIBE that granted it: restored together with the filters it came with.
    batch: u64,
    /// Last refusal while restoring it; retried until granted or unsubscribed.
    refused: Option<(SubscribeAckReason, Option<ByteString>)>,
}

impl Default for SubscriptionRegistry {
    fn default() -> Self {
        Self::new(watch::channel(Vec::new()).0)
    }
}

/// Filters granted by one SUBSCRIBE (so sharing its Subscription Identifier): re-issued as
/// one SUBSCRIBE again, which the broker took once already. Brokers cap the filters per
/// SUBSCRIBE (AWS IoT Core at 8), so the whole registry in one packet could be refused.
pub(crate) type ResubscribeGroup = (Option<NonZeroU32>, Vec<(ByteString, SubscriptionOptions)>);

impl SubscriptionRegistry {
    pub(crate) fn new(refused_tx: watch::Sender<Vec<RefusedSubscription>>) -> Self {
        Self {
            entries: HashMap::new(),
            refused_tx,
            next_batch: 0,
        }
    }

    /// Registers or replaces filters granted by the broker (no Subscription Identifier).
    #[cfg(test)]
    pub(crate) fn upsert(&mut self, filters: impl IntoIterator<Item = (ByteString, SubscriptionOptions)>) {
        self.upsert_with_id(filters, None);
    }

    /// Registers or replaces filters granted by one SUBSCRIBE with `id`.
    pub(crate) fn upsert_with_id(
        &mut self,
        filters: impl IntoIterator<Item = (ByteString, SubscriptionOptions)>,
        id: Option<NonZeroU32>,
    ) {
        let batch = self.next_batch;
        self.next_batch += 1;
        for (filter, options) in filters {
            self.entries.insert(
                filter,
                Entry {
                    options,
                    id,
                    batch,
                    refused: None,
                },
            );
        }
        self.publish_refused();
    }

    /// Removes filters after a successful UNSUBSCRIBE.
    pub(crate) fn remove(&mut self, filters: impl IntoIterator<Item = ByteString>) {
        for filter in filters {
            self.entries.remove(&filter);
        }
        self.publish_refused();
    }

    /// Flat snapshot (filter + options), for tests.
    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> Vec<(ByteString, SubscriptionOptions)> {
        self.entries
            .iter()
            .map(|(filter, entry)| (filter.clone(), entry.options))
            .collect()
    }

    /// Snapshot grouped by the SUBSCRIBE that granted them, in the order they were granted.
    pub(crate) fn snapshot_grouped(&self) -> Vec<ResubscribeGroup> {
        self.grouped(|_| true)
    }

    /// Like [`Self::snapshot_grouped`], only the filters the broker refused to restore.
    pub(crate) fn refused_grouped(&self) -> Vec<ResubscribeGroup> {
        self.grouped(|entry| entry.refused.is_some())
    }

    pub(crate) fn has_refused(&self) -> bool {
        self.entries.values().any(|entry| entry.refused.is_some())
    }

    fn grouped(&self, keep: impl Fn(&Entry) -> bool) -> Vec<ResubscribeGroup> {
        let mut batches: std::collections::BTreeMap<u64, ResubscribeGroup> = std::collections::BTreeMap::new();
        for (filter, entry) in self.entries.iter().filter(|(_, entry)| keep(entry)) {
            batches
                .entry(entry.batch)
                .or_insert_with(|| (entry.id, Vec::new()))
                .1
                .push((filter.clone(), entry.options));
        }
        batches.into_values().collect()
    }

    /// Records the broker's answer to restoring `filters`: granted ones are active again,
    /// refused ones stay registered and are retried.
    ///
    /// `retry` (a SUBSCRIBE sent while the application could subscribe and unsubscribe too)
    /// only touches filters still waiting for a retry: one the application unsubscribed, or
    /// subscribed again with its own answer, is left as the application made it.
    pub(crate) fn apply_restore_ack(
        &mut self,
        filters: &[(ByteString, SubscriptionOptions)],
        reasons: &[SubscribeAckReason],
        reason_string: Option<&ByteString>,
        retry: bool,
    ) {
        for ((filter, _), reason) in filters.iter().zip(reasons) {
            let Some(entry) = self.entries.get_mut(filter) else {
                continue;
            };
            if retry && entry.refused.is_none() {
                continue;
            }
            if subscribe_reason_granted(*reason) {
                if entry.refused.take().is_some() {
                    tracing::info!(filter = %filter, "broker granted the refused subscription");
                }
            } else {
                tracing::warn!(
                    filter = %filter,
                    reason = ?reason,
                    reason_string = reason_string.map(|text| &**text),
                    "broker refused to restore a subscription; retrying on this connection"
                );
                entry.refused = Some((*reason, reason_string.cloned()));
            }
        }
        self.publish_refused();
    }

    fn publish_refused(&self) {
        let mut refused: Vec<RefusedSubscription> = self
            .entries
            .iter()
            .filter_map(|(filter, entry)| {
                let (reason, reason_string) = entry.refused.as_ref()?;
                Some(RefusedSubscription {
                    filter: filter.clone(),
                    reason: *reason,
                    reason_string: reason_string.clone(),
                })
            })
            .collect();
        refused.sort_by(|a, b| a.filter.cmp(&b.filter));
        self.refused_tx.send_if_modified(|current| {
            let changed = *current != refused;
            if changed {
                *current = refused;
            }
            changed
        });
    }
}

/// First wait before asking again for refused subscriptions; doubles per attempt.
const RESTORE_RETRY_FIRST: Duration = if cfg!(test) {
    Duration::from_millis(50)
} else {
    Duration::from_secs(5)
};
/// Longest wait between two attempts.
const RESTORE_RETRY_MAX: Duration = if cfg!(test) {
    Duration::from_millis(400)
} else {
    Duration::from_secs(300)
};

/// Wait before retry `attempt` (0-based): doubling from [`RESTORE_RETRY_FIRST`] up to
/// [`RESTORE_RETRY_MAX`], of which a random half, so clients refused at the same moment (a
/// broker that drops every client of a changed role) do not keep retrying in lockstep.
pub(crate) fn restore_retry_delay(attempt: u32) -> Duration {
    let full = RESTORE_RETRY_FIRST
        .saturating_mul(1 << attempt.min(16))
        .min(RESTORE_RETRY_MAX);
    let random = uuid::Uuid::new_v4().as_u64_pair().0 as f64 / u64::MAX as f64;
    full.mul_f64(0.5 + random / 2.0)
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
) -> Result<Vec<ResubscribeGroup>, ConnectionOutcome> {
    registry
        .read()
        .map(|guard| guard.snapshot_grouped())
        .map_err(|_| ConnectionOutcome::Application(AppError::Internal("subscription registry lock poisoned".into())))
}

/// Applies per-filter SUBACK results to the registry (app-initiated subscribe only).
pub(crate) fn apply_subscribe_ack_to_registry(
    registry: &mut SubscriptionRegistry,
    filters: &[(ByteString, SubscriptionOptions)],
    id: Option<NonZeroU32>,
    reasons: &[SubscribeAckReason],
) {
    let granted: Vec<_> = filters
        .iter()
        .zip(reasons)
        .filter(|(_, reason)| subscribe_reason_granted(**reason))
        .map(|(filter, _)| filter.clone())
        .collect();
    if !granted.is_empty() {
        registry.upsert_with_id(granted, id);
    }
}

/// Removes the filters the broker no longer holds: unsubscribed, or never subscribed in its
/// session (No subscription existed, an UNSUBSCRIBE repeated or sent after a broker restart).
/// A refused UNSUBSCRIBE leaves the broker's subscription, and the filter stays.
pub(crate) fn apply_unsubscribe_ack_to_registry(
    registry: &mut SubscriptionRegistry,
    filters: &[ByteString],
    reasons: &[UnsubscribeAckReason],
) {
    let to_remove: Vec<_> = filters
        .iter()
        .zip(reasons.iter())
        .filter(|(_, reason)| {
            matches!(
                reason,
                UnsubscribeAckReason::Success | UnsubscribeAckReason::NoSubscriptionExisted
            )
        })
        .map(|(filter, _)| filter.clone())
        .collect();
    registry.remove(to_remove);
}

/// SUBSCRIBEs a resubscribe keeps waiting for their SUBACK at once.
const RESUBSCRIBE_WINDOW: usize = 10;

/// Re-issues prior SUBSCRIBE filters on a fresh session before read/write tasks start.
///
/// One SUBSCRIBE per earlier one, up to [`RESUBSCRIBE_WINDOW`] unanswered at a time.
/// `cancel` must abort the wait. Inbound PUBLISH during the wait is delivered to `handoff`
/// (enqueue-before-ack PUBACK on the wire for QoS 1). A filter the broker refuses stays in
/// `registry` as refused, and the write task asks for it again; only a SUBACK that does not
/// answer the request (wrong reason count, none within `deadline` of the last one) ends the
/// connection.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn resubscribe_pending<H: InboundSink, R, W>(
    framed_read: &mut FramedRead<R, V5Codec>,
    framed_write: &mut FramedWrite<W, V5Codec>,
    inflight: &mut InflightPots,
    registry: &RwLock<SubscriptionRegistry>,
    groups: Vec<ResubscribeGroup>,
    deadline: Duration,
    cancel: &CancellationToken,
    mut inbound: ResubscribeInbound<'_, H>,
) -> Result<(), ConnectionOutcome>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    if groups.iter().any(|(id, _)| id.is_some()) && !inbound.subscription_identifiers_available {
        // A broker that stopped offering identifiers must not get one (§3.2.2.3.12);
        // the subscription itself still matters more than the routing hint.
        tracing::warn!("broker no longer supports Subscription Identifiers; restoring without");
    }
    let mut groups = groups.into_iter();
    let mut waiting: HashMap<NonZeroU16, Vec<(ByteString, SubscriptionOptions)>> = HashMap::new();
    let result = async {
        loop {
            while waiting.len() < RESUBSCRIBE_WINDOW
                && let Some((id, filters)) = groups.next()
            {
                let packet_id = inflight.alloc_subscribe().map_err(|_| {
                    ConnectionOutcome::Protocol(ProtocolError::Violation("resubscribe packet id exhausted".into()))
                })?;
                waiting.insert(packet_id, filters.clone());
                let packet = Packet::Subscribe(Subscribe {
                    packet_id,
                    id: id.filter(|_| inbound.subscription_identifiers_available),
                    user_properties: Vec::new(),
                    topic_filters: filters,
                });
                framed_write.send(packet).await.map_err(map_resubscribe_send_error)?;
            }
            if waiting.is_empty() {
                return Ok(());
            }
            let ack = tokio::select! {
                () = cancel.cancelled() => return Err(ConnectionOutcome::Shutdown(ShutdownError::Cancelled)),
                result = timeout(deadline, read_suback(framed_read, framed_write, &waiting, &mut inbound)) => {
                    result.map_err(|_elapsed| {
                        ConnectionOutcome::Protocol(ProtocolError::Violation("resubscribe SUBACK timeout".into()))
                    })??
                }
            };
            let filters = waiting
                .remove(&ack.packet_id)
                .expect("read_suback returns a waiting id");
            inflight.release_subscribe(ack.packet_id);
            validate_resubscribe_ack(&ack, filters.len())?;
            registry
                .write()
                .map_err(|_| {
                    ConnectionOutcome::Application(AppError::Internal("subscription registry lock poisoned".into()))
                })?
                .apply_restore_ack(&filters, &ack.status, ack.reason_string.as_ref(), false);
        }
    }
    .await;
    for packet_id in waiting.into_keys() {
        inflight.release_subscribe(packet_id);
    }
    result
}

/// Where PUBLISHes that arrive during the resubscribe wait go: the application sink, after
/// resolving Topic Aliases with the connection's alias state.
pub(crate) struct ResubscribeInbound<'a, H> {
    pub(crate) handoff: &'a H,
    /// Counters behind [`crate::MqttClient::stats`].
    pub(crate) stats: &'a crate::stats::Stats,
    pub(crate) topic_aliases: &'a mut super::topic_alias::InboundTopicAliases,
    /// CONNACK Subscription Identifiers Available.
    pub(crate) subscription_identifiers_available: bool,
    /// Request/response routing (responses bypass the inbound sink).
    pub(crate) response_router: &'a crate::request::ResponseRouter,
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

/// Whether the broker's Session Present flag is consistent with local session state.
///
/// - [MQTT-3.2.2-2]: Clean Start = 1 requires Session Present = 0. A `1` is a broker bug;
///   reconnecting with Clean Start again will not help, so the driver must stop.
/// - [MQTT-3.2.2-4]: a client with no Session State must close when Session Present = 1.
///   A client that connects with Clean Start = 0 always holds Session State: whatever it has
///   in memory, which after a process restart is empty. A QoS 0/1 client's state is only its
///   unacknowledged outbound publishes, so an empty one is valid, and resuming the broker's
///   session (its subscriptions and queued messages) is what Clean Start = 0 asks for.
pub(crate) fn session_resume_consistent(clean_start: bool, session_present: bool) -> bool {
    !(clean_start && session_present)
}

/// Rejects an auto-resubscribe SUBACK that does not answer every filter.
///
/// [MQTT-3.8.4-6]: the SUBACK must carry one reason code per sent filter; an empty or
/// short status list is a protocol violation, not an implicit success. A refusal is an
/// answer: [`SubscriptionRegistry::apply_restore_ack`] keeps the filter for a retry.
fn validate_resubscribe_ack(ack: &SubscribeAck, expected_filters: usize) -> Result<(), ConnectionOutcome> {
    if ack.status.len() == expected_filters {
        Ok(())
    } else {
        Err(ConnectionOutcome::Protocol(ProtocolError::Violation(
            "auto-resubscribe SUBACK reason count did not match filters".into(),
        )))
    }
}

/// Reads until a SUBACK for one of the `waiting` SUBSCRIBEs arrives.
async fn read_suback<H: InboundSink, R, W, V>(
    framed_read: &mut FramedRead<R, V5Codec>,
    framed_write: &mut FramedWrite<W, V5Codec>,
    waiting: &HashMap<NonZeroU16, V>,
    inbound: &mut ResubscribeInbound<'_, H>,
) -> Result<SubscribeAck, ConnectionOutcome>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    loop {
        match framed_read.next().await {
            Some(Ok((DecodedFrame::Control(Packet::SubscribeAck(ack)), _))) if waiting.contains_key(&ack.packet_id) => {
                return Ok(ack);
            }
            Some(Ok((DecodedFrame::Control(Packet::SubscribeAck(_)), _))) => {}
            Some(Ok((DecodedFrame::Publish(packet), _))) => {
                deliver_publish_during_resubscribe(framed_write, inbound, packet).await?;
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
    inbound: &mut ResubscribeInbound<'_, H>,
    mut packet: crate::codec::PublishPacket,
) -> Result<(), ConnectionOutcome>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    inbound
        .topic_aliases
        .resolve(&mut packet)
        .map_err(ConnectionOutcome::Protocol)?;
    let puback_id = inbound_publish_policy(&packet).map_err(ConnectionOutcome::Protocol)?;
    inbound.stats.publish_received(packet.meta.qos, packet.meta.dup);
    if let Some(message) = inbound
        .response_router
        .route(crate::codec::InboundMessage::from(packet))
        && inbound.handoff.send(message).await.is_err()
    {
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
            None,
            &[SubscribeAckReason::GrantedQos1, SubscribeAckReason::NotAuthorized],
        );
        let snap = registry.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(&*snap[0].0, "ok");
    }

    /// Only a filter the broker still holds survives an UNSUBACK: one it had no subscription
    /// for is gone too, or the next reconnect would subscribe to it again.
    #[test]
    fn unsubscribe_ack_keeps_only_filters_the_broker_still_holds() {
        let mut registry = SubscriptionRegistry::default();
        let filters = ["gone", "never", "refused"].map(ByteString::from_static);
        registry.upsert(filters.clone().map(|filter| (filter, SubscriptionOptions::default())));
        apply_unsubscribe_ack_to_registry(
            &mut registry,
            &filters,
            &[
                UnsubscribeAckReason::Success,
                UnsubscribeAckReason::NoSubscriptionExisted,
                UnsubscribeAckReason::NotAuthorized,
            ],
        );
        let snap = registry.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(&*snap[0].0, "refused");
    }

    /// A refused restore keeps the filter (it is retried) and lists it; a later grant clears it.
    #[test]
    fn restore_refusal_is_kept_listed_and_cleared_by_a_grant() {
        let (tx, rx) = watch::channel(Vec::new());
        let mut registry = SubscriptionRegistry::new(tx);
        let opts = SubscriptionOptions::default();
        let filters = [
            (ByteString::from_static("ok"), opts),
            (ByteString::from_static("denied"), opts),
        ];
        registry.upsert(filters.clone());

        let reason_string = ByteString::from_static("ACL");
        registry.apply_restore_ack(
            &filters,
            &[SubscribeAckReason::GrantedQos0, SubscribeAckReason::NotAuthorized],
            Some(&reason_string),
            false,
        );
        assert_eq!(registry.snapshot().len(), 2, "a refused filter stays registered");
        assert_eq!(
            *rx.borrow(),
            vec![RefusedSubscription {
                filter: "denied".into(),
                reason: SubscribeAckReason::NotAuthorized,
                reason_string: Some(reason_string),
            }]
        );
        let retry = registry.refused_grouped();
        assert_eq!(retry.len(), 1);
        assert_eq!(&*retry[0].1[0].0, "denied", "only the refused filter is retried");

        registry.apply_restore_ack(&filters[1..], &[SubscribeAckReason::GrantedQos0], None, true);
        assert!(rx.borrow().is_empty());
        assert!(!registry.has_refused());
    }

    /// A retry answer must not undo what the application did meanwhile: an unsubscribed
    /// filter stays gone, a re-subscribed (granted) one stays active.
    #[test]
    fn restore_retry_ack_leaves_filters_the_application_changed() {
        let (tx, rx) = watch::channel(Vec::new());
        let mut registry = SubscriptionRegistry::new(tx);
        let opts = SubscriptionOptions::default();
        let filters = [
            (ByteString::from_static("gone"), opts),
            (ByteString::from_static("again"), opts),
        ];
        registry.upsert(filters.clone());
        let refused = [SubscribeAckReason::NotAuthorized, SubscribeAckReason::NotAuthorized];
        registry.apply_restore_ack(&filters, &refused, None, false);
        assert_eq!(rx.borrow().len(), 2);

        registry.remove([ByteString::from_static("gone")]);
        registry.upsert([(ByteString::from_static("again"), opts)]);
        assert!(
            rx.borrow().is_empty(),
            "unsubscribe and a granted subscribe clear the list"
        );

        registry.apply_restore_ack(&filters, &refused, None, true);
        assert!(rx.borrow().is_empty(), "the late retry answer changes nothing");
        assert_eq!(registry.snapshot().len(), 1, "the unsubscribed filter is not re-added");
    }

    /// Doubling from the first delay up to the cap, each wait between half and all of it.
    #[test]
    fn restore_retry_delay_doubles_up_to_the_cap() {
        for attempt in 0..40 {
            let full = RESTORE_RETRY_FIRST
                .saturating_mul(1 << attempt.min(16))
                .min(RESTORE_RETRY_MAX);
            let delay = restore_retry_delay(attempt);
            assert!(
                delay >= full / 2 && delay <= full,
                "attempt {attempt}: {delay:?} of {full:?}"
            );
        }
        assert!(restore_retry_delay(40) >= RESTORE_RETRY_MAX / 2);
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
    /// ([MQTT-3.8.4-6]); empty or short status lists are protocol violations.
    #[rstest]
    #[case::all_granted(vec![SubscribeAckReason::GrantedQos1], 1, true)]
    #[case::empty_status(vec![], 1, false)]
    #[case::short_status(vec![SubscribeAckReason::GrantedQos0], 2, false)]
    #[case::extra_reasons(vec![SubscribeAckReason::GrantedQos0, SubscribeAckReason::GrantedQos1], 1, false)]
    #[case::not_authorized(vec![SubscribeAckReason::NotAuthorized], 1, true)]
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

    /// Session Present consistency: Clean Start forbids Present=1; Clean Start 0 accepts both.
    #[rstest]
    #[case::clean_start_rejects_present(true, true, false)]
    #[case::clean_start_ok(true, false, true)]
    #[case::resume(false, true, true)]
    #[case::no_session_on_broker(false, false, true)]
    fn session_resume_consistency(#[case] clean_start: bool, #[case] session_present: bool, #[case] expected: bool) {
        assert_eq!(session_resume_consistent(clean_start, session_present), expected);
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
