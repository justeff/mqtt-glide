//! Retrying subscriptions the broker refused to restore after a reconnect.
//!
//! A refusal (an ACL revoked meanwhile, a quota) is an answer, so the connection stays up
//! and the write task asks again on it, with backoff, until the broker grants the filter or
//! the application unsubscribes. A retry the broker does not answer within the ack timeout
//! ends the connection like any missing acknowledgement.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use tokio::io::AsyncWrite;
use tokio::time::Instant;
use tokio_util::codec::FramedWrite;

use crate::codec::{Packet, Subscribe, V5Codec};
use crate::connection::packet_id::InflightPots;
use crate::connection::subscriptions::{SubscriptionRegistry, fit_to_packet_size, restore_retry_delay};
use crate::error::ProtocolError;

use super::io::{FlushAbort, send_packet};
use super::{PendingSubscribe, SubUnsubWaiters, WriteOutcome};

/// Retry schedule of one connection; starts armed when the resubscribe left refusals.
pub(super) struct RestoreRetry {
    /// Retries sent so far (drives the backoff).
    attempt: u32,
    /// When to ask again; `None` once nothing is refused.
    next_at: Option<Instant>,
    /// When the retries in flight must have been answered.
    deadline: Option<Instant>,
    ack_timeout: Duration,
}

impl RestoreRetry {
    pub(super) fn new(registry: &RwLock<SubscriptionRegistry>, ack_timeout: Duration) -> Self {
        let refused = registry.read().is_ok_and(|registry| registry.has_refused());
        Self {
            attempt: 0,
            next_at: refused.then(|| Instant::now() + restore_retry_delay(0)),
            deadline: None,
            ack_timeout,
        }
    }

    /// When the write task must call [`Self::on_wake`] next.
    pub(super) fn wake_at(&self) -> Option<Instant> {
        match (self.next_at, self.deadline) {
            (Some(next), Some(deadline)) => Some(next.min(deadline)),
            (next, deadline) => next.or(deadline),
        }
    }

    /// Checks the retries in flight and sends the next ones when due.
    pub(super) async fn on_wake<W>(
        &mut self,
        framed: &mut FramedWrite<W, V5Codec>,
        inflight: &mut InflightPots,
        waiters: &mut SubUnsubWaiters,
        registry: &Arc<RwLock<SubscriptionRegistry>>,
        subscription_identifiers_available: bool,
        flush_abort: FlushAbort<'_>,
    ) -> Result<(), WriteOutcome>
    where
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let now = Instant::now();
        let in_flight = waiters.subscribe.values().any(|pending| pending.reply.is_none());
        if !in_flight {
            self.deadline = None;
        } else if self.deadline.is_some_and(|deadline| now >= deadline) {
            return Err(WriteOutcome::Protocol(ProtocolError::Violation(
                "no SUBACK for the retried subscription restore".into(),
            )));
        }
        if self.next_at.is_none_or(|next| now < next) {
            return Ok(());
        }

        let groups = registry
            .read()
            .map(|registry| registry.refused_grouped())
            .unwrap_or_default();
        let groups = fit_to_packet_size(groups, framed.encoder(), subscription_identifiers_available, registry);
        if groups.is_empty() {
            self.next_at = None;
            self.attempt = 0;
            return Ok(());
        }
        self.attempt += 1;
        self.next_at = Some(now + restore_retry_delay(self.attempt));
        if in_flight {
            return Ok(());
        }

        for (id, filters) in groups {
            let Ok(packet_id) = inflight.alloc_subscribe() else {
                break; // every subscribe id taken: the next attempt tries again
            };
            // As on reconnect: the subscription matters more than the routing hint.
            let id = id.filter(|_| subscription_identifiers_available);
            tracing::debug!(filters = filters.len(), "retrying refused subscriptions");
            waiters.subscribe.insert(
                packet_id,
                PendingSubscribe {
                    reply: None,
                    filters: filters.clone(),
                    id,
                },
            );
            let packet = Packet::Subscribe(Subscribe {
                packet_id,
                id,
                user_properties: Vec::new(),
                topic_filters: filters,
            });
            send_packet(framed, packet, flush_abort).await?;
            self.deadline = Some(now + self.ack_timeout);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bytestring::ByteString;
    use nonzero_ext::nonzero;

    use super::*;
    use crate::codec::{SubscribeAckReason, SubscriptionOptions};

    /// A registry whose one filter the broker refused to restore.
    fn refused_registry() -> Arc<RwLock<SubscriptionRegistry>> {
        let filters = vec![(ByteString::from_static("restore/acl"), SubscriptionOptions::default())];
        let mut registry = SubscriptionRegistry::default();
        registry.upsert(filters.clone());
        registry.apply_restore_ack(&filters, &[SubscribeAckReason::NotAuthorized], None, false);
        Arc::new(RwLock::new(registry))
    }

    struct Harness {
        framed: FramedWrite<tokio::io::DuplexStream, V5Codec>,
        _peer: tokio::io::DuplexStream,
        inflight: InflightPots,
        waiters: SubUnsubWaiters,
        registry: Arc<RwLock<SubscriptionRegistry>>,
    }

    impl Harness {
        fn new() -> Self {
            let (task_end, peer) = tokio::io::duplex(4096);
            Self {
                framed: FramedWrite::new(task_end, V5Codec::with_max_packet_size(0)),
                _peer: peer,
                inflight: InflightPots::new(nonzero!(4u16)),
                waiters: SubUnsubWaiters::new(),
                registry: refused_registry(),
            }
        }

        /// Sleeps until `retry` asks to be woken, then wakes it.
        async fn wake(&mut self, retry: &mut RestoreRetry) -> Result<(), WriteOutcome> {
            tokio::time::sleep_until(retry.wake_at().expect("retry armed")).await;
            retry
                .on_wake(
                    &mut self.framed,
                    &mut self.inflight,
                    &mut self.waiters,
                    &self.registry,
                    true,
                    FlushAbort::Graceful,
                )
                .await
        }

        /// The broker refused the retry again: its waiter and Packet Identifier are gone.
        fn refuse_retries(&mut self) {
            for packet_id in self.waiters.subscribe.keys().copied().collect::<Vec<_>>() {
                self.waiters.subscribe.remove(&packet_id);
                self.inflight.release_subscribe(packet_id);
            }
        }
    }

    /// The next attempt can fall due while the last retry still waits for its SUBACK: that is
    /// no reason to give up on it before the ack timeout, nor to ask a second time.
    #[tokio::test]
    async fn retry_in_flight_keeps_until_its_ack_timeout() {
        let mut harness = Harness::new();
        let mut retry = RestoreRetry::new(&harness.registry, Duration::from_secs(30));

        harness.wake(&mut retry).await.expect("first retry");
        assert_eq!(harness.waiters.subscribe.len(), 1, "one retry in flight");

        harness
            .wake(&mut retry)
            .await
            .expect("unanswered before the ack timeout");
        assert_eq!(
            harness.waiters.subscribe.len(),
            1,
            "no second retry while one is in flight"
        );
    }

    /// A refused retry with a short ack timeout leaves a deadline that passes before the next
    /// attempt is due: waking at it must not send that attempt early.
    #[tokio::test]
    async fn answered_retry_waits_for_the_backoff() {
        let mut harness = Harness::new();
        let mut retry = RestoreRetry::new(&harness.registry, Duration::from_millis(1));
        // A later attempt, so the backoff (200–400 ms) dwarfs the ack timeout.
        retry.attempt = 4;

        harness.wake(&mut retry).await.expect("retry");
        assert_eq!(harness.waiters.subscribe.len(), 1);
        let due = retry.next_at.expect("next attempt scheduled");
        harness.refuse_retries();

        harness.wake(&mut retry).await.expect("deadline wake");
        assert!(harness.waiters.subscribe.is_empty(), "nothing sent before the backoff");
        assert_eq!(retry.wake_at(), Some(due), "the next wake is the scheduled attempt");

        harness.wake(&mut retry).await.expect("next attempt");
        assert_eq!(
            harness.waiters.subscribe.len(),
            1,
            "asked again once the backoff passed"
        );
    }
}
