//! Reconnect-delay helpers built on [`ReconnectPolicy`](crate::ReconnectPolicy).

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use crate::client::ReconnectPolicy;
use crate::error::ConnectionOutcome;

/// Consecutive auth failures before switching to the max auth backoff delay.
pub const DEFAULT_AUTH_FAILURE_THRESHOLD: u32 = 10;

/// Computes the back-off delay after an authentication failure.
///
/// Returns `reconnect_delay` for the first [`DEFAULT_AUTH_FAILURE_THRESHOLD`] - 1
/// failures (when using the default threshold), then switches to `max_auth_backoff`.
fn auth_failure_delay(
    consecutive_failures: u32,
    threshold: u32,
    reconnect_delay: Duration,
    max_auth_backoff: Duration,
) -> Duration {
    if consecutive_failures >= threshold {
        max_auth_backoff
    } else {
        reconnect_delay
    }
}

#[derive(Debug)]
struct StandardReconnectPolicyInner {
    reconnect_delay: Duration,
    max_auth_backoff: Duration,
    auth_failure_threshold: u32,
    consecutive_auth_failures: AtomicU32,
    /// When [`StandardReconnectPolicy::mark_live`] was called; consumed on the next delay.
    live_since: Mutex<Option<Instant>>,
}

impl StandardReconnectPolicyInner {
    fn take_live_since(&self) -> Option<Instant> {
        self.live_since.lock().unwrap_or_else(PoisonError::into_inner).take()
    }
}

/// Fixed reconnect delay, one immediate transport retry after a live session, and
/// capped auth backoff.
///
/// Cheap to [`Clone`]: clones share the same atomic state via [`Arc`], so one
/// instance can be passed to [`connect`](crate::connect) while the app retains a
/// handle for [`mark_live`](Self::mark_live) and subscribe-auth recording.
///
/// Use [`Self::new`] for defaults, or [`Self::with_auth_failure_threshold`] to
/// override the auth-streak switchover.
///
/// Apps that need different behaviour implement [`ReconnectPolicy`](crate::ReconnectPolicy)
/// directly.
///
/// # Auth streak vs credential invalidation
///
/// This policy **only** returns delays. Token / password invalidation is owned by
/// [`CredentialsProvider::invalidate`](crate::CredentialsProvider::invalidate),
/// which the driver invokes on CONNECT-level
/// [`ConnectionOutcome::Auth`].
///
/// Subscription-level `NotAuthorized` (ACL mismatch) is not a connection outcome.
/// Apps that treat ACL failures like auth for backoff can call
/// [`Self::record_subscribe_auth_failure`] without invalidating
/// credentials.
///
/// # Immediate retry after a live session
///
/// Call [`Self::mark_live`] when the session is fully ready
/// (apps often do this after SUBACK confirms subscriptions). The next
/// [`ConnectionOutcome::Transport`] then returns `Duration::ZERO` once, if the session
/// stayed up for at least the reconnect delay; subsequent transport failures, and a session
/// that dropped sooner, use the configured reconnect delay.
/// Protocol outcomes (e.g. broker DISCONNECT) always use the fixed delay.
#[derive(Debug, Clone)]
pub struct StandardReconnectPolicy {
    inner: Arc<StandardReconnectPolicyInner>,
}

impl StandardReconnectPolicy {
    /// Builds a policy with the given reconnect delay and auth backoff cap.
    ///
    /// Uses [`DEFAULT_AUTH_FAILURE_THRESHOLD`] for the auth-streak switchover.
    pub fn new(reconnect_delay: Duration, max_auth_backoff: Duration) -> Self {
        Self::with_auth_failure_threshold(reconnect_delay, max_auth_backoff, DEFAULT_AUTH_FAILURE_THRESHOLD)
    }

    /// Like [`Self::new`], but with an explicit consecutive-auth threshold.
    pub fn with_auth_failure_threshold(
        reconnect_delay: Duration,
        max_auth_backoff: Duration,
        auth_failure_threshold: u32,
    ) -> Self {
        Self {
            inner: Arc::new(StandardReconnectPolicyInner {
                reconnect_delay,
                max_auth_backoff,
                auth_failure_threshold,
                consecutive_auth_failures: AtomicU32::new(0),
                live_since: Mutex::new(None),
            }),
        }
    }

    /// Marks the session fully operational.
    ///
    /// Resets the consecutive auth-failure streak and arms a single immediate
    /// transport reconnect. Apps often call this after SUBACK success — not on
    /// CONNACK alone — so health / readiness match a fully subscribed session.
    ///
    /// The reconnect is immediate only if the session then lasted at least the reconnect
    /// delay: a broker that drops every connection right after accepting it is not dialled
    /// in a loop, however often this is called.
    pub fn mark_live(&self) {
        self.inner.consecutive_auth_failures.store(0, Ordering::Relaxed);
        *self.inner.live_since.lock().unwrap_or_else(PoisonError::into_inner) = Some(Instant::now());
    }

    /// [`Self::mark_live`] for a session that went live `ago`.
    #[cfg(test)]
    fn mark_live_ago(&self, ago: Duration) {
        self.mark_live();
        *self.inner.live_since.lock().unwrap_or_else(PoisonError::into_inner) = Some(Instant::now() - ago);
    }

    /// Returns the current consecutive auth-failure count (for metrics / logging).
    pub fn consecutive_auth_failures(&self) -> u32 {
        self.inner.consecutive_auth_failures.load(Ordering::Relaxed)
    }

    /// Returns the consecutive-auth threshold used for degraded backoff.
    pub fn auth_failure_threshold(&self) -> u32 {
        self.inner.auth_failure_threshold
    }

    /// Records a subscription-level `NotAuthorized` (ACL) rejection.
    ///
    /// Shares the consecutive-auth streak with CONNECT Auth for degraded-mode
    /// backoff, but **does not** invalidate credentials (CONNECT succeeded).
    pub fn record_subscribe_auth_failure(&self) -> Duration {
        self.inner.take_live_since();
        let failures = self.inner.consecutive_auth_failures.fetch_add(1, Ordering::Relaxed) + 1;
        auth_failure_delay(
            failures,
            self.inner.auth_failure_threshold,
            self.inner.reconnect_delay,
            self.inner.max_auth_backoff,
        )
    }
}

impl ReconnectPolicy for StandardReconnectPolicy {
    fn delay_after(&self, outcome: &ConnectionOutcome) -> Option<Duration> {
        match outcome {
            ConnectionOutcome::Shutdown(_) | ConnectionOutcome::Application(_) => None,
            ConnectionOutcome::Auth(_) => {
                self.inner.take_live_since();
                let failures = self.inner.consecutive_auth_failures.fetch_add(1, Ordering::Relaxed) + 1;
                Some(auth_failure_delay(
                    failures,
                    self.inner.auth_failure_threshold,
                    self.inner.reconnect_delay,
                    self.inner.max_auth_backoff,
                ))
            }
            ConnectionOutcome::Transport(_) => {
                let live_since = self.inner.take_live_since();
                if live_since.is_some_and(|since| since.elapsed() >= self.inner.reconnect_delay) {
                    Some(Duration::ZERO)
                } else {
                    Some(self.inner.reconnect_delay)
                }
            }
            ConnectionOutcome::Protocol(_) => {
                self.inner.take_live_since();
                Some(self.inner.reconnect_delay)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{AppError, AuthError, ProtocolError, ShutdownError, TransportError};
    use rstest::rstest;

    fn policy() -> StandardReconnectPolicy {
        StandardReconnectPolicy::new(Duration::from_secs(5), Duration::from_secs(600))
    }

    fn transport() -> ConnectionOutcome {
        ConnectionOutcome::Transport(TransportError::Connection("reset".into()))
    }

    fn protocol() -> ConnectionOutcome {
        ConnectionOutcome::Protocol(ProtocolError::Violation("disconnect".into()))
    }

    fn auth() -> ConnectionOutcome {
        ConnectionOutcome::Auth(AuthError::NotAuthorized)
    }

    // ── auth_failure_delay ───────────────────────────────────────────────────

    #[rstest]
    #[case(0, 10, 5, 600, 5)]
    #[case(1, 10, 5, 600, 5)]
    #[case(9, 10, 5, 600, 5)]
    #[case(10, 10, 5, 600, 600)]
    #[case(11, 10, 5, 600, 600)]
    #[case::custom_cap(10, 10, 5, 120, 120)]
    #[case::cap_equals_delay(10, 10, 5, 5, 5)]
    #[case::custom_threshold(3, 3, 5, 600, 600)]
    fn auth_failure_delay_cases(
        #[case] failures: u32,
        #[case] threshold: u32,
        #[case] reconnect_secs: u64,
        #[case] max_backoff_secs: u64,
        #[case] expected_secs: u64,
    ) {
        assert_eq!(
            auth_failure_delay(
                failures,
                threshold,
                Duration::from_secs(reconnect_secs),
                Duration::from_secs(max_backoff_secs),
            ),
            Duration::from_secs(expected_secs)
        );
    }

    // ── delay matrix ─────────────────────────────────────────────────────────

    #[test]
    fn shutdown_and_app_stop_reconnect() {
        let p = policy();
        assert_eq!(
            p.delay_after(&ConnectionOutcome::Shutdown(ShutdownError::Requested)),
            None
        );
        assert_eq!(
            p.delay_after(&ConnectionOutcome::Application(AppError::InboundSinkClosed)),
            None
        );
    }

    #[test]
    fn transport_before_live_uses_reconnect_delay() {
        let p = policy();
        assert_eq!(p.delay_after(&transport()), Some(Duration::from_secs(5)));
        assert_eq!(p.delay_after(&transport()), Some(Duration::from_secs(5)));
    }

    #[test]
    fn transport_after_live_is_immediate_once() {
        let p = policy();
        p.mark_live_ago(Duration::from_secs(5));
        assert_eq!(p.delay_after(&transport()), Some(Duration::ZERO));
        assert_eq!(p.delay_after(&transport()), Some(Duration::from_secs(5)));
    }

    /// A broker that drops every session right after it went live gets the reconnect delay,
    /// however often the application marks the session live.
    #[test]
    fn transport_soon_after_live_uses_reconnect_delay() {
        let p = policy();
        for _ in 0..3 {
            p.mark_live_ago(Duration::from_secs(4));
            assert_eq!(p.delay_after(&transport()), Some(Duration::from_secs(5)));
        }
    }

    #[test]
    fn protocol_always_uses_reconnect_delay_even_after_live() {
        let p = policy();
        p.mark_live();
        assert_eq!(p.delay_after(&protocol()), Some(Duration::from_secs(5)));
        // Live flag cleared — next transport is not immediate.
        assert_eq!(p.delay_after(&transport()), Some(Duration::from_secs(5)));
    }

    #[test]
    fn auth_uses_reconnect_delay_until_cap() {
        let p = policy();
        for i in 1..DEFAULT_AUTH_FAILURE_THRESHOLD {
            assert_eq!(
                p.delay_after(&auth()),
                Some(Duration::from_secs(5)),
                "failure {i} should use reconnect_delay"
            );
        }
        assert_eq!(p.consecutive_auth_failures(), DEFAULT_AUTH_FAILURE_THRESHOLD - 1);

        assert_eq!(
            p.delay_after(&auth()),
            Some(Duration::from_secs(600)),
            "at cap should use max_auth_backoff"
        );
        assert_eq!(p.consecutive_auth_failures(), DEFAULT_AUTH_FAILURE_THRESHOLD);
        assert_eq!(p.delay_after(&auth()), Some(Duration::from_secs(600)));
    }

    #[test]
    fn mark_live_resets_auth_streak() {
        let p = policy();
        for _ in 0..3 {
            let _ = p.delay_after(&auth());
        }
        assert_eq!(p.consecutive_auth_failures(), 3);

        p.mark_live();
        assert_eq!(p.consecutive_auth_failures(), 0);
        assert_eq!(p.delay_after(&auth()), Some(Duration::from_secs(5)));
        assert_eq!(p.consecutive_auth_failures(), 1);
    }

    #[test]
    fn auth_after_live_does_not_use_immediate_zero() {
        let p = policy();
        p.mark_live();
        assert_eq!(p.delay_after(&auth()), Some(Duration::from_secs(5)));
        // Auth cleared the live flag.
        assert_eq!(p.delay_after(&transport()), Some(Duration::from_secs(5)));
    }

    #[test]
    fn clone_shares_auth_streak_and_live_flag() {
        let p = StandardReconnectPolicy::new(Duration::from_secs(3), Duration::from_secs(90));
        let clone = p.clone();
        assert_eq!(clone.delay_after(&transport()), Some(Duration::from_secs(3)));

        for _ in 0..DEFAULT_AUTH_FAILURE_THRESHOLD {
            let _ = p.delay_after(&auth());
        }
        assert_eq!(clone.consecutive_auth_failures(), DEFAULT_AUTH_FAILURE_THRESHOLD);
        assert_eq!(clone.delay_after(&auth()), Some(Duration::from_secs(90)));

        p.mark_live_ago(Duration::from_secs(3));
        assert_eq!(clone.consecutive_auth_failures(), 0);
        assert_eq!(clone.delay_after(&transport()), Some(Duration::ZERO));
    }

    #[test]
    fn record_subscribe_auth_failure_shares_streak_without_being_outcome_auth() {
        let p = policy();
        assert_eq!(p.record_subscribe_auth_failure(), Duration::from_secs(5));
        assert_eq!(p.consecutive_auth_failures(), 1);
        p.mark_live();
        assert_eq!(p.consecutive_auth_failures(), 0);
    }

    #[test]
    fn record_subscribe_auth_failure_escalates_at_threshold() {
        let p = policy();
        for i in 1..DEFAULT_AUTH_FAILURE_THRESHOLD {
            assert_eq!(
                p.record_subscribe_auth_failure(),
                Duration::from_secs(5),
                "subscribe auth failure {i} should use reconnect_delay"
            );
        }
        assert_eq!(
            p.record_subscribe_auth_failure(),
            Duration::from_secs(600),
            "at cap subscribe auth should use max_auth_backoff"
        );
        assert_eq!(p.consecutive_auth_failures(), DEFAULT_AUTH_FAILURE_THRESHOLD);
    }

    #[test]
    fn with_auth_failure_threshold_applies() {
        let p =
            StandardReconnectPolicy::with_auth_failure_threshold(Duration::from_secs(5), Duration::from_secs(600), 3);
        assert_eq!(p.delay_after(&auth()), Some(Duration::from_secs(5)));
        assert_eq!(p.delay_after(&auth()), Some(Duration::from_secs(5)));
        assert_eq!(p.delay_after(&auth()), Some(Duration::from_secs(600)));
        assert_eq!(p.auth_failure_threshold(), 3);
    }
}
