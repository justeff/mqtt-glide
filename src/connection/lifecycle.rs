//! Session phase watch + lifecycle side-effect hook.

use std::sync::Arc;

use crate::error::ConnectionOutcome;

use super::handshake::SessionParams;

/// Current session phase — single type for the connect-time [`tokio::sync::watch`]
/// channel and for [`LifecycleHook`] callbacks.
///
/// Initial watch value is [`SessionPhase::NeverConnected`]. After a session ends,
/// the phase is [`SessionPhase::Disconnected`]. Hook callbacks receive `Connected`
/// or `Disconnected` only — never `NeverConnected`.
///
/// Applications **SHOULD** use the hook **or** the watch for side effects, not both —
/// double-counting is easy when both paths react. Prefer the watch as the source of
/// truth for "are we connected right now?".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionPhase {
    /// Watch initial value / before the first session attempt completes.
    NeverConnected,
    /// Active MQTT session after a successful CONNECT/CONNACK.
    ///
    /// On a reconnect where the broker holds no session (Clean Start, or no Session
    /// Present), every previously granted subscription has already been restored and
    /// granted again when this is published; if the broker refuses one, the connection
    /// fails instead. A resumed session keeps the broker's subscriptions as they are.
    Connected {
        /// Connection generation for this session.
        generation: u64,
        /// CONNACK-derived session parameters (`from_connack` is true after a real broker handshake).
        session: SessionParams,
    },
    /// A session ended with this outcome.
    Disconnected(ConnectionOutcome),
}

impl SessionPhase {
    /// Initial watch value: not yet connected.
    pub const fn initial() -> Self {
        Self::NeverConnected
    }
}

/// Synchronous side effects for lifecycle transitions.
///
/// Invoked on the connection driver task — **MUST** return quickly (no await).
/// Prefer [`SessionPhase`] watch for readiness / health; use this hook for tracing,
/// metrics, or other fire-and-forget reactions.
///
/// [`crate::connect`] always takes a `lifecycle` argument (not `Option`). Pass
/// [`NoopLifecycle`] when you only need the phase watch.
///
/// Callbacks receive [`SessionPhase::Connected`] or [`SessionPhase::Disconnected`] —
/// never [`SessionPhase::NeverConnected`].
pub trait LifecycleHook: Send + Sync {
    /// Called on each Connected / Disconnected transition (same value as the watch).
    fn on_lifecycle(&self, phase: &SessionPhase);
}

/// No-op [`LifecycleHook`] when the caller only observes [`SessionPhase`].
///
/// This is how the hook is "optional": the `connect` parameter is required, but
/// this type performs no side effects.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopLifecycle;

impl LifecycleHook for NoopLifecycle {
    fn on_lifecycle(&self, _phase: &SessionPhase) {}
}

impl<T> LifecycleHook for Arc<T>
where
    T: LifecycleHook + ?Sized,
{
    fn on_lifecycle(&self, phase: &SessionPhase) {
        (**self).on_lifecycle(phase)
    }
}
