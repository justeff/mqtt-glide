//! Keep-alive PINGREQ timer for the write task.

use std::time::Duration;

/// Schedules automatic PINGREQ at the negotiated keep-alive interval.
///
/// The timer lives on the write task so probes continue while read is blocked on a full handoff.
pub(super) fn keep_alive_ping_timer(keep_alive: Duration) -> Option<tokio::time::Interval> {
    if keep_alive.is_zero() {
        return None;
    }
    let start = tokio::time::Instant::now() + keep_alive;
    let mut timer = tokio::time::interval_at(start, keep_alive);
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    Some(timer)
}
