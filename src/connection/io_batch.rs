//! Adaptive I/O coalesce sizing (read burst / inbound PUBACK flush).
//!
//! Scales with the negotiated Receive Maximum window and shrinks when competing
//! work is pending (fairness). Derived from session parameters — not a
//! bench-specific knob.

use std::num::NonZeroU16;

/// Upper bound on frames/acks coalesced before yielding back to `select!`.
///
/// Ceiling for the adaptive `receive_maximum / 8` formula — raised so high
/// `receive_maximum` sessions (e.g. 1024) can coalesce more than 32 frames per
/// wakeup before the fairness/soft-flush paths run.
pub(crate) const MAX_IO_BATCH: usize = 64;

/// Cap when outbound commands (or other competing work) are waiting.
///
/// Keeps control/keepalive responsive under publish load while still allowing
/// a meaningful coalesce when the adaptive batch would otherwise be huge.
const FAIRNESS_CAP: usize = 16;

/// Safety cap on `try_recv` drain loops in the write task.
///
/// Must stay ≥ [`MAX_IO_BATCH`] (the adaptive batch breaks the loop first).
/// Not a throughput knob — only prevents unbounded spin if the channel stays hot.
pub(crate) const MAX_READY_DRAIN: usize = {
    const TWICE: usize = MAX_IO_BATCH * 2;
    if TWICE > 128 { TWICE } else { 128 }
};

/// Computes how many frames/PUBACKs to coalesce before flushing / yielding.
///
/// - Scales with [`ConnectOptions::receive_maximum`](crate::ConnectOptions::receive_maximum)
///   so delayed inbound PUBACKs stay a small fraction of the MQTT receive window.
/// - When `competing_work` is set (e.g. application commands queued), the batch is capped
///   so keep-alive / outbound publishes are not starved.
pub(crate) fn effective_io_batch(receive_maximum: NonZeroU16, competing_work: bool) -> usize {
    let window = usize::from(receive_maximum.get());
    // ~1/8 of the inbound QoS1 window, clamped — keeps broker flow-control responsive
    // without one TCP write per PUBACK under load.
    let mut adaptive = (window / 8).clamp(1, MAX_IO_BATCH);
    if competing_work {
        adaptive = adaptive.min(FAIRNESS_CAP);
    }
    adaptive
}

#[cfg(test)]
mod tests {
    use super::*;
    use nonzero_ext::nonzero;

    #[test]
    fn scales_with_receive_maximum() {
        assert_eq!(effective_io_batch(nonzero!(8u16), false), 1);
        assert_eq!(effective_io_batch(nonzero!(64u16), false), 8);
        assert_eq!(effective_io_batch(nonzero!(1024u16), false), 64); // 1024/8=128 → MAX
    }

    #[test]
    fn fairness_caps_when_competing_work() {
        assert_eq!(effective_io_batch(nonzero!(1024u16), true), FAIRNESS_CAP);
        assert_eq!(effective_io_batch(nonzero!(8u16), true), 1); // already below fairness cap
    }

    #[test]
    fn ready_drain_tracks_max_io_batch_floor() {
        // max(128, MAX_IO_BATCH * 2) with MAX_IO_BATCH = 64.
        assert_eq!(MAX_READY_DRAIN, 128);
        const { assert!(MAX_READY_DRAIN >= MAX_IO_BATCH) };
    }
}
