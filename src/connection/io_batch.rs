//! I/O coalesce sizing (read burst / inbound PUBACK flush).
//!
//! Fixed, not derived from the Receive Maximum: both loops only take what is already buffered
//! or queued, so a larger batch never delays a frame or a PUBACK. The cap only bounds how long
//! other work (keep-alive, outbound commands) waits behind one burst.

/// Upper bound on frames/acks coalesced before yielding back to `select!`.
pub(crate) const MAX_IO_BATCH: usize = 64;

/// Cap when outbound commands (or other competing work) are waiting.
///
/// Keeps control/keepalive responsive under publish load while still allowing
/// a meaningful coalesce.
const FAIRNESS_CAP: usize = 16;

/// Safety cap on `try_recv` drain loops in the write task.
///
/// Must stay ≥ [`MAX_IO_BATCH`] (the batch breaks the loop first).
/// Not a throughput knob — only prevents unbounded spin if the channel stays hot.
pub(crate) const MAX_READY_DRAIN: usize = {
    const TWICE: usize = MAX_IO_BATCH * 2;
    if TWICE > 128 { TWICE } else { 128 }
};

/// How many ready frames/PUBACKs to handle before flushing / yielding.
///
/// Not sized from the client's Receive Maximum: with a small window that split every
/// burst of ready PUBACKs into many small writes (four per write at the default of 32)
/// without getting any of them out sooner. When `competing_work` is set (e.g. application
/// commands queued), the batch is capped so keep-alive and outbound publishes are not starved.
pub(crate) fn io_batch(competing_work: bool) -> usize {
    if competing_work { FAIRNESS_CAP } else { MAX_IO_BATCH }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_batch_without_competing_work() {
        assert_eq!(io_batch(false), MAX_IO_BATCH);
    }

    #[test]
    fn fairness_caps_when_competing_work() {
        assert_eq!(io_batch(true), FAIRNESS_CAP);
    }

    #[test]
    fn ready_drain_tracks_max_io_batch_floor() {
        // max(128, MAX_IO_BATCH * 2) with MAX_IO_BATCH = 64.
        assert_eq!(MAX_READY_DRAIN, 128);
        const { assert!(MAX_READY_DRAIN >= MAX_IO_BATCH) };
    }
}
