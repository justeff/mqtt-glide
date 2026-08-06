//! Connection lifecycle state and generation binding.

/// Coordinator lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// A connect attempt is in progress.
    Connecting,
    /// Session is active after a successful connect.
    Connected,
    /// Graceful shutdown in progress.
    Draining,
    /// Waiting before the next reconnect attempt.
    Backoff,
    /// Driver stopped; no further reconnect.
    Stopped,
}

/// Monotonic connection generation — incremented on each successful connect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Generation(pub u64);

impl Generation {
    /// Returns the next generation value.
    pub fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }

    /// Returns whether `observed` matches the active generation.
    ///
    /// Frames, ACKs, and waiters from a stale generation are discarded.
    pub fn accepts(self, observed: Self) -> bool {
        self == observed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generation_increments() {
        assert_eq!(Generation(0).next(), Generation(1));
        assert_eq!(Generation(1).next(), Generation(2));
    }
}
