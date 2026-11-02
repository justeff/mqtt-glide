//! Shared helpers (client ID generation, broker URL and proxy parsing, shared filters).

pub mod broker_url;
pub mod client_id;
pub mod proxy;
pub mod shared_filter;
pub mod topic_match;

pub use broker_url::{BrokerUrl, BrokerUrlError};
pub use client_id::{ClientId, ClientIdError};
pub use proxy::HttpProxy;
pub use shared_filter::{ShareNameError, is_shared_subscription_filter, shared_filter, validate_share_name};
pub use topic_match::topic_matches;

use std::time::Duration;

/// `now + timeout`, or a deadline decades away when that does not fit in an `Instant`:
/// `Duration::MAX` is the usual way to ask for no timeout, and a plain `+` panics on it.
pub(crate) fn deadline_after(timeout: Duration) -> tokio::time::Instant {
    extend_deadline(tokio::time::Instant::now(), timeout)
}

/// [`deadline_after`] on the standard clock, for the connect path's deadlines.
pub(crate) fn std_deadline_after(timeout: Duration) -> std::time::Instant {
    let now = std::time::Instant::now();
    now.checked_add(timeout).unwrap_or(now + FAR_FUTURE)
}

/// `deadline + extra`, or a deadline decades away when that overflows.
pub(crate) fn extend_deadline(deadline: tokio::time::Instant, extra: Duration) -> tokio::time::Instant {
    deadline.checked_add(extra).unwrap_or(deadline + FAR_FUTURE)
}

/// About 30 years, as Tokio's own timers use for a deadline that does not fit.
const FAR_FUTURE: Duration = Duration::from_secs(86_400 * 365 * 30);

#[cfg(test)]
mod tests {
    use std::time::Duration;

    #[test]
    fn unbounded_timeouts_give_a_far_deadline_instead_of_panicking() {
        let now = tokio::time::Instant::now();
        assert!(super::deadline_after(Duration::MAX) > now + Duration::from_secs(86_400 * 365));
        assert!(
            super::std_deadline_after(Duration::MAX) > std::time::Instant::now() + Duration::from_secs(86_400 * 365)
        );
        assert!(super::extend_deadline(now, Duration::MAX) > now);
        assert_eq!(
            super::extend_deadline(now, Duration::from_secs(1)),
            now + Duration::from_secs(1)
        );
    }
}
