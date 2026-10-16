//! Shared-subscription topic helper (`$share/{group}/{topic}`).

use thiserror::Error;

/// ShareName validation failure ([MQTT-4.8.2-1], [MQTT-4.8.2-2]).
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum ShareNameError {
    /// ShareName must contain at least one character.
    #[error("share name must not be empty")]
    Empty,
    /// ShareName must not contain `/`, `+`, or `#`.
    #[error("share name must not contain '/', '+', or '#'")]
    InvalidCharacter,
}

/// Returns true when `filter` is a shared-subscription topic filter (`$share/...`).
pub fn is_shared_subscription_filter(filter: &str) -> bool {
    filter.starts_with("$share/")
}

/// Validates a ShareName segment for [`shared_filter`] ([MQTT-4.8.2-1], [MQTT-4.8.2-2]).
pub fn validate_share_name(name: &str) -> Result<(), ShareNameError> {
    if name.is_empty() {
        return Err(ShareNameError::Empty);
    }
    if name.as_bytes().iter().any(|&b| matches!(b, b'/' | b'+' | b'#')) {
        return Err(ShareNameError::InvalidCharacter);
    }
    Ok(())
}

/// Builds an MQTT 5 shared-subscription filter string.
///
/// Format: `$share/{group}/{topic}`.
/// Callers pass the result as a normal topic filter to [`crate::MqttClient::subscribe`].
///
/// # Errors
///
/// Returns [`ShareNameError`] when `group` is not a valid ShareName ([MQTT-4.8.2-1/2]).
///
/// # Examples
///
/// ```
/// use mqtt_glide::shared_filter;
///
/// assert_eq!(
///     shared_filter("sensors", "system/lifecycle/connected/+/+").expect("valid"),
///     "$share/sensors/system/lifecycle/connected/+/+"
/// );
/// assert!(shared_filter("a/b", "t").is_err());
/// ```
pub fn shared_filter(group: &str, topic: &str) -> Result<String, ShareNameError> {
    validate_share_name(group)?;
    Ok(format!("$share/{group}/{topic}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    /// `$share/{group}/{topic}`, with the topic's wildcards kept as they are.
    #[rstest]
    #[case::lifecycle(
        "my-group",
        "system/lifecycle/connected/+/+",
        "$share/my-group/system/lifecycle/connected/+/+"
    )]
    #[case::wildcards("g", "a/+/b/#", "$share/g/a/+/b/#")]
    #[case::single_level("sensors", "a/+/b", "$share/sensors/a/+/b")]
    fn shared_filter_prepends_share_prefix(#[case] group: &str, #[case] topic: &str, #[case] expected: &str) {
        assert_eq!(shared_filter(group, topic).expect("valid"), expected);
    }

    /// [MQTT-4.8.2-1] A ShareName is at least one character long.
    #[test]
    fn validate_share_name_rejects_empty() {
        assert_eq!(validate_share_name(""), Err(ShareNameError::Empty));
    }

    /// [MQTT-4.8.2-2] A ShareName never contains `/`, `+` or `#`.
    #[rstest]
    #[case::slash("a/b")]
    #[case::plus("a+b")]
    #[case::hash("a#b")]
    #[case::leading_slash("/x")]
    #[case::leading_plus("+g")]
    #[case::leading_hash("#g")]
    fn validate_share_name_rejects_slash_plus_hash(#[case] bad: &str) {
        assert_eq!(
            validate_share_name(bad),
            Err(ShareNameError::InvalidCharacter),
            "expected reject for {bad:?}"
        );
    }

    #[test]
    fn validate_share_name_accepts_alphanumeric() {
        assert!(validate_share_name("sensors").is_ok());
        assert!(validate_share_name("my-group").is_ok());
    }

    #[test]
    fn shared_filter_rejects_invalid_share_name() {
        assert!(shared_filter("a/b", "t").is_err());
    }

    #[test]
    fn is_shared_subscription_filter_detects_prefix() {
        assert!(is_shared_subscription_filter("$share/g/t"));
        assert!(!is_shared_subscription_filter("t/1"));
    }
}
