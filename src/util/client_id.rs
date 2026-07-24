//! Client ID generation (`prefix` + UUID).

use std::fmt;
use uuid::Uuid;

/// Unique MQTT client identifier (`prefix` + hyphen + UUID v4 simple hex).
///
/// The UUID v4 suffix provides 122 bits of cryptographic randomness, making
/// collisions negligible across many concurrent connections.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ClientId(String);

impl ClientId {
    /// Builds a new client ID from `prefix` and a freshly generated UUID v4 suffix.
    ///
    /// The result has the form `{prefix}-{32 hex chars}` (no hyphens in the UUID).
    pub fn from_prefix(prefix: &str) -> Self {
        Self(format!("{prefix}-{}", Uuid::new_v4().simple()))
    }

    /// Returns the wire-format client ID string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ClientId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl AsRef<str> for ClientId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Client IDs must start with the configured prefix and be unique across calls.
    #[test]
    fn from_prefix_contains_prefix_and_is_unique() {
        let a = ClientId::from_prefix("demo");
        let b = ClientId::from_prefix("demo");
        assert!(
            a.as_str().starts_with("demo-"),
            "expected 'demo-' prefix, got: {}",
            a.as_str()
        );
        assert_eq!(a.as_str().len(), 4 + 1 + 32, "prefix + hyphen + 32 hex chars");
        let suffix = a.as_str().strip_prefix("demo-").expect("prefix already asserted");
        assert!(
            suffix.len() == 32 && suffix.chars().all(|c| c.is_ascii_hexdigit()),
            "expected 32 hex chars after prefix, got: {suffix}"
        );
        assert_ne!(a, b, "client IDs must be unique");
    }
}
