//! MQTT Client Identifier (`ClientId`).
//!
//! Prefer [`ClientId::try_new`] for IDs you persist across process restarts.
//! [`ClientId::from_prefix`] mints a fresh random ID (default builder path).

use std::fmt;
use std::str::FromStr;

use thiserror::Error;
use uuid::Uuid;

/// Maximum Client Identifier length in UTF-8 bytes ([MQTT-1.5.4], u16 length prefix).
const MAX_CLIENT_ID_BYTES: usize = u16::MAX as usize;

/// Unique MQTT client identifier sent in CONNECT.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ClientId(String);

/// Invalid Client Identifier for CONNECT ([MQTT-1.5.4], [MQTT-3.1.3]).
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum ClientIdError {
    /// Empty Client Identifier. Use `ConnectOptions::client_id(None)` (or leave unset) when
    /// the broker should assign an ID under `clean_start = true` ([MQTT-3.1.3-6]).
    #[error("client ID must not be empty")]
    Empty,
    /// Client Identifier longer than 65535 UTF-8 bytes (MQTT UTF-8 string limit).
    #[error("client ID must be at most {MAX_CLIENT_ID_BYTES} UTF-8 bytes")]
    TooLong,
    /// MQTT UTF-8 strings MUST NOT encode U+0000 ([MQTT-1.5.4-2]).
    #[error("client ID must not contain U+0000")]
    ContainsNul,
}

impl ClientId {
    /// Builds a client ID from a caller-chosen string (stable across process restarts).
    ///
    /// Validates MQTT UTF-8 string rules for CONNECT Client Identifier: non-empty,
    /// at most 65535 UTF-8 bytes, no U+0000.
    ///
    /// # Errors
    ///
    /// Returns [`ClientIdError`] when `id` is empty, too long, or contains U+0000.
    ///
    /// # Examples
    ///
    /// ```
    /// use mqtt_glide::ClientId;
    ///
    /// let id = ClientId::try_new("sensors-edge-01").expect("valid");
    /// assert_eq!(id.as_str(), "sensors-edge-01");
    /// assert!(ClientId::try_new("").is_err());
    /// ```
    pub fn try_new(id: impl AsRef<str>) -> Result<Self, ClientIdError> {
        let id = id.as_ref();
        if id.is_empty() {
            return Err(ClientIdError::Empty);
        }
        if id.len() > MAX_CLIENT_ID_BYTES {
            return Err(ClientIdError::TooLong);
        }
        if id.as_bytes().contains(&0) {
            return Err(ClientIdError::ContainsNul);
        }
        Ok(Self(id.to_owned()))
    }

    /// Builds a new client ID from `prefix` and a freshly generated UUID v4 suffix.
    ///
    /// The result has the form `{prefix}-{32 hex chars}` (no hyphens in the UUID).
    ///
    /// # Panics
    ///
    /// Panics if `prefix` contains U+0000 or the composed ID exceeds 65535 UTF-8 bytes
    /// (programmer error: prefixes are expected to be short static labels).
    pub fn from_prefix(prefix: &str) -> Self {
        let id = format!("{prefix}-{}", Uuid::new_v4().simple());
        Self::try_new(&id)
            .unwrap_or_else(|err| panic!("ClientId::from_prefix produced an invalid client ID ({err}): {id:?}"))
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

impl FromStr for ClientId {
    type Err = ClientIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_new(s)
    }
}

impl TryFrom<&str> for ClientId {
    type Error = ClientIdError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

impl TryFrom<String> for ClientId {
    type Error = ClientIdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_new(value)
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

    #[test]
    fn try_new_accepts_stable_id() {
        let id = ClientId::try_new("my-service").expect("valid");
        assert_eq!(id.as_str(), "my-service");
        assert_eq!(id.to_string(), "my-service");
    }

    #[test]
    fn try_new_rejects_empty() {
        assert_eq!(ClientId::try_new(""), Err(ClientIdError::Empty));
        assert_eq!("".parse::<ClientId>(), Err(ClientIdError::Empty));
    }

    #[test]
    fn try_new_rejects_nul() {
        assert_eq!(ClientId::try_new("bad\0id"), Err(ClientIdError::ContainsNul));
    }

    #[test]
    fn try_new_rejects_too_long() {
        let too_long = "a".repeat(MAX_CLIENT_ID_BYTES + 1);
        assert_eq!(ClientId::try_new(&too_long), Err(ClientIdError::TooLong));
    }

    #[test]
    fn try_new_accepts_max_length() {
        let max = "a".repeat(MAX_CLIENT_ID_BYTES);
        let id = ClientId::try_new(&max).expect("max length is valid");
        assert_eq!(id.as_str().len(), MAX_CLIENT_ID_BYTES);
    }

    #[test]
    #[should_panic(expected = "invalid client ID")]
    fn from_prefix_panics_on_nul_prefix() {
        let _ = ClientId::from_prefix("bad\0");
    }
}
