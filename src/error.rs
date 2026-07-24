//! Typed client errors (transport, protocol, auth, shutdown).
//!
//! Error classes: each category maps to a distinct reconnect or
//! shutdown reaction in the connection driver ([`crate::connection`]).

use std::fmt;
use std::time::Duration;

use thiserror::Error;

/// Authentication failure (CONNACK NotAuthorized, credentials provider error, etc.).
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AuthError {
    /// Broker rejected the supplied username or password.
    ///
    /// Triggers [`crate::client::CredentialsProvider::invalidate`] on connect and active session end.
    #[error("broker rejected credentials")]
    NotAuthorized,
    /// Credentials could not be obtained from the provider (transient upstream failure).
    ///
    /// Does **not** invalidate cached credentials — reconnect may retry after auth backoff.
    #[error("credentials provider failed: {0}")]
    Provider(String),
}

/// Transport-level failure (TCP, TLS, DNS).
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TransportError {
    /// The TCP connection could not be established or was reset.
    #[error("connection failed: {0}")]
    Connection(String),
    /// TLS handshake or certificate validation failed.
    #[error("tls error: {0}")]
    Tls(String),
}

/// MQTT protocol violation or unexpected control packet.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ProtocolError {
    /// A received frame could not be decoded.
    #[error("decode error: {0}")]
    Decode(String),
    /// An unexpected or disallowed packet was received.
    #[error("protocol violation: {0}")]
    Violation(String),
    /// Inbound Topic Alias is not supported (Topic Alias Maximum = 0).
    #[error("inbound topic alias is not supported")]
    TopicAliasNotSupported,
}

/// Application-level failure (inbound sink closed, command channel closed).
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AppError {
    /// The injectable [`crate::InboundSink`] was closed or disconnected.
    #[error("inbound sink closed")]
    InboundSinkClosed,
    /// The client command channel is no longer accepting work.
    #[error("client command channel closed")]
    CommandChannelClosed,
    /// Backpressure: an internal bounded queue is full.
    #[error("backpressure: {0}")]
    Backpressure(String),
    /// An internal driver task panicked or failed to join.
    #[error("internal task failure: {0}")]
    TaskJoin(String),
    /// Internal invariant failure (for example a poisoned lock).
    ///
    /// Distinct from [`Self::TaskJoin`]: use this for shared-state corruption,
    /// not for tokio task join failures.
    #[error("internal error: {0}")]
    Internal(String),
    /// Driver or API surface not yet implemented.
    #[error("not yet implemented")]
    NotImplemented,
    /// SUBSCRIBE was sent but no matching SUBACK arrived before the deadline.
    #[error("subscribe acknowledgement timed out after {0:?}")]
    SubscribeAckTimeout(Duration),
    /// UNSUBSCRIBE was sent but no matching UNSUBACK arrived before the deadline.
    #[error("unsubscribe acknowledgement timed out after {0:?}")]
    UnsubscribeAckTimeout(Duration),
    /// PUBLISH was sent but no matching PUBACK arrived before the deadline (QoS 1).
    #[error("publish acknowledgement timed out after {0:?}")]
    PublishAckTimeout(Duration),
    /// Caller request was invalid (e.g. empty subscribe filter list).
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    /// Outbound encode/write failed while handling a client command.
    #[error("send failed: {0}")]
    SendFailed(String),
    /// Connection ended before an outstanding QoS-1 publish received PUBACK (no hang).
    #[error("connection lost before publish acknowledgement")]
    ConnectionLost,
    /// Broker PUBACK completed the packet id with a non-success reason code.
    #[error("publish rejected by broker: {0}")]
    PublishRejected(String),
}

/// Graceful or requested shutdown.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ShutdownError {
    /// Shutdown was requested by the application.
    #[error("shutdown requested")]
    Requested,
    /// The driver was cancelled before completing graceful disconnect.
    #[error("shutdown cancelled")]
    Cancelled,
}

/// Outcome of a connection attempt or session end, fed to [`crate::ReconnectPolicy`].
///
/// Shares the same category taxonomy as [`ConnectError`] (auth / transport / protocol /
/// application / shutdown). Prefer this type for session-lifetime and reconnect decisions;
/// use [`ConnectError`] for the synchronous [`crate::connect`] precondition path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionOutcome {
    /// Authentication failure — reconnect per policy; invalidate credentials only for
    /// [`AuthError::NotAuthorized`].
    Auth(AuthError),
    /// Transport failure — reconnect per policy.
    Transport(TransportError),
    /// Protocol error — disconnect and reconnect per policy.
    Protocol(ProtocolError),
    /// Application-level failure — connection ends.
    Application(AppError),
    /// Graceful shutdown — stop reconnecting.
    Shutdown(ShutdownError),
}

impl ConnectionOutcome {
    /// Returns the error category name for logging and metrics.
    pub fn category(&self) -> &'static str {
        match self {
            Self::Auth(_) => "auth",
            Self::Transport(_) => "transport",
            Self::Protocol(_) => "protocol",
            // Keep `"app"` (not `"application"`) to avoid dashboard / metric churn.
            Self::Application(_) => "app",
            Self::Shutdown(_) => "shutdown",
        }
    }
}

impl fmt::Display for ConnectionOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Auth(error) => fmt::Display::fmt(error, formatter),
            Self::Transport(error) => fmt::Display::fmt(error, formatter),
            Self::Protocol(error) => fmt::Display::fmt(error, formatter),
            Self::Application(error) => fmt::Display::fmt(error, formatter),
            Self::Shutdown(error) => fmt::Display::fmt(error, formatter),
        }
    }
}

/// Error returned by [`crate::connect`] when the initial session cannot be established.
///
/// Connect-time subset of the same category taxonomy as [`ConnectionOutcome`].
/// Variants mirror that enum so callers can match auth / transport / protocol /
/// application / shutdown uniformly; convert with [`From`] rather than duplicating
/// mapping logic.
///
/// [`crate::connect`] currently returns this only for synchronous preconditions
/// (for example a closed handoff); TCP + CONNECT/CONNACK failures surface later via
/// [`Connection::run`](crate::Connection::run) as [`ConnectionOutcome`].
#[derive(Debug, Error)]
pub enum ConnectError {
    /// Authentication failed before or during CONNECT/CONNACK.
    #[error(transparent)]
    Auth(#[from] AuthError),
    /// Transport failure during connect.
    #[error(transparent)]
    Transport(#[from] TransportError),
    /// Protocol error during connect handshake.
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
    /// Application precondition failed (for example closed handoff).
    #[error(transparent)]
    Application(#[from] AppError),
    /// Connect aborted by shutdown.
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
}

impl From<ConnectionOutcome> for ConnectError {
    fn from(outcome: ConnectionOutcome) -> Self {
        match outcome {
            ConnectionOutcome::Auth(error) => Self::Auth(error),
            ConnectionOutcome::Transport(error) => Self::Transport(error),
            ConnectionOutcome::Protocol(error) => Self::Protocol(error),
            ConnectionOutcome::Application(error) => Self::Application(error),
            ConnectionOutcome::Shutdown(error) => Self::Shutdown(error),
        }
    }
}

impl From<ConnectError> for ConnectionOutcome {
    fn from(error: ConnectError) -> Self {
        match error {
            ConnectError::Auth(error) => Self::Auth(error),
            ConnectError::Transport(error) => Self::Transport(error),
            ConnectError::Protocol(error) => Self::Protocol(error),
            ConnectError::Application(error) => Self::Application(error),
            ConnectError::Shutdown(error) => Self::Shutdown(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_outcome_categories() {
        assert_eq!(ConnectionOutcome::Auth(AuthError::NotAuthorized).category(), "auth");
        assert_eq!(
            ConnectionOutcome::Transport(TransportError::Connection("reset".into())).category(),
            "transport"
        );
        assert_eq!(
            ConnectionOutcome::Protocol(ProtocolError::TopicAliasNotSupported).category(),
            "protocol"
        );
        assert_eq!(
            ConnectionOutcome::Application(AppError::InboundSinkClosed).category(),
            "app"
        );
        assert_eq!(
            ConnectionOutcome::Shutdown(ShutdownError::Requested).category(),
            "shutdown"
        );
    }

    #[test]
    fn connect_error_from_auth() {
        let err: ConnectError = AuthError::NotAuthorized.into();
        assert!(matches!(err, ConnectError::Auth(AuthError::NotAuthorized)));
    }

    #[test]
    fn connect_error_round_trips_connection_outcome() {
        let outcome = ConnectionOutcome::Application(AppError::Internal("lock poisoned".into()));
        let connect: ConnectError = outcome.clone().into();
        assert!(matches!(
            &connect,
            ConnectError::Application(AppError::Internal(message)) if message == "lock poisoned"
        ));
        assert_eq!(ConnectionOutcome::from(connect), outcome);
    }

    #[test]
    fn error_types_are_usable_in_result() {
        fn auth_fail() -> Result<(), AuthError> {
            Err(AuthError::NotAuthorized)
        }
        fn transport_fail() -> Result<(), TransportError> {
            Err(TransportError::Tls("cert".into()))
        }
        assert!(auth_fail().is_err());
        assert!(transport_fail().is_err());
    }
}
