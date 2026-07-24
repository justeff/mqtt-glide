//! CONNECT build, CONNACK apply, TLS mode.

use std::num::NonZeroU16;
use std::time::Duration;

use bytes::Bytes;
use bytestring::ByteString;
use secrecy::{ExposeSecret, SecretString};

use crate::codec::{Connect, ConnectAck, ConnectAckReason, QoS};
use crate::error::{AuthError, ConnectionOutcome, ProtocolError};
use crate::options::{ConnectOptions, TlsConfig};

/// Session parameters captured from a successful CONNACK.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct SessionParams {
    /// `true` only when values came from a broker CONNACK (not a local placeholder).
    pub from_connack: bool,
    /// Effective keep-alive after Server Keep Alive override (if any).
    pub keep_alive: Duration,
    /// Broker Shared Subscription Available flag from CONNACK.
    pub shared_subscription_available: bool,
    /// Broker Maximum Packet Size from CONNACK (`None` = unlimited / absent).
    pub max_packet_size: Option<u32>,
    /// Broker Maximum QoS from CONNACK (`ExactlyOnce` when the property is absent).
    pub max_qos: QoS,
    /// Broker Retain Available from CONNACK (`true` when the property is absent).
    pub retain_available: bool,
    /// Server Receive Maximum from CONNACK (outbound QoS-1 window).
    pub server_receive_maximum: NonZeroU16,
}

/// Whether the connector must negotiate TLS with system roots.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsMode {
    /// Plain TCP (`mqtt://` without explicit TLS config).
    PlainTcp,
    /// TLS via rustls using the OS trust store (`mqtts`/`ssl` or [`TlsConfig::SystemRoots`]).
    RustlsSystemRoots,
}

/// Credentials material for CONNECT username/password.
#[derive(Debug)]
pub(crate) struct ConnectCredentials {
    /// MQTT CONNECT username.
    pub username: String,
    /// MQTT CONNECT password (secret).
    pub password: SecretString,
}

/// Builds the MQTT 5 CONNECT packet from options + credentials.
///
/// Empty username / password strings are omitted on the wire (no USERNAME /
/// PASSWORD flags) so [`crate::AnonymousCredentials`] produces an anonymous CONNECT.
pub(crate) fn build_connect(options: &ConnectOptions, client_id: &str, credentials: &ConnectCredentials) -> Connect {
    let keep_alive = u16::try_from(options.keep_alive.as_secs()).unwrap_or(u16::MAX);
    let username = if credentials.username.is_empty() {
        None
    } else {
        Some(ByteString::from(credentials.username.as_str()))
    };
    let password_bytes = credentials.password.expose_secret().as_bytes();
    let password = if password_bytes.is_empty() {
        None
    } else {
        Some(Bytes::copy_from_slice(password_bytes))
    };
    Connect {
        clean_start: options.clean_start,
        keep_alive,
        session_expiry_interval_secs: session_expiry_secs(options.session_expiry),
        receive_max: Some(options.receive_maximum),
        topic_alias_max: 0,
        client_id: ByteString::from(client_id),
        username,
        password,
        ..Default::default()
    }
}

/// Applies CONNACK properties onto local session parameters.
pub(crate) fn apply_connack(client_keep_alive: Duration, ack: &ConnectAck) -> Result<SessionParams, ConnectionOutcome> {
    if ack.reason_code != ConnectAckReason::Success {
        return Err(connack_failure_outcome(ack.reason_code));
    }

    let keep_alive = ack
        .server_keepalive_sec
        .map(|secs| Duration::from_secs(u64::from(secs)))
        .unwrap_or(client_keep_alive);

    Ok(SessionParams {
        from_connack: true,
        keep_alive,
        shared_subscription_available: ack.shared_subscription_available,
        max_packet_size: ack.max_packet_size,
        max_qos: ack.max_qos,
        retain_available: ack.retain_available,
        server_receive_maximum: ack.receive_max,
    })
}

/// Maps a non-success CONNACK reason to a typed connection outcome.
pub(crate) fn connack_failure_outcome(reason: ConnectAckReason) -> ConnectionOutcome {
    match reason {
        ConnectAckReason::NotAuthorized | ConnectAckReason::BadUserNameOrPassword => {
            ConnectionOutcome::Auth(AuthError::NotAuthorized)
        }
        other => ConnectionOutcome::Protocol(ProtocolError::Violation(format!("CONNACK rejected: {other:?}"))),
    }
}

/// Resolves whether the transport must use TLS (rustls / system roots).
pub fn resolve_tls_mode(options: &ConnectOptions) -> TlsMode {
    if options.tls == Some(TlsConfig::SystemRoots) || options.broker_url.use_tls() {
        TlsMode::RustlsSystemRoots
    } else {
        TlsMode::PlainTcp
    }
}

/// Converts optional session-expiry [`Duration`] to MQTT Session Expiry Interval (`u32` seconds).
///
/// `None` → `0` (omit / default on the wire when encoding CONNECT). Saturates at `u32::MAX`.
pub(crate) fn session_expiry_secs(expiry: Option<Duration>) -> u32 {
    expiry
        .map(|duration| u32::try_from(duration.as_secs()).unwrap_or(u32::MAX))
        .unwrap_or(0)
}

/// Resolves the CONNECT client identifier string from options (empty if unset).
pub(crate) fn resolve_client_id_str(options: &ConnectOptions) -> String {
    options
        .client_id
        .as_ref()
        .map(|client_id| client_id.as_str().to_owned())
        .unwrap_or_default()
}

/// Builds the distinct [`crate::V5Codec`] instances for each `Framed` half after
/// `TcpStream::into_split()` — never a shared `Arc`.
pub(crate) fn distinct_v5_codecs_for_framed_halves() -> (crate::V5Codec, crate::V5Codec) {
    (
        crate::V5Codec::with_max_packet_size(0),
        crate::V5Codec::with_max_packet_size(0),
    )
}

/// Auth path: invalidate credentials on broker rejection only.
pub(crate) async fn on_auth_failure(
    credentials: &crate::client::SharedCredentials,
    outcome: ConnectionOutcome,
) -> ConnectionOutcome {
    if matches!(&outcome, ConnectionOutcome::Auth(AuthError::NotAuthorized)) {
        credentials.invalidate().await;
    }
    outcome
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::Bytes;
    use nonzero_ext::nonzero;
    use secrecy::{ExposeSecret, SecretString};

    use super::*;
    use crate::codec::{ConnectAck, ConnectAckReason, QoS, RECEIVE_MAX_DEFAULT};
    use crate::options::{ConnectOptions, DEFAULT_RECEIVE_MAXIMUM, TlsConfig};
    use crate::util::{BrokerUrl, ClientId};

    // ── CONNECT from ConnectOptions + credentials ─────────────────────────────

    fn sample_options() -> ConnectOptions {
        let url = BrokerUrl::parse("mqtt://broker.example:1883").expect("valid url");
        ConnectOptions::new(url)
            .client_id(ClientId::from_prefix("connect"))
            .keep_alive(Duration::from_secs(45))
            .receive_maximum(nonzero!(256u16))
            .clean_start(true)
            .session_expiry(Some(Duration::from_secs(120)))
    }

    fn sample_credentials() -> ConnectCredentials {
        ConnectCredentials {
            username: "oauth-user".into(),
            password: SecretString::from("secret-token".to_owned()),
        }
    }

    /// CONNECT carries Client-ID, keep-alive, username/password from options + creds.
    #[test]
    fn build_connect_maps_client_id_keepalive_and_credentials() {
        let options = sample_options();
        let client_id = resolve_client_id_str(&options);
        let credentials = sample_credentials();
        let packet = build_connect(&options, &client_id, &credentials);

        assert_eq!(&*packet.client_id, client_id.as_str());
        assert_eq!(packet.keep_alive, 45);
        assert_eq!(packet.username.as_ref().map(|u| u.as_ref() as &str), Some("oauth-user"));
        assert_eq!(
            packet.password.as_ref().map(Bytes::as_ref),
            Some(credentials.password.expose_secret().as_bytes())
        );
    }

    /// Empty username/password are omitted (anonymous CONNECT — no auth flags).
    #[test]
    fn build_connect_omits_empty_username_and_password() {
        let options = sample_options();
        let client_id = resolve_client_id_str(&options);
        let credentials = ConnectCredentials {
            username: String::new(),
            password: SecretString::from(String::new()),
        };
        let packet = build_connect(&options, &client_id, &credentials);

        assert!(packet.username.is_none());
        assert!(packet.password.is_none());
    }

    /// CONNECT Receive Maximum comes from `ConnectOptions`, not codec absent-default 65535.
    #[test]
    fn build_connect_uses_options_receive_maximum_not_codec_default() {
        let options = sample_options();
        let client_id = resolve_client_id_str(&options);
        let packet = build_connect(&options, &client_id, &sample_credentials());

        assert_eq!(packet.receive_max, Some(nonzero!(256u16)));
        assert_ne!(
            packet.receive_max_or_default(),
            RECEIVE_MAX_DEFAULT,
            "must not fall through to codec RECEIVE_MAX_DEFAULT (65535)"
        );
        assert_ne!(options.receive_maximum, DEFAULT_RECEIVE_MAXIMUM);
    }

    /// Clean Start, Session Expiry Duration→u32 s, Topic Alias Maximum = 0.
    #[test]
    fn build_connect_clean_start_session_expiry_and_topic_alias_max_zero() {
        let options = sample_options();
        let client_id = resolve_client_id_str(&options);
        let packet = build_connect(&options, &client_id, &sample_credentials());

        assert!(packet.clean_start);
        assert_eq!(
            packet.session_expiry_interval_secs,
            session_expiry_secs(options.session_expiry)
        );
        assert_eq!(packet.session_expiry_interval_secs, 120);
        assert_eq!(
            packet.topic_alias_max, 0,
            "Topic Alias Maximum must be 0 (product policy)"
        );
    }

    #[test]
    fn session_expiry_secs_converts_duration() {
        assert_eq!(session_expiry_secs(None), 0);
        assert_eq!(session_expiry_secs(Some(Duration::from_secs(0))), 0);
        assert_eq!(session_expiry_secs(Some(Duration::from_secs(3600))), 3600);
        assert_eq!(
            session_expiry_secs(Some(Duration::from_secs(u64::from(u32::MAX) + 10))),
            u32::MAX
        );
    }

    // ── CONNACK apply ───────────────────────────────────────────────────────

    fn sample_connack() -> ConnectAck {
        ConnectAck {
            session_present: false,
            reason_code: ConnectAckReason::Success,
            session_expiry_interval_secs: None,
            receive_max: nonzero!(512u16),
            max_qos: QoS::AtLeastOnce,
            max_packet_size: Some(1_048_576),
            assigned_client_id: None,
            topic_alias_max: 0,
            retain_available: true,
            wildcard_subscription_available: true,
            subscription_identifiers_available: false,
            shared_subscription_available: false,
            server_keepalive_sec: Some(30),
            response_info: None,
            server_reference: None,
            auth_method: None,
            auth_data: None,
            reason_string: None,
            user_properties: Vec::new(),
        }
    }

    /// Server Keep Alive from CONNACK overrides client keep-alive when set.
    #[test]
    fn apply_connack_server_keep_alive_overrides_client() {
        let client_keep_alive = Duration::from_secs(60);
        let ack = sample_connack();
        let session = apply_connack(client_keep_alive, &ack).expect("success CONNACK");

        assert!(session.from_connack);
        assert_eq!(
            session.keep_alive,
            Duration::from_secs(30),
            "Server Keep Alive must override client keep-alive"
        );
    }

    /// Capture Shared Subscription Available, Maximum Packet Size, Server Receive Maximum.
    #[test]
    fn apply_connack_captures_broker_limits() {
        let ack = sample_connack();
        let session = apply_connack(Duration::from_secs(60), &ack).expect("success CONNACK");

        assert!(!session.shared_subscription_available);
        assert_eq!(session.max_packet_size, Some(1_048_576));
        assert_eq!(session.max_qos, QoS::AtLeastOnce);
        assert!(session.retain_available);
        assert_eq!(session.server_receive_maximum, nonzero!(512u16));
    }

    /// Absent Server Keep Alive leaves client keep-alive unchanged.
    #[test]
    fn apply_connack_keeps_client_keep_alive_when_server_absent() {
        let mut ack = sample_connack();
        ack.server_keepalive_sec = None;
        let session = apply_connack(Duration::from_secs(60), &ack).expect("success CONNACK");
        assert_eq!(session.keep_alive, Duration::from_secs(60));
    }

    // ── Auth classification ─────────────────────────────────────────────────

    #[test]
    fn connack_not_authorized_maps_to_auth_outcome() {
        let outcome = connack_failure_outcome(ConnectAckReason::NotAuthorized);
        assert!(matches!(outcome, ConnectionOutcome::Auth(AuthError::NotAuthorized)));
    }

    /// [MQTT-4.13.2-1] CONNACK reason codes ≥0x80 classify as session/network termination.
    #[test]
    fn connack_unspecified_error_maps_to_protocol_outcome() {
        let outcome = connack_failure_outcome(ConnectAckReason::UnspecifiedError);
        assert!(matches!(outcome, ConnectionOutcome::Protocol(_)));
    }

    // ── TLS decision ────────────────────────────────────────────────────────

    /// `mqtts`/`ssl` or `TlsConfig::SystemRoots` selects rustls system-roots path.
    #[test]
    fn tls_mode_uses_rustls_system_roots_when_required() {
        let mqtts = BrokerUrl::parse("mqtts://broker.example").expect("valid");
        let options = ConnectOptions::new(mqtts);
        assert_eq!(resolve_tls_mode(&options), TlsMode::RustlsSystemRoots);

        let ssl = BrokerUrl::parse("ssl://broker.example").expect("valid");
        let options = ConnectOptions::new(ssl);
        assert_eq!(resolve_tls_mode(&options), TlsMode::RustlsSystemRoots);

        let plain = BrokerUrl::parse("mqtt://broker.example").expect("valid");
        let options = ConnectOptions::new(plain).tls(TlsConfig::SystemRoots);
        assert_eq!(resolve_tls_mode(&options), TlsMode::RustlsSystemRoots);

        let plain = BrokerUrl::parse("mqtt://broker.example").expect("valid");
        let options = ConnectOptions::new(plain);
        assert_eq!(resolve_tls_mode(&options), TlsMode::PlainTcp);
    }

    /// `CredentialsProvider::invalidate` is invoked on broker NotAuthorized only.
    #[tokio::test]
    async fn auth_failure_invokes_invalidate_hook() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        use crate::client::{CredentialsProvider, SharedCredentials};
        use crate::error::AuthError;

        struct CountingCreds {
            invalidations: Arc<AtomicUsize>,
        }

        impl CredentialsProvider for CountingCreds {
            async fn credentials(&self) -> Result<(String, SecretString), AuthError> {
                Ok(("user".into(), SecretString::from("pass".to_owned())))
            }

            async fn invalidate(&self) {
                self.invalidations.fetch_add(1, Ordering::SeqCst);
            }
        }

        let invalidations = Arc::new(AtomicUsize::new(0));
        let shared = SharedCredentials::new(CountingCreds {
            invalidations: Arc::clone(&invalidations),
        });
        let outcome = on_auth_failure(&shared, ConnectionOutcome::Auth(AuthError::NotAuthorized)).await;
        assert!(matches!(outcome, ConnectionOutcome::Auth(AuthError::NotAuthorized)));
        assert_eq!(invalidations.load(Ordering::SeqCst), 1);
    }

    /// Provider failures must not invalidate cached credentials (transient upstream error).
    #[tokio::test]
    async fn auth_provider_failure_does_not_invalidate() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        use crate::client::{CredentialsProvider, SharedCredentials};
        use crate::error::AuthError;

        struct CountingCreds {
            invalidations: Arc<AtomicUsize>,
        }

        impl CredentialsProvider for CountingCreds {
            async fn credentials(&self) -> Result<(String, SecretString), AuthError> {
                Ok(("user".into(), SecretString::from("pass".to_owned())))
            }

            async fn invalidate(&self) {
                self.invalidations.fetch_add(1, Ordering::SeqCst);
            }
        }

        let invalidations = Arc::new(AtomicUsize::new(0));
        let shared = SharedCredentials::new(CountingCreds {
            invalidations: Arc::clone(&invalidations),
        });
        let outcome = on_auth_failure(
            &shared,
            ConnectionOutcome::Auth(AuthError::Provider("token endpoint down".into())),
        )
        .await;
        assert!(matches!(outcome, ConnectionOutcome::Auth(AuthError::Provider(_))));
        assert_eq!(invalidations.load(Ordering::SeqCst), 0);
    }
}
