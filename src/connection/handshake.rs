//! CONNECT build, CONNACK apply, TLS mode.

use std::num::NonZeroU16;
use std::time::Duration;

use bytes::Bytes;
use bytestring::ByteString;
use secrecy::{ExposeSecret, SecretString};

use crate::codec::{Connect, ConnectAck, ConnectAckReason, QoS, WillMessage};
use crate::error::{AppError, AuthError, ConnectionOutcome, ProtocolError};
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
    /// Server Topic Alias Maximum from CONNACK (0 = the broker accepts no aliases).
    pub server_topic_alias_maximum: u16,
    /// Subscription Identifiers Available from CONNACK (`true` when the property is absent).
    pub subscription_identifiers_available: bool,
    /// Session Present flag from CONNACK (§3.2.2.1.1): `true` when the broker resumed a
    /// stored session. Drives resubscribe for `clean_start = false` connections.
    pub session_present: bool,
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
///
/// Caller **MUST** have validated [`ConnectOptions::will_message`] via [`validate_will_message`]
/// when present (Topic Name / QoS rules).
pub(crate) fn build_connect(options: &ConnectOptions, client_id: &str, credentials: &ConnectCredentials) -> Connect {
    let keep_alive = keep_alive_secs(options.keep_alive);
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
        session_expiry_interval: session_expiry_secs(options.session_expiry),
        receive_max: Some(options.receive_maximum),
        max_packet_size: Some(options.max_packet_size),
        topic_alias_max: options.topic_alias_maximum,
        client_id: ByteString::from(client_id),
        username,
        password,
        will_message: options.will_message.clone(),
        ..Default::default()
    }
}

/// Validates a CONNECT Will Message for the client API (Topic Name rules, QoS 0|1 only).
///
/// [MQTT-3.1.2-9] Will Topic + Payload present when Will is set (enforced by `Some(WillMessage)`).
/// [MQTT-3.1.2-12] Will QoS may be 0|1|2 on the wire; this client rejects QoS 2 like PUBLISH.
/// [MQTT-3.1.3-11] Will Topic MUST be a UTF-8 Topic Name (no wildcards / U+0000 / empty).
pub(crate) fn validate_will_message(will: &WillMessage) -> Result<(), AppError> {
    if matches!(will.qos, QoS::ExactlyOnce) {
        return Err(AppError::InvalidRequest("Will QoS 2 is not supported".into()));
    }
    if let Some(reason) = topic_name_invalid_reason(will.topic.as_ref()) {
        return Err(AppError::InvalidRequest(format!("will topic: {reason}")));
    }
    if let Some(response_topic) = will.response_topic.as_ref()
        && let Some(reason) = topic_name_invalid_reason(response_topic.as_ref())
    {
        return Err(AppError::InvalidRequest(format!("will response_topic: {reason}")));
    }
    // Everything else the encoder would refuse on every attempt, so a reconnect policy would
    // retry forever: UTF-8 strings without U+0000 ([MQTT-1.5.4-2]) and two-byte lengths (§1.5).
    let mut strings = vec![("will topic", &will.topic)];
    strings.extend(will.response_topic.iter().map(|topic| ("will response_topic", topic)));
    strings.extend(will.content_type.iter().map(|value| ("will content_type", value)));
    for property in &will.user_properties {
        strings.push(("will user property name", &property.name));
        strings.push(("will user property value", &property.value));
    }
    for (field, value) in strings {
        if value.as_bytes().contains(&0) {
            return Err(AppError::InvalidRequest(format!("{field} must not contain U+0000")));
        }
        if value.len() > MAX_TWO_BYTE_LENGTH {
            return Err(AppError::InvalidRequest(format!("{field} is longer than 65535 bytes")));
        }
    }
    let binary = [
        ("will payload", Some(&will.payload)),
        ("will correlation_data", will.correlation_data.as_ref()),
    ];
    for (field, value) in binary {
        if value.is_some_and(|value| value.len() > MAX_TWO_BYTE_LENGTH) {
            return Err(AppError::InvalidRequest(format!("{field} is longer than 65535 bytes")));
        }
    }
    Ok(())
}

/// Longest UTF-8 string or binary data with a two-byte length prefix (§1.5.4, §1.5.6).
const MAX_TWO_BYTE_LENGTH: usize = u16::MAX as usize;

/// Topic Name rules for Will Topic / Will Response Topic (mirrors outbound PUBLISH checks).
pub(crate) fn topic_name_invalid_reason(topic: &str) -> Option<&'static str> {
    if topic.is_empty() {
        Some("topic must not be empty")
    } else if topic.as_bytes().contains(&0) {
        Some("topic must not contain U+0000")
    } else if topic.as_bytes().iter().any(|&b| matches!(b, b'+' | b'#')) {
        Some("topic must not contain wildcard characters")
    } else {
        None
    }
}

/// The Keep Alive CONNECT carries: whole seconds, at most `u16::MAX` (§3.1.2.10).
fn keep_alive_secs(keep_alive: Duration) -> u16 {
    u16::try_from(keep_alive.as_secs()).unwrap_or(u16::MAX)
}

/// Applies CONNACK properties onto local session parameters.
pub(crate) fn apply_connack(client_keep_alive: Duration, ack: &ConnectAck) -> Result<SessionParams, ConnectionOutcome> {
    // Ping on the interval the broker was told, not the unrounded option: 1.9 s goes out as
    // 1 s, and pinging every 1.9 s would let the broker drop an idle client.
    let client_keep_alive = Duration::from_secs(u64::from(keep_alive_secs(client_keep_alive)));
    if ack.reason_code != ConnectAckReason::Success {
        return Err(connack_failure_outcome(
            ack.reason_code,
            ack.reason_string.as_deref(),
            ack.server_reference.as_deref(),
        ));
    }

    let keep_alive = ack
        .server_keep_alive
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
        server_topic_alias_maximum: ack.topic_alias_max,
        subscription_identifiers_available: ack.subscription_identifiers_available,
        session_present: ack.session_present,
    })
}

/// Maps a non-success CONNACK reason to a typed connection outcome.
pub(crate) fn connack_failure_outcome(
    reason: ConnectAckReason,
    reason_string: Option<&str>,
    server_reference: Option<&str>,
) -> ConnectionOutcome {
    let detail = super::broker_detail(reason_string, server_reference);
    match reason {
        ConnectAckReason::NotAuthorized | ConnectAckReason::BadUserNameOrPassword => {
            if !detail.is_empty() {
                tracing::warn!(reason = ?reason, detail = %detail, "CONNACK refused credentials");
            }
            ConnectionOutcome::Auth(AuthError::NotAuthorized)
        }
        other => ConnectionOutcome::Protocol(ProtocolError::Violation(format!("CONNACK rejected: {other:?}{detail}"))),
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

/// Builds the distinct [`crate::codec::V5Codec`] instances for each `Framed` half after
/// `TcpStream::into_split()` — never a shared `Arc`.
///
/// `inbound_max` is the client Maximum Packet Size from CONNECT (0 = unlimited).
/// The write half starts with unlimited outbound until CONNACK applies the broker limit.
pub(crate) fn distinct_v5_codecs_for_framed_halves(inbound_max: u32) -> (crate::codec::V5Codec, crate::codec::V5Codec) {
    (
        crate::codec::V5Codec::new(inbound_max, 0),
        crate::codec::V5Codec::new(0, 0),
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

    use rstest::rstest;

    use super::*;
    use crate::codec::{ConnectAck, ConnectAckReason, QoS, RECEIVE_MAX_DEFAULT, WillMessage};
    use crate::error::AppError;
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
            packet.session_expiry_interval,
            session_expiry_secs(options.session_expiry)
        );
        assert_eq!(packet.session_expiry_interval, 120);
        assert_eq!(
            packet.topic_alias_max, 0,
            "Topic Alias Maximum must be 0 (product policy)"
        );
        assert_eq!(
            packet.max_packet_size,
            Some(options.max_packet_size),
            "CONNECT must advertise client Maximum Packet Size"
        );
        assert!(packet.will_message.is_none(), "default ConnectOptions has no Will");
    }

    #[test]
    fn build_connect_maps_will_message() {
        let options = sample_options()
            .will("status/offline", "dead")
            .will_qos(QoS::AtLeastOnce)
            .will_retain(true)
            .will_delay_interval(10);
        let will = options.will_message.clone().expect("will set");
        validate_will_message(&will).expect("valid will");
        assert_eq!(will.qos, QoS::AtLeastOnce);
        assert!(will.retain);
        assert_eq!(will.will_delay_interval, Some(10));
        let client_id = resolve_client_id_str(&options);
        let packet = build_connect(&options, &client_id, &sample_credentials());
        assert_eq!(packet.will_message.as_ref(), Some(&will));
    }

    #[rstest]
    #[case::qos2(WillMessage::new("t", "p").qos(QoS::ExactlyOnce))]
    #[case::empty_topic(WillMessage::new("", "p"))]
    #[case::wildcard(WillMessage::new("a/+/b", "p"))]
    #[case::nul(WillMessage::new("a\0b", "p"))]
    #[case::bad_response_topic(WillMessage::new("ok", "p").response_topic("bad/#"))]
    #[case::nul_in_content_type(WillMessage::new("ok", "p").content_type("text\0plain"))]
    #[case::nul_in_user_property(WillMessage::new("ok", "p").user_property("k", "v\0"))]
    #[case::long_topic(WillMessage::new("t".repeat(65_536), "p"))]
    #[case::long_payload(WillMessage::new("ok", vec![0u8; 65_536]))]
    fn validate_will_message_rejects_invalid(#[case] will: WillMessage) {
        assert!(matches!(validate_will_message(&will), Err(AppError::InvalidRequest(_))));
    }

    /// 65535 bytes is the longest a two-byte length can carry, so it is still accepted (§1.5.4, §1.5.6).
    #[test]
    fn validate_will_message_accepts_the_longest_two_byte_lengths() {
        let will = WillMessage::new("t".repeat(65_535), vec![0u8; 65_535]).correlation_data(vec![0u8; 65_535]);
        validate_will_message(&will).expect("at the limit");
    }

    /// A Will QoS of 1 is accepted ([MQTT-3.1.2-12]); QoS 2 is refused above, like PUBLISH.
    #[test]
    fn validate_will_message_accepts_qos1() {
        validate_will_message(&WillMessage::new("ok", "p").qos(QoS::AtLeastOnce)).expect("qos1 ok");
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
            session_expiry_interval: None,
            receive_max: nonzero!(512u16),
            max_qos: QoS::AtLeastOnce,
            max_packet_size: Some(1_048_576),
            assigned_client_id: None,
            topic_alias_max: 0,
            retain_available: true,
            wildcard_subscription_available: true,
            subscription_identifiers_available: false,
            shared_subscription_available: false,
            server_keep_alive: Some(30),
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
    fn apply_connack_pings_on_the_interval_sent_in_connect() {
        let ack = ConnectAck::default();
        for (configured, effective) in [
            (Duration::from_millis(1900), Duration::from_secs(1)),
            (Duration::from_millis(500), Duration::ZERO),
            (Duration::from_secs(100_000), Duration::from_secs(u64::from(u16::MAX))),
        ] {
            let session = apply_connack(configured, &ack).expect("success CONNACK");
            assert_eq!(session.keep_alive, effective, "keep_alive({configured:?})");
        }
    }

    #[test]
    fn apply_connack_keeps_client_keep_alive_when_server_absent() {
        let mut ack = sample_connack();
        ack.server_keep_alive = None;
        let session = apply_connack(Duration::from_secs(60), &ack).expect("success CONNACK");
        assert_eq!(session.keep_alive, Duration::from_secs(60));
    }

    // ── Auth classification ─────────────────────────────────────────────────

    #[test]
    fn connack_not_authorized_maps_to_auth_outcome() {
        let outcome = connack_failure_outcome(ConnectAckReason::NotAuthorized, None, None);
        assert!(matches!(outcome, ConnectionOutcome::Auth(AuthError::NotAuthorized)));
    }

    /// [MQTT-4.13.2-1] CONNACK reason codes ≥0x80 classify as session/network termination.
    #[test]
    fn connack_unspecified_error_maps_to_protocol_outcome() {
        let outcome = connack_failure_outcome(ConnectAckReason::UnspecifiedError, Some("maintenance"), None);
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
