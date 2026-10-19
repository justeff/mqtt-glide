//! Property-based round-trip tests for the public MQTT v5 codec facade.
//!
//! Encodes generated packets, decodes the wire bytes back, and asserts equality — no
//! malformed-input / reject-path coverage here (see the codec module unit tests under
//! `src/codec/` for that).
//!
//! Generators cover the full valid input space of the codec (it also serves the broker side):
//! - Every reason code of every packet, listed explicitly (not via `TryFrom<u8>`), plus
//!   unassigned CONNACK codes as `Unknown`
//! - Every property of CONNECT (incl. Will and its properties), CONNACK, PUBLISH, SUBSCRIBE,
//!   DISCONNECT and AUTH (Success with properties too), each both present and absent
//! - QoS 0/1/2, all Retain Handling values, wildcard / `$share` filters, Subscription Identifier
//! - Valid UTF-8 topics and identifiers; bounded topic (1..=128 chars) and payload (0..=4096 bytes) sizes
//! - Spec constraints respected: non-zero Receive Maximum / Maximum Packet Size, Authentication Data
//!   only with Authentication Method, Response Topic is a plain Topic Name, no DUP on QoS 0

use std::num::{NonZeroU16, NonZeroU32};

use bytes::{Bytes, BytesMut};
use bytestring::ByteString;
use mqtt_glide::codec::{
    Auth, AuthReasonCode, Connect, ConnectAck, ConnectAckReason, DecodedFrame, Disconnect, DisconnectReasonCode,
    Packet, PubRelComp, PubRelCompReason, PublishAck, PublishAckReason, PublishDelivery, PublishMessage, PublishPacket,
    PublishPacketMeta, PublishProperties, QoS, RetainHandling, Subscribe, SubscribeAck, SubscribeAckReason,
    SubscriptionOptions, Unsubscribe, UnsubscribeAck, UnsubscribeAckReason, UserProperties, V5Codec, WillMessage,
    encode_publish,
};
use mqtt_glide::codec::{DEFAULT_MAX_PACKET_SIZE, EncodeError};
use proptest::prelude::*;
use proptest::strategy::Strategy;

const PROPTEST_CASES: u32 = 1024;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(PROPTEST_CASES))]

    #[test]
    fn v5_connect_round_trip(connect in arb_v5_connect()) {
        round_trip_v5(Packet::Connect(Box::new(connect)));
    }

    #[test]
    fn v5_publish_without_properties_round_trip((message, delivery) in arb_v5_publish_without_properties()) {
        round_trip_v5_publish(message, delivery);
    }

    #[test]
    fn v5_publish_with_properties_round_trip((message, delivery) in arb_v5_publish_with_properties()) {
        round_trip_v5_publish(message, delivery);
    }

    #[test]
    fn v5_subscribe_round_trip(subscribe in arb_v5_subscribe()) {
        round_trip_v5(Packet::Subscribe(subscribe));
    }

    #[test]
    fn v5_connack_round_trip(connack in arb_v5_connack()) {
        round_trip_v5(Packet::ConnectAck(Box::new(connack)));
    }

    #[test]
    fn v5_puback_round_trip(ack in arb_v5_publish_ack()) {
        round_trip_v5(Packet::PublishAck(ack));
    }

    #[test]
    fn v5_pubrec_round_trip(ack in arb_v5_publish_ack()) {
        round_trip_v5(Packet::PublishReceived(ack));
    }

    #[test]
    fn v5_pubrel_round_trip(ack in arb_v5_pub_rel_comp()) {
        round_trip_v5(Packet::PublishRelease(ack));
    }

    #[test]
    fn v5_pubcomp_round_trip(ack in arb_v5_pub_rel_comp()) {
        round_trip_v5(Packet::PublishComplete(ack));
    }

    #[test]
    fn v5_suback_round_trip(suback in arb_v5_suback()) {
        round_trip_v5(Packet::SubscribeAck(suback));
    }

    #[test]
    fn v5_unsubscribe_round_trip(unsubscribe in arb_v5_unsubscribe()) {
        round_trip_v5(Packet::Unsubscribe(unsubscribe));
    }

    #[test]
    fn v5_unsuback_round_trip(unsuback in arb_v5_unsuback()) {
        round_trip_v5(Packet::UnsubscribeAck(unsuback));
    }

    #[test]
    fn v5_disconnect_round_trip(disconnect in arb_v5_disconnect()) {
        round_trip_v5(Packet::Disconnect(disconnect));
    }

    #[test]
    fn v5_auth_round_trip(auth in arb_v5_auth()) {
        round_trip_v5(Packet::Auth(auth));
    }
}

fn round_trip_v5(packet: Packet) {
    let expected = DecodedFrame::Control(packet.clone());
    let mut codec = V5Codec::with_max_packet_size(DEFAULT_MAX_PACKET_SIZE);
    let mut wire = BytesMut::new();
    codec
        .encode_packet(packet, &mut wire)
        .expect("encode must succeed for generated packet");
    assert_decodes_to(&mut codec, wire, expected);
}

fn round_trip_v5_publish(message: PublishMessage, delivery: PublishDelivery) {
    let expected = DecodedFrame::Publish(PublishPacket {
        message: message.clone(),
        meta: PublishPacketMeta {
            dup: delivery.dup,
            retain: delivery.retain,
            qos: delivery.qos,
            packet_id: delivery.packet_id,
        },
        wire_topic_alias: None,
        subscription_ids: Vec::new(),
    });
    let mut codec = V5Codec::with_max_packet_size(DEFAULT_MAX_PACKET_SIZE);
    let mut wire = BytesMut::new();
    encode_publish(&message, delivery, DEFAULT_MAX_PACKET_SIZE, &mut wire)
        .expect("encode must succeed for generated publish");

    // The size check before writing uses the computed wire length; it must be the length written.
    let wire_len = u32::try_from(wire.len()).expect("generated publish fits in u32");
    let mut exact = BytesMut::new();
    encode_publish(&message, delivery, wire_len, &mut exact)
        .expect("a limit of the exact wire length holds the publish");
    assert_eq!(exact, wire);
    let mut short = BytesMut::new();
    let error = encode_publish(&message, delivery, wire_len - 1, &mut short).expect_err("one byte short must not fit");
    assert!(
        matches!(error, EncodeError::OverMaxPacketSize { size, max } if size == wire_len && max == wire_len - 1),
        "{error:?}"
    );
    assert!(short.is_empty());

    assert_decodes_to(&mut codec, wire, expected);
}

/// Checks that the fixed header frames exactly the bytes written, then decodes the frame with
/// nothing left over.
fn assert_decodes_to(codec: &mut V5Codec, mut wire: BytesMut, expected: DecodedFrame) {
    assert_eq!(
        framed_len(&wire),
        wire.len(),
        "Remaining Length must match the body written"
    );
    let decoded = codec
        .decode_frame(&mut wire)
        .expect("decode must not error for generated wire")
        .expect("generated wire must contain a full packet");
    assert_eq!(decoded, expected);
    assert!(
        wire.is_empty(),
        "decoding must consume the whole frame, {} bytes left",
        wire.len()
    );
}

/// Frame length the fixed header declares: type byte, Remaining Length varint (§1.5.5), body.
fn framed_len(wire: &[u8]) -> usize {
    let mut remaining_length = 0usize;
    for (i, &byte) in wire[1..].iter().take(4).enumerate() {
        remaining_length |= usize::from(byte & 0x7F) << (7 * i);
        if byte & 0x80 == 0 {
            return 1 + (i + 1) + remaining_length;
        }
    }
    panic!("Remaining Length must end within four bytes");
}

// --- Shared building blocks -------------------------------------------------------------

fn arb_valid_topic() -> impl Strategy<Value = String> {
    // MQTT topic names are UTF-8; keep to a conservative printable subset.
    const TOPIC_CHARS: &[char] = &[
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
        'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R',
        'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '/', '_', '-',
    ];
    prop::collection::vec(prop::sample::select(TOPIC_CHARS), 1..=128).prop_map(|chars| chars.into_iter().collect())
}

/// Non-empty UTF-8 string without wildcards or NUL (usable as topic name, reason string, ...).
fn arb_text() -> impl Strategy<Value = ByteString> {
    arb_valid_topic().prop_map(ByteString::from)
}

fn arb_opt_text() -> impl Strategy<Value = Option<ByteString>> {
    prop::option::of(arb_text())
}

/// Binary data (Correlation Data, Authentication Data, passwords); may be empty.
fn arb_binary() -> impl Strategy<Value = Bytes> {
    prop::collection::vec(any::<u8>(), 0..=64).prop_map(Bytes::from)
}

fn arb_payload() -> impl Strategy<Value = Bytes> {
    prop::collection::vec(any::<u8>(), 0..=4096).prop_map(Bytes::from)
}

fn arb_nonzero_u16() -> impl Strategy<Value = NonZeroU16> {
    (1u16..=u16::MAX).prop_map(|v| NonZeroU16::new(v).expect("non-zero u16"))
}

fn arb_nonzero_u32() -> impl Strategy<Value = NonZeroU32> {
    (1u32..=u32::MAX).prop_map(|v| NonZeroU32::new(v).expect("non-zero u32"))
}

fn arb_qos() -> impl Strategy<Value = QoS> {
    prop::sample::select(&[QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce][..])
}

fn arb_v5_user_properties() -> impl Strategy<Value = UserProperties> {
    prop::collection::vec((arb_valid_topic(), arb_valid_topic()), 0..=4)
        .prop_map(|pairs| pairs.into_iter().map(|(key, value)| (key, value).into()).collect())
}

/// Authentication Method with optional Authentication Data (data requires a method).
fn arb_auth_method_and_data() -> impl Strategy<Value = (Option<ByteString>, Option<Bytes>)> {
    prop_oneof![
        Just((None, None)),
        (arb_text(), prop::option::of(arb_binary())).prop_map(|(method, data)| (Some(method), data)),
    ]
}

// --- Reason codes: every variant listed explicitly (never via `TryFrom<u8>`) -------------

const CONNACK_REASONS: &[ConnectAckReason] = &[
    ConnectAckReason::Success,
    ConnectAckReason::UnspecifiedError,
    ConnectAckReason::MalformedPacket,
    ConnectAckReason::ProtocolError,
    ConnectAckReason::ImplementationSpecificError,
    ConnectAckReason::UnsupportedProtocolVersion,
    ConnectAckReason::ClientIdentifierNotValid,
    ConnectAckReason::BadUserNameOrPassword,
    ConnectAckReason::NotAuthorized,
    ConnectAckReason::ServerUnavailable,
    ConnectAckReason::ServerBusy,
    ConnectAckReason::Banned,
    ConnectAckReason::BadAuthenticationMethod,
    ConnectAckReason::TopicNameInvalid,
    ConnectAckReason::PacketTooLarge,
    ConnectAckReason::QuotaExceeded,
    ConnectAckReason::PayloadFormatInvalid,
    ConnectAckReason::RetainNotSupported,
    ConnectAckReason::QosNotSupported,
    ConnectAckReason::UseAnotherServer,
    ConnectAckReason::ServerMoved,
    ConnectAckReason::ConnectionRateExceeded,
];

/// Wire values assigned to a named `ConnectAckReason` (§3.2.2.2); everything else is `Unknown`.
const CONNACK_ASSIGNED_CODES: &[u8] = &[
    0, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 140, 144, 149, 151, 153, 154, 155, 156, 157, 159,
];

const PUBLISH_ACK_REASONS: &[PublishAckReason] = &[
    PublishAckReason::Success,
    PublishAckReason::NoMatchingSubscribers,
    PublishAckReason::UnspecifiedError,
    PublishAckReason::ImplementationSpecificError,
    PublishAckReason::NotAuthorized,
    PublishAckReason::TopicNameInvalid,
    PublishAckReason::PacketIdentifierInUse,
    PublishAckReason::QuotaExceeded,
    PublishAckReason::PayloadFormatInvalid,
];

const PUB_REL_COMP_REASONS: &[PubRelCompReason] = &[PubRelCompReason::Success, PubRelCompReason::PacketIdNotFound];

const SUBACK_REASONS: &[SubscribeAckReason] = &[
    SubscribeAckReason::GrantedQos0,
    SubscribeAckReason::GrantedQos1,
    SubscribeAckReason::GrantedQos2,
    SubscribeAckReason::UnspecifiedError,
    SubscribeAckReason::ImplementationSpecificError,
    SubscribeAckReason::NotAuthorized,
    SubscribeAckReason::TopicFilterInvalid,
    SubscribeAckReason::PacketIdentifierInUse,
    SubscribeAckReason::QuotaExceeded,
    SubscribeAckReason::SharedSubscriptionNotSupported,
    SubscribeAckReason::SubscriptionIdentifiersNotSupported,
    SubscribeAckReason::WildcardSubscriptionsNotSupported,
];

const UNSUBACK_REASONS: &[UnsubscribeAckReason] = &[
    UnsubscribeAckReason::Success,
    UnsubscribeAckReason::NoSubscriptionExisted,
    UnsubscribeAckReason::UnspecifiedError,
    UnsubscribeAckReason::ImplementationSpecificError,
    UnsubscribeAckReason::NotAuthorized,
    UnsubscribeAckReason::TopicFilterInvalid,
    UnsubscribeAckReason::PacketIdentifierInUse,
];

const DISCONNECT_REASONS: &[DisconnectReasonCode] = &[
    DisconnectReasonCode::NormalDisconnection,
    DisconnectReasonCode::DisconnectWithWillMessage,
    DisconnectReasonCode::UnspecifiedError,
    DisconnectReasonCode::MalformedPacket,
    DisconnectReasonCode::ProtocolError,
    DisconnectReasonCode::ImplementationSpecificError,
    DisconnectReasonCode::NotAuthorized,
    DisconnectReasonCode::ServerBusy,
    DisconnectReasonCode::ServerShuttingDown,
    DisconnectReasonCode::BadAuthenticationMethod,
    DisconnectReasonCode::KeepAliveTimeout,
    DisconnectReasonCode::SessionTakenOver,
    DisconnectReasonCode::TopicFilterInvalid,
    DisconnectReasonCode::TopicNameInvalid,
    DisconnectReasonCode::ReceiveMaximumExceeded,
    DisconnectReasonCode::TopicAliasInvalid,
    DisconnectReasonCode::PacketTooLarge,
    DisconnectReasonCode::MessageRateTooHigh,
    DisconnectReasonCode::QuotaExceeded,
    DisconnectReasonCode::AdministrativeAction,
    DisconnectReasonCode::PayloadFormatInvalid,
    DisconnectReasonCode::RetainNotSupported,
    DisconnectReasonCode::QosNotSupported,
    DisconnectReasonCode::UseAnotherServer,
    DisconnectReasonCode::ServerMoved,
    DisconnectReasonCode::SharedSubscriptionNotSupported,
    DisconnectReasonCode::ConnectionRateExceeded,
    DisconnectReasonCode::MaximumConnectTime,
    DisconnectReasonCode::SubscriptionIdentifiersNotSupported,
    DisconnectReasonCode::WildcardSubscriptionsNotSupported,
];

const AUTH_REASONS: &[AuthReasonCode] = &[
    AuthReasonCode::Success,
    AuthReasonCode::ContinueAuth,
    AuthReasonCode::ReAuth,
];

const RETAIN_HANDLINGS: &[RetainHandling] = &[
    RetainHandling::AtSubscribe,
    RetainHandling::AtSubscribeNew,
    RetainHandling::NoAtSubscribe,
];

fn arb_v5_connack_reason() -> impl Strategy<Value = ConnectAckReason> {
    prop_oneof![
        8 => prop::sample::select(CONNACK_REASONS),
        // Unassigned wire values decode as `Unknown(code)` and must survive a round trip.
        1 => any::<u8>()
            .prop_filter("unassigned code", |code| !CONNACK_ASSIGNED_CODES.contains(code))
            .prop_map(ConnectAckReason::Unknown),
    ]
}

// --- CONNACK ----------------------------------------------------------------------------

fn arb_v5_connack() -> impl Strategy<Value = ConnectAck> {
    (
        (
            any::<bool>(),
            arb_v5_connack_reason(),
            prop::option::of(any::<u32>()),
            arb_nonzero_u16(),
            arb_qos(),
        ),
        (
            prop::option::of(1u32..=u32::MAX),
            arb_opt_text(),
            any::<u16>(),
            [any::<bool>(), any::<bool>(), any::<bool>(), any::<bool>()],
        ),
        (
            prop::option::of(any::<u16>()),
            arb_opt_text(),
            arb_opt_text(),
            arb_auth_method_and_data(),
            arb_opt_text(),
            arb_v5_user_properties(),
        ),
    )
        .prop_map(
            |(
                (session_present, reason_code, session_expiry_interval, receive_max, max_qos),
                (max_packet_size, assigned_client_id, topic_alias_max, [retain, wildcard, sub_ids, shared]),
                (
                    server_keep_alive,
                    response_info,
                    server_reference,
                    (auth_method, auth_data),
                    reason_string,
                    user_properties,
                ),
            )| ConnectAck {
                session_present,
                reason_code,
                session_expiry_interval,
                receive_max,
                max_qos,
                max_packet_size,
                assigned_client_id,
                topic_alias_max,
                retain_available: retain,
                wildcard_subscription_available: wildcard,
                subscription_identifiers_available: sub_ids,
                shared_subscription_available: shared,
                server_keep_alive,
                response_info,
                server_reference,
                auth_method,
                auth_data,
                reason_string,
                user_properties,
            },
        )
}

// --- PUBACK / PUBREC / PUBREL / PUBCOMP -------------------------------------------------

fn arb_v5_publish_ack() -> impl Strategy<Value = PublishAck> {
    (
        arb_nonzero_u16(),
        prop::sample::select(PUBLISH_ACK_REASONS),
        arb_v5_user_properties(),
        arb_opt_text(),
    )
        .prop_map(|(packet_id, reason_code, properties, reason_string)| PublishAck {
            packet_id,
            reason_code,
            properties,
            reason_string,
        })
}

fn arb_v5_pub_rel_comp() -> impl Strategy<Value = PubRelComp> {
    (
        arb_nonzero_u16(),
        prop::sample::select(PUB_REL_COMP_REASONS),
        arb_v5_user_properties(),
        arb_opt_text(),
    )
        .prop_map(|(packet_id, reason_code, properties, reason_string)| PubRelComp {
            packet_id,
            reason_code,
            properties,
            reason_string,
        })
}

// --- SUBACK / UNSUBSCRIBE / UNSUBACK ----------------------------------------------------

fn arb_v5_suback() -> impl Strategy<Value = SubscribeAck> {
    (
        arb_nonzero_u16(),
        prop::collection::vec(prop::sample::select(SUBACK_REASONS), 1..=16),
        arb_v5_user_properties(),
        arb_opt_text(),
    )
        .prop_map(|(packet_id, status, properties, reason_string)| SubscribeAck {
            packet_id,
            status,
            properties,
            reason_string,
        })
}

fn arb_v5_unsubscribe() -> impl Strategy<Value = Unsubscribe> {
    (
        arb_nonzero_u16(),
        prop::collection::vec(arb_topic_filter(), 1..=8),
        arb_v5_user_properties(),
    )
        .prop_map(|(packet_id, topic_filters, user_properties)| Unsubscribe {
            packet_id,
            topic_filters,
            user_properties,
        })
}

fn arb_v5_unsuback() -> impl Strategy<Value = UnsubscribeAck> {
    (
        arb_nonzero_u16(),
        prop::collection::vec(prop::sample::select(UNSUBACK_REASONS), 1..=16),
        arb_v5_user_properties(),
        arb_opt_text(),
    )
        .prop_map(|(packet_id, status, properties, reason_string)| UnsubscribeAck {
            packet_id,
            status,
            properties,
            reason_string,
        })
}

// --- DISCONNECT / AUTH ------------------------------------------------------------------

fn arb_v5_disconnect() -> impl Strategy<Value = Disconnect> {
    (
        prop::sample::select(DISCONNECT_REASONS),
        prop::option::of(any::<u32>()),
        arb_opt_text(),
        arb_opt_text(),
        arb_v5_user_properties(),
    )
        .prop_map(
            |(reason_code, session_expiry_interval, server_reference, reason_string, user_properties)| Disconnect {
                reason_code,
                session_expiry_interval,
                server_reference,
                reason_string,
                user_properties,
            },
        )
}

fn arb_v5_auth() -> impl Strategy<Value = Auth> {
    prop_oneof![
        // Minimal Success (no body on the wire).
        Just(Auth {
            reason_code: AuthReasonCode::Success,
            ..Default::default()
        }),
        // Success carrying properties (§3.15.2.2): method, data, reason string, user properties.
        (
            arb_text(),
            prop::option::of(arb_binary()),
            arb_opt_text(),
            arb_v5_user_properties(),
        )
            .prop_map(|(method, data, reason_string, user_properties)| Auth {
                reason_code: AuthReasonCode::Success,
                auth_method: Some(method),
                auth_data: data,
                reason_string,
                user_properties,
            }),
        // Continue / Re-authenticate require an Authentication Method (§3.15.2.2.2).
        (
            prop::sample::select(AUTH_REASONS).prop_filter("not Success", |r| *r != AuthReasonCode::Success),
            arb_text(),
            prop::option::of(arb_binary()),
            arb_v5_user_properties(),
            arb_opt_text(),
        )
            .prop_map(
                |(reason_code, auth_method, auth_data, user_properties, reason_string)| Auth {
                    reason_code,
                    auth_method: Some(auth_method),
                    auth_data,
                    user_properties,
                    reason_string,
                }
            ),
    ]
}

// --- CONNECT ----------------------------------------------------------------------------

fn arb_will_message() -> impl Strategy<Value = WillMessage> {
    (
        (arb_qos(), any::<bool>(), arb_text(), arb_binary()),
        (
            prop::option::of(any::<u32>()),
            prop::option::of(arb_binary()),
            prop::option::of(any::<u32>()),
            arb_opt_text(),
            arb_v5_user_properties(),
            prop::option::of(any::<bool>()),
            arb_opt_text(), // Response Topic: a valid Topic Name (no wildcards / NUL)
        ),
    )
        .prop_map(
            |(
                (qos, retain, topic, payload),
                (
                    will_delay_interval,
                    correlation_data,
                    message_expiry_interval,
                    content_type,
                    user_properties,
                    payload_format_indicator,
                    response_topic,
                ),
            )| WillMessage {
                qos,
                retain,
                topic,
                payload,
                will_delay_interval,
                correlation_data,
                message_expiry_interval,
                content_type,
                user_properties,
                payload_format_indicator,
                response_topic,
            },
        )
}

fn arb_v5_connect() -> impl Strategy<Value = Connect> {
    (
        (
            any::<bool>(),
            any::<u16>(),
            prop_oneof![Just(0u32), any::<u32>()],
            arb_auth_method_and_data(),
            any::<bool>(),
            any::<bool>(),
        ),
        (
            prop::option::of(arb_nonzero_u16()),
            any::<u16>(),
            arb_v5_user_properties(),
            prop::option::of(arb_nonzero_u32()),
            prop::option::of(arb_will_message()),
        ),
        (
            // Zero-length Client Identifier is valid in v5 (server assigns one, §3.1.3.1).
            prop_oneof![1 => Just(ByteString::new()), 4 => arb_text()],
            arb_opt_text(),
            // Password without User Name is valid in v5 (§3.1.2.9).
            prop::option::of(arb_binary()),
        ),
    )
        .prop_map(
            |(
                (clean_start, keep_alive, session_expiry_interval, (auth_method, auth_data), req_problem, req_response),
                (receive_max, topic_alias_max, user_properties, max_packet_size, will_message),
                (client_id, username, password),
            )| Connect {
                clean_start,
                keep_alive,
                session_expiry_interval,
                auth_method,
                auth_data,
                request_problem_info: req_problem,
                request_response_info: req_response,
                receive_max,
                topic_alias_max,
                user_properties,
                max_packet_size,
                will_message,
                client_id,
                username,
                password,
            },
        )
}

// --- PUBLISH ----------------------------------------------------------------------------

fn arb_v5_publish_properties() -> impl Strategy<Value = PublishProperties> {
    (
        prop::option::of(arb_binary()),
        prop::option::of(any::<u32>()),
        arb_opt_text(),
        arb_v5_user_properties(),
        any::<bool>(),
        arb_opt_text(), // Response Topic: a valid Topic Name (no wildcards / NUL)
    )
        .prop_map(
            |(
                correlation_data,
                message_expiry_interval,
                content_type,
                user_properties,
                payload_format_indicator,
                response_topic,
            )| PublishProperties {
                correlation_data,
                message_expiry_interval,
                content_type,
                user_properties,
                payload_format_indicator,
                response_topic,
            },
        )
}

fn arb_v5_publish(
    properties: impl Strategy<Value = PublishProperties>,
) -> impl Strategy<Value = (PublishMessage, PublishDelivery)> {
    (
        arb_qos(),
        any::<bool>(),
        any::<bool>(),
        arb_nonzero_u16(),
        arb_valid_topic(),
        arb_payload(),
        properties,
    )
        .prop_map(|(qos, dup, retain, packet_id, topic, payload, properties)| {
            let has_id = qos != QoS::AtMostOnce;
            (
                PublishMessage {
                    topic: ByteString::from(topic),
                    payload,
                    properties: properties.into(),
                },
                PublishDelivery {
                    dup: dup && has_id, // QoS 0: [MQTT-3.3.1-2]
                    retain,
                    qos,
                    packet_id: has_id.then_some(packet_id),
                },
            )
        })
}

fn arb_v5_publish_without_properties() -> impl Strategy<Value = (PublishMessage, PublishDelivery)> {
    arb_v5_publish(Just(PublishProperties::default()))
}

fn arb_v5_publish_with_properties() -> impl Strategy<Value = (PublishMessage, PublishDelivery)> {
    arb_v5_publish(arb_v5_publish_properties())
}

// --- SUBSCRIBE --------------------------------------------------------------------------

/// Topic Filter: plain, `+` / `#` wildcard (§4.7.1), or `$share` form (§4.8.2).
fn arb_topic_filter() -> impl Strategy<Value = ByteString> {
    prop_oneof![
        4 => arb_text(),
        1 => Just(ByteString::from_static("#")),
        1 => Just(ByteString::from_static("+")),
        1 => arb_valid_topic().prop_map(|t| ByteString::from(format!("{t}/+/x"))),
        1 => arb_valid_topic().prop_map(|t| ByteString::from(format!("{t}/#"))),
        1 => (arb_valid_topic(), arb_valid_topic())
            .prop_map(|(group, t)| ByteString::from(format!("$share/{}/{t}", group.replace('/', "_")))),
    ]
}

fn arb_v5_subscribe() -> impl Strategy<Value = Subscribe> {
    (
        arb_nonzero_u16(),
        prop::collection::vec(
            (
                arb_topic_filter(),
                arb_qos(),
                any::<bool>(),
                any::<bool>(),
                prop::sample::select(RETAIN_HANDLINGS),
            ),
            1..=8,
        ),
        prop::option::of(arb_subscription_identifier()),
        arb_v5_user_properties(),
    )
        .prop_map(|(packet_id, filters, id, user_properties)| Subscribe {
            packet_id,
            id,
            user_properties,
            topic_filters: filters
                .into_iter()
                .map(|(topic, qos, no_local, retain_as_published, retain_handling)| {
                    (
                        topic,
                        SubscriptionOptions {
                            qos,
                            no_local,
                            retain_as_published,
                            retain_handling,
                        },
                    )
                })
                .collect(),
        })
}

/// Subscription Identifier: Variable Byte Integer 1..=268_435_455 (§3.8.2.1.2).
fn arb_subscription_identifier() -> impl Strategy<Value = NonZeroU32> {
    (1u32..=268_435_455).prop_map(|v| NonZeroU32::new(v).expect("non-zero subscription id"))
}
