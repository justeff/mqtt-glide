//! Property-based round-trip tests for the public MQTT v5 codec facade.
//!
//! Encodes generated packets, decodes the wire bytes back, and asserts equality — no
//! malformed-input / reject-path coverage here (see the codec module unit tests under
//! `src/codec/` for that).
//!
//! Generators are constrained to Lean-profile inputs:
//! - QoS 0 and 1 only
//! - No last will / LWT
//! - Valid UTF-8 topics and client identifiers
//! - Bounded topic (1..=128 chars) and payload (0..=4096 bytes) sizes
//! - v5: `topic_alias_max = 0`, no subscription identifiers on SUBSCRIBE/PUBLISH

use std::num::NonZeroU16;

use bytes::{Bytes, BytesMut};
use bytestring::ByteString;
use mqtt_glide::codec::DEFAULT_MAX_PACKET_SIZE;
use mqtt_glide::codec::{
    Auth, AuthReasonCode, Connect, ConnectAck, ConnectAckReason, DecodedFrame, Disconnect, DisconnectReasonCode,
    Packet, PubRelComp, PubRelCompReason, PublishAck, PublishAckReason, PublishDelivery, PublishMessage, PublishPacket,
    PublishPacketMeta, PublishProperties, QoS, Subscribe, SubscribeAck, SubscribeAckReason, SubscriptionOptions,
    Unsubscribe, UnsubscribeAck, UnsubscribeAckReason, UserProperties, V5Codec, encode_publish,
};
use proptest::prelude::*;
use proptest::strategy::Strategy;

const PROPTEST_CASES: u32 = 256;

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
    fn v5_pingreq_round_trip(_seed in any::<u8>()) {
        round_trip_v5(Packet::PingRequest);
    }

    #[test]
    fn v5_pingresp_round_trip(_seed in any::<u8>()) {
        round_trip_v5(Packet::PingResponse);
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
    let decoded = codec
        .decode_frame(&mut wire)
        .expect("decode must not error for generated wire")
        .expect("generated wire must contain a full packet");
    assert_eq!(decoded, expected);
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
    });
    let mut codec = V5Codec::with_max_packet_size(DEFAULT_MAX_PACKET_SIZE);
    let mut wire = BytesMut::new();
    encode_publish(&message, delivery, DEFAULT_MAX_PACKET_SIZE, &mut wire)
        .expect("encode must succeed for generated publish");
    let decoded = codec
        .decode_frame(&mut wire)
        .expect("decode must not error for generated wire")
        .expect("generated wire must contain a full packet");
    assert_eq!(decoded, expected);
}

fn arb_v5_user_properties() -> impl Strategy<Value = UserProperties> {
    prop::collection::vec((arb_valid_topic(), arb_valid_topic()), 0..=4).prop_map(|pairs| {
        pairs
            .into_iter()
            .map(|(key, value)| (ByteString::from(key), ByteString::from(value)))
            .collect()
    })
}

fn arb_optional_reason_string() -> impl Strategy<Value = Option<ByteString>> {
    prop_oneof![Just(None), arb_valid_topic().prop_map(|s| Some(ByteString::from(s))),]
}

fn arb_v5_connack() -> impl Strategy<Value = ConnectAck> {
    (any::<bool>(), arb_v5_connack_reason()).prop_map(|(session_present, reason_code)| ConnectAck {
        session_present,
        reason_code,
        max_qos: QoS::AtLeastOnce,
        retain_available: false,
        subscription_identifiers_available: false,
        topic_alias_max: 0,
        ..Default::default()
    })
}

fn arb_v5_connack_reason() -> impl Strategy<Value = ConnectAckReason> {
    prop_oneof![
        Just(ConnectAckReason::Success),
        Just(ConnectAckReason::BadUserNameOrPassword),
        Just(ConnectAckReason::NotAuthorized),
        Just(ConnectAckReason::ServerUnavailable),
    ]
}

fn arb_v5_publish_ack() -> impl Strategy<Value = PublishAck> {
    (
        arb_nonzero_packet_id(),
        arb_v5_publish_ack_reason(),
        arb_v5_user_properties(),
        arb_optional_reason_string(),
    )
        .prop_map(|(packet_id, reason_code, properties, reason_string)| PublishAck {
            packet_id,
            reason_code,
            properties,
            reason_string,
        })
}

fn arb_v5_publish_ack_reason() -> impl Strategy<Value = PublishAckReason> {
    prop_oneof![
        Just(PublishAckReason::Success),
        Just(PublishAckReason::NoMatchingSubscribers),
        Just(PublishAckReason::NotAuthorized),
        Just(PublishAckReason::UnspecifiedError),
    ]
}

fn arb_v5_pub_rel_comp() -> impl Strategy<Value = PubRelComp> {
    (
        arb_nonzero_packet_id(),
        arb_v5_pub_rel_comp_reason(),
        arb_v5_user_properties(),
        arb_optional_reason_string(),
    )
        .prop_map(|(packet_id, reason_code, properties, reason_string)| PubRelComp {
            packet_id,
            reason_code,
            properties,
            reason_string,
        })
}

fn arb_v5_pub_rel_comp_reason() -> impl Strategy<Value = PubRelCompReason> {
    prop_oneof![
        Just(PubRelCompReason::Success),
        Just(PubRelCompReason::PacketIdNotFound)
    ]
}

fn arb_v5_suback() -> impl Strategy<Value = SubscribeAck> {
    (
        arb_nonzero_packet_id(),
        prop::collection::vec(arb_v5_suback_reason(), 1..=8),
        arb_v5_user_properties(),
        arb_optional_reason_string(),
    )
        .prop_map(|(packet_id, status, properties, reason_string)| SubscribeAck {
            packet_id,
            status,
            properties,
            reason_string,
        })
}

fn arb_v5_suback_reason() -> impl Strategy<Value = SubscribeAckReason> {
    prop_oneof![
        Just(SubscribeAckReason::GrantedQos0),
        Just(SubscribeAckReason::GrantedQos1),
        Just(SubscribeAckReason::NotAuthorized),
        Just(SubscribeAckReason::UnspecifiedError),
    ]
}

fn arb_v5_unsubscribe() -> impl Strategy<Value = Unsubscribe> {
    (
        arb_nonzero_packet_id(),
        prop::collection::vec(arb_valid_topic(), 1..=8),
        arb_v5_user_properties(),
    )
        .prop_map(|(packet_id, topic_filters, user_properties)| Unsubscribe {
            packet_id,
            topic_filters: topic_filters.into_iter().map(ByteString::from).collect(),
            user_properties,
        })
}

fn arb_v5_unsuback() -> impl Strategy<Value = UnsubscribeAck> {
    (
        arb_nonzero_packet_id(),
        prop::collection::vec(arb_v5_unsuback_reason(), 1..=8),
        arb_v5_user_properties(),
        arb_optional_reason_string(),
    )
        .prop_map(|(packet_id, status, properties, reason_string)| UnsubscribeAck {
            packet_id,
            status,
            properties,
            reason_string,
        })
}

fn arb_v5_unsuback_reason() -> impl Strategy<Value = UnsubscribeAckReason> {
    prop_oneof![
        Just(UnsubscribeAckReason::Success),
        Just(UnsubscribeAckReason::NoSubscriptionExisted),
        Just(UnsubscribeAckReason::NotAuthorized),
    ]
}

fn arb_v5_disconnect() -> impl Strategy<Value = Disconnect> {
    (
        arb_v5_disconnect_reason(),
        prop_oneof![Just(None), any::<u32>().prop_map(Some)],
        arb_optional_reason_string(),
        arb_v5_user_properties(),
    )
        .prop_map(
            |(reason_code, session_expiry_interval_secs, reason_string, user_properties)| Disconnect {
                reason_code,
                session_expiry_interval_secs,
                server_reference: None,
                reason_string,
                user_properties,
            },
        )
}

fn arb_v5_disconnect_reason() -> impl Strategy<Value = DisconnectReasonCode> {
    prop_oneof![
        Just(DisconnectReasonCode::NormalDisconnection),
        Just(DisconnectReasonCode::NotAuthorized),
        Just(DisconnectReasonCode::KeepAliveTimeout),
        Just(DisconnectReasonCode::UnspecifiedError),
    ]
}

fn arb_v5_auth() -> impl Strategy<Value = Auth> {
    prop_oneof![
        Just(Auth {
            reason_code: AuthReasonCode::Success,
            ..Default::default()
        }),
        (
            prop_oneof![Just(AuthReasonCode::ContinueAuth), Just(AuthReasonCode::ReAuth)],
            arb_valid_topic().prop_map(ByteString::from),
            prop::collection::vec(any::<u8>(), 0..=32).prop_map(Bytes::from),
            arb_v5_user_properties(),
            arb_optional_reason_string(),
        )
            .prop_map(
                |(reason_code, auth_method, auth_data, user_properties, reason_string)| Auth {
                    reason_code,
                    auth_method: Some(auth_method),
                    auth_data: if auth_data.is_empty() { None } else { Some(auth_data) },
                    user_properties,
                    reason_string,
                }
            ),
    ]
}

fn arb_valid_topic() -> impl Strategy<Value = String> {
    // MQTT topic names are UTF-8; keep to a conservative printable subset.
    const TOPIC_CHARS: &[char] = &[
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
        'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R',
        'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '/', '_', '-',
    ];
    prop::collection::vec(prop::sample::select(TOPIC_CHARS), 1..=128).prop_map(|chars| chars.into_iter().collect())
}

fn arb_payload() -> impl Strategy<Value = Bytes> {
    prop::collection::vec(any::<u8>(), 0..=4096).prop_map(Bytes::from)
}

fn arb_lean_qos() -> impl Strategy<Value = QoS> {
    prop_oneof![Just(QoS::AtMostOnce), Just(QoS::AtLeastOnce)]
}

fn arb_nonzero_packet_id() -> impl Strategy<Value = NonZeroU16> {
    (1u16..=u16::MAX).prop_map(|id| NonZeroU16::new(id).expect("non-zero packet id"))
}

fn arb_optional_credentials() -> impl Strategy<Value = (Option<ByteString>, Option<Bytes>)> {
    prop_oneof![
        Just((None, None)),
        arb_valid_topic().prop_map(|s| (Some(ByteString::from(s)), None)),
        (
            arb_valid_topic().prop_map(ByteString::from),
            prop::collection::vec(any::<u8>(), 1..=64).prop_map(Bytes::from)
        )
            .prop_map(|(user, pass)| (Some(user), Some(pass))),
    ]
}

fn arb_v5_connect() -> impl Strategy<Value = Connect> {
    let client_id = arb_valid_topic();
    (
        any::<bool>(),
        any::<u16>(),
        prop_oneof![Just(0u32), any::<u32>()],
        client_id,
        arb_optional_credentials(),
    )
        .prop_map(
            |(clean_start, keep_alive, session_expiry_interval_secs, client_id, (username, password))| Connect {
                clean_start,
                keep_alive,
                session_expiry_interval_secs,
                auth_method: None,
                auth_data: None,
                request_problem_info: true,
                request_response_info: false,
                receive_max: None,
                topic_alias_max: 0,
                user_properties: Vec::new(),
                max_packet_size: None,
                last_will: None,
                client_id: ByteString::from(client_id),
                username,
                password,
            },
        )
}

fn arb_v5_publish_without_properties() -> impl Strategy<Value = (PublishMessage, PublishDelivery)> {
    prop_oneof![
        (any::<bool>(), arb_valid_topic(), arb_payload(),).prop_map(|(retain, topic, payload)| {
            (
                PublishMessage {
                    topic: ByteString::from(topic),
                    payload,
                    properties: PublishProperties::default(),
                },
                PublishDelivery {
                    dup: false, // QoS 0: [MQTT-3.3.1-2]
                    retain,
                    qos: QoS::AtMostOnce,
                    packet_id: None,
                },
            )
        }),
        (
            any::<bool>(),
            any::<bool>(),
            arb_valid_topic(),
            arb_nonzero_packet_id(),
            arb_payload(),
        )
            .prop_map(|(dup, retain, topic, packet_id, payload)| {
                (
                    PublishMessage {
                        topic: ByteString::from(topic),
                        payload,
                        properties: PublishProperties::default(),
                    },
                    PublishDelivery {
                        dup,
                        retain,
                        qos: QoS::AtLeastOnce,
                        packet_id: Some(packet_id),
                    },
                )
            }),
    ]
}

fn arb_v5_publish_with_properties() -> impl Strategy<Value = (PublishMessage, PublishDelivery)> {
    prop_oneof![
        (
            any::<bool>(),
            arb_valid_topic(),
            arb_payload(),
            prop::collection::vec((arb_valid_topic(), arb_valid_topic()), 0..=4),
            any::<bool>(),
        )
            .prop_map(|(retain, topic, payload, user_properties, is_utf8_payload)| {
                let user_properties = user_properties
                    .into_iter()
                    .map(|(key, value)| (ByteString::from(key), ByteString::from(value)))
                    .collect();
                (
                    PublishMessage {
                        topic: ByteString::from(topic),
                        payload,
                        properties: PublishProperties {
                            user_properties,
                            is_utf8_payload,
                            ..Default::default()
                        },
                    },
                    PublishDelivery {
                        dup: false, // QoS 0: [MQTT-3.3.1-2]
                        retain,
                        qos: QoS::AtMostOnce,
                        packet_id: None,
                    },
                )
            }),
        (
            any::<bool>(),
            any::<bool>(),
            arb_valid_topic(),
            arb_nonzero_packet_id(),
            arb_payload(),
            prop::collection::vec((arb_valid_topic(), arb_valid_topic()), 0..=4),
            any::<bool>(),
        )
            .prop_map(
                |(dup, retain, topic, packet_id, payload, user_properties, is_utf8_payload)| {
                    let user_properties = user_properties
                        .into_iter()
                        .map(|(key, value)| (ByteString::from(key), ByteString::from(value)))
                        .collect();
                    (
                        PublishMessage {
                            topic: ByteString::from(topic),
                            payload,
                            properties: PublishProperties {
                                user_properties,
                                is_utf8_payload,
                                ..Default::default()
                            },
                        },
                        PublishDelivery {
                            dup,
                            retain,
                            qos: QoS::AtLeastOnce,
                            packet_id: Some(packet_id),
                        },
                    )
                }
            ),
    ]
}

fn arb_v5_subscribe() -> impl Strategy<Value = Subscribe> {
    (
        arb_nonzero_packet_id(),
        prop::collection::vec((arb_valid_topic(), arb_lean_qos(), any::<bool>(), any::<bool>()), 1..=8),
        prop::collection::vec((arb_valid_topic(), arb_valid_topic()), 0..=4),
    )
        .prop_map(|(packet_id, filters, user_properties)| {
            let topic_filters = filters
                .into_iter()
                .map(|(topic, qos, no_local, retain_as_published)| {
                    (
                        ByteString::from(topic),
                        SubscriptionOptions {
                            qos,
                            no_local,
                            retain_as_published,
                            ..Default::default()
                        },
                    )
                })
                .collect();
            let user_properties = user_properties
                .into_iter()
                .map(|(key, value)| (ByteString::from(key), ByteString::from(value)))
                .collect();
            Subscribe {
                packet_id,
                id: None,
                user_properties,
                topic_filters,
            }
        })
}
