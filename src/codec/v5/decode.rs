use bytes::Bytes;
use bytestring::ByteString;

use super::{UserProperty, packet::*};
use crate::codec::error::DecodeError;
use crate::codec::publish::PublishPacket;
use crate::codec::types::packet_type;
use crate::codec::utils::{Decode, ensure_empty_body, ensure_exact_fixed_header};

/// Dispatches MQTT v5 packet decoding based on the first byte's packet type
pub(super) fn decode_packet(mut src: Bytes, first_byte: u8) -> Result<DecodedFrame, DecodeError> {
    match first_byte >> 4 {
        0x1 => {
            ensure_exact_fixed_header(first_byte, packet_type::CONNECT)?; // [MQTT-2.1.3-1]
            Ok(DecodedFrame::Control(Packet::Connect(Box::new(Connect::decode(
                &mut src,
            )?))))
        }
        0x2 => {
            ensure_exact_fixed_header(first_byte, packet_type::CONNACK)?; // [MQTT-2.1.3-1]
            Ok(DecodedFrame::Control(Packet::ConnectAck(Box::new(ConnectAck::decode(
                &mut src,
            )?))))
        }
        0x3 => {
            ensure!(
                (packet_type::PUBLISH_START..=packet_type::PUBLISH_END).contains(&first_byte),
                DecodeError::MalformedPacket("invalid encoding")
            );
            Ok(DecodedFrame::Publish(PublishPacket::decode(
                src,
                first_byte & 0b0000_1111,
            )?))
        }
        0x4 => {
            ensure_exact_fixed_header(first_byte, packet_type::PUBACK)?; // [MQTT-2.1.3-1]
            Ok(DecodedFrame::Control(Packet::PublishAck(PublishAck::decode(&mut src)?)))
        }
        0x5 => {
            ensure_exact_fixed_header(first_byte, packet_type::PUBREC)?; // [MQTT-2.1.3-1]
            Ok(DecodedFrame::Control(Packet::PublishReceived(PublishAck::decode(
                &mut src,
            )?)))
        }
        0x6 => {
            ensure_exact_fixed_header(first_byte, packet_type::PUBREL)?; // [MQTT-3.6.1-1]
            Ok(DecodedFrame::Control(Packet::PublishRelease(PubRelComp::decode(
                &mut src,
            )?)))
        }
        0x7 => {
            ensure_exact_fixed_header(first_byte, packet_type::PUBCOMP)?; // [MQTT-2.1.3-1]
            Ok(DecodedFrame::Control(Packet::PublishComplete(PubRelComp::decode(
                &mut src,
            )?)))
        }
        0x8 => {
            ensure_exact_fixed_header(first_byte, packet_type::SUBSCRIBE)?; // [MQTT-3.8.1-1]
            Ok(DecodedFrame::Control(Packet::Subscribe(Subscribe::decode(&mut src)?)))
        }
        0x9 => {
            ensure_exact_fixed_header(first_byte, packet_type::SUBACK)?; // [MQTT-2.1.3-1]
            Ok(DecodedFrame::Control(Packet::SubscribeAck(SubscribeAck::decode(
                &mut src,
            )?)))
        }
        0xA => {
            ensure_exact_fixed_header(first_byte, packet_type::UNSUBSCRIBE)?; // [MQTT-3.10.1-1]
            Ok(DecodedFrame::Control(Packet::Unsubscribe(Unsubscribe::decode(
                &mut src,
            )?)))
        }
        0xB => {
            ensure_exact_fixed_header(first_byte, packet_type::UNSUBACK)?; // [MQTT-2.1.3-1]
            Ok(DecodedFrame::Control(Packet::UnsubscribeAck(UnsubscribeAck::decode(
                &mut src,
            )?)))
        }
        0xC => {
            ensure_exact_fixed_header(first_byte, packet_type::PINGREQ)?;
            ensure_empty_body(&mut src)?;
            Ok(DecodedFrame::Control(Packet::PingRequest))
        }
        0xD => {
            ensure_exact_fixed_header(first_byte, packet_type::PINGRESP)?;
            ensure_empty_body(&mut src)?;
            Ok(DecodedFrame::Control(Packet::PingResponse))
        }
        0xE => {
            ensure_exact_fixed_header(first_byte, packet_type::DISCONNECT)?; // [MQTT-3.14.1-1]
            Ok(DecodedFrame::Control(Packet::Disconnect(Disconnect::decode(&mut src)?)))
        }
        0xF => {
            ensure_exact_fixed_header(first_byte, packet_type::AUTH)?; // [MQTT-3.15.1-1]
            Ok(DecodedFrame::Control(Packet::Auth(Auth::decode(&mut src)?)))
        }
        _ => Err(DecodeError::UnsupportedPacketType),
    }
}

impl Decode for UserProperty {
    fn decode(src: &mut Bytes) -> Result<Self, DecodeError> {
        let key = ByteString::decode(src)?;
        let val = ByteString::decode(src)?;
        Ok((key, val))
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use std::num::{NonZeroU16, NonZeroU32};
    use tokio_util::codec::Encoder;

    use super::*;
    use crate::codec::publish::{PublishDelivery, PublishMessage, PublishPacket, PublishPacketMeta, PublishProperties};
    use crate::codec::types::QoS;
    use crate::codec::utils::decode_variable_length;
    use crate::codec::v5::UserProperties;
    use crate::codec::v5::encode::encode_publish;
    use rstest::rstest;

    fn packet_id(v: u16) -> NonZeroU16 {
        NonZeroU16::new(v).unwrap()
    }

    fn control(packet: Packet) -> DecodedFrame {
        DecodedFrame::Control(packet)
    }

    fn publish_frame(packet: PublishPacket) -> DecodedFrame {
        DecodedFrame::Publish(packet)
    }

    fn assert_decode_frame<B: AsRef<[u8]>>(bytes: B, res: DecodedFrame) {
        let bytes = bytes.as_ref();
        let fixed = bytes[0];
        let (_len, consumed) = decode_variable_length(&bytes[1..]).unwrap().unwrap();
        let cur = Bytes::copy_from_slice(&bytes[consumed + 1..]);
        let mut tmp = BytesMut::with_capacity(4096);
        match &res {
            DecodedFrame::Control(packet) => {
                Encoder::encode(
                    &mut crate::codec::v5::codec::V5Codec::default(),
                    packet.clone(),
                    &mut tmp,
                )
                .unwrap();
            }
            DecodedFrame::Publish(pkt) => {
                let delivery = PublishDelivery {
                    dup: pkt.meta.dup,
                    retain: pkt.meta.retain,
                    qos: pkt.meta.qos,
                    packet_id: pkt.meta.packet_id,
                };
                encode_publish(&pkt.message, delivery, 0, &mut tmp).unwrap();
            }
        }
        let decoded = decode_packet(cur, fixed);
        let res = Ok(&res);
        if decoded.as_ref().map_err(|e| e.to_string()) != res {
            panic!(
                "decoded packet does not match expectations.\nexpected: {:?}\nactual: {:?}\nencoding output for expected: {:X?}",
                res,
                decoded,
                tmp.as_ref()
            );
        }
    }

    #[test]
    fn test_decode_connect_packets() {
        assert_eq!(
            Connect::decode(&mut Bytes::from_static(
                b"\x00\x04MQTT\x05\xC0\x00\x3C\x00\x00\x0512345\x00\x04user\x00\x04pass"
            ))
            .unwrap(),
            Connect {
                clean_start: false,
                keep_alive: 60,
                client_id: ByteString::from_static("12345"),
                last_will: None,
                username: Some(ByteString::from_static("user")),
                password: Some(Bytes::from_static(&b"pass"[..])),
                session_expiry_interval: 0,
                auth_method: None,
                auth_data: None,
                request_problem_info: true,
                request_response_info: false,
                receive_max: None,
                topic_alias_max: 0,
                user_properties: Vec::new(),
                max_packet_size: None,
            }
        );

        assert_eq!(
            Connect::decode(&mut Bytes::from_static(
                b"\x00\x04MQTT\x05\x14\x00\x3C\x00\x00\x0512345\x00\x00\x05topic\x00\x07message"
            ))
            .unwrap(),
            Connect {
                clean_start: false,
                keep_alive: 60,
                client_id: ByteString::from_static("12345"),
                last_will: Some(LastWill {
                    qos: QoS::ExactlyOnce,
                    retain: false,
                    topic: ByteString::from_static("topic"),
                    payload: Bytes::from_static(&b"message"[..]),
                    will_delay_interval: None,
                    correlation_data: None,
                    message_expiry_interval: None,
                    content_type: None,
                    user_properties: Vec::new(),
                    payload_format_indicator: None,
                    response_topic: None,
                }),
                username: None,
                password: None,
                session_expiry_interval: 0,
                auth_method: None,
                auth_data: None,
                request_problem_info: true,
                request_response_info: false,
                receive_max: None,
                topic_alias_max: 0,
                user_properties: Vec::new(),
                max_packet_size: None,
            }
        );

        assert_eq!(
            Connect::decode(&mut Bytes::from_static(b"\x00\x02MQ00000000000000000000"))
                .map_err(|e| matches!(e, DecodeError::InvalidProtocol)),
            Err(true),
        );
        assert_eq!(
            Connect::decode(&mut Bytes::from_static(b"\x00\x04MQAA00000000000000000000"))
                .map_err(|e| matches!(e, DecodeError::InvalidProtocol)),
            Err(true),
        );
        assert_eq!(
            Connect::decode(&mut Bytes::from_static(b"\x00\x04MQTT\x0300000000000000000000"))
                .map_err(|e| matches!(e, DecodeError::UnsupportedProtocolLevel)),
            Err(true),
        );
        assert_eq!(
            Connect::decode(&mut Bytes::from_static(b"\x00\x04MQTT\x05\xff00000000000000000000"))
                .map_err(|e| matches!(e, DecodeError::ConnectReservedFlagSet)),
            Err(true)
        );

        assert_eq!(
            ConnectAck::decode(&mut Bytes::from_static(b"\x01\x86\x00")).unwrap(),
            ConnectAck {
                session_present: true,
                reason_code: ConnectAckReason::BadUserNameOrPassword,
                ..ConnectAck::default()
            }
        );

        assert_eq!(
            ConnectAck::decode(&mut Bytes::from_static(b"\x03\x86\x00"))
                .map_err(|e| matches!(e, DecodeError::ConnectAckReservedFlagSet)),
            Err(true)
        );

        assert_decode_frame(
            b"\x20\x03\x01\x86\x00",
            control(Packet::ConnectAck(Box::new(ConnectAck {
                session_present: true,
                reason_code: ConnectAckReason::BadUserNameOrPassword,
                ..ConnectAck::default()
            }))),
        );

        assert_decode_frame([0b1110_0000, 0], control(Packet::Disconnect(Disconnect::default())));
    }

    #[test]
    fn test_decode_publish_packets() {
        //assert_eq!(
        //    decode_publish_packet(b"\x00\x05topic\x12\x34"),
        //    Done(&b""[..], ("topic".to_owned(), 0x1234))
        //);

        assert_decode_frame(
            b"\x3d\x0E\x00\x05topic\x43\x21\x00data",
            publish_frame(PublishPacket {
                message: PublishMessage {
                    topic: ByteString::from_static("topic"),
                    payload: Bytes::from_static(b"data"),
                    properties: PublishProperties::default(),
                },
                meta: PublishPacketMeta {
                    dup: true,
                    retain: true,
                    qos: QoS::ExactlyOnce,
                    packet_id: Some(packet_id(0x4321)),
                },
                wire_topic_alias: None,
            }),
        );

        assert_decode_frame(
            b"\x30\x0C\x00\x05topic\x00data",
            publish_frame(PublishPacket {
                message: PublishMessage {
                    topic: ByteString::from_static("topic"),
                    payload: Bytes::from_static(b"data"),
                    properties: PublishProperties::default(),
                },
                meta: PublishPacketMeta {
                    dup: false,
                    retain: false,
                    qos: QoS::AtMostOnce,
                    packet_id: None,
                },
                wire_topic_alias: None,
            }),
        );

        assert_decode_frame(
            b"\x40\x02\x43\x21",
            control(Packet::PublishAck(PublishAck {
                packet_id: packet_id(0x4321),
                reason_code: PublishAckReason::Success,
                properties: UserProperties::default(),
                reason_string: None,
            })),
        );
        assert_decode_frame(
            b"\x50\x02\x43\x21",
            control(Packet::PublishReceived(PublishAck {
                packet_id: packet_id(0x4321),
                reason_code: PublishAckReason::Success,
                properties: UserProperties::default(),
                reason_string: None,
            })),
        );
        assert_decode_frame(
            b"\x62\x02\x43\x21",
            control(Packet::PublishRelease(PubRelComp {
                packet_id: packet_id(0x4321),
                reason_code: PubRelCompReason::Success,
                properties: UserProperties::default(),
                reason_string: None,
            })),
        );
        assert_decode_frame(
            b"\x70\x02\x43\x21",
            control(Packet::PublishComplete(PubRelComp {
                packet_id: packet_id(0x4321),
                reason_code: PubRelCompReason::Success,
                properties: UserProperties::default(),
                reason_string: None,
            })),
        );
    }

    #[test]
    fn test_decode_subscribe_packets() {
        let p = Packet::Subscribe(Subscribe {
            packet_id: packet_id(0x1234),
            topic_filters: vec![
                (
                    ByteString::from_static("test"),
                    SubscriptionOptions {
                        qos: QoS::AtLeastOnce,
                        no_local: false,
                        retain_as_published: false,
                        retain_handling: RetainHandling::AtSubscribe,
                    },
                ),
                (
                    ByteString::from_static("filter"),
                    SubscriptionOptions {
                        qos: QoS::ExactlyOnce,
                        no_local: false,
                        retain_as_published: false,
                        retain_handling: RetainHandling::AtSubscribe,
                    },
                ),
            ],
            id: None,
            user_properties: Vec::new(),
        });

        assert_decode_frame(b"\x82\x13\x12\x34\x00\x00\x04test\x01\x00\x06filter\x02", control(p));

        let p = Packet::Subscribe(Subscribe {
            packet_id: packet_id(0x1234),
            topic_filters: vec![
                (
                    ByteString::from_static("test"),
                    SubscriptionOptions {
                        qos: QoS::AtLeastOnce,
                        no_local: false,
                        retain_as_published: false,
                        retain_handling: RetainHandling::AtSubscribe,
                    },
                ),
                (
                    ByteString::from_static("filter"),
                    SubscriptionOptions {
                        qos: QoS::ExactlyOnce,
                        no_local: false,
                        retain_as_published: false,
                        retain_handling: RetainHandling::AtSubscribe,
                    },
                ),
            ],
            id: Some(NonZeroU32::new(1).unwrap()),
            user_properties: Vec::new(),
        });

        assert_decode_frame(
            b"\x82\x15\x12\x34\x02\x0b\x01\x00\x04test\x01\x00\x06filter\x02",
            control(p),
        );

        let p = Packet::SubscribeAck(SubscribeAck {
            packet_id: packet_id(0x1234),
            status: vec![
                SubscribeAckReason::GrantedQos1,
                SubscribeAckReason::UnspecifiedError,
                SubscribeAckReason::GrantedQos2,
            ],
            properties: UserProperties::default(),
            reason_string: None,
        });

        assert_decode_frame(b"\x90\x05\x12\x34\x00\x01\x80\x02", control(p));

        let p = Packet::Unsubscribe(Unsubscribe {
            packet_id: packet_id(0x1234),
            topic_filters: vec![ByteString::from_static("test"), ByteString::from_static("filter")],
            user_properties: UserProperties::default(),
        });

        assert_eq!(
            Packet::Unsubscribe(
                Unsubscribe::decode(&mut Bytes::from_static(b"\x12\x34\x00\x00\x04test\x00\x06filter")).unwrap()
            ),
            p.clone()
        );
        assert_decode_frame(b"\xa2\x11\x12\x34\x00\x00\x04test\x00\x06filter", control(p));

        assert_decode_frame(
            b"\xb0\x04\x43\x21\x00\x00",
            control(Packet::UnsubscribeAck(UnsubscribeAck {
                packet_id: packet_id(0x4321),
                properties: UserProperties::default(),
                reason_string: None,
                status: vec![UnsubscribeAckReason::Success],
            })),
        );
    }

    #[test]
    fn test_decode_ping_packets() {
        assert_decode_frame(b"\xc0\x00", control(Packet::PingRequest));
        assert_decode_frame(b"\xd0\x00", control(Packet::PingResponse));
    }

    fn decode_wire(bytes: &[u8]) -> Result<DecodedFrame, DecodeError> {
        let fixed = bytes[0];
        let (_len, consumed) = decode_variable_length(&bytes[1..]).unwrap().unwrap();
        let cur = Bytes::copy_from_slice(&bytes[consumed + 1..]);
        decode_packet(cur, fixed)
    }

    /// Reserved Fixed Header flag bits must match the packet type ([MQTT-2.1.3-1] and per-type rules).
    #[rstest]
    #[case::puback_reserved_flag_set(&[0x41, 0x02, 0x43, 0x21])]
    #[case::subscribe_reserved_flag_set(&[
        0x83, 0x02, 0x12, 0x34, 0x00, 0x00, 0x04, b't', b'e', b's', b't', 0x01,
    ])]
    #[case::disconnect_reserved_flag_set(&[0xE1, 0x00])]
    #[case::connect_reserved_flag_set(&[0x11, 0x02, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05])]
    #[case::connack_reserved_flag_set(&[0x21, 0x03, 0x01, 0x86, 0x00])]
    #[case::suback_reserved_flag_set(&[0x91, 0x02, 0x12, 0x34, 0x00])]
    #[case::unsuback_reserved_flag_set(&[0xB1, 0x02, 0x12, 0x34])]
    // [MQTT-3.10.1-1] UNSUBSCRIBE reserved flags must be 0,0,1,0 (0xA2).
    #[case::unsubscribe_flags_all_clear(&[0xA0, 0x06, 0x12, 0x34, 0x00, 0x00, 0x01, b't'])]
    #[case::unsubscribe_flags_extra_bit(&[0xA3, 0x06, 0x12, 0x34, 0x00, 0x00, 0x01, b't'])]
    fn test_decode_rejects_malformed_fixed_headers(#[case] wire: &[u8]) {
        assert!(matches!(decode_wire(wire), Err(DecodeError::MalformedPacket(_))));
    }

    #[rstest]
    #[case::pingreq(&[0xC0, 0x01, 0x00])]
    #[case::pingresp(&[0xD0, 0x01, 0x00])]
    fn test_decode_rejects_ping_non_empty_body(#[case] wire: &[u8]) {
        assert!(matches!(decode_wire(wire), Err(DecodeError::MalformedPacket(_))));
    }

    /// [MQTT-1.5.7-1] Both strings in a UTF-8 String Pair must be well-formed UTF-8.
    #[rstest]
    #[case::invalid_key(&[0x00, 0x02, 0xC0, 0x80, 0x00, 0x01, b'a'])]
    #[case::invalid_value(&[0x00, 0x01, b'a', 0x00, 0x02, 0xC0, 0x80])]
    fn user_property_decode_rejects_invalid_utf8(#[case] wire: &[u8]) {
        let mut src = Bytes::copy_from_slice(wire);
        assert!(matches!(
            UserProperty::decode(&mut src),
            Err(DecodeError::MalformedPacket(_))
        ));
    }
}
