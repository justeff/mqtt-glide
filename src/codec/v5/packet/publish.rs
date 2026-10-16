//! MQTT PUBLISH (§3.3) — variable header, properties, and payload decode.
//!
//! # Errors
//! Rejects QoS 0 with DUP ([MQTT-3.3.1-2]), invalid Topic Names ([MQTT-3.3.2-2]),
//! empty Topic Name without Topic Alias (§3.3.2.1), and trailing/malformed
//! property blocks (§2.2.2.2, §2.1.4).

use std::{num::NonZeroU16, num::NonZeroU32};

use bytes::{Buf, Bytes};
use bytestring::ByteString;

use crate::codec::error::DecodeError;
use crate::codec::publish::{PublishMessage, PublishPacket, PublishPacketMeta, PublishProperties};
use crate::codec::types::QoS;
use crate::codec::utils::{self, Decode, Property};
use crate::codec::v5::{UserProperties, UserProperty, property_type as pt};

/// MQTT v5 PUBLISH wire properties (includes Topic Alias on the wire).
#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub(crate) struct PublishWireProperties {
    pub topic_alias: Option<NonZeroU16>,
    pub correlation_data: Option<Bytes>,
    pub message_expiry_interval: Option<u32>,
    pub content_type: Option<ByteString>,
    pub user_properties: UserProperties,
    pub payload_format_indicator: bool,
    pub response_topic: Option<ByteString>,
    pub subscription_ids: Vec<NonZeroU32>,
}

impl PublishPacket {
    /// Decode a PUBLISH packet body (variable header + properties + payload).
    pub(crate) fn decode(mut src: Bytes, packet_flags: u8) -> Result<Self, DecodeError> {
        let dup = (packet_flags & 0b1000) == 0b1000;
        let qos = QoS::try_from((packet_flags & 0b0110) >> 1)?; // [MQTT-3.3.1-4]
        if qos == QoS::AtMostOnce && dup {
            // [MQTT-3.3.1-2]
            return Err(DecodeError::MalformedPacket("PUBLISH QoS 0 with DUP"));
        }

        let topic = utils::decode_topic_name_allow_empty(&mut src)?;
        let packet_id = if qos == QoS::AtMostOnce {
            reject_qos0_packet_identifier(&src)?;
            None
        } else {
            Some(NonZeroU16::decode(&mut src)?) // packet id = 0 encountered
        };

        let properties = parse_publish_properties(&mut src)?;
        let wire_topic_alias = properties.as_ref().and_then(|p| p.topic_alias);
        if topic.is_empty() && wire_topic_alias.is_none() {
            // Protocol Error if Topic Name is zero length and there is no Topic Alias (§3.3.2.1).
            return Err(DecodeError::ProtocolViolation("empty topic without topic alias"));
        }
        let payload = src;
        let mut props = properties.unwrap_or_default();
        let subscription_ids = std::mem::take(&mut props.subscription_ids);

        Ok(PublishPacket {
            message: PublishMessage {
                topic,
                payload,
                properties: PublishProperties {
                    correlation_data: props.correlation_data,
                    message_expiry_interval: props.message_expiry_interval,
                    content_type: props.content_type,
                    user_properties: props.user_properties,
                    payload_format_indicator: props.payload_format_indicator,
                    response_topic: props.response_topic,
                },
            },
            meta: PublishPacketMeta {
                dup,
                retain: (packet_flags & 0b0001) == 0b0001,
                qos,
                packet_id,
            },
            wire_topic_alias,
            subscription_ids,
        })
    }
}

/// [MQTT-2.2.1-2] QoS 0 PUBLISH must not include a Packet Identifier before Property Length.
fn reject_qos0_packet_identifier(src: &Bytes) -> Result<(), DecodeError> {
    // Lean / common case: empty remainder or Property Length = 0 — no clone.
    if src.is_empty() || src[0] == 0 {
        return Ok(());
    }
    let mut at_property_length = src.clone();
    if utils::take_properties(&mut at_property_length).is_ok() {
        return Ok(());
    }
    // Two-byte Packet Identifier before the mandatory Property Length field.
    if src.remaining() >= 3 {
        let mut after_packet_id = src.slice(2..);
        if utils::take_properties(&mut after_packet_id).is_ok() {
            return Err(DecodeError::MalformedPacket(
                "QoS 0 PUBLISH must not include packet identifier",
            ));
        }
    }
    Ok(())
}

/// Parses the PUBLISH property block. Property Length `0` (common ingress case) returns
/// [`None`] without allocating property locals or empty vectors.
fn parse_publish_properties(src: &mut Bytes) -> Result<Option<PublishWireProperties>, DecodeError> {
    // Minimal encoding of length 0 is a single `0x00` byte ([MQTT-1.5.5-1]).
    if src.first() == Some(&0) {
        src.advance(1);
        return Ok(None);
    }

    let prop_src = &mut utils::take_properties(src)?;

    let mut message_expiry_interval = None;
    let mut topic_alias = None;
    let mut content_type = None;
    let mut correlation_data = None;
    let mut response_topic = None;
    let mut payload_format_indicator = None;
    let mut user_props = Vec::new();
    let mut subscription_ids = Vec::new();

    while prop_src.has_remaining() {
        match prop_src.get_u8() {
            pt::UTF8_PAYLOAD => payload_format_indicator.read_value(prop_src)?,
            pt::MSG_EXPIRY_INT => message_expiry_interval.read_value(prop_src)?,
            pt::CONTENT_TYPE => content_type.read_value(prop_src)?,
            pt::RESP_TOPIC => response_topic.read_value(prop_src)?,
            pt::CORR_DATA => correlation_data.read_value(prop_src)?,
            pt::SUB_ID => {
                // One per matching subscription that carried an identifier (§3.3.2.3.8).
                // An illegal 0 is skipped rather than failing the packet: a broker that sends
                // it does so for every redelivery, so a disconnect would loop forever.
                let id = utils::decode_variable_length_cursor(prop_src)?;
                match NonZeroU32::new(id) {
                    Some(id) => subscription_ids.push(id),
                    None => tracing::debug!("ignoring inbound PUBLISH Subscription Identifier 0"),
                }
            }
            pt::TOPIC_ALIAS => {
                let mut alias: Option<u16> = None;
                alias.read_value(prop_src)?;
                ensure!(
                    topic_alias.is_none(),
                    DecodeError::ProtocolViolation("property included more than once")
                );
                topic_alias = Some(NonZeroU16::new(alias.unwrap_or(0)).ok_or(DecodeError::TopicAliasInvalid)?);
            }
            pt::USER => user_props.push(UserProperty::decode(prop_src)?),
            _ => return Err(DecodeError::MalformedPacket("unknown PUBLISH property")),
        }
    }

    Ok(Some(PublishWireProperties {
        message_expiry_interval,
        topic_alias,
        content_type,
        correlation_data,
        subscription_ids,
        response_topic,
        payload_format_indicator: payload_format_indicator.unwrap_or(false),
        user_properties: user_props,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::v5::{DisconnectReasonCode, ToReasonCode};
    use bytes::{BufMut, BytesMut};

    use crate::codec::publish::{PublishDelivery, PublishMessage, PublishProperties};
    use crate::codec::utils::Encode;
    use crate::codec::v5::encode::encode_publish;

    const QOS0_FLAGS: u8 = 0;
    const QOS0_DUP_FLAGS: u8 = 0b1000;

    fn topic_and_prop0_payload(payload: &[u8]) -> Bytes {
        let mut body = BytesMut::new();
        ByteString::from_static("topic").encode(&mut body).unwrap();
        body.put_u8(0); // Property Length 0
        body.put(payload);
        body.freeze()
    }

    #[test]
    fn publish_decode_property_length_zero_yields_empty_properties() {
        let publish = PublishPacket::decode(topic_and_prop0_payload(b"data"), QOS0_FLAGS).unwrap();
        assert!(publish.message.properties.is_wire_empty());
        assert_eq!(publish.message.payload.as_ref(), b"data");
    }

    /// A PUBLISH with QoS 0 and DUP 1 is malformed ([MQTT-3.3.1-2]).
    #[test]
    fn publish_decode_qos0_dup_rejected() {
        let result = PublishPacket::decode(topic_and_prop0_payload(b"data"), QOS0_DUP_FLAGS);
        assert_eq!(
            result.map_err(|e| e.to_string()),
            Err(DecodeError::MalformedPacket("PUBLISH QoS 0 with DUP").to_string())
        );
    }

    /// §3.3.2.3.4: Topic Alias 0 is not permitted; the receiver answers with DISCONNECT 0x94.
    #[test]
    fn publish_decode_topic_alias_zero_is_topic_alias_invalid() {
        let mut body = BytesMut::new();
        ByteString::from_static("t").encode(&mut body).unwrap();
        body.put_u8(3); // Property Length
        body.put_u8(pt::TOPIC_ALIAS);
        body.put_u16(0);
        let error = PublishPacket::decode(body.freeze(), QOS0_FLAGS).expect_err("alias 0");
        assert!(matches!(error, DecodeError::TopicAliasInvalid), "{error:?}");
        assert_eq!(error.to_reason_code(), DisconnectReasonCode::TopicAliasInvalid);
    }

    /// §3.3.2.3.3: the Message Expiry Interval is a plain Four Byte Integer, and a broker
    /// forwards the remaining lifetime, which can be 0. It decodes instead of failing the packet.
    #[test]
    fn publish_decode_message_expiry_zero_is_kept() {
        let mut body = BytesMut::new();
        ByteString::from_static("t").encode(&mut body).unwrap();
        body.put_u8(5); // Property Length
        body.put_u8(pt::MSG_EXPIRY_INT);
        body.put_u32(0);
        body.put_slice(b"x");

        let publish = PublishPacket::decode(body.freeze(), QOS0_FLAGS).expect("expiry 0 is valid");
        assert_eq!(publish.message.properties.message_expiry_interval, Some(0));
    }

    #[test]
    fn publish_decode_keeps_server_subscription_identifier() {
        // Server→Client may carry Subscription Identifier; the decoder keeps it apart from the properties.
        let mut body = BytesMut::new();
        ByteString::from_static("topic").encode(&mut body).unwrap();
        body.put_u8(2); // Property Length
        body.put_u8(pt::SUB_ID);
        body.put_u8(1); // Subscription Identifier = 1
        body.put_slice(b"data");

        let publish = PublishPacket::decode(body.freeze(), QOS0_FLAGS).expect("inbound SUB_ID ok");
        assert_eq!(&*publish.message.topic, "topic");
        assert_eq!(publish.message.payload.as_ref(), b"data");
        assert!(publish.message.properties.is_wire_empty());
        assert_eq!(publish.subscription_ids, [NonZeroU32::new(1).unwrap()]);
    }

    #[test]
    fn publish_decode_subscription_identifier_zero_accepted_and_discarded() {
        let mut body = BytesMut::new();
        ByteString::from_static("topic").encode(&mut body).unwrap();
        body.put_u8(2); // Property Length
        body.put_u8(pt::SUB_ID);
        body.put_u8(0); // illegal per spec; tolerated inbound
        body.put_slice(b"data");

        let publish = PublishPacket::decode(body.freeze(), QOS0_FLAGS).expect("SUB_ID 0 ok");
        assert_eq!(&*publish.message.topic, "topic");
        assert_eq!(publish.message.payload.as_ref(), b"data");
        assert!(publish.subscription_ids.is_empty());
    }

    #[test]
    fn publish_decode_qos1_without_packet_identifier_rejected() {
        // QoS 1 PUBLISH: topic then Property Length 0 with no 2-byte Packet Identifier.
        const QOS1_FLAGS: u8 = 0b0010;
        let body = Bytes::from_static(&[0x00, 0x01, b't', 0x00]);
        let result = PublishPacket::decode(body, QOS1_FLAGS);
        assert!(matches!(result, Err(DecodeError::InvalidLength)), "{result:?}");
    }

    #[test]
    fn publish_decode_qos1_zero_packet_identifier_rejected() {
        const QOS1_FLAGS: u8 = 0b0010;
        let mut body = BytesMut::new();
        ByteString::from_static("t").encode(&mut body).unwrap();
        body.put_u16(0);
        body.put_u8(0);
        let result = PublishPacket::decode(body.freeze(), QOS1_FLAGS);
        assert!(matches!(result, Err(DecodeError::MalformedPacket(_))));
    }

    /// [MQTT-3.3.1-4] QoS bits 1,1 are not a valid PUBLISH.
    #[test]
    fn publish_decode_rejects_both_qos_bits_set() {
        const QOS3_FLAGS: u8 = 0b0110;
        let result = PublishPacket::decode(topic_and_prop0_payload(b""), QOS3_FLAGS);
        assert!(matches!(result, Err(DecodeError::MalformedPacket(_))));
    }

    #[rstest::rstest]
    #[case::high_bit_set(0x8001)]
    #[case::low(0x1234)]
    fn publish_decode_qos0_packet_identifier_prefix_rejected(#[case] packet_id: u16) {
        let mut body = BytesMut::new();
        ByteString::from_static("topic").encode(&mut body).unwrap();
        body.put_u16(packet_id);
        body.put_u8(0);
        body.put_slice(b"data");

        let result = PublishPacket::decode(body.freeze(), QOS0_FLAGS);
        assert_eq!(
            result.map_err(|e| e.to_string()),
            Err(DecodeError::MalformedPacket("QoS 0 PUBLISH must not include packet identifier").to_string())
        );
    }

    #[test]
    fn publish_decode_rejects_empty_topic_without_alias() {
        let src = Bytes::from_static(&[0x00, 0x00, 0x00]);
        assert!(matches!(
            PublishPacket::decode(src, 0),
            Err(DecodeError::ProtocolViolation("empty topic without topic alias"))
        ));
    }

    #[test]
    fn publish_decode_rejects_invalid_utf8_topic() {
        // Topic length 2 + invalid UTF-8 (overlong C0 80), Property Length 0, empty payload.
        let src = Bytes::from_static(&[0x00, 0x02, 0xC0, 0x80, 0x00]);
        assert!(matches!(
            PublishPacket::decode(src, QOS0_FLAGS),
            Err(DecodeError::MalformedPacket(_))
        ));
    }

    /// [MQTT-3.3.2-19] Content Type must be a UTF-8 Encoded String.
    #[test]
    fn publish_decode_rejects_invalid_utf8_content_type() {
        // Topic "t" + Property Length 5 + CONTENT_TYPE + invalid UTF-8 + empty payload.
        let mut body = BytesMut::new();
        ByteString::from_static("t").encode(&mut body).unwrap();
        body.put_u8(5); // Property Length
        body.put_u8(pt::CONTENT_TYPE);
        body.put_u16(2);
        body.put_slice(&[0xC0, 0x80]); // invalid UTF-8
        body.put_u8(0); // payload (empty)

        let result = PublishPacket::decode(body.freeze(), QOS0_FLAGS);
        assert!(matches!(result, Err(DecodeError::MalformedPacket(_))));
    }

    #[test]
    fn publish_encode_topic_name_is_first_variable_header_field() {
        // [MQTT-3.3.2-1] Topic Name length + bytes precede Packet Identifier and Property Length.
        let message = PublishMessage {
            topic: ByteString::from_static("ab"),
            payload: Bytes::from_static(b"x"),
            properties: PublishProperties::default(),
        };
        let delivery = PublishDelivery {
            dup: false,
            retain: false,
            qos: QoS::AtLeastOnce,
            packet_id: Some(NonZeroU16::new(9).unwrap()),
        };
        let mut buf = BytesMut::new();
        encode_publish(&message, delivery, 1024, &mut buf).unwrap();
        // Skip fixed header (type + remaining length)
        let body_start = buf.iter().position(|&b| b == b'a').expect("topic bytes");
        assert_eq!(
            &buf[body_start - 2..body_start + 2],
            &[0x00, 0x02, b'a', b'b'],
            "Topic Name UTF-8 length + bytes must be the first variable-header fields"
        );
        assert_eq!(
            &buf[body_start + 2..body_start + 4],
            &[0x00, 0x09],
            "Packet Identifier follows Topic Name for QoS > 0"
        );
    }

    #[test]
    fn publish_decode_accepts_empty_topic_with_alias() {
        // Topic Name length 0 + Topic Alias property 1
        let src = Bytes::from_static(&[0x00, 0x00, 0x03, 0x23, 0x00, 0x01, b'd']);
        let publish = PublishPacket::decode(src, 0).unwrap();
        assert!(publish.message.topic.is_empty());
        assert_eq!(publish.wire_topic_alias.map(NonZeroU16::get), Some(1));
        assert_eq!(publish.message.payload.as_ref(), b"d");
    }

    #[test]
    fn publish_decodes_every_property_from_spec_wire_bytes() {
        // Identifiers spelled out from §3.3.2.3, not taken from `property_type`.
        #[rustfmt::skip]
        let props: &[u8] = &[
            0x01, 0x01,                                    // Payload Format Indicator 1
            0x02, 0x00, 0x00, 0x00, 0x3C,                  // Message Expiry Interval 60
            0x23, 0x00, 0x05,                              // Topic Alias 5
            0x08, 0x00, 0x03, b'r', b'/', b'1',            // Response Topic
            0x09, 0x00, 0x02, 0x09, 0x08,                  // Correlation Data
            0x26, 0x00, 0x01, b'k', 0x00, 0x01, b'v',      // User Property
            0x0B, 0x81, 0x01,                              // Subscription Identifier 129
            0x03, 0x00, 0x03, b't', b'/', b'p',            // Content Type
        ];
        let mut body = vec![0x00, 0x03, b'a', b'/', b'b', 0x00, 0x07, props.len() as u8];
        body.extend_from_slice(props);
        body.extend_from_slice(b"hi");

        let publish = PublishPacket::decode(Bytes::from(body), 0b0011).unwrap();
        assert_eq!(
            publish,
            PublishPacket {
                message: PublishMessage {
                    topic: ByteString::from_static("a/b"),
                    payload: Bytes::from_static(b"hi"),
                    properties: PublishProperties {
                        correlation_data: Some(Bytes::from_static(&[0x09, 0x08])),
                        message_expiry_interval: Some(60),
                        content_type: Some(ByteString::from_static("t/p")),
                        user_properties: vec![("k", "v").into()],
                        payload_format_indicator: true,
                        response_topic: Some(ByteString::from_static("r/1")),
                    },
                },
                meta: PublishPacketMeta {
                    dup: false,
                    retain: true,
                    qos: QoS::AtLeastOnce,
                    packet_id: NonZeroU16::new(7),
                },
                wire_topic_alias: NonZeroU16::new(5),
                subscription_ids: vec![NonZeroU32::new(129).unwrap()],
            }
        );
    }
}
