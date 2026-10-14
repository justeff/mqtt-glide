//! MQTT PUBLISH (§3.3) — variable header, properties, and payload decode.
//!
//! # Errors
//! Rejects QoS 0 with DUP ([MQTT-3.3.1-2]), invalid Topic Names ([MQTT-3.3.2-2]),
//! empty Topic Name without Topic Alias (§3.3.2.1), and trailing/malformed
//! property blocks ([MQTT-2.2.2-2], §2.1.4).

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
    pub message_expiry_interval: Option<NonZeroU32>,
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
            return Err(DecodeError::MalformedPacket("empty topic without topic alias"));
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

/// Converts a `UserProperties` vector into `PublishWireProperties`.
impl std::convert::From<UserProperties> for PublishWireProperties {
    fn from(props: UserProperties) -> Self {
        PublishWireProperties {
            user_properties: props,
            ..Default::default()
        }
    }
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
                // An illegal 0 from a lenient peer is skipped rather than failing the packet.
                let id = utils::decode_variable_length_cursor(prop_src)?;
                match NonZeroU32::new(id) {
                    Some(id) => subscription_ids.push(id),
                    None => tracing::debug!("ignoring inbound PUBLISH Subscription Identifier 0"),
                }
            }
            pt::TOPIC_ALIAS => topic_alias.read_value(prop_src)?,
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
    use bytes::{BufMut, BytesMut};

    use crate::codec::error::EncodeError;
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
    fn publish_decode_qos0_ok() {
        let result = PublishPacket::decode(topic_and_prop0_payload(b"data"), QOS0_FLAGS);
        assert!(result.is_ok());
    }

    #[test]
    fn publish_decode_property_length_zero_yields_empty_properties() {
        let publish = PublishPacket::decode(topic_and_prop0_payload(b"data"), QOS0_FLAGS).unwrap();
        assert!(publish.message.properties.is_wire_empty());
        assert_eq!(publish.message.payload.as_ref(), b"data");
    }

    #[test]
    fn publish_decode_qos0_dup_rejected() {
        let result = PublishPacket::decode(topic_and_prop0_payload(b"data"), QOS0_DUP_FLAGS);
        assert_eq!(
            result.map_err(|e| e.to_string()),
            Err(DecodeError::MalformedPacket("PUBLISH QoS 0 with DUP").to_string())
        );
    }

    #[test]
    fn publish_decode_server_subscription_identifier_accepted_and_discarded() {
        // Server→Client may carry Subscription Identifier; product API does not expose it.
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
    }

    #[test]
    fn publish_decode_qos1_without_packet_identifier_rejected() {
        // QoS 1 PUBLISH: topic then Property Length 0 with no 2-byte Packet Identifier.
        const QOS1_FLAGS: u8 = 0b0010;
        let body = Bytes::from_static(&[0x00, 0x01, b't', 0x00]);
        let result = PublishPacket::decode(body, QOS1_FLAGS);
        assert!(matches!(
            result,
            Err(DecodeError::InvalidLength) | Err(DecodeError::MalformedPacket(_))
        ));
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

    #[test]
    fn publish_decode_rejects_both_qos_bits_set() {
        const QOS3_FLAGS: u8 = 0b0110;
        let result = PublishPacket::decode(topic_and_prop0_payload(b""), QOS3_FLAGS);
        assert!(matches!(result, Err(DecodeError::MalformedPacket(_))));
    }

    #[test]
    fn publish_decode_qos0_packet_identifier_prefix_rejected() {
        let mut body = BytesMut::new();
        ByteString::from_static("topic").encode(&mut body).unwrap();
        body.put_u16(0x8001);
        body.put_u8(0);
        body.put_slice(b"data");

        let result = PublishPacket::decode(body.freeze(), QOS0_FLAGS);
        assert_eq!(
            result.map_err(|e| e.to_string()),
            Err(DecodeError::MalformedPacket("QoS 0 PUBLISH must not include packet identifier").to_string())
        );
    }

    #[test]
    fn publish_decode_qos0_low_packet_identifier_prefix_rejected() {
        let mut body = BytesMut::new();
        ByteString::from_static("topic").encode(&mut body).unwrap();
        body.put_u16(0x1234);
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
            Err(DecodeError::MalformedPacket("empty topic without topic alias"))
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
    fn publish_encode_qos0_wire_dup_flag_clear() {
        let message = PublishMessage {
            topic: ByteString::from_static("t"),
            payload: Bytes::from_static(b"p"),
            properties: PublishProperties::default(),
        };
        let delivery = PublishDelivery {
            dup: false,
            retain: false,
            qos: QoS::AtMostOnce,
            packet_id: None,
        };
        let mut buf = BytesMut::new();
        encode_publish(&message, delivery, u32::MAX, &mut buf).expect("encode");
        assert_eq!(
            buf[0] & 0b1000,
            0,
            "QoS 0 outbound PUBLISH must set DUP flag to 0 on the wire"
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
    fn publish_encode_rejects_empty_topic_without_alias() {
        let message = PublishMessage {
            topic: ByteString::new(),
            payload: Bytes::from_static(b"data"),
            properties: PublishProperties::default(),
        };
        let delivery = PublishDelivery {
            dup: false,
            retain: false,
            qos: QoS::AtMostOnce,
            packet_id: None,
        };
        let mut buf = BytesMut::new();
        assert!(matches!(
            encode_publish(&message, delivery, 1024, &mut buf),
            Err(EncodeError::MalformedPacket)
        ));
    }
}
