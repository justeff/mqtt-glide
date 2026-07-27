use bytes::{BufMut, BytesMut};

use super::packet::*;
pub(crate) use crate::codec::v5::body_encode::WireBodyEncode;

use crate::codec::error::EncodeError;
use crate::codec::properties::{
    encode_property, encode_property_default, encoded_property_size, encoded_property_size_default, var_int_len,
    var_int_len_from_size, var_int_len_u32,
};
use crate::codec::publish::{PublishDelivery, PublishMessage, PublishProperties};
use crate::codec::types::QoS;
use crate::codec::types::packet_type;
use crate::codec::utils::{Encode, encode_minimal_packet_id_ack, topic_name_bytes_forbidden, write_variable_length};
use crate::codec::v5::property_type as pt;

/// Borrowed v5 PUBLISH body — avoids cloning topic/payload/properties for outbound encode.
struct V5PublishBody<'a> {
    message: &'a PublishMessage,
    delivery: PublishDelivery,
}

fn canonical_props_len(props: &PublishProperties) -> usize {
    let prop_len = encoded_property_size(&props.correlation_data)
        + encoded_property_size(&props.message_expiry_interval)
        + encoded_property_size(&props.content_type)
        + encoded_property_size_default(&props.payload_format_indicator, false)
        + encoded_property_size(&props.response_topic)
        + props.user_properties.encoded_size();
    prop_len + var_int_len(prop_len) as usize
}

fn encode_canonical_props(props: &PublishProperties, buf: &mut BytesMut, max_out: u32) -> Result<(), EncodeError> {
    let prop_len = var_int_len_from_size(max_out);
    write_variable_length(prop_len, buf)?;
    encode_property(&props.correlation_data, pt::CORR_DATA, buf)?;
    encode_property(&props.message_expiry_interval, pt::MSG_EXPIRY_INT, buf)?;
    encode_property(&props.content_type, pt::CONTENT_TYPE, buf)?;
    encode_property_default(&props.payload_format_indicator, false, pt::UTF8_PAYLOAD, buf)?;
    encode_property(&props.response_topic, pt::RESP_TOPIC, buf)?;
    props.user_properties.encode(buf)
}

impl WireBodyEncode for V5PublishBody<'_> {
    fn encoded_body_len(&self, _max_out: u32) -> Result<usize, EncodeError> {
        let packet_id_size = if self.delivery.qos == QoS::AtMostOnce { 0 } else { 2 };
        let property_size = if self.message.properties.is_wire_empty() {
            1
        } else {
            canonical_props_len(&self.message.properties)
        };
        Ok(self.message.topic.encoded_size() + packet_id_size + property_size + self.message.payload.len())
    }

    fn encode_body(&self, buf: &mut BytesMut, max_out: u32) -> Result<(), EncodeError> {
        // Topic / QoS0 delivery rules are enforced in `publish_wire_plan` before any header bytes.
        let start_len = buf.len();
        self.message.topic.encode(buf)?;
        if self.delivery.qos != QoS::AtMostOnce {
            self.delivery
                .packet_id
                .ok_or(EncodeError::PacketIdRequired)?
                .encode(buf)?;
        }
        let property_budget = max_out.saturating_sub((buf.len() - start_len + self.message.payload.len()) as u32);
        if self.message.properties.is_wire_empty() {
            if property_budget == 0 {
                return Err(EncodeError::OverMaxPacketSize {
                    size: max_out + 1,
                    max: max_out,
                });
            }
            buf.put_u8(0);
        } else {
            encode_canonical_props(&self.message.properties, buf, property_budget)?;
        }
        buf.put(self.message.payload.as_ref());
        Ok(())
    }
}

/// Wire length of an outbound PUBLISH after size checks (does not write).
pub(crate) fn publish_wire_len(
    message: &PublishMessage,
    delivery: PublishDelivery,
    max_packet_size: u32,
) -> Result<usize, EncodeError> {
    Ok(publish_wire_plan(message, delivery, max_packet_size)?.wire_len)
}

struct PublishWirePlan<'a> {
    wire_len: usize,
    body_len: u32,
    body: V5PublishBody<'a>,
}

fn publish_wire_plan(
    message: &PublishMessage,
    delivery: PublishDelivery,
    max_packet_size: u32,
) -> Result<PublishWirePlan<'_>, EncodeError> {
    if delivery.qos == QoS::AtMostOnce && (delivery.dup || delivery.packet_id.is_some()) {
        return Err(EncodeError::MalformedPacket);
    }
    if (delivery.qos == QoS::AtLeastOnce || delivery.qos == QoS::ExactlyOnce) && delivery.packet_id.is_none() {
        return Err(EncodeError::PacketIdRequired);
    }
    // Topic Name rules before any wire bytes ([MQTT-3.3.2-2], [MQTT-4.7.0-1], [MQTT-4.7.3-1/2]).
    if message.topic.is_empty() || topic_name_bytes_forbidden(message.topic.as_bytes()) {
        return Err(EncodeError::MalformedPacket);
    }
    let body = V5PublishBody { message, delivery };
    let body_len = body.encoded_body_len(max_packet_size)? as u32;
    let wire_len = 1 + var_int_len_u32(body_len) + body_len;
    if max_packet_size != 0 && wire_len > max_packet_size {
        return Err(EncodeError::OverMaxPacketSize {
            size: wire_len,
            max: max_packet_size,
        });
    }
    Ok(PublishWirePlan {
        wire_len: wire_len as usize,
        body_len,
        body,
    })
}

/// Encode an MQTT v5 PUBLISH by appending to `buf`.
///
/// Does not clear `buf` — safe for writer batching. Callers that replace a single-frame buffer
/// must `clear()` first. On encode failure, `buf` is truncated back to its pre-call length so
/// a rejected publish cannot leave a partial frame for a later flush.
pub fn encode_publish(
    message: &PublishMessage,
    delivery: PublishDelivery,
    max_packet_size: u32,
    buf: &mut BytesMut,
) -> Result<(), EncodeError> {
    let plan = publish_wire_plan(message, delivery, max_packet_size)?;
    let start_len = buf.len();
    buf.reserve(plan.wire_len);
    buf.put_u8(
        packet_type::PUBLISH_START
            | (u8::from(delivery.qos) << 1)
            | ((delivery.dup as u8) << 3)
            | (delivery.retain as u8),
    );
    if let Err(error) = write_variable_length(plan.body_len, buf) {
        buf.truncate(start_len);
        return Err(error);
    }
    if let Err(error) = plan.body.encode_body(buf, plan.body_len) {
        buf.truncate(start_len);
        return Err(error);
    }
    Ok(())
}

impl Packet {
    pub(crate) fn encoded_body_len(&self, max_out: u32) -> Result<usize, EncodeError> {
        match self {
            Packet::Connect(connect) => connect.encoded_body_len(max_out),
            Packet::ConnectAck(ack) => ack.encoded_body_len(max_out),
            Packet::PublishAck(ack) | Packet::PublishReceived(ack) if ack.is_minimal_success() => Ok(2),
            Packet::PublishAck(ack) | Packet::PublishReceived(ack) => ack.encoded_body_len(max_out),
            Packet::PublishRelease(ack) | Packet::PublishComplete(ack) if ack.is_minimal_success() => Ok(2),
            Packet::PublishRelease(ack) | Packet::PublishComplete(ack) => ack.encoded_body_len(max_out),
            Packet::Subscribe(sub) => sub.encoded_body_len(max_out),
            Packet::SubscribeAck(ack) => ack.encoded_body_len(max_out),
            Packet::Unsubscribe(unsub) => unsub.encoded_body_len(max_out),
            Packet::UnsubscribeAck(ack) => ack.encoded_body_len(max_out),
            Packet::PingRequest | Packet::PingResponse => Ok(0),
            Packet::Disconnect(disconnect) if disconnect.is_minimal_success() => Ok(0),
            Packet::Disconnect(disconnect) => disconnect.encoded_body_len(max_out),
            Packet::Auth(auth) if auth.is_minimal_success() => Ok(0),
            Packet::Auth(auth) => auth.encoded_body_len(max_out),
        }
    }

    pub(crate) fn encode_wire(&self, buf: &mut BytesMut, body_len: u32) -> Result<(), EncodeError> {
        match self {
            Packet::Connect(connect) => {
                buf.put_u8(packet_type::CONNECT);
                write_variable_length(body_len, buf)?;
                connect.encode_body(buf, body_len)
            }
            Packet::ConnectAck(ack) => {
                buf.put_u8(packet_type::CONNACK);
                write_variable_length(body_len, buf)?;
                ack.encode_body(buf, body_len)
            }
            Packet::PublishAck(ack) if ack.is_minimal_success() => {
                encode_minimal_packet_id_ack(buf, packet_type::PUBACK, ack.packet_id);
                Ok(())
            }
            Packet::PublishAck(ack) => {
                buf.put_u8(packet_type::PUBACK);
                write_variable_length(body_len, buf)?;
                ack.encode_body(buf, body_len)
            }
            Packet::PublishReceived(ack) if ack.is_minimal_success() => {
                encode_minimal_packet_id_ack(buf, packet_type::PUBREC, ack.packet_id);
                Ok(())
            }
            Packet::PublishReceived(ack) => {
                buf.put_u8(packet_type::PUBREC);
                write_variable_length(body_len, buf)?;
                ack.encode_body(buf, body_len)
            }
            Packet::PublishRelease(ack) if ack.is_minimal_success() => {
                encode_minimal_packet_id_ack(buf, packet_type::PUBREL, ack.packet_id);
                Ok(())
            }
            Packet::PublishRelease(ack) => {
                buf.put_u8(packet_type::PUBREL);
                write_variable_length(body_len, buf)?;
                ack.encode_body(buf, body_len)
            }
            Packet::PublishComplete(ack) if ack.is_minimal_success() => {
                encode_minimal_packet_id_ack(buf, packet_type::PUBCOMP, ack.packet_id);
                Ok(())
            }
            Packet::PublishComplete(ack) => {
                buf.put_u8(packet_type::PUBCOMP);
                write_variable_length(body_len, buf)?;
                ack.encode_body(buf, body_len)
            }
            Packet::Subscribe(sub) => {
                buf.put_u8(packet_type::SUBSCRIBE);
                write_variable_length(body_len, buf)?;
                sub.encode_body(buf, body_len)
            }
            Packet::SubscribeAck(ack) => {
                buf.put_u8(packet_type::SUBACK);
                write_variable_length(body_len, buf)?;
                ack.encode_body(buf, body_len)
            }
            Packet::Unsubscribe(unsub) => {
                buf.put_u8(packet_type::UNSUBSCRIBE);
                write_variable_length(body_len, buf)?;
                unsub.encode_body(buf, body_len)
            }
            Packet::UnsubscribeAck(ack) => {
                buf.put_u8(packet_type::UNSUBACK);
                write_variable_length(body_len, buf)?;
                ack.encode_body(buf, body_len)
            }
            Packet::PingRequest => {
                buf.put_slice(&[packet_type::PINGREQ, 0]);
                Ok(())
            }
            Packet::PingResponse => {
                buf.put_slice(&[packet_type::PINGRESP, 0]);
                Ok(())
            }
            Packet::Disconnect(disconnect) if disconnect.is_minimal_success() => {
                buf.put_slice(&[packet_type::DISCONNECT, 0]);
                Ok(())
            }
            Packet::Disconnect(disconnect) => {
                buf.put_u8(packet_type::DISCONNECT);
                write_variable_length(body_len, buf)?;
                disconnect.encode_body(buf, body_len)
            }
            Packet::Auth(auth) if auth.is_minimal_success() => {
                buf.put_slice(&[packet_type::AUTH, 0]);
                Ok(())
            }
            Packet::Auth(auth) => {
                buf.put_u8(packet_type::AUTH);
                write_variable_length(body_len, buf)?;
                auth.encode_body(buf, body_len)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use bytestring::ByteString;
    use std::num::{NonZeroU16, NonZeroU32};

    use super::*;
    use crate::codec::publish::{PublishDelivery, PublishMessage, PublishProperties};
    use crate::codec::types::{MAX_PACKET_SIZE, QoS};
    use crate::codec::v5::UserProperties;
    use rstest::rstest;

    fn packet_id(v: u16) -> NonZeroU16 {
        NonZeroU16::new(v).unwrap()
    }

    #[test]
    fn test_encode_fixed_header() {
        let mut v = BytesMut::with_capacity(270);
        let p = Packet::PingRequest;

        assert_eq!(p.encoded_body_len(MAX_PACKET_SIZE).unwrap(), 0);
        p.encode_wire(&mut v, 0).unwrap();
        assert_eq!(&v[..2], b"\xc0\x00".as_ref());

        v.clear();

        let message = PublishMessage {
            topic: ByteString::from_static("topic"),
            payload: (0..255).collect::<Vec<u8>>().into(),
            properties: PublishProperties::default(),
        };
        let delivery = PublishDelivery {
            dup: true,
            retain: true,
            qos: QoS::ExactlyOnce,
            packet_id: Some(packet_id(0x4321)),
        };
        let mut buf = BytesMut::new();
        encode_publish(&message, delivery, 0, &mut buf).unwrap();
        assert_eq!(&buf[..3], b"\x3d\x89\x02".as_ref());
    }

    fn assert_encode_packet(packet: &Packet, expected: &[u8]) {
        let mut v = BytesMut::with_capacity(1024);
        let body_len = packet.encoded_body_len(1024).unwrap() as u32;
        packet.encode_wire(&mut v, body_len).unwrap();
        assert_eq!(expected.len(), v.len());
        assert_eq!(expected, &v[..]);
    }

    #[test]
    fn test_encode_connect_packets() {
        assert_encode_packet(
            &Packet::Connect(Box::new(Connect {
                clean_start: false,
                keep_alive: 60,
                client_id: ByteString::from_static("12345"),
                last_will: None,
                username: Some(ByteString::from_static("user")),
                password: Some(Bytes::from_static(b"pass")),
                session_expiry_interval: 0,
                auth_method: None,
                auth_data: None,
                request_problem_info: true,
                request_response_info: false,
                receive_max: None,
                topic_alias_max: 0,
                user_properties: vec![],
                max_packet_size: None,
            })),
            &b"\x10\x1E\x00\x04MQTT\x05\xC0\x00\x3C\x00\x00\
\x0512345\x00\x04user\x00\x04pass"[..],
        );

        assert_encode_packet(
            &Packet::Connect(Box::new(Connect {
                clean_start: false,
                keep_alive: 60,
                client_id: ByteString::from_static("12345"),
                last_will: Some(LastWill {
                    qos: QoS::ExactlyOnce,
                    retain: false,
                    topic: ByteString::from_static("topic"),
                    payload: Bytes::from_static(b"message"),
                    will_delay_interval: None,
                    correlation_data: None,
                    message_expiry_interval: None,
                    content_type: None,
                    user_properties: vec![],
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
                user_properties: vec![],
                max_packet_size: None,
            })),
            &b"\x10\x23\x00\x04MQTT\x05\x14\x00\x3C\x00\x00\
\x0512345\x00\x00\x05topic\x00\x07message"[..],
        );

        assert_encode_packet(
            &Packet::Connect(Box::new(Connect {
                clean_start: false,
                keep_alive: 60,
                client_id: ByteString::from_static("12345"),
                last_will: Some(LastWill {
                    qos: QoS::ExactlyOnce,
                    retain: true,
                    topic: ByteString::from_static("topic"),
                    payload: Bytes::from_static(b"message"),
                    will_delay_interval: Some(5),
                    correlation_data: Some(Bytes::from_static(b"correlationData")),
                    message_expiry_interval: NonZeroU32::new(7),
                    content_type: Some(ByteString::from_static("contentType")),
                    user_properties: vec![("name", "value").into()],
                    payload_format_indicator: Some(true),
                    response_topic: Some(ByteString::from_static("responseTopic")),
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
                user_properties: vec![],
                max_packet_size: None,
            })),
            &b"\x10\x6D\x00\x04MQTT\x05\x34\x00\x3C\x00\x00\
\x0512345\x4A\x18\0\0\0\x05\x01\x01\x02\0\0\0\x07\x03\0\x0bcontentType\x08\x00\x0dresponseTopic\x09\0\x0fcorrelationData\x26\0\x04name\0\x05value\x00\x05topic\x00\x07message"[..],
        );

        assert_encode_packet(
            &Packet::Disconnect(Disconnect {
                reason_code: DisconnectReasonCode::NormalDisconnection,
                session_expiry_interval: None,
                server_reference: None,
                reason_string: None,
                user_properties: vec![],
            }),
            b"\xe0\x00",
        );

        assert_encode_packet(
            &Packet::ConnectAck(Box::new(ConnectAck {
                session_present: false,
                reason_code: ConnectAckReason::Success,
                max_qos: QoS::AtLeastOnce,
                retain_available: false,
                subscription_identifiers_available: false,
                ..Default::default()
            })),
            b"\x20\x09\x00\x00\x06\x24\x01\x25\x00\x29\x00",
        );

        assert_encode_packet(
            &Packet::ConnectAck(Box::new(ConnectAck {
                reason_code: ConnectAckReason::NotAuthorized,
                ..Default::default()
            })),
            b"\x20\x03\x00\x87\x00",
        );

        assert_encode_packet(&Packet::Auth(Auth::default()), b"\xf0\x00");
    }

    fn assert_encode_publish(message: &PublishMessage, delivery: PublishDelivery, expected: &[u8]) {
        let mut wire = BytesMut::new();
        encode_publish(message, delivery, MAX_PACKET_SIZE, &mut wire).expect("encode publish");
        assert_eq!(expected, &wire[..]);
    }

    #[test]
    fn test_encode_publish_packets() {
        assert_encode_publish(
            &PublishMessage {
                topic: ByteString::from_static("topic"),
                payload: Bytes::from_static(b"data"),
                properties: PublishProperties::default(),
            },
            PublishDelivery {
                dup: true,
                retain: true,
                qos: QoS::ExactlyOnce,
                packet_id: Some(packet_id(0x4321)),
            },
            b"\x3d\x0E\x00\x05topic\x43\x21\x00data",
        );

        assert_encode_publish(
            &PublishMessage {
                topic: ByteString::from_static("topic"),
                payload: Bytes::from_static(b"data"),
                properties: PublishProperties::default(),
            },
            PublishDelivery {
                dup: false,
                retain: false,
                qos: QoS::AtMostOnce,
                packet_id: None,
            },
            b"\x30\x0c\x00\x05topic\x00data",
        );

        assert_encode_packet(
            &Packet::PublishAck(PublishAck {
                packet_id: packet_id(0x0001),
                reason_code: PublishAckReason::NotAuthorized,
                properties: Vec::new(),
                reason_string: None,
            }),
            b"\x40\x03\x00\x01\x87",
        );
    }

    #[test]
    fn test_encode_publish_qos0_dup_rejected() {
        let message = PublishMessage {
            topic: ByteString::from_static("topic"),
            payload: Bytes::from_static(b"data"),
            properties: PublishProperties::default(),
        };
        let delivery = PublishDelivery {
            dup: true,
            retain: false,
            qos: QoS::AtMostOnce,
            packet_id: None,
        };
        let mut buf = BytesMut::new();
        assert!(matches!(
            encode_publish(&message, delivery, MAX_PACKET_SIZE, &mut buf),
            Err(EncodeError::MalformedPacket)
        ));
    }

    #[test]
    fn test_encode_subscribe_packets() {
        assert_encode_packet(
            &Packet::Subscribe(Subscribe {
                packet_id: packet_id(0x1234),
                id: None,
                user_properties: Vec::new(),
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
            }),
            b"\x82\x13\x12\x34\x00\x00\x04test\x01\x00\x06filter\x02",
        );

        assert_encode_packet(
            &Packet::Subscribe(Subscribe {
                packet_id: packet_id(0x1234),
                id: Some(NonZeroU32::new(1).unwrap()),
                user_properties: Vec::new(),
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
            }),
            b"\x82\x15\x12\x34\x02\x0b\x01\x00\x04test\x01\x00\x06filter\x02",
        );

        assert_encode_packet(
            &Packet::SubscribeAck(SubscribeAck {
                packet_id: packet_id(0x1234),
                properties: UserProperties::default(),
                reason_string: None,
                status: vec![
                    SubscribeAckReason::GrantedQos1,
                    SubscribeAckReason::UnspecifiedError,
                    SubscribeAckReason::GrantedQos2,
                ],
            }),
            b"\x90\x06\x12\x34\x00\x01\x80\x02",
        );

        assert_encode_packet(
            &Packet::Unsubscribe(Unsubscribe {
                packet_id: packet_id(0x1234),
                topic_filters: vec![ByteString::from_static("test"), ByteString::from_static("filter")],
                user_properties: Vec::new(),
            }),
            b"\xa2\x11\x12\x34\x00\x00\x04test\x00\x06filter",
        );

        assert_encode_packet(
            &Packet::UnsubscribeAck(UnsubscribeAck {
                packet_id: packet_id(0x4321),
                properties: UserProperties::default(),
                reason_string: None,
                status: vec![UnsubscribeAckReason::Success, UnsubscribeAckReason::NotAuthorized],
            }),
            b"\xb0\x05\x43\x21\x00\x00\x87",
        );
    }

    #[test]
    fn test_encode_ping_packets() {
        assert_encode_packet(&Packet::PingRequest, b"\xc0\x00");
        assert_encode_packet(&Packet::PingResponse, b"\xd0\x00");
    }

    #[test]
    fn encode_publish_wire_len_matches_encoded_size() {
        let message = PublishMessage {
            topic: ByteString::from_static("telemetry/device/status"),
            payload: Bytes::from(vec![0_u8; 64]),
            properties: PublishProperties::default(),
        };
        let delivery = PublishDelivery {
            dup: false,
            retain: false,
            qos: QoS::AtLeastOnce,
            packet_id: Some(packet_id(42)),
        };
        let wire_len = publish_wire_len(&message, delivery, u32::MAX).expect("wire len");
        let mut buf = BytesMut::new();
        encode_publish(&message, delivery, u32::MAX, &mut buf).expect("encode");
        assert_eq!(buf.len(), wire_len);
    }

    #[test]
    fn encode_publish_never_writes_topic_alias_property() {
        // [MQTT-3.2.2-17], [MQTT-3.2.2-18] outbound path never encodes Topic Alias (product max=0)
        let message = PublishMessage {
            topic: ByteString::from_static("telemetry/device/status"),
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
        encode_publish(&message, delivery, u32::MAX, &mut buf).expect("encode");
        assert!(
            !buf.as_ref().windows(1).any(|w| w[0] == pt::TOPIC_ALIAS),
            "outbound PUBLISH must not contain Topic Alias property id 0x23"
        );
    }

    #[rstest]
    #[case::empty("")]
    #[case::plus_wildcard("a/+")]
    #[case::hash_wildcard("a/#")]
    #[case::embedded_nul("a\0b")]
    fn encode_publish_rejects_wildcard_and_nul_topic_names(#[case] topic: &str) {
        let delivery = PublishDelivery {
            dup: false,
            retain: false,
            qos: QoS::AtMostOnce,
            packet_id: None,
        };
        let message = PublishMessage {
            topic: ByteString::from(topic),
            payload: Bytes::from_static(b"x"),
            properties: PublishProperties::default(),
        };
        let mut buf = BytesMut::from(&b"keep"[..]);
        let err = encode_publish(&message, delivery, u32::MAX, &mut buf).expect_err(topic);
        assert!(
            matches!(err, EncodeError::MalformedPacket),
            "{topic}: expected MalformedPacket, got {err:?}"
        );
        assert_eq!(
            &buf[..],
            b"keep",
            "{topic}: rejected encode must not leave a partial PUBLISH frame"
        );
    }

    #[test]
    fn encode_publish_reuse_with_caller_clear() {
        let message = PublishMessage {
            topic: ByteString::from_static("telemetry/device/status"),
            payload: Bytes::from(vec![0_u8; 64]),
            properties: PublishProperties::default(),
        };
        let delivery = PublishDelivery {
            dup: false,
            retain: false,
            qos: QoS::AtLeastOnce,
            packet_id: Some(packet_id(42)),
        };
        let mut reused = BytesMut::with_capacity(256);
        encode_publish(&message, delivery, 0, &mut reused).expect("first encode");
        let first = reused.clone();
        reused.clear();
        encode_publish(&message, delivery, 0, &mut reused).expect("second encode");
        assert_eq!(first, reused);
    }
}
