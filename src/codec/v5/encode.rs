use std::num::NonZeroU16;

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
use crate::codec::utils::{
    Encode, MAX_VARIABLE_LENGTH, contains_nul, encode_minimal_packet_id_ack, topic_name_bytes_forbidden,
    write_variable_length,
};
use crate::codec::v5::property_type as pt;

/// Borrowed v5 PUBLISH body — avoids cloning topic/payload/properties for outbound encode.
struct V5PublishBody<'a> {
    message: &'a PublishMessage,
    delivery: PublishDelivery,
    alias: Option<OutboundTopicAlias>,
}

/// How an outbound PUBLISH uses a Topic Alias (§3.3.2.3.4). Aliases are per connection
/// and must not exceed the broker's CONNACK Topic Alias Maximum ([MQTT-3.3.2-9]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboundTopicAlias {
    /// Send the Topic Name together with the alias, (re)binding the alias to that topic.
    Assign(NonZeroU16),
    /// Send a zero-length Topic Name plus the alias already bound on this connection.
    Reuse(NonZeroU16),
}

impl OutboundTopicAlias {
    fn alias(self) -> NonZeroU16 {
        match self {
            Self::Assign(alias) | Self::Reuse(alias) => alias,
        }
    }

    fn omits_topic(self) -> bool {
        matches!(self, Self::Reuse(_))
    }
}

/// Topic Alias property: type byte + two-byte value.
const TOPIC_ALIAS_PROPERTY_LEN: usize = 3;

/// Summed property wire size. Rejects U+0000 in the UTF-8 property strings
/// ([MQTT-1.5.4-2]) within the same plan-time walk over the properties — the hot
/// no-properties path never calls this (see `is_wire_empty`).
fn canonical_props_len(props: &PublishProperties, alias: Option<OutboundTopicAlias>) -> Result<usize, EncodeError> {
    if let Some(content_type) = props.content_type.as_ref()
        && contains_nul(content_type.as_bytes())
    {
        return Err(EncodeError::MalformedPacket);
    }
    let mut user_props_len = 0;
    for property in &props.user_properties {
        if contains_nul(property.name.as_bytes()) || contains_nul(property.value.as_bytes()) {
            return Err(EncodeError::MalformedPacket);
        }
        user_props_len += 1 + property.encoded_size(); // prop type byte + name + value
    }
    let prop_len = encoded_property_size(&props.correlation_data)
        + encoded_property_size(&props.message_expiry_interval)
        + encoded_property_size(&props.content_type)
        + encoded_property_size_default(&props.payload_format_indicator, false)
        + encoded_property_size(&props.response_topic)
        + user_props_len
        + alias.map_or(0, |_| TOPIC_ALIAS_PROPERTY_LEN);
    Ok(prop_len + var_int_len(prop_len) as usize)
}

fn encode_canonical_props(
    props: &PublishProperties,
    alias: Option<OutboundTopicAlias>,
    buf: &mut BytesMut,
    max_out: u32,
) -> Result<(), EncodeError> {
    let prop_len = var_int_len_from_size(max_out);
    write_variable_length(prop_len, buf)?;
    encode_property(&alias.map(OutboundTopicAlias::alias), pt::TOPIC_ALIAS, buf)?;
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
        let property_size = if self.message.properties.is_wire_empty() && self.alias.is_none() {
            1
        } else {
            canonical_props_len(&self.message.properties, self.alias)?
        };
        let topic_size = if self.alias.is_some_and(OutboundTopicAlias::omits_topic) {
            2
        } else {
            self.message.topic.encoded_size()
        };
        Ok(topic_size + packet_id_size + property_size + self.message.payload.len())
    }

    fn encode_body(&self, buf: &mut BytesMut, max_out: u32) -> Result<(), EncodeError> {
        // Topic / QoS0 delivery rules are enforced in `publish_wire_plan` before any header bytes.
        let start_len = buf.len();
        if self.alias.is_some_and(OutboundTopicAlias::omits_topic) {
            buf.put_u16(0); // zero-length Topic Name: the alias stands in ([MQTT-3.3.2-12])
        } else {
            self.message.topic.encode(buf)?;
        }
        if self.delivery.qos != QoS::AtMostOnce {
            self.delivery
                .packet_id
                .ok_or(EncodeError::PacketIdRequired)?
                .encode(buf)?;
        }
        let property_budget = max_out.saturating_sub((buf.len() - start_len + self.message.payload.len()) as u32);
        if self.message.properties.is_wire_empty() && self.alias.is_none() {
            if property_budget == 0 {
                return Err(EncodeError::OverMaxPacketSize {
                    size: max_out + 1,
                    max: max_out,
                });
            }
            buf.put_u8(0);
        } else {
            encode_canonical_props(&self.message.properties, self.alias, buf, property_budget)?;
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
    Ok(publish_wire_plan(message, delivery, None, max_packet_size)?.wire_len)
}

struct PublishWirePlan<'a> {
    wire_len: usize,
    body_len: u32,
    body: V5PublishBody<'a>,
}

fn publish_wire_plan(
    message: &PublishMessage,
    delivery: PublishDelivery,
    alias: Option<OutboundTopicAlias>,
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
    // Response Topic follows Topic Name rules ([MQTT-3.3.2-13/14]) — same single pass
    // family as the topic check above (NUL included via topic_name_bytes_forbidden).
    // U+0000 in the remaining UTF-8 properties is rejected inside `canonical_props_len`
    // ([MQTT-1.5.4-2]), within its existing plan-time walk over the properties.
    if let Some(response_topic) = message.properties.response_topic.as_ref()
        && (response_topic.is_empty() || topic_name_bytes_forbidden(response_topic.as_bytes()))
    {
        return Err(EncodeError::MalformedPacket);
    }
    let body = V5PublishBody {
        message,
        delivery,
        alias,
    };
    let body_len = body.encoded_body_len(max_packet_size)?;
    // Bodies beyond the Remaining Length ceiling (§2.1.4) cannot be framed at all;
    // reject before the u32 cast below can truncate (e.g. multi-GiB payloads).
    if body_len > MAX_VARIABLE_LENGTH as usize {
        return Err(EncodeError::InvalidLength);
    }
    let body_len = body_len as u32;
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
#[inline]
pub fn encode_publish(
    message: &PublishMessage,
    delivery: PublishDelivery,
    max_packet_size: u32,
    buf: &mut BytesMut,
) -> Result<(), EncodeError> {
    encode_publish_with_alias(message, delivery, None, max_packet_size, buf)
}

/// [`encode_publish`] with an optional outbound Topic Alias.
///
/// The Topic Name rules are still checked against `message.topic` with
/// [`OutboundTopicAlias::Reuse`], which only leaves it off the wire. Choosing valid aliases
/// (non-zero, within the broker's maximum, bound before reuse on this connection) is the
/// caller's job.
pub fn encode_publish_with_alias(
    message: &PublishMessage,
    delivery: PublishDelivery,
    alias: Option<OutboundTopicAlias>,
    max_packet_size: u32,
    buf: &mut BytesMut,
) -> Result<(), EncodeError> {
    let plan = publish_wire_plan(message, delivery, alias, max_packet_size)?;
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
        // Roll back on failure (same contract as `encode_publish`): the fixed header is
        // written before the body, so a rejected body must not leave stray header bytes
        // in a caller-batched buffer.
        let start_len = buf.len();
        if let Err(error) = self.encode_wire_inner(buf, body_len) {
            buf.truncate(start_len);
            return Err(error);
        }
        Ok(())
    }

    fn encode_wire_inner(&self, buf: &mut BytesMut, body_len: u32) -> Result<(), EncodeError> {
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

    /// Exact CONNECT bytes: Protocol Name "MQTT" ([MQTT-3.1.2-1]); the Client Identifier is the
    /// first Payload field ([MQTT-3.1.3-3]); Will Flag 1 brings Will Properties, Will Topic and
    /// Will Message ([MQTT-3.1.2-9]) with the Will QoS bits as set ([MQTT-3.1.2-12]); Will Flag 0
    /// leaves Will QoS and Will Retain at 0 ([MQTT-3.1.2-11], [MQTT-3.1.2-13]); User Name and
    /// Password are present exactly when their flags are set ([MQTT-3.1.2-17], [MQTT-3.1.2-18],
    /// [MQTT-3.1.2-19]). Fixed Header flags of CONNECT and DISCONNECT are 0 ([MQTT-2.1.3-1]) and
    /// DISCONNECT carries a defined Reason Code ([MQTT-3.14.2-1]).
    #[test]
    fn test_encode_connect_packets() {
        assert_encode_packet(
            &Packet::Connect(Box::new(Connect {
                clean_start: false,
                keep_alive: 60,
                client_id: ByteString::from_static("12345"),
                will_message: None,
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
                will_message: Some(WillMessage {
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
                will_message: Some(WillMessage {
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

    /// With every optional Payload field present, CONNECT carries them in the order Client
    /// Identifier, Will Properties, Will Topic, Will Payload, User Name, Password
    /// ([MQTT-3.1.3-1]). Each field holds distinct bytes so a swapped pair changes the wire.
    #[test]
    fn test_encode_connect_payload_field_order() {
        assert_encode_packet(
            &Packet::Connect(Box::new(Connect {
                clean_start: false,
                keep_alive: 60,
                client_id: ByteString::from_static("cid"),
                will_message: Some(WillMessage {
                    qos: QoS::AtLeastOnce,
                    retain: false,
                    topic: ByteString::from_static("wt"),
                    payload: Bytes::from_static(b"wpayload"),
                    will_delay_interval: Some(5),
                    correlation_data: None,
                    message_expiry_interval: None,
                    content_type: None,
                    user_properties: vec![("k", "v").into()],
                    payload_format_indicator: None,
                    response_topic: None,
                }),
                username: Some(ByteString::from_static("usr")),
                password: Some(Bytes::from_static(b"pw")),
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
            &b"\x10\x34\x00\x04MQTT\x05\xCC\x00\x3C\x00\
\x00\x03cid\
\x0C\x18\x00\x00\x00\x05\x26\x00\x01k\x00\x01v\
\x00\x02wt\
\x00\x08wpayload\
\x00\x03usr\
\x00\x02pw"[..],
        );
    }

    fn assert_encode_publish(message: &PublishMessage, delivery: PublishDelivery, expected: &[u8]) {
        let mut wire = BytesMut::new();
        encode_publish(message, delivery, MAX_PACKET_SIZE, &mut wire).expect("encode publish");
        assert_eq!(expected, &wire[..]);
    }

    /// Exact PUBLISH bytes; a PUBLISH without properties still carries a Property Length of
    /// zero ([MQTT-2.2.2-1]).
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

    /// A QoS 0 PUBLISH is never sent with DUP set ([MQTT-4.3.1-1]).
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

    /// Exact SUBSCRIBE / UNSUBSCRIBE bytes: Fixed Header 0x82 / 0xA2, i.e. reserved flags
    /// 0,0,1,0 ([MQTT-2.1.3-1], [MQTT-3.8.1-1], [MQTT-3.10.1-1]); no properties still means a
    /// Property Length of zero ([MQTT-2.2.2-1]).
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

    /// Assign sends topic + alias; Reuse sends a zero-length topic + alias; both decode back
    /// to the alias, and only Assign carries the Topic Name.
    #[test]
    fn encode_publish_with_alias_assign_and_reuse_roundtrip() {
        use bytes::BytesMut;
        use tokio_util::codec::Decoder;

        let message = PublishMessage::new("telemetry/device/status", "on")
            .properties(PublishProperties::default().user_property("k", "v"));
        let delivery = PublishDelivery {
            dup: false,
            retain: false,
            qos: QoS::AtLeastOnce,
            packet_id: NonZeroU16::new(3),
        };
        let alias = NonZeroU16::new(2).expect("nz");
        for (mode, expect_topic) in [
            (OutboundTopicAlias::Assign(alias), "telemetry/device/status"),
            (OutboundTopicAlias::Reuse(alias), ""),
        ] {
            let mut buf = BytesMut::new();
            encode_publish_with_alias(&message, delivery, Some(mode), u32::MAX, &mut buf).expect("encode");
            let wire_len = buf.len();
            let mut codec = crate::codec::V5Codec::with_max_packet_size(0);
            let (frame, _) = codec.decode(&mut buf).expect("decode").expect("one frame");
            let crate::codec::DecodedFrame::Publish(packet) = frame else {
                panic!("expected PUBLISH, got {frame:?}");
            };
            assert_eq!(&*packet.message.topic, expect_topic, "{mode:?}");
            assert_eq!(packet.wire_topic_alias, Some(alias), "{mode:?}");
            assert_eq!(packet.message.payload, message.payload);
            assert_eq!(
                packet.message.properties.user_properties,
                message.properties.user_properties
            );
            assert_eq!(
                publish_wire_len(&message, delivery, u32::MAX).expect("len") + 3
                    - if mode == OutboundTopicAlias::Reuse(alias) {
                        message.topic.len()
                    } else {
                        0
                    },
                wire_len,
                "{mode:?}: alias adds 3 bytes, Reuse drops the topic bytes"
            );
        }
    }

    /// A message without properties still gets a property section for the alias.
    #[test]
    fn encode_publish_with_alias_without_other_properties() {
        use bytes::BytesMut;
        use tokio_util::codec::Decoder;

        let message = PublishMessage::new("a/b", "x");
        let delivery = PublishDelivery {
            dup: false,
            retain: false,
            qos: QoS::AtMostOnce,
            packet_id: None,
        };
        let alias = NonZeroU16::new(1).expect("nz");
        let mut buf = BytesMut::new();
        encode_publish_with_alias(
            &message,
            delivery,
            Some(OutboundTopicAlias::Reuse(alias)),
            u32::MAX,
            &mut buf,
        )
        .expect("encode");
        let mut codec = crate::codec::V5Codec::with_max_packet_size(0);
        let (frame, _) = codec.decode(&mut buf).expect("decode").expect("frame");
        let crate::codec::DecodedFrame::Publish(packet) = frame else {
            panic!("expected PUBLISH");
        };
        assert!(packet.message.topic.is_empty());
        assert_eq!(packet.wire_topic_alias, Some(alias));
        assert!(packet.message.properties.is_wire_empty());
    }

    /// Property identifiers of an encoded PUBLISH, in wire order. Walks the property block
    /// (every value is skipped by its data type, so an unknown identifier panics instead of
    /// being guessed at).
    fn publish_property_ids(wire: &[u8], has_packet_id: bool) -> Vec<u8> {
        fn var_int(bytes: &[u8]) -> (usize, usize) {
            let (mut value, mut shift) = (0usize, 0);
            for (index, byte) in bytes.iter().enumerate() {
                value |= usize::from(byte & 0x7F) << shift;
                if byte & 0x80 == 0 {
                    return (value, index + 1);
                }
                shift += 7;
            }
            panic!("truncated variable byte integer");
        }
        let u16_at = |at: usize| usize::from(u16::from_be_bytes([wire[at], wire[at + 1]]));

        let (_remaining, consumed) = var_int(&wire[1..]);
        let mut at = 1 + consumed;
        at += 2 + u16_at(at); // Topic Name
        if has_packet_id {
            at += 2;
        }
        let (props_len, consumed) = var_int(&wire[at..]);
        at += consumed;
        let end = at + props_len;

        let mut ids = Vec::new();
        while at < end {
            let id = wire[at];
            at += 1;
            ids.push(id);
            at += match id {
                0x01 => 1,                      // Payload Format Indicator
                0x02 => 4,                      // Message Expiry Interval
                0x03 | 0x08 => 2 + u16_at(at),  // Content Type, Response Topic
                0x09 => 2 + u16_at(at),         // Correlation Data
                0x0B => var_int(&wire[at..]).1, // Subscription Identifier
                0x23 => 2,                      // Topic Alias
                0x26 => {
                    let key = 2 + u16_at(at);
                    key + 2 + u16_at(at + key) // User Property
                }
                other => panic!("unexpected PUBLISH property {other:#04x}"),
            };
        }
        assert_eq!(at, end, "property block ends on a property boundary");
        assert_eq!(end, wire.len() - 1, "payload (one byte) follows the property block");
        ids
    }

    /// A PUBLISH the client sends never carries a Subscription Identifier (0x0B), even with
    /// every property it can set ([MQTT-3.3.4-6]); the walk also pins the properties that are
    /// there, so a skipped or misparsed block cannot pass.
    #[test]
    fn encode_publish_never_writes_subscription_identifier() {
        let message = PublishMessage::new("telemetry/device/status", "x").properties(
            PublishProperties::default()
                .correlation_data(Bytes::from_static(b"corr"))
                .message_expiry(60)
                .content_type("text/plain")
                .user_property("k1", "v1")
                .user_property("k2", "v2")
                .payload_format_indicator(true)
                .response_topic("reply/to"),
        );
        let props = &message.properties;
        assert!(
            props.correlation_data.is_some()
                && props.message_expiry_interval.is_some()
                && props.content_type.is_some()
                && !props.user_properties.is_empty()
                && props.payload_format_indicator
                && props.response_topic.is_some(),
            "every PublishProperties field is set"
        );
        let alias = NonZeroU16::new(7).expect("nz");

        for qos in [QoS::AtMostOnce, QoS::AtLeastOnce] {
            let delivery = PublishDelivery {
                dup: false,
                retain: false,
                qos,
                packet_id: (qos != QoS::AtMostOnce).then(|| packet_id(9)),
            };
            let has_packet_id = delivery.packet_id.is_some();

            let mut buf = BytesMut::new();
            encode_publish(&message, delivery, u32::MAX, &mut buf).expect("encode");
            let mut ids = publish_property_ids(&buf, has_packet_id);
            assert!(
                !ids.contains(&0x0B),
                "{qos:?}: Subscription Identifier on the wire: {ids:02x?}"
            );
            ids.sort_unstable();
            assert_eq!(ids, [0x01, 0x02, 0x03, 0x08, 0x09, 0x26, 0x26], "{qos:?}");

            for mode in [OutboundTopicAlias::Assign(alias), OutboundTopicAlias::Reuse(alias)] {
                let mut buf = BytesMut::new();
                encode_publish_with_alias(&message, delivery, Some(mode), u32::MAX, &mut buf).expect("encode");
                let mut ids = publish_property_ids(&buf, has_packet_id);
                assert!(
                    !ids.contains(&0x0B),
                    "{qos:?} {mode:?}: Subscription Identifier on the wire: {ids:02x?}"
                );
                ids.sort_unstable();
                assert_eq!(
                    ids,
                    [0x01, 0x02, 0x03, 0x08, 0x09, 0x23, 0x26, 0x26],
                    "{qos:?} {mode:?}"
                );
            }
        }
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

    /// A Topic Name never carries wildcards ([MQTT-3.3.2-2], [MQTT-4.7.0-1]) or U+0000
    /// ([MQTT-4.7.3-2]); nothing is written for a rejected PUBLISH.
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

    /// [MQTT-3.3.2-13], [MQTT-3.3.2-14] Response Topic follows Topic Name rules: non-empty, no wildcards.
    #[rstest]
    #[case::empty("")]
    #[case::plus_wildcard("reply/+")]
    #[case::hash_wildcard("reply/#")]
    #[case::embedded_nul("reply\0topic")]
    fn encode_publish_rejects_invalid_response_topic(#[case] response_topic: &str) {
        let delivery = PublishDelivery {
            dup: false,
            retain: false,
            qos: QoS::AtMostOnce,
            packet_id: None,
        };
        let message = PublishMessage {
            topic: ByteString::from_static("req/topic"),
            payload: Bytes::from_static(b"x"),
            properties: PublishProperties::default().response_topic(response_topic),
        };
        let mut buf = BytesMut::from(&b"keep"[..]);
        let err = encode_publish(&message, delivery, u32::MAX, &mut buf).expect_err(response_topic);
        assert!(
            matches!(err, EncodeError::MalformedPacket),
            "{response_topic}: expected MalformedPacket, got {err:?}"
        );
        assert_eq!(
            &buf[..],
            b"keep",
            "{response_topic}: rejected encode must not leave a partial PUBLISH frame"
        );
    }

    #[test]
    fn encode_publish_accepts_valid_response_topic() {
        let delivery = PublishDelivery {
            dup: false,
            retain: false,
            qos: QoS::AtMostOnce,
            packet_id: None,
        };
        let message = PublishMessage {
            topic: ByteString::from_static("req/topic"),
            payload: Bytes::from_static(b"x"),
            properties: PublishProperties::default().response_topic("reply/to/me"),
        };
        let mut buf = BytesMut::new();
        encode_publish(&message, delivery, u32::MAX, &mut buf).expect("valid response topic encodes");
    }

    /// [MQTT-1.5.4-2] U+0000 in publish UTF-8 properties is rejected at plan time,
    /// before any wire bytes. The Content Type case covers [MQTT-3.3.2-19].
    #[rstest]
    #[case::content_type(PublishProperties::default().content_type("text/\0plain"))]
    #[case::user_property_name(PublishProperties::default().user_property("k\0", "v"))]
    #[case::user_property_value(PublishProperties::default().user_property("k", "v\0"))]
    fn encode_publish_rejects_nul_in_utf8_properties(#[case] properties: PublishProperties) {
        let delivery = PublishDelivery {
            dup: false,
            retain: false,
            qos: QoS::AtMostOnce,
            packet_id: None,
        };
        let message = PublishMessage {
            topic: ByteString::from_static("topic/a"),
            payload: Bytes::from_static(b"x"),
            properties,
        };
        let mut buf = BytesMut::from(&b"keep"[..]);
        let err = encode_publish(&message, delivery, u32::MAX, &mut buf).expect_err("nul property");
        assert!(matches!(err, EncodeError::MalformedPacket), "got {err:?}");
        assert_eq!(
            &buf[..],
            b"keep",
            "rejected encode must not leave a partial PUBLISH frame"
        );
    }

    /// Bodies beyond the Remaining Length ceiling (§2.1.4) cannot be framed; the plan
    /// rejects them before the u32 cast could truncate (multi-GiB payloads with an
    /// unlimited max packet size would otherwise produce a corrupt Remaining Length).
    #[test]
    fn encode_publish_rejects_body_beyond_remaining_length_ceiling() {
        // `vec![0; n]` is lazily zero-mapped; the rejected encode never touches the pages.
        let message = PublishMessage {
            topic: ByteString::from_static("t"),
            payload: Bytes::from(vec![0u8; crate::codec::utils::MAX_VARIABLE_LENGTH as usize]),
            properties: PublishProperties::default(),
        };
        let delivery = PublishDelivery {
            dup: false,
            retain: false,
            qos: QoS::AtMostOnce,
            packet_id: None,
        };
        let mut buf = BytesMut::from(&b"keep"[..]);
        let err = encode_publish(&message, delivery, 0, &mut buf).expect_err("oversize body");
        assert!(matches!(err, EncodeError::InvalidLength), "got {err:?}");
        assert_eq!(&buf[..], b"keep", "rejected encode must not leave a partial frame");
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
