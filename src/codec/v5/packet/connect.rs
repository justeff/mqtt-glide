//! MQTT CONNECT (§3.1).
//!
//! Variable header, properties, and payload per OASIS MQTT Version 5.0.
//!
//! # Errors
//! Decode rejects reserved Connect flags ([MQTT-3.1.2-3]), Will flag inconsistencies
//! ([MQTT-3.1.2-11], [MQTT-3.1.2-13]), Authentication Data without Authentication Method,
//! and trailing bytes after the payload (§2.1.4).

use std::num::{NonZeroU16, NonZeroU32};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use bytestring::ByteString;

use crate::codec::error::{DecodeError, EncodeError};
use crate::codec::properties::{
    encode_property, encode_property_default, encoded_property_size, encoded_property_size_default, var_int_len,
};
use crate::codec::types::{ConnectFlags, MQTT, MQTT_LEVEL_5, QoS, WILL_QOS_SHIFT};
use crate::codec::utils::{self, Decode, Encode, Property, decode_topic_name};
use crate::codec::v5::body_encode::WireBodyEncode;
use crate::codec::v5::{RECEIVE_MAX_DEFAULT, UserProperties, UserProperty, property_type as pt};

/// CONNECT packet body (§3.1).
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Connect {
    /// Clean Start ([MQTT-3.1.2-4], §3.1.2.4).
    pub clean_start: bool,
    /// Keep Alive in seconds (§3.1.2.10).
    pub keep_alive: u16,

    /// Session Expiry Interval (§3.1.2.11.2).
    pub session_expiry_interval: u32,
    /// Authentication Method (§3.1.2.11.9).
    pub auth_method: Option<ByteString>,
    /// Authentication Data (§3.1.2.11.10).
    pub auth_data: Option<Bytes>,
    /// Request Problem Information (§3.1.2.11.7); default `true` when omitted.
    pub request_problem_info: bool,
    /// Request Response Information (§3.1.2.11.6); default `false` when omitted.
    pub request_response_info: bool,
    /// Receive Maximum (§3.1.2.11.3); absent means [`RECEIVE_MAX_DEFAULT`].
    pub receive_max: Option<NonZeroU16>,
    /// Topic Alias Maximum (§3.1.2.11.5).
    pub topic_alias_max: u16,
    /// User Property (§3.1.2.11.8).
    pub user_properties: UserProperties,
    /// Maximum Packet Size (§3.1.2.11.4).
    pub max_packet_size: Option<NonZeroU32>,

    /// Will Message (§3.1.2.5, §3.1.3.2–§3.1.3.4).
    pub will_message: Option<WillMessage>,
    /// Client Identifier (§3.1.3.1).
    pub client_id: ByteString,
    /// User Name (§3.1.3.5).
    pub username: Option<ByteString>,
    /// Password (§3.1.3.6).
    pub password: Option<Bytes>,
}

/// Will Message fields carried in CONNECT (§3.1.2.5, §3.1.3.2–§3.1.3.4).
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct WillMessage {
    /// Will QoS (§3.1.2.6).
    pub qos: QoS,
    /// Will Retain (§3.1.2.7).
    pub retain: bool,
    /// Will Topic (§3.1.3.3).
    pub topic: ByteString,
    /// Will Payload (§3.1.3.4).
    pub payload: Bytes,

    /// Will Delay Interval (§3.1.3.2.2).
    pub will_delay_interval: Option<u32>,
    /// Correlation Data (§3.1.3.2.7).
    pub correlation_data: Option<Bytes>,
    /// Message Expiry Interval (§3.1.3.2.4).
    pub message_expiry_interval: Option<NonZeroU32>,
    /// Content Type (§3.1.3.2.5).
    pub content_type: Option<ByteString>,
    /// User Property (§3.1.3.2.8).
    pub user_properties: UserProperties,
    /// Payload Format Indicator (§3.1.3.2.3).
    pub payload_format_indicator: Option<bool>,
    /// Response Topic (§3.1.3.2.6).
    pub response_topic: Option<ByteString>,
}

impl WillMessage {
    /// Builds a Will with QoS 0, retain false, and empty Will Properties.
    ///
    /// `topic` / `payload` accept any [`Into<ByteString>`] / [`Into<Bytes>`] (same contract
    /// as [`crate::PublishMessage::new`]).
    #[inline]
    pub fn new(topic: impl Into<ByteString>, payload: impl Into<Bytes>) -> Self {
        Self {
            qos: QoS::AtMostOnce,
            retain: false,
            topic: topic.into(),
            payload: payload.into(),
            will_delay_interval: None,
            correlation_data: None,
            message_expiry_interval: None,
            content_type: None,
            user_properties: Vec::new(),
            payload_format_indicator: None,
            response_topic: None,
        }
    }

    /// Sets Will QoS (§3.1.2.6). QoS 2 is rejected when connecting (client API).
    #[inline]
    pub fn qos(mut self, qos: QoS) -> Self {
        self.qos = qos;
        self
    }

    /// Sets Will Retain (§3.1.2.7).
    #[inline]
    pub fn retain(mut self, retain: bool) -> Self {
        self.retain = retain;
        self
    }

    /// Sets Will Delay Interval in **seconds** (§3.1.3.2.2).
    #[inline]
    pub fn will_delay_interval(mut self, seconds: u32) -> Self {
        self.will_delay_interval = Some(seconds);
        self
    }

    /// Sets Payload Format Indicator (§3.1.3.2.3); `true` means UTF-8 payload.
    ///
    /// `None` (default) omits the property; `Some(false)` encodes indicator `0` on the wire
    /// (unlike [`crate::PublishProperties::payload_format_indicator`], which is a plain `bool`
    /// that omits the default `false`).
    #[inline]
    pub fn payload_format_indicator(mut self, is_utf8: bool) -> Self {
        self.payload_format_indicator = Some(is_utf8);
        self
    }

    /// Sets Message Expiry Interval in **seconds** (§3.1.3.2.4).
    ///
    /// `0` clears expiry (property omitted).
    #[inline]
    pub fn message_expiry(mut self, seconds: u32) -> Self {
        self.message_expiry_interval = NonZeroU32::new(seconds);
        self
    }

    /// Sets Content Type (§3.1.3.2.5).
    #[inline]
    pub fn content_type(mut self, content_type: impl Into<ByteString>) -> Self {
        self.content_type = Some(content_type.into());
        self
    }

    /// Sets Response Topic (§3.1.3.2.6).
    #[inline]
    pub fn response_topic(mut self, response_topic: impl Into<ByteString>) -> Self {
        self.response_topic = Some(response_topic.into());
        self
    }

    /// Sets Correlation Data (§3.1.3.2.7).
    #[inline]
    pub fn correlation_data(mut self, data: impl Into<Bytes>) -> Self {
        self.correlation_data = Some(data.into());
        self
    }

    /// Appends one User Property (§3.1.3.2.8).
    #[inline]
    pub fn user_property(mut self, name: impl Into<ByteString>, value: impl Into<ByteString>) -> Self {
        self.user_properties.push(UserProperty::from((name, value)));
        self
    }

    fn properties_len(&self) -> usize {
        encoded_property_size(&self.will_delay_interval)
            + encoded_property_size(&self.correlation_data)
            + encoded_property_size(&self.message_expiry_interval)
            + encoded_property_size(&self.content_type)
            + encoded_property_size(&self.payload_format_indicator)
            + encoded_property_size(&self.response_topic)
            + self.user_properties.encoded_size()
    }
}

impl Connect {
    /// Sets Client Identifier (§3.1.3.1).
    pub fn client_id(mut self, client_id: impl Into<ByteString>) -> Self {
        self.client_id = client_id.into();
        self
    }

    /// Sets User Name (§3.1.3.5).
    pub fn username(mut self, username: impl Into<ByteString>) -> Self {
        self.username = Some(username.into());
        self
    }

    /// Sets Password (§3.1.3.6).
    pub fn password(mut self, password: impl Into<Bytes>) -> Self {
        self.password = Some(password.into());
        self
    }

    /// Sets Receive Maximum (§3.1.2.11.3); `0` clears the property (wire default applies).
    pub fn receive_max(mut self, max: u16) -> Self {
        if let Some(num) = NonZeroU16::new(max) {
            self.receive_max = Some(num);
        } else {
            self.receive_max = None;
        }
        self
    }

    fn properties_len(&self) -> usize {
        encoded_property_size(&self.auth_method)
            + encoded_property_size(&self.auth_data)
            + encoded_property_size_default(&self.session_expiry_interval, 0)
            + encoded_property_size_default(&self.request_problem_info, true) // §3.1.2.11.7
            + encoded_property_size_default(&self.request_response_info, false) // §3.1.2.11.6
            + encoded_property_size(&self.receive_max)
            + encoded_property_size(&self.max_packet_size)
            + encoded_property_size_default(&self.topic_alias_max, 0)
            + self.user_properties.encoded_size()
    }

    pub(crate) fn decode(src: &mut Bytes) -> Result<Self, DecodeError> {
        ensure!(src.remaining() >= 10, DecodeError::InvalidLength);
        let len = src.get_u16();

        ensure!(len == 4 && &src.as_ref()[0..4] == MQTT, DecodeError::InvalidProtocol);
        src.advance(4);

        let level = src.get_u8();
        ensure!(level == MQTT_LEVEL_5, DecodeError::UnsupportedProtocolLevel);

        let flags = ConnectFlags::from_bits(src.get_u8()).ok_or(DecodeError::ConnectReservedFlagSet)?; // [MQTT-3.1.2-3]
        if !flags.contains(ConnectFlags::WILL) {
            // [MQTT-3.1.2-11] Will QoS bits must be zero when Will Flag is 0
            ensure!(
                (flags & ConnectFlags::WILL_QOS).is_empty(),
                DecodeError::MalformedPacket("Will QoS set without Will Flag")
            );
            // [MQTT-3.1.2-13] Will Retain must be 0 when Will Flag is 0
            ensure!(
                !flags.contains(ConnectFlags::WILL_RETAIN),
                DecodeError::MalformedPacket("Will Retain set without Will Flag")
            );
        }
        let keep_alive = src.get_u16();

        // reading properties
        let mut session_expiry_interval = None;
        let mut auth_method = None;
        let mut auth_data = None;
        let mut request_problem_info = None;
        let mut request_response_info = None;
        let mut receive_max = None;
        let mut topic_alias_max = None;
        let mut user_properties = Vec::new();
        let mut max_packet_size = None;
        let prop_src = &mut utils::take_properties(src)?;
        while prop_src.has_remaining() {
            match prop_src.get_u8() {
                pt::SESS_EXPIRY_INT => session_expiry_interval.read_value(prop_src)?,
                pt::AUTH_METHOD => auth_method.read_value(prop_src)?,
                pt::AUTH_DATA => auth_data.read_value(prop_src)?,
                pt::REQ_PROB_INFO => request_problem_info.read_value(prop_src)?,
                pt::REQ_RESP_INFO => request_response_info.read_value(prop_src)?,
                pt::RECEIVE_MAX => receive_max.read_value(prop_src)?,
                pt::TOPIC_ALIAS_MAX => topic_alias_max.read_value(prop_src)?,
                pt::USER => user_properties.push(UserProperty::decode(prop_src)?),
                pt::MAX_PACKET_SIZE => max_packet_size.read_value(prop_src)?,
                _ => return Err(DecodeError::MalformedPacket("unknown CONNECT property")),
            }
        }

        // CONNECT property rules: Authentication Data requires Authentication Method
        ensure!(
            auth_data.is_none() || auth_method.is_some(),
            DecodeError::MalformedPacket("Authentication Data without Authentication Method")
        );

        let client_id = ByteString::decode(src)?;

        let will_message = if flags.contains(ConnectFlags::WILL) {
            Some(decode_will_message(src, flags)?)
        } else {
            None
        };

        let username = if flags.contains(ConnectFlags::USERNAME) {
            Some(ByteString::decode(src)?)
        } else {
            None
        };
        let password = if flags.contains(ConnectFlags::PASSWORD) {
            Some(Bytes::decode(src)?)
        } else {
            None
        };

        ensure!(!src.has_remaining(), DecodeError::InvalidLength);

        Ok(Connect {
            clean_start: flags.contains(ConnectFlags::CLEAN_START),
            keep_alive,
            session_expiry_interval: session_expiry_interval.unwrap_or(0),
            auth_method,
            auth_data,
            receive_max,
            topic_alias_max: topic_alias_max.unwrap_or(0u16),
            request_problem_info: request_problem_info.unwrap_or(true),
            request_response_info: request_response_info.unwrap_or(false),
            user_properties,
            max_packet_size,

            client_id,
            will_message,
            username,
            password,
        })
    }
}

impl Connect {
    /// Effective Receive Maximum for server→client flow control when the property was omitted on the wire.
    ///
    /// MQTT v5 defaults absent CONNECT Receive Maximum to 65535 ([`RECEIVE_MAX_DEFAULT`]).
    pub fn receive_max_or_default(&self) -> NonZeroU16 {
        self.receive_max.unwrap_or(RECEIVE_MAX_DEFAULT)
    }
}

impl Default for Connect {
    fn default() -> Connect {
        Connect {
            clean_start: false,
            keep_alive: 0,
            session_expiry_interval: 0,
            auth_method: None,
            auth_data: None,
            request_problem_info: true,
            request_response_info: false,
            receive_max: None,
            topic_alias_max: 0,
            user_properties: Vec::new(),
            max_packet_size: None,
            will_message: None,
            client_id: ByteString::default(),
            username: None,
            password: None,
        }
    }
}

fn decode_will_message(src: &mut Bytes, flags: ConnectFlags) -> Result<WillMessage, DecodeError> {
    let mut will_delay_interval = None;
    let mut correlation_data = None;
    let mut message_expiry_interval = None;
    let mut content_type = None;
    let mut user_properties = Vec::new();
    let mut payload_format_indicator = None;
    let mut response_topic = None;
    let prop_src = &mut utils::take_properties(src)?;
    while prop_src.has_remaining() {
        match prop_src.get_u8() {
            pt::WILL_DELAY_INT => will_delay_interval.read_value(prop_src)?,
            pt::CORR_DATA => correlation_data.read_value(prop_src)?,
            pt::MSG_EXPIRY_INT => message_expiry_interval.read_value(prop_src)?,
            pt::CONTENT_TYPE => content_type.read_value(prop_src)?,
            pt::UTF8_PAYLOAD => payload_format_indicator.read_value(prop_src)?,
            pt::RESP_TOPIC => response_topic.read_value(prop_src)?,
            pt::USER => user_properties.push(UserProperty::decode(prop_src)?),
            _ => return Err(DecodeError::MalformedPacket("unknown property or reason code")),
        }
    }

    let topic = decode_topic_name(src)?;
    let payload = Bytes::decode(src)?;
    Ok(WillMessage {
        qos: QoS::try_from((flags & ConnectFlags::WILL_QOS).bits() >> WILL_QOS_SHIFT)?,
        retain: flags.contains(ConnectFlags::WILL_RETAIN),
        topic,
        payload,
        will_delay_interval,
        correlation_data,
        message_expiry_interval,
        content_type,
        user_properties,
        payload_format_indicator,
        response_topic,
    })
}

impl WireBodyEncode for Connect {
    fn encoded_body_len(&self, _max_out: u32) -> Result<usize, EncodeError> {
        let prop_len = self.properties_len();
        Ok(6 // protocol name
            + 1 // protocol level
            + 1 // connect flags
            + 2 // keep alive
            + var_int_len(prop_len) as usize // properties len
            + prop_len // properties
            + self.client_id.encoded_size()
            + self.will_message.as_ref().map_or(0, |will| { // will message content
                let prop_len = will.properties_len();
                var_int_len(prop_len) as usize + prop_len + will.topic.encoded_size() + will.payload.encoded_size()
            })
            + self.username.as_ref().map_or(0, |v| v.encoded_size())
            + self.password.as_ref().map_or(0, |v| v.encoded_size()))
    }

    fn encode_body(&self, buf: &mut BytesMut, _max_out: u32) -> Result<(), EncodeError> {
        b"MQTT".as_ref().encode(buf)?;

        let mut flags = ConnectFlags::empty();

        if self.username.is_some() {
            flags |= ConnectFlags::USERNAME;
        }
        if self.password.is_some() {
            flags |= ConnectFlags::PASSWORD;
        }

        if let Some(will) = self.will_message.as_ref() {
            flags |= ConnectFlags::WILL;

            if will.retain {
                flags |= ConnectFlags::WILL_RETAIN;
            }

            flags |= ConnectFlags::from_bits_truncate(u8::from(will.qos) << WILL_QOS_SHIFT);
        }

        if self.clean_start {
            flags |= ConnectFlags::CLEAN_START;
        }

        buf.put_slice(&[MQTT_LEVEL_5, flags.bits()]);

        self.keep_alive.encode(buf)?;

        let prop_len = self.properties_len();
        utils::write_variable_length(prop_len as u32, buf)?; // safe: whole message size is vetted via max size check in codec

        encode_property_default(&self.session_expiry_interval, 0, pt::SESS_EXPIRY_INT, buf)?;
        encode_property(&self.auth_method, pt::AUTH_METHOD, buf)?;
        encode_property(&self.auth_data, pt::AUTH_DATA, buf)?;
        encode_property_default(&self.request_problem_info, true, pt::REQ_PROB_INFO, buf)?; // §3.1.2.11.7
        encode_property_default(&self.request_response_info, false, pt::REQ_RESP_INFO, buf)?; // §3.1.2.11.6
        encode_property(&self.receive_max, pt::RECEIVE_MAX, buf)?;
        encode_property(&self.max_packet_size, pt::MAX_PACKET_SIZE, buf)?;
        encode_property_default(&self.topic_alias_max, 0, pt::TOPIC_ALIAS_MAX, buf)?;
        self.user_properties.encode(buf)?;

        self.client_id.encode(buf)?;

        if let Some(will) = self.will_message.as_ref() {
            if will.topic.is_empty() {
                return Err(EncodeError::MalformedPacket);
            }
            let prop_len = will.properties_len();
            utils::write_variable_length(prop_len as u32, buf)?; // safe: whole message size is checked for max already

            encode_property(&will.will_delay_interval, pt::WILL_DELAY_INT, buf)?;
            encode_property(&will.payload_format_indicator, pt::UTF8_PAYLOAD, buf)?;
            encode_property(&will.message_expiry_interval, pt::MSG_EXPIRY_INT, buf)?;
            encode_property(&will.content_type, pt::CONTENT_TYPE, buf)?;
            encode_property(&will.response_topic, pt::RESP_TOPIC, buf)?;
            encode_property(&will.correlation_data, pt::CORR_DATA, buf)?;
            will.user_properties.encode(buf)?;
            will.topic.encode(buf)?;
            will.payload.encode(buf)?;
        }
        if let Some(s) = self.username.as_ref() {
            s.encode(buf)?;
        }
        if let Some(pwd) = self.password.as_ref() {
            pwd.encode(buf)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};
    use rstest::rstest;

    use super::*;
    use crate::codec::error::DecodeError;

    /// Builds a CONNECT body: header + `flags` + fixed keep-alive + `properties` + `tail`.
    ///
    /// `tail` holds everything after the property block (Client ID and, per case,
    /// User Name / Password) so reject-vector tests can vary just that suffix.
    fn connect_payload(flags: u8, properties: &[u8], tail: &[u8]) -> Vec<u8> {
        let mut payload = Vec::from(b"\x00\x04MQTT\x05".as_slice());
        payload.push(flags);
        payload.extend_from_slice(&[0x00, 0x3C]);
        payload.extend_from_slice(properties);
        payload.extend_from_slice(tail);
        payload
    }

    fn minimal_connect_payload(flags: u8, properties: &[u8]) -> Vec<u8> {
        connect_payload(flags, properties, b"\x00\x0512345")
    }

    #[rstest]
    // [MQTT-3.1.2-3] reserved Connect Flags bit 0 must be 0
    #[case::reserved_flag_bit_zero_must_be_clear(0x01, b"\x00".as_slice(), b"\x00\x0512345".as_slice(), DecodeError::ConnectReservedFlagSet)]
    // [MQTT-3.1.2-16] User Name Flag 0 → User Name must not be present
    #[case::username_present_without_username_flag(0x00, b"\x00".as_slice(), b"\x00\x0512345\x00\x04user".as_slice(), DecodeError::InvalidLength)]
    // [MQTT-3.1.2-17] User Name Flag 1 → User Name must be present
    #[case::username_missing_with_username_flag_set(0x80, b"\x00".as_slice(), b"\x00\x0512345".as_slice(), DecodeError::InvalidLength)]
    // [MQTT-3.1.2-18] Password Flag 0 → Password must not be present
    #[case::password_present_without_password_flag(0x00, b"\x00".as_slice(), b"\x00\x0512345\x00\x04pass".as_slice(), DecodeError::InvalidLength)]
    // [MQTT-3.1.2-19] Password Flag 1 → Password must be present
    #[case::password_missing_with_password_flag_set(0x40, b"\x00".as_slice(), b"\x00\x0512345".as_slice(), DecodeError::InvalidLength)]
    // [MQTT-3.1.3-4] ClientID must be a UTF-8 Encoded String
    #[case::invalid_utf8_client_id(0x02, b"\x00".as_slice(), b"\x00\x02\xFF\xFE".as_slice(), DecodeError::MalformedPacket("invalid UTF-8 string"))]
    // [MQTT-3.1.3-12] User Name must be UTF-8 when User Name Flag is 1
    #[case::invalid_utf8_username_when_flag_set(0x80, b"\x00".as_slice(), b"\x00\x0512345\x00\x02\xFF\xFE".as_slice(), DecodeError::MalformedPacket("invalid UTF-8 string"))]
    // [MQTT-3.1.2-11] Will QoS bits must be zero when Will Flag is 0
    #[case::will_qos_bits_set_without_will_flag(0x08, b"\x00".as_slice(), b"\x00\x0512345".as_slice(), DecodeError::MalformedPacket("Will QoS set without Will Flag"))]
    // [MQTT-3.1.2-13] Will Retain must be 0 when Will Flag is 0
    #[case::will_retain_set_without_will_flag(0x20, b"\x00".as_slice(), b"\x00\x0512345".as_slice(), DecodeError::MalformedPacket("Will Retain set without Will Flag"))]
    // CONNECT property rules: Authentication Data requires Authentication Method
    #[case::auth_data_without_auth_method(0x02, b"\x05\x16\x00\x02ab".as_slice(), b"\x00\x0512345".as_slice(), DecodeError::MalformedPacket("Authentication Data without Authentication Method"))]
    fn connect_decode_rejects_invalid_flags(
        #[case] flags: u8,
        #[case] properties: &[u8],
        #[case] tail: &[u8],
        #[case] expected: DecodeError,
    ) {
        let mut src = Bytes::from(connect_payload(flags, properties, tail));
        let result = Connect::decode(&mut src);
        assert_eq!(result.map_err(|e| e.to_string()), Err(expected.to_string()));
    }

    #[test]
    fn connect_encode_sets_username_password_flags_with_payload() {
        // [MQTT-3.1.2-16..19] encode: flags match username/password presence
        let connect = Connect {
            client_id: ByteString::from_static("id"),
            username: Some(ByteString::from_static("user")),
            password: Some(Bytes::from_static(b"pass")),
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        let body_len = connect.encoded_body_len(u32::MAX).unwrap() as u32;
        connect.encode_body(&mut buf, body_len).unwrap();
        let flags = buf[7];
        assert_eq!(flags & 0xC0, 0xC0, "USERNAME and PASSWORD flags must be set");
        let decoded = Connect::decode(&mut buf.freeze()).unwrap();
        assert_eq!(decoded.username.as_ref().map(|u| u.as_ref()), Some("user"));
        assert_eq!(decoded.password.as_deref(), Some(b"pass".as_ref()));
    }

    #[test]
    fn connect_decodes_password_only_without_username() {
        // v5 allows Password without User Name (e.g. token/JWT auth); v3.1.1 [MQTT-3.1.2-22] does not.
        let mut payload = minimal_connect_payload(0x40, b"\x00");
        payload.extend_from_slice(b"\x00\x05token");
        let mut src = Bytes::from(payload);
        let connect = Connect::decode(&mut src).unwrap();
        assert!(connect.username.is_none());
        assert_eq!(connect.password.as_deref(), Some(b"token".as_ref()));
    }

    #[test]
    fn connect_password_only_without_username_round_trips() {
        let connect = Connect {
            clean_start: true,
            keep_alive: 60,
            client_id: ByteString::from_static("id"),
            username: None,
            password: Some(Bytes::from_static(b"token")),
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        let body_len = connect.encoded_body_len(1024).unwrap() as u32;
        connect.encode_body(&mut buf, body_len).unwrap();
        let decoded = Connect::decode(&mut buf.freeze()).unwrap();
        assert_eq!(decoded.username, None);
        assert_eq!(decoded.password.as_deref(), Some(b"token".as_ref()));
    }

    #[test]
    fn connect_receive_max_or_default_uses_spec_default_when_absent() {
        let connect = Connect::default();
        assert_eq!(connect.receive_max, None);
        assert_eq!(connect.receive_max_or_default(), RECEIVE_MAX_DEFAULT);
    }

    #[test]
    fn connect_receive_max_or_default_preserves_explicit_value() {
        let explicit = NonZeroU16::new(2).unwrap();
        let connect = Connect {
            receive_max: Some(explicit),
            ..Default::default()
        };
        assert_eq!(connect.receive_max_or_default(), explicit);
    }

    fn round_trip_connect(connect: &Connect) {
        let mut buf = BytesMut::new();
        let body_len = connect.encoded_body_len(u32::MAX).unwrap() as u32;
        connect.encode_body(&mut buf, body_len).unwrap();
        let decoded = Connect::decode(&mut buf.freeze()).unwrap();
        assert_eq!(decoded, *connect);
    }

    #[test]
    fn connect_client_fields_round_trip() {
        round_trip_connect(&Connect {
            clean_start: true,
            keep_alive: 120,
            client_id: ByteString::from_static("mqtt-glide-01"),
            username: Some(ByteString::from_static("mqtt-user")),
            password: Some(Bytes::from_static(b"mqtt-pass")),
            receive_max: NonZeroU16::new(42),
            ..Default::default()
        });

        round_trip_connect(&Connect {
            clean_start: false,
            keep_alive: 0,
            client_id: ByteString::from_static(""),
            username: None,
            password: None,
            receive_max: None,
            ..Default::default()
        });

        round_trip_connect(&Connect::default().client_id("builder-id").receive_max(10));
        round_trip_connect(
            &Connect::default()
                .client_id("builder-id")
                .username("mqtt-user")
                .password("mqtt-pass")
                .receive_max(10),
        );
    }
}
