//! MQTT CONNACK (§3.2).
//!
//! Variable header and properties per OASIS MQTT Version 5.0.
//!
//! # Errors
//! Decode rejects reserved Acknowledge flags ([MQTT-3.2.2-1]), duplicate property
//! identifiers (§2.2.2.2), and trailing bytes after the properties (§2.1.4).

use std::num::NonZeroU16;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use bytestring::ByteString;

use crate::codec::error::{DecodeError, EncodeError};
use crate::codec::properties::{
    encode_opt_props, encode_property, encode_property_default, encoded_property_size, encoded_property_size_default,
    encoded_size_opt_props, reduce_limit, var_int_len, var_int_len_from_size,
};
use crate::codec::types::{ConnectAckFlags, QoS};
use crate::codec::utils::{self, Decode, Encode, Property};
use crate::codec::v5::RECEIVE_MAX_DEFAULT;
use crate::codec::v5::body_encode::WireBodyEncode;
use crate::codec::v5::{UserProperties, UserProperty, property_type as pt};

/// CONNACK packet body (§3.2).
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ConnectAck {
    /// Session Present flag (§3.2.2.1.1).
    pub session_present: bool,
    /// Connect Reason Code (§3.2.2.2).
    pub reason_code: ConnectAckReason,

    /// Session Expiry Interval (§3.2.2.3.2).
    pub session_expiry_interval: Option<u32>,
    /// Receive Maximum (§3.2.2.3.3); default 65_535 when omitted.
    pub receive_max: NonZeroU16,
    /// Maximum QoS (§3.2.2.3.4).
    pub max_qos: QoS,
    /// Maximum Packet Size (§3.2.2.3.6).
    pub max_packet_size: Option<u32>,
    /// Assigned Client Identifier (§3.2.2.3.7).
    pub assigned_client_id: Option<ByteString>,
    /// Topic Alias Maximum (§3.2.2.3.8).
    pub topic_alias_max: u16,
    /// Retain Available (§3.2.2.3.5).
    pub retain_available: bool,
    /// Wildcard Subscription Available (§3.2.2.3.11).
    pub wildcard_subscription_available: bool,
    /// Subscription Identifier Available (§3.2.2.3.12).
    pub subscription_identifiers_available: bool,
    /// Shared Subscription Available (§3.2.2.3.13).
    pub shared_subscription_available: bool,
    /// Server Keep Alive (§3.2.2.3.14).
    pub server_keep_alive: Option<u16>,
    /// Response Information (§3.2.2.3.15).
    pub response_info: Option<ByteString>,
    /// Server Reference (§3.2.2.3.16).
    pub server_reference: Option<ByteString>,
    /// Authentication Method (§3.2.2.3.17).
    pub auth_method: Option<ByteString>,
    /// Authentication Data (§3.2.2.3.18).
    pub auth_data: Option<Bytes>,
    /// Reason String (§3.2.2.3.9).
    pub reason_string: Option<ByteString>,
    /// User Property (§3.2.2.3.10).
    pub user_properties: UserProperties,
}

impl Default for ConnectAck {
    fn default() -> ConnectAck {
        ConnectAck {
            session_present: false,
            reason_code: ConnectAckReason::Success,
            session_expiry_interval: None,
            receive_max: RECEIVE_MAX_DEFAULT,
            max_qos: QoS::ExactlyOnce,
            max_packet_size: None,
            assigned_client_id: None,
            topic_alias_max: 0,
            retain_available: true,
            wildcard_subscription_available: true,
            subscription_identifiers_available: true,
            shared_subscription_available: true,
            server_keep_alive: None,
            response_info: None,
            server_reference: None,
            auth_method: None,
            auth_data: None,
            reason_string: None,
            user_properties: Vec::new(),
        }
    }
}

/// CONNACK reason codes (§3.2.2.2). Unlisted wire values decode as [`Unknown`](ConnectAckReason::Unknown).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectAckReason {
    /// 0x00: the connection is accepted.
    Success,
    /// 0x80: the broker does not say why it refused.
    UnspecifiedError,
    /// 0x81: the CONNECT could not be parsed.
    MalformedPacket,
    /// 0x82: the CONNECT violated the protocol.
    ProtocolError,
    /// 0x83: valid CONNECT, refused by the broker implementation.
    ImplementationSpecificError,
    /// 0x84: the broker does not support the requested MQTT version.
    UnsupportedProtocolVersion,
    /// 0x85: the Client Identifier is valid UTF-8 but not accepted by the broker.
    ClientIdentifierNotValid,
    /// 0x86: the user name or password is not accepted.
    BadUserNameOrPassword,
    /// 0x87: the client is not authorized to connect.
    NotAuthorized,
    /// 0x88: the MQTT service is not available.
    ServerUnavailable,
    /// 0x89: the broker is busy; try again later.
    ServerBusy,
    /// 0x8A: the client is banned.
    Banned,
    /// 0x8C: the Authentication Method is not supported or does not match the one in use.
    BadAuthenticationMethod,
    /// 0x90: the Will Topic Name is well formed but not accepted.
    TopicNameInvalid,
    /// 0x95: the CONNECT exceeds the broker's maximum packet size.
    PacketTooLarge,
    /// 0x97: an implementation or administrative limit was exceeded.
    QuotaExceeded,
    /// 0x99: the Will payload does not match its Payload Format Indicator.
    PayloadFormatInvalid,
    /// 0x9A: the broker does not support retained messages, but Will Retain was set.
    RetainNotSupported,
    /// 0x9B: the broker does not support the Will QoS.
    QosNotSupported,
    /// 0x9C: connect to another broker (see the Server Reference).
    UseAnotherServer,
    /// 0x9D: the broker has moved for good (see the Server Reference).
    ServerMoved,
    /// 0x9F: the connection rate limit was exceeded.
    ConnectionRateExceeded,
    /// A reason code this client does not know, as received.
    Unknown(u8),
}

impl From<u8> for ConnectAckReason {
    fn from(v: u8) -> Self {
        match v {
            0 => Self::Success,
            128 => Self::UnspecifiedError,
            129 => Self::MalformedPacket,
            130 => Self::ProtocolError,
            131 => Self::ImplementationSpecificError,
            132 => Self::UnsupportedProtocolVersion,
            133 => Self::ClientIdentifierNotValid,
            134 => Self::BadUserNameOrPassword,
            135 => Self::NotAuthorized,
            136 => Self::ServerUnavailable,
            137 => Self::ServerBusy,
            138 => Self::Banned,
            140 => Self::BadAuthenticationMethod,
            144 => Self::TopicNameInvalid,
            149 => Self::PacketTooLarge,
            151 => Self::QuotaExceeded,
            153 => Self::PayloadFormatInvalid,
            154 => Self::RetainNotSupported,
            155 => Self::QosNotSupported,
            156 => Self::UseAnotherServer,
            157 => Self::ServerMoved,
            159 => Self::ConnectionRateExceeded,
            other => Self::Unknown(other),
        }
    }
}

impl From<ConnectAckReason> for u8 {
    fn from(v: ConnectAckReason) -> Self {
        match v {
            ConnectAckReason::Success => 0,
            ConnectAckReason::UnspecifiedError => 128,
            ConnectAckReason::MalformedPacket => 129,
            ConnectAckReason::ProtocolError => 130,
            ConnectAckReason::ImplementationSpecificError => 131,
            ConnectAckReason::UnsupportedProtocolVersion => 132,
            ConnectAckReason::ClientIdentifierNotValid => 133,
            ConnectAckReason::BadUserNameOrPassword => 134,
            ConnectAckReason::NotAuthorized => 135,
            ConnectAckReason::ServerUnavailable => 136,
            ConnectAckReason::ServerBusy => 137,
            ConnectAckReason::Banned => 138,
            ConnectAckReason::BadAuthenticationMethod => 140,
            ConnectAckReason::TopicNameInvalid => 144,
            ConnectAckReason::PacketTooLarge => 149,
            ConnectAckReason::QuotaExceeded => 151,
            ConnectAckReason::PayloadFormatInvalid => 153,
            ConnectAckReason::RetainNotSupported => 154,
            ConnectAckReason::QosNotSupported => 155,
            ConnectAckReason::UseAnotherServer => 156,
            ConnectAckReason::ServerMoved => 157,
            ConnectAckReason::ConnectionRateExceeded => 159,
            ConnectAckReason::Unknown(code) => code,
        }
    }
}

impl ConnectAckReason {
    /// Short English label for logging and diagnostics (not a wire field).
    pub fn reason(self) -> &'static str {
        match self {
            ConnectAckReason::Success => "Connection Accepted",
            ConnectAckReason::UnsupportedProtocolVersion => "protocol version is not supported",
            ConnectAckReason::ClientIdentifierNotValid => "client identifier is invalid",
            ConnectAckReason::ServerUnavailable => "Server unavailable",
            ConnectAckReason::BadUserNameOrPassword => "bad user name or password",
            ConnectAckReason::NotAuthorized => "not authorized",
            ConnectAckReason::Unknown(_) => "Connection Refused (unknown reason code)",
            _ => "Connection Refused",
        }
    }
}

impl ConnectAck {
    pub(crate) fn decode(src: &mut Bytes) -> Result<Self, DecodeError> {
        ensure!(src.remaining() >= 2, DecodeError::InvalidLength);
        let flags = ConnectAckFlags::from_bits(src.get_u8()).ok_or(DecodeError::ConnectAckReservedFlagSet)?; // [MQTT-3.2.2-1]

        let reason_code = ConnectAckReason::from(src.get_u8());

        let prop_src = &mut utils::take_properties(src)?;

        let mut session_expiry_interval = None;
        let mut receive_max = None;
        let mut max_qos = None;
        let mut retain_available = None;
        let mut max_packet_size = None;
        let mut assigned_client_id = None;
        let mut topic_alias_max = None;
        let mut reason_string = None;
        let mut user_properties = Vec::new();
        let mut wildcard_sub_avail = None;
        let mut sub_ids_avail = None;
        let mut shared_sub_avail = None;
        let mut server_ka_sec = None;
        let mut response_info = None;
        let mut server_reference = None;
        let mut auth_method = None;
        let mut auth_data = None;
        while prop_src.has_remaining() {
            match prop_src.get_u8() {
                pt::SESS_EXPIRY_INT => session_expiry_interval.read_value(prop_src)?,
                pt::RECEIVE_MAX => {
                    let mut value: Option<u16> = None;
                    value.read_value(prop_src)?;
                    ensure!(
                        receive_max.is_none(),
                        DecodeError::ProtocolViolation("property included more than once")
                    );
                    // §3.2.2.3.3: a Receive Maximum of 0 is a Protocol Error.
                    receive_max = Some(
                        NonZeroU16::new(value.unwrap_or(0))
                            .ok_or(DecodeError::ProtocolViolation("Receive Maximum 0 is not allowed"))?,
                    );
                }
                pt::MAX_QOS => {
                    // Duplicate property identifier (§2.2.2.2); only 0 and 1 may be sent
                    // (§3.2.2.3.4): an absent property means QoS 2.
                    ensure!(
                        max_qos.is_none(),
                        DecodeError::ProtocolViolation("property included more than once")
                    );
                    ensure!(prop_src.has_remaining(), DecodeError::InvalidLength);
                    let qos: QoS = prop_src.get_u8().try_into()?;
                    ensure!(
                        qos != QoS::ExactlyOnce,
                        DecodeError::ProtocolViolation("CONNACK Maximum QoS must be 0 or 1")
                    );
                    max_qos = Some(qos);
                }
                pt::RETAIN_AVAIL => retain_available.read_value(prop_src)?,
                pt::MAX_PACKET_SIZE => {
                    max_packet_size.read_value(prop_src)?;
                    // §3.2.2.3.6: Maximum Packet Size of 0 is a Protocol Error (no Appendix-B
                    // ID); the codec treats limit 0 as "unbounded", so accepting it would
                    // silently disable enforcement.
                    ensure!(max_packet_size != Some(0), DecodeError::InvalidProtocol);
                }
                pt::ASSND_CLIENT_ID => assigned_client_id.read_value(prop_src)?,
                pt::TOPIC_ALIAS_MAX => topic_alias_max.read_value(prop_src)?,
                pt::REASON_STRING => reason_string.read_value(prop_src)?,
                pt::USER => user_properties.push(UserProperty::decode(prop_src)?),
                pt::WILDCARD_SUB_AVAIL => wildcard_sub_avail.read_value(prop_src)?,
                pt::SUB_IDS_AVAIL => sub_ids_avail.read_value(prop_src)?,
                pt::SHARED_SUB_AVAIL => shared_sub_avail.read_value(prop_src)?,
                pt::SERVER_KA => server_ka_sec.read_value(prop_src)?,
                pt::RESP_INFO => response_info.read_value(prop_src)?,
                pt::SERVER_REF => server_reference.read_value(prop_src)?,
                pt::AUTH_METHOD => auth_method.read_value(prop_src)?,
                pt::AUTH_DATA => auth_data.read_value(prop_src)?,
                _ => return Err(DecodeError::MalformedPacket("unknown property or reason code")),
            }
        }
        ensure!(!src.has_remaining(), DecodeError::InvalidLength);

        Ok(ConnectAck {
            session_present: flags.contains(ConnectAckFlags::SESSION_PRESENT),
            reason_code,
            session_expiry_interval,
            receive_max: receive_max.unwrap_or(RECEIVE_MAX_DEFAULT),
            max_qos: max_qos.unwrap_or(QoS::ExactlyOnce),
            max_packet_size,
            assigned_client_id,
            topic_alias_max: topic_alias_max.unwrap_or(0u16),
            retain_available: retain_available.unwrap_or(true),
            wildcard_subscription_available: wildcard_sub_avail.unwrap_or(true),
            subscription_identifiers_available: sub_ids_avail.unwrap_or(true),
            shared_subscription_available: shared_sub_avail.unwrap_or(true),
            server_keep_alive: server_ka_sec,
            response_info,
            server_reference,
            auth_method,
            auth_data,
            reason_string,
            user_properties,
        })
    }
}

impl WireBodyEncode for ConnectAck {
    fn encoded_body_len(&self, max_out: u32) -> Result<usize, EncodeError> {
        // Acknowledge Flags + Reason Code (§3.2.2).
        const HEADER_LEN: usize = 2;

        let mut prop_len = encoded_property_size(&self.session_expiry_interval)
            + encoded_property_size_default(&self.receive_max, RECEIVE_MAX_DEFAULT)
            + if self.max_qos < QoS::ExactlyOnce { 1 + 1 } else { 0 }
            + encoded_property_size(&self.max_packet_size)
            + encoded_property_size(&self.assigned_client_id)
            + encoded_property_size_default(&self.retain_available, true)
            + encoded_property_size_default(&self.wildcard_subscription_available, true)
            + encoded_property_size_default(&self.subscription_identifiers_available, true)
            + encoded_property_size_default(&self.shared_subscription_available, true)
            + encoded_property_size(&self.server_keep_alive)
            + encoded_property_size(&self.response_info)
            + encoded_property_size(&self.server_reference)
            + encoded_property_size(&self.auth_method)
            + encoded_property_size(&self.auth_data);
        if self.topic_alias_max > 0 {
            // Property identifier + Two Byte Integer value (§2.2.2).
            prop_len += 1 + self.topic_alias_max.encoded_size();
        }

        // Reason String / User Properties trimmed to fit; reserve ≤4 bytes for Property Length varint.
        let diag_len = encoded_size_opt_props(
            &self.user_properties,
            &self.reason_string,
            reduce_limit(max_out, HEADER_LEN + 4 + prop_len),
        );
        prop_len += diag_len;
        Ok(HEADER_LEN + var_int_len(prop_len) as usize + prop_len)
    }

    fn encode_body(&self, buf: &mut BytesMut, max_out: u32) -> Result<(), EncodeError> {
        let start_len = buf.len();

        buf.put_slice(&[u8::from(self.session_present), self.reason_code.into()]);

        let prop_len = var_int_len_from_size(max_out - 2);
        utils::write_variable_length(prop_len, buf)?;

        encode_property(&self.session_expiry_interval, pt::SESS_EXPIRY_INT, buf)?;
        encode_property_default(&self.receive_max, RECEIVE_MAX_DEFAULT, pt::RECEIVE_MAX, buf)?;
        if self.max_qos < QoS::ExactlyOnce {
            buf.put_slice(&[pt::MAX_QOS, self.max_qos.into()]);
        }
        encode_property_default(&self.retain_available, true, pt::RETAIN_AVAIL, buf)?;
        encode_property(&self.max_packet_size, pt::MAX_PACKET_SIZE, buf)?;
        encode_property(&self.assigned_client_id, pt::ASSND_CLIENT_ID, buf)?;
        encode_property_default(&self.topic_alias_max, 0, pt::TOPIC_ALIAS_MAX, buf)?;
        encode_property_default(&self.wildcard_subscription_available, true, pt::WILDCARD_SUB_AVAIL, buf)?;
        encode_property_default(&self.subscription_identifiers_available, true, pt::SUB_IDS_AVAIL, buf)?;
        encode_property_default(&self.shared_subscription_available, true, pt::SHARED_SUB_AVAIL, buf)?;
        encode_property(&self.server_keep_alive, pt::SERVER_KA, buf)?;
        encode_property(&self.response_info, pt::RESP_INFO, buf)?;
        encode_property(&self.server_reference, pt::SERVER_REF, buf)?;
        encode_property(&self.auth_method, pt::AUTH_METHOD, buf)?;
        encode_property(&self.auth_data, pt::AUTH_DATA, buf)?;

        encode_opt_props(
            &self.user_properties,
            &self.reason_string,
            buf,
            max_out - (buf.len() - start_len) as u32,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::v5::{DisconnectReasonCode, ToReasonCode};

    #[test]
    fn connack_rejects_reserved_ack_flags_bits_7_through_1() {
        // [MQTT-3.2.2-1] CONNACK Connect Acknowledge Flags bits 7-1 must be 0
        let mut src = Bytes::from_static(&[0x02, 0x00, 0x00]);
        assert!(matches!(
            ConnectAck::decode(&mut src),
            Err(DecodeError::ConnectAckReservedFlagSet)
        ));
    }

    #[test]
    fn connack_decodes_unknown_reason_code() {
        // Reason 141 (0x8D) is not assigned in MQTT 5.0 but must parse for client decode.
        let mut src = Bytes::from_static(&[0x00, 0x8D, 0x00]);
        let ack = ConnectAck::decode(&mut src).unwrap();
        assert_eq!(ack.reason_code, ConnectAckReason::Unknown(141));
    }

    /// Well-formed CONNACKs the spec calls a Protocol Error: the client answers them with
    /// DISCONNECT 0x82, not 0x81 Malformed Packet.
    #[rstest::rstest]
    #[case::receive_maximum_zero(&[0x00, 0x00, 0x03, 0x21, 0x00, 0x00])] // §3.2.2.3.3
    #[case::maximum_qos_two(&[0x00, 0x00, 0x02, 0x24, 0x02])] // §3.2.2.3.4
    #[case::duplicate_property(&[0x00, 0x00, 0x04, 0x25, 0x01, 0x25, 0x01])] // §2.2.2.2
    fn connack_protocol_errors_map_to_reason_0x82(#[case] wire: &'static [u8]) {
        let error = ConnectAck::decode(&mut Bytes::from_static(wire)).expect_err("Protocol Error");
        assert!(matches!(error, DecodeError::ProtocolViolation(_)), "{error:?}");
        assert_eq!(error.to_reason_code(), DisconnectReasonCode::ProtocolError);
    }

    #[test]
    fn connack_rejects_zero_max_packet_size() {
        // §3.2.2.3.6: Maximum Packet Size of 0 is a Protocol Error; the codec uses
        // limit 0 as "unbounded", so accepting it would silently disable size enforcement.
        let mut src = Bytes::from_static(&[0x00, 0x00, 0x05, 0x27, 0x00, 0x00, 0x00, 0x00]);
        assert!(matches!(
            ConnectAck::decode(&mut src),
            Err(DecodeError::InvalidProtocol)
        ));

        // Non-zero still parses.
        let mut src = Bytes::from_static(&[0x00, 0x00, 0x05, 0x27, 0x00, 0x00, 0x04, 0x00]);
        let ack = ConnectAck::decode(&mut src).unwrap();
        assert_eq!(ack.max_packet_size, Some(1024));
    }

    #[test]
    fn connack_unknown_reason_code_round_trips() {
        let ack = ConnectAck {
            session_present: false,
            reason_code: ConnectAckReason::Unknown(141),
            receive_max: RECEIVE_MAX_DEFAULT,
            max_qos: QoS::AtMostOnce,
            retain_available: false,
            wildcard_subscription_available: false,
            subscription_identifiers_available: false,
            shared_subscription_available: false,
            topic_alias_max: 0,
            ..Default::default()
        };
        let body_len = ack.encoded_body_len(99999).unwrap() as u32;
        let mut buf = BytesMut::with_capacity(body_len as usize);
        ack.encode_body(&mut buf, body_len).unwrap();
        let decoded = ConnectAck::decode(&mut buf.freeze()).unwrap();
        assert_eq!(decoded.reason_code, ConnectAckReason::Unknown(141));
        assert_eq!(u8::from(decoded.reason_code), 141);
    }
}
