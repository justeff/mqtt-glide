//! MQTT **CONNACK** control packet — variable header and properties.
//!
//! - OASIS MQTT v5.0: §3.2 CONNACK — Connect acknowledgement

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

/// Connect acknowledgment packet
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ConnectAck {
    /// enables a Client to establish whether the Client and Server have a consistent view
    /// about whether there is already stored Session state.
    pub session_present: bool,
    pub reason_code: ConnectAckReason,

    pub session_expiry_interval_secs: Option<u32>,
    pub receive_max: NonZeroU16,
    pub max_qos: QoS,
    pub max_packet_size: Option<u32>,
    pub assigned_client_id: Option<ByteString>,
    pub topic_alias_max: u16,
    pub retain_available: bool,
    pub wildcard_subscription_available: bool,
    pub subscription_identifiers_available: bool,
    pub shared_subscription_available: bool,
    pub server_keepalive_sec: Option<u16>,
    pub response_info: Option<ByteString>,
    pub server_reference: Option<ByteString>,
    pub auth_method: Option<ByteString>,
    pub auth_data: Option<Bytes>,
    pub reason_string: Option<ByteString>,
    pub user_properties: UserProperties,
}

impl Default for ConnectAck {
    fn default() -> ConnectAck {
        ConnectAck {
            session_present: false,
            reason_code: ConnectAckReason::Success,
            session_expiry_interval_secs: None,
            receive_max: RECEIVE_MAX_DEFAULT,
            max_qos: QoS::ExactlyOnce,
            max_packet_size: None,
            assigned_client_id: None,
            topic_alias_max: 0,
            retain_available: true,
            wildcard_subscription_available: true,
            subscription_identifiers_available: true,
            shared_subscription_available: true,
            server_keepalive_sec: None,
            response_info: None,
            server_reference: None,
            auth_method: None,
            auth_data: None,
            reason_string: None,
            user_properties: Vec::new(),
        }
    }
}

/// CONNACK reason codes ([MQTT-3.2.2.3]). Unlisted wire values decode as [`Unknown`](ConnectAckReason::Unknown).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectAckReason {
    Success,
    UnspecifiedError,
    MalformedPacket,
    ProtocolError,
    ImplementationSpecificError,
    UnsupportedProtocolVersion,
    ClientIdentifierNotValid,
    BadUserNameOrPassword,
    NotAuthorized,
    ServerUnavailable,
    ServerBusy,
    Banned,
    BadAuthenticationMethod,
    TopicNameInvalid,
    PacketTooLarge,
    QuotaExceeded,
    PayloadFormatInvalid,
    RetainNotSupported,
    QosNotSupported,
    UseAnotherServer,
    ServerMoved,
    ConnectionRateExceeded,
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
    /// Returns a human-readable description of the reason code
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

        let mut session_expiry_interval_secs = None;
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
                pt::SESS_EXPIRY_INT => session_expiry_interval_secs.read_value(prop_src)?,
                pt::RECEIVE_MAX => receive_max.read_value(prop_src)?,
                pt::MAX_QOS => {
                    ensure!(max_qos.is_none(), DecodeError::MalformedPacket); // property is set twice while not allowed
                    ensure!(prop_src.has_remaining(), DecodeError::InvalidLength);
                    max_qos = Some(prop_src.get_u8().try_into()?);
                }
                pt::RETAIN_AVAIL => retain_available.read_value(prop_src)?,
                pt::MAX_PACKET_SIZE => max_packet_size.read_value(prop_src)?,
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
                _ => return Err(DecodeError::MalformedPacket),
            }
        }
        ensure!(!src.has_remaining(), DecodeError::InvalidLength);

        Ok(ConnectAck {
            session_present: flags.contains(ConnectAckFlags::SESSION_PRESENT),
            reason_code,
            session_expiry_interval_secs,
            receive_max: receive_max.unwrap_or(RECEIVE_MAX_DEFAULT),
            max_qos: max_qos.unwrap_or(QoS::ExactlyOnce),
            max_packet_size,
            assigned_client_id,
            topic_alias_max: topic_alias_max.unwrap_or(0u16),
            retain_available: retain_available.unwrap_or(true),
            wildcard_subscription_available: wildcard_sub_avail.unwrap_or(true),
            subscription_identifiers_available: sub_ids_avail.unwrap_or(true),
            shared_subscription_available: shared_sub_avail.unwrap_or(true),
            server_keepalive_sec: server_ka_sec,
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
        const HEADER_LEN: usize = 2; // state flags byte + reason code

        let mut prop_len = encoded_property_size(&self.session_expiry_interval_secs)
            + encoded_property_size_default(&self.receive_max, RECEIVE_MAX_DEFAULT)
            + if self.max_qos < QoS::ExactlyOnce { 1 + 1 } else { 0 }
            + encoded_property_size(&self.max_packet_size)
            + encoded_property_size(&self.assigned_client_id)
            + encoded_property_size_default(&self.retain_available, true)
            + encoded_property_size_default(&self.wildcard_subscription_available, true)
            + encoded_property_size_default(&self.subscription_identifiers_available, true)
            + encoded_property_size_default(&self.shared_subscription_available, true)
            + encoded_property_size(&self.server_keepalive_sec)
            + encoded_property_size(&self.response_info)
            + encoded_property_size(&self.server_reference)
            + encoded_property_size(&self.auth_method)
            + encoded_property_size(&self.auth_data);
        if self.topic_alias_max > 0 {
            prop_len += 1 + self.topic_alias_max.encoded_size(); // [property type, value..]
        }

        let diag_len = encoded_size_opt_props(
            &self.user_properties,
            &self.reason_string,
            reduce_limit(max_out, HEADER_LEN + 4 + prop_len),
        ); // exclude other props and max of 4 bytes for property length value
        prop_len += diag_len;
        Ok(HEADER_LEN + var_int_len(prop_len) as usize + prop_len)
    }

    fn encode_body(&self, buf: &mut BytesMut, max_out: u32) -> Result<(), EncodeError> {
        let start_len = buf.len();

        buf.put_slice(&[u8::from(self.session_present), self.reason_code.into()]);

        let prop_len = var_int_len_from_size(max_out - 2);
        utils::write_variable_length(prop_len, buf)?;

        encode_property(&self.session_expiry_interval_secs, pt::SESS_EXPIRY_INT, buf)?;
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
        encode_property(&self.server_keepalive_sec, pt::SERVER_KA, buf)?;
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
