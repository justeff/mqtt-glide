//! MQTT DISCONNECT (§3.14).
//!
//! Variable header and properties per OASIS MQTT Version 5.0.
//!
//! # Errors
//! Decode rejects unknown Reason Codes and trailing bytes after the properties (§2.1.4).

use std::convert::TryFrom;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use bytestring::ByteString;

use crate::codec::error::{DecodeError, EncodeError};
use crate::codec::properties::{
    encode_opt_props, encode_property, encoded_property_size, encoded_size_opt_props, reduce_limit, var_int_len,
    var_int_len_from_size,
};
use crate::codec::utils::{self, Decode, Property};
use crate::codec::v5::body_encode::WireBodyEncode;
use crate::codec::v5::{UserProperties, UserProperty, property_type as pt};

/// DISCONNECT packet body (§3.14).
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Disconnect {
    pub reason_code: DisconnectReasonCode,
    pub session_expiry_interval: Option<u32>,
    pub server_reference: Option<ByteString>,
    pub reason_string: Option<ByteString>,
    pub user_properties: UserProperties,
}

/// Maps application or codec failures to a DISCONNECT Reason Code (§3.14.2.1).
pub trait ToReasonCode {
    fn to_reason_code(&self) -> DisconnectReasonCode;
}

/// DISCONNECT Reason Codes (§3.14.2.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DisconnectReasonCode {
    NormalDisconnection = 0,
    DisconnectWithWillMessage = 4,
    UnspecifiedError = 128,
    MalformedPacket = 129,
    ProtocolError = 130,
    ImplementationSpecificError = 131,
    NotAuthorized = 135,
    ServerBusy = 137,
    ServerShuttingDown = 139,
    BadAuthenticationMethod = 140,
    KeepAliveTimeout = 141,
    SessionTakenOver = 142,
    TopicFilterInvalid = 143,
    TopicNameInvalid = 144,
    ReceiveMaximumExceeded = 147,
    TopicAliasInvalid = 148,
    PacketTooLarge = 149,
    MessageRateTooHigh = 150,
    QuotaExceeded = 151,
    AdministrativeAction = 152,
    PayloadFormatInvalid = 153,
    RetainNotSupported = 154,
    QosNotSupported = 155,
    UseAnotherServer = 156,
    ServerMoved = 157,
    SharedSubscriptionNotSupported = 158,
    ConnectionRateExceeded = 159,
    MaximumConnectTime = 160,
    SubscriptionIdentifiersNotSupported = 161,
    WildcardSubscriptionsNotSupported = 162,
}

impl TryFrom<u8> for DisconnectReasonCode {
    type Error = DecodeError;

    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(Self::NormalDisconnection),
            4 => Ok(Self::DisconnectWithWillMessage),
            128 => Ok(Self::UnspecifiedError),
            129 => Ok(Self::MalformedPacket),
            130 => Ok(Self::ProtocolError),
            131 => Ok(Self::ImplementationSpecificError),
            135 => Ok(Self::NotAuthorized),
            137 => Ok(Self::ServerBusy),
            139 => Ok(Self::ServerShuttingDown),
            140 => Ok(Self::BadAuthenticationMethod),
            141 => Ok(Self::KeepAliveTimeout),
            142 => Ok(Self::SessionTakenOver),
            143 => Ok(Self::TopicFilterInvalid),
            144 => Ok(Self::TopicNameInvalid),
            147 => Ok(Self::ReceiveMaximumExceeded),
            148 => Ok(Self::TopicAliasInvalid),
            149 => Ok(Self::PacketTooLarge),
            150 => Ok(Self::MessageRateTooHigh),
            151 => Ok(Self::QuotaExceeded),
            152 => Ok(Self::AdministrativeAction),
            153 => Ok(Self::PayloadFormatInvalid),
            154 => Ok(Self::RetainNotSupported),
            155 => Ok(Self::QosNotSupported),
            156 => Ok(Self::UseAnotherServer),
            157 => Ok(Self::ServerMoved),
            158 => Ok(Self::SharedSubscriptionNotSupported),
            159 => Ok(Self::ConnectionRateExceeded),
            160 => Ok(Self::MaximumConnectTime),
            161 => Ok(Self::SubscriptionIdentifiersNotSupported),
            162 => Ok(Self::WildcardSubscriptionsNotSupported),
            _ => Err(DecodeError::MalformedPacket("unknown property or reason code")),
        }
    }
}

impl From<DisconnectReasonCode> for u8 {
    fn from(v: DisconnectReasonCode) -> Self {
        v as u8
    }
}

impl Disconnect {
    #[inline]
    pub(crate) fn is_minimal_success(&self) -> bool {
        self.reason_code == DisconnectReasonCode::NormalDisconnection
            && self.session_expiry_interval.is_none()
            && self.server_reference.is_none()
            && self.reason_string.is_none()
            && self.user_properties.is_empty()
    }

    /// Builds a DISCONNECT with the given Reason Code and empty properties (§3.14).
    pub fn new(reason_code: DisconnectReasonCode) -> Self {
        Self {
            reason_code,
            session_expiry_interval: None,
            server_reference: None,
            reason_string: None,
            user_properties: Vec::new(),
        }
    }

    pub(crate) fn decode(src: &mut Bytes) -> Result<Self, DecodeError> {
        let disconnect = if src.has_remaining() {
            let reason_code = src.get_u8().try_into()?;

            if src.has_remaining() {
                let mut session_exp_secs = None;
                let mut server_reference = None;
                let mut reason_string = None;
                let mut user_properties = Vec::new();

                let prop_src = &mut utils::take_properties(src)?;
                while prop_src.has_remaining() {
                    match prop_src.get_u8() {
                        pt::SESS_EXPIRY_INT => session_exp_secs.read_value(prop_src)?,
                        pt::REASON_STRING => reason_string.read_value(prop_src)?,
                        pt::USER => user_properties.push(UserProperty::decode(prop_src)?),
                        pt::SERVER_REF => server_reference.read_value(prop_src)?,
                        _ => return Err(DecodeError::MalformedPacket("unknown property or reason code")),
                    }
                }
                ensure!(!src.has_remaining(), DecodeError::InvalidLength);

                Self {
                    reason_code,
                    session_expiry_interval: session_exp_secs,
                    server_reference,
                    reason_string,
                    user_properties,
                }
            } else {
                Self {
                    reason_code,
                    ..Default::default()
                }
            }
        } else {
            Self::default()
        };
        Ok(disconnect)
    }
}

impl Default for Disconnect {
    fn default() -> Self {
        Self {
            reason_code: DisconnectReasonCode::NormalDisconnection,
            session_expiry_interval: None,
            server_reference: None,
            reason_string: None,
            user_properties: Vec::new(),
        }
    }
}

impl WireBodyEncode for Disconnect {
    fn encoded_body_len(&self, max_out: u32) -> Result<usize, EncodeError> {
        if self.is_minimal_success() {
            return Ok(0);
        }
        const HEADER_LEN: usize = 1; // Reason Code (§3.14.2.1)

        let mut prop_len =
            encoded_property_size(&self.session_expiry_interval) + encoded_property_size(&self.server_reference);
        // Reason String / User Properties trimmed to fit; reserve ≤4 bytes for Property Length varint.
        let diag_len = encoded_size_opt_props(
            &self.user_properties,
            &self.reason_string,
            reduce_limit(max_out, prop_len + HEADER_LEN + 4),
        );
        prop_len += diag_len;
        Ok(HEADER_LEN + var_int_len(prop_len) as usize + prop_len)
    }

    fn encode_body(&self, buf: &mut BytesMut, max_out: u32) -> Result<(), EncodeError> {
        if self.is_minimal_success() {
            return Ok(());
        }
        let start_len = buf.len();
        buf.put_u8(self.reason_code.into());

        let prop_len = var_int_len_from_size(max_out - 1);
        utils::write_variable_length(prop_len, buf)?;
        encode_property(&self.session_expiry_interval, pt::SESS_EXPIRY_INT, buf)?;
        encode_property(&self.server_reference, pt::SERVER_REF, buf)?;
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
    use bytes::{Bytes, BytesMut};

    use super::*;
    use crate::codec::v5::UserProperties;

    #[test]
    fn disconnect_decode_rejects_unknown_reason_code() {
        let mut src = Bytes::from_static(&[0x7F]);
        assert!(matches!(
            Disconnect::decode(&mut src),
            Err(DecodeError::MalformedPacket(_))
        ));
    }

    #[test]
    fn disconnect_encode_omits_reason_string_when_it_exceeds_max_packet_size() {
        let disconnect = Disconnect {
            reason_code: DisconnectReasonCode::NotAuthorized,
            session_expiry_interval: None,
            server_reference: None,
            reason_string: Some(ByteString::from_static("too long for budget")),
            user_properties: UserProperties::default(),
        };
        let mut buf = BytesMut::new();
        disconnect.encode_body(&mut buf, 2).expect("encode");
        assert!(
            !buf.as_ref().contains(&pt::REASON_STRING),
            "Reason String property must be omitted when it would exceed the DISCONNECT body budget"
        );
    }

    #[test]
    fn disconnect_encode_omits_user_property_when_it_exceeds_max_packet_size() {
        let disconnect = Disconnect {
            reason_code: DisconnectReasonCode::NotAuthorized,
            session_expiry_interval: None,
            server_reference: None,
            reason_string: None,
            user_properties: vec![(ByteString::from_static("k"), ByteString::from_static("v"))],
        };
        let mut buf = BytesMut::new();
        disconnect.encode_body(&mut buf, 2).expect("encode");
        assert!(
            !buf.as_ref().contains(&pt::USER),
            "User Property must be omitted when it would exceed the DISCONNECT body budget"
        );
    }
}
