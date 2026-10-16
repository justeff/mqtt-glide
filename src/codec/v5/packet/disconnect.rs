//! MQTT DISCONNECT (§3.14).
//!
//! Variable header and properties per OASIS MQTT Version 5.0.
//!
//! # Errors
//! Decode rejects unknown Reason Codes and trailing bytes after the properties (§2.1.4).

use std::convert::TryFrom;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use bytestring::ByteString;

use crate::codec::error::{DecodeError, EncodeError, UnknownCode};
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
    /// Close the connection normally; the Will Message is not published.
    NormalDisconnection = 0,
    /// Close the connection, but publish the Will Message.
    DisconnectWithWillMessage = 4,
    /// The sender does not say why.
    UnspecifiedError = 128,
    /// The received packet could not be parsed.
    MalformedPacket = 129,
    /// The received packet was valid but violated the protocol.
    ProtocolError = 130,
    /// Valid packet, refused by the implementation.
    ImplementationSpecificError = 131,
    /// The request is not authorized.
    NotAuthorized = 135,
    /// The broker is busy; try again later.
    ServerBusy = 137,
    /// The broker is shutting down.
    ServerShuttingDown = 139,
    /// The Authentication Method is not supported or does not match the one in use.
    BadAuthenticationMethod = 140,
    /// No packet arrived within 1.5 times the Keep Alive.
    KeepAliveTimeout = 141,
    /// Another connection with the same Client Identifier took over the session.
    SessionTakenOver = 142,
    /// A Topic Filter is well formed but not accepted.
    TopicFilterInvalid = 143,
    /// A Topic Name is well formed but not accepted.
    TopicNameInvalid = 144,
    /// The peer sent more unacknowledged QoS 1 publishes than Receive Maximum allows.
    ReceiveMaximumExceeded = 147,
    /// A Topic Alias is not valid (zero, or above the Topic Alias Maximum).
    TopicAliasInvalid = 148,
    /// The packet exceeds the maximum packet size.
    PacketTooLarge = 149,
    /// The message rate is too high.
    MessageRateTooHigh = 150,
    /// An implementation or administrative limit was exceeded.
    QuotaExceeded = 151,
    /// The connection is closed by administrative action.
    AdministrativeAction = 152,
    /// The payload does not match its Payload Format Indicator.
    PayloadFormatInvalid = 153,
    /// The broker does not support retained messages.
    RetainNotSupported = 154,
    /// The broker does not support the requested QoS.
    QosNotSupported = 155,
    /// Connect to another broker (see the Server Reference).
    UseAnotherServer = 156,
    /// The broker has moved for good (see the Server Reference).
    ServerMoved = 157,
    /// The broker does not support shared subscriptions.
    SharedSubscriptionNotSupported = 158,
    /// The connection rate limit was exceeded.
    ConnectionRateExceeded = 159,
    /// The maximum connection time authorized for this connection was exceeded.
    MaximumConnectTime = 160,
    /// The broker does not support Subscription Identifiers.
    SubscriptionIdentifiersNotSupported = 161,
    /// The broker does not support wildcard subscriptions.
    WildcardSubscriptionsNotSupported = 162,
}

impl TryFrom<u8> for DisconnectReasonCode {
    type Error = UnknownCode;

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
            _ => Err(UnknownCode(v)),
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

    #[test]
    fn disconnect_decode_rejects_unknown_reason_code() {
        let mut src = Bytes::from_static(&[0x7F]);
        assert!(matches!(
            Disconnect::decode(&mut src),
            Err(DecodeError::MalformedPacket(_))
        ));
    }

    /// [MQTT-3.14.2-4] User Property is dropped rather than exceed the receiver's Maximum Packet Size.
    #[test]
    fn disconnect_encode_omits_user_property_when_it_exceeds_max_packet_size() {
        let disconnect = Disconnect {
            reason_code: DisconnectReasonCode::NotAuthorized,
            session_expiry_interval: None,
            server_reference: None,
            reason_string: None,
            user_properties: vec![("k", "v").into()],
        };
        let mut buf = BytesMut::new();
        disconnect.encode_body(&mut buf, 2).expect("encode");
        assert_eq!(
            buf.as_ref(),
            [u8::from(DisconnectReasonCode::NotAuthorized), 0x00],
            "User Property must be omitted when it would exceed the DISCONNECT body budget"
        );
    }

    #[test]
    fn disconnect_with_only_a_reason_code_keeps_it() {
        // §3.14.2.2.1: the Property Length may be left out, so a broker can send `E0 01 8B`.
        for code in [
            DisconnectReasonCode::ServerShuttingDown,
            DisconnectReasonCode::UseAnotherServer,
        ] {
            let disconnect = Disconnect::decode(&mut Bytes::copy_from_slice(&[code.into()])).unwrap();
            assert_eq!(disconnect, Disconnect::new(code));
        }
    }

    #[test]
    fn disconnect_decodes_every_property_from_spec_wire_bytes() {
        // Identifiers spelled out from §3.14.2.2, not taken from `property_type`.
        #[rustfmt::skip]
        let props: &[u8] = &[
            0x11, 0x00, 0x00, 0x00, 0x1E,                  // Session Expiry Interval 30
            0x1F, 0x00, 0x02, b'b', b'y',                  // Reason String
            0x26, 0x00, 0x01, b'k', 0x00, 0x01, b'v',      // User Property
            0x1C, 0x00, 0x03, b'h', b':', b'1',            // Server Reference
        ];
        let mut body = vec![0x9C, props.len() as u8];
        body.extend_from_slice(props);

        let disconnect = Disconnect::decode(&mut Bytes::from(body)).unwrap();
        assert_eq!(
            disconnect,
            Disconnect {
                reason_code: DisconnectReasonCode::UseAnotherServer,
                session_expiry_interval: Some(30),
                server_reference: Some(ByteString::from_static("h:1")),
                reason_string: Some(ByteString::from_static("by")),
                user_properties: vec![("k", "v").into()],
            }
        );
    }
}
