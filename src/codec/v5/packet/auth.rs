//! MQTT **AUTH** control packet — variable header and properties.
//!
//! - OASIS MQTT v5.0: §3.15 AUTH — Authentication exchange

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

/// AUTH message
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Auth {
    pub reason_code: AuthReasonCode,
    pub auth_method: Option<ByteString>,
    pub auth_data: Option<Bytes>,
    pub reason_string: Option<ByteString>,
    pub user_properties: UserProperties,
}

/// AUTH reason codes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AuthReasonCode {
    Success = 0,
    ContinueAuth = 24,
    ReAuth = 25,
}

impl TryFrom<u8> for AuthReasonCode {
    type Error = DecodeError;

    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(Self::Success),
            24 => Ok(Self::ContinueAuth),
            25 => Ok(Self::ReAuth),
            _ => Err(DecodeError::MalformedPacket("unknown property or reason code")),
        }
    }
}

impl From<AuthReasonCode> for u8 {
    fn from(v: AuthReasonCode) -> Self {
        v as u8
    }
}

impl Auth {
    #[inline]
    pub(crate) fn is_minimal_success(&self) -> bool {
        self.reason_code == AuthReasonCode::Success
            && self.auth_method.is_none()
            && self.auth_data.is_none()
            && self.reason_string.is_none()
            && self.user_properties.is_empty()
    }

    pub(crate) fn decode(src: &mut Bytes) -> Result<Self, DecodeError> {
        let auth = if src.has_remaining() {
            let reason_code = src.get_u8().try_into()?; // [MQTT-3.15.2-1]

            if src.has_remaining() {
                let mut auth_method = None;
                let mut auth_data = None;
                let mut reason_string = None;
                let mut user_properties = Vec::new();

                if reason_code != AuthReasonCode::Success || src.has_remaining() {
                    let prop_src = &mut utils::take_properties(src)?;
                    while prop_src.has_remaining() {
                        match prop_src.get_u8() {
                            pt::AUTH_METHOD => auth_method.read_value(prop_src)?,
                            pt::AUTH_DATA => auth_data.read_value(prop_src)?,
                            pt::REASON_STRING => reason_string.read_value(prop_src)?,
                            pt::USER => user_properties.push(UserProperty::decode(prop_src)?),
                            _ => return Err(DecodeError::MalformedPacket("unknown property or reason code")),
                        }
                    }
                    ensure!(!src.has_remaining(), DecodeError::InvalidLength);
                }

                if matches!(reason_code, AuthReasonCode::ContinueAuth | AuthReasonCode::ReAuth) && auth_method.is_none()
                {
                    // [MQTT-3.15.2.2.2] Continue Authentication / Re-authenticate must include Authentication Method.
                    return Err(DecodeError::MalformedPacket("invalid encoding"));
                }

                Self {
                    reason_code,
                    auth_method,
                    auth_data,
                    reason_string,
                    user_properties,
                }
            } else {
                // Only Success (0x00) with no Properties may omit Reason Code body.
                if reason_code != AuthReasonCode::Success {
                    return Err(DecodeError::MalformedPacket("invalid encoding"));
                }
                Self {
                    reason_code,
                    ..Default::default()
                }
            }
        } else {
            Self::default()
        };
        Ok(auth)
    }
}

impl Default for Auth {
    fn default() -> Self {
        Self {
            reason_code: AuthReasonCode::Success,
            auth_method: None,
            auth_data: None,
            reason_string: None,
            user_properties: Vec::new(),
        }
    }
}

impl WireBodyEncode for Auth {
    fn encoded_body_len(&self, max_out: u32) -> Result<usize, EncodeError> {
        if self.is_minimal_success() {
            return Ok(0);
        }
        const HEADER_LEN: usize = 1; // reason code

        let mut prop_len = encoded_property_size(&self.auth_method) + encoded_property_size(&self.auth_data);
        let diag_len = encoded_size_opt_props(
            &self.user_properties,
            &self.reason_string,
            reduce_limit(max_out, prop_len + HEADER_LEN + 4),
        ); // exclude other props and max of 4 bytes for property length value
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
    use rstest::rstest;

    use super::*;

    #[test]
    fn auth_decode_success_reason_only_ok() {
        let mut src = Bytes::from_static(&[0x00]);
        let auth = Auth::decode(&mut src).unwrap();
        assert_eq!(auth.reason_code, AuthReasonCode::Success);
        assert_eq!(src.len(), 0);
    }

    #[rstest]
    #[case::continue_auth_reason_only(0x18, DecodeError::MalformedPacket("invalid encoding"))]
    #[case::reauth_reason_only(0x19, DecodeError::MalformedPacket("invalid encoding"))]
    fn auth_decode_non_success_reason_only_rejected(#[case] reason: u8, #[case] error: DecodeError) {
        let mut src = Bytes::from(vec![reason]);
        let result = Auth::decode(&mut src);
        assert_eq!(result.map_err(|e| e.to_string()), Err(error.to_string()));
    }

    #[test]
    fn auth_decode_non_success_requires_auth_method() {
        let mut src = Bytes::from_static(&[0x18, 0x00]);
        let result = Auth::decode(&mut src);
        assert_eq!(
            result.map_err(|e| e.to_string()),
            Err(DecodeError::MalformedPacket("invalid encoding").to_string())
        );
    }
}
