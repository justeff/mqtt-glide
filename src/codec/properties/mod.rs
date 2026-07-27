//! MQTT v5 property-block helpers (§2.2.2).
//!
//! Shared length budgeting and wire write logic for v5 property blocks.
//! Per-packet validation (duplicate rejection, lean-disabled props) stays in
//! each packet module — this module only handles common property wire ops.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use bytestring::ByteString;

use crate::codec::error::{DecodeError, EncodeError};
use crate::codec::utils::{Decode, Encode, Property, take_properties, write_variable_length};
use crate::codec::v5::{UserProperties, UserProperty, property_type as pt};

/// Calculates encoded size of optional user properties and reason string, respecting size limit.
pub(crate) fn encoded_size_opt_props(
    user_props: &[UserProperty],
    reason_str: &Option<ByteString>,
    mut limit: u32,
) -> usize {
    let mut len = 0;
    for up in user_props.iter() {
        let prop_len = 1 + up.encoded_size(); // prop type byte + key.len() + val.len()
        if prop_len > limit as usize {
            return len;
        }
        limit -= prop_len as u32;
        len += prop_len;
    }

    if let Some(reason) = reason_str {
        let reason_len = 1 + reason.encoded_size();
        if reason_len <= limit as usize {
            len += reason_len;
        }
    }

    len
}

/// Encodes optional user properties and reason string, respecting size limit.
pub(crate) fn encode_opt_props(
    user_props: &[UserProperty],
    reason_str: &Option<ByteString>,
    buf: &mut BytesMut,
    mut size: u32,
) -> Result<(), EncodeError> {
    for up in user_props.iter() {
        let prop_len = 1 + up.0.encoded_size() + up.1.encoded_size(); // prop_type.len() + key.len() + val.len()
        if prop_len > size as usize {
            return Ok(());
        }
        buf.put_u8(pt::USER);
        up.encode(buf)?;
        size -= prop_len as u32; // safe: checked it's less already
    }

    if let Some(reason) = reason_str
        && reason.len() < size as usize
    {
        buf.put_u8(pt::REASON_STRING);
        reason.encode(buf)?;
    }

    Ok(())
}

pub(crate) fn encoded_property_size<T: Encode>(v: &Option<T>) -> usize {
    v.as_ref().map_or(0, |v| 1 + v.encoded_size()) // 1 - property type byte
}

pub(crate) fn encoded_property_size_default<T: Encode + PartialEq>(v: &T, default: T) -> usize {
    if *v == default {
        0
    } else {
        1 + v.encoded_size() // 1 - property type byte
    }
}

pub(crate) fn encode_property<T: Encode>(v: &Option<T>, prop_type: u8, buf: &mut BytesMut) -> Result<(), EncodeError> {
    if let Some(v) = v {
        buf.put_u8(prop_type);
        v.encode(buf)
    } else {
        Ok(())
    }
}

pub(crate) fn encode_property_default<T: Encode + PartialEq>(
    v: &T,
    default: T,
    prop_type: u8,
    buf: &mut BytesMut,
) -> Result<(), EncodeError> {
    if *v != default {
        buf.put_u8(prop_type);
        v.encode(buf)
    } else {
        Ok(())
    }
}

/// Calculates length of variable length integer based on its value.
pub(crate) fn var_int_len(val: usize) -> u32 {
    #[cfg(target_pointer_width = "16")]
    panic!("16-bit platforms are not supported");
    #[cfg(target_pointer_width = "32")]
    const MAP: [u32; 33] = [
        5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1,
    ];
    #[cfg(target_pointer_width = "64")]
    const MAP: [u32; 65] = [
        10, 9, 9, 9, 9, 9, 9, 9, 8, 8, 8, 8, 8, 8, 8, 7, 7, 7, 7, 7, 7, 7, 6, 6, 6, 6, 6, 6, 6, 5, 5, 5, 5, 5, 5, 5, 4,
        4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1,
    ];
    let zeros = val.leading_zeros() as usize;
    MAP[zeros]
}

/// Calculates length of variable length integer based on its value.
pub(crate) fn var_int_len_u32(val: u32) -> u32 {
    const MAP: [u32; 33] = [
        5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1,
    ];
    let zeros = val.leading_zeros() as usize;
    MAP[zeros] // The range of `zeros` is 0..=32, so it will not go out of bounds.
}

/// Calculates `len` from `var_int_len(len) + len` value.
pub(crate) fn var_int_len_from_size(val: u32) -> u32 {
    let over_size = var_int_len_u32(val);
    let res = val - over_size + 1;
    val - var_int_len_u32(res)
}

pub(crate) fn reduce_limit(limit: u32, reduction: usize) -> u32 {
    if reduction > limit as usize {
        return 0;
    }
    limit - (reduction as u32) // safe: by now we're sure `reduction` fits in u32
}

impl Encode for UserProperties {
    fn encoded_size(&self) -> usize {
        let mut len = 0;
        for prop in self {
            len += 1 + prop.encoded_size();
        }
        len
    }
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodeError> {
        for prop in self {
            buf.put_u8(pt::USER);
            prop.encode(buf)?;
        }
        Ok(())
    }
}

/// ACK/SUBACK/UNSUBACK property blocks: user properties + reason string only.
pub mod ack_props {
    use super::*;

    pub(crate) fn encoded_size(properties: &[UserProperty], reason_string: &Option<ByteString>, limit: u32) -> usize {
        if limit < 4 {
            // Property block needs at least 4 bytes (varint length + one property byte).
            return 1; // 1 byte to encode property length = 0
        }

        let len = encoded_size_opt_props(properties, reason_string, limit - 4);
        var_int_len(len) as usize + len
    }

    pub(crate) fn encode(
        properties: &[UserProperty],
        reason_string: &Option<ByteString>,
        buf: &mut BytesMut,
        size: u32,
    ) -> Result<(), EncodeError> {
        debug_assert!(size > 0); // formalize in signature?

        if size == 1 {
            // empty properties
            buf.put_u8(0);
            return Ok(());
        }

        let size = var_int_len_from_size(size);
        write_variable_length(size, buf)?;
        encode_opt_props(properties, reason_string, buf, size)
    }

    /// Parses ACK properties (User and Reason String properties) from `src`.
    pub(crate) fn decode(src: &mut Bytes) -> Result<(UserProperties, Option<ByteString>), DecodeError> {
        let prop_src = &mut take_properties(src)?;
        let mut reason_string = None;
        let mut user_props = Vec::new();
        while prop_src.has_remaining() {
            let prop_id = prop_src.get_u8();
            match prop_id {
                pt::REASON_STRING => reason_string.read_value(prop_src)?,
                pt::USER => user_props.push(<(ByteString, ByteString)>::decode(prop_src)?),
                _ => return Err(DecodeError::MalformedPacket("unknown property or reason code")),
            }
        }

        Ok((user_props, reason_string))
    }
}
