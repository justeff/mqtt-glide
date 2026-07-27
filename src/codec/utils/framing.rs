//! MQTT fixed header and Remaining Length framing (OASIS MQTT §2.1; Remaining Length §2.1.4; VBI §1.5.5).
//!
//! Owns variable-length integer encode/decode and fixed-header validation helpers used
//! when splitting or assembling control packets. Does **not** own packet bodies, property
//! bags, or UTF-8 string primitives — those live in `primitives` and version codecs.

use std::io::Cursor;
use std::num::NonZeroU16;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::reader::WireReader;
use crate::codec::error::{DecodeError, EncodeError};

/// Wire length of a packet-id-only control frame: type + Remaining Length `2` + Packet Identifier.
pub(crate) const MINIMAL_PACKET_ID_ACK_WIRE_LEN: usize = 4;

/// Encode a minimal packet-id ack frame (PUBACK/PUBREC/PUBREL/PUBCOMP Success without reason/props).
///
/// Layout: `header`, Remaining Length `0x02`, 16-bit Packet Identifier.
#[inline]
pub(crate) fn encode_minimal_packet_id_ack(buf: &mut BytesMut, header: u8, packet_id: NonZeroU16) {
    let pid = packet_id.get();
    buf.put_slice(&[header, 0x02, (pid >> 8) as u8, (pid & 0xFF) as u8]);
}

/// Reject packets whose Remaining Length payload is non-empty (PINGREQ/PINGRESP).
#[inline]
pub(crate) fn ensure_empty_body(src: &mut Bytes) -> Result<(), DecodeError> {
    WireReader::new(src).ensure_empty()
}

/// Fixed header must match exact byte (type nibble + reserved flags). [MQTT-3.8.1-1] etc.
#[inline]
pub(crate) fn ensure_exact_fixed_header(actual: u8, expected: u8) -> Result<(), DecodeError> {
    if (actual & 0xF0) == (expected & 0xF0) && actual != expected {
        return Err(DecodeError::MalformedPacket("invalid fixed header flags"));
    }
    Ok(())
}

/// Decodes MQTT variable-length integer from byte slice
///
/// # Returns
/// (decoded value, bytes consumed) or None if incomplete
pub(crate) fn decode_variable_length(src: &[u8]) -> Result<Option<(u32, usize)>, DecodeError> {
    if src.is_empty() {
        return Ok(None);
    }
    let mut cur = Cursor::new(src);
    match decode_variable_length_cursor(&mut cur) {
        Ok(len) => Ok(Some((len, cur.position() as usize))),
        Err(DecodeError::MalformedPacket(_)) => {
            let consumed = cur.position() as usize;
            // Continuation bit on the last consumed byte means the varint is truncated.
            if consumed > 0 && src[consumed - 1] & 0x80 != 0 {
                Ok(None)
            } else {
                Err(DecodeError::MalformedPacket("invalid variable byte integer"))
            }
        }
        Err(e) => Err(e),
    }
}

/// Minimum on-wire byte count for a decoded Variable Byte Integer value. [MQTT-1.5.5-1]
#[inline]
fn minimal_variable_length_bytes(value: u32) -> usize {
    if value <= 127 {
        1
    } else if value <= 16_383 {
        2
    } else if value <= 2_097_151 {
        3
    } else {
        4
    }
}

/// Decodes variable-length integer from buffer
///
/// # MQTT Spec
/// Variable length integers use up to 4 bytes with continuation bits
/// Max value: 268,435,455 (0xFFFFFF7F)
#[inline]
#[allow(clippy::cast_lossless)]
pub(crate) fn decode_variable_length_cursor<B: Buf>(src: &mut B) -> Result<u32, DecodeError> {
    let start = src.remaining();
    let mut shift: u32 = 0;
    let mut len: u32 = 0;
    loop {
        ensure!(
            src.has_remaining(),
            DecodeError::MalformedPacket("truncated variable byte integer")
        );
        let val = src.get_u8();
        len += ((val & 0b0111_1111) as u32) << shift;
        if val & 0b1000_0000 == 0 {
            let consumed = start - src.remaining();
            ensure!(
                consumed == minimal_variable_length_bytes(len),
                DecodeError::MalformedPacket("non-minimal variable byte integer")
            );
            return Ok(len);
        }
        ensure!(shift < 21, DecodeError::InvalidLength);
        shift += 7;
    }
}

/// MQTT maximum encodable remaining length (268,435,455).
pub(crate) const MAX_VARIABLE_LENGTH: u32 = 268_435_455;

/// Writes MQTT variable-length integer to buffer.
#[inline]
pub(crate) fn write_variable_length(len: u32, dst: &mut BytesMut) -> Result<(), EncodeError> {
    match len {
        0..=127 => dst.put_u8(len as u8),
        128..=16_383 => dst.put_slice(&[((len & 0b0111_1111) | 0b1000_0000) as u8, (len >> 7) as u8]),
        16_384..=2_097_151 => {
            dst.put_slice(&[
                ((len & 0b0111_1111) | 0b1000_0000) as u8,
                (((len >> 7) & 0b0111_1111) | 0b1000_0000) as u8,
                (len >> 14) as u8,
            ]);
        }
        2_097_152..=MAX_VARIABLE_LENGTH => {
            dst.put_slice(&[
                ((len & 0b0111_1111) | 0b1000_0000) as u8,
                (((len >> 7) & 0b0111_1111) | 0b1000_0000) as u8,
                (((len >> 14) & 0b0111_1111) | 0b1000_0000) as u8,
                (len >> 21) as u8,
            ]);
        }
        _ => return Err(EncodeError::InvalidLength),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::error::EncodeError;

    #[test]
    fn variable_length_decoding() {
        // Test valid decodes
        assert_eq!(decode_variable_length(&[0x00]).unwrap(), Some((0, 1)));
        assert_eq!(decode_variable_length(&[0x7F]).unwrap(), Some((127, 1)));
        assert_eq!(decode_variable_length(&[0x80, 0x01]).unwrap(), Some((128, 2)));

        // Test incomplete data
        assert_eq!(decode_variable_length(&[0x80]).unwrap(), None);

        // Test invalid length
        let res = decode_variable_length(&[0xFF, 0xFF, 0xFF, 0xFF]);
        assert!(matches!(res, Err(DecodeError::InvalidLength)));
    }

    #[test]
    fn variable_length_encoding() {
        let mut buf = BytesMut::new();

        write_variable_length(127, &mut buf).unwrap();
        assert_eq!(buf.as_ref(), &[0x7F]);

        buf.clear();
        write_variable_length(16_383, &mut buf).unwrap();
        assert_eq!(buf.as_ref(), &[0xFF, 0x7F]);

        buf.clear();
        write_variable_length(268_435_455, &mut buf).unwrap();
        assert_eq!(buf.as_ref(), &[0xFF, 0xFF, 0xFF, 0x7F]);
    }

    #[test]
    fn variable_length_rejects_oversized_values() {
        let mut buf = BytesMut::new();
        assert!(matches!(
            write_variable_length(268_435_456, &mut buf),
            Err(EncodeError::InvalidLength)
        ));
    }

    #[test]
    fn variable_length_rejects_non_minimal_encoding() {
        // 0 must be a single 0x00 byte, not 0x80 0x00. [MQTT-1.5.5-1]
        assert!(matches!(
            decode_variable_length(&[0x80, 0x00]),
            Err(DecodeError::MalformedPacket(_))
        ));
        // 128 must be 0x80 0x01, not 0x80 0x80 0x00.
        assert!(matches!(
            decode_variable_length(&[0x80, 0x80, 0x00]),
            Err(DecodeError::MalformedPacket(_))
        ));
        // Trailing continuation without follow-up stays incomplete.
        assert_eq!(decode_variable_length(&[0x80]).unwrap(), None);
    }

    #[test]
    fn ensure_empty_body_rejects_non_empty() {
        let mut empty = Bytes::new();
        assert!(ensure_empty_body(&mut empty).is_ok());
        let mut non_empty = Bytes::from_static(b"\x00");
        assert!(matches!(
            ensure_empty_body(&mut non_empty),
            Err(DecodeError::MalformedPacket(_))
        ));
    }

    #[test]
    fn ensure_exact_fixed_header_rejects_wrong_reserved_flags() {
        assert!(ensure_exact_fixed_header(0x82, 0x82).is_ok());
        assert!(ensure_exact_fixed_header(0x10, 0x20).is_ok()); // different type nibble
        assert!(matches!(
            ensure_exact_fixed_header(0x83, 0x82),
            Err(DecodeError::MalformedPacket(_))
        ));
    }
}
