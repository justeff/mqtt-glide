//! MQTT wire data types (OASIS MQTT §1.5).
//!
//! Owns primitive encode/decode traits, UTF-8 string and binary field helpers, and the
//! v5 property-length block extractor (`take_properties`). Does **not** own fixed-header
//! or Remaining Length framing — those live in `framing`.

use std::num::{NonZeroU16, NonZeroU32};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use bytestring::ByteString;

use crate::codec::error::{DecodeError, EncodeError};

use super::framing::decode_variable_length_cursor;
use super::reader::WireReader;

/// Decode a value from MQTT wire bytes (§1.5).
pub(crate) trait Decode: Sized {
    /// Decodes from the buffer, advancing the cursor.
    ///
    /// # Errors
    /// Returns [`DecodeError`] on invalid data or insufficient bytes.
    fn decode(src: &mut Bytes) -> Result<Self, DecodeError>;
}

/// Read a property value once; duplicate identifiers are a protocol error (§2.2.2.2).
pub(in crate::codec) trait Property {
    /// Reads the property value, rejecting a second assignment of the same field.
    fn read_value(&mut self, src: &mut Bytes) -> Result<(), DecodeError>;
}

impl<T: Decode> Property for Option<T> {
    fn read_value(&mut self, src: &mut Bytes) -> Result<(), DecodeError> {
        ensure!(
            self.is_none(),
            DecodeError::ProtocolViolation("property included more than once")
        );
        *self = Some(T::decode(src)?);
        Ok(())
    }
}

// Primitive type decoding implementations
impl Decode for bool {
    fn decode(src: &mut Bytes) -> Result<Self, DecodeError> {
        let v = WireReader::new(src).read_u8()?;
        ensure!(v <= 0x1, DecodeError::MalformedPacket("invalid encoding"));
        Ok(v == 0x1)
    }
}

impl Decode for u16 {
    fn decode(src: &mut Bytes) -> Result<Self, DecodeError> {
        WireReader::new(src).read_u16()
    }
}

impl Decode for u32 {
    fn decode(src: &mut Bytes) -> Result<Self, DecodeError> {
        WireReader::new(src).read_u32()
    }
}

impl Decode for NonZeroU32 {
    fn decode(src: &mut Bytes) -> Result<Self, DecodeError> {
        NonZeroU32::new(u32::decode(src)?).ok_or(DecodeError::MalformedPacket("invalid encoding"))
    }
}

impl Decode for NonZeroU16 {
    fn decode(src: &mut Bytes) -> Result<Self, DecodeError> {
        NonZeroU16::new(u16::decode(src)?).ok_or(DecodeError::MalformedPacket("invalid encoding"))
    }
}

impl Decode for Bytes {
    fn decode(src: &mut Bytes) -> Result<Self, DecodeError> {
        let len = u16::decode(src)? as usize;
        ensure!(src.remaining() >= len, DecodeError::InvalidLength);
        Ok(src.split_to(len))
    }
}

/// Topic-scan crossover length: below this, the simple (auto-vectorized) loop beats
/// the `memchr` crate's runtime-dispatched SIMD kernels, whose call + setup overhead
/// dominates short slices (typical topic names). Measured on x86_64 AVX2; see
/// `benches/codec_throughput.rs`.
const SIMD_SCAN_MIN_LEN: usize = 32;

/// True when the bytes contain a `0x00` octet (U+0000 in UTF-8). [MQTT-1.5.4-2]
///
/// Plain `contains` on purpose: core lowers it to a SWAR memchr that wins for the
/// short strings dominating property/identifier fields, without the code-size cost
/// a `memchr`-crate dual path adds at every call site (measured slower here).
#[inline]
pub(crate) fn contains_nul(bytes: &[u8]) -> bool {
    bytes.contains(&0)
}

/// MQTT UTF-8 strings must be well-formed UTF-8 and MUST NOT encode U+0000. [MQTT-1.5.4-1], [MQTT-1.5.4-2]
///
/// Zero-copy: after validation, wraps the frame [`Bytes`] via [`ByteString::try_from`]
/// (no `copy_from_slice`). Ill-formed UTF-8 / embedded NUL is a Malformed Packet.
#[inline]
pub(crate) fn decode_mqtt_utf8_string(bytes: Bytes) -> Result<ByteString, DecodeError> {
    // U+0000 is a single 0x00 byte in UTF-8 — reject before wrapping.
    if contains_nul(bytes.as_ref()) {
        return Err(DecodeError::MalformedPacket("MQTT UTF-8 string contains U+0000"));
    }
    ByteString::try_from(bytes).map_err(|_| DecodeError::MalformedPacket("invalid UTF-8 string"))
}

impl Decode for ByteString {
    fn decode(src: &mut Bytes) -> Result<Self, DecodeError> {
        decode_mqtt_utf8_string(Bytes::decode(src)?)
    }
}

/// True when Topic Name bytes contain a forbidden octet: `NUL`, `+`, or `#`.
///
/// Long topics scan via SIMD `memchr3` (SSE2/AVX2 or NEON at runtime); short ones
/// use a plain loop (see [`SIMD_SCAN_MIN_LEN`]). The long path stays outlined so
/// hot callers only inline the short loop.
#[inline]
pub(crate) fn topic_name_bytes_forbidden(bytes: &[u8]) -> bool {
    if bytes.len() < SIMD_SCAN_MIN_LEN {
        bytes.iter().any(|&b| matches!(b, 0 | b'+' | b'#'))
    } else {
        topic_name_bytes_forbidden_simd(bytes)
    }
}

#[inline(never)]
fn topic_name_bytes_forbidden_simd(bytes: &[u8]) -> bool {
    memchr::memchr3(0, b'+', b'#', bytes).is_some()
}

/// True when Topic Filter bytes violate `NUL` ([MQTT-4.7.3-2]) or `+`/`#` placement ([MQTT-4.7.1-1/2]).
///
/// Empty filters are rejected by callers ([MQTT-4.7.3-1]).
#[inline]
pub(crate) fn topic_filter_bytes_invalid(bytes: &[u8]) -> bool {
    let mut level_start = true;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            0 => return true,
            b'#' => {
                // Must occupy its own level and be the last character. [MQTT-4.7.1-1]
                return !level_start || i + 1 != bytes.len();
            }
            b'+' => {
                // Must occupy an entire level: preceded by start/`/`, followed by `/` or end. [MQTT-4.7.1-2]
                if !level_start || (i + 1 < bytes.len() && bytes[i + 1] != b'/') {
                    return true;
                }
                level_start = false;
            }
            b'/' => level_start = true,
            _ => level_start = false,
        }
        i += 1;
    }
    false
}

/// Topic Names and Topic Filters must be at least one UTF-8 character. [MQTT-4.7.3-1]
#[inline]
pub(crate) fn decode_non_empty_topic(src: &mut Bytes) -> Result<ByteString, DecodeError> {
    let topic = ByteString::decode(src)?;
    ensure_non_empty_topic(&topic)?;
    Ok(topic)
}

/// Decode a non-empty MQTT Topic Name (wildcards forbidden). [MQTT-3.3.2-2], [MQTT-4.7.0-1]
#[inline]
pub(crate) fn decode_topic_name(src: &mut Bytes) -> Result<ByteString, DecodeError> {
    let raw = Bytes::decode(src)?;
    ensure!(!raw.is_empty(), DecodeError::MalformedPacket("invalid encoding"));
    decode_mqtt_topic_name(raw)
}

/// Like [`decode_topic_name`], but empty topic bytes are allowed (v5 Topic Alias path).
#[inline]
pub(crate) fn decode_topic_name_allow_empty(src: &mut Bytes) -> Result<ByteString, DecodeError> {
    let raw = Bytes::decode(src)?;
    if raw.is_empty() {
        return Ok(ByteString::default());
    }
    decode_mqtt_topic_name(raw)
}

/// MQTT Topic Name: reject `NUL` / `+` / `#`, then UTF-8. [MQTT-3.3.2-2]
#[inline]
fn decode_mqtt_topic_name(bytes: Bytes) -> Result<ByteString, DecodeError> {
    if topic_name_bytes_forbidden(bytes.as_ref()) {
        return Err(DecodeError::MalformedPacket("topic name contains NUL, '+' or '#'"));
    }
    ByteString::try_from(bytes).map_err(|_| DecodeError::MalformedPacket("invalid UTF-8 topic name"))
}

#[inline]
pub(crate) fn ensure_non_empty_topic(topic: &ByteString) -> Result<(), DecodeError> {
    ensure!(!topic.is_empty(), DecodeError::MalformedPacket("invalid encoding"));
    Ok(())
}

/// Slice the Property Length prefix and return the property block (§2.2.2.1).
#[inline]
pub(crate) fn take_properties(src: &mut Bytes) -> Result<Bytes, DecodeError> {
    let prop_len = decode_variable_length_cursor(src)?;
    ensure!(src.remaining() >= prop_len as usize, DecodeError::InvalidLength);
    Ok(src.split_to(prop_len as usize))
}

/// Encode a value to MQTT wire bytes (§1.5).
pub(crate) trait Encode {
    /// Serialized size in bytes.
    fn encoded_size(&self) -> usize;

    /// Appends the serialized form to `buf`.
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodeError>;
}

// Primitive type encoding implementations
impl Encode for bool {
    fn encoded_size(&self) -> usize {
        1
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodeError> {
        buf.put_u8(u8::from(*self));
        Ok(())
    }
}

impl Encode for u16 {
    fn encoded_size(&self) -> usize {
        2
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodeError> {
        buf.put_u16(*self);
        Ok(())
    }
}

impl Encode for NonZeroU16 {
    fn encoded_size(&self) -> usize {
        2
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodeError> {
        self.get().encode(buf)
    }
}

impl Encode for u32 {
    fn encoded_size(&self) -> usize {
        4
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodeError> {
        buf.put_u32(*self);
        Ok(())
    }
}

impl Encode for NonZeroU32 {
    fn encoded_size(&self) -> usize {
        4
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodeError> {
        self.get().encode(buf)
    }
}

impl Encode for Bytes {
    fn encoded_size(&self) -> usize {
        2 + self.len() // Length prefix + data
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodeError> {
        let len = u16::try_from(self.len()).map_err(|_| EncodeError::InvalidLength)?;
        buf.put_u16(len);
        buf.extend_from_slice(self);
        Ok(())
    }
}

impl Encode for ByteString {
    fn encoded_size(&self) -> usize {
        self.as_bytes().encoded_size()
    }

    /// No U+0000 check here on purpose ([MQTT-1.5.4-2] is enforced at plan time per
    /// packet — `publish_wire_plan`, `Connect::encode_body`): the per-message encode
    /// path stays single-pass over each string.
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodeError> {
        self.as_bytes().encode(buf)
    }
}

impl Encode for crate::codec::v5::UserProperty {
    fn encoded_size(&self) -> usize {
        self.name.encoded_size() + self.value.encoded_size()
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodeError> {
        self.name.encode(buf)?;
        self.value.encode(buf)
    }
}

impl Encode for &[u8] {
    fn encoded_size(&self) -> usize {
        2 + self.len()
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodeError> {
        let len = u16::try_from(self.len()).map_err(|_| EncodeError::InvalidLength)?;
        buf.put_u16(len);
        buf.extend_from_slice(self);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[test]
    fn decode_non_empty_topic_rejects_zero_length() {
        let mut src = Bytes::from_static(&[0x00, 0x00]);
        assert!(matches!(
            decode_non_empty_topic(&mut src),
            Err(DecodeError::MalformedPacket(_))
        ));
    }

    #[test]
    fn decode_non_empty_topic_accepts_non_empty() {
        let mut src = Bytes::from_static(&[0x00, 0x03, b'a', b'b', b'c']);
        assert_eq!(
            decode_non_empty_topic(&mut src).unwrap(),
            ByteString::from_static("abc")
        );
    }

    #[rstest]
    #[case::plus([0x00, 0x03, b'a', b'+', b'c'])]
    #[case::hash([0x00, 0x03, b'a', b'#', b'c'])]
    #[case::nul([0x00, 0x03, b'a', 0x00, b'c'])]
    fn decode_topic_name_rejects_forbidden(#[case] wire: [u8; 5]) {
        let mut src = Bytes::copy_from_slice(&wire);
        assert!(matches!(
            decode_topic_name(&mut src),
            Err(DecodeError::MalformedPacket(_))
        ));
    }

    /// [MQTT-4.7.1-1] `#` alone or after `/`, and last character only.
    #[rstest]
    #[case::hash_alone("#")]
    #[case::hash_after_level("sport/tennis/#")]
    #[case::hash_parent("sport/#")]
    fn topic_filter_accepts_valid_multi_level_wildcard(#[case] filter: &str) {
        assert!(!topic_filter_bytes_invalid(filter.as_bytes()), "{filter}");
    }

    /// [MQTT-4.7.1-1] invalid `#` placement (spec non-normative examples).
    #[rstest]
    #[case::hash_glued("sport/tennis#")]
    #[case::hash_not_last("sport/tennis/#/ranking")]
    #[case::hash_mid_level("a#b")]
    fn topic_filter_rejects_invalid_multi_level_wildcard(#[case] filter: &str) {
        assert!(topic_filter_bytes_invalid(filter.as_bytes()), "{filter}");
    }

    /// [MQTT-4.7.1-2] `+` occupying an entire level.
    #[rstest]
    #[case::plus_alone("+")]
    #[case::plus_first("+/tennis/#")]
    #[case::plus_middle("sport/+/player1")]
    #[case::plus_last("sport/tennis/+")]
    #[case::shared_with_wildcards("$share/g/a/+/b/#")]
    fn topic_filter_accepts_valid_single_level_wildcard(#[case] filter: &str) {
        assert!(!topic_filter_bytes_invalid(filter.as_bytes()), "{filter}");
    }

    /// [MQTT-4.7.1-2] invalid `+` placement (spec non-normative examples).
    #[rstest]
    #[case::plus_glued("sport+")]
    #[case::plus_prefix("sport/+player")]
    #[case::plus_mid_level("a+b")]
    fn topic_filter_rejects_invalid_single_level_wildcard(#[case] filter: &str) {
        assert!(topic_filter_bytes_invalid(filter.as_bytes()), "{filter}");
    }

    #[test]
    fn topic_filter_rejects_embedded_nul() {
        assert!(topic_filter_bytes_invalid(b"a\0b"));
    }

    #[test]
    fn decode_topic_name_accepts_literal_topic() {
        let mut src = Bytes::from_static(&[
            0x00, 0x0E, b't', b'e', b'l', b'e', b'm', b'e', b't', b'r', b'y', b'/', b'c', b'o', b'n', b'f',
        ]);
        assert_eq!(
            decode_topic_name(&mut src).unwrap(),
            ByteString::from_static("telemetry/conf")
        );
    }

    #[test]
    fn decode_mqtt_utf8_string_rejects_embedded_nul() {
        let bytes = Bytes::from_static(b"a\0b");
        assert!(matches!(
            decode_mqtt_utf8_string(bytes),
            Err(DecodeError::MalformedPacket(_))
        ));
    }

    /// Frame `Bytes` must be wrapped, not recopied into a new buffer.
    #[test]
    fn decode_mqtt_utf8_string_is_zero_copy() {
        let bytes = Bytes::from(vec![b't', b'/', b'a']);
        let ptr = bytes.as_ptr();
        let decoded = decode_mqtt_utf8_string(bytes).expect("valid utf-8");
        assert_eq!(decoded.as_bytes().as_ptr(), ptr);
        assert_eq!(&*decoded, "t/a");
    }

    /// [MQTT-1.5.4-3] U+FEFF (UTF-8 `EF BB BF`) must not be stripped on decode.
    #[test]
    fn decode_mqtt_utf8_string_preserves_bom() {
        let bytes = Bytes::from_static(&[0xEF, 0xBB, 0xBF, b'a']);
        let decoded = decode_mqtt_utf8_string(bytes).expect("BOM-prefixed UTF-8 must decode");
        assert_eq!(decoded.as_bytes().as_ref(), &[0xEF, 0xBB, 0xBF, b'a']);
        assert_eq!(decoded.chars().next(), Some('\u{FEFF}'));
    }

    #[test]
    fn take_properties_rejects_truncated_property_block() {
        let mut src = Bytes::from_static(&[0x03, 0x01]);
        assert!(matches!(take_properties(&mut src), Err(DecodeError::InvalidLength)));
    }

    #[test]
    fn decode_bytestring_rejects_embedded_nul() {
        let mut src = Bytes::from_static(&[0x00, 0x03, b'a', 0x00, b'b']);
        assert!(matches!(
            ByteString::decode(&mut src),
            Err(DecodeError::MalformedPacket(_))
        ));
    }

    /// [MQTT-1.5.4-1]: ill-formed UTF-8, including an encoded UTF-16 surrogate (U+D800 as
    /// `ED A0 80`), is rejected when a string is decoded.
    #[test]
    fn decode_bytestring_rejects_ill_formed_utf8_and_surrogates() {
        for bytes in [
            &[0x00, 0x03, 0xED, 0xA0, 0x80][..],
            &[0x00, 0x02, 0xC3, 0x28],
            &[0x00, 0x01, 0xFF],
        ] {
            let mut src = Bytes::copy_from_slice(bytes);
            assert!(
                matches!(ByteString::decode(&mut src), Err(DecodeError::MalformedPacket(_))),
                "{bytes:02X?}"
            );
        }
    }

    #[test]
    fn decode_bytestring_accepts_valid_without_nul() {
        let mut src = Bytes::from_static(&[0x00, 0x03, b'a', b'b', b'c']);
        assert_eq!(ByteString::decode(&mut src).unwrap(), ByteString::from_static("abc"));
    }

    /// MQTT-4.7.3-3: UTF-8 strings may encode to at most 65535 bytes (u16 length prefix).
    #[test]
    fn decode_topic_accepts_max_length_65535_bytes() {
        let mut topic = vec![b'a'; 65535];
        let mut wire = Vec::with_capacity(2 + 65535);
        wire.extend_from_slice(&[0xFF, 0xFF]);
        wire.append(&mut topic);
        let mut src = Bytes::from(wire);
        let decoded = decode_non_empty_topic(&mut src).expect("65535-byte topic must decode");
        assert_eq!(decoded.len(), 65535);
    }

    /// [MQTT-4.7.3-3]: a string longer than 65535 bytes is never encoded.
    #[test]
    fn encode_topic_rejects_length_65536_bytes() {
        let oversized = vec![b'a'; 65536];
        let mut buf = BytesMut::new();
        assert!(matches!(
            oversized.as_slice().encode(&mut buf),
            Err(EncodeError::InvalidLength)
        ));
    }
}
