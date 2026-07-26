use bytes::{Buf, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use super::{DecodedFrame, Packet, decode::decode_packet};
use crate::codec::error::{DecodeError, EncodeError};
use crate::codec::properties::var_int_len_u32;
use crate::codec::types::MAX_PACKET_SIZE;
use crate::codec::utils::decode_variable_length;

/// Worst-case MQTT fixed-header overhead: 1 type byte + 4-byte Remaining Length varint.
const WIRE_HEADER_BUDGET: u32 = 5;

/// Remaining body budget for property trimming given a total wire-size limit.
fn outbound_body_budget(wire_limit: u32) -> u32 {
    if wire_limit == 0 {
        MAX_PACKET_SIZE
    } else {
        wire_limit.saturating_sub(WIRE_HEADER_BUDGET)
    }
}

/// Total on-wire packet size for a body of `body_len` bytes.
fn mqtt_wire_len(body_len: usize) -> u32 {
    1 + var_int_len_u32(body_len as u32) + body_len as u32
}

/// MQTT v5.0 protocol codec implementing `tokio_util::codec` Decoder/Encoder
///
/// Handles MQTT v5.0 wire format framing including:
/// - Fixed header decoding with configurable max inbound packet size
/// - Variable-length integer parsing
/// - Configurable max outbound packet size enforcement
/// - Frame reassembly across multiple network reads (header bytes stay in `src` until complete)
/// - Negotiation flags tracking (problem info, retain, subscription IDs)
#[derive(Debug, Clone)]
pub struct V5Codec {
    max_in_size: u32,
    max_out_size: u32,
    flags: CodecFlags,
}

bitflags::bitflags! {
    #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct CodecFlags: u8 {
        const NO_PROBLEM_INFO = 0b0000_0001;
        const NO_RETAIN       = 0b0000_0010;
        const NO_SUB_IDS      = 0b0000_1000;
    }
}

impl V5Codec {
    /// Create `V5Codec` instance.
    pub fn new(max_in_size: u32, max_out_size: u32) -> Self {
        V5Codec {
            max_in_size,
            max_out_size,
            flags: CodecFlags::empty(),
        }
    }

    /// Codec with the same max size for inbound and outbound frames.
    ///
    /// `0` means unlimited (see [`Self::new`]). Prefer this over separate in/out limits
    /// when constructing `Framed` halves for tests or the connection driver.
    pub fn with_max_packet_size(max_packet_size: u32) -> Self {
        Self::new(max_packet_size, max_packet_size)
    }

    /// Max inbound frame size (total wire bytes, including fixed header).
    ///
    /// If max size is set to `0`, size is unlimited.
    /// By default max size is set to `0`.
    pub fn max_inbound_size(&self) -> u32 {
        self.max_in_size
    }

    /// Max outbound frame size (total wire bytes, including fixed header).
    ///
    /// If max size is set to `0`, size is unlimited.
    /// By default max size is set to `0`.
    pub fn max_outbound_size(&self) -> u32 {
        self.max_out_size
    }

    /// Set max inbound frame size (total wire bytes, including fixed header).
    ///
    /// If max size is set to `0`, size is unlimited.
    /// By default max size is set to `0`.
    pub fn set_max_inbound_size(&mut self, size: u32) {
        self.max_in_size = size;
    }

    /// Set max outbound frame size (total wire bytes, including fixed header).
    ///
    /// If max size is set to `0`, size is unlimited.
    /// By default max size is set to `0`.
    pub fn set_max_outbound_size(&mut self, size: u32) {
        self.max_out_size = size;
    }

    #[inline]
    #[allow(dead_code)]
    pub(crate) fn retain_available(&self) -> bool {
        !self.flags.contains(CodecFlags::NO_RETAIN)
    }

    #[inline]
    #[allow(dead_code)]
    pub(crate) fn sub_ids_available(&self) -> bool {
        !self.flags.contains(CodecFlags::NO_SUB_IDS)
    }

    #[inline]
    #[allow(dead_code)]
    pub(crate) fn set_retain_available(&mut self, val: bool) {
        self.flags.set(CodecFlags::NO_RETAIN, !val);
    }

    #[inline]
    #[allow(dead_code)]
    pub(crate) fn set_sub_ids_available(&mut self, val: bool) {
        self.flags.set(CodecFlags::NO_SUB_IDS, !val);
    }

    /// Encode one control [`Packet`] by appending to `buf`.
    ///
    /// Does not clear `buf`. Callers that replace a single-frame buffer must `clear()` first.
    /// For PUBLISH use [`crate::codec::encode_publish`].
    pub fn encode_packet(&mut self, packet: Packet, buf: &mut BytesMut) -> Result<(), EncodeError> {
        self.encode(packet, buf)
    }

    /// Decode one frame, discarding the consumed byte count from the codec.
    pub fn decode_frame(&mut self, src: &mut BytesMut) -> Result<Option<DecodedFrame>, DecodeError> {
        self.decode(src).map(|maybe| maybe.map(|(packet, _)| packet))
    }
}

impl Default for V5Codec {
    fn default() -> Self {
        Self::new(0, 0)
    }
}

impl Decoder for V5Codec {
    type Item = (DecodedFrame, u32);
    type Error = DecodeError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, DecodeError> {
        if src.len() < 2 {
            return Ok(None);
        }
        let first_byte = src[0];
        let Some((remaining_length, varint_len)) = decode_variable_length(&src[1..])? else {
            return Ok(None);
        };
        let header_len = 1 + varint_len;
        let wire_size = header_len + remaining_length as usize;
        if self.max_in_size != 0 && wire_size as u32 > self.max_in_size {
            return Err(DecodeError::MaxSizeExceeded {
                size: wire_size as u32,
                max: self.max_in_size,
            });
        }
        if src.len() < wire_size {
            src.reserve(wire_size - src.len());
            return Ok(None);
        }

        let mut frame = src.split_to(wire_size);
        frame.advance(header_len);
        let packet = decode_packet(frame.freeze(), first_byte)?;

        if let DecodedFrame::Control(Packet::Connect(ref pkt)) = packet {
            self.flags.set(CodecFlags::NO_PROBLEM_INFO, !pkt.request_problem_info);
        }
        Ok(Some((packet, remaining_length)))
    }
}

impl Encoder<Packet> for V5Codec {
    type Error = EncodeError;

    fn encode(&mut self, mut item: Packet, dst: &mut BytesMut) -> Result<(), EncodeError> {
        // handle [MQTT 3.1.2.11.7]
        if self.flags.contains(CodecFlags::NO_PROBLEM_INFO) {
            match item {
                Packet::PublishAck(ref mut pkt) | Packet::PublishReceived(ref mut pkt) => {
                    pkt.properties.clear();
                    let _ = pkt.reason_string.take();
                }
                Packet::PublishRelease(ref mut pkt) | Packet::PublishComplete(ref mut pkt) => {
                    pkt.properties.clear();
                    let _ = pkt.reason_string.take();
                }
                Packet::Subscribe(ref mut pkt) => {
                    pkt.user_properties.clear();
                }
                Packet::SubscribeAck(ref mut pkt) => {
                    pkt.properties.clear();
                    let _ = pkt.reason_string.take();
                }
                Packet::Unsubscribe(ref mut pkt) => {
                    pkt.user_properties.clear();
                }
                Packet::UnsubscribeAck(ref mut pkt) => {
                    pkt.properties.clear();
                    let _ = pkt.reason_string.take();
                }
                Packet::Auth(ref mut pkt) => {
                    pkt.user_properties.clear();
                    let _ = pkt.reason_string.take();
                }
                _ => (),
            }
        }

        let wire_limit = self.max_out_size;
        let body_budget = outbound_body_budget(wire_limit);
        let body_len = item.encoded_body_len(body_budget)?;
        if wire_limit != 0 {
            let wire_size = mqtt_wire_len(body_len);
            if wire_size > wire_limit {
                return Err(EncodeError::OverMaxPacketSize {
                    size: wire_size,
                    max: wire_limit,
                });
            }
        }
        dst.reserve(1 + var_int_len_u32(body_len as u32) as usize + body_len);
        item.encode_wire(dst, body_len as u32)?; // safe: body_budget <= u32::MAX
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU16;

    use bytestring::ByteString;

    use super::*;
    use crate::codec::v5::packet::Connect;

    fn assert_sync<T: Sync>() {}

    #[test]
    fn v5_codec_is_sync() {
        assert_sync::<V5Codec>();
    }

    #[test]
    fn test_max_inbound_size() {
        let mut codec = V5Codec::new(5, 0);
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"\0\x09");
        assert_eq!(
            codec
                .decode(&mut buf)
                .map_err(|e| matches!(e, DecodeError::MaxSizeExceeded { .. })),
            Err(true)
        );
    }

    #[test]
    fn outbound_max_size_counts_fixed_header() {
        let connect = Packet::Connect(Box::new(Connect {
            clean_start: true,
            keep_alive: 0,
            client_id: ByteString::from_static("x"),
            ..Default::default()
        }));
        let body_len = connect.encoded_body_len(u32::MAX).unwrap();
        let wire_len = mqtt_wire_len(body_len);

        let mut codec = V5Codec::new(0, wire_len);
        let mut buf = BytesMut::new();
        codec
            .encode(connect.clone(), &mut buf)
            .expect("fits at exact wire limit");

        let mut codec = V5Codec::new(0, wire_len - 1);
        let err = codec
            .encode(connect, &mut BytesMut::new())
            .expect_err("exceeds wire limit");
        assert!(matches!(err, EncodeError::OverMaxPacketSize { max, .. } if max == wire_len - 1));
    }

    #[test]
    fn set_max_outbound_size_matches_new_semantics() {
        let mut via_new = V5Codec::new(0, 128);
        let mut via_setter = V5Codec::default();
        via_setter.set_max_outbound_size(128);
        assert_eq!(via_new.max_outbound_size(), via_setter.max_outbound_size());

        let connect = Packet::Connect(Box::new(Connect {
            client_id: ByteString::from_static("client"),
            ..Default::default()
        }));
        let mut buf_new = BytesMut::new();
        let mut buf_set = BytesMut::new();
        via_new.encode(connect.clone(), &mut buf_new).expect("encode via new");
        via_setter.encode(connect, &mut buf_set).expect("encode via setter");
        assert_eq!(buf_new, buf_set);
    }

    #[test]
    fn outbound_max_size_via_setter_rejects_oversize_ping() {
        let mut codec = V5Codec::default();
        codec.set_max_outbound_size(1);
        let err = codec
            .encode(Packet::PingRequest, &mut BytesMut::new())
            .expect_err("PINGREQ wire is 2 bytes");
        assert!(matches!(err, EncodeError::OverMaxPacketSize { max: 1, .. }));
    }

    #[test]
    fn outbound_max_size_allows_minimal_puback() {
        let ack = Packet::PublishAck(crate::codec::v5::PublishAck {
            packet_id: NonZeroU16::new(1).unwrap(),
            ..Default::default()
        });
        let body_len = ack.encoded_body_len(u32::MAX).unwrap();
        let wire_len = mqtt_wire_len(body_len);

        let mut codec = V5Codec::new(0, wire_len);
        codec.encode(ack, &mut BytesMut::new()).expect("minimal PUBACK fits");
    }

    #[test]
    fn v5_codec_decode_waits_for_incomplete_frame_body() {
        let mut codec = V5Codec::new(0, 0);
        let mut buf = BytesMut::from(&[0x30, 0x09][..]);
        assert_eq!(codec.decode(&mut buf).unwrap(), None);
    }

    #[test]
    fn v5_codec_decode_rejects_non_minimal_remaining_length() {
        let mut codec = V5Codec::new(0, 0);
        let mut buf = BytesMut::from(&[0xC0, 0x80, 0x00][..]);
        assert!(matches!(codec.decode(&mut buf), Err(DecodeError::MalformedPacket(_))));
    }
}
