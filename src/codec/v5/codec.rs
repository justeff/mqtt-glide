use std::num::NonZeroU16;

use bytes::{Buf, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use super::{DecodedFrame, Packet, PublishAck, decode::decode_packet};
use crate::codec::error::{DecodeError, EncodeError};
use crate::codec::properties::var_int_len_u32;
use crate::codec::types::{MAX_PACKET_SIZE, packet_type};
use crate::codec::utils::{MAX_VARIABLE_LENGTH, MINIMAL_PACKET_ID_ACK_WIRE_LEN, decode_variable_length};

/// Worst-case fixed-header overhead: 1 type byte + 4-byte Remaining Length varint (§2.1.4).
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

/// MQTT 5.0 `tokio_util::codec` adapter for framing and control-packet encode/decode.
///
/// Responsibilities:
/// - Split inbound bytes on Remaining Length (§2.1.4) with optional max wire size
/// - Encode outbound [`Packet`] bodies within the negotiated Maximum Packet Size
/// - Honour CONNECT Request Problem Information ([MQTT-3.1.2-29], §3.1.2.11.7)
///
/// Application PUBLISH uses [`crate::codec::encode_publish`] / [`DecodedFrame::Publish`].
#[derive(Debug, Clone)]
pub struct V5Codec {
    max_in_size: u32,
    max_out_size: u32,
    negotiated: NegotiatedLimits,
}

bitflags::bitflags! {
    /// CONNECT-derived encode constraints applied after the peer's CONNECT is decoded.
    #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
    struct NegotiatedLimits: u8 {
        /// Strip Reason String / diagnostic properties on subsequent encodes ([MQTT-3.1.2-29]).
        const STRIP_PROBLEM_INFO = 0b0000_0001;
    }
}

impl V5Codec {
    /// Creates a codec with independent inbound and outbound wire-size limits.
    ///
    /// `0` means unlimited for that direction.
    pub fn new(max_in_size: u32, max_out_size: u32) -> Self {
        V5Codec {
            max_in_size,
            max_out_size,
            negotiated: NegotiatedLimits::empty(),
        }
    }

    /// Codec with the same max size for inbound and outbound frames.
    ///
    /// `0` means unlimited (see [`Self::new`]). Prefer this over separate in/out limits
    /// when constructing `Framed` halves for tests or the connection driver.
    pub fn with_max_packet_size(max_packet_size: u32) -> Self {
        Self::new(max_packet_size, max_packet_size)
    }

    /// Maximum inbound frame size in total wire bytes (fixed header included).
    ///
    /// `0` means unlimited.
    pub fn max_inbound_size(&self) -> u32 {
        self.max_in_size
    }

    /// Maximum outbound frame size in total wire bytes (fixed header included).
    ///
    /// `0` means unlimited.
    pub fn max_outbound_size(&self) -> u32 {
        self.max_out_size
    }

    /// Sets the maximum inbound frame size in total wire bytes.
    ///
    /// `0` means unlimited.
    pub fn set_max_inbound_size(&mut self, size: u32) {
        self.max_in_size = size;
    }

    /// Sets the maximum outbound frame size in total wire bytes.
    ///
    /// `0` means unlimited.
    pub fn set_max_outbound_size(&mut self, size: u32) {
        self.max_out_size = size;
    }

    /// Encode one control [`Packet`] by appending to `buf`.
    ///
    /// Does not clear `buf`. Callers that replace a single-frame buffer must `clear()` first.
    /// For PUBLISH use [`crate::codec::encode_publish`].
    pub fn encode_packet(&mut self, packet: Packet, buf: &mut BytesMut) -> Result<(), EncodeError> {
        self.encode(packet, buf)
    }

    /// Decode one frame, discarding the consumed Remaining Length from the codec item.
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
        // Most frames are small enough for a one-byte Remaining Length.
        let (remaining_length, varint_len) = if src[1] & 0x80 == 0 {
            (u32::from(src[1]), 1)
        } else {
            match decode_variable_length(&src[1..])? {
                Some(length) => length,
                None => return Ok(None),
            }
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

        // Success PUBACK without reason code or properties (§3.4.2.1), one per outbound QoS 1
        // publish: read the Packet Identifier in place instead of splitting off a frame.
        if first_byte == packet_type::PUBACK && remaining_length == 2 {
            let packet_id = NonZeroU16::new(u16::from_be_bytes([src[2], src[3]]))
                .ok_or(DecodeError::MalformedPacket("invalid encoding"))?; // §2.2.1
            src.advance(MINIMAL_PACKET_ID_ACK_WIRE_LEN);
            let ack = PublishAck {
                packet_id,
                ..PublishAck::default()
            };
            return Ok(Some((DecodedFrame::Control(Packet::PublishAck(ack)), remaining_length)));
        }

        let mut frame = src.split_to(wire_size);
        frame.advance(header_len);
        let packet = decode_packet(frame.freeze(), first_byte)?;

        if let DecodedFrame::Control(Packet::Connect(ref pkt)) = packet {
            // [MQTT-3.1.2-29] Request Problem Information = 0 → omit diagnostics on replies.
            self.negotiated
                .set(NegotiatedLimits::STRIP_PROBLEM_INFO, !pkt.request_problem_info);
        }
        Ok(Some((packet, remaining_length)))
    }
}

impl Encoder<Packet> for V5Codec {
    type Error = EncodeError;

    fn encode(&mut self, mut item: Packet, dst: &mut BytesMut) -> Result<(), EncodeError> {
        // [MQTT-3.1.2-29] / §3.1.2.11.7 — strip Reason String and User Properties when requested.
        if self.negotiated.contains(NegotiatedLimits::STRIP_PROBLEM_INFO) {
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
        // Bodies beyond the Remaining Length ceiling (§2.1.4) cannot be framed at all;
        // reject before `mqtt_wire_len` / `encode_wire` can truncate the u32 cast.
        if body_len > MAX_VARIABLE_LENGTH as usize {
            return Err(EncodeError::InvalidLength);
        }
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

    /// Control bodies beyond the Remaining Length ceiling (§2.1.4) are rejected before
    /// any header byte is written; previously `write_variable_length` failed only after
    /// the packet type byte, leaving a partial frame in the write buffer.
    #[test]
    fn encode_control_rejects_body_beyond_remaining_length_ceiling() {
        use bytes::Bytes;
        // `vec![0; n]` is lazily zero-mapped; the rejected encode never touches the pages.
        let connect = Packet::Connect(Box::new(Connect {
            client_id: ByteString::from_static("id"),
            password: Some(Bytes::from(vec![0u8; MAX_VARIABLE_LENGTH as usize])),
            ..Default::default()
        }));
        let mut codec = V5Codec::default();
        let mut buf = BytesMut::new();
        let err = codec.encode(connect, &mut buf).expect_err("oversize body");
        assert!(matches!(err, EncodeError::InvalidLength), "got {err:?}");
        assert!(buf.is_empty(), "rejected encode must not leave partial bytes");
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

    #[test]
    fn minimal_puback_fast_path_matches_general_decode() {
        let mut codec = V5Codec::default();
        let mut buf = BytesMut::from(&[0x40, 0x02, 0x12, 0x34, 0x40, 0x03, 0x12, 0x35, 0x10][..]);
        let (fast, len) = codec.decode(&mut buf).unwrap().expect("minimal PUBACK");
        assert_eq!(len, 2);
        assert_eq!(
            fast,
            DecodedFrame::Control(Packet::PublishAck(crate::codec::v5::PublishAck {
                packet_id: NonZeroU16::new(0x1234).unwrap(),
                ..Default::default()
            }))
        );
        // Next frame (reason code 0x10, general path) is untouched by the fast path.
        let (general, _) = codec.decode(&mut buf).unwrap().expect("PUBACK with reason");
        let DecodedFrame::Control(Packet::PublishAck(ack)) = general else {
            panic!("expected PUBACK, got {general:?}");
        };
        assert_eq!(ack.packet_id.get(), 0x1235);
        assert_eq!(
            ack.reason_code,
            crate::codec::v5::PublishAckReason::NoMatchingSubscribers
        );
        assert!(buf.is_empty());
    }

    #[test]
    fn minimal_puback_fast_path_rejects_zero_packet_id_and_waits_for_bytes() {
        let mut codec = V5Codec::default();
        let mut buf = BytesMut::from(&[0x40, 0x02, 0x00][..]);
        assert_eq!(codec.decode(&mut buf).unwrap(), None, "incomplete frame waits");
        buf.extend_from_slice(&[0x00]);
        assert!(matches!(codec.decode(&mut buf), Err(DecodeError::MalformedPacket(_))));
    }

    #[test]
    fn strip_problem_info_clears_puback_diagnostics() {
        let mut codec = V5Codec::default();
        codec.negotiated.insert(NegotiatedLimits::STRIP_PROBLEM_INFO);

        let mut buf = BytesMut::new();
        let ack = Packet::PublishAck(crate::codec::v5::PublishAck {
            packet_id: NonZeroU16::new(1).unwrap(),
            reason_code: crate::codec::v5::PublishAckReason::Success,
            properties: vec![("k", "v").into()],
            reason_string: Some(ByteString::from_static("oops")),
        });
        codec.encode(ack, &mut buf).unwrap();
        // After stripping diagnostics, Success PUBACK is Packet Identifier only (Remaining Length 2).
        assert_eq!(buf[1], 0x02);
    }
}
