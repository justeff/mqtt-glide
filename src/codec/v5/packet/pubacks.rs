//! MQTT PUBACK (§3.4), PUBREC (§3.5), PUBREL (§3.6), and PUBCOMP (§3.7).
//!
//! QoS 1/2 acknowledgement packets per OASIS MQTT Version 5.0.
//!
//! # Errors
//! Decode rejects unknown Reason Codes and trailing bytes after the ack body (§2.1.4).

use std::convert::TryFrom;
use std::num::NonZeroU16;

use bytes::{BufMut, Bytes, BytesMut};
use bytestring::ByteString;

use crate::codec::error::{DecodeError, EncodeError};
use crate::codec::properties::ack_props;
use crate::codec::types::packet_type;
use crate::codec::utils::{Decode, Encode, MINIMAL_PACKET_ID_ACK_WIRE_LEN, WireReader, encode_minimal_packet_id_ack};
use crate::codec::v5::body_encode::WireBodyEncode;
use crate::codec::v5::{DisconnectReasonCode, ToReasonCode, UserProperties};

/// Append a minimal Success PUBACK to `buf` without building [`crate::codec::Packet`].
///
/// Client inbound path: Success only — no Reason Code or Properties ([MQTT-3.4.2-1]).
/// Write-task counterpart to [`crate::codec::encode_publish`]: does not clear `buf`
/// (safe for batching). Honours `max_packet_size` when non-zero.
pub(crate) fn encode_publish_ack(
    packet_id: NonZeroU16,
    max_packet_size: u32,
    buf: &mut BytesMut,
) -> Result<(), EncodeError> {
    if max_packet_size != 0 && MINIMAL_PACKET_ID_ACK_WIRE_LEN as u32 > max_packet_size {
        return Err(EncodeError::OverMaxPacketSize {
            size: MINIMAL_PACKET_ID_ACK_WIRE_LEN as u32,
            max: max_packet_size,
        });
    }
    buf.reserve(MINIMAL_PACKET_ID_ACK_WIRE_LEN);
    encode_minimal_packet_id_ack(buf, packet_type::PUBACK, packet_id);
    Ok(())
}

/// Packet Identifier (2) + Reason Code (1).
const HEADER_LEN: u32 = 2 + 1;

/// PUBACK / PUBREC body (§3.4 / §3.5).
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct PublishAck {
    /// Packet Identifier (§2.2.1).
    pub packet_id: NonZeroU16,
    pub reason_code: PublishAckReason,
    pub properties: UserProperties,
    pub reason_string: Option<ByteString>,
}

/// PUBREL / PUBCOMP body (§3.6 / §3.7).
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct PubRelComp {
    /// Packet Identifier (§2.2.1).
    pub packet_id: NonZeroU16,
    pub reason_code: PubRelCompReason,
    pub properties: UserProperties,
    pub reason_string: Option<ByteString>,
}

/// PUBACK / PUBREC Reason Codes (§3.4.2.1 / §3.5.2.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PublishAckReason {
    Success = 0,
    NoMatchingSubscribers = 16,
    UnspecifiedError = 128,
    ImplementationSpecificError = 131,
    NotAuthorized = 135,
    TopicNameInvalid = 144,
    PacketIdentifierInUse = 145,
    QuotaExceeded = 151,
    PayloadFormatInvalid = 153,
}

impl TryFrom<u8> for PublishAckReason {
    type Error = DecodeError;

    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(Self::Success),
            16 => Ok(Self::NoMatchingSubscribers),
            128 => Ok(Self::UnspecifiedError),
            131 => Ok(Self::ImplementationSpecificError),
            135 => Ok(Self::NotAuthorized),
            144 => Ok(Self::TopicNameInvalid),
            145 => Ok(Self::PacketIdentifierInUse),
            151 => Ok(Self::QuotaExceeded),
            153 => Ok(Self::PayloadFormatInvalid),
            _ => Err(DecodeError::MalformedPacket("unknown property or reason code")),
        }
    }
}

impl From<PublishAckReason> for u8 {
    fn from(v: PublishAckReason) -> Self {
        v as u8
    }
}

impl PublishAckReason {
    #[inline]
    pub fn should_disconnect(&self) -> bool {
        matches!(
            self,
            PublishAckReason::UnspecifiedError
                | PublishAckReason::ImplementationSpecificError
                | PublishAckReason::NotAuthorized
                | PublishAckReason::TopicNameInvalid
        )
    }
}

impl ToReasonCode for PublishAckReason {
    fn to_reason_code(&self) -> DisconnectReasonCode {
        match self {
            PublishAckReason::Success | PublishAckReason::NoMatchingSubscribers => {
                DisconnectReasonCode::NormalDisconnection
            }
            PublishAckReason::UnspecifiedError => DisconnectReasonCode::UnspecifiedError,
            PublishAckReason::ImplementationSpecificError => DisconnectReasonCode::ImplementationSpecificError,
            PublishAckReason::NotAuthorized => DisconnectReasonCode::NotAuthorized,
            PublishAckReason::TopicNameInvalid => DisconnectReasonCode::TopicNameInvalid,
            PublishAckReason::PacketIdentifierInUse => DisconnectReasonCode::ProtocolError,
            PublishAckReason::QuotaExceeded => DisconnectReasonCode::QuotaExceeded,
            PublishAckReason::PayloadFormatInvalid => DisconnectReasonCode::PayloadFormatInvalid,
        }
    }
}

/// PUBREL / PUBCOMP Reason Codes (§3.6.2.1 / §3.7.2.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PubRelCompReason {
    Success = 0,
    PacketIdNotFound = 146,
}

impl TryFrom<u8> for PubRelCompReason {
    type Error = DecodeError;

    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(Self::Success),
            146 => Ok(Self::PacketIdNotFound),
            _ => Err(DecodeError::MalformedPacket("unknown property or reason code")),
        }
    }
}

impl From<PubRelCompReason> for u8 {
    fn from(v: PubRelCompReason) -> Self {
        v as u8
    }
}

impl PubRelCompReason {
    #[inline]
    pub fn should_disconnect(&self) -> bool {
        matches!(self, PubRelCompReason::PacketIdNotFound)
    }
}

impl ToReasonCode for PubRelCompReason {
    fn to_reason_code(&self) -> DisconnectReasonCode {
        match self {
            PubRelCompReason::Success => DisconnectReasonCode::NormalDisconnection,
            PubRelCompReason::PacketIdNotFound => DisconnectReasonCode::ImplementationSpecificError,
        }
    }
}

impl PublishAck {
    #[inline]
    pub(crate) fn is_minimal_success(&self) -> bool {
        self.reason_code == PublishAckReason::Success && self.properties.is_empty() && self.reason_string.is_none()
    }

    #[inline]
    fn has_ack_properties(&self) -> bool {
        !self.properties.is_empty() || self.reason_string.is_some()
    }

    pub(crate) fn decode(src: &mut Bytes) -> Result<Self, DecodeError> {
        let reader = WireReader::new(src);
        let packet_id = NonZeroU16::decode(reader.into_inner())?; // §2.2.1 — Packet Identifier must be non-zero

        let mut reader = WireReader::new(src);
        if reader.remaining() == 0 {
            return Ok(Self {
                packet_id,
                ..Default::default()
            });
        }

        let reason_code = reader.read_u8()?.try_into()?;
        if reader.remaining() == 0 {
            return Ok(Self {
                packet_id,
                reason_code,
                ..Default::default()
            });
        }

        let (properties, reason_string) = ack_props::decode(reader.into_inner())?;
        // No payload after properties (§2.1.4).
        ensure!(src.is_empty(), DecodeError::InvalidLength);
        Ok(Self {
            packet_id,
            reason_code,
            properties,
            reason_string,
        })
    }
}

impl Default for PublishAck {
    fn default() -> Self {
        Self {
            packet_id: NonZeroU16::MIN,
            reason_code: PublishAckReason::Success,
            properties: UserProperties::default(),
            reason_string: None,
        }
    }
}

impl PubRelComp {
    #[inline]
    pub(crate) fn is_minimal_success(&self) -> bool {
        self.reason_code == PubRelCompReason::Success && self.properties.is_empty() && self.reason_string.is_none()
    }

    #[inline]
    fn has_ack_properties(&self) -> bool {
        !self.properties.is_empty() || self.reason_string.is_some()
    }

    pub(crate) fn decode(src: &mut Bytes) -> Result<Self, DecodeError> {
        let reader = WireReader::new(src);
        let packet_id = NonZeroU16::decode(reader.into_inner())?; // §2.2.1 — Packet Identifier must be non-zero

        let mut reader = WireReader::new(src);
        if reader.remaining() == 0 {
            return Ok(Self {
                packet_id,
                ..Default::default()
            });
        }

        let reason_code = reader.read_u8()?.try_into()?;
        if reader.remaining() == 0 {
            return Ok(Self {
                packet_id,
                reason_code,
                ..Default::default()
            });
        }

        let (properties, reason_string) = ack_props::decode(reader.into_inner())?;
        // No payload after properties (§2.1.4).
        ensure!(src.is_empty(), DecodeError::InvalidLength);
        Ok(Self {
            packet_id,
            reason_code,
            properties,
            reason_string,
        })
    }
}

impl Default for PubRelComp {
    fn default() -> Self {
        Self {
            packet_id: NonZeroU16::MIN,
            reason_code: PubRelCompReason::Success,
            properties: UserProperties::default(),
            reason_string: None,
        }
    }
}

impl WireBodyEncode for PublishAck {
    fn encoded_body_len(&self, max_out: u32) -> Result<usize, EncodeError> {
        if self.is_minimal_success() {
            return Ok(2);
        }
        if !self.has_ack_properties() {
            return Ok(HEADER_LEN as usize);
        }
        // Body budget: Packet Identifier + Reason Code; reserve ≤4 bytes for Property Length varint.
        let prop_len = ack_props::encoded_size(&self.properties, &self.reason_string, max_out - HEADER_LEN - 4);
        Ok(HEADER_LEN as usize + prop_len)
    }

    fn encode_body(&self, buf: &mut BytesMut, max_out: u32) -> Result<(), EncodeError> {
        self.packet_id.get().encode(buf)?;
        if self.is_minimal_success() {
            return Ok(());
        }
        buf.put_u8(self.reason_code.into());
        if !self.has_ack_properties() {
            return Ok(());
        }
        ack_props::encode(&self.properties, &self.reason_string, buf, max_out - HEADER_LEN)?;
        Ok(())
    }
}

impl WireBodyEncode for PubRelComp {
    fn encoded_body_len(&self, max_out: u32) -> Result<usize, EncodeError> {
        // Packet Identifier + Reason Code.
        const HEADER_LEN: u32 = 2 + 1;
        if self.is_minimal_success() {
            return Ok(2);
        }
        if !self.has_ack_properties() {
            return Ok(HEADER_LEN as usize);
        }
        // Body budget: Packet Identifier + Reason Code; reserve ≤4 bytes for Property Length varint.
        let prop_len = ack_props::encoded_size(&self.properties, &self.reason_string, max_out - HEADER_LEN - 4);
        Ok(HEADER_LEN as usize + prop_len)
    }

    fn encode_body(&self, buf: &mut BytesMut, max_out: u32) -> Result<(), EncodeError> {
        self.packet_id.get().encode(buf)?;
        if self.is_minimal_success() {
            return Ok(());
        }
        buf.put_u8(self.reason_code.into());
        if !self.has_ack_properties() {
            return Ok(());
        }
        ack_props::encode(&self.properties, &self.reason_string, buf, max_out - 3)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case::success_empty(b"\xFF\xFF\x00\x00", 65535, PublishAckReason::Success, vec![], None)]
    #[case::success_no_reason(b"\x00\x01", 1, PublishAckReason::Success, vec![], None)]
    #[case::success_no_prop_len(b"\x01\x01\x00", 257, PublishAckReason::Success, vec![], None)]
    #[case::no_success_no_prop_len(b"\x00\x01\x87", 1, PublishAckReason::NotAuthorized, vec![], None)]
    #[case::no_success_min(b"\x00\x01\x83\x00", 1, PublishAckReason::ImplementationSpecificError, vec![], None)]
    #[case::all_out(b"\x00\xFF\x80\x0D\x26\x00\x01a\x00\x01b\x1F\x00\x03123", 255, PublishAckReason::UnspecifiedError, vec![("a", "b")], Some("123"))]
    fn puback_decode_success(
        #[case] input: &'static [u8],
        #[case] packet_id: u16,
        #[case] reason_code: PublishAckReason,
        #[case] properties: Vec<(&'static str, &'static str)>,
        #[case] reason_string: Option<&'static str>,
    ) {
        let mut input = input.into();
        let result = PublishAck::decode(&mut input);
        assert_eq!(
            result.unwrap(),
            PublishAck {
                packet_id: packet_id.try_into().unwrap(),
                reason_code,
                properties: properties.into_iter().map(Into::into).collect(),
                reason_string: reason_string.map(|s| s.into())
            }
        );
        assert_eq!(input.len(), 0);
    }

    #[rstest]
    #[case::packet_id_zero(b"\x00\x00", DecodeError::MalformedPacket("invalid encoding"))]
    #[case::properties_promised(b"\x00\x01\x00\x01", DecodeError::InvalidLength)]
    fn puback_decode_must_fail(#[case] input: &'static [u8], #[case] error: DecodeError) {
        let mut input = input.into();
        let result = PublishAck::decode(&mut input);
        assert_eq!(result.map_err(|e| e.to_string()), Err(error.to_string()));
    }

    #[test]
    fn encode_publish_ack_matches_minimal_packet_path_bytes() {
        let mut buf = BytesMut::new();
        encode_publish_ack(NonZeroU16::new(0x4321).unwrap(), 0, &mut buf).expect("encode");
        assert_eq!(&buf[..], b"\x40\x02\x43\x21");
    }

    #[test]
    fn encode_puback_rejects_over_max_packet_size() {
        let mut buf = BytesMut::new();
        let err = encode_publish_ack(NonZeroU16::new(1).unwrap(), 3, &mut buf).expect_err("oversize");
        assert!(matches!(err, EncodeError::OverMaxPacketSize { size: 4, max: 3 }));
        assert!(buf.is_empty(), "rejected encode must not leave partial bytes");
    }

    fn round_trip_puback(ack: &PublishAck) {
        let mut body = BytesMut::new();
        let body_len = ack.encoded_body_len(u32::MAX).unwrap() as u32;
        ack.encode_body(&mut body, body_len).unwrap();
        let decoded = PublishAck::decode(&mut body.freeze()).unwrap();
        assert_eq!(decoded, *ack);
    }

    #[test]
    fn puback_encode_decode_round_trip() {
        round_trip_puback(&PublishAck {
            packet_id: NonZeroU16::new(42).unwrap(),
            reason_code: PublishAckReason::Success,
            properties: UserProperties::default(),
            reason_string: None,
        });

        round_trip_puback(&PublishAck {
            packet_id: NonZeroU16::new(7).unwrap(),
            reason_code: PublishAckReason::NotAuthorized,
            properties: vec![("k", "v").into()],
            reason_string: Some(ByteString::from_static("denied")),
        });
    }

    #[test]
    fn puback_encode_omits_reason_string_when_it_exceeds_max_packet_size() {
        let ack = PublishAck {
            packet_id: NonZeroU16::new(1).unwrap(),
            reason_code: PublishAckReason::NotAuthorized,
            properties: Vec::new(),
            reason_string: Some(ByteString::from_static("too long for budget")),
        };
        let mut buf = BytesMut::new();
        // Body budget: packet id + reason code + empty Property Length only.
        ack.encode_body(&mut buf, 4).expect("encode");
        assert_eq!(
            buf.as_ref(),
            &[0x00, 0x01, 0x87, 0x00],
            "Reason String property must be omitted when it would exceed the PUBACK body budget"
        );
    }

    #[test]
    fn puback_encode_omits_user_property_when_it_exceeds_max_packet_size() {
        let ack = PublishAck {
            packet_id: NonZeroU16::new(2).unwrap(),
            reason_code: PublishAckReason::UnspecifiedError,
            properties: vec![("k", "value-does-not-fit").into()],
            reason_string: None,
        };
        let mut buf = BytesMut::new();
        ack.encode_body(&mut buf, 4).expect("encode");
        assert_eq!(
            buf.as_ref(),
            &[0x00, 0x02, 0x80, 0x00],
            "User Property must be omitted when it would exceed the PUBACK body budget"
        );
    }
}
