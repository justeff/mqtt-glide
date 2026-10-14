//! Codec errors for MQTT encode and decode.
//!
//! - [`DecodeError`]: failures while parsing frames (§2 / §3 wire rules)
//! - [`EncodeError`]: failures while serializing packets to bytes

use std::io;

use bytestring::ByteString;

use crate::codec::v5::{DisconnectReasonCode, ToReasonCode};

/// Errors while decoding MQTT frames from a byte stream.
#[derive(Debug, Clone, thiserror::Error)]
#[non_exhaustive]
pub enum DecodeError {
    #[error("Invalid protocol")]
    InvalidProtocol,
    #[error("Invalid length")]
    InvalidLength,
    /// Packet bytes violate MQTT framing or field rules (`reason` is ops-facing).
    #[error("Malformed packet: {0}")]
    MalformedPacket(&'static str),
    /// Well-formed bytes the spec calls a Protocol Error, such as a property included twice
    /// or a value it forbids (`reason` is ops-facing).
    #[error("Protocol error: {0}")]
    ProtocolViolation(&'static str),
    /// A PUBLISH with Topic Alias 0 (§3.3.2.3.4).
    #[error("Topic Alias 0 is not allowed")]
    TopicAliasInvalid,
    #[error("Unsupported protocol level")]
    UnsupportedProtocolLevel,
    #[error("Connect frame's reserved flag is set")]
    ConnectReservedFlagSet,
    #[error("ConnectAck frame's reserved flag is set")]
    ConnectAckReservedFlagSet,
    #[error("Unsupported packet type")]
    UnsupportedPacketType,
    #[error("Packet id is required")]
    PacketIdRequired,
    #[error("Max size exceeded: size={size}, max={max}")]
    MaxSizeExceeded { size: u32, max: u32 },
    #[error("io error, {:?}", _0)]
    Io(ByteString),
}

impl From<io::Error> for DecodeError {
    fn from(e: io::Error) -> DecodeError {
        DecodeError::Io(e.to_string().into())
    }
}

impl ToReasonCode for DecodeError {
    fn to_reason_code(&self) -> DisconnectReasonCode {
        match self {
            DecodeError::InvalidProtocol => DisconnectReasonCode::ProtocolError,
            DecodeError::InvalidLength => DisconnectReasonCode::MalformedPacket,
            DecodeError::MalformedPacket(_) => DisconnectReasonCode::MalformedPacket,
            DecodeError::ProtocolViolation(_) => DisconnectReasonCode::ProtocolError,
            DecodeError::TopicAliasInvalid => DisconnectReasonCode::TopicAliasInvalid,
            DecodeError::UnsupportedProtocolLevel => DisconnectReasonCode::ImplementationSpecificError,
            // A set reserved bit, and the forbidden packet type 0, make a Malformed Packet
            // (§2.1.2, §2.1.3, [MQTT-3.1.2-3]).
            DecodeError::ConnectReservedFlagSet => DisconnectReasonCode::MalformedPacket,
            DecodeError::ConnectAckReservedFlagSet => DisconnectReasonCode::MalformedPacket,
            DecodeError::UnsupportedPacketType => DisconnectReasonCode::MalformedPacket,
            DecodeError::PacketIdRequired => DisconnectReasonCode::ProtocolError,
            DecodeError::MaxSizeExceeded { .. } => DisconnectReasonCode::PacketTooLarge,
            DecodeError::Io(_) => DisconnectReasonCode::UnspecifiedError,
        }
    }
}

/// Errors while encoding MQTT packets to a byte stream.
#[derive(Debug, Clone, thiserror::Error)]
#[non_exhaustive]
pub enum EncodeError {
    #[error("Packet is bigger than peer's Maximum Packet Size: size={size}, max={max}")]
    OverMaxPacketSize { size: u32, max: u32 },
    #[error("Invalid length")]
    InvalidLength,
    #[error("Malformed packet")]
    MalformedPacket,
    #[error("Packet id is required")]
    PacketIdRequired,
    #[error("Unsupported version")]
    UnsupportedVersion,
    #[error("io error, {:?}", _0)]
    Io(ByteString),
}

impl From<io::Error> for EncodeError {
    fn from(e: io::Error) -> EncodeError {
        EncodeError::Io(e.to_string().into())
    }
}

impl ToReasonCode for EncodeError {
    fn to_reason_code(&self) -> DisconnectReasonCode {
        match self {
            EncodeError::OverMaxPacketSize { .. } => DisconnectReasonCode::PacketTooLarge,
            EncodeError::InvalidLength => DisconnectReasonCode::MalformedPacket,
            EncodeError::MalformedPacket => DisconnectReasonCode::MalformedPacket,
            EncodeError::PacketIdRequired => DisconnectReasonCode::ProtocolError,
            EncodeError::UnsupportedVersion => DisconnectReasonCode::ImplementationSpecificError,
            EncodeError::Io(_) => DisconnectReasonCode::UnspecifiedError,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::v5::ToReasonCode;

    #[test]
    fn malformed_packet_decode_error_maps_to_malformed_packet_reason() {
        // [MQTT-1.5.4-1], [MQTT-1.5.4-2] — ill-formed UTF-8 / U+0000 use MalformedPacket.
        assert_eq!(
            DecodeError::MalformedPacket("utf-8").to_reason_code(),
            DisconnectReasonCode::MalformedPacket
        );
    }
}
