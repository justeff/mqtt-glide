//! MQTT 5 wire codec.
//!
//! # Free functions vs methods
//!
//! Decode/encode that a type owns use associated methods (`Connect::decode`,
//! `PublishPacket::decode`, `V5Codec::encode_packet` / `V5Codec::decode_frame`).
//!
//! [`encode_publish`] and [`ensure_publish_wire_fits`] stay free: the hot-path
//! inputs are [`PublishMessage`] + [`PublishDelivery`] + a buffer (or size check),
//! not a [`Packet`] and not codec state.

#[macro_use]
mod utils;

pub mod error;
pub(crate) mod properties;
pub mod publish;
pub mod types;
pub mod v5;

pub const DEFAULT_MAX_PACKET_SIZE: u32 = 1024 * 1024;

pub use error::{DecodeError, EncodeError};
pub use publish::{PublishDelivery, PublishMessage, PublishPacket, PublishPacketMeta, PublishProperties};
pub use types::QoS;

pub(crate) use utils::topic_filter_bytes_invalid;

pub(crate) use v5::encode_publish_ack;
pub use v5::{
    Auth, AuthReasonCode, Connect, ConnectAck, ConnectAckReason, DecodedFrame, Disconnect, DisconnectReasonCode,
    Packet, PubRelComp, PubRelCompReason, PublishAck, PublishAckReason, RECEIVE_MAX_DEFAULT, RetainHandling, Subscribe,
    SubscribeAck, SubscribeAckReason, SubscriptionOptions, Unsubscribe, UnsubscribeAck, UnsubscribeAckReason,
    UserProperties, UserProperty, WillMessage, encode_publish,
};

/// PUBREL packet body (alias for the shared PUBREL/PUBCOMP payload type).
pub type PubRel = PubRelComp;
/// PUBCOMP packet body (alias for the shared PUBREL/PUBCOMP payload type).
pub type PubComp = PubRelComp;

/// MQTT v5.0 wire codec (`tokio_util::codec::Decoder` / `Encoder` for [`Packet`]).
pub type V5Codec = v5::V5Codec;

/// Reject oversize or malformed outbound PUBLISH before queueing.
pub fn ensure_publish_wire_fits(
    message: &PublishMessage,
    delivery: PublishDelivery,
    max_packet_size: u32,
) -> Result<(), EncodeError> {
    v5::publish_wire_len(message, delivery, max_packet_size)?;
    Ok(())
}

#[cfg(test)]
mod facade_tests {
    use super::*;
    use bytes::{Bytes, BytesMut};
    use bytestring::ByteString;

    fn sample_publish_message(payload_len: usize) -> PublishMessage {
        PublishMessage {
            topic: ByteString::from_static("telemetry/device/status"),
            payload: Bytes::from(vec![0_u8; payload_len]),
            properties: PublishProperties::default(),
        }
    }

    #[test]
    fn ensure_publish_wire_fits_accepts_qos0() {
        let message = sample_publish_message(4);
        let delivery = PublishDelivery {
            dup: false,
            retain: false,
            qos: QoS::AtMostOnce,
            packet_id: None,
        };
        ensure_publish_wire_fits(&message, delivery, DEFAULT_MAX_PACKET_SIZE).expect("fits qos0");
    }

    #[test]
    fn ensure_publish_wire_fits_rejects_over_max_packet_size() {
        let message = sample_publish_message(512);
        let delivery = PublishDelivery {
            dup: false,
            retain: false,
            qos: QoS::AtMostOnce,
            packet_id: None,
        };
        let err = ensure_publish_wire_fits(&message, delivery, 64).expect_err("oversize");
        assert!(matches!(err, EncodeError::OverMaxPacketSize { .. }));
    }

    #[test]
    fn ensure_publish_wire_fits_rejects_malformed_dup_on_qos0() {
        let message = sample_publish_message(0);
        let delivery = PublishDelivery {
            dup: true,
            retain: false,
            qos: QoS::AtMostOnce,
            packet_id: None,
        };
        let err = ensure_publish_wire_fits(&message, delivery, DEFAULT_MAX_PACKET_SIZE).expect_err("dup on qos0");
        assert!(matches!(err, EncodeError::MalformedPacket));
    }

    #[test]
    fn ensure_publish_wire_fits_rejects_qos1_without_packet_id() {
        let message = sample_publish_message(0);
        let delivery = PublishDelivery {
            dup: false,
            retain: false,
            qos: QoS::AtLeastOnce,
            packet_id: None,
        };
        let err =
            ensure_publish_wire_fits(&message, delivery, DEFAULT_MAX_PACKET_SIZE).expect_err("packet id required");
        assert!(matches!(err, EncodeError::PacketIdRequired));
    }

    #[test]
    fn encode_packet_ping_round_trips_via_decode() {
        let mut codec = V5Codec::with_max_packet_size(0);
        let mut buf = BytesMut::with_capacity(16);
        codec
            .encode_packet(Packet::PingRequest, &mut buf)
            .expect("encode PINGREQ");
        let frame = codec.decode_frame(&mut buf).expect("decode").expect("frame");
        assert!(matches!(frame, DecodedFrame::Control(Packet::PingRequest)));
    }

    #[test]
    fn encode_packet_reuses_buffer_capacity() {
        let mut codec = V5Codec::with_max_packet_size(0);
        let mut buf = BytesMut::with_capacity(64);
        codec
            .encode_packet(Packet::PingRequest, &mut buf)
            .expect("encode PINGREQ into");
        let cap = buf.capacity();
        buf.clear();
        codec
            .encode_packet(Packet::PingRequest, &mut buf)
            .expect("reuse encode PINGREQ");
        assert!(buf.capacity() >= cap);
    }
}
