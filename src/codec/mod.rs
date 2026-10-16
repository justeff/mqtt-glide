//! MQTT 5 wire codec.
//!
//! # Free functions vs methods
//!
//! Control packets go through [`V5Codec`]: `V5Codec::encode_packet` for a [`Packet`] and
//! `V5Codec::decode_frame` for any inbound frame (the per-packet `decode` functions are
//! crate-private).
//!
//! [`encode_publish`] stays free: the hot-path inputs are [`PublishMessage`] +
//! [`PublishDelivery`] + a buffer, not a [`Packet`] and not codec state.

#[macro_use]
mod utils;

pub mod error;
pub(crate) mod properties;
pub mod publish;
pub mod types;
pub mod v5;

/// Default Maximum Packet Size (1 MiB) the client advertises in CONNECT and enforces on inbound
/// packets; see `ConnectOptions::max_packet_size`.
pub const DEFAULT_MAX_PACKET_SIZE: u32 = 1024 * 1024;

pub use error::{DecodeError, EncodeError, UnknownCode};
#[cfg_attr(
    not(feature = "__internals"),
    allow(unused_imports, reason = "used by tests and benches")
)]
pub use publish::{
    InboundMessage, PublishDelivery, PublishMessage, PublishPacket, PublishPacketMeta, PublishProperties,
};
pub use types::QoS;

pub(crate) use utils::topic_filter_bytes_invalid;

pub(crate) use v5::encode_publish_ack;
#[cfg_attr(
    not(feature = "__internals"),
    allow(unused_imports, reason = "used by tests and benches")
)]
pub use v5::{
    Auth, AuthReasonCode, Connect, ConnectAck, ConnectAckReason, DecodedFrame, Disconnect, DisconnectReasonCode,
    Packet, PubRelComp, PubRelCompReason, PublishAck, PublishAckReason, RECEIVE_MAX_DEFAULT, RetainHandling, Subscribe,
    SubscribeAck, SubscribeAckReason, SubscriptionOptions, Unsubscribe, UnsubscribeAck, UnsubscribeAckReason,
    UserProperties, UserProperty, WillMessage, encode_publish,
};
pub use v5::{OutboundTopicAlias, encode_publish_with_alias};

/// MQTT v5.0 wire codec (`tokio_util::codec::Decoder` / `Encoder` for [`Packet`]).
pub type V5Codec = v5::V5Codec;

#[cfg(test)]
mod facade_tests {
    use super::*;
    use bytes::BytesMut;
    use bytestring::ByteString;

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

    #[test]
    fn undefined_code_byte_is_reported_and_malforms_a_packet() {
        let error = QoS::try_from(3).expect_err("QoS 3 is undefined");
        assert_eq!(error.code(), 3);
        assert!(matches!(DecodeError::from(error), DecodeError::MalformedPacket(_)));
    }

    #[test]
    fn user_property_splits_into_name_and_value() {
        let (name, value): (ByteString, ByteString) = UserProperty::from(("unit", "°C")).into();
        assert_eq!((name.as_ref(), value.as_ref()), ("unit", "°C"));
    }
}
