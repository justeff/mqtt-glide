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
    use bytestring::ByteString;

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
