//! Canonical PUBLISH model and delivery-time wire metadata.
//!
//! Type ladder: inbound wire decode → [`PublishPacket`] (with [`PublishPacketMeta`] and optional
//! `wire_topic_alias`) → session policy → [`PublishMessage`] handoff → outbound
//! [`encode_publish`](crate::codec::v5::encode_publish) via [`PublishDelivery`].
//!
//! [`PublishPacket`] is the decoded MQTT PUBLISH control packet. [`PublishMessage`] is the
//! transferable application message (topic, payload, v5 properties). [`PublishDelivery`]
//! carries outbound wire overrides (`dup`, `retain`, `qos`, `packet_id`).
//!
//! Topic Alias is **not** part of [`PublishProperties`]: it is connection-local wire state
//! surfaced on [`PublishPacket::wire_topic_alias`] until session policy validates or resolves it.

use std::num::{NonZeroU16, NonZeroU32};

use bytes::Bytes;
use bytestring::ByteString;

use crate::codec::types::QoS;

/// Transferable MQTT v5 PUBLISH properties (no Topic Alias, no Subscription Identifiers).
#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct PublishProperties {
    pub correlation_data: Option<Bytes>,
    pub message_expiry_interval: Option<NonZeroU32>,
    pub content_type: Option<ByteString>,
    pub user_properties: Vec<(ByteString, ByteString)>,
    pub is_utf8_payload: bool,
    pub response_topic: Option<ByteString>,
}

impl PublishProperties {
    /// True when the v5 property block encodes as Property Length zero.
    pub(crate) fn is_wire_empty(&self) -> bool {
        self.correlation_data.is_none()
            && self.message_expiry_interval.is_none()
            && self.content_type.is_none()
            && !self.is_utf8_payload
            && self.response_topic.is_none()
            && self.user_properties.is_empty()
    }
}

/// Application PUBLISH message: topic, payload, and transferable v5 properties.
#[derive(PartialEq, Eq, Clone)]
pub struct PublishMessage {
    pub topic: ByteString,
    pub payload: Bytes,
    pub properties: PublishProperties,
}

impl std::fmt::Debug for PublishMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PublishMessage")
            .field("topic", &self.topic)
            .field("payload", &"<REDACTED>")
            .field("properties", &self.properties)
            .finish()
    }
}

/// Inbound PUBLISH wire metadata from the fixed header and variable header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishPacketMeta {
    pub dup: bool,
    pub retain: bool,
    pub qos: QoS,
    pub packet_id: Option<NonZeroU16>,
}

/// Decoded MQTT PUBLISH control packet.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct PublishPacket {
    pub message: PublishMessage,
    pub meta: PublishPacketMeta,
    /// Decoded v5 Topic Alias — connection-local wire state, not part of [`PublishProperties`].
    pub wire_topic_alias: Option<NonZeroU16>,
}

impl PublishPacket {
    /// Build a [`PublishMessage`] after inbound policy accepts the packet.
    pub fn into_message(self) -> PublishMessage {
        self.message
    }
}

/// Outbound PUBLISH delivery overrides for a single send.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishDelivery {
    pub dup: bool,
    pub retain: bool,
    pub qos: QoS,
    pub packet_id: Option<NonZeroU16>,
}
