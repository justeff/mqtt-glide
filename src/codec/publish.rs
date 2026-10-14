//! Canonical PUBLISH model and delivery-time wire metadata (§3.3).
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
//! (§3.3.2.3.4) surfaced on [`PublishPacket::wire_topic_alias`] until session policy
//! validates or resolves it.

use std::num::NonZeroU16;

use bytes::Bytes;
use bytestring::ByteString;

use crate::codec::types::QoS;
use crate::codec::v5::UserProperties;

/// Transferable MQTT v5 PUBLISH properties (no Topic Alias, no Subscription Identifiers).
#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct PublishProperties {
    /// Correlation Data (§3.3.2.3.6).
    pub correlation_data: Option<Bytes>,
    /// Message Expiry Interval (§3.3.2.3.3).
    pub message_expiry_interval: Option<u32>,
    /// Content Type (§3.3.2.3.9).
    pub content_type: Option<ByteString>,
    /// User Property (§3.3.2.3.7).
    pub user_properties: UserProperties,
    /// Payload Format Indicator (§3.3.2.3.2); `true` = UTF-8 payload.
    pub payload_format_indicator: bool,
    /// Response Topic (§3.3.2.3.5).
    pub response_topic: Option<ByteString>,
}

impl PublishProperties {
    /// Appends one User Property (`name`/`value` accept any [`Into<ByteString>`]).
    #[inline]
    pub fn user_property(mut self, name: impl Into<ByteString>, value: impl Into<ByteString>) -> Self {
        self.user_properties
            .push(crate::codec::v5::UserProperty::from((name, value)));
        self
    }

    /// Sets Content Type (§3.3.2.3.9).
    #[inline]
    pub fn content_type(mut self, content_type: impl Into<ByteString>) -> Self {
        self.content_type = Some(content_type.into());
        self
    }

    /// Sets Response Topic (§3.3.2.3.5).
    #[inline]
    pub fn response_topic(mut self, response_topic: impl Into<ByteString>) -> Self {
        self.response_topic = Some(response_topic.into());
        self
    }

    /// Sets Message Expiry Interval in **seconds** (§3.3.2.3.3).
    ///
    /// `0` clears expiry (property omitted).
    #[inline]
    pub fn message_expiry(mut self, seconds: u32) -> Self {
        self.message_expiry_interval = (seconds != 0).then_some(seconds);
        self
    }

    /// Sets Correlation Data (§3.3.2.3.6).
    #[inline]
    pub fn correlation_data(mut self, data: impl Into<Bytes>) -> Self {
        self.correlation_data = Some(data.into());
        self
    }

    /// Sets Payload Format Indicator (§3.3.2.3.2); `true` means UTF-8 payload.
    #[inline]
    pub fn payload_format_indicator(mut self, is_utf8: bool) -> Self {
        self.payload_format_indicator = is_utf8;
        self
    }

    /// True when the v5 property block encodes as Property Length zero.
    pub(crate) fn is_wire_empty(&self) -> bool {
        self.correlation_data.is_none()
            && self.message_expiry_interval.is_none()
            && self.content_type.is_none()
            && !self.payload_format_indicator
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

impl PublishMessage {
    /// Builds a message with empty [`PublishProperties`].
    ///
    /// `topic` accepts any [`Into<ByteString>`].
    ///
    /// `payload` accepts any [`Into<Bytes>`]:
    /// - `&'static str` / `&'static [u8]`: [`Bytes::from_static`] (no copy) — prefer
    ///   plain `"..."` when the payload is just text
    /// - [`Bytes`], `Vec<u8>`, `String`, `Box<[u8]>`: move / take ownership (no extra copy)
    /// - non-`'static` `&[u8]`: not accepted — use `Bytes::copy_from_slice(...)` or
    ///   `.to_vec()` at the call site when a copy is intentional
    ///
    /// Byte-string literals (`b"..."`) are `&[u8; N]` and do **not** implement [`Into<Bytes>`];
    /// use `"..."` for text or [`Bytes::from_static`] for binary.
    ///
    /// Same contract as [`crate::Publish::new`].
    #[inline]
    pub fn new(topic: impl Into<ByteString>, payload: impl Into<Bytes>) -> Self {
        Self {
            topic: topic.into(),
            payload: payload.into(),
            properties: PublishProperties::default(),
        }
    }

    /// Replaces transferable MQTT 5 PUBLISH properties.
    #[inline]
    pub fn properties(mut self, properties: PublishProperties) -> Self {
        self.properties = properties;
        self
    }
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

/// An inbound application message as delivered to the [`InboundSink`](crate::InboundSink).
///
/// Besides topic, payload and properties it carries the delivery flags the application may
/// need: [`Self::retain`] tells a retained message (the broker's stored last value, sent on
/// subscribe) from a live one, [`Self::dup`] marks a possible redelivery (QoS 1), and
/// [`Self::qos`] is the QoS the broker delivered it with. Wire-level details (packet id,
/// topic alias) are not exposed.
#[derive(Clone, PartialEq, Eq)]
pub struct InboundMessage {
    /// Topic Name (aliases already resolved).
    pub topic: ByteString,
    /// Application payload.
    pub payload: Bytes,
    /// Transferable MQTT 5 properties.
    pub properties: PublishProperties,
    /// QoS the broker delivered the message with (at most the subscription's QoS).
    pub qos: QoS,
    /// RETAIN flag: a retained message sent because of a subscription (or forwarded as-is
    /// with Retain As Published).
    pub retain: bool,
    /// DUP flag: the broker may have delivered this message before (QoS 1 redelivery).
    pub dup: bool,
    /// Subscription Identifiers of every subscription (made with
    /// [`MqttClient::subscribe_with_id`](crate::MqttClient::subscribe_with_id)) this message
    /// matched; empty when none carried one. Lets an application route by subscription
    /// instead of re-matching topics.
    pub subscription_ids: Vec<std::num::NonZeroU32>,
}

impl InboundMessage {
    /// Topic, payload and properties as a [`PublishMessage`] (flags dropped).
    #[inline]
    pub fn into_message(self) -> PublishMessage {
        PublishMessage {
            topic: self.topic,
            payload: self.payload,
            properties: self.properties,
        }
    }
}

impl From<PublishPacket> for InboundMessage {
    #[inline]
    fn from(packet: PublishPacket) -> Self {
        let PublishMessage {
            topic,
            payload,
            properties,
        } = packet.message;
        Self {
            topic,
            payload,
            properties,
            qos: packet.meta.qos,
            retain: packet.meta.retain,
            dup: packet.meta.dup,
            subscription_ids: packet.subscription_ids,
        }
    }
}

impl std::fmt::Debug for InboundMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InboundMessage")
            .field("topic", &self.topic)
            .field("payload", &"<REDACTED>")
            .field("properties", &self.properties)
            .field("qos", &self.qos)
            .field("retain", &self.retain)
            .field("dup", &self.dup)
            .field("subscription_ids", &self.subscription_ids)
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
    /// Subscription Identifiers the broker attached (inbound only, §3.3.2.3.8).
    pub subscription_ids: Vec<std::num::NonZeroU32>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_message_new_accepts_str_payload() {
        let msg = PublishMessage::new("out/1", "payload");
        assert_eq!(&*msg.topic, "out/1");
        assert_eq!(msg.payload.as_ref(), b"payload");
        assert_eq!(msg.properties, PublishProperties::default());
    }

    #[test]
    fn publish_properties_fluent_setters() {
        let props = PublishProperties::default()
            .content_type("application/json")
            .response_topic("resp/1")
            .message_expiry(60)
            .correlation_data("corr")
            .payload_format_indicator(true)
            .user_property("k", "v");

        assert_eq!(props.content_type.as_deref(), Some("application/json"));
        assert_eq!(props.response_topic.as_deref(), Some("resp/1"));
        assert_eq!(props.message_expiry_interval, Some(60));
        assert_eq!(props.correlation_data.as_deref(), Some(b"corr".as_slice()));
        assert!(props.payload_format_indicator);
        assert_eq!(props.user_properties.len(), 1);
        assert_eq!(&*props.user_properties[0].name, "k");
        assert_eq!(&*props.user_properties[0].value, "v");

        let cleared = props.message_expiry(0);
        assert!(cleared.message_expiry_interval.is_none());
    }

    #[test]
    fn publish_message_properties_chain() {
        let msg = PublishMessage::new("t", "p").properties(PublishProperties::default().content_type("text/plain"));
        assert_eq!(msg.properties.content_type.as_deref(), Some("text/plain"));
    }
}
