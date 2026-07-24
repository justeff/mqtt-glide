//! Shared handoff payload helpers for fair peer ↔ mqtt-glide comparison.
//!
//! Peer crates build a [`BenchHandoff`] before flume (owned topic/payload/user-props).
//! mqtt-glide pushes production [`PublishMessage`] — consumers must only *touch* that value,
//! not re-convert it into [`BenchHandoff`] (that would add extra post-handoff work only on glide).

use bytes::Bytes;
use mqtt_glide::PublishMessage;
use rumqttc::mqttbytes::v5::Publish as RumqttcPublish;

/// Application-facing handoff record built on peer bench paths.
#[derive(Debug, Clone)]
pub struct BenchHandoff {
    /// Topic bytes (MQTT topics are UTF-8).
    pub topic: Bytes,
    /// Payload bytes.
    pub payload: Bytes,
    /// User properties copied into owned strings.
    pub user_properties: Vec<(String, String)>,
}

impl BenchHandoff {
    /// Builds a handoff from rumqttc's inbound publish.
    pub fn from_rumqttc(publish: &RumqttcPublish) -> Self {
        let user_properties = publish
            .properties
            .as_ref()
            .map(|props| props.user_properties.clone())
            .unwrap_or_default();
        Self {
            topic: publish.topic.clone(),
            payload: publish.payload.clone(),
            user_properties,
        }
    }

    /// Builds a handoff from the LabOverWire `mqtt5` crate inbound [`mqtt5::Message`].
    pub fn from_peer_mqtt5(message: &mqtt5::Message) -> Self {
        Self {
            topic: Bytes::copy_from_slice(message.topic.as_bytes()),
            payload: Bytes::copy_from_slice(&message.payload),
            user_properties: message.properties.user_properties.clone(),
        }
    }

    /// Prevents the optimizer from eliding the handoff payload.
    pub fn touch(&self) {
        std::hint::black_box(self.topic.len());
        std::hint::black_box(self.payload.len());
        std::hint::black_box(self.user_properties.len());
    }
}

/// Touches mqtt-glide [`PublishMessage`] fields without re-encoding (fair vs [`BenchHandoff::touch`]).
pub fn touch_publish(message: &PublishMessage) {
    std::hint::black_box(message.topic.len());
    std::hint::black_box(message.payload.len());
    std::hint::black_box(message.properties.user_properties.len());
}
