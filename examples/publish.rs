//! Fluent publish paths, MQTT 5 properties, `publish_batch`, shared filter, and Will Message.
//!
//! Prefer [`MqttClient::publish_with`](mqtt_glide::MqttClient::publish_with) for one-shots.
//! Use [`PublishMessage::new`](mqtt_glide::PublishMessage::new) when the message is built
//! ahead of time (workers, reuse). Use [`publish_batch`](mqtt_glide::MqttClient::publish_batch)
//! when every message shares the same QoS/retain — otherwise prefer [`pipeline.rs`](pipeline.rs).
//! Auth / reconnect hooks: see [`hooks.rs`](hooks.rs).
//!
//! ```shell
//! MQTT_GLIDE_BROKER=mqtt://127.0.0.1:1883 cargo run --example publish
//! ```

use std::num::NonZeroUsize;

use mqtt_glide::{ClientBuilder, PublishMessage, PublishProperties, QoS, shared_filter};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let broker = std::env::var("MQTT_GLIDE_BROKER").unwrap_or_else(|_| "mqtt://127.0.0.1:1883".into());
    let (tx, _rx) = flume::bounded(256);

    let client = ClientBuilder::new(&broker)?
        // Graceful `shutdown()` → Normal DISCONNECT → broker deletes the Will.
        .will("demo/glide/lwt", "offline")
        .will_qos(QoS::AtLeastOnce)
        .will_retain(true)
        .inbound_sink(tx)
        .connect()
        .await?;

    // Fluent one-shot (default path).
    client
        .publish_with()
        .topic("sensors/room1/temp")
        .payload("22.5")
        .qos(QoS::AtLeastOnce)
        .message_expiry(3600)
        .user_property("unit", "celsius")
        .send()
        .await?;

    // Pre-built message + power `publish` (same Into contract as publish_with payload).
    let message = PublishMessage::new("sensors/room1/humidity", "48").properties(
        PublishProperties::default()
            .content_type("text/plain")
            .user_property("unit", "percent")
            .message_expiry(3600),
    );
    client.publish(message, QoS::AtLeastOnce, false).await?;

    // Same QoS/retain for the whole burst — simpler than a manual window loop.
    let batch = [
        PublishMessage::new("sensors/room1/batch/0", "a"),
        PublishMessage::new("sensors/room1/batch/1", "b"),
        PublishMessage::new("sensors/room1/batch/2", "c"),
    ];
    client
        .publish_batch(batch, QoS::AtLeastOnce, false, NonZeroUsize::new(8).expect("window"))
        .await?;

    let filter = shared_filter("workers", "jobs/inbox")?;
    client.subscribe([(filter, QoS::AtLeastOnce)]).await?;

    client.shutdown().await?;
    Ok(())
}
