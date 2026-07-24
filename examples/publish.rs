//! Fluent publish, MQTT 5 properties, and a shared subscription filter.
//!
//! ```shell
//! MQTT_GLIDE_BROKER=mqtt://127.0.0.1:1883 cargo run --example publish
//! ```

use std::time::Duration;

use mqtt_glide::{ClientBuilder, QoS, StandardReconnectPolicy, StaticCredentials, shared_filter};
use secrecy::SecretString;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let broker = std::env::var("MQTT_GLIDE_BROKER").unwrap_or_else(|_| "mqtt://127.0.0.1:1883".into());
    let (tx, _rx) = flume::bounded(256);

    let client = ClientBuilder::new(&broker)?
        .credentials(StaticCredentials::new("user", SecretString::from("secret")))
        .reconnect(StandardReconnectPolicy::new(
            Duration::from_secs(5),
            Duration::from_secs(60),
        ))
        .inbound_sink(tx)
        .connect()
        .await?;

    client
        .publish_with()
        .topic("sensors/room1/temp")
        .payload("22.5")
        .qos(QoS::AtLeastOnce)
        .message_expiry(3600)
        .user_property("unit", "celsius")
        .send()
        .await?;

    let filter = shared_filter("workers", "jobs/inbox")?;
    client.subscribe([(filter, QoS::AtLeastOnce)]).await?;

    client.shutdown().await?;
    Ok(())
}
