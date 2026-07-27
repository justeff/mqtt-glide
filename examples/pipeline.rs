//! Pipelined QoS 1 publish via [`mqtt_glide::PublishWindow`].
//!
//! Prefer this when topics/QoS vary or you need per-message fluent builders.
//! For a same-QoS/retain burst of ready [`PublishMessage`]s, see `publish_batch` in
//! [`publish.rs`](publish.rs) — less flexible, less boilerplate.
//!
//! ```shell
//! MQTT_GLIDE_BROKER=mqtt://127.0.0.1:1883 cargo run --example pipeline
//! ```

use std::num::NonZeroUsize;

use mqtt_glide::{ClientBuilder, QoS};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let broker = std::env::var("MQTT_GLIDE_BROKER").unwrap_or_else(|_| "mqtt://127.0.0.1:1883".into());

    let client = ClientBuilder::new(&broker)?.discard_inbound().connect().await?;

    let mut pipe = client.publish_window(NonZeroUsize::new(64).expect("window"));
    for i in 0..1_000 {
        pipe.publish_with()
            .topic(format!("out/{i}"))
            .payload("x")
            .qos(QoS::AtLeastOnce)
            .push_retry()
            .await?;
    }
    pipe.drain().await?;

    client.shutdown().await?;
    Ok(())
}
