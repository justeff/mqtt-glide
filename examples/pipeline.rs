//! Pipelined QoS 1 publish via [`mqtt_glide::PublishWindow`].
//!
//! Prefer this when messages are produced over time (a loop, a stream, other work in between)
//! or the loop needs to react to an error before the burst is over. When you already hold all
//! the messages, `publish_batch` in [`publish.rs`](publish.rs) is the shorter form: it runs the
//! same window, and each message carries its own QoS and retain flag in both cases.
//!
//! ```shell
//! MQTT_GLIDE_BROKER=mqtt://127.0.0.1:1883 cargo run --example pipeline
//! ```

use std::num::NonZeroUsize;

use mqtt_glide::{ClientBuilder, Publish, QoS};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let broker = std::env::var("MQTT_GLIDE_BROKER").unwrap_or_else(|_| "mqtt://127.0.0.1:1883".into());

    let client = ClientBuilder::new(&broker)?.discard_inbound().connect().await?;

    let mut pipe = client.publish_window(NonZeroUsize::new(64).expect("window"));
    for i in 0..1_000 {
        pipe.push(Publish::new(format!("out/{i}"), "x").qos(QoS::AtLeastOnce))
            .await?;
    }
    pipe.drain().await?;

    client.shutdown().await?;
    Ok(())
}
