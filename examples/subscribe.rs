//! Subscribe and receive via `inbound_sink` (enqueue-before-ack).
//!
//! Inbound QoS 1 is `PUBACK`ed only after the flume sink accepts the message.
//! For tiny sync work on the read task, see [`on_inbound.rs`](on_inbound.rs).
//!
//! ```shell
//! MQTT_GLIDE_BROKER=mqtt://127.0.0.1:1883 cargo run --example subscribe
//! ```

use mqtt_glide::{ClientBuilder, QoS};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let broker = std::env::var("MQTT_GLIDE_BROKER").unwrap_or_else(|_| "mqtt://127.0.0.1:1883".into());
    let (tx, rx) = flume::bounded(1024);

    let client = ClientBuilder::new(&broker)?.inbound_sink(tx).connect().await?;

    let reasons = client.subscribe([("sensors/+/temp", QoS::AtLeastOnce)]).await?;
    // Per-filter SUBACK reason codes — never collapsed to a bool.
    println!("SUBACK: {reasons:?}");

    while let Ok(msg) = rx.recv_async().await {
        println!("{} => {} bytes", msg.topic, msg.payload.len());
    }

    Ok(())
}
