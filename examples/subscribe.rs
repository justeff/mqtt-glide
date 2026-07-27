//! Subscribe and receive via `inbound_sink` (enqueue-before-ack).
//!
//! Shows [`Subscription`] options, a bounded receive loop, `unsubscribe`, and `shutdown`.
//! Inbound QoS 1 is `PUBACK`ed only after the flume sink accepts the message.
//! For tiny sync work on the read task, see [`on_inbound.rs`](on_inbound.rs).
//!
//! ```shell
//! MQTT_GLIDE_BROKER=mqtt://127.0.0.1:1883 cargo run --example subscribe
//! ```

use std::time::Duration;

use mqtt_glide::{ClientBuilder, QoS, Subscription};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let broker = std::env::var("MQTT_GLIDE_BROKER").unwrap_or_else(|_| "mqtt://127.0.0.1:1883".into());
    let (tx, rx) = flume::bounded(1024);

    let client = ClientBuilder::new(&broker)?.inbound_sink(tx).connect().await?;

    let filter = "demo/glide/subscribe";
    let reasons = client
        .subscribe([
            // Tuple form still works; Subscription adds No Local / retain options.
            Subscription::new(filter, QoS::AtLeastOnce).retain_as_published(true),
            Subscription::new("demo/glide/cmd", QoS::AtLeastOnce).no_local(true),
        ])
        .await?;
    // Per-filter SUBACK reason codes — never collapsed to a bool.
    println!("SUBACK: {reasons:?}");

    // Self-publish so the example works without an external publisher.
    client
        .publish_with()
        .topic(filter)
        .payload("ping")
        .qos(QoS::AtLeastOnce)
        .send()
        .await?;

    // App-level bound only: ack/connect timeouts do not cover waiting on the inbound sink.
    let msg = tokio::time::timeout(Duration::from_secs(5), rx.recv_async())
        .await
        .map_err(|_| "timed out waiting for inbound publish")??;
    println!("{} => {} bytes", msg.topic, msg.payload.len());

    let unsub = client.unsubscribe([filter, "demo/glide/cmd"]).await?;
    println!("UNSUBACK: {unsub:?}");

    client.shutdown().await?;
    Ok(())
}
