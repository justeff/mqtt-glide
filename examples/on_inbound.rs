//! Inline inbound handler via [`ClientBuilder::on_inbound`](mqtt_glide::ClientBuilder::on_inbound).
//!
//! Pairs with [`discard_inbound`](mqtt_glide::ClientBuilder::discard_inbound): both configure
//! inbound delivery without an app queue. The closure runs on the connection **read task** —
//! keep it short (counters, filters, metrics). For worker-style processing use
//! [`inbound_sink`](mqtt_glide::ClientBuilder::inbound_sink) instead — see [`subscribe.rs`](subscribe.rs).
//!
//! ```shell
//! MQTT_GLIDE_BROKER=mqtt://127.0.0.1:1883 cargo run --example on_inbound
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use mqtt_glide::{ClientBuilder, InboundMessage, QoS};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let broker = std::env::var("MQTT_GLIDE_BROKER").unwrap_or_else(|_| "mqtt://127.0.0.1:1883".into());
    let seen = Arc::new(AtomicU64::new(0));

    let client = ClientBuilder::new(&broker)?
        .on_inbound({
            let seen = Arc::clone(&seen);
            move |msg: InboundMessage| {
                // Read-task: no await, no heavy work.
                seen.fetch_add(1, Ordering::Relaxed);
                println!("{} => {} bytes", msg.topic, msg.payload.len());
            }
        })
        .connect()
        .await?;

    client.subscribe([("sensors/+/temp", QoS::AtLeastOnce)]).await?;

    println!("listening (on_inbound); stop the process to exit");
    // Keep the connection alive; handler runs on the read task as messages arrive.
    std::future::pending::<Result<(), Box<dyn std::error::Error>>>().await
}
