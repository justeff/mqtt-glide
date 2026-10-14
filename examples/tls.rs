//! TLS connect via `mqtts://` (rustls + OS trust store / [`TlsConfig::SystemRoots`](mqtt_glide::TlsConfig::SystemRoots)).
//!
//! Raise `connect_timeout` for slower TLS dials; `ack_timeout` is shown at the default (5s) and
//! covers PUBACK / SUBACK / UNSUBACK only — not app-level channel waits.
//!
//! The broker certificate **MUST** chain to the OS trust store. A local Mosquitto with a
//! self-signed (or private-CA) cert will fail unless that CA is trusted by the OS. On
//! `test.mosquitto.org`, use port **8886** (Let's Encrypt); port **8883** uses a private CA
//! and will not verify with system roots.
//!
//! ```shell
//! MQTT_GLIDE_BROKER=mqtts://test.mosquitto.org:8886 cargo run --example tls
//! ```

use std::time::Duration;

use mqtt_glide::{ClientBuilder, QoS};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let broker = std::env::var("MQTT_GLIDE_BROKER").unwrap_or_else(|_| "mqtts://test.mosquitto.org:8886".into());

    let client = ClientBuilder::new(&broker)?
        .options(|o| {
            o.connect_timeout(Duration::from_secs(10))
                .ack_timeout(Duration::from_secs(5))
        })
        .discard_inbound()
        .connect()
        .await?;

    client
        .publish("demo/glide/tls", "hello over tls")
        .qos(QoS::AtLeastOnce)
        .send()
        .await?;

    println!("published over TLS to {broker}");
    client.shutdown().await?;
    Ok(())
}
