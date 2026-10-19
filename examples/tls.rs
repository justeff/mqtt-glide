//! TLS connect via `mqtts://`: the OS trust store by default, or a [`TlsConfig`] with a
//! private CA, a client certificate (mutual TLS) and ALPN.
//!
//! Raise `connect_timeout` for slower TLS dials; `ack_timeout` is shown at the default (5s). It
//! bounds the wait for PUBACK / SUBACK / UNSUBACK and for room in the command channel, not
//! waits on your own channels.
//!
//! Without further settings the broker certificate **MUST** chain to the OS trust store. On
//! `test.mosquitto.org`, port **8886** has a Let's Encrypt certificate:
//!
//! ```shell
//! MQTT_GLIDE_BROKER=mqtts://test.mosquitto.org:8886 cargo run --example tls
//! ```
//!
//! A broker on a private CA, optionally asking for a client certificate and ALPN (all PEM files;
//! re-read on every reconnect, so rotated certificates are picked up):
//!
//! ```shell
//! MQTT_GLIDE_BROKER=mqtts://broker.internal:8883 \
//! MQTT_GLIDE_CA=ca.pem MQTT_GLIDE_CLIENT_CERT=client.pem MQTT_GLIDE_CLIENT_KEY=client.key \
//! MQTT_GLIDE_ALPN=x-amzn-mqtt-ca cargo run --example tls
//! ```

use std::time::Duration;

use mqtt_glide::{ClientBuilder, QoS, TlsConfig};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let broker = std::env::var("MQTT_GLIDE_BROKER").unwrap_or_else(|_| "mqtts://test.mosquitto.org:8886".into());
    let var = |name| std::env::var(name).ok();

    let mut tls = match var("MQTT_GLIDE_CA") {
        Some(ca) => TlsConfig::ca_pem_file(ca),
        None => TlsConfig::system_roots(),
    };
    if let (Some(cert), Some(key)) = (var("MQTT_GLIDE_CLIENT_CERT"), var("MQTT_GLIDE_CLIENT_KEY")) {
        tls = tls.client_auth_pem_files(cert, key);
    }
    if let Some(alpn) = var("MQTT_GLIDE_ALPN") {
        tls = tls.alpn(alpn.split(','))?;
    }

    let client = ClientBuilder::new(&broker)?
        .options(|o| {
            o.tls(tls)
                .connect_timeout(Duration::from_secs(10))
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
