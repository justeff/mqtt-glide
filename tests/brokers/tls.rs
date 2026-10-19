//! Mutual TLS against Mosquitto (OpenSSL on the broker side) with the test CA.

use std::path::{Path, PathBuf};
use std::time::Duration;

use mqtt_glide::{ClientBuilder, ConnectError, QoS, Subscription, TlsConfig, TransportError};
use tokio::sync::mpsc;
use tokio::time::timeout;

use crate::helpers::start_mosquitto_tls;

fn cert_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/certs").join(name)
}

/// A client with the CA and a client certificate subscribes and receives its own publish.
#[tokio::test]
async fn mosquitto_mutual_tls_with_a_private_ca() {
    let Some((_container, port)) = start_mosquitto_tls().await else {
        return;
    };
    let url = format!("mqtts://localhost:{port}");
    let tls = TlsConfig::ca_pem_file(cert_path("ca.pem"))
        .client_auth_pem_files(cert_path("client.pem"), cert_path("client.key"));
    let (tx, mut inbound) = mpsc::channel(8);
    let client = ClientBuilder::new(&url)
        .expect("url")
        .options(|o| o.tls(tls).connect_timeout(Duration::from_secs(10)))
        .inbound_sink(tx)
        .connect()
        .await
        .expect("mutual TLS connect");
    client
        .subscribe(vec![Subscription::new("tls/echo", QoS::AtLeastOnce)])
        .await
        .expect("subscribe");
    client
        .publish("tls/echo", "over mtls")
        .qos(QoS::AtLeastOnce)
        .await
        .expect("publish");
    let message = timeout(Duration::from_secs(10), inbound.recv())
        .await
        .expect("message in time")
        .expect("inbound open");
    assert_eq!(&*message.payload, b"over mtls");
    client.shutdown().await.expect("shutdown");

    // Without a client certificate Mosquitto ends the handshake.
    let refused = ClientBuilder::new(&url)
        .expect("url")
        .options(|o| o.tls(TlsConfig::ca_pem_file(cert_path("ca.pem"))))
        .discard_inbound()
        .connect()
        .await;
    assert!(
        matches!(
            refused,
            Err(ConnectError::Transport(
                TransportError::Tls(_) | TransportError::Connection(_)
            ))
        ),
        "expected a transport error, got {:?}",
        refused.err()
    );
}
