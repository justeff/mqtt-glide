//! TLS against a rustls proxy in front of the in-process `TestBroker`: custom CA, mutual TLS,
//! ALPN, server name override and certificate rotation on disk.
//!
//! The certificates come from `tests/certs/generate.sh`.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mqtt_glide::testing::TestBroker;
use mqtt_glide::{
    ClientBuilder, ConnectError, MqttClient, NoReconnect, QoS, StandardReconnectPolicy, TlsConfig, TransportError,
};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;

const TIMEOUT: Duration = Duration::from_secs(5);

fn cert_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/certs").join(name)
}

fn read(name: &str) -> Vec<u8> {
    std::fs::read(cert_path(name)).expect("test certificate")
}

fn certificate(name: &str) -> CertificateDer<'static> {
    CertificateDer::from_pem_file(cert_path(name)).expect("certificate")
}

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    #[cfg(feature = "tls-rustls-aws-lc")]
    let provider = rustls::crypto::aws_lc_rs::default_provider();
    #[cfg(feature = "tls-rustls-ring")]
    let provider = rustls::crypto::ring::default_provider();
    Arc::new(provider)
}

/// What the proxy saw in the last TLS handshake it completed.
#[derive(Debug, Default, Clone)]
struct Handshake {
    alpn: Option<Vec<u8>>,
    sni: Option<String>,
    client_certificate: Option<CertificateDer<'static>>,
}

/// Terminates TLS with `server.pem` and forwards the plain bytes to a `TestBroker`.
struct TlsProxy {
    port: u16,
    last: Arc<Mutex<Option<Handshake>>>,
    task: tokio::task::JoinHandle<()>,
}

impl TlsProxy {
    /// `require_client_cert`: only clients with a certificate from `ca.pem` get through.
    async fn start(broker: &TestBroker, require_client_cert: bool, alpn: &[&str]) -> Self {
        let chain = vec![certificate("server.pem")];
        let key = PrivateKeyDer::from_pem_file(cert_path("server.key")).expect("server key");
        let builder = rustls::ServerConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()
            .expect("protocol versions");
        let builder = if require_client_cert {
            let mut roots = rustls::RootCertStore::empty();
            roots.add(certificate("ca.pem")).expect("ca");
            let verifier = WebPkiClientVerifier::builder_with_provider(roots.into(), provider())
                .build()
                .expect("client verifier");
            builder.with_client_cert_verifier(verifier)
        } else {
            builder.with_no_client_auth()
        };
        let mut config = builder.with_single_cert(chain, key).expect("server config");
        config.alpn_protocols = alpn.iter().map(|protocol| protocol.as_bytes().to_vec()).collect();
        let acceptor = TlsAcceptor::from(Arc::new(config));

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let broker_port = broker.port();
        let last = Arc::new(Mutex::new(None));
        let seen = Arc::clone(&last);
        let task = tokio::spawn(async move {
            loop {
                let Ok((tcp, _)) = listener.accept().await else { return };
                let acceptor = acceptor.clone();
                let seen = Arc::clone(&seen);
                tokio::spawn(async move {
                    let Ok(mut tls) = acceptor.accept(tcp).await else {
                        return;
                    };
                    let (_, connection) = tls.get_ref();
                    *seen.lock().expect("lock") = Some(Handshake {
                        alpn: connection.alpn_protocol().map(<[u8]>::to_vec),
                        sni: connection.server_name().map(str::to_owned),
                        client_certificate: connection
                            .peer_certificates()
                            .and_then(|chain| chain.first())
                            .map(|cert| cert.clone().into_owned()),
                    });
                    let Ok(mut upstream) = TcpStream::connect(("127.0.0.1", broker_port)).await else {
                        return;
                    };
                    let _ = tokio::io::copy_bidirectional(&mut tls, &mut upstream).await;
                });
            }
        });
        Self { port, last, task }
    }

    fn url(&self, host: &str) -> String {
        format!("mqtts://{host}:{}", self.port)
    }

    fn last(&self) -> Handshake {
        self.last
            .lock()
            .expect("lock")
            .clone()
            .expect("a completed TLS handshake")
    }
}

impl Drop for TlsProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn connect(url: &str, tls: TlsConfig) -> Result<MqttClient, ConnectError> {
    ClientBuilder::new(url)
        .expect("url")
        .options(|o| o.tls(tls).connect_timeout(TIMEOUT))
        .reconnect(NoReconnect)
        .discard_inbound()
        .connect()
        .await
}

/// A QoS 1 publish through the proxy reaches the broker.
async fn assert_publishes(client: &MqttClient, broker: &TestBroker) {
    client
        .publish("tls/test", "hello")
        .qos(QoS::AtLeastOnce)
        .await
        .expect("publish over TLS");
    assert_eq!(broker.wait_for_publishes(1, TIMEOUT).await.len(), 1);
}

fn tls_error(result: Result<MqttClient, ConnectError>) -> String {
    match result {
        Err(ConnectError::Transport(TransportError::Tls(message))) => message,
        Err(other) => panic!("expected a TLS error, got {other:?}"),
        Ok(_) => panic!("expected a TLS error, the client connected"),
    }
}

#[tokio::test]
async fn a_broker_on_a_private_ca_is_trusted_with_that_ca() {
    let broker = TestBroker::start().await.expect("broker");
    let proxy = TlsProxy::start(&broker, false, &[]).await;

    let client = connect(&proxy.url("localhost"), TlsConfig::ca_pem(read("ca.pem")).expect("ca"))
        .await
        .expect("connect");
    assert_publishes(&client, &broker).await;
    assert_eq!(proxy.last().sni.as_deref(), Some("localhost"));
    client.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_broker_on_a_private_ca_is_refused_with_system_roots_or_another_ca() {
    let broker = TestBroker::start().await.expect("broker");
    let proxy = TlsProxy::start(&broker, false, &[]).await;

    let message = tls_error(connect(&proxy.url("localhost"), TlsConfig::system_roots()).await);
    assert!(message.contains("UnknownIssuer"), "{message}");
    let other = TlsConfig::ca_pem(read("other-ca.pem")).expect("other ca");
    let message = tls_error(connect(&proxy.url("localhost"), other).await);
    assert!(message.contains("UnknownIssuer"), "{message}");
    assert_eq!(broker.connect_count(), 0);
}

#[tokio::test]
async fn the_private_ca_works_next_to_the_system_roots() {
    let broker = TestBroker::start().await.expect("broker");
    let proxy = TlsProxy::start(&broker, false, &[]).await;

    let tls = TlsConfig::ca_pem_file(cert_path("ca.pem")).with_system_roots();
    let client = connect(&proxy.url("localhost"), tls).await.expect("connect");
    assert_publishes(&client, &broker).await;
    client.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn mutual_tls_presents_the_client_certificate() {
    let broker = TestBroker::start().await.expect("broker");
    let proxy = TlsProxy::start(&broker, true, &[]).await;

    let tls = TlsConfig::ca_pem(read("ca.pem"))
        .and_then(|tls| tls.client_auth_pem(read("client.pem"), read("client.key")))
        .expect("tls");
    let client = connect(&proxy.url("localhost"), tls).await.expect("connect");
    assert_publishes(&client, &broker).await;
    assert_eq!(proxy.last().client_certificate, Some(certificate("client.pem")));
    client.shutdown().await.expect("shutdown");
}

/// Under TLS 1.3 the client finishes its handshake before the broker has checked the client
/// certificate, so the refusal can arrive as a TLS alert while waiting for CONNACK.
#[tokio::test]
async fn mutual_tls_without_a_client_certificate_is_refused() {
    let broker = TestBroker::start().await.expect("broker");
    let proxy = TlsProxy::start(&broker, true, &[]).await;

    let result = connect(&proxy.url("localhost"), TlsConfig::ca_pem(read("ca.pem")).expect("ca")).await;
    match result {
        Err(ConnectError::Transport(TransportError::Tls(_) | TransportError::Connection(_))) => {}
        Err(other) => panic!("expected a transport error, got {other:?}"),
        Ok(_) => panic!("the broker requires a client certificate"),
    }
    assert_eq!(broker.connect_count(), 0);
}

#[tokio::test]
async fn alpn_offers_the_configured_protocols() {
    let broker = TestBroker::start().await.expect("broker");
    let proxy = TlsProxy::start(&broker, false, &["mqtt", "x-amzn-mqtt-ca"]).await;

    let tls = TlsConfig::ca_pem(read("ca.pem"))
        .and_then(|tls| tls.alpn(["x-amzn-mqtt-ca"]))
        .expect("tls");
    let client = connect(&proxy.url("localhost"), tls).await.expect("connect");
    assert_eq!(proxy.last().alpn.as_deref(), Some(&b"x-amzn-mqtt-ca"[..]));
    client.shutdown().await.expect("shutdown");

    let client = connect(&proxy.url("localhost"), TlsConfig::ca_pem(read("ca.pem")).expect("ca"))
        .await
        .expect("connect");
    assert_eq!(proxy.last().alpn, None, "no ALPN unless configured");
    client.shutdown().await.expect("shutdown");
}

/// The override names the certificate to expect and the SNI to send; the URL still names where
/// to connect.
#[tokio::test]
async fn server_name_overrides_the_url_host_for_verification_and_sni() {
    let broker = TestBroker::start().await.expect("broker");
    let proxy = TlsProxy::start(&broker, false, &[]).await;
    let ca = TlsConfig::ca_pem(read("ca.pem")).expect("ca");

    let tls = ca.clone().server_name("broker.internal").expect("name");
    let client = connect(&proxy.url("127.0.0.1"), tls).await.expect("connect");
    assert_eq!(proxy.last().sni.as_deref(), Some("broker.internal"));
    client.shutdown().await.expect("shutdown");

    let tls = ca.server_name("wrong.internal").expect("name");
    let message = tls_error(connect(&proxy.url("127.0.0.1"), tls).await);
    assert!(message.contains("not valid for name"), "{message}");
}

/// A renewed client certificate on disk is presented from the next reconnect on.
#[tokio::test]
async fn certificate_files_are_read_again_on_reconnect() {
    let broker = TestBroker::start().await.expect("broker");
    let proxy = TlsProxy::start(&broker, true, &[]).await;
    let dir = std::env::temp_dir().join(format!("mqtt-glide-tls-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let (cert, key) = (dir.join("tls.crt"), dir.join("tls.key"));
    std::fs::copy(cert_path("client.pem"), &cert).expect("copy cert");
    std::fs::copy(cert_path("client.key"), &key).expect("copy key");

    let tls = TlsConfig::ca_pem_file(cert_path("ca.pem")).client_auth_pem_files(&cert, &key);
    let client = ClientBuilder::new(proxy.url("localhost"))
        .expect("url")
        .options(|o| o.tls(tls).connect_timeout(TIMEOUT))
        .reconnect(StandardReconnectPolicy::new(
            Duration::from_millis(20),
            Duration::from_millis(200),
        ))
        .discard_inbound()
        .connect()
        .await
        .expect("connect");
    assert_eq!(proxy.last().client_certificate, Some(certificate("client.pem")));

    std::fs::copy(cert_path("client2.pem"), &cert).expect("rotate cert");
    std::fs::copy(cert_path("client2.key"), &key).expect("rotate key");
    broker.drop_connections();
    assert!(broker.wait_for_connects(2, TIMEOUT).await, "reconnected");
    assert_eq!(proxy.last().client_certificate, Some(certificate("client2.pem")));

    client.shutdown().await.expect("shutdown");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn a_missing_ca_file_fails_the_connect_naming_the_file() {
    let broker = TestBroker::start().await.expect("broker");
    let proxy = TlsProxy::start(&broker, false, &[]).await;

    let missing = cert_path("does-not-exist.pem");
    let message = tls_error(connect(&proxy.url("localhost"), TlsConfig::ca_pem_file(&missing)).await);
    assert!(message.contains("does-not-exist.pem"), "{message}");
}
