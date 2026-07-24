//! TCP/TLS dial and framed CONNECT/CONNACK handshake.

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use rustls::RootCertStore;
use rustls::pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncWrite, ReadHalf, WriteHalf, split};
use tokio::net::TcpStream;
use tokio::time::timeout_at;
use tokio_rustls::{TlsConnector, client::TlsStream};
use tokio_util::codec::{FramedRead, FramedWrite};

use super::handshake::{
    ConnectCredentials, SessionParams, apply_connack, build_connect, distinct_v5_codecs_for_framed_halves,
    on_auth_failure, resolve_client_id_str,
};
use crate::client::SharedCredentials;
use crate::codec::{ConnectAck, DecodedFrame, Packet, V5Codec};
use crate::error::{ConnectionOutcome, ProtocolError, TransportError};
use crate::options::ConnectOptions;

type TlsIo = TlsStream<TcpStream>;

/// CONNECT/CONNACK on already-split transport halves.
pub(crate) async fn establish_session_on<R, W>(
    read_half: R,
    write_half: W,
    options: &ConnectOptions,
    credentials: &SharedCredentials,
    deadline: Instant,
) -> Result<(FramedRead<R, V5Codec>, FramedWrite<W, V5Codec>, SessionParams), ConnectionOutcome>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let (read_codec, write_codec) = distinct_v5_codecs_for_framed_halves();
    let mut framed_read = FramedRead::new(read_half, read_codec);
    let mut framed_write = FramedWrite::new(write_half, write_codec);

    let (username, password) = match credentials.credentials().await {
        Ok(pair) => pair,
        Err(error) => {
            let outcome = ConnectionOutcome::Auth(error);
            return Err(on_auth_failure(credentials, outcome).await);
        }
    };

    let connect_credentials = ConnectCredentials { username, password };
    let client_id = resolve_client_id_str(options);
    let connect = build_connect(options, &client_id, &connect_credentials);

    framed_write
        .send(Packet::Connect(Box::new(connect)))
        .await
        .map_err(map_connect_send_error)?;

    let ack = read_connack(&mut framed_read, deadline, options.connect_timeout).await?;
    match apply_connack(options.keep_alive, &ack) {
        Ok(session) => {
            apply_session_limits_to_codecs(&mut framed_read, &mut framed_write, &session);
            Ok((framed_read, framed_write, session))
        }
        Err(outcome) => Err(on_auth_failure(credentials, outcome).await),
    }
}

/// Applies broker Maximum Packet Size from CONNACK onto both framed codecs (0 = unlimited).
fn apply_session_limits_to_codecs<R, W>(
    framed_read: &mut FramedRead<R, V5Codec>,
    framed_write: &mut FramedWrite<W, V5Codec>,
    session: &SessionParams,
) {
    if let Some(max_packet_size) = session.max_packet_size {
        framed_read.decoder_mut().set_max_inbound_size(max_packet_size);
        framed_write.encoder_mut().set_max_outbound_size(max_packet_size);
    }
    framed_read.decoder_mut().set_retain_available(session.retain_available);
    framed_write
        .encoder_mut()
        .set_retain_available(session.retain_available);
}

fn map_connect_send_error(error: crate::codec::EncodeError) -> ConnectionOutcome {
    match error {
        crate::codec::EncodeError::Io(message) => {
            ConnectionOutcome::Transport(TransportError::Connection(message.to_string()))
        }
        other => ConnectionOutcome::Protocol(ProtocolError::Violation(other.to_string())),
    }
}

pub(crate) async fn open_tcp(options: &ConnectOptions, deadline: Instant) -> Result<TcpStream, ConnectionOutcome> {
    let addr = (options.broker_url.host(), options.broker_url.port());
    let stream = match timeout_at(tokio::time::Instant::from_std(deadline), TcpStream::connect(addr)).await {
        Ok(Ok(stream)) => stream,
        Ok(Err(error)) => {
            return Err(ConnectionOutcome::Transport(TransportError::Connection(
                error.to_string(),
            )));
        }
        Err(_) => {
            return Err(ConnectionOutcome::Transport(TransportError::Connection(format!(
                "TCP connect timed out after {:?}",
                options.connect_timeout
            ))));
        }
    };

    if let Err(error) = stream.set_nodelay(options.tcp_nodelay) {
        return Err(ConnectionOutcome::Transport(TransportError::Connection(
            error.to_string(),
        )));
    }

    Ok(stream)
}

pub(crate) async fn tls_handshake(
    options: &ConnectOptions,
    tcp_stream: TcpStream,
    deadline: Instant,
) -> Result<(ReadHalf<TlsIo>, WriteHalf<TlsIo>), ConnectionOutcome> {
    let connector = rustls_connector()?;
    let server_name = tls_server_name(options.broker_url.host())?;
    let tls_stream = match timeout_at(
        tokio::time::Instant::from_std(deadline),
        connector.connect(server_name, tcp_stream),
    )
    .await
    {
        Ok(Ok(stream)) => stream,
        Ok(Err(error)) => {
            return Err(ConnectionOutcome::Transport(TransportError::Tls(error.to_string())));
        }
        Err(_) => {
            return Err(ConnectionOutcome::Transport(TransportError::Tls(format!(
                "TLS handshake timed out after {:?}",
                options.connect_timeout
            ))));
        }
    };
    Ok(split(tls_stream))
}

/// Crypto provider selected by the active `tls-rustls-*` Cargo feature.
fn selected_crypto_provider() -> rustls::crypto::CryptoProvider {
    #[cfg(feature = "tls-rustls-aws-lc")]
    {
        rustls::crypto::aws_lc_rs::default_provider()
    }
    #[cfg(feature = "tls-rustls-ring")]
    {
        rustls::crypto::ring::default_provider()
    }
}

fn rustls_connector() -> Result<TlsConnector, ConnectionOutcome> {
    let mut root_store = RootCertStore::empty();
    let cert_result = rustls_native_certs::load_native_certs();
    for cert in cert_result.certs {
        if let Err(error) = root_store.add(cert) {
            return Err(ConnectionOutcome::Transport(TransportError::Tls(format!(
                "invalid system trust anchor: {error}"
            ))));
        }
    }

    if root_store.is_empty() {
        let detail = cert_result
            .errors
            .first()
            .map(|error| error.to_string())
            .unwrap_or_else(|| "no system trust anchors found".into());
        return Err(ConnectionOutcome::Transport(TransportError::Tls(detail)));
    }

    // Explicit provider: MQTT TLS works even if the host process also enables
    // another rustls crypto feature (no reliance on process-global install_default).
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(selected_crypto_provider()))
        .with_safe_default_protocol_versions()
        .map_err(|error| {
            ConnectionOutcome::Transport(TransportError::Tls(format!(
                "rustls protocol versions unavailable for selected crypto provider: {error}"
            )))
        })?
        .with_root_certificates(root_store)
        .with_no_client_auth();

    Ok(TlsConnector::from(Arc::new(config)))
}

fn tls_server_name(host: &str) -> Result<ServerName<'static>, ConnectionOutcome> {
    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        return Ok(ServerName::IpAddress(ip.into()));
    }

    ServerName::try_from(host.to_owned())
        .map_err(|_| ConnectionOutcome::Transport(TransportError::Tls(format!("invalid TLS server name: {host}"))))
}

async fn read_connack<R>(
    framed_read: &mut FramedRead<R, V5Codec>,
    deadline: Instant,
    connect_timeout: Duration,
) -> Result<ConnectAck, ConnectionOutcome>
where
    R: AsyncRead + Unpin,
{
    timeout_at(
        tokio::time::Instant::from_std(deadline),
        read_connack_unbounded(framed_read),
    )
    .await
    .unwrap_or_else(|_| {
        Err(ConnectionOutcome::Transport(TransportError::Connection(format!(
            "CONNACK timed out after {connect_timeout:?}"
        ))))
    })
}

async fn read_connack_unbounded<R>(framed_read: &mut FramedRead<R, V5Codec>) -> Result<ConnectAck, ConnectionOutcome>
where
    R: AsyncRead + Unpin,
{
    match framed_read.next().await {
        Some(Ok((DecodedFrame::Control(Packet::ConnectAck(ack)), _wire_len))) => Ok(*ack),
        Some(Ok((DecodedFrame::Control(other), _))) => Err(ConnectionOutcome::Protocol(ProtocolError::Violation(
            format!("expected CONNACK, got {:?}", other.packet_type()),
        ))),
        Some(Ok((DecodedFrame::Publish(_), _))) => Err(ConnectionOutcome::Protocol(ProtocolError::Violation(
            "expected CONNACK, got PUBLISH".into(),
        ))),
        Some(Err(error)) => Err(ConnectionOutcome::Protocol(ProtocolError::Decode(error.to_string()))),
        None => Err(ConnectionOutcome::Transport(TransportError::Connection(
            "connection closed before CONNACK".into(),
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::V5Codec;
    use crate::options::TlsConfig;
    use crate::util::BrokerUrl;
    use tokio_util::codec::{FramedRead, FramedWrite};

    #[test]
    fn tls_server_name_accepts_ip_literal() {
        let name = tls_server_name("127.0.0.1").expect("ip literal");
        assert!(matches!(name, ServerName::IpAddress(_)));
    }

    #[test]
    fn tls_server_name_accepts_dns_host() {
        let name = tls_server_name("broker.example.com").expect("dns host");
        assert!(matches!(name, ServerName::DnsName(_)));
    }

    #[test]
    fn rustls_connector_loads_system_roots_or_errors() {
        match rustls_connector() {
            Ok(_) => {}
            Err(ConnectionOutcome::Transport(TransportError::Tls(message))) => {
                assert!(
                    message.contains("trust anchor") || message.contains("system"),
                    "unexpected tls bootstrap error: {message}"
                );
            }
            Err(other) => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn mqtts_url_selects_tls_mode() {
        use crate::connection::handshake::{TlsMode, resolve_tls_mode};

        let mqtts = BrokerUrl::parse("mqtts://broker.example").expect("valid");
        let options = ConnectOptions::new(mqtts);
        assert_eq!(resolve_tls_mode(&options), TlsMode::RustlsSystemRoots);

        let plain = BrokerUrl::parse("mqtt://broker.example").expect("valid");
        let options = ConnectOptions::new(plain).tls(TlsConfig::SystemRoots);
        assert_eq!(resolve_tls_mode(&options), TlsMode::RustlsSystemRoots);
    }

    #[tokio::test]
    async fn handshake_sends_connect_as_first_client_packet() {
        use bytes::BytesMut;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        use crate::client::{CredentialsProvider, SharedCredentials};
        use crate::codec::{ConnectAck, ConnectAckReason, Packet};
        use crate::error::AuthError;
        use crate::options::ConnectOptions;
        use crate::util::BrokerUrl;
        use secrecy::SecretString;

        use tokio_util::codec::Encoder;

        struct TestCreds;

        impl CredentialsProvider for TestCreds {
            async fn credentials(&self) -> Result<(String, SecretString), AuthError> {
                Ok(("user".into(), SecretString::from("pass".to_owned())))
            }
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let saw_connect = Arc::new(AtomicBool::new(false));
        let saw_connect_peer = Arc::clone(&saw_connect);

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut header = [0_u8; 1];
            stream.read_exact(&mut header).await.expect("read first byte");
            if header[0] == 0x10 {
                saw_connect_peer.store(true, Ordering::SeqCst);
            }
            let mut buf = BytesMut::new();
            V5Codec::with_max_packet_size(0)
                .encode(
                    Packet::ConnectAck(Box::new(ConnectAck {
                        reason_code: ConnectAckReason::Success,
                        ..Default::default()
                    })),
                    &mut buf,
                )
                .expect("encode CONNACK");
            stream.write_all(&buf).await.expect("write CONNACK");
        });

        let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{}", addr.port())).expect("valid");
        let options = ConnectOptions::new(url);
        let credentials = SharedCredentials::new(TestCreds);
        let deadline = Instant::now() + options.connect_timeout;
        let tcp_stream = open_tcp(&options, deadline).await.expect("tcp");
        let (read_half, write_half) = tcp_stream.into_split();
        establish_session_on(read_half, write_half, &options, &credentials, deadline)
            .await
            .expect("handshake");

        server.await.expect("server join");
        assert!(
            saw_connect.load(Ordering::SeqCst),
            "first client packet on the wire must be CONNECT (0x10)"
        );
    }

    /// §3.2 SHOULD: missing CONNACK within the shared `connect_timeout` ends the attempt.
    #[tokio::test]
    async fn connack_timeout_surfaces_transport_error() {
        use tokio::net::TcpListener;

        use crate::client::{CredentialsProvider, SharedCredentials};
        use crate::error::AuthError;
        use secrecy::SecretString;

        struct TestCreds;

        impl CredentialsProvider for TestCreds {
            async fn credentials(&self) -> Result<(String, SecretString), AuthError> {
                Ok(("user".into(), SecretString::from("pass".to_owned())))
            }
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        // Accept + hold the socket open without sending CONNACK.
        let server = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.expect("accept");
            tokio::time::sleep(Duration::from_secs(30)).await;
        });

        let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{}", addr.port())).expect("valid");
        let options = ConnectOptions::new(url).connect_timeout(Duration::from_millis(50));
        let credentials = SharedCredentials::new(TestCreds);
        let deadline = Instant::now() + options.connect_timeout;
        let tcp_stream = open_tcp(&options, deadline).await.expect("tcp");
        let (read_half, write_half) = tcp_stream.into_split();
        let err = match establish_session_on(read_half, write_half, &options, &credentials, deadline).await {
            Err(outcome) => outcome,
            Ok(_) => panic!("CONNACK must time out"),
        };
        match err {
            ConnectionOutcome::Transport(TransportError::Connection(message)) => {
                assert!(message.contains("CONNACK timed out"), "unexpected message: {message}");
            }
            other => panic!("expected Transport CONNACK timeout, got {other:?}"),
        }

        server.abort();
    }

    /// Elapsed connect budget during dial surfaces a TCP-stage timeout.
    #[tokio::test]
    async fn tcp_connect_timeout_surfaces_transport_error() {
        let url = BrokerUrl::parse("mqtt://127.0.0.1:9").expect("valid");
        let options = ConnectOptions::new(url).connect_timeout(Duration::from_millis(80));
        // Past deadline exercises the timeout branch without depending on routing/firewall.
        let deadline = Instant::now() - Duration::from_secs(1);
        let err = open_tcp(&options, deadline).await.expect_err("dial must time out");
        match err {
            ConnectionOutcome::Transport(TransportError::Connection(message)) => {
                assert!(
                    message.contains("TCP connect timed out"),
                    "unexpected message: {message}"
                );
            }
            other => panic!("expected Transport TCP timeout, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn connack_max_packet_size_applied_to_codecs() {
        use crate::codec::ConnectAck;
        use tokio::net::{TcpListener, TcpStream};

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let accept = tokio::spawn(async move { listener.accept().await.expect("accept") });
        let client = TcpStream::connect(addr).await.expect("connect");
        let _server = accept.await.expect("join accept");

        let (read_half, write_half) = client.into_split();
        let mut framed_read = FramedRead::new(read_half, V5Codec::with_max_packet_size(0));
        let mut framed_write = FramedWrite::new(write_half, V5Codec::with_max_packet_size(0));

        let ack = ConnectAck {
            max_packet_size: Some(1024),
            ..Default::default()
        };
        let session = apply_connack(Duration::from_secs(60), &ack).expect("success");
        apply_session_limits_to_codecs(&mut framed_read, &mut framed_write, &session);

        assert_eq!(framed_read.decoder().max_inbound_size(), 1024);
        assert_eq!(framed_write.encoder().max_outbound_size(), 1024);
    }
}
