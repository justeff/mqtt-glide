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
use super::redirect::ServerRedirect;
use super::subscriptions::refuse_packet;
use crate::client::SharedCredentials;
use crate::codec::v5::ToReasonCode;
use crate::codec::{ConnectAck, DecodeError, DecodedFrame, DisconnectReasonCode, Packet, V5Codec};
use crate::error::{ConnectionOutcome, ProtocolError, TransportError};
use crate::options::ConnectOptions;

type TlsIo = TlsStream<TcpStream>;
#[cfg(feature = "websocket")]
type WsIo<S> = super::websocket::WsByteStream<S>;

/// CONNECT/CONNACK on already-split transport halves.
///
/// A CONNACK that redirects (§4.11) leaves its target in `redirect`.
pub(crate) async fn establish_session_on<R, W>(
    read_half: R,
    write_half: W,
    options: &ConnectOptions,
    credentials: &SharedCredentials,
    deadline: Instant,
    redirect: &mut Option<ServerRedirect>,
) -> Result<(FramedRead<R, V5Codec>, FramedWrite<W, V5Codec>, SessionParams), ConnectionOutcome>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let (read_codec, write_codec) = distinct_v5_codecs_for_framed_halves(options.max_packet_size.get());
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

    let ack = match read_connack(&mut framed_read, deadline, options.connect_timeout).await {
        Ok(ack) => ack,
        Err((outcome, None)) => return Err(outcome),
        Err((outcome, Some(reason))) => return Err(refuse_packet(&mut framed_write, reason, outcome).await),
    };
    match apply_connack(options.keep_alive, &ack) {
        Ok(session) => {
            apply_session_limits_to_codecs(&mut framed_write, &session);
            Ok((framed_read, framed_write, session))
        }
        Err(outcome) => {
            *redirect = ServerRedirect::from_connack(&ack);
            Err(on_auth_failure(credentials, outcome).await)
        }
    }
}

/// Applies the broker Maximum Packet Size from CONNACK to the **outbound** encoder only.
///
/// The broker's limit constrains client→server packets ([MQTT-3.2.2-15]). Inbound stays
/// at the client Maximum Packet Size advertised in CONNECT ([MQTT-3.1.2-24]).
fn apply_session_limits_to_codecs<W>(framed_write: &mut FramedWrite<W, V5Codec>, session: &SessionParams) {
    if let Some(max_packet_size) = session.max_packet_size {
        framed_write.encoder_mut().set_max_outbound_size(max_packet_size);
    }
}

fn map_connect_send_error(error: crate::codec::EncodeError) -> ConnectionOutcome {
    match error {
        crate::codec::EncodeError::Io(message) => {
            ConnectionOutcome::Transport(TransportError::Connection(message.to_string()))
        }
        other => ConnectionOutcome::Protocol(ProtocolError::Violation(other.to_string())),
    }
}

/// TCP to the broker, or through [`ConnectOptions::http_proxy`] to it.
pub(crate) async fn open_tcp(options: &ConnectOptions, deadline: Instant) -> Result<TcpStream, ConnectionOutcome> {
    let (host, port) = (options.broker_url.host(), options.broker_url.port());
    let addr = match &options.http_proxy {
        Some(proxy) => (proxy.host(), proxy.port()),
        None => (host, port),
    };
    let mut stream = match timeout_at(tokio::time::Instant::from_std(deadline), TcpStream::connect(addr)).await {
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

    if let Some(proxy) = &options.http_proxy {
        let tunnel = super::proxy::tunnel(&mut stream, proxy, host, port);
        match timeout_at(tokio::time::Instant::from_std(deadline), tunnel).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return Err(ConnectionOutcome::Transport(error)),
            Err(_) => {
                return Err(ConnectionOutcome::Transport(TransportError::Connection(format!(
                    "HTTP proxy tunnel timed out after {:?}",
                    options.connect_timeout
                ))));
            }
        }
    }

    Ok(stream)
}

/// TCP, split into owned halves.
pub(crate) async fn dial_tcp(
    options: &ConnectOptions,
    deadline: Instant,
) -> Result<(tokio::net::tcp::OwnedReadHalf, tokio::net::tcp::OwnedWriteHalf), ConnectionOutcome> {
    Ok(open_tcp(options, deadline).await?.into_split())
}

/// TCP + TLS, split.
pub(crate) async fn dial_tls(
    options: &ConnectOptions,
    deadline: Instant,
) -> Result<(ReadHalf<TlsIo>, WriteHalf<TlsIo>), ConnectionOutcome> {
    let tcp_stream = open_tcp(options, deadline).await?;
    Ok(split(tls_connect(options, tcp_stream, deadline).await?))
}

/// TCP + WebSocket handshake, split.
#[cfg(feature = "websocket")]
pub(crate) async fn dial_ws(
    options: &ConnectOptions,
    deadline: Instant,
) -> Result<(ReadHalf<WsIo<TcpStream>>, WriteHalf<WsIo<TcpStream>>), ConnectionOutcome> {
    let tcp_stream = open_tcp(options, deadline).await?;
    Ok(split(super::websocket::handshake(options, tcp_stream, deadline).await?))
}

/// TCP + TLS (this crate's rustls setup) + WebSocket handshake, split.
#[cfg(feature = "websocket")]
pub(crate) async fn dial_wss(
    options: &ConnectOptions,
    deadline: Instant,
) -> Result<(ReadHalf<WsIo<TlsIo>>, WriteHalf<WsIo<TlsIo>>), ConnectionOutcome> {
    let tcp_stream = open_tcp(options, deadline).await?;
    let tls_stream = tls_connect(options, tcp_stream, deadline).await?;
    Ok(split(super::websocket::handshake(options, tls_stream, deadline).await?))
}

async fn tls_connect(
    options: &ConnectOptions,
    tcp_stream: TcpStream,
    deadline: Instant,
) -> Result<TlsIo, ConnectionOutcome> {
    let connector = rustls_connector()?;
    let server_name = tls_server_name(options.broker_url.host())?;
    match timeout_at(
        tokio::time::Instant::from_std(deadline),
        connector.connect(server_name, tcp_stream),
    )
    .await
    {
        Ok(Ok(stream)) => Ok(stream),
        Ok(Err(error)) => Err(ConnectionOutcome::Transport(TransportError::Tls(error.to_string()))),
        Err(_) => Err(ConnectionOutcome::Transport(TransportError::Tls(format!(
            "TLS handshake timed out after {:?}",
            options.connect_timeout
        )))),
    }
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

/// The CONNACK, or why there is none: with the DISCONNECT reason when the broker sent a
/// packet that is broken or has no place before CONNACK.
type ConnackRead = Result<ConnectAck, (ConnectionOutcome, Option<DisconnectReasonCode>)>;

async fn read_connack<R>(
    framed_read: &mut FramedRead<R, V5Codec>,
    deadline: Instant,
    connect_timeout: Duration,
) -> ConnackRead
where
    R: AsyncRead + Unpin,
{
    timeout_at(
        tokio::time::Instant::from_std(deadline),
        read_connack_unbounded(framed_read),
    )
    .await
    .unwrap_or_else(|_| {
        Err((
            ConnectionOutcome::Transport(TransportError::Connection(format!(
                "CONNACK timed out after {connect_timeout:?}"
            ))),
            None,
        ))
    })
}

async fn read_connack_unbounded<R>(framed_read: &mut FramedRead<R, V5Codec>) -> ConnackRead
where
    R: AsyncRead + Unpin,
{
    let unexpected = |packet: String| {
        Err((
            ConnectionOutcome::Protocol(ProtocolError::Violation(format!("expected CONNACK, got {packet}"))),
            Some(DisconnectReasonCode::ProtocolError),
        ))
    };
    match framed_read.next().await {
        Some(Ok((DecodedFrame::Control(Packet::ConnectAck(ack)), _wire_len))) => Ok(*ack),
        Some(Ok((DecodedFrame::Control(other), _))) => unexpected(format!("{:?}", other.packet_type())),
        Some(Ok((DecodedFrame::Publish(_), _))) => unexpected("PUBLISH".into()),
        Some(Err(DecodeError::Io(message))) => Err((
            ConnectionOutcome::Transport(TransportError::Connection(message.to_string())),
            None,
        )),
        Some(Err(error)) => Err((
            ConnectionOutcome::Protocol(ProtocolError::Decode(error.to_string())),
            Some(error.to_reason_code()),
        )),
        None => Err((
            ConnectionOutcome::Transport(TransportError::Connection("connection closed before CONNACK".into())),
            None,
        )),
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

    /// Set in the child run of `rustls_connector_fails_without_trust_anchors`.
    #[cfg(unix)]
    const ROOTS_CHILD: &str = "MQTT_GLIDE_ROOTS_CHILD";

    /// No trust anchor is a TLS error, never a connector that trusts nothing (or everything).
    /// The system store comes from `SSL_CERT_FILE` when it is set, so the test runs itself again
    /// in a child process with it pointing at an empty file: the environment of this process
    /// stays untouched for the tests running next to it.
    #[cfg(unix)]
    #[test]
    fn rustls_connector_fails_without_trust_anchors() {
        if std::env::var_os(ROOTS_CHILD).is_some() {
            match rustls_connector() {
                Ok(_) => println!("ROOTS=ok"),
                Err(ConnectionOutcome::Transport(TransportError::Tls(message))) => println!("ROOTS=tls:{message}"),
                Err(other) => println!("ROOTS=other:{other:?}"),
            }
            return;
        }
        let name = module_path!()
            .split_once("::")
            .map(|(_, path)| format!("{path}::rustls_connector_fails_without_trust_anchors"))
            .expect("module path");
        let output = std::process::Command::new(std::env::current_exe().expect("test binary"))
            .args([name.as_str(), "--exact", "--nocapture", "--test-threads=1"])
            .env(ROOTS_CHILD, "1")
            .env("SSL_CERT_FILE", "/dev/null")
            .env_remove("SSL_CERT_DIR")
            .output()
            .expect("run child");
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(output.status.success(), "child failed: {stdout}");
        assert!(stdout.contains("ROOTS=tls:no system trust anchors found\n"), "{stdout}");
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

    /// The first byte the client puts on a fresh connection is the CONNECT Fixed Header
    /// ([MQTT-3.1.0-1]).
    #[tokio::test]
    async fn handshake_sends_connect_as_first_client_packet() {
        use bytes::BytesMut;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        use crate::client::Password;
        use crate::client::{CredentialsProvider, SharedCredentials};
        use crate::codec::{ConnectAck, ConnectAckReason, Packet};
        use crate::error::AuthError;
        use crate::options::ConnectOptions;
        use crate::util::BrokerUrl;

        use tokio_util::codec::Encoder;

        struct TestCreds;

        impl CredentialsProvider for TestCreds {
            async fn credentials(&self) -> Result<(String, Password), AuthError> {
                Ok(("user".into(), Password::from("pass")))
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
        establish_session_on(read_half, write_half, &options, &credentials, deadline, &mut None)
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

        use crate::client::Password;
        use crate::client::{CredentialsProvider, SharedCredentials};
        use crate::error::AuthError;

        struct TestCreds;

        impl CredentialsProvider for TestCreds {
            async fn credentials(&self) -> Result<(String, Password), AuthError> {
                Ok(("user".into(), Password::from("pass")))
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
        let err = match establish_session_on(read_half, write_half, &options, &credentials, deadline, &mut None).await {
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
        let framed_read = FramedRead::new(read_half, V5Codec::with_max_packet_size(0));
        let mut framed_write = FramedWrite::new(write_half, V5Codec::with_max_packet_size(0));

        let ack = ConnectAck {
            max_packet_size: Some(1024),
            ..Default::default()
        };
        let session = apply_connack(Duration::from_secs(60), &ack).expect("success");
        apply_session_limits_to_codecs(&mut framed_write, &session);

        // Outbound capped by broker CONNACK ([MQTT-3.2.2-15]); this unit test starts with
        // unbounded inbound — production framed halves use ConnectOptions::max_packet_size.
        assert_eq!(framed_write.encoder().max_outbound_size(), 1024);
        assert_eq!(framed_read.decoder().max_inbound_size(), 0);
    }
}
