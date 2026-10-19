//! Shared Mosquitto container helpers for mqtt-glide broker tests.
//!
//! eclipse-mosquitto `2.1.2-alpine`: anonymous, reject-connect and Dynamic Security configs.
//!
//! When Docker is unavailable, [`start_mosquitto`] / [`start_mosquitto_reject_connect`] /
//! [`start_mosquitto_on_port`] return `None` after printing
//! `skipping brokers test: docker unavailable` so integration tests can early-return
//! instead of panicking.

use std::process::{Command, Stdio};
use std::sync::OnceLock;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use bytestring::ByteString;
use futures_util::{SinkExt, StreamExt};
use mqtt_glide::codec::{Connect, DecodedFrame, Disconnect, Packet, PublishDelivery, V5Codec, encode_publish};
use mqtt_glide::{
    BrokerUrl, ConnectAckReason, ConnectOptions, ConnectionOutcome, DEFAULT_MAX_PACKET_SIZE, InboundMessage,
    MqttClient, NoopLifecycle, PublishMessage, QoS, ReconnectPolicy, SessionParams, SessionPhase, connect,
};
use testcontainers::ContainerAsync;
use testcontainers::ContainerRequest;
use testcontainers::GenericImage;
use testcontainers::ImageExt;
use testcontainers::core::{CmdWaitFor, ExecCommand, WaitFor};
use testcontainers::runners::AsyncRunner;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::codec::{FramedRead, FramedWrite};

/// A client whose driver runs and whose first CONNACK arrived (see [`connected`]).
pub struct Client {
    pub client: MqttClient,
    /// Generation and session of the first `Connected` phase.
    pub generation: u64,
    pub session: SessionParams,
    pub phase: watch::Receiver<SessionPhase>,
    pub inbound: mpsc::Receiver<InboundMessage>,
    pub run: JoinHandle<Result<(), ConnectionOutcome>>,
}

/// `mqtt://127.0.0.1:{port}`.
pub fn tcp_url(port: u16) -> BrokerUrl {
    BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid broker url")
}

/// Connects with fixed credentials and `policy`, an inbound sink of `inbound_capacity`, spawns
/// the driver and waits for the CONNACK.
pub async fn connected(
    options: ConnectOptions,
    policy: impl ReconnectPolicy + 'static,
    inbound_capacity: usize,
) -> Client {
    let (tx, inbound) = mpsc::channel::<InboundMessage>(inbound_capacity);
    let (client, connection, mut phase) =
        connect(options, crate::common::static_credentials(), policy, tx, NoopLifecycle).expect("connect wiring");
    let run = tokio::spawn(connection.run());
    let SessionPhase::Connected { generation, session } =
        crate::common::wait_for_phase(&mut phase, Duration::from_secs(15), |p| {
            matches!(
                p,
                SessionPhase::Connected {
                    session: SessionParams { from_connack: true, .. },
                    ..
                }
            )
        })
        .await
    else {
        unreachable!("waited for Connected");
    };
    Client {
        client,
        generation,
        session,
        phase,
        inbound,
        run,
    }
}

impl Client {
    /// Shuts down, checks the driver ends cleanly and hands back the inbound sink.
    pub async fn stop(self) -> mpsc::Receiver<InboundMessage> {
        self.client.shutdown().await.expect("shutdown");
        join_run_after_shutdown(self.run).await;
        self.inbound
    }
}

/// Joins `Connection::run` after `shutdown()`: fails if the driver hangs or ends with anything
/// but a shutdown outcome.
pub async fn join_run_after_shutdown(run: JoinHandle<Result<(), ConnectionOutcome>>) {
    let result = timeout(Duration::from_secs(10), run)
        .await
        .expect("timed out joining run after shutdown")
        .expect("run task join");
    assert!(
        matches!(result, Err(ConnectionOutcome::Shutdown(_)) | Ok(())),
        "expected shutdown outcome after disconnect, got {result:?}"
    );
}

/// Mosquitto config used by most tests: anonymous access, no auth.
const CONF_NO_AUTH: &str = "listener 1883\nallow_anonymous true\n";

/// Mosquitto with the Dynamic Security plugin; the config file is created at start with an
/// `admin` / `adminpw` administrator (see [`dynsec`]).
const CONF_DYNSEC: &str = "listener 1883\nplugin /usr/lib/mosquitto_dynamic_security.so\nplugin_opt_config_file /mosquitto/data/dynsec.json\n";

/// Mosquitto config that rejects all connections: anonymous access disabled,
/// no password file configured → every CONNECT results in ConnAck(NotAuthorized).
const CONF_REJECT_CONNECT: &str = "listener 1883\nallow_anonymous false\n";

/// Whether a Docker-compatible CLI answers `docker info` (cached for the process).
///
/// Podman works when the `docker` CLI is available and `DOCKER_HOST` points at the engine.
pub fn docker_available() -> bool {
    static AVAILABLE: OnceLock<bool> = OnceLock::new();
    *AVAILABLE.get_or_init(|| {
        Command::new("docker")
            .args(["info"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    })
}

fn skip_without_docker() -> bool {
    if docker_available() {
        return false;
    }
    eprintln!("skipping brokers test: docker unavailable");
    true
}

fn mosquitto_image(conf: &'static str) -> ContainerRequest<GenericImage> {
    GenericImage::new("eclipse-mosquitto", "2.1.2-alpine")
        .with_wait_for(WaitFor::message_on_stderr("mosquitto version"))
        .with_wait_for(WaitFor::message_on_stderr(" running"))
        .with_copy_to("/mosquitto-test.conf", conf.as_bytes().to_vec())
        .with_cmd(["mosquitto", "-c", "/mosquitto-test.conf"])
}

/// Starts a Mosquitto container with anonymous access and returns the container
/// and the mapped host port.
///
/// Returns `None` (after a skip message) when Docker is unavailable.
pub async fn start_mosquitto() -> Option<(ContainerAsync<GenericImage>, u16)> {
    if skip_without_docker() {
        return None;
    }
    let container = mosquitto_image(CONF_NO_AUTH)
        .start()
        .await
        .expect("failed to start Mosquitto container");

    let port = container.get_host_port_ipv4(1883).await.expect("mapped mosquitto port");
    Some((container, port))
}

/// Mosquitto with only a TLS listener that requires a client certificate from the test CA.
const CONF_TLS: &str = "listener 8883\nallow_anonymous true\ncafile /certs/ca.pem\ncertfile /certs/server.pem\nkeyfile /certs/server.key\nrequire_certificate true\n";

/// Starts a Mosquitto container with the TLS listener of [`CONF_TLS`] (certificates from
/// `tests/certs`) and returns the container and the mapped host port.
///
/// Returns `None` (after a skip message) when Docker is unavailable.
pub async fn start_mosquitto_tls() -> Option<(ContainerAsync<GenericImage>, u16)> {
    if skip_without_docker() {
        return None;
    }
    use testcontainers::core::ContainerPort;

    let certs = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/certs");
    // The image only exposes 1883; the TLS port has to be exposed explicitly.
    let mut image = GenericImage::new("eclipse-mosquitto", "2.1.2-alpine")
        .with_exposed_port(ContainerPort::Tcp(8883))
        .with_wait_for(WaitFor::message_on_stderr("mosquitto version"))
        .with_wait_for(WaitFor::message_on_stderr(" running"))
        .with_copy_to("/mosquitto-test.conf", CONF_TLS.as_bytes().to_vec())
        .with_cmd(["mosquitto", "-c", "/mosquitto-test.conf"]);
    for name in ["ca.pem", "server.pem", "server.key"] {
        image = image.with_copy_to(format!("/certs/{name}"), certs.join(name));
    }
    let container = image.start().await.expect("failed to start Mosquitto container (TLS)");
    let port = container
        .get_host_port_ipv4(8883)
        .await
        .expect("mapped mosquitto TLS port");
    Some((container, port))
}

/// Starts a Mosquitto container that rejects all CONNECT attempts with
/// `ConnAck(NotAuthorized)`. Used to test auth-failure handling.
///
/// Returns `None` (after a skip message) when Docker is unavailable.
pub async fn start_mosquitto_reject_connect() -> Option<(ContainerAsync<GenericImage>, u16)> {
    if skip_without_docker() {
        return None;
    }
    let container = mosquitto_image(CONF_REJECT_CONNECT)
        .start()
        .await
        .expect("failed to start Mosquitto container (reject-connect)");

    let port = container.get_host_port_ipv4(1883).await.expect("mapped mosquitto port");
    Some((container, port))
}

/// Starts a Mosquitto container with Dynamic Security (no clients yet besides the admin);
/// set it up with [`dynsec`].
///
/// Returns `None` (after a skip message) when Docker is unavailable.
pub async fn start_mosquitto_dynsec() -> Option<(ContainerAsync<GenericImage>, u16)> {
    if skip_without_docker() {
        return None;
    }
    let container = GenericImage::new("eclipse-mosquitto", "2.1.2-alpine")
        .with_wait_for(WaitFor::message_on_stderr(" running"))
        .with_copy_to("/mosquitto-test.conf", CONF_DYNSEC.as_bytes().to_vec())
        .with_cmd([
            "sh",
            "-c",
            "mosquitto_ctrl dynsec init /mosquitto/data/dynsec.json admin adminpw >/dev/null \
             && chown mosquitto /mosquitto/data/dynsec.json \
             && exec mosquitto -c /mosquitto-test.conf",
        ])
        .start()
        .await
        .expect("failed to start Mosquitto container (dynsec)");
    let port = container.get_host_port_ipv4(1883).await.expect("mapped mosquitto port");
    Some((container, port))
}

/// Runs `mosquitto_ctrl dynsec <args>` as the admin of a [`start_mosquitto_dynsec`] broker.
/// A change to a role's ACLs makes Mosquitto drop every connected client with that role.
pub async fn dynsec(container: &ContainerAsync<GenericImage>, args: &[&str]) {
    let mut command = vec!["mosquitto_ctrl", "-u", "admin", "-P", "adminpw", "dynsec"];
    command.extend_from_slice(args);
    let result = container
        .exec(ExecCommand::new(command).with_cmd_ready_condition(CmdWaitFor::exit()))
        .await
        .expect("exec in mosquitto container");
    assert_eq!(
        result.exit_code().await.expect("exit code"),
        Some(0),
        "dynsec {args:?} failed"
    );
}

/// Starts Mosquitto mapped to a fixed host port (reconnect tests after stop/drop).
///
/// After the previous container releases the port, the reconnect loop can find the
/// broker again on the same host port.
///
/// Returns `None` (after a skip message) when Docker is unavailable.
pub async fn start_mosquitto_on_port(port: u16) -> Option<ContainerAsync<GenericImage>> {
    if skip_without_docker() {
        return None;
    }
    use testcontainers::core::ContainerPort;

    Some(
        mosquitto_image(CONF_NO_AUTH)
            .with_mapped_port(port, ContainerPort::Tcp(1883))
            .start()
            .await
            .expect("failed to start Mosquitto container on fixed port"),
    )
}

/// Mosquitto image for the WebSocket tests (see [`start_mosquitto_websockets`]).
#[cfg(feature = "websocket")]
pub const MOSQUITTO_WEBSOCKET_TAG: &str = "2.0.22";

/// Mosquitto with a plain TCP listener (1883) and a WebSocket listener (8080).
///
/// Uses the 2.0 line (libwebsockets) on purpose: Mosquitto 2.1.x's built-in WebSocket
/// support drops a client whose frames reach it split across TCP reads ("disconnected:
/// oversize packet"); 2.0.22 and HiveMQ accept the same frames.
#[cfg(feature = "websocket")]
const CONF_WEBSOCKETS: &str = "listener 1883\nlistener 8080\nprotocol websockets\nallow_anonymous true\n";

/// Starts Mosquitto with TCP and WebSocket listeners; returns `(container, tcp_port, ws_port)`.
///
/// Returns `None` (after a skip message) when Docker is unavailable.
#[cfg(feature = "websocket")]
pub async fn start_mosquitto_websockets() -> Option<(ContainerAsync<GenericImage>, u16, u16)> {
    if skip_without_docker() {
        return None;
    }
    use testcontainers::core::ContainerPort;

    // The image only exposes 1883; the WebSocket port has to be exposed explicitly.
    let container = GenericImage::new("eclipse-mosquitto", MOSQUITTO_WEBSOCKET_TAG)
        .with_exposed_port(ContainerPort::Tcp(8080))
        .with_wait_for(WaitFor::message_on_stderr("mosquitto version"))
        .with_wait_for(WaitFor::message_on_stderr(" running"))
        .with_copy_to("/mosquitto-test.conf", CONF_WEBSOCKETS.as_bytes().to_vec())
        .with_cmd(["mosquitto", "-c", "/mosquitto-test.conf"])
        .start()
        .await
        .expect("failed to start Mosquitto container (websockets)");
    let tcp_port = container.get_host_port_ipv4(1883).await.expect("mapped tcp port");
    let ws_port = container.get_host_port_ipv4(8080).await.expect("mapped ws port");
    Some((container, tcp_port, ws_port))
}

/// Starts HiveMQ Community Edition (anonymous via its bundled allow-all extension) and
/// returns the container and the mapped MQTT port.
///
/// A second broker implementation with much tighter defaults than Mosquitto: CONNACK
/// Receive Maximum 10, Topic Alias Maximum 5. Returns `None` when Docker is unavailable.
pub async fn start_hivemq() -> Option<(ContainerAsync<GenericImage>, u16)> {
    if skip_without_docker() {
        return None;
    }
    let (container, port, _ws_port) = start_hivemq_with_websocket().await?;
    Some((container, port))
}

/// [`start_hivemq`] plus HiveMQ's WebSocket listener (container port 8000, path `/mqtt`);
/// returns `(container, tcp_port, ws_port)`.
pub async fn start_hivemq_with_websocket() -> Option<(ContainerAsync<GenericImage>, u16, u16)> {
    if skip_without_docker() {
        return None;
    }
    // The image already exposes 8000 (as "8000", without "/tcp"). Exposing it again as
    // "8000/tcp" makes Docker bind the same host port twice ("address already in use").
    let container = GenericImage::new("hivemq/hivemq-ce", "2026.5")
        .with_wait_for(WaitFor::message_on_stdout("Started HiveMQ in"))
        .start()
        .await
        .expect("failed to start HiveMQ CE container");
    let port = container.get_host_port_ipv4(1883).await.expect("mapped hivemq port");
    let ws_port = container.get_host_port_ipv4(8000).await.expect("mapped hivemq ws port");
    Some((container, port, ws_port))
}

/// Second MQTT 5 session: CONNECT + QoS 0 PUBLISH + DISCONNECT (no client crate publish API yet).
///
/// Used so broker integration can deliver application messages without.
pub async fn publish_qos0_via_raw_session(port: u16, client_id: &str, topic: &str, payload: &[u8]) {
    let stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("raw publisher connect");
    let (read_half, write_half) = stream.into_split();
    let mut framed_read = FramedRead::new(read_half, V5Codec::with_max_packet_size(0));
    let mut framed_write = FramedWrite::new(write_half, V5Codec::with_max_packet_size(0));

    let connect = Connect {
        clean_start: true,
        keep_alive: 30,
        client_id: ByteString::from(client_id.to_owned()),
        topic_alias_max: 0,
        ..Connect::default()
    };
    framed_write
        .send(Packet::Connect(Box::new(connect)))
        .await
        .expect("send CONNECT");

    let connack = timeout(Duration::from_secs(5), framed_read.next())
        .await
        .expect("CONNACK wait")
        .expect("stream closed")
        .expect("decode CONNACK");
    match connack {
        (DecodedFrame::Control(Packet::ConnectAck(ack)), _) if ack.reason_code == ConnectAckReason::Success => {}
        other => panic!("raw publisher expected Success CONNACK, got {other:?}"),
    }

    let message = PublishMessage {
        topic: ByteString::from(topic.to_owned()),
        payload: Bytes::copy_from_slice(payload),
        properties: Default::default(),
    };
    let delivery = PublishDelivery {
        dup: false,
        retain: false,
        qos: QoS::AtMostOnce,
        packet_id: None,
    };
    let mut buf = BytesMut::new();
    encode_publish(&message, delivery, DEFAULT_MAX_PACKET_SIZE, &mut buf).expect("encode QoS0 publish");
    framed_write
        .get_mut()
        .write_all(&buf)
        .await
        .expect("write PUBLISH bytes");
    framed_write.flush().await.expect("flush PUBLISH");

    framed_write
        .send(Packet::Disconnect(Disconnect::default()))
        .await
        .expect("send DISCONNECT");
}

/// Second MQTT 5 session: CONNECT + QoS 1 PUBLISH + wait for PUBACK + DISCONNECT.
///
/// Delivers an application message that the broker will forward as QoS 1 to subscribers.
pub async fn publish_qos1_via_raw_session(port: u16, client_id: &str, topic: &str, payload: &[u8]) {
    use std::num::NonZeroU16;

    let stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("raw publisher connect");
    let (read_half, write_half) = stream.into_split();
    let mut framed_read = FramedRead::new(read_half, V5Codec::with_max_packet_size(0));
    let mut framed_write = FramedWrite::new(write_half, V5Codec::with_max_packet_size(0));

    let connect = Connect {
        clean_start: true,
        keep_alive: 30,
        client_id: ByteString::from(client_id.to_owned()),
        topic_alias_max: 0,
        ..Connect::default()
    };
    framed_write
        .send(Packet::Connect(Box::new(connect)))
        .await
        .expect("send CONNECT");

    let connack = timeout(Duration::from_secs(5), framed_read.next())
        .await
        .expect("CONNACK wait")
        .expect("stream closed")
        .expect("decode CONNACK");
    match connack {
        (DecodedFrame::Control(Packet::ConnectAck(ack)), _) if ack.reason_code == ConnectAckReason::Success => {}
        other => panic!("raw publisher expected Success CONNACK, got {other:?}"),
    }

    let message = PublishMessage {
        topic: ByteString::from(topic.to_owned()),
        payload: Bytes::copy_from_slice(payload),
        properties: Default::default(),
    };
    let delivery = PublishDelivery {
        dup: false,
        retain: false,
        qos: QoS::AtLeastOnce,
        packet_id: Some(NonZeroU16::new(1).expect("nz")),
    };
    let mut buf = BytesMut::new();
    encode_publish(&message, delivery, DEFAULT_MAX_PACKET_SIZE, &mut buf).expect("encode QoS1 publish");
    framed_write
        .get_mut()
        .write_all(&buf)
        .await
        .expect("write PUBLISH bytes");
    framed_write.flush().await.expect("flush PUBLISH");

    let puback = timeout(Duration::from_secs(5), framed_read.next())
        .await
        .expect("PUBACK wait")
        .expect("stream closed")
        .expect("decode PUBACK");
    match puback {
        (DecodedFrame::Control(Packet::PublishAck(_)), _) => {}
        other => panic!("raw publisher expected PUBACK, got {other:?}"),
    }

    framed_write
        .send(Packet::Disconnect(Disconnect::default()))
        .await
        .expect("send DISCONNECT");
}
