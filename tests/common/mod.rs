//! Shared test doubles and helpers for `mqtt-glide` integration tests.
//!
//! Included via `#[path = "../common/mod.rs"] mod common;` in each integration
//! binary (`tests/connection.rs`, `tests/brokers/main.rs`) since Cargo builds
//! every file under `tests/` as an independent binary and does not share
//! modules across them automatically.
//!
//! Not every helper is used by every binary; allow unused items so `-D warnings`
//! stays green when Mosquitto brokers tests omit the PeerBroker path.
#![allow(dead_code)]

use std::future::Future;
use std::num::NonZeroU16;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use mqtt_glide::Password;
use mqtt_glide::codec::{
    Connect, ConnectAck, DecodedFrame, Disconnect, Packet, PublishAck, PublishDelivery, PublishMessage, QoS,
    SubscribeAck, V5Codec, encode_publish,
};
use mqtt_glide::{
    AuthError, BrokerUrl, ClientId, ConnectOptions, ConnectionOutcome, CredentialsProvider, DisconnectReasonCode,
    InboundMessage, MqttClient, NoopLifecycle, PublishAckReason, ReconnectPolicy, SessionPhase,
    StandardReconnectPolicy, SubscribeAckReason, connect,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::codec::{FramedRead, FramedWrite};

/// Framed write half used by scripted mock brokers.
pub type MockFramedWrite = FramedWrite<tokio::net::tcp::OwnedWriteHalf, V5Codec>;
/// Framed read half used by scripted mock brokers.
pub type MockFramedRead = FramedRead<tokio::net::tcp::OwnedReadHalf, V5Codec>;

/// Fixed username/password credentials ("user"/"pass") that never invalidate.
pub fn static_credentials() -> mqtt_glide::StaticCredentials {
    mqtt_glide::StaticCredentials::new("user", "pass")
}

/// Fixed credentials that count [`CredentialsProvider::invalidate`] calls.
pub struct CountingInvalidate {
    pub invalidations: Arc<AtomicUsize>,
}

impl CredentialsProvider for CountingInvalidate {
    async fn credentials(&self) -> Result<(String, Password), AuthError> {
        Ok(("user".into(), Password::from("pass")))
    }

    async fn invalidate(&self) {
        self.invalidations.fetch_add(1, Ordering::SeqCst);
    }
}

/// Reconnect policy that never reconnects — a single connect attempt.
pub struct NoReconnect;

impl ReconnectPolicy for NoReconnect {
    fn delay_after(&self, _outcome: &ConnectionOutcome) -> Option<Duration> {
        None
    }
}

/// Reconnect policy that retries 20 ms after every failure.
pub fn quick_retry() -> StandardReconnectPolicy {
    StandardReconnectPolicy::new(Duration::from_millis(20), Duration::from_millis(20))
}

/// URL of a peer broker listening on `127.0.0.1:port`.
pub fn local_url(port: u16) -> BrokerUrl {
    BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url")
}

/// Options for a peer broker on `127.0.0.1:port` with a fresh client id starting with `prefix`.
pub fn local_options(port: u16, prefix: &str) -> ConnectOptions {
    ConnectOptions::new(local_url(port)).client_id(ClientId::from_prefix(prefix))
}

/// A client wired with [`connect`] whose driver runs in its own task.
pub struct TestClient {
    pub client: MqttClient,
    pub phase: watch::Receiver<SessionPhase>,
    pub run: JoinHandle<Result<(), ConnectionOutcome>>,
}

impl TestClient {
    /// Wire the client and spawn its driver.
    pub fn spawn(
        options: ConnectOptions,
        credentials: impl CredentialsProvider + 'static,
        policy: impl ReconnectPolicy + 'static,
        sink: mpsc::Sender<InboundMessage>,
    ) -> Self {
        let (client, connection, phase) = connect(options, credentials, policy, sink, NoopLifecycle).expect("wiring");
        let run = tokio::spawn(connection.run());
        Self { client, phase, run }
    }

    /// Like [`TestClient::spawn`], then wait for the first `Connected`.
    pub async fn connected(
        options: ConnectOptions,
        credentials: impl CredentialsProvider + 'static,
        policy: impl ReconnectPolicy + 'static,
        sink: mpsc::Sender<InboundMessage>,
    ) -> Self {
        let mut started = Self::spawn(options, credentials, policy, sink);
        wait_for_phase(&mut started.phase, Duration::from_secs(5), |phase| {
            matches!(phase, SessionPhase::Connected { .. })
        })
        .await;
        started
    }

    /// Shut the client down and wait for its driver to stop.
    pub async fn shutdown(self) {
        self.client.shutdown().await.expect("shutdown");
        let _ = timeout(Duration::from_secs(5), self.run)
            .await
            .expect("driver stops after shutdown")
            .expect("driver task");
    }
}

/// Wait until the phase watch matches `pred` or the deadline elapses.
pub async fn wait_for_phase<F>(
    phase: &mut watch::Receiver<SessionPhase>,
    deadline: Duration,
    mut pred: F,
) -> SessionPhase
where
    F: FnMut(&SessionPhase) -> bool,
{
    match timeout(deadline, phase.wait_for(|p| pred(p))).await {
        Ok(Ok(guard)) => guard.clone(),
        Ok(Err(_)) => panic!("phase watch closed before matching SessionPhase"),
        Err(_) => panic!("timed out waiting for matching SessionPhase"),
    }
}

/// Poll until `pred` is true or `deadline` elapses.
pub async fn wait_until(deadline: Duration, mut pred: impl FnMut() -> bool, label: &str) {
    let end = std::time::Instant::now() + deadline;
    while !pred() {
        assert!(std::time::Instant::now() < end, "timed out waiting for {label}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Split a TCP stream into framed MQTT 5 read/write halves (max packet size 0 = unlimited).
pub fn framed_halves(stream: TcpStream) -> (MockFramedRead, MockFramedWrite) {
    let (read_half, write_half) = stream.into_split();
    (
        FramedRead::new(read_half, V5Codec::with_max_packet_size(0)),
        FramedWrite::new(write_half, V5Codec::with_max_packet_size(0)),
    )
}

/// Listening MQTT peer that accepts one or more client sessions.
pub struct PeerBroker {
    listener: TcpListener,
}

/// One accepted client TCP session with framed MQTT halves.
pub struct PeerSession {
    pub read: MockFramedRead,
    pub write: MockFramedWrite,
}

impl PeerBroker {
    /// Accept the next client connection and return framed halves.
    pub async fn accept_session(&self) -> PeerSession {
        let (stream, _) = self.listener.accept().await.expect("peer accept");
        let (read, write) = framed_halves(stream);
        PeerSession { read, write }
    }

    /// Accept the next client, expect its CONNECT and answer with a success CONNACK.
    pub async fn accept_connected(&self) -> PeerSession {
        let mut session = self.accept_session().await;
        session.expect_connect().await;
        session.send_connack_success().await;
        session
    }

    /// Returns `true` if another client dials within `deadline` (used to detect unwanted reconnects).
    pub async fn try_accept_within(&self, deadline: Duration) -> bool {
        timeout(deadline, self.listener.accept()).await.is_ok()
    }
}

impl PeerSession {
    /// Expect CONNECT and return the decoded packet.
    pub async fn expect_connect(&mut self) -> Connect {
        match self.read.next().await {
            Some(Ok((DecodedFrame::Control(Packet::Connect(connect)), _))) => *connect,
            other => panic!("peer expected CONNECT, got {other:?}"),
        }
    }

    /// Reply with the given CONNACK.
    pub async fn send_connack(&mut self, ack: ConnectAck) {
        self.write
            .send(Packet::ConnectAck(Box::new(ack)))
            .await
            .expect("peer send CONNACK");
    }

    /// Reply with a default success CONNACK (`session_present = false`).
    pub async fn send_connack_success(&mut self) {
        self.send_connack(ConnectAck::default()).await;
    }

    /// Send a broker DISCONNECT with `reason`.
    pub async fn send_disconnect(&mut self, reason: DisconnectReasonCode) {
        self.write
            .send(Packet::Disconnect(Disconnect::new(reason)))
            .await
            .expect("peer send DISCONNECT");
    }

    /// Expect a client DISCONNECT with `reason` (before EOF).
    pub async fn expect_disconnect(&mut self, reason: DisconnectReasonCode) {
        match self.read.next().await {
            Some(Ok((DecodedFrame::Control(Packet::Disconnect(disconnect)), _))) => {
                assert_eq!(disconnect.reason_code, reason, "peer expected DISCONNECT {reason:?}");
            }
            other => panic!("peer expected DISCONNECT {reason:?}, got {other:?}"),
        }
    }

    /// Expect SUBSCRIBE and reply with Granted QoS 0 for every filter.
    pub async fn expect_subscribe_grant_qos0(&mut self) -> NonZeroU16 {
        let (packet_id, filter_count) = match self.read.next().await {
            Some(Ok((DecodedFrame::Control(Packet::Subscribe(sub)), _))) => (sub.packet_id, sub.topic_filters.len()),
            other => panic!("peer expected SUBSCRIBE, got {other:?}"),
        };
        self.send_suback_qos0(packet_id, filter_count).await;
        packet_id
    }

    /// Collect the Packet Identifiers of the next `count` SUBSCRIBEs, skipping other packets.
    pub async fn expect_subscribe_ids(&mut self, count: usize) -> Vec<NonZeroU16> {
        let mut ids = Vec::with_capacity(count);
        while ids.len() < count {
            match self.read.next().await {
                Some(Ok((DecodedFrame::Control(Packet::Subscribe(subscribe)), _))) => ids.push(subscribe.packet_id),
                Some(Ok(_)) => {}
                other => panic!("peer expected SUBSCRIBE, got {other:?}"),
            }
        }
        ids
    }

    /// Queue a QoS 0 PUBLISH in the write buffer without flushing it.
    pub fn feed_publish(&mut self, topic: &'static str, payload: impl Into<bytes::Bytes>, retain: bool) {
        let delivery = PublishDelivery {
            dup: false,
            retain,
            qos: QoS::AtMostOnce,
            packet_id: None,
        };
        encode_publish(
            &PublishMessage::new(topic, payload.into()),
            delivery,
            0,
            self.write.write_buffer_mut(),
        )
        .expect("encode PUBLISH");
    }

    /// Reply with a SUBACK granting QoS 0 to `filter_count` filters.
    pub async fn send_suback_qos0(&mut self, packet_id: NonZeroU16, filter_count: usize) {
        self.write
            .send(Packet::SubscribeAck(SubscribeAck {
                packet_id,
                status: vec![SubscribeAckReason::GrantedQos0; filter_count],
                properties: Default::default(),
                reason_string: None,
            }))
            .await
            .expect("peer send SUBACK");
    }

    /// Reply with a success PUBACK; ignores a closed connection.
    pub async fn send_puback(&mut self, packet_id: NonZeroU16) {
        let ack = Packet::PublishAck(PublishAck {
            packet_id,
            reason_code: PublishAckReason::Success,
            properties: Default::default(),
            reason_string: None,
        });
        let _ = self.write.send(ack).await;
    }

    /// Keep the TCP session open until the test aborts the peer task.
    pub async fn hold(self) {
        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}

/// Bind an ephemeral listener and run `script` against a [`PeerBroker`].
///
/// Returns `(port, join_handle)`. Abort the handle when the test finishes if the peer may still
/// be blocked in `hold` / `accept`.
pub async fn spawn_peer_broker<F, Fut>(script: F) -> (u16, JoinHandle<()>)
where
    F: FnOnce(PeerBroker) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind scripted peer broker");
    let port = listener.local_addr().expect("local addr").port();
    let handle = tokio::spawn(async move {
        script(PeerBroker { listener }).await;
    });
    (port, handle)
}

/// Single-session peer: CONNECT → CONNACK Success → DISCONNECT(`reason`).
pub async fn spawn_connack_then_disconnect(reason: DisconnectReasonCode) -> (u16, JoinHandle<()>) {
    spawn_peer_broker(move |broker| async move {
        let mut session = broker.accept_session().await;
        session.expect_connect().await;
        session.send_connack_success().await;
        session.send_disconnect(reason).await;
    })
    .await
}

/// Await a peer task and panic if it times out or panics.
pub async fn join_peer(peer: JoinHandle<()>) {
    timeout(Duration::from_secs(5), peer)
        .await
        .expect("peer timed out")
        .expect("peer panicked");
}
