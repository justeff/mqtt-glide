//! Shared mock-broker spawners and CONNECT→CONNACK helpers for connection unit tests.

use std::num::NonZeroU16;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::time::timeout;
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::client::{ReconnectPolicy, StaticCredentials, connect};
use crate::codec::{ConnectAck, DecodedFrame, Packet, PublishMessage, V5Codec};
use crate::connection::{Connection, NoopLifecycle, SessionParams, SessionPhase};
use crate::error::ConnectionOutcome;
use crate::options::ConnectOptions;
use crate::util::BrokerUrl;

/// Framed write half used by mock brokers in unit tests.
pub(crate) type MockFramedWrite = FramedWrite<tokio::net::tcp::OwnedWriteHalf, crate::codec::V5Codec>;
/// Framed read half used by mock brokers in unit tests.
pub(crate) type MockFramedRead = FramedRead<tokio::net::tcp::OwnedReadHalf, crate::codec::V5Codec>;

/// Default mock-broker username/password (`user` / `pass`).
pub(crate) fn static_test_credentials() -> StaticCredentials {
    StaticCredentials::new("user", "pass")
}

/// Single-session policy — peer close / protocol errors must finish `run()` (no hang).
pub(crate) use crate::client::NoReconnect;

/// Reconnect with a fixed backoff so the gap between sessions is observable.
pub(crate) struct DelayedReconnect {
    pub delay: Duration,
}

impl ReconnectPolicy for DelayedReconnect {
    fn delay_after(&self, outcome: &ConnectionOutcome) -> Option<Duration> {
        match outcome {
            ConnectionOutcome::Shutdown(_) | ConnectionOutcome::Application(_) => None,
            _ => Some(self.delay),
        }
    }
}

/// Accept CONNECT and reply with CONNACK Success. Returns `false` if the peer did not send CONNECT.
pub(crate) async fn accept_connect_send_connack(
    framed_read: &mut MockFramedRead,
    framed_write: &mut MockFramedWrite,
) -> bool {
    match framed_read.next().await {
        Some(Ok((DecodedFrame::Control(Packet::Connect(_)), _))) => {
            framed_write.send(Packet::ConnectAck(Box::default())).await.is_ok()
        }
        _ => false,
    }
}

/// Accept CONNECT and reply with a caller-supplied CONNACK (e.g. a custom `receive_max`).
/// Returns `false` if the peer did not send CONNECT.
pub(crate) async fn accept_connect_send_connack_with(
    framed_read: &mut MockFramedRead,
    framed_write: &mut MockFramedWrite,
    ack: ConnectAck,
) -> bool {
    match framed_read.next().await {
        Some(Ok((DecodedFrame::Control(Packet::Connect(_)), _))) => {
            framed_write.send(Packet::ConnectAck(Box::new(ack))).await.is_ok()
        }
        _ => false,
    }
}

/// Split a TCP stream into framed MQTT 5 read/write halves (max packet size 0 = unlimited decode).
pub(crate) fn framed_halves(stream: tokio::net::TcpStream) -> (MockFramedRead, MockFramedWrite) {
    let (read_half, write_half) = stream.into_split();
    (
        FramedRead::new(read_half, V5Codec::with_max_packet_size(0)),
        FramedWrite::new(write_half, V5Codec::with_max_packet_size(0)),
    )
}

/// Bind an ephemeral listener and run `on_session` for every accepted connection (loop).
pub(crate) async fn spawn_broker_loop<F, Fut>(on_session: F) -> u16
where
    F: Fn(MockFramedRead, MockFramedWrite) -> Fut + Send + Sync + 'static + Clone,
    Fut: Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock broker");
    let port = listener.local_addr().expect("local addr").port();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let on_session = on_session.clone();
            tokio::spawn(async move {
                let (framed_read, framed_write) = framed_halves(stream);
                on_session(framed_read, framed_write).await;
            });
        }
    });
    port
}

/// Bind an ephemeral listener and run `on_session` for the first accepted connection only.
pub(crate) async fn spawn_broker_once<F, Fut>(on_session: F) -> u16
where
    F: FnOnce(MockFramedRead, MockFramedWrite) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock broker");
    let port = listener.local_addr().expect("local addr").port();
    tokio::spawn(async move {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let (framed_read, framed_write) = framed_halves(stream);
        on_session(framed_read, framed_write).await;
    });
    port
}

/// Minimal MQTT 5 acceptor: CONNACK Success to any CONNECT, then drain until EOF.
pub(crate) async fn spawn_mock_broker() -> u16 {
    spawn_broker_loop(|mut framed_read, mut framed_write| async move {
        if !accept_connect_send_connack(&mut framed_read, &mut framed_write).await {
            return;
        }
        while framed_read.next().await.is_some() {}
    })
    .await
}

/// Keep-alive enforcer: CONNACK, optional inbound PUBLISH to stall handoff, counts PINGREQs,
/// and closes if no PINGREQ within `idle`.
///
/// On PINGREQ: optional PINGRESP and/or a QoS-0 PUBLISH (liveness without PINGRESP).
pub(crate) async fn spawn_mock_broker_keepalive_enforcer(
    idle: Duration,
    stall_publish: bool,
    respond_pingresp: bool,
    liveness_publish: bool,
    ping_count: Arc<std::sync::atomic::AtomicUsize>,
) -> u16 {
    use crate::codec::{DEFAULT_MAX_PACKET_SIZE, PublishDelivery, encode_publish};
    use bytes::BytesMut;

    async fn send_qos0_publish(
        framed_write: &mut MockFramedWrite,
        topic: &'static str,
        payload: &'static [u8],
    ) -> bool {
        let message = PublishMessage {
            topic: bytestring::ByteString::from_static(topic),
            payload: bytes::Bytes::from_static(payload),
            properties: crate::codec::PublishProperties::default(),
        };
        let delivery = PublishDelivery {
            dup: false,
            retain: false,
            qos: crate::codec::QoS::AtMostOnce,
            packet_id: None,
        };
        let mut buf = BytesMut::new();
        if encode_publish(&message, delivery, DEFAULT_MAX_PACKET_SIZE, &mut buf).is_err() {
            return false;
        }
        if framed_write.get_mut().write_all(&buf).await.is_err() {
            return false;
        }
        framed_write.flush().await.is_ok()
    }

    spawn_broker_loop(move |mut framed_read, mut framed_write| {
        let ping_count = Arc::clone(&ping_count);
        async move {
            if !accept_connect_send_connack(&mut framed_read, &mut framed_write).await {
                return;
            }

            if stall_publish && !send_qos0_publish(&mut framed_write, "keepalive/stall", b"stall").await {
                return;
            }

            loop {
                match timeout(idle, framed_read.next()).await {
                    Ok(Some(Ok((DecodedFrame::Control(Packet::PingRequest), _)))) => {
                        ping_count.fetch_add(1, Ordering::SeqCst);
                        if respond_pingresp && framed_write.send(Packet::PingResponse).await.is_err() {
                            return;
                        }
                        if liveness_publish && !send_qos0_publish(&mut framed_write, "ka/liveness", b"alive").await {
                            return;
                        }
                    }
                    Ok(Some(Ok(_))) => {
                        // Any other client packet also proves liveness — reset idle.
                    }
                    Ok(Some(Err(_))) | Ok(None) | Err(_) => {
                        // Idle without PINGREQ / peer closed — drop TCP (keepalive death).
                        return;
                    }
                }
            }
        }
    })
    .await
}

/// PUBLISH with Topic Alias property (empty topic + alias=1) after CONNACK.
pub(crate) async fn spawn_mock_broker_with_inbound_topic_alias() -> u16 {
    spawn_broker_loop(|mut framed_read, mut framed_write| async move {
        if !accept_connect_send_connack(&mut framed_read, &mut framed_write).await {
            return;
        }
        // QoS 0 PUBLISH: empty topic + Topic Alias property (1) + payload "d".
        let publish_with_alias = [0x30_u8, 0x07, 0x00, 0x00, 0x03, 0x23, 0x00, 0x01, b'd'];
        if framed_write.get_mut().write_all(&publish_with_alias).await.is_err() {
            return;
        }
        if framed_write.flush().await.is_err() {
            return;
        }
        while framed_read.next().await.is_some() {}
    })
    .await
}

/// Accepts CONNECT, CONNACK, then closes so the client observes Transport loss.
pub(crate) async fn spawn_mock_broker_close_after_connack() -> u16 {
    spawn_broker_loop(|mut framed_read, mut framed_write| async move {
        let _ = accept_connect_send_connack(&mut framed_read, &mut framed_write).await;
        // Drop TCP immediately so the session ends with Transport.
    })
    .await
}

/// Encode a QoS 1 PUBLISH onto the mock broker write half (PUBLISH is not a `Packet` variant).
pub(crate) async fn send_inbound_qos1_publish(
    framed_write: &mut MockFramedWrite,
    topic: &str,
    payload: &[u8],
    packet_id: NonZeroU16,
    properties: crate::codec::PublishProperties,
) {
    use crate::codec::{DEFAULT_MAX_PACKET_SIZE, PublishDelivery, encode_publish};
    use bytes::BytesMut;

    let message = PublishMessage {
        topic: bytestring::ByteString::from(topic.to_owned()),
        payload: bytes::Bytes::copy_from_slice(payload),
        properties,
    };
    let delivery = PublishDelivery {
        dup: false,
        retain: false,
        qos: crate::codec::QoS::AtLeastOnce,
        packet_id: Some(packet_id),
    };
    let mut buf = BytesMut::new();
    encode_publish(&message, delivery, DEFAULT_MAX_PACKET_SIZE, &mut buf).expect("encode");
    framed_write.get_mut().write_all(&buf).await.expect("write publish");
    framed_write.flush().await.expect("flush publish");
}

/// Wire a client against [`spawn_mock_broker`] with [`NoReconnect`].
pub(crate) async fn test_connect() -> (crate::MqttClient, Connection, watch::Receiver<SessionPhase>) {
    let port = spawn_mock_broker().await;
    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
    let options = ConnectOptions::new(url).receive_maximum(NonZeroU16::new(8).expect("nz"));
    let (tx, _rx) = flume::bounded::<PublishMessage>(4);
    // NoReconnect: single mock session — peer close must not spin ImmediateReconnect.
    connect(options, static_test_credentials(), NoReconnect, tx, NoopLifecycle).expect("connect wiring")
}

/// Wait until the phase watch reports a CONNACK-backed Connected session.
pub(crate) async fn await_connected(phase: &mut watch::Receiver<SessionPhase>) {
    let guard = timeout(
        Duration::from_secs(5),
        phase.wait_for(|p| {
            matches!(
                p,
                SessionPhase::Connected {
                    session: SessionParams { from_connack: true, .. },
                    ..
                }
            )
        }),
    )
    .await
    .expect("Connected timeout")
    .expect("phase watch closed");
    assert!(matches!(
        *guard,
        SessionPhase::Connected {
            session: SessionParams { from_connack: true, .. },
            ..
        }
    ));
}

/// Wait until the phase watch reports Disconnected; returns the observed phase.
pub(crate) async fn await_disconnected(phase: &mut watch::Receiver<SessionPhase>) -> SessionPhase {
    timeout(
        Duration::from_secs(5),
        phase.wait_for(|p| matches!(p, SessionPhase::Disconnected(_))),
    )
    .await
    .expect("Disconnected timeout")
    .expect("phase watch closed")
    .clone()
}
