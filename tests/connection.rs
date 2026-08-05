//! Connection and protocol integration tests without a real broker.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::{CountingInvalidate, NoReconnect, static_credentials, wait_for_phase};
use futures_util::{SinkExt, StreamExt};
use mqtt_glide::{
    AuthError, BrokerUrl, ClientId, ConnectOptions, ConnectionOutcome, DecodedFrame, Disconnect, DisconnectReasonCode,
    NoopLifecycle, Packet, ProtocolError, PublishMessage, QoS, SessionPhase, StandardReconnectPolicy, V5Codec, connect,
};
use secrecy::SecretString;
use tokio::net::TcpListener;
use tokio::time::timeout;
use tokio_util::codec::{FramedRead, FramedWrite};

/// Start a minimal MQTT 5 peer that sends CONNACK and then DISCONNECT.
///
/// Mosquitto cannot be configured to emit every DISCONNECT reason class needed
/// by the classification regression test. Failures panic (or surface via the
/// returned [`JoinHandle`]) instead of silent early-return + client timeout.
async fn start_broker_disconnect_peer(reason: DisconnectReasonCode) -> (u16, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind disconnect-reason peer");
    let port = listener.local_addr().expect("local addr").port();
    let peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("peer accept client");
        let (read_half, write_half) = stream.into_split();
        let mut framed_read = FramedRead::new(read_half, V5Codec::with_max_packet_size(0));
        let mut framed_write = FramedWrite::new(write_half, V5Codec::with_max_packet_size(0));

        match framed_read.next().await {
            Some(Ok((DecodedFrame::Control(Packet::Connect(_)), _))) => {}
            other => panic!("peer expected CONNECT, got {other:?}"),
        }

        framed_write
            .send(Packet::ConnectAck(Box::default()))
            .await
            .expect("peer send CONNACK");
        framed_write
            .send(Packet::Disconnect(Disconnect::new(reason)))
            .await
            .expect("peer send DISCONNECT");
    });
    (port, peer)
}

async fn join_disconnect_peer(peer: tokio::task::JoinHandle<()>) {
    timeout(Duration::from_secs(5), peer)
        .await
        .expect("disconnect peer timed out")
        .expect("disconnect peer panicked");
}

/// An unavailable endpoint must surface a transport failure, not a fake
/// `Connected` event.
#[tokio::test]
async fn unreachable_broker_returns_transport_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind ephemeral port");
    let port = listener.local_addr().expect("local addr").port();
    drop(listener);

    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid broker url");
    let options = ConnectOptions::new(url).client_id(ClientId::from_prefix("connect-down"));
    let (tx, _rx) = flume::bounded::<PublishMessage>(4);
    let (_client, connection, mut phase) =
        connect(options, static_credentials(), NoReconnect, tx, NoopLifecycle).expect("connect wiring");

    let run = tokio::spawn(connection.run());
    let first = wait_for_phase(&mut phase, Duration::from_secs(5), |p| {
        matches!(p, SessionPhase::Disconnected(_) | SessionPhase::Connected { .. })
    })
    .await;

    match first {
        SessionPhase::Disconnected(ConnectionOutcome::Transport(_)) => {}
        SessionPhase::Connected { session, .. } if !session.from_connack => {
            panic!("skeleton Connected without TCP — connection must attempt broker connect");
        }
        other => panic!("expected Disconnected(Transport(_)), got {other:?}"),
    }

    let outcome = timeout(Duration::from_secs(5), run)
        .await
        .expect("run must finish after transport failure")
        .expect("run task join");
    assert!(
        matches!(outcome, Err(ConnectionOutcome::Transport(_))),
        "unreachable broker must end run as Transport, got {outcome:?}"
    );
}

/// Broker-sent MQTT DISCONNECT reasons map to Auth, Transport, and Protocol.
#[tokio::test]
async fn broker_disconnect_reason_classifies_auth_transport_protocol() {
    {
        let (port, peer) = start_broker_disconnect_peer(DisconnectReasonCode::NotAuthorized).await;
        let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
        let options = ConnectOptions::new(url).client_id(ClientId::from_prefix("disc-auth"));
        let invalidations = Arc::new(AtomicUsize::new(0));
        let creds = CountingInvalidate {
            invalidations: Arc::clone(&invalidations),
        };
        let (tx, _rx) = flume::bounded::<PublishMessage>(4);
        let (_client, connection, mut phase) = connect(options, creds, NoReconnect, tx, NoopLifecycle).expect("wiring");
        let run = tokio::spawn(connection.run());

        // Broker DISCONNECT right after CONNACK — watch may skip Connected.
        let disconnected = wait_for_phase(&mut phase, Duration::from_secs(5), |event| {
            matches!(event, SessionPhase::Disconnected(_))
        })
        .await;
        assert!(matches!(
            disconnected,
            SessionPhase::Disconnected(ConnectionOutcome::Auth(AuthError::NotAuthorized))
        ));
        assert_eq!(invalidations.load(Ordering::SeqCst), 1);

        let outcome = timeout(Duration::from_secs(5), run)
            .await
            .expect("run join")
            .expect("join");
        assert!(matches!(
            outcome,
            Err(ConnectionOutcome::Auth(AuthError::NotAuthorized))
        ));
        join_disconnect_peer(peer).await;
    }

    {
        let (port, peer) = start_broker_disconnect_peer(DisconnectReasonCode::KeepAliveTimeout).await;
        let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
        let options = ConnectOptions::new(url).client_id(ClientId::from_prefix("disc-tx"));
        let (tx, _rx) = flume::bounded::<PublishMessage>(4);
        let (_client, connection, mut phase) =
            connect(options, static_credentials(), NoReconnect, tx, NoopLifecycle).expect("wiring");
        let run = tokio::spawn(connection.run());

        // Broker DISCONNECT right after CONNACK — watch may skip Connected.
        let disconnected = wait_for_phase(&mut phase, Duration::from_secs(5), |event| {
            matches!(event, SessionPhase::Disconnected(_))
        })
        .await;
        assert!(matches!(
            disconnected,
            SessionPhase::Disconnected(ConnectionOutcome::Transport(_))
        ));

        let outcome = timeout(Duration::from_secs(5), run)
            .await
            .expect("run join")
            .expect("join");
        assert!(matches!(outcome, Err(ConnectionOutcome::Transport(_))));
        join_disconnect_peer(peer).await;
    }

    {
        let (port, peer) = start_broker_disconnect_peer(DisconnectReasonCode::ProtocolError).await;
        let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
        let options = ConnectOptions::new(url).client_id(ClientId::from_prefix("disc-proto"));
        let (tx, _rx) = flume::bounded::<PublishMessage>(4);
        let (_client, connection, mut phase) =
            connect(options, static_credentials(), NoReconnect, tx, NoopLifecycle).expect("wiring");
        let run = tokio::spawn(connection.run());

        // Broker DISCONNECT right after CONNACK — watch may skip Connected.
        let disconnected = wait_for_phase(&mut phase, Duration::from_secs(5), |event| {
            matches!(event, SessionPhase::Disconnected(_))
        })
        .await;
        assert!(matches!(
            disconnected,
            SessionPhase::Disconnected(ConnectionOutcome::Protocol(ProtocolError::Violation(_)))
        ));

        let outcome = timeout(Duration::from_secs(5), run)
            .await
            .expect("run join")
            .expect("join");
        assert!(matches!(outcome, Err(ConnectionOutcome::Protocol(_))));
        join_disconnect_peer(peer).await;
    }
}

/// Credentials that block inside `invalidate()` until the test releases the latch.
struct LatchedInvalidate {
    started: Arc<AtomicUsize>,
    invalidations: Arc<AtomicUsize>,
    release: Arc<tokio::sync::Notify>,
}

impl mqtt_glide::CredentialsProvider for LatchedInvalidate {
    async fn credentials(&self) -> Result<(String, SecretString), AuthError> {
        Ok(("user".into(), SecretString::from("pass".to_owned())))
    }

    async fn invalidate(&self) {
        self.started.fetch_add(1, Ordering::SeqCst);
        self.release.notified().await;
        self.invalidations.fetch_add(1, Ordering::SeqCst);
    }
}

/// Auth failure on an active session with a retrying policy: the command channel must be
/// replenished before the (possibly slow) credential invalidation runs, and invalidation
/// must happen exactly once.
#[tokio::test]
async fn auth_failure_reconnect_replenishes_channel_before_invalidate() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind peer");
    let port = listener.local_addr().expect("local addr").port();
    let second_connect = Arc::new(tokio::sync::Notify::new());
    let peer = tokio::spawn({
        let second_connect = Arc::clone(&second_connect);
        async move {
            // Session 1: CONNECT → CONNACK → DISCONNECT(NotAuthorized).
            let (stream, _) = listener.accept().await.expect("peer accept #1");
            let (read_half, write_half) = stream.into_split();
            let mut framed_read = FramedRead::new(read_half, V5Codec::with_max_packet_size(0));
            let mut framed_write = FramedWrite::new(write_half, V5Codec::with_max_packet_size(0));
            match framed_read.next().await {
                Some(Ok((DecodedFrame::Control(Packet::Connect(_)), _))) => {}
                other => panic!("peer expected CONNECT #1, got {other:?}"),
            }
            framed_write
                .send(Packet::ConnectAck(Box::default()))
                .await
                .expect("peer send CONNACK");
            framed_write
                .send(Packet::Disconnect(Disconnect::new(DisconnectReasonCode::NotAuthorized)))
                .await
                .expect("peer send DISCONNECT");

            // Session 2 (reconnect after invalidate): observe the retry CONNECT, then hold.
            let (stream, _) = listener.accept().await.expect("peer accept #2");
            let (read_half, _write_half) = stream.into_split();
            let mut framed_read = FramedRead::new(read_half, V5Codec::with_max_packet_size(0));
            match framed_read.next().await {
                Some(Ok((DecodedFrame::Control(Packet::Connect(_)), _))) => {
                    second_connect.notify_one();
                }
                other => panic!("peer expected CONNECT #2, got {other:?}"),
            }
            // Hold the connection open until the test drops it.
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
    });

    let started = Arc::new(AtomicUsize::new(0));
    let invalidations = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(tokio::sync::Notify::new());
    let creds = LatchedInvalidate {
        started: Arc::clone(&started),
        invalidations: Arc::clone(&invalidations),
        release: Arc::clone(&release),
    };
    let policy = StandardReconnectPolicy::new(Duration::from_millis(300), Duration::from_secs(5));

    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
    let options = ConnectOptions::new(url)
        .client_id(ClientId::from_prefix("invalidate-order"))
        .connect_timeout(Duration::from_secs(1));
    let (tx, _rx) = flume::bounded::<PublishMessage>(4);
    let (client, connection, mut phase) = connect(options, creds, policy, tx, NoopLifecycle).expect("wiring");
    let run = tokio::spawn(connection.run());

    let disconnected = wait_for_phase(&mut phase, Duration::from_secs(5), |event| {
        matches!(event, SessionPhase::Disconnected(_))
    })
    .await;
    assert!(matches!(
        disconnected,
        SessionPhase::Disconnected(ConnectionOutcome::Auth(AuthError::NotAuthorized))
    ));

    // Wait until the driver is blocked inside the latched invalidate (after the backoff).
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while started.load(Ordering::SeqCst) == 0 {
        assert!(std::time::Instant::now() < deadline, "invalidate never started");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // While the credentials provider is refreshing, the replenished command channel must
    // accept application work (not AppError::CommandChannelClosed).
    let queued = client.start_publish(PublishMessage::new("invalidate/order", "x"), QoS::AtMostOnce, false);
    assert!(
        queued.is_ok(),
        "command channel must stay open during invalidation, got {queued:?}"
    );

    release.notify_one();

    // The retry must dial the broker again after invalidation.
    timeout(Duration::from_secs(5), second_connect.notified())
        .await
        .expect("client never reconnected after invalidate");
    assert_eq!(
        invalidations.load(Ordering::SeqCst),
        1,
        "invalidate must run exactly once"
    );

    client.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), run)
        .await
        .expect("run must finish after shutdown")
        .expect("run task join");
    peer.abort();
}
