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
    NoopLifecycle, Packet, ProtocolError, PublishMessage, SessionPhase, V5Codec, connect,
};
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
