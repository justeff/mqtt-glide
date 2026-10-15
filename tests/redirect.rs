//! Server redirection (§4.11) against scripted in-process brokers.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::{NoReconnect, PeerSession, join_peer, spawn_peer_broker, static_credentials, wait_for_phase};
use futures_util::SinkExt;
use mqtt_glide::codec::{ConnectAck, ConnectAckReason, Disconnect, Packet};
use mqtt_glide::{
    BrokerUrl, ClientId, ConnectOptions, ConnectionOutcome, DisconnectReasonCode, InboundMessage, NoopLifecycle,
    ReconnectPolicy, SessionPhase, connect,
};
use tokio::sync::mpsc;
use tokio::time::timeout;

/// Reconnects at once after anything but shutdown.
#[derive(Debug, Clone, Copy)]
struct Immediately;

impl ReconnectPolicy for Immediately {
    fn delay_after(&self, outcome: &ConnectionOutcome) -> Option<Duration> {
        match outcome {
            ConnectionOutcome::Shutdown(_) | ConnectionOutcome::Application(_) => None,
            _ => Some(Duration::ZERO),
        }
    }
}

fn options(port: u16, follow: bool) -> ConnectOptions {
    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
    ConnectOptions::new(url)
        .client_id(ClientId::from_prefix("redirect"))
        .connect_timeout(Duration::from_secs(1))
        .follow_server_redirects(follow)
}

async fn refuse_with_redirect(session: &mut PeerSession, reason: ConnectAckReason, reference: String) {
    session.expect_connect().await;
    session
        .send_connack(ConnectAck {
            reason_code: reason,
            server_reference: Some(reference.into()),
            ..ConnectAck::default()
        })
        .await;
}

async fn disconnect_with_redirect(session: &mut PeerSession, reason: DisconnectReasonCode, reference: String) {
    session
        .write
        .send(Packet::Disconnect(Disconnect {
            reason_code: reason,
            server_reference: Some(reference.into()),
            ..Disconnect::default()
        }))
        .await
        .expect("send DISCONNECT");
}

/// A CONNACK 0x9C with a Server Reference sends the next CONNECT to that server at once, even
/// when the reconnect policy would not retry.
#[tokio::test]
async fn connack_redirect_is_followed_at_once() {
    let (target_port, target) = spawn_peer_broker(|broker| async move {
        let mut session = broker.accept_session().await;
        session.expect_connect().await;
        session.send_connack_success().await;
        session.hold().await;
    })
    .await;
    let (port, origin) = spawn_peer_broker(move |broker| async move {
        let mut session = broker.accept_session().await;
        refuse_with_redirect(
            &mut session,
            ConnectAckReason::UseAnotherServer,
            format!("127.0.0.1:{target_port}"),
        )
        .await;
    })
    .await;

    let (tx, _rx) = mpsc::channel::<InboundMessage>(4);
    let (client, connection, mut phase) = connect(
        options(port, true),
        static_credentials(),
        NoReconnect,
        tx,
        NoopLifecycle,
    )
    .expect("wiring");
    let run = tokio::spawn(connection.run());

    wait_for_phase(&mut phase, Duration::from_secs(5), |event| {
        matches!(event, SessionPhase::Connected { .. })
    })
    .await;
    assert!(client.is_connected());

    client.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), run).await.expect("run join");
    join_peer(origin).await;
    target.abort();
}

/// Without `follow_server_redirects` a redirecting CONNACK is a refusal like any other, and its
/// Server Reference is reported.
#[tokio::test]
async fn connack_redirect_is_reported_when_not_followed() {
    let (port, origin) = spawn_peer_broker(|broker| async move {
        let mut session = broker.accept_session().await;
        refuse_with_redirect(
            &mut session,
            ConnectAckReason::ServerMoved,
            "elsewhere.example.com".into(),
        )
        .await;
    })
    .await;

    let (tx, _rx) = mpsc::channel::<InboundMessage>(4);
    let (_client, connection, _phase) = connect(
        options(port, false),
        static_credentials(),
        NoReconnect,
        tx,
        NoopLifecycle,
    )
    .expect("wiring");
    let outcome = timeout(Duration::from_secs(5), connection.run()).await.expect("run");
    let message = format!("{outcome:?}");
    assert!(
        matches!(outcome, Err(ConnectionOutcome::Protocol(_))),
        "refusal expected, got {message}"
    );
    assert!(
        message.contains("elsewhere.example.com"),
        "reference reported: {message}"
    );
    join_peer(origin).await;
}

/// DISCONNECT 0x9D (Server moved) points every later connection at the new server; the
/// original broker never sees the client again.
#[tokio::test]
async fn server_moved_is_permanent() {
    let target_sessions = Arc::new(AtomicUsize::new(0));
    let (target_port, target) = spawn_peer_broker({
        let target_sessions = Arc::clone(&target_sessions);
        move |broker| async move {
            for _ in 0..2 {
                let mut session = broker.accept_session().await;
                session.expect_connect().await;
                session.send_connack_success().await;
                target_sessions.fetch_add(1, Ordering::SeqCst);
                // First session: drop the connection so the client reconnects.
                if target_sessions.load(Ordering::SeqCst) == 1 {
                    drop(session);
                } else {
                    session.hold().await;
                }
            }
        }
    })
    .await;
    let (port, origin) = spawn_peer_broker(move |broker| async move {
        let mut session = broker.accept_session().await;
        session.expect_connect().await;
        session.send_connack_success().await;
        disconnect_with_redirect(
            &mut session,
            DisconnectReasonCode::ServerMoved,
            format!("127.0.0.1:{target_port}"),
        )
        .await;
        assert!(
            !broker.try_accept_within(Duration::from_millis(500)).await,
            "a moved client must not come back"
        );
    })
    .await;

    let (tx, _rx) = mpsc::channel::<InboundMessage>(4);
    let (client, connection, _phase) = connect(
        options(port, true),
        static_credentials(),
        Immediately,
        tx,
        NoopLifecycle,
    )
    .expect("wiring");
    let run = tokio::spawn(connection.run());

    common::wait_until(
        Duration::from_secs(5),
        || target_sessions.load(Ordering::SeqCst) == 2,
        "second session on the new server",
    )
    .await;
    join_peer(origin).await;
    client.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), run).await.expect("run join");
    target.abort();
}

/// DISCONNECT 0x9C (Use another server) only redirects the next connection; after that the
/// client returns to its configured broker.
#[tokio::test]
async fn use_another_server_is_one_connection_only() {
    let (target_port, target) = spawn_peer_broker(|broker| async move {
        let mut session = broker.accept_session().await;
        session.expect_connect().await;
        session.send_connack_success().await;
        drop(session);
    })
    .await;
    let returned = Arc::new(AtomicUsize::new(0));
    let (port, origin) = spawn_peer_broker({
        let returned = Arc::clone(&returned);
        move |broker| async move {
            let mut session = broker.accept_session().await;
            session.expect_connect().await;
            session.send_connack_success().await;
            disconnect_with_redirect(
                &mut session,
                DisconnectReasonCode::UseAnotherServer,
                format!("127.0.0.1:{target_port}"),
            )
            .await;
            let mut back = broker.accept_session().await;
            back.expect_connect().await;
            returned.fetch_add(1, Ordering::SeqCst);
            back.send_connack_success().await;
            back.hold().await;
        }
    })
    .await;

    let (tx, _rx) = mpsc::channel::<InboundMessage>(4);
    let (client, connection, _phase) = connect(
        options(port, true),
        static_credentials(),
        Immediately,
        tx,
        NoopLifecycle,
    )
    .expect("wiring");
    let run = tokio::spawn(connection.run());

    common::wait_until(
        Duration::from_secs(5),
        || returned.load(Ordering::SeqCst) == 1,
        "back on the configured broker",
    )
    .await;
    join_peer(target).await;
    client.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), run).await.expect("run join");
    origin.abort();
}

/// A broker that keeps redirecting (here: to itself) is followed five times in a row, then the
/// reconnect policy decides again (here: stop).
#[tokio::test]
async fn redirects_in_a_row_are_capped() {
    let connects = Arc::new(AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let origin = tokio::spawn({
        let connects = Arc::clone(&connects);
        async move {
            loop {
                let (stream, _) = listener.accept().await.expect("accept");
                let (read, write) = common::framed_halves(stream);
                let mut session = PeerSession { read, write };
                connects.fetch_add(1, Ordering::SeqCst);
                refuse_with_redirect(
                    &mut session,
                    ConnectAckReason::UseAnotherServer,
                    format!("127.0.0.1:{port}"),
                )
                .await;
            }
        }
    });

    let (tx, _rx) = mpsc::channel::<InboundMessage>(4);
    let (_client, connection, _phase) = connect(
        options(port, true),
        static_credentials(),
        NoReconnect,
        tx,
        NoopLifecycle,
    )
    .expect("wiring");
    let outcome = timeout(Duration::from_secs(5), connection.run()).await.expect("run");
    assert!(matches!(outcome, Err(ConnectionOutcome::Protocol(_))), "{outcome:?}");
    assert_eq!(connects.load(Ordering::SeqCst), 6, "first attempt plus five redirects");
    origin.abort();
}
