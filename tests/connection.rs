//! Connection and protocol integration tests without a real broker.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::{
    CountingInvalidate, NoReconnect, join_peer, spawn_connack_then_disconnect, spawn_peer_broker, static_credentials,
    wait_for_phase, wait_until,
};
use mqtt_glide::{
    AuthError, BrokerUrl, ClientId, ConnectAck, ConnectOptions, ConnectionOutcome, DisconnectReasonCode, NoopLifecycle,
    ProtocolError, PublishMessage, QoS, SessionPhase, StandardReconnectPolicy, connect,
};
use secrecy::SecretString;
use tokio::net::TcpListener;
use tokio::time::timeout;

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
        let (port, peer) = spawn_connack_then_disconnect(DisconnectReasonCode::NotAuthorized).await;
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
        join_peer(peer).await;
    }

    {
        let (port, peer) = spawn_connack_then_disconnect(DisconnectReasonCode::KeepAliveTimeout).await;
        let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
        let options = ConnectOptions::new(url).client_id(ClientId::from_prefix("disc-tx"));
        let (tx, _rx) = flume::bounded::<PublishMessage>(4);
        let (_client, connection, mut phase) =
            connect(options, static_credentials(), NoReconnect, tx, NoopLifecycle).expect("wiring");
        let run = tokio::spawn(connection.run());

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
        join_peer(peer).await;
    }

    {
        let (port, peer) = spawn_connack_then_disconnect(DisconnectReasonCode::ProtocolError).await;
        let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
        let options = ConnectOptions::new(url).client_id(ClientId::from_prefix("disc-proto"));
        let (tx, _rx) = flume::bounded::<PublishMessage>(4);
        let (_client, connection, mut phase) =
            connect(options, static_credentials(), NoReconnect, tx, NoopLifecycle).expect("wiring");
        let run = tokio::spawn(connection.run());

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
        join_peer(peer).await;
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
    let second_connect = Arc::new(tokio::sync::Notify::new());
    let (port, peer) = spawn_peer_broker({
        let second_connect = Arc::clone(&second_connect);
        move |broker| async move {
            let mut session = broker.accept_session().await;
            session.expect_connect().await;
            session.send_connack_success().await;
            session.send_disconnect(DisconnectReasonCode::NotAuthorized).await;

            let mut session = broker.accept_session().await;
            session.expect_connect().await;
            second_connect.notify_one();
            session.hold().await;
        }
    })
    .await;

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

    wait_until(
        Duration::from_secs(5),
        || started.load(Ordering::SeqCst) > 0,
        "invalidate start",
    )
    .await;

    // While the credentials provider is refreshing, the replenished command channel must
    // accept application work (not AppError::CommandChannelClosed).
    let queued = client.start_publish(PublishMessage::new("invalidate/order", "x"), QoS::AtMostOnce, false);
    assert!(
        queued.is_ok(),
        "command channel must stay open during invalidation, got {queued:?}"
    );

    release.notify_one();

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

/// [MQTT-3.2.2-2]: Clean Start = 1 with Session Present = 1 stops the driver (no reconnect loop).
#[tokio::test]
async fn clean_start_session_present_stops_without_reconnect() {
    let (port, peer) = spawn_peer_broker(|broker| async move {
        let mut session = broker.accept_session().await;
        session.expect_connect().await;
        session
            .send_connack(ConnectAck {
                session_present: true,
                ..Default::default()
            })
            .await;
        assert!(
            !broker.try_accept_within(Duration::from_millis(150)).await,
            "must not reconnect after Session Present desync"
        );
    })
    .await;

    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
    let options = ConnectOptions::new(url)
        .client_id(ClientId::from_prefix("desync-clean"))
        .clean_start(true)
        .connect_timeout(Duration::from_secs(1));
    let policy = StandardReconnectPolicy::new(Duration::from_millis(50), Duration::from_secs(1));
    let (tx, _rx) = flume::bounded::<PublishMessage>(4);
    let (_client, connection, mut phase) =
        connect(options, static_credentials(), policy, tx, NoopLifecycle).expect("wiring");
    let run = tokio::spawn(connection.run());

    let disconnected = wait_for_phase(&mut phase, Duration::from_secs(5), |event| {
        matches!(event, SessionPhase::Disconnected(_))
    })
    .await;
    assert!(
        matches!(
            disconnected,
            SessionPhase::Disconnected(ConnectionOutcome::Application(
                mqtt_glide::AppError::SessionPresentInconsistent(_)
            ))
        ),
        "expected SessionPresentInconsistent stop, got {disconnected:?}"
    );

    let outcome = timeout(Duration::from_secs(5), run)
        .await
        .expect("run join")
        .expect("join");
    assert!(
        matches!(
            outcome,
            Err(ConnectionOutcome::Application(
                mqtt_glide::AppError::SessionPresentInconsistent(_)
            ))
        ),
        "driver must stop, got {outcome:?}"
    );
    join_peer(peer).await;
}

/// [MQTT-3.2.2-4]: Present=1 without prior affinity stops; after a successful resume session,
/// an empty registry still keeps affinity so Present=1 is accepted.
#[tokio::test]
async fn resume_affinity_survives_empty_registry() {
    let stage = Arc::new(AtomicUsize::new(0));
    let (port, peer) = spawn_peer_broker({
        let stage = Arc::clone(&stage);
        move |broker| async move {
            let mut session = broker.accept_session().await;
            let connect = session.expect_connect().await;
            assert!(!connect.clean_start);
            session.send_connack_success().await;
            stage.store(1, Ordering::SeqCst);
            session.send_disconnect(DisconnectReasonCode::NormalDisconnection).await;

            let mut session = broker.accept_session().await;
            session.expect_connect().await;
            session
                .send_connack(ConnectAck {
                    session_present: true,
                    ..Default::default()
                })
                .await;
            stage.store(2, Ordering::SeqCst);
            session.hold().await;
        }
    })
    .await;

    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
    let options = ConnectOptions::new(url)
        .client_id(ClientId::from_prefix("desync-affinity"))
        .clean_start(false)
        .session_expiry(Some(Duration::from_secs(60)))
        .connect_timeout(Duration::from_secs(1));
    let policy = StandardReconnectPolicy::new(Duration::from_millis(50), Duration::from_secs(1));
    let (tx, _rx) = flume::bounded::<PublishMessage>(4);
    let (client, connection, mut phase) =
        connect(options, static_credentials(), policy, tx, NoopLifecycle).expect("wiring");
    let run = tokio::spawn(connection.run());

    wait_for_phase(&mut phase, Duration::from_secs(5), |event| {
        matches!(event, SessionPhase::Connected { .. })
    })
    .await;

    wait_until(
        Duration::from_secs(5),
        || stage.load(Ordering::SeqCst) >= 2,
        "second session",
    )
    .await;

    let reconnected = wait_for_phase(&mut phase, Duration::from_secs(5), |event| {
        matches!(event, SessionPhase::Connected { .. })
    })
    .await;
    assert!(
        matches!(reconnected, SessionPhase::Connected { .. }),
        "empty-registry resume with Present=1 must stay connected, got {reconnected:?}"
    );

    client.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), run)
        .await
        .expect("run finish")
        .expect("join");
    peer.abort();
}

/// Lost broker session (`session_present = false`) must re-SUBSCRIBE pending filters.
#[tokio::test]
async fn lost_session_resubscribes_on_reconnect() {
    let saw_resubscribe = Arc::new(AtomicUsize::new(0));
    let (port, peer) = spawn_peer_broker({
        let saw_resubscribe = Arc::clone(&saw_resubscribe);
        move |broker| async move {
            let mut session = broker.accept_session().await;
            session.expect_connect().await;
            session.send_connack_success().await;
            session.expect_subscribe_grant_qos0().await;
            session.send_disconnect(DisconnectReasonCode::NormalDisconnection).await;

            let mut session = broker.accept_session().await;
            session.expect_connect().await;
            session.send_connack_success().await;
            session.expect_subscribe_grant_qos0().await;
            saw_resubscribe.fetch_add(1, Ordering::SeqCst);
            session.hold().await;
        }
    })
    .await;

    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
    let options = ConnectOptions::new(url)
        .client_id(ClientId::from_prefix("resub"))
        .clean_start(false)
        .session_expiry(Some(Duration::from_secs(60)))
        .connect_timeout(Duration::from_secs(1));
    let policy = StandardReconnectPolicy::new(Duration::from_millis(50), Duration::from_secs(1));
    let (tx, _rx) = flume::bounded::<PublishMessage>(4);
    let (client, connection, mut phase) =
        connect(options, static_credentials(), policy, tx, NoopLifecycle).expect("wiring");
    let run = tokio::spawn(connection.run());

    wait_for_phase(&mut phase, Duration::from_secs(5), |event| {
        matches!(event, SessionPhase::Connected { .. })
    })
    .await;
    client
        .subscribe([("resub/topic", QoS::AtMostOnce)])
        .await
        .expect("subscribe");

    wait_until(
        Duration::from_secs(5),
        || saw_resubscribe.load(Ordering::SeqCst) > 0,
        "auto-resubscribe",
    )
    .await;

    client.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), run)
        .await
        .expect("run finish")
        .expect("join");
    peer.abort();
}
