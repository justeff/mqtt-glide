//! Connection and protocol integration tests without a real broker.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::{
    CountingInvalidate, NoReconnect, TestClient, join_peer, local_options, local_url, quick_retry,
    spawn_connack_then_disconnect, spawn_peer_broker, static_credentials, wait_for_phase, wait_until,
};
use std::num::NonZeroU16;

use futures_util::{SinkExt, StreamExt};
use mqtt_glide::Password;
use mqtt_glide::codec::{ConnectAck, DecodedFrame, Packet, PublishDelivery, encode_publish};
use mqtt_glide::{
    AuthError, ClientId, ConnectAckReason, ConnectOptions, ConnectionOutcome, DEFAULT_MAX_PACKET_SIZE,
    DisconnectReasonCode, InboundMessage, NoopLifecycle, ProtocolError, Publish, PublishMessage, QoS, SessionPhase,
    StandardReconnectPolicy, connect,
};
use rstest::rstest;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::time::timeout;

/// An unavailable endpoint must surface a transport failure, not a fake
/// `Connected` event.
#[tokio::test]
async fn unreachable_broker_returns_transport_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind ephemeral port");
    let port = listener.local_addr().expect("local addr").port();
    drop(listener);

    let (tx, _rx) = mpsc::channel(4);
    let mut started = TestClient::spawn(
        local_options(port, "connect-down"),
        static_credentials(),
        NoReconnect,
        tx,
    );
    let first = wait_for_phase(&mut started.phase, Duration::from_secs(5), |p| {
        matches!(p, SessionPhase::Disconnected(_) | SessionPhase::Connected { .. })
    })
    .await;

    match first {
        SessionPhase::Disconnected(ConnectionOutcome::Transport(_)) => {}
        other => panic!("expected Disconnected(Transport(_)), got {other:?}"),
    }

    let outcome = timeout(Duration::from_secs(5), started.run)
        .await
        .expect("run must finish after transport failure")
        .expect("run task join");
    assert!(
        matches!(outcome, Err(ConnectionOutcome::Transport(_))),
        "unreachable broker must end run as Transport, got {outcome:?}"
    );
}

/// Broker-sent MQTT DISCONNECT reasons map to Auth, Transport, and Protocol; only the Auth one
/// invalidates the credentials.
#[rstest]
#[case::not_authorized(
    DisconnectReasonCode::NotAuthorized,
    |o: &ConnectionOutcome| matches!(o, ConnectionOutcome::Auth(AuthError::NotAuthorized)),
    1
)]
#[case::keep_alive_timeout(
    DisconnectReasonCode::KeepAliveTimeout,
    |o: &ConnectionOutcome| matches!(o, ConnectionOutcome::Transport(_)),
    0
)]
#[case::protocol_error(
    DisconnectReasonCode::ProtocolError,
    |o: &ConnectionOutcome| matches!(o, ConnectionOutcome::Protocol(ProtocolError::Violation(_))),
    0
)]
#[tokio::test]
async fn broker_disconnect_reason_classifies_auth_transport_protocol(
    #[case] reason: DisconnectReasonCode,
    #[case] expected: fn(&ConnectionOutcome) -> bool,
    #[case] expected_invalidations: usize,
) {
    let (port, peer) = spawn_connack_then_disconnect(reason).await;
    let invalidations = Arc::new(AtomicUsize::new(0));
    let creds = CountingInvalidate {
        invalidations: Arc::clone(&invalidations),
    };
    let (tx, _rx) = mpsc::channel(4);
    let mut started = TestClient::spawn(local_options(port, "disc-reason"), creds, NoReconnect, tx);

    // Broker DISCONNECT right after CONNACK — watch may skip Connected.
    let disconnected = wait_for_phase(&mut started.phase, Duration::from_secs(5), |event| {
        matches!(event, SessionPhase::Disconnected(_))
    })
    .await;
    assert!(
        matches!(&disconnected, SessionPhase::Disconnected(outcome) if expected(outcome)),
        "{reason:?}: {disconnected:?}"
    );
    assert_eq!(invalidations.load(Ordering::SeqCst), expected_invalidations);

    let outcome = timeout(Duration::from_secs(5), started.run)
        .await
        .expect("run join")
        .expect("join");
    assert!(
        matches!(&outcome, Err(outcome) if expected(outcome)),
        "{reason:?}: {outcome:?}"
    );
    join_peer(peer).await;
}

/// Credentials that block inside `invalidate()` until the test releases the latch.
struct LatchedInvalidate {
    started: Arc<AtomicUsize>,
    invalidations: Arc<AtomicUsize>,
    release: Arc<tokio::sync::Notify>,
}

impl mqtt_glide::CredentialsProvider for LatchedInvalidate {
    async fn credentials(&self) -> Result<(String, Password), AuthError> {
        Ok(("user".into(), Password::from("pass")))
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
            let mut session = broker.accept_connected().await;
            session.send_disconnect(DisconnectReasonCode::NotAuthorized).await;

            let mut session = broker.accept_session().await;
            session.expect_connect().await;
            second_connect.notify_one();
            session.hold().await;
        }
    })
    .await;

    let invalidate_started = Arc::new(AtomicUsize::new(0));
    let invalidations = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(tokio::sync::Notify::new());
    let creds = LatchedInvalidate {
        started: Arc::clone(&invalidate_started),
        invalidations: Arc::clone(&invalidations),
        release: Arc::clone(&release),
    };
    let policy = StandardReconnectPolicy::new(Duration::from_millis(300), Duration::from_secs(5));

    let options = local_options(port, "invalidate-order").connect_timeout(Duration::from_secs(1));
    let (tx, _rx) = mpsc::channel(4);
    let mut started = TestClient::spawn(options, creds, policy, tx);

    let disconnected = wait_for_phase(&mut started.phase, Duration::from_secs(5), |event| {
        matches!(event, SessionPhase::Disconnected(_))
    })
    .await;
    assert!(matches!(
        disconnected,
        SessionPhase::Disconnected(ConnectionOutcome::Auth(AuthError::NotAuthorized))
    ));

    wait_until(
        Duration::from_secs(5),
        || invalidate_started.load(Ordering::SeqCst) > 0,
        "invalidate start",
    )
    .await;

    // While the credentials provider is refreshing, the replenished command channel must
    // accept application work (not AppError::CommandChannelClosed).
    let queued = started.client.try_start_publish(Publish::new("invalidate/order", "x"));
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

    started.shutdown().await;
    peer.abort();
}

/// A CONNACK refusing the credentials invalidates them once: the failed attempt does not
/// invalidate them again when it ends.
#[tokio::test]
async fn refused_connack_invalidates_credentials_once() {
    let (port, peer) = spawn_peer_broker(|broker| async move {
        let mut session = broker.accept_session().await;
        session.expect_connect().await;
        session
            .send_connack(ConnectAck {
                reason_code: ConnectAckReason::NotAuthorized,
                ..Default::default()
            })
            .await;
    })
    .await;

    let options = local_options(port, "refused-creds");
    let invalidations = Arc::new(AtomicUsize::new(0));
    let creds = CountingInvalidate {
        invalidations: Arc::clone(&invalidations),
    };
    let (tx, _rx) = mpsc::channel::<InboundMessage>(4);
    let (_client, connection, _phase) = connect(options, creds, NoReconnect, tx, NoopLifecycle).expect("wiring");

    let outcome = timeout(Duration::from_secs(5), connection.run())
        .await
        .expect("run finishes");
    assert!(matches!(
        outcome,
        Err(ConnectionOutcome::Auth(AuthError::NotAuthorized))
    ));
    assert_eq!(invalidations.load(Ordering::SeqCst), 1);
    join_peer(peer).await;
}

/// [MQTT-3.2.2-2]: Clean Start = 1 with Session Present = 1 stops the driver (no reconnect loop).
/// Peer observes Protocol Error DISCONNECT before TCP close.
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
        session.expect_disconnect(DisconnectReasonCode::ProtocolError).await;
        assert!(
            !broker.try_accept_within(Duration::from_millis(150)).await,
            "must not reconnect after Session Present desync"
        );
    })
    .await;

    let options = local_options(port, "desync-clean")
        .clean_start(true)
        .connect_timeout(Duration::from_secs(1));
    let policy = StandardReconnectPolicy::new(Duration::from_millis(50), Duration::from_secs(1));
    let (tx, _rx) = mpsc::channel(4);
    let mut started = TestClient::spawn(options, static_credentials(), policy, tx);

    let disconnected = wait_for_phase(&mut started.phase, Duration::from_secs(5), |event| {
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

    let outcome = timeout(Duration::from_secs(5), started.run)
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

/// [MQTT-3.2.2-4]: a client connecting with Clean Start = 0 holds Session State (empty after
/// a process restart), so the first connection of a process resumes the broker's session:
/// Session Present = 1 is accepted and messages the broker queued for it are delivered,
/// although this process never subscribed to them.
#[tokio::test]
async fn first_connect_resumes_an_existing_broker_session() {
    let (port, peer) = spawn_peer_broker(|broker| async move {
        let mut session = broker.accept_session().await;
        let connect = session.expect_connect().await;
        assert!(!connect.clean_start);
        session
            .send_connack(ConnectAck {
                session_present: true,
                ..Default::default()
            })
            .await;
        // A message queued for the session while the client was gone.
        let delivery = PublishDelivery {
            dup: false,
            retain: false,
            qos: QoS::AtLeastOnce,
            packet_id: NonZeroU16::new(7),
        };
        encode_publish(
            &PublishMessage::new("queued/while/offline", "hello"),
            delivery,
            DEFAULT_MAX_PACKET_SIZE,
            session.write.write_buffer_mut(),
        )
        .expect("encode");
        session.write.flush().await.expect("flush");
        match session.read.next().await {
            Some(Ok((DecodedFrame::Control(Packet::PublishAck(ack)), _))) => assert_eq!(ack.packet_id.get(), 7),
            other => panic!("expected PUBACK, got {other:?}"),
        }
        session.hold().await;
    })
    .await;

    let options = ConnectOptions::new(local_url(port))
        .client_id(ClientId::try_new("restarted-service").expect("client id"))
        .clean_start(false)
        .session_expiry(Some(Duration::from_secs(60)))
        .connect_timeout(Duration::from_secs(1));
    let (tx, mut rx) = mpsc::channel(4);
    let mut started = TestClient::spawn(options, static_credentials(), NoReconnect, tx);

    wait_for_phase(
        &mut started.phase,
        Duration::from_secs(5),
        |event| matches!(event, SessionPhase::Connected { session, .. } if session.session_present),
    )
    .await;
    let message = timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("queued message in time")
        .expect("sink open");
    assert_eq!(&*message.topic, "queued/while/offline");
    assert!(started.client.is_connected(), "resuming must not end the session");

    started.shutdown().await;
    peer.abort();
}

/// Lost broker session (`session_present = false`) must re-SUBSCRIBE pending filters.
#[tokio::test]
async fn lost_session_resubscribes_on_reconnect() {
    let saw_resubscribe = Arc::new(AtomicUsize::new(0));
    let (port, peer) = spawn_peer_broker({
        let saw_resubscribe = Arc::clone(&saw_resubscribe);
        move |broker| async move {
            let mut session = broker.accept_connected().await;
            session.expect_subscribe_grant_qos0().await;
            session.send_disconnect(DisconnectReasonCode::NormalDisconnection).await;

            let mut session = broker.accept_connected().await;
            session.expect_subscribe_grant_qos0().await;
            saw_resubscribe.fetch_add(1, Ordering::SeqCst);
            session.hold().await;
        }
    })
    .await;

    let options = local_options(port, "resub")
        .clean_start(false)
        .session_expiry(Some(Duration::from_secs(60)))
        .connect_timeout(Duration::from_secs(1));
    let policy = StandardReconnectPolicy::new(Duration::from_millis(50), Duration::from_secs(1));
    let (tx, _rx) = mpsc::channel(4);
    let started = TestClient::connected(options, static_credentials(), policy, tx).await;
    started
        .client
        .subscribe([("resub/topic", QoS::AtMostOnce)])
        .await
        .expect("subscribe");

    wait_until(
        Duration::from_secs(5),
        || saw_resubscribe.load(Ordering::SeqCst) > 0,
        "auto-resubscribe",
    )
    .await;

    started.shutdown().await;
    peer.abort();
}

/// A QoS 1 publish the broker answers with a DISCONNECT each time (an ACL refusal) is given up
/// after three sends instead of being resent on every reconnect for ever, and the publishes
/// behind it go on. The broker acknowledges in order, so the oldest unacknowledged publish is
/// the one it stopped at.
#[tokio::test]
async fn publish_the_broker_disconnects_on_is_given_up() {
    let connects = Arc::new(AtomicUsize::new(0));
    let refused_sends = Arc::new(AtomicUsize::new(0));
    let (port, broker) = spawn_peer_broker({
        let connects = Arc::clone(&connects);
        let refused_sends = Arc::clone(&refused_sends);
        move |broker| async move {
            loop {
                let mut session = broker.accept_session().await;
                let index = connects.fetch_add(1, Ordering::SeqCst);
                let refused_sends = Arc::clone(&refused_sends);
                tokio::spawn(async move {
                    session.expect_connect().await;
                    session
                        .send_connack(ConnectAck {
                            session_present: index > 0,
                            ..ConnectAck::default()
                        })
                        .await;
                    while let Some(Ok((frame, _))) = session.read.next().await {
                        let DecodedFrame::Publish(packet) = frame else {
                            continue;
                        };
                        if &*packet.message.topic == "forbidden" {
                            refused_sends.fetch_add(1, Ordering::SeqCst);
                            session.send_disconnect(DisconnectReasonCode::NotAuthorized).await;
                            return;
                        }
                        session.send_puback(packet.meta.packet_id.expect("QoS 1")).await;
                    }
                });
            }
        }
    })
    .await;

    let options = ConnectOptions::new(local_url(port))
        .client_id(ClientId::try_new("refused-publish").expect("client id"))
        .clean_start(false)
        .session_expiry(Some(Duration::from_secs(3600)));
    let (tx, _rx) = mpsc::channel(4);
    let started = TestClient::connected(options, static_credentials(), quick_retry(), tx).await;
    let client = &started.client;

    let refused = timeout(
        Duration::from_secs(5),
        client.publish("forbidden", "x").qos(QoS::AtLeastOnce).ack_timeout(None),
    )
    .await
    .expect("given up in time")
    .expect_err("refused");
    assert_eq!(
        refused.error,
        mqtt_glide::AppError::PublishRefusedByDisconnect(DisconnectReasonCode::NotAuthorized)
    );
    assert_eq!(refused_sends.load(Ordering::SeqCst), 3);

    timeout(
        Duration::from_secs(5),
        client.publish("allowed", "y").qos(QoS::AtLeastOnce),
    )
    .await
    .expect("in time")
    .expect("the next publish goes through");
    assert_eq!(refused_sends.load(Ordering::SeqCst), 3, "not sent again");
    assert_eq!(connects.load(Ordering::SeqCst), 4);

    started.shutdown().await;
    broker.abort();
}

/// A PUBLISH larger than the link carries in a Keep Alive is no dead broker while its bytes
/// keep going out: the write stall limit (1.5 x Keep Alive) is for a flush that gets nothing
/// out. Here 2 MiB leave at about 640 KiB/s, so the flush takes about twice that limit.
#[tokio::test]
async fn slow_but_progressing_publish_keeps_the_connection() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const PAYLOAD_LEN: usize = 2 * 1024 * 1024;
    // Fixed header (1 + 4 bytes Remaining Length), topic "big/blob" (2 + 8), no properties (1).
    const PUBLISH_WIRE_LEN: usize = 5 + 10 + 1 + PAYLOAD_LEN;

    let (port, broker) = spawn_peer_broker(|broker| async move {
        let session = broker.accept_connected().await;
        // Read raw and slowly; answer the PINGREQs (0xC0 0x00) that follow the publish.
        let mut raw = session.read.into_inner();
        let mut raw_write = session.write.into_inner();
        let mut chunk = vec![0u8; 64 * 1024];
        let mut offset = 0;
        loop {
            match raw.read(&mut chunk).await {
                Ok(0) | Err(_) => return,
                Ok(read) => {
                    for (index, &byte) in chunk[..read].iter().enumerate() {
                        if offset + index >= PUBLISH_WIRE_LEN && byte == 0xC0 {
                            let _ = raw_write.write_all(&[0xD0, 0x00]).await;
                        }
                    }
                    offset += read;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    })
    .await;

    let options = local_options(port, "slow-out")
        .keep_alive(Duration::from_secs(1))
        .tcp_send_buffer_size(std::num::NonZeroU32::new(16 * 1024).expect("nz"));
    let (tx, _rx) = mpsc::channel(4);
    let mut started = TestClient::connected(options, static_credentials(), NoReconnect, tx).await;

    let publish = tokio::spawn({
        let client = started.client.clone();
        async move {
            client
                .publish("big/blob", bytes::Bytes::from(vec![0x55u8; PAYLOAD_LEN]))
                .await
        }
    });
    let lost = timeout(
        Duration::from_secs(4),
        started
            .phase
            .wait_for(|phase| matches!(phase, SessionPhase::Disconnected(_))),
    )
    .await
    .map(|seen| seen.map(|phase| phase.clone()));
    assert!(
        lost.is_err(),
        "connection closed while the publish was going out: {lost:?}"
    );
    timeout(Duration::from_secs(5), publish)
        .await
        .expect("in time")
        .expect("join")
        .expect("written");

    started.shutdown().await;
    broker.abort();
}

/// An inbound PUBLISH larger than the link carries in a Keep Alive is no dead broker while its
/// bytes keep coming: the PINGRESP waits behind it, but every byte counts as traffic. Here
/// 2 MiB arrive at about 640 KiB/s, about three Keep Alive intervals.
#[tokio::test]
async fn slow_but_progressing_inbound_publish_keeps_the_connection() {
    use tokio::io::AsyncWriteExt;

    let (port, broker) = spawn_peer_broker(|broker| async move {
        let mut session = broker.accept_connected().await;
        session.feed_publish("big/in", vec![7u8; 2 * 1024 * 1024], false);
        let wire = session.write.write_buffer_mut().split();
        // Take the client's PINGREQs so its writes never block.
        let mut read = session.read;
        tokio::spawn(async move { while let Some(Ok(_)) = read.next().await {} });
        let mut raw = session.write.into_inner();
        for chunk in wire.chunks(64 * 1024) {
            if raw.write_all(chunk).await.is_err() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        std::future::pending::<()>().await;
    })
    .await;

    let options = local_options(port, "slow-in")
        .keep_alive(Duration::from_secs(1))
        .max_packet_size(std::num::NonZeroU32::new(4 * 1024 * 1024).expect("nz"));
    let (tx, mut rx) = mpsc::channel(4);
    let started = TestClient::connected(options, static_credentials(), NoReconnect, tx).await;

    let message = timeout(Duration::from_secs(8), rx.recv())
        .await
        .expect("delivered in time")
        .expect("connection kept");
    // Delivered whole is the proof the connection held: a closed one delivers nothing, and the
    // mock never answers PINGREQ, so `is_connected` afterwards would only measure the next tick.
    assert_eq!(message.payload.len(), 2 * 1024 * 1024);

    started.shutdown().await;
    broker.abort();
}

/// A broker DISCONNECT while the client restores its subscriptions ends the connection as it
/// would a running session: its reason reaches the reconnect policy (a session taken over is
/// no client protocol error) and the client sends no DISCONNECT back ([MQTT-3.14.4-1]).
#[tokio::test]
async fn broker_disconnect_during_resubscribe_keeps_its_reason() {
    let (sent_back_tx, sent_back_rx) = tokio::sync::oneshot::channel();
    let (port, peer) = spawn_peer_broker(move |broker| async move {
        broker.accept_connected().await.expect_subscribe_grant_qos0().await;

        // Session Present = 0: the client restores the subscription, and the broker hands the
        // session to another client instead of answering.
        let mut session = broker.accept_connected().await;
        let _subscribe = session.read.next().await;
        session.send_disconnect(DisconnectReasonCode::SessionTakenOver).await;
        let next = timeout(Duration::from_secs(2), session.read.next()).await;
        let _ = sent_back_tx.send(format!("{next:?}"));
        broker.accept_session().await.hold().await;
    })
    .await;

    let policy = StandardReconnectPolicy::new(Duration::from_millis(20), Duration::from_secs(30));
    let (tx, _rx) = mpsc::channel(4);
    let mut started = TestClient::connected(local_options(port, "taken-over"), static_credentials(), policy, tx).await;
    started
        .client
        .subscribe([("a/b", QoS::AtMostOnce)])
        .await
        .expect("subscribe");

    let taken_over = |phase: &SessionPhase| {
        matches!(
            phase,
            SessionPhase::Disconnected(ConnectionOutcome::Transport(
                mqtt_glide::TransportError::BrokerDisconnect {
                    reason: DisconnectReasonCode::SessionTakenOver,
                    ..
                }
            ))
        )
    };
    let mut seen = Vec::new();
    timeout(Duration::from_secs(5), async {
        loop {
            started.phase.changed().await.expect("driver alive");
            let current = started.phase.borrow_and_update().clone();
            if taken_over(&current) {
                return;
            }
            seen.push(current);
        }
    })
    .await
    .unwrap_or_else(|_| panic!("no SessionTakenOver outcome; phases: {seen:?}"));
    let sent_back = timeout(Duration::from_secs(3), sent_back_rx)
        .await
        .expect("in time")
        .expect("peer alive");
    assert!(
        sent_back.contains("None"),
        "the client answered the DISCONNECT: {sent_back}"
    );

    started.shutdown().await;
    peer.abort();
}

/// Waiting for room in a full inbound sink while restoring subscriptions does not count
/// against the SUBACK deadline: a slow consumer is no broker that does not answer, and the
/// client used to reconnect into the same messages again and again.
#[tokio::test]
async fn slow_consumer_during_resubscribe_keeps_the_connection() {
    let connects = Arc::new(AtomicUsize::new(0));
    let (port, broker) = spawn_peer_broker({
        let connects = Arc::clone(&connects);
        move |broker| async move {
            loop {
                let mut session = broker.accept_session().await;
                let index = connects.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(async move {
                    session.expect_connect().await;
                    session.send_connack_success().await;
                    if index == 0 {
                        session.expect_subscribe_grant_qos0().await;
                        session.expect_subscribe_grant_qos0().await;
                        return;
                    }
                    // Both SUBSCRIBEs of the restore, then retained messages for the first
                    // filter between the two SUBACKs.
                    let ids = session.expect_subscribe_ids(2).await;
                    session.send_suback_qos0(ids[0], 1).await;
                    for payload in ["r1", "r2", "r3"] {
                        session.feed_publish("a/retained", payload, true);
                    }
                    session.send_suback_qos0(ids[1], 1).await;
                    while let Some(Ok(_)) = session.read.next().await {}
                });
            }
        }
    })
    .await;

    let options = local_options(port, "slow-sink").ack_timeout(Duration::from_secs(1));
    let (tx, mut rx) = mpsc::channel(1);
    let mut started = TestClient::connected(options, static_credentials(), quick_retry(), tx).await;
    for filter in ["a/#", "b/#"] {
        started
            .client
            .subscribe([(filter, QoS::AtMostOnce)])
            .await
            .expect("subscribe");
    }

    // One message per 1.5 s: the three retained ones hold the restore for well over the 1 s
    // SUBACK deadline, which still leaves room for a slow scheduler.
    let mut got = Vec::new();
    while got.len() < 3 {
        let message = timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("in time")
            .expect("open");
        got.push(String::from_utf8_lossy(&message.payload).into_owned());
        tokio::time::sleep(Duration::from_millis(1500)).await;
    }
    assert_eq!(got, ["r1", "r2", "r3"]);
    wait_for_phase(&mut started.phase, Duration::from_secs(2), |phase| {
        matches!(phase, SessionPhase::Connected { .. })
    })
    .await;
    assert_eq!(connects.load(Ordering::SeqCst), 2, "no reconnect after the restore");

    started.shutdown().await;
    broker.abort();
}

/// A broker that refuses the client with DISCONNECT 0x87 while subscriptions are restored makes
/// the client drop its cached credentials, as the same DISCONNECT does in a running session.
#[tokio::test]
async fn not_authorized_during_resubscribe_invalidates_credentials() {
    let (port, peer) = spawn_peer_broker(move |broker| async move {
        broker.accept_connected().await.expect_subscribe_grant_qos0().await;

        let mut session = broker.accept_connected().await;
        let _subscribe = session.read.next().await;
        session.send_disconnect(DisconnectReasonCode::NotAuthorized).await;
        session.hold().await;
    })
    .await;

    let invalidations = Arc::new(AtomicUsize::new(0));
    let credentials = CountingInvalidate {
        invalidations: Arc::clone(&invalidations),
    };
    let policy = StandardReconnectPolicy::new(Duration::from_millis(20), Duration::from_secs(30));
    let (tx, _rx) = mpsc::channel(4);
    let started = TestClient::connected(local_options(port, "refused-restore"), credentials, policy, tx).await;
    started
        .client
        .subscribe([("a/b", QoS::AtMostOnce)])
        .await
        .expect("subscribe");
    wait_until(
        Duration::from_secs(5),
        || invalidations.load(Ordering::SeqCst) == 1,
        "credentials invalidated",
    )
    .await;

    started.shutdown().await;
    peer.abort();
}

/// While a full inbound sink holds the restore of subscriptions, the client keeps the
/// connection alive with PINGREQs and takes the PINGRESPs: nothing else is written then, and a
/// broker closes a client that stays silent for 1.5 times the Keep Alive ([MQTT-3.1.2-22]).
#[tokio::test]
async fn resubscribe_held_by_a_full_sink_keeps_pinging() {
    let (pings_tx, pings_rx) = tokio::sync::oneshot::channel();
    let (port, peer) = spawn_peer_broker(move |broker| async move {
        let mut session = broker.accept_connected().await;
        session.expect_subscribe_grant_qos0().await;
        session.expect_subscribe_grant_qos0().await;
        drop(session);

        let mut session = broker.accept_connected().await;
        let ids = session.expect_subscribe_ids(2).await;
        session.send_suback_qos0(ids[0], 1).await;
        for payload in ["r1", "r2"] {
            session.feed_publish("a/retained", payload, true);
        }
        session.write.flush().await.expect("flush");
        // The sink holds one message and the consumer is asleep: count the PINGREQs meanwhile.
        let mut pings = 0;
        let listen = tokio::time::sleep(Duration::from_millis(2500));
        tokio::pin!(listen);
        loop {
            tokio::select! {
                () = &mut listen => break,
                frame = session.read.next() => match frame {
                    Some(Ok((DecodedFrame::Control(Packet::PingRequest), _))) => {
                        pings += 1;
                        session.write.send(Packet::PingResponse).await.expect("PINGRESP");
                    }
                    Some(Ok(_)) => {}
                    _ => break,
                },
            }
        }
        let _ = pings_tx.send(pings);
        session.send_suback_qos0(ids[1], 1).await;
        session.hold().await;
    })
    .await;

    let options = local_options(port, "pinging-restore")
        .keep_alive(Duration::from_secs(1))
        .ack_timeout(Duration::from_secs(1));
    let (tx, mut rx) = mpsc::channel(1);
    let started = TestClient::connected(options, static_credentials(), quick_retry(), tx).await;
    for filter in ["a/#", "b/#"] {
        started
            .client
            .subscribe([(filter, QoS::AtMostOnce)])
            .await
            .expect("subscribe");
    }

    // Take nothing until the broker has counted; r1 fills the one-slot sink, r2 waits.
    let pings = timeout(Duration::from_secs(8), pings_rx)
        .await
        .expect("in time")
        .expect("peer alive");
    assert!(pings >= 1, "no PINGREQ while the sink held the restore");
    for expected in ["r1", "r2"] {
        let message = timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("in time")
            .expect("open");
        assert_eq!(&message.payload[..], expected.as_bytes());
    }

    started.shutdown().await;
    peer.abort();
}

/// A credentials provider that never answers fails the connect within `connect_timeout`
/// instead of keeping the driver connecting for good.
#[tokio::test]
async fn hanging_credentials_provider_is_bounded_by_the_connect_timeout() {
    struct Hangs;
    impl mqtt_glide::CredentialsProvider for Hangs {
        async fn credentials(&self) -> Result<(String, mqtt_glide::Password), AuthError> {
            std::future::pending().await
        }
    }

    let (port, peer) = spawn_peer_broker(|broker| async move {
        broker.accept_session().await.hold().await;
    })
    .await;
    let options = local_options(port, "hanging-credentials").connect_timeout(Duration::from_millis(300));
    let (tx, _rx) = mpsc::channel(4);
    let mut started = TestClient::spawn(options, Hangs, NoReconnect, tx);

    let disconnected = wait_for_phase(&mut started.phase, Duration::from_secs(3), |phase| {
        matches!(phase, SessionPhase::Disconnected(_))
    })
    .await;
    assert!(
        format!("{disconnected:?}").contains("credentials timed out"),
        "{disconnected:?}"
    );
    let _ = timeout(Duration::from_secs(5), started.run)
        .await
        .expect("driver stops");
    peer.abort();
}

/// `shutdown()` while subscriptions are being restored ends the session with a Normal
/// Disconnection: the session is up since the CONNACK, and a dropped connection would make the
/// broker publish the Will.
#[tokio::test]
async fn shutdown_during_resubscribe_sends_disconnect() {
    let (in_resubscribe_tx, in_resubscribe_rx) = tokio::sync::oneshot::channel();
    let (port, peer) = spawn_peer_broker(move |broker| async move {
        broker.accept_connected().await.expect_subscribe_grant_qos0().await;

        let mut session = broker.accept_connected().await;
        let _subscribe = session.read.next().await; // left unanswered
        let _ = in_resubscribe_tx.send(());
        timeout(
            Duration::from_secs(3),
            session.expect_disconnect(DisconnectReasonCode::NormalDisconnection),
        )
        .await
        .expect("DISCONNECT in time");
    })
    .await;

    let options = local_options(port, "will")
        .will("status/will", "died")
        .ack_timeout(Duration::from_secs(10));
    let (tx, _rx) = mpsc::channel(4);
    let started = TestClient::connected(options, static_credentials(), quick_retry(), tx).await;
    started
        .client
        .subscribe([("a/b", QoS::AtMostOnce)])
        .await
        .expect("subscribe");
    timeout(Duration::from_secs(5), in_resubscribe_rx)
        .await
        .expect("restore started")
        .expect("peer alive");

    started.shutdown().await;
    join_peer(peer).await;
}

/// A PUBACK that arrives right before the broker closes the connection still completes the
/// publish: the write task applies the acknowledgements already queued before it fails what
/// is left with `ConnectionLost`. Two workers, so the read task's end and the write task race.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn puback_right_before_close_completes_the_publish() {
    let (port, broker) = spawn_peer_broker(|broker| async move {
        loop {
            let mut session = broker.accept_session().await;
            tokio::spawn(async move {
                session.expect_connect().await;
                session.send_connack_success().await;
                while let Some(Ok((frame, _))) = session.read.next().await {
                    if let DecodedFrame::Publish(packet) = frame
                        && let Some(packet_id) = packet.meta.packet_id
                    {
                        session.send_puback(packet_id).await;
                        return; // the PUBACK is on the wire, then the close
                    }
                }
            });
        }
    })
    .await;

    let mut lost = 0;
    for _ in 0..40 {
        let (tx, _rx) = mpsc::channel(4);
        let started =
            TestClient::connected(local_options(port, "ack-close"), static_credentials(), NoReconnect, tx).await;
        match started.client.publish("t/ack", "x").qos(QoS::AtLeastOnce).await {
            Ok(_) => {}
            Err(error) if matches!(error.error, mqtt_glide::AppError::ConnectionLost) => lost += 1,
            Err(error) => panic!("unexpected {error:?}"),
        }
        let _ = timeout(Duration::from_secs(5), started.run).await;
    }
    assert_eq!(lost, 0, "publishes the broker acknowledged failed with ConnectionLost");
    broker.abort();
}

/// Dropping the driver future of `connect` ends the session with it: the read and write tasks
/// it spawned stop, the socket closes, a publish in flight resolves instead of hanging, and the
/// client no longer reports the session as up.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_the_driver_ends_the_session() {
    use tokio::io::AsyncReadExt;

    let (closed_tx, closed_rx) = tokio::sync::oneshot::channel();
    let (port, peer) = spawn_peer_broker(move |broker| async move {
        let session = broker.accept_connected().await;
        // Never acknowledges; reports when the client closes the socket.
        let mut raw = session.read.into_inner();
        let mut buf = vec![0u8; 4096];
        while let Ok(read) = raw.read(&mut buf).await {
            if read == 0 {
                break;
            }
        }
        let _ = closed_tx.send(());
        drop(session.write);
    })
    .await;

    let (tx, _rx) = mpsc::channel(4);
    let started = TestClient::connected(
        local_options(port, "dropped-driver"),
        static_credentials(),
        NoReconnect,
        tx,
    )
    .await;

    let publish = tokio::spawn({
        let client = started.client.clone();
        async move {
            client
                .publish("t/hang", "x")
                .qos(QoS::AtLeastOnce)
                .ack_timeout(None)
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    started.run.abort();
    let _ = started.run.await;

    timeout(Duration::from_secs(3), closed_rx)
        .await
        .expect("socket closed")
        .expect("peer alive");
    let in_flight = timeout(Duration::from_secs(3), publish)
        .await
        .expect("the publish in flight resolves")
        .expect("join");
    assert!(in_flight.is_err(), "{in_flight:?}");
    // A health check must not see the dropped session as up.
    assert!(!started.client.is_connected(), "still reported as connected");
    assert!(
        matches!(
            *started.phase.borrow(),
            SessionPhase::Disconnected(ConnectionOutcome::Shutdown(_))
        ),
        "{:?}",
        *started.phase.borrow()
    );
    peer.abort();
}

/// A `requester()` call given up before its SUBACK leaves no route behind: a later message on
/// that topic reaches the application instead of being swallowed as a response nobody waits for.
#[tokio::test]
async fn cancelled_requester_leaves_no_route_behind() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let (go_tx, go_rx) = tokio::sync::oneshot::channel::<()>();
    let broker = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept");
        let mut buf = vec![0u8; 4096];
        let _ = socket.read(&mut buf).await.expect("CONNECT");
        socket
            .write_all(&[0x20, 0x03, 0x00, 0x00, 0x00])
            .await
            .expect("CONNACK");
        // Never answers the SUBSCRIBE; on cue delivers a QoS 0 PUBLISH with Correlation Data.
        let _ = go_rx.await;
        let topic = b"svc/replies";
        let mut body = Vec::new();
        body.extend_from_slice(&u16::try_from(topic.len()).expect("short").to_be_bytes());
        body.extend_from_slice(topic);
        body.extend_from_slice(&[5, 0x09, 0, 2, b'c', b'1']);
        body.extend_from_slice(b"payload");
        let mut frame = vec![0x30, u8::try_from(body.len()).expect("short")];
        frame.extend_from_slice(&body);
        socket.write_all(&frame).await.expect("PUBLISH");
        std::future::pending::<()>().await;
    });

    let (tx, mut rx) = mpsc::channel::<InboundMessage>(4);
    let client = mqtt_glide::ClientBuilder::new(format!("mqtt://127.0.0.1:{port}"))
        .expect("url")
        .inbound_sink(tx)
        .connect()
        .await
        .expect("connect");
    let gave_up = timeout(Duration::from_millis(200), client.requester("svc/replies")).await;
    assert!(gave_up.is_err(), "the SUBACK never comes");

    let _ = go_tx.send(());
    let message = timeout(Duration::from_secs(3), rx.recv())
        .await
        .expect("delivered to the application")
        .expect("open");
    assert_eq!(&*message.topic, "svc/replies");
    client.shutdown().await.expect("shutdown");
    broker.abort();
}

/// `shutdown()` while the CONNACK is still due ends the attempt with a Normal Disconnection: the
/// broker may have accepted the CONNECT already, and a dropped connection would make it publish
/// the Will. DISCONNECT may precede the CONNACK ([MQTT-3.1.2-30]).
#[tokio::test]
async fn shutdown_before_the_connack_sends_disconnect() {
    let (connect_seen_tx, connect_seen_rx) = tokio::sync::oneshot::channel();
    let (port, peer) = spawn_peer_broker(move |broker| async move {
        let mut session = broker.accept_session().await;
        session.expect_connect().await;
        let _ = connect_seen_tx.send(());
        timeout(
            Duration::from_secs(3),
            session.expect_disconnect(DisconnectReasonCode::NormalDisconnection),
        )
        .await
        .expect("DISCONNECT in time");
    })
    .await;

    let options = local_options(port, "will-before-connack").will("status/will", "died");
    let (tx, _rx) = mpsc::channel(4);
    let started = TestClient::spawn(options, static_credentials(), NoReconnect, tx);
    timeout(Duration::from_secs(5), connect_seen_rx)
        .await
        .expect("CONNECT in time")
        .expect("peer alive");

    started.client.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), started.run)
        .await
        .expect("driver stops");
    join_peer(peer).await;
}

/// `shutdown()` in the moment the CONNACK arrives (here from the lifecycle hook, which runs
/// right after it) still ends the session with a Normal Disconnection instead of dropping it.
#[tokio::test]
async fn shutdown_racing_the_connack_sends_disconnect() {
    use futures_util::FutureExt;

    struct ShutdownOnConnected(Arc<std::sync::OnceLock<mqtt_glide::MqttClient>>);
    impl mqtt_glide::LifecycleHook for ShutdownOnConnected {
        fn on_lifecycle(&self, phase: &SessionPhase) {
            if matches!(phase, SessionPhase::Connected { .. })
                && let Some(client) = self.0.get()
            {
                let _ = client.shutdown().now_or_never();
            }
        }
    }

    let (port, peer) = spawn_peer_broker(move |broker| async move {
        let mut session = broker.accept_connected().await;
        timeout(
            Duration::from_secs(3),
            session.expect_disconnect(DisconnectReasonCode::NormalDisconnection),
        )
        .await
        .expect("DISCONNECT in time");
    })
    .await;

    let shared = Arc::new(std::sync::OnceLock::new());
    let options = local_options(port, "will-at-connack").will("status/will", "died");
    let (tx, _rx) = mpsc::channel::<InboundMessage>(4);
    let (client, connection, _phase) = connect(
        options,
        static_credentials(),
        NoReconnect,
        tx,
        ShutdownOnConnected(Arc::clone(&shared)),
    )
    .expect("wiring");
    let _ = shared.set(client);
    let outcome = timeout(Duration::from_secs(5), connection.run())
        .await
        .expect("driver stops");
    assert!(outcome.is_ok(), "a shutdown ends the driver cleanly: {outcome:?}");
    join_peer(peer).await;
}
