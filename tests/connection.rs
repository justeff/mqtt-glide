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
use std::num::NonZeroU16;

use futures_util::{SinkExt, StreamExt};
use mqtt_glide::Password;
use mqtt_glide::codec::{ConnectAck, DecodedFrame, Packet, PublishDelivery, encode_publish};
use mqtt_glide::{
    AuthError, BrokerUrl, ClientId, ConnectAckReason, ConnectOptions, ConnectionOutcome, DEFAULT_MAX_PACKET_SIZE,
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

    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid broker url");
    let options = ConnectOptions::new(url).client_id(ClientId::from_prefix("connect-down"));
    let (tx, _rx) = mpsc::channel::<InboundMessage>(4);
    let (_client, connection, mut phase) =
        connect(options, static_credentials(), NoReconnect, tx, NoopLifecycle).expect("connect wiring");

    let run = tokio::spawn(connection.run());
    let first = wait_for_phase(&mut phase, Duration::from_secs(5), |p| {
        matches!(p, SessionPhase::Disconnected(_) | SessionPhase::Connected { .. })
    })
    .await;

    match first {
        SessionPhase::Disconnected(ConnectionOutcome::Transport(_)) => {}
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
    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
    let options = ConnectOptions::new(url).client_id(ClientId::from_prefix("disc-reason"));
    let invalidations = Arc::new(AtomicUsize::new(0));
    let creds = CountingInvalidate {
        invalidations: Arc::clone(&invalidations),
    };
    let (tx, _rx) = mpsc::channel::<InboundMessage>(4);
    let (_client, connection, mut phase) = connect(options, creds, NoReconnect, tx, NoopLifecycle).expect("wiring");
    let run = tokio::spawn(connection.run());

    // Broker DISCONNECT right after CONNACK — watch may skip Connected.
    let disconnected = wait_for_phase(&mut phase, Duration::from_secs(5), |event| {
        matches!(event, SessionPhase::Disconnected(_))
    })
    .await;
    assert!(
        matches!(&disconnected, SessionPhase::Disconnected(outcome) if expected(outcome)),
        "{reason:?}: {disconnected:?}"
    );
    assert_eq!(invalidations.load(Ordering::SeqCst), expected_invalidations);

    let outcome = timeout(Duration::from_secs(5), run)
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
    let (tx, _rx) = mpsc::channel::<InboundMessage>(4);
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
    let queued = client.try_start_publish(Publish::new("invalidate/order", "x"));
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

    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
    let options = ConnectOptions::new(url).client_id(ClientId::from_prefix("refused-creds"));
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

    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
    let options = ConnectOptions::new(url)
        .client_id(ClientId::from_prefix("desync-clean"))
        .clean_start(true)
        .connect_timeout(Duration::from_secs(1));
    let policy = StandardReconnectPolicy::new(Duration::from_millis(50), Duration::from_secs(1));
    let (tx, _rx) = mpsc::channel::<InboundMessage>(4);
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

    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
    let options = ConnectOptions::new(url)
        .client_id(ClientId::try_new("restarted-service").expect("client id"))
        .clean_start(false)
        .session_expiry(Some(Duration::from_secs(60)))
        .connect_timeout(Duration::from_secs(1));
    let (tx, mut rx) = mpsc::channel::<InboundMessage>(4);
    let (client, connection, mut phase) =
        connect(options, static_credentials(), NoReconnect, tx, NoopLifecycle).expect("wiring");
    let run = tokio::spawn(connection.run());

    wait_for_phase(
        &mut phase,
        Duration::from_secs(5),
        |event| matches!(event, SessionPhase::Connected { session, .. } if session.session_present),
    )
    .await;
    let message = timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("queued message in time")
        .expect("sink open");
    assert_eq!(&*message.topic, "queued/while/offline");
    assert!(client.is_connected(), "resuming must not end the session");

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
    let (tx, _rx) = mpsc::channel::<InboundMessage>(4);
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

/// A QoS 1 publish the broker answers with a DISCONNECT each time (an ACL refusal) is given up
/// after three sends instead of being resent on every reconnect for ever, and the publishes
/// behind it go on. The broker acknowledges in order, so the oldest unacknowledged publish is
/// the one it stopped at.
#[tokio::test]
async fn publish_the_broker_disconnects_on_is_given_up() {
    use mqtt_glide::codec::{PublishAck, PublishAckReason};

    let connects = Arc::new(AtomicUsize::new(0));
    let refused_sends = Arc::new(AtomicUsize::new(0));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let broker = tokio::spawn({
        let connects = Arc::clone(&connects);
        let refused_sends = Arc::clone(&refused_sends);
        async move {
            loop {
                let (stream, _) = listener.accept().await.expect("accept");
                let index = connects.fetch_add(1, Ordering::SeqCst);
                let refused_sends = Arc::clone(&refused_sends);
                tokio::spawn(async move {
                    let (read, write) = common::framed_halves(stream);
                    let mut session = common::PeerSession { read, write };
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
                        let packet_id = packet.meta.packet_id.expect("QoS 1");
                        let ack = Packet::PublishAck(PublishAck {
                            packet_id,
                            reason_code: PublishAckReason::Success,
                            properties: Default::default(),
                            reason_string: None,
                        });
                        let _ = session.write.send(ack).await;
                    }
                });
            }
        }
    });

    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
    let options = ConnectOptions::new(url)
        .client_id(ClientId::try_new("refused-publish").expect("client id"))
        .clean_start(false)
        .session_expiry(Some(Duration::from_secs(3600)));
    let (tx, _rx) = mpsc::channel::<InboundMessage>(4);
    let (client, connection, mut phase) = connect(
        options,
        static_credentials(),
        StandardReconnectPolicy::new(Duration::from_millis(20), Duration::from_millis(20)),
        tx,
        NoopLifecycle,
    )
    .expect("wiring");
    let run = tokio::spawn(connection.run());
    wait_for_phase(&mut phase, Duration::from_secs(5), |phase| {
        matches!(phase, SessionPhase::Connected { .. })
    })
    .await;

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

    client.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), run).await;
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

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let broker = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let (read, write) = common::framed_halves(stream);
        let mut session = common::PeerSession { read, write };
        session.expect_connect().await;
        session.send_connack_success().await;
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
    });

    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
    let options = ConnectOptions::new(url)
        .client_id(ClientId::from_prefix("slow-out"))
        .keep_alive(Duration::from_secs(1))
        .tcp_send_buffer_size(std::num::NonZeroU32::new(16 * 1024).expect("nz"));
    let (tx, _rx) = mpsc::channel::<InboundMessage>(4);
    let (client, connection, mut phase) =
        connect(options, static_credentials(), NoReconnect, tx, NoopLifecycle).expect("wiring");
    let run = tokio::spawn(connection.run());
    wait_for_phase(&mut phase, Duration::from_secs(5), |phase| {
        matches!(phase, SessionPhase::Connected { .. })
    })
    .await;

    let publish = tokio::spawn({
        let client = client.clone();
        async move {
            client
                .publish("big/blob", bytes::Bytes::from(vec![0x55u8; PAYLOAD_LEN]))
                .await
        }
    });
    let lost = timeout(
        Duration::from_secs(4),
        phase.wait_for(|phase| matches!(phase, SessionPhase::Disconnected(_))),
    )
    .await;
    assert!(
        lost.is_err(),
        "connection closed while the publish was going out: {lost:?}"
    );
    timeout(Duration::from_secs(5), publish)
        .await
        .expect("in time")
        .expect("join")
        .expect("written");

    client.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), run).await;
    broker.abort();
}

/// An inbound PUBLISH larger than the link carries in a Keep Alive is no dead broker while its
/// bytes keep coming: the PINGRESP waits behind it, but every byte counts as traffic. Here
/// 2 MiB arrive at about 640 KiB/s, about three Keep Alive intervals.
#[tokio::test]
async fn slow_but_progressing_inbound_publish_keeps_the_connection() {
    use tokio::io::AsyncWriteExt;

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let broker = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let (read, write) = common::framed_halves(stream);
        let mut session = common::PeerSession { read, write };
        session.expect_connect().await;
        session.send_connack_success().await;
        let message = PublishMessage::new("big/in", bytes::Bytes::from(vec![7u8; 2 * 1024 * 1024]));
        let delivery = PublishDelivery {
            dup: false,
            retain: false,
            qos: QoS::AtMostOnce,
            packet_id: None,
        };
        let mut wire = bytes::BytesMut::new();
        encode_publish(&message, delivery, 0, &mut wire).expect("encode");
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
    });

    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
    let options = ConnectOptions::new(url)
        .client_id(ClientId::from_prefix("slow-in"))
        .keep_alive(Duration::from_secs(1))
        .max_packet_size(std::num::NonZeroU32::new(4 * 1024 * 1024).expect("nz"));
    let (tx, mut rx) = mpsc::channel::<InboundMessage>(4);
    let (client, connection, mut phase) =
        connect(options, static_credentials(), NoReconnect, tx, NoopLifecycle).expect("wiring");
    let run = tokio::spawn(connection.run());
    wait_for_phase(&mut phase, Duration::from_secs(5), |phase| {
        matches!(phase, SessionPhase::Connected { .. })
    })
    .await;

    let message = timeout(Duration::from_secs(8), rx.recv())
        .await
        .expect("delivered in time")
        .expect("connection kept");
    // Delivered whole is the proof the connection held: a closed one delivers nothing, and the
    // mock never answers PINGREQ, so `is_connected` afterwards would only measure the next tick.
    assert_eq!(message.payload.len(), 2 * 1024 * 1024);

    client.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), run).await;
    broker.abort();
}
