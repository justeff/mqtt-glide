use std::num::NonZeroU16;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::time::timeout;

use super::test_support::*;
use super::*;
use crate::Publish;
use crate::client::connect;
use crate::codec::{DecodedFrame, InboundMessage, Packet};
use crate::error::{AuthError, ProtocolError};
use crate::options::ConnectOptions;
use crate::util::BrokerUrl;

/// [MQTT-4.13.2-1] DISCONNECT reason codes ≥0x80 classify as session/network termination.
#[test]
fn map_broker_disconnect_classifies_error_reason_codes() {
    use crate::codec::DisconnectReasonCode;

    assert!(matches!(
        map_broker_disconnect_outcome(DisconnectReasonCode::NotAuthorized),
        ConnectionOutcome::Auth(AuthError::NotAuthorized)
    ));
    assert!(matches!(
        map_broker_disconnect_outcome(DisconnectReasonCode::UnspecifiedError),
        ConnectionOutcome::Transport(_)
    ));
    assert!(matches!(
        map_broker_disconnect_outcome(DisconnectReasonCode::ProtocolError),
        ConnectionOutcome::Protocol(_)
    ));
}

/// flume disconnects permanently when the last sender drops; anchor + Connection sender must survive read respawn.
#[tokio::test]
async fn handoff_channel_survives_read_sender_drop() {
    use crate::codec::{InboundMessage, PublishProperties, QoS};
    use bytes::Bytes;
    use bytestring::ByteString;

    let (tx, rx) = flume::bounded::<InboundMessage>(8);
    let anchor = tx.clone();
    let conn = tx;
    drop(conn.clone()); // simulate read-task sender drop
    assert!(!anchor.is_disconnected());
    assert!(!rx.is_disconnected());
    let msg = InboundMessage {
        topic: ByteString::from_static("t"),
        payload: Bytes::from_static(b"x"),
        properties: PublishProperties::default(),
        qos: QoS::AtMostOnce,
        retain: false,
        dup: false,
        subscription_ids: Vec::new(),
    };
    anchor.send_async(msg).await.expect("send after read drop");
    let got = rx.recv_async().await.expect("recv after read drop");
    assert_eq!(&*got.payload, b"x");
}

#[tokio::test]
async fn run_returns_observable_result_on_shutdown() {
    let (client, connection, mut phase) = test_connect().await;
    let run = tokio::spawn(connection.run());
    // Wait until the session is active so Shutdown reaches the write task (DISCONNECT path).
    await_connected(&mut phase).await;
    client.shutdown().await.expect("shutdown not backpressured");
    let result = run.await.expect("join run task");
    assert!(result.is_ok(), "shutdown of an active session: {result:?}");
}

/// Waits a minute before every reconnect, so a test can shut down during the backoff.
struct SlowReconnect;

impl ReconnectPolicy for SlowReconnect {
    fn delay_after(&self, _outcome: &ConnectionOutcome) -> Option<Duration> {
        Some(Duration::from_secs(60))
    }
}

/// Commands queued while the client waits to reconnect are answered when the driver stops,
/// not dropped: a publish with `ConnectionLost`, a subscribe with `ShuttingDown`. Dropping them
/// would read as `CommandChannelClosed`, a client that is gone.
#[tokio::test]
async fn commands_queued_in_the_backoff_fail_when_shutdown_stops_the_driver() {
    let refused = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = refused.local_addr().expect("addr").port();
    drop(refused);
    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
    let (tx, _rx) = flume::bounded::<InboundMessage>(4);
    let (client, connection, _phase) = connect(
        ConnectOptions::new(url),
        static_test_credentials(),
        SlowReconnect,
        tx,
        NoopLifecycle,
    )
    .expect("connect wiring");
    let run = tokio::spawn(connection.run());
    tokio::time::sleep(Duration::from_millis(100)).await;

    let publish = client
        .try_start_publish(Publish::new("t/queued", "x").qos(crate::QoS::AtLeastOnce))
        .expect("queued");
    let subscriber = client.clone();
    let subscribe = tokio::spawn(async move { subscriber.subscribe([("t/queued", crate::QoS::AtLeastOnce)]).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    client.shutdown().await.expect("shutdown");

    let result = timeout(Duration::from_secs(5), run)
        .await
        .expect("stops")
        .expect("join");
    assert!(result.is_ok(), "{result:?}");
    let publish = timeout(Duration::from_secs(1), publish.wait()).await.expect("answered");
    assert!(matches!(publish, Err(AppError::ConnectionLost)), "{publish:?}");
    let subscribe = timeout(Duration::from_secs(1), subscribe)
        .await
        .expect("answered")
        .expect("join");
    assert!(matches!(subscribe, Err(AppError::ShuttingDown)), "{subscribe:?}");
}

/// `shutdown()` ends `run()` with `Ok(())` in every phase, not only with a session up: before
/// the first dial, while the broker has not answered CONNECT, and while waiting to reconnect.
#[tokio::test]
async fn run_returns_ok_after_shutdown_in_every_phase() {
    async fn run_until_shutdown(port: u16, before_shutdown: impl std::future::Future<Output = ()>, early: bool) {
        let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
        let (tx, _rx) = flume::bounded::<InboundMessage>(4);
        let (client, connection, _phase) = connect(
            ConnectOptions::new(url),
            static_test_credentials(),
            SlowReconnect,
            tx,
            NoopLifecycle,
        )
        .expect("connect wiring");
        if early {
            client.shutdown().await.expect("shutdown");
        }
        let run = tokio::spawn(connection.run());
        before_shutdown.await;
        client.shutdown().await.expect("shutdown");
        let result = timeout(Duration::from_secs(5), run)
            .await
            .expect("driver stops")
            .expect("join");
        assert!(result.is_ok(), "{result:?}");
    }

    // Before the first dial.
    run_until_shutdown(1, async {}, true).await;

    // CONNECT sent, CONNACK never comes.
    let silent = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = silent.local_addr().expect("addr").port();
    let held = tokio::spawn(async move {
        let (stream, _) = silent.accept().await.expect("accept");
        tokio::time::sleep(Duration::from_secs(10)).await;
        drop(stream);
    });
    run_until_shutdown(port, tokio::time::sleep(Duration::from_millis(100)), false).await;
    held.abort();

    // Refused dial, then the one-minute backoff.
    let refused = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = refused.local_addr().expect("addr").port();
    drop(refused);
    run_until_shutdown(port, tokio::time::sleep(Duration::from_millis(100)), false).await;
}

/// Only a redirect that ends a long session starts a new streak: a redirecting CONNACK still
/// counts towards the cap, however long the session before it ran.
#[tokio::test]
async fn a_redirect_while_connecting_does_not_end_the_streak() {
    let url = BrokerUrl::parse("mqtt://127.0.0.1:1883").expect("valid url");
    let options = ConnectOptions::new(url).follow_server_redirects(true);
    let (tx, _rx) = flume::bounded::<InboundMessage>(4);
    let (_client, mut connection, _phase) =
        connect(options, static_test_credentials(), NoReconnect, tx, NoopLifecycle).expect("connect wiring");
    connection.redirects_in_row = redirect::MAX_REDIRECTS_IN_ROW;
    connection.session_up_since = Instant::now().checked_sub(redirect::STREAK_RESET_AFTER * 2);

    let refused = ConnectionOutcome::Protocol(ProtocolError::Violation("CONNACK rejected: UseAnotherServer".into()));
    let redirect = redirect::ServerRedirect {
        reference: "other.example".into(),
        permanent: false,
    };
    let disposition = connection
        .finish_session_or_redirect(refused, false, Some(redirect))
        .await;
    assert!(
        matches!(disposition, SessionDisposition::Stop(Err(_))),
        "the sixth redirect in a row is not followed"
    );
    assert!(connection.redirect_once.is_none());
}

#[tokio::test]
async fn read_task_exit_cancels_write_and_joins() {
    let (_client, connection, mut phase) = test_connect().await;
    let read_exit = connection.read_stub_exit_sender();
    let run = tokio::spawn(connection.run());
    // Wait until CONNACK session is active so the read task is running.
    await_connected(&mut phase).await;
    read_exit.send(()).await.expect("signal read exit");
    let result = run.await.expect("join run task");
    assert!(matches!(
        result,
        Err(ConnectionOutcome::Application(AppError::Internal(_)))
    ));
}

#[tokio::test]
async fn write_task_exit_cancels_read_and_joins() {
    let (_client, connection, mut phase) = test_connect().await;
    let write_exit = connection.write_stub_exit_sender();
    let run = tokio::spawn(connection.run());
    await_connected(&mut phase).await;
    write_exit.send(()).await.expect("signal write exit");
    let result = run.await.expect("join run task");
    assert!(matches!(
        result,
        Err(ConnectionOutcome::Application(AppError::Internal(_)))
    ));
}

#[test]
fn generation_binding_smoke() {
    let current = Generation(1);
    assert!(current.accepts(Generation(1)));
    assert!(!current.accepts(Generation(0)));
}

/// Read and Write are separate tasks; Write can progress on the control path while Read
/// blocks on a full handoff. Uses the same bounded flume sender injected into [`connect`]
/// (Connection's real handoff) and Connection's control channel — not a standalone demo
/// queue. While Read is parked on `send_async` to a full handoff, Write/control must still
/// accept work (`PingReq`).
#[tokio::test]
async fn write_can_progress_while_handoff_stalls() {
    use crate::codec::{InboundMessage, PublishProperties, QoS};
    use crate::connection::ControlMessage;
    use bytes::Bytes;
    use bytestring::ByteString;

    fn sample_publish(topic: &'static str, payload: &'static [u8]) -> InboundMessage {
        InboundMessage {
            topic: ByteString::from_static(topic),
            payload: Bytes::from_static(payload),
            properties: PublishProperties::default(),
            qos: QoS::AtMostOnce,
            retain: false,
            dup: false,
            subscription_ids: Vec::new(),
        }
    }

    // Connection-bound handoff: capacity 1 so one fill saturates it.
    let port = spawn_mock_broker().await;
    let (handoff_tx, _handoff_rx) = flume::bounded::<InboundMessage>(1);
    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
    let options = ConnectOptions::new(url).receive_maximum(NonZeroU16::new(8).expect("nz"));
    let (client, connection, _phase) = connect(
        options,
        static_test_credentials(),
        NoReconnect,
        handoff_tx.clone(),
        NoopLifecycle,
    )
    .expect("connect wiring");

    handoff_tx
        .send_async(sample_publish("fill", b"1"))
        .await
        .expect("fill Connection handoff to capacity");

    // Simulated Read stall: blocked push on the **same** injectable handoff.
    let stall_tx = handoff_tx.clone();
    let stall = tokio::spawn(async move { stall_tx.send_async(sample_publish("stall", b"2")).await });

    tokio::task::yield_now().await;
    assert!(
        !stall.is_finished(),
        "handoff push must still be waiting (simulates Read blocked on Connection handoff, §3.2)"
    );

    // Observable Write/control path on **this** Connection — must accept while handoff is full.
    let control_tx = connection.control_sender();
    control_tx
        .try_send(ControlMessage::PingReq)
        .expect("Connection control path must accept PingReq while its handoff is stalled (§3.2)");

    let run = tokio::spawn(connection.run());
    client.shutdown().await.expect("shutdown");
    let _ = run.await.expect("join run");

    assert!(
        !stall.is_finished(),
        "handoff stall must still be blocked (consumer never drained)"
    );
    stall.abort();
}

/// MQTT §3.2: keep-alive must send PINGREQ during a full-handoff stall; stall must not
/// cause keepalive-death or a spurious reconnect (self-induced Read stall ≠ PINGRESP timeout).
/// The mock broker drops a connection that is idle past the Keep Alive window, so surviving it
/// proves the idle client sends PINGREQ ([MQTT-3.1.2-20]).
#[tokio::test]
async fn keep_alive_survives_handoff_stall_without_spurious_reconnect() {
    use crate::codec::{InboundMessage, PublishProperties, QoS};
    use bytes::Bytes;
    use bytestring::ByteString;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let keep_alive = Duration::from_secs(1);
    // Broker closes if no PINGREQ within this idle window (stricter than Mosquitto for TDD).
    let idle = Duration::from_millis(1500);
    let ping_count = Arc::new(AtomicUsize::new(0));
    let port = spawn_mock_broker_keepalive_enforcer(idle, true, true, false, Arc::clone(&ping_count)).await;

    let (handoff_tx, handoff_rx) = flume::bounded::<InboundMessage>(1);
    // Pre-fill so the inbound stall PUBLISH parks Read on handoff push.
    handoff_tx
        .send_async(InboundMessage {
            topic: ByteString::from_static("fill"),
            payload: Bytes::from_static(b"1"),
            properties: PublishProperties::default(),
            qos: QoS::AtMostOnce,
            retain: false,
            dup: false,
            subscription_ids: Vec::new(),
        })
        .await
        .expect("pre-fill handoff");

    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
    let options = ConnectOptions::new(url)
        .keep_alive(keep_alive)
        .receive_maximum(NonZeroU16::new(8).expect("nz"));
    // NoReconnect: a keepalive-death must surface as Disconnected, not be masked by retry.
    let (client, connection, mut phase) = connect(
        options,
        static_test_credentials(),
        NoReconnect,
        handoff_tx,
        NoopLifecycle,
    )
    .expect("connect wiring");

    let run = tokio::spawn(connection.run());

    await_connected(&mut phase).await;
    let gen_before = match &*phase.borrow() {
        SessionPhase::Connected { generation, .. } => *generation,
        other => panic!("expected Connected, got {other:?}"),
    };

    // Allow the stall PUBLISH to park Read, then wait past one keep-alive interval.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let liveness_window = keep_alive + Duration::from_millis(500);
    match timeout(
        liveness_window,
        phase.wait_for(|p| match p {
            SessionPhase::Disconnected(_) => true,
            SessionPhase::Connected { generation, .. } => *generation != gen_before,
            SessionPhase::NeverConnected => false,
        }),
    )
    .await
    {
        Err(_) => {
            // No lifecycle transition during the window — good if PINGs were also observed.
        }
        Ok(Ok(guard)) => match &*guard {
            SessionPhase::Disconnected(outcome) => {
                panic!(
                    "handoff stall must not cause keepalive-death; got Disconnected({outcome:?}) — \
                     driver must schedule PINGREQ while Read is stalled (§3.2)"
                );
            }
            SessionPhase::Connected { generation, .. } => {
                panic!(
                    "handoff stall must not cause spurious reconnect (Connected gen={generation}, was {gen_before})"
                );
            }
            other => panic!("unexpected phase change under stall: {other:?}"),
        },
        Ok(Err(_)) => panic!("phase watch closed while connection should stay alive under stall"),
    }

    assert!(
        ping_count.load(Ordering::SeqCst) >= 1,
        "driver must auto-send PINGREQ during a full-handoff stall (got {} PINGREQs); \
         manual PingReq injection does not satisfy this invariant",
        ping_count.load(Ordering::SeqCst)
    );

    // Handoff still held the pre-fill (consumer never drained during the window).
    assert!(
        handoff_rx.is_full(),
        "handoff must remain full for the duration of the stall window"
    );

    client.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), run).await;
}

/// MQTT §3.1.2.10 SHOULD: no PINGRESP within ~keep_alive after PINGREQ → transport disconnect.
#[tokio::test]
async fn missing_pingresp_disconnects_after_keep_alive() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let keep_alive = Duration::from_secs(1);
    // Broker stays up (long idle) but never answers PINGREQ.
    let idle = Duration::from_secs(30);
    let ping_count = Arc::new(AtomicUsize::new(0));
    let port = spawn_mock_broker_keepalive_enforcer(idle, false, false, false, Arc::clone(&ping_count)).await;

    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
    let options = ConnectOptions::new(url)
        .keep_alive(keep_alive)
        .receive_maximum(NonZeroU16::new(8).expect("nz"));
    let (client, connection, mut phase) = connect(
        options,
        static_test_credentials(),
        NoReconnect,
        flume::bounded(4).0,
        NoopLifecycle,
    )
    .expect("connect wiring");

    let run = tokio::spawn(connection.run());
    await_connected(&mut phase).await;

    // First ping at ~KA; watchdog fires on the next tick (~2× KA from connect).
    let disconnected = timeout(
        keep_alive.mul_f32(3.5),
        phase.wait_for(|p| matches!(p, SessionPhase::Disconnected(_))),
    )
    .await
    .expect("PINGRESP watchdog must disconnect")
    .expect("phase watch");

    match &*disconnected {
        SessionPhase::Disconnected(ConnectionOutcome::Transport(TransportError::Connection(msg))) => {
            assert!(
                msg.contains("PINGRESP timed out"),
                "expected PINGRESP timeout message, got {msg}"
            );
        }
        other => panic!("expected Transport PINGRESP timeout, got {other:?}"),
    }
    assert!(
        ping_count.load(Ordering::SeqCst) >= 1,
        "client must have sent at least one PINGREQ before timing out"
    );

    let _ = client.shutdown().await;
    let _ = timeout(Duration::from_secs(5), run).await;
}

/// Inbound PUBLISH without PINGRESP must clear the keepalive watchdog.
#[tokio::test]
async fn inbound_publish_clears_keepalive_watchdog_without_pingresp() {
    use crate::codec::InboundMessage;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let keep_alive = Duration::from_secs(1);
    let idle = Duration::from_secs(30);
    let ping_count = Arc::new(AtomicUsize::new(0));
    let port = spawn_mock_broker_keepalive_enforcer(idle, false, false, true, Arc::clone(&ping_count)).await;

    let (handoff_tx, handoff_rx) = flume::bounded::<InboundMessage>(8);
    let drain = tokio::spawn(async move { while handoff_rx.recv_async().await.is_ok() {} });

    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
    let options = ConnectOptions::new(url)
        .keep_alive(keep_alive)
        .receive_maximum(NonZeroU16::new(8).expect("nz"));
    let (client, connection, mut phase) = connect(
        options,
        static_test_credentials(),
        NoReconnect,
        handoff_tx,
        NoopLifecycle,
    )
    .expect("connect wiring");

    let run = tokio::spawn(connection.run());
    await_connected(&mut phase).await;
    let gen_before = match &*phase.borrow() {
        SessionPhase::Connected { generation, .. } => *generation,
        other => panic!("expected Connected, got {other:?}"),
    };

    // Past first PINGREQ + watchdog tick (~2× KA). Must stay up thanks to PUBLISH liveness.
    match timeout(
        keep_alive.mul_f32(3.5),
        phase.wait_for(|p| match p {
            SessionPhase::Disconnected(_) => true,
            SessionPhase::Connected { generation, .. } => *generation != gen_before,
            SessionPhase::NeverConnected => false,
        }),
    )
    .await
    {
        Err(_) => {}
        Ok(Ok(guard)) => panic!("expected session to stay Connected; got {:?}", *guard),
        Ok(Err(_)) => panic!("phase watch closed while connection should stay alive"),
    }

    assert!(
        ping_count.load(Ordering::SeqCst) >= 1,
        "client must have sent PINGREQ (got {})",
        ping_count.load(Ordering::SeqCst)
    );
    assert!(
        matches!(
            &*phase.borrow(),
            SessionPhase::Connected { generation, .. } if *generation == gen_before
        ),
        "must still be Connected gen={gen_before}"
    );

    client.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), run).await;
    drain.abort();
}

/// [MQTT-3.1.2-21] / [MQTT-3.2.2-21] Server Keep Alive from CONNACK drives PINGREQ cadence;
/// an otherwise idle client sends PINGREQ within the Keep Alive interval ([MQTT-3.1.2-20]).
#[tokio::test]
async fn server_keep_alive_from_connack_drives_pingreq_interval() {
    use crate::codec::ConnectAck;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // Client keep_alive is 60s — without Server KA override, no PINGREQ within a few seconds.
    let client_keep_alive = Duration::from_secs(60);
    let idle = Duration::from_millis(1500);
    let ping_count = Arc::new(AtomicUsize::new(0));
    let pings = Arc::clone(&ping_count);

    let port = spawn_broker_once(move |mut framed_read, mut framed_write| async move {
        let ack = ConnectAck {
            server_keep_alive: Some(1),
            ..ConnectAck::default()
        };
        if !accept_connect_send_connack_with(&mut framed_read, &mut framed_write, ack).await {
            return;
        }
        loop {
            match timeout(idle, framed_read.next()).await {
                Ok(Some(Ok((DecodedFrame::Control(Packet::PingRequest), _)))) => {
                    pings.fetch_add(1, Ordering::SeqCst);
                    if framed_write.send(Packet::PingResponse).await.is_err() {
                        return;
                    }
                }
                Ok(Some(Ok(_))) => {}
                Ok(Some(Err(_))) | Ok(None) | Err(_) => return,
            }
        }
    })
    .await;

    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
    let options = ConnectOptions::new(url)
        .keep_alive(client_keep_alive)
        .receive_maximum(NonZeroU16::new(8).expect("nz"));
    let (client, connection, mut phase) = connect(
        options,
        static_test_credentials(),
        NoReconnect,
        flume::bounded(4).0,
        NoopLifecycle,
    )
    .expect("connect wiring");

    let run = tokio::spawn(connection.run());
    await_connected(&mut phase).await;
    let gen_before = match &*phase.borrow() {
        SessionPhase::Connected { generation, .. } => *generation,
        other => panic!("expected Connected, got {other:?}"),
    };

    // Server KA=1s → expect ≥1 PINGREQ within ~2–3s; connection must stay up.
    match timeout(
        Duration::from_secs(3),
        phase.wait_for(|p| match p {
            SessionPhase::Disconnected(_) => true,
            SessionPhase::Connected { generation, .. } => *generation != gen_before,
            SessionPhase::NeverConnected => false,
        }),
    )
    .await
    {
        Err(_) => {}
        Ok(Ok(guard)) => panic!("expected session to stay Connected; got {:?}", *guard),
        Ok(Err(_)) => panic!("phase watch closed while connection should stay alive"),
    }

    assert!(
        ping_count.load(Ordering::SeqCst) >= 1,
        "Server Keep Alive=1s must drive PINGREQ despite client keep_alive=60s (got {})",
        ping_count.load(Ordering::SeqCst)
    );
    assert!(
        matches!(
            &*phase.borrow(),
            SessionPhase::Connected { generation, .. } if *generation == gen_before
        ),
        "must still be Connected gen={gen_before}"
    );

    client.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), run).await;
}

/// An inbound Topic Alias when we advertised a Topic Alias Maximum of 0 (§3.3.2.3.4) ends the
/// session with a Protocol Error locally, and the broker is told with DISCONNECT 0x94.
#[tokio::test]
async fn inbound_topic_alias_on_read_path_disconnects() {
    let (port, wire_reason) = spawn_mock_broker_with_inbound_topic_alias().await;
    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
    let options = ConnectOptions::new(url).receive_maximum(NonZeroU16::new(8).expect("nz"));
    let (tx, _rx) = flume::bounded::<InboundMessage>(4);
    let (_client, connection, mut phase) =
        connect(options, static_test_credentials(), NoReconnect, tx, NoopLifecycle).expect("connect wiring");

    let run = tokio::spawn(connection.run());

    // Session can disconnect immediately after CONNACK+bad PUBLISH; watch may skip Connected.
    let disconnected = await_disconnected(&mut phase).await;
    match disconnected {
        SessionPhase::Disconnected(ConnectionOutcome::Protocol(ProtocolError::TopicAliasNotSupported)) => {}
        other => panic!("expected Protocol(TopicAliasNotSupported), got {other:?}"),
    }

    let result = run.await.expect("join run task");
    assert!(matches!(
        result,
        Err(ConnectionOutcome::Protocol(ProtocolError::TopicAliasNotSupported))
    ));
    let reason = timeout(Duration::from_secs(5), wire_reason.recv_async())
        .await
        .expect("broker result timeout")
        .expect("broker result");
    assert_eq!(reason, Some(crate::codec::DisconnectReasonCode::TopicAliasInvalid));
}

// --- inbound enqueue-before-ack / flow-control contracts ---

/// Inbound QoS 1 must not PUBACK before flume handoff accepts the message. The PUBACK carries
/// the Packet Identifier of the incoming PUBLISH ([MQTT-2.2.1-5], [MQTT-4.3.2-4]); a QoS 1
/// PUBLISH is answered with a PUBACK ([MQTT-3.3.4-1]).
#[tokio::test]
async fn inbound_qos1_does_not_puback_before_handoff_accept() {
    use std::sync::Arc;
    use tokio::time::timeout;

    let puback_ids = Arc::new(tokio::sync::Mutex::new(Vec::<NonZeroU16>::new()));
    let pubacks = Arc::clone(&puback_ids);
    let port = spawn_broker_once(move |mut framed_read, mut framed_write| async move {
        if !accept_connect_send_connack(&mut framed_read, &mut framed_write).await {
            return;
        }
        send_inbound_qos1_publish(
            &mut framed_write,
            "in/handoff",
            b"blocked",
            NonZeroU16::new(7).expect("nz"),
            crate::codec::PublishProperties::default(),
        )
        .await;
        loop {
            match framed_read.next().await {
                Some(Ok((DecodedFrame::Control(Packet::PublishAck(ack)), _))) => {
                    pubacks.lock().await.push(ack.packet_id);
                }
                Some(Ok(_)) => {}
                Some(Err(_)) | None => break,
            }
        }
    })
    .await;

    // Capacity 0: rendezvous — no message is accepted until a receiver appears.
    let (handoff_tx, handoff_rx) = flume::bounded::<InboundMessage>(0);
    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
    let options = ConnectOptions::new(url).receive_maximum(NonZeroU16::new(2).expect("nz"));
    let (client, connection, mut phase) = connect(
        options,
        static_test_credentials(),
        NoReconnect,
        handoff_tx,
        NoopLifecycle,
    )
    .expect("connect");

    let run = tokio::spawn(connection.run());
    await_connected(&mut phase).await;

    // Accepted/unacked invariant before handoff accept: nothing in handoff, no PUBACK on wire.
    // Do not call try_recv here — with capacity 0 that would complete the rendezvous.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        handoff_rx.is_empty(),
        "handoff must not show an accepted message while send_async is still blocked (capacity 0)"
    );
    assert!(
        puback_ids.lock().await.is_empty(),
        "must not PUBACK before flume accept (regression against auto-ack-on-read); unacked until accept"
    );

    // After the consumer accepts, PUBACK must carry the inbound packet id (enqueue-before-ack).
    let received = timeout(Duration::from_secs(2), handoff_rx.recv_async())
        .await
        .expect("handoff receive timed out")
        .expect("handoff closed");
    assert_eq!(&*received.payload, b"blocked");

    let acked_id = timeout(Duration::from_secs(2), async {
        loop {
            if let Some(id) = puback_ids.lock().await.first().copied() {
                break id;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("PUBACK must be sent only after successful handoff accept");

    assert_eq!(
        acked_id,
        NonZeroU16::new(7).expect("nz"),
        "PUBACK packet id must match the accepted inbound QoS1 publish"
    );
    assert_eq!(
        puback_ids.lock().await.len(),
        1,
        "exactly one PUBACK after handoff accept"
    );

    client.shutdown().await.expect("shutdown");
    let _ = run.await;
}

/// [MQTT-4.3.2-5] After PUBACK, the same Packet Identifier may identify a new Application Message.
/// Each PUBACK repeats the Packet Identifier of its PUBLISH ([MQTT-2.2.1-5], [MQTT-4.3.2-4]).
#[tokio::test]
async fn inbound_qos1_same_packet_id_after_puback_is_new_message() {
    use crate::codec::InboundMessage;
    use std::sync::Arc;
    use tokio::time::timeout;

    let puback_ids = Arc::new(tokio::sync::Mutex::new(Vec::<NonZeroU16>::new()));
    let pubacks = Arc::clone(&puback_ids);
    let (second_tx, mut second_rx) = tokio::sync::mpsc::channel::<()>(1);
    let port = spawn_broker_once(move |mut framed_read, mut framed_write| async move {
        if !accept_connect_send_connack(&mut framed_read, &mut framed_write).await {
            return;
        }
        send_inbound_qos1_publish(
            &mut framed_write,
            "in/reuse",
            b"first",
            NonZeroU16::new(7).expect("nz"),
            crate::codec::PublishProperties::default(),
        )
        .await;
        // Wait for first PUBACK before reusing packet id 7.
        loop {
            match framed_read.next().await {
                Some(Ok((DecodedFrame::Control(Packet::PublishAck(ack)), _))) => {
                    pubacks.lock().await.push(ack.packet_id);
                    if ack.packet_id == NonZeroU16::new(7).expect("nz") {
                        break;
                    }
                }
                Some(Ok(_)) => {}
                Some(Err(_)) | None => return,
            }
        }
        send_inbound_qos1_publish(
            &mut framed_write,
            "in/reuse",
            b"second",
            NonZeroU16::new(7).expect("nz"),
            crate::codec::PublishProperties::default(),
        )
        .await;
        let _ = second_tx.send(()).await;
        loop {
            match framed_read.next().await {
                Some(Ok((DecodedFrame::Control(Packet::PublishAck(ack)), _))) => {
                    pubacks.lock().await.push(ack.packet_id);
                }
                Some(Ok(_)) => {}
                Some(Err(_)) | None => break,
            }
        }
    })
    .await;

    let (handoff_tx, handoff_rx) = flume::bounded::<InboundMessage>(4);
    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
    let options = ConnectOptions::new(url).receive_maximum(NonZeroU16::new(2).expect("nz"));
    let (client, connection, mut phase) = connect(
        options,
        static_test_credentials(),
        NoReconnect,
        handoff_tx,
        NoopLifecycle,
    )
    .expect("connect");

    let run = tokio::spawn(connection.run());
    await_connected(&mut phase).await;

    let first = timeout(Duration::from_secs(2), handoff_rx.recv_async())
        .await
        .expect("first handoff timed out")
        .expect("handoff closed");
    assert_eq!(&*first.payload, b"first");

    timeout(Duration::from_secs(2), async {
        loop {
            if !puback_ids.lock().await.is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first PUBACK id=7");

    second_rx.recv().await.expect("second publish sent");

    let second = timeout(Duration::from_secs(2), handoff_rx.recv_async())
        .await
        .expect("second handoff timed out")
        .expect("handoff closed");
    assert_eq!(&*second.payload, b"second");

    timeout(Duration::from_secs(2), async {
        loop {
            if puback_ids.lock().await.len() >= 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("second PUBACK id=7");

    let ids = puback_ids.lock().await.clone();
    assert_eq!(ids.len(), 2, "exactly two PUBACKs for two Application Messages");
    assert_eq!(ids[0], NonZeroU16::new(7).expect("nz"));
    assert_eq!(ids[1], NonZeroU16::new(7).expect("nz"));

    client.shutdown().await.expect("shutdown");
    let _ = run.await;
}

/// Full handoff: no further inbound PUBACKs until push succeeds; inflight ≤ client Receive Maximum.
#[tokio::test]
async fn full_handoff_defers_pubacks_within_client_receive_maximum() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::mpsc as tokio_mpsc;
    use tokio::time::timeout;

    let puback_ids = Arc::new(tokio::sync::Mutex::new(Vec::<NonZeroU16>::new()));
    let accepted = Arc::new(AtomicUsize::new(0));
    let (ready_tx, mut ready_rx) = tokio_mpsc::channel::<()>(1);
    let pubacks = Arc::clone(&puback_ids);
    let port = spawn_broker_once(move |mut framed_read, mut framed_write| async move {
        if !accept_connect_send_connack(&mut framed_read, &mut framed_write).await {
            return;
        }
        let _ = ready_tx.send(()).await;
        for id in 1u16..=3 {
            send_inbound_qos1_publish(
                &mut framed_write,
                "in/rm",
                b"x",
                NonZeroU16::new(id).expect("nz"),
                crate::codec::PublishProperties::default(),
            )
            .await;
        }
        loop {
            match framed_read.next().await {
                Some(Ok((DecodedFrame::Control(Packet::PublishAck(ack)), _))) => {
                    pubacks.lock().await.push(ack.packet_id);
                }
                Some(Ok(_)) => {}
                Some(Err(_)) | None => break,
            }
        }
    })
    .await;

    // Handoff capacity 1 + blocking consumer: after one accept, further pushes stall.
    let (handoff_tx, handoff_rx) = flume::bounded::<InboundMessage>(1);
    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
    let options = ConnectOptions::new(url).receive_maximum(NonZeroU16::new(2).expect("nz"));
    let (client, connection, mut phase) = connect(
        options,
        static_test_credentials(),
        NoReconnect,
        handoff_tx,
        NoopLifecycle,
    )
    .expect("connect");

    let run = tokio::spawn(connection.run());
    await_connected(&mut phase).await;
    ready_rx.recv().await.expect("broker ready");

    // Wait until the first message is accepted into the handoff (capacity 1 fills).
    timeout(Duration::from_secs(2), async {
        loop {
            if !handoff_rx.is_empty() {
                accepted.store(1, Ordering::SeqCst);
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first inbound publish must be accepted into the handoff");

    tokio::time::sleep(Duration::from_millis(200)).await;
    let acked_while_stalled = puback_ids.lock().await.len();
    let accepted_count = accepted.load(Ordering::SeqCst);
    assert_eq!(accepted_count, 1, "exactly one message accepted while consumer blocked");
    assert!(
        acked_while_stalled <= accepted_count,
        "PUBACKs ({acked_while_stalled}) must not exceed accepted handoff messages ({accepted_count}) — enqueue-before-ack"
    );
    assert!(
        acked_while_stalled <= 2,
        "inbound unacked/inflight must respect client_receive_maximum=2; saw {acked_while_stalled} PUBACKs"
    );
    // While stalled on the second push, at most the one accepted message may be acked.
    assert!(
        acked_while_stalled <= 1,
        "while handoff is full/stalled, PUBACKs must not advance beyond accepted messages; saw {acked_while_stalled}"
    );

    // Drain one → another accept can complete → PUBACK count may advance by one, still ≤ RM.
    let _ = handoff_rx.recv_async().await;
    accepted.fetch_add(1, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let after_drain = puback_ids.lock().await.len();
    assert!(
        after_drain > acked_while_stalled,
        "draining handoff must allow further PUBACKs (enqueue-before-ack); before={acked_while_stalled} after={after_drain}"
    );
    assert!(
        after_drain <= 2,
        "still capped by client_receive_maximum; saw {after_drain}"
    );
    assert!(
        after_drain <= accepted.load(Ordering::SeqCst),
        "PUBACKs must remain ≤ accepted messages after drain"
    );

    client.shutdown().await.expect("shutdown");
    let _ = run.await;
}

/// [MQTT-4.6.0-2] inbound PUBACKs must follow PUBLISH receipt order.
#[tokio::test]
async fn inbound_pubacks_sent_in_publish_receipt_order() {
    use std::sync::Arc;
    use tokio::time::timeout;

    let puback_order = Arc::new(tokio::sync::Mutex::new(Vec::<u16>::new()));
    let order = Arc::clone(&puback_order);
    let port = spawn_broker_once(move |mut framed_read, mut framed_write| async move {
        if !accept_connect_send_connack(&mut framed_read, &mut framed_write).await {
            return;
        }
        for id in [5u16, 9] {
            send_inbound_qos1_publish(
                &mut framed_write,
                "in/order",
                b"x",
                NonZeroU16::new(id).expect("nz"),
                crate::codec::PublishProperties::default(),
            )
            .await;
        }
        loop {
            match framed_read.next().await {
                Some(Ok((DecodedFrame::Control(Packet::PublishAck(ack)), _))) => {
                    order.lock().await.push(ack.packet_id.get());
                }
                Some(Ok(_)) => {}
                Some(Err(_)) | None => break,
            }
        }
    })
    .await;

    let (handoff_tx, handoff_rx) = flume::bounded::<InboundMessage>(4);
    let drain = tokio::spawn(async move { while handoff_rx.recv_async().await.is_ok() {} });
    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
    let options = ConnectOptions::new(url).receive_maximum(NonZeroU16::new(4).expect("nz"));
    let (client, connection, mut phase) = connect(
        options,
        static_test_credentials(),
        NoReconnect,
        handoff_tx,
        NoopLifecycle,
    )
    .expect("connect");

    let run = tokio::spawn(connection.run());
    await_connected(&mut phase).await;

    let observed = timeout(Duration::from_secs(2), async {
        loop {
            let guard = puback_order.lock().await;
            if guard.len() >= 2 {
                return guard.clone();
            }
            drop(guard);
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("two inbound PUBACKs must be observed");

    assert_eq!(
        observed,
        vec![5, 9],
        "PUBACK packet identifiers must follow inbound QoS1 PUBLISH receipt order"
    );

    client.shutdown().await.expect("shutdown");
    let _ = run.await;
    drain.abort();
}

// --- (critical-review hardening) ---

/// app commands must keep a live `command_tx` across the reconnect/backoff gap.
#[tokio::test]
async fn command_sender_remains_usable_during_reconnect_backoff() {
    let port = spawn_mock_broker_close_after_connack().await;
    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
    let options = ConnectOptions::new(url).receive_maximum(NonZeroU16::new(8).expect("nz"));
    let (tx, _rx) = flume::bounded::<InboundMessage>(4);
    let (client, connection, mut phase) = connect(
        options,
        static_test_credentials(),
        DelayedReconnect {
            delay: Duration::from_secs(5),
        },
        tx,
        NoopLifecycle,
    )
    .expect("connect wiring");

    let run = tokio::spawn(connection.run());

    // Close-after-CONNACK is fast: watch may only observe Disconnected (Connected overwritten).
    let _ = await_disconnected(&mut phase).await;

    // During Backoff the shared sender must still accept work (not CommandChannelClosed).
    let send_result = client.try_send_noop();
    assert!(
        !matches!(send_result, Err(AppError::CommandChannelClosed)),
        "command_tx must stay usable during reconnect backoff; got {send_result:?}"
    );
    assert!(
        send_result.is_ok(),
        "expected Ok (or Backpressure), not a dead channel; got {send_result:?}"
    );

    client.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), run).await;
}

/// Commands queued between sessions survive the per-session channel refresh: the command
/// channel lives as long as the client.
#[test]
fn replenish_session_channels_keeps_the_command_channel() {
    let (mut channels, shared) = test_channels();
    shared
        .load()
        .try_send(ClientCommand::Noop)
        .expect("queued while disconnected");

    channels.replenish_session_channels(COMMAND_CHANNEL_CAPACITY, CONTROL_CHANNEL_CAPACITY);
    channels.replenish_session_channels(COMMAND_CHANNEL_CAPACITY, CONTROL_CHANNEL_CAPACITY);

    let receiver = channels.command_rx.as_mut().expect("receiver kept");
    assert!(matches!(receiver.try_recv(), Ok(ClientCommand::Noop)));
}

/// A receiver lost with a panicked write task is replaced, and the shared sender swapped so
/// [`crate::MqttClient`] sends into the new channel.
#[test]
fn replenish_session_channels_replaces_a_lost_command_receiver() {
    let (mut channels, shared) = test_channels();
    let old_receiver = channels.command_rx.take().expect("receiver");
    drop(old_receiver);

    channels.replenish_session_channels(COMMAND_CHANNEL_CAPACITY, CONTROL_CHANNEL_CAPACITY);

    shared.load().try_send(ClientCommand::Noop).expect("post-swap send");
    let receiver = channels.command_rx.as_mut().expect("new receiver");
    assert!(matches!(receiver.try_recv(), Ok(ClientCommand::Noop)));
}

fn test_channels() -> (
    ConnectionChannels<flume::Sender<InboundMessage>>,
    Arc<ArcSwap<crate::connection::CommandSender>>,
) {
    let (control_tx, control_rx) = mpsc::channel(CONTROL_CHANNEL_CAPACITY);
    let (command_tx, command_rx) = mpsc::channel(COMMAND_CHANNEL_CAPACITY);
    let shared = Arc::new(ArcSwap::from_pointee(command_tx));
    let (handoff, _handoff_rx) = flume::bounded::<InboundMessage>(4);
    let (read_stub_exit_tx, read_stub_exit_rx) = mpsc::channel(1);
    let (write_stub_exit_tx, write_stub_exit_rx) = mpsc::channel(1);
    let channels = ConnectionChannels {
        control_tx,
        control_rx,
        command_tx: Arc::clone(&shared),
        command_rx: Some(command_rx),
        handoff: handoff.clone(),
        handoff_anchor: handoff,
        phase_tx: watch::channel(SessionPhase::initial()).0,
        lifecycle: Arc::new(NoopLifecycle),
        connected: Arc::default(),
        response_router: Arc::default(),
        stats: Default::default(),
        refused_subscriptions: watch::channel(Vec::new()).0,
        read_stub_exit_tx,
        read_stub_exit_rx,
        write_stub_exit_tx,
        write_stub_exit_rx,
    };
    (channels, shared)
}

/// cancel during auto-resubscribe wait must abort promptly.
#[tokio::test]
async fn resubscribe_aborts_on_cancel() {
    use crate::client::Subscription;
    use crate::codec::{QoS, SubscribeAckReason};
    use std::sync::atomic::{AtomicBool, Ordering};

    let holding_suback = Arc::new(AtomicBool::new(false));
    let holding_flag = Arc::clone(&holding_suback);
    let sessions = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let port = spawn_broker_loop({
        let sessions = Arc::clone(&sessions);
        let holding_flag = Arc::clone(&holding_flag);
        move |mut framed_read, mut framed_write| {
            let n = sessions.fetch_add(1, Ordering::SeqCst) + 1;
            let holding_flag = Arc::clone(&holding_flag);
            async move {
                if !accept_connect_send_connack(&mut framed_read, &mut framed_write).await {
                    return;
                }
                loop {
                    match framed_read.next().await {
                        Some(Ok((DecodedFrame::Control(Packet::Subscribe(sub)), _))) => {
                            if n == 1 {
                                let _ = framed_write
                                    .send(Packet::SubscribeAck(crate::codec::SubscribeAck {
                                        packet_id: sub.packet_id,
                                        properties: Vec::new(),
                                        reason_string: None,
                                        status: vec![SubscribeAckReason::GrantedQos1],
                                    }))
                                    .await;
                                return; // drop → reconnect
                            }
                            holding_flag.store(true, Ordering::SeqCst);
                            // Never SUBACK — resubscribe must be aborted by cancel.
                            loop {
                                tokio::time::sleep(Duration::from_secs(60)).await;
                            }
                        }
                        Some(Ok(_)) => {}
                        Some(Err(_)) | None => return,
                    }
                }
            }
        }
    })
    .await;

    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
    let options = ConnectOptions::new(url)
        .receive_maximum(NonZeroU16::new(8).expect("nz"))
        .ack_timeout(Duration::from_secs(30));
    let (tx, _rx) = flume::bounded::<InboundMessage>(4);
    let (client, connection, mut phase) = connect(
        options,
        static_test_credentials(),
        DelayedReconnect { delay: Duration::ZERO },
        tx,
        NoopLifecycle,
    )
    .expect("connect");

    let run = tokio::spawn(connection.run());
    await_connected(&mut phase).await;

    let reasons = client
        .subscribe(vec![Subscription::new("resub/cancel", QoS::AtLeastOnce)])
        .await
        .expect("subscribe");
    assert!(matches!(
        reasons.as_slice(),
        [SubscribeAckReason::GrantedQos0 | SubscribeAckReason::GrantedQos1]
    ));

    let _ = await_disconnected(&mut phase).await;

    timeout(Duration::from_secs(5), async {
        while !holding_suback.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("broker must observe resubscribe SUBSCRIBE");

    client.shutdown().await.expect("shutdown");
    let finished = timeout(Duration::from_secs(1), run).await;
    assert!(
        finished.is_ok(),
        "resubscribe_pending must respect cancel — run must finish well before SubAck timeout"
    );
}

/// PUBLISH arriving during resubscribe wait must reach the handoff (not discarded).
#[tokio::test]
async fn resubscribe_does_not_silently_drop_inbound_publish() {
    use crate::client::Subscription;
    use crate::codec::{QoS, SubscribeAckReason};
    use std::sync::atomic::{AtomicU32, Ordering};

    let sessions = Arc::new(AtomicU32::new(0));
    let port = spawn_broker_loop({
        let sessions = Arc::clone(&sessions);
        move |mut framed_read, mut framed_write| {
            let n = sessions.fetch_add(1, Ordering::SeqCst) + 1;
            async move {
                if !accept_connect_send_connack(&mut framed_read, &mut framed_write).await {
                    return;
                }
                loop {
                    match framed_read.next().await {
                        Some(Ok((DecodedFrame::Control(Packet::Subscribe(sub)), _))) => {
                            if n == 1 {
                                let _ = framed_write
                                    .send(Packet::SubscribeAck(crate::codec::SubscribeAck {
                                        packet_id: sub.packet_id,
                                        properties: Vec::new(),
                                        reason_string: None,
                                        status: vec![SubscribeAckReason::GrantedQos1],
                                    }))
                                    .await;
                                return;
                            }
                            // PUBLISH first, then matching SUBACK — today PUBLISH is dropped.
                            send_inbound_qos1_publish(
                                &mut framed_write,
                                "resub/during",
                                b"keep-me",
                                NonZeroU16::new(9).expect("nz"),
                                crate::codec::PublishProperties::default(),
                            )
                            .await;
                            let _ = framed_write
                                .send(Packet::SubscribeAck(crate::codec::SubscribeAck {
                                    packet_id: sub.packet_id,
                                    properties: Vec::new(),
                                    reason_string: None,
                                    status: vec![SubscribeAckReason::GrantedQos1],
                                }))
                                .await;
                        }
                        Some(Ok(_)) => {}
                        Some(Err(_)) | None => return,
                    }
                }
            }
        }
    })
    .await;

    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
    let options = ConnectOptions::new(url).receive_maximum(NonZeroU16::new(8).expect("nz"));
    let (tx, rx) = flume::bounded::<InboundMessage>(8);
    let (client, connection, mut phase) = connect(
        options,
        static_test_credentials(),
        DelayedReconnect { delay: Duration::ZERO },
        tx,
        NoopLifecycle,
    )
    .expect("connect");

    let run = tokio::spawn(connection.run());
    await_connected(&mut phase).await;
    client
        .subscribe(vec![Subscription::new("resub/during", QoS::AtLeastOnce)])
        .await
        .expect("subscribe");
    let _ = await_disconnected(&mut phase).await;

    // After reconnect + resubscribe, the mid-wait PUBLISH must not be lost.
    let msg = timeout(Duration::from_secs(5), rx.recv_async())
        .await
        .expect("inbound PUBLISH during resubscribe must reach handoff (not silently discarded)")
        .expect("handoff closed");
    assert_eq!(&*msg.payload, b"keep-me");

    client.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), run).await;
}

/// What the mock broker of the restore tests answers to the n-th SUBSCRIBE of a session.
#[derive(Clone, Copy)]
enum SubackPlan {
    Grant,
    Refuse,
    Silent,
}

/// (session, filters) of every SUBSCRIBE a mock broker received.
type SubscribeLog = Arc<std::sync::Mutex<Vec<(u32, Vec<String>)>>>;

/// Mock broker: session 1 grants and then drops the client; every later session answers its
/// SUBSCRIBEs as `plan(session, nth_subscribe)` says. Returns the port and a log of
/// (session, filters) per SUBSCRIBE.
async fn spawn_restore_broker(plan: fn(u32, usize) -> SubackPlan) -> (u16, SubscribeLog) {
    use crate::codec::SubscribeAckReason;
    use std::sync::atomic::{AtomicU32, Ordering};

    let sessions = Arc::new(AtomicU32::new(0));
    let log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let port = spawn_broker_loop({
        let log = Arc::clone(&log);
        move |mut framed_read, mut framed_write| {
            let session = sessions.fetch_add(1, Ordering::SeqCst) + 1;
            let log = Arc::clone(&log);
            async move {
                if !accept_connect_send_connack(&mut framed_read, &mut framed_write).await {
                    return;
                }
                let mut nth = 0;
                loop {
                    match framed_read.next().await {
                        Some(Ok((DecodedFrame::Control(Packet::Subscribe(sub)), _))) => {
                            log.lock().unwrap().push((
                                session,
                                sub.topic_filters.iter().map(|(filter, _)| filter.to_string()).collect(),
                            ));
                            let answer = if session == 1 {
                                SubackPlan::Grant
                            } else {
                                plan(session, nth)
                            };
                            nth += 1;
                            let (status, reason_string) = match answer {
                                SubackPlan::Grant => (SubscribeAckReason::GrantedQos1, None),
                                SubackPlan::Refuse => (SubscribeAckReason::NotAuthorized, Some("ACL".into())),
                                SubackPlan::Silent => continue,
                            };
                            let _ = framed_write
                                .send(Packet::SubscribeAck(crate::codec::SubscribeAck {
                                    packet_id: sub.packet_id,
                                    properties: Vec::new(),
                                    reason_string,
                                    status: vec![status; sub.topic_filters.len()],
                                }))
                                .await;
                            if session == 1 {
                                return; // drop → reconnect → resubscribe
                            }
                        }
                        Some(Ok((DecodedFrame::Control(Packet::Unsubscribe(unsub)), _))) => {
                            let _ = framed_write
                                .send(Packet::UnsubscribeAck(crate::codec::UnsubscribeAck {
                                    packet_id: unsub.packet_id,
                                    properties: Vec::new(),
                                    reason_string: None,
                                    status: vec![
                                        crate::codec::UnsubscribeAckReason::Success;
                                        unsub.topic_filters.len()
                                    ],
                                }))
                                .await;
                        }
                        Some(Ok(_)) => {}
                        Some(Err(_)) | None => return,
                    }
                }
            }
        }
    })
    .await;
    (port, log)
}

/// Connects, subscribes `filter` (granted by session 1, which then drops the client) and
/// waits for the reconnect.
async fn connect_and_lose_session(
    port: u16,
    filter: &str,
) -> (
    crate::MqttClient,
    tokio::task::JoinHandle<Result<(), ConnectionOutcome>>,
    watch::Receiver<SessionPhase>,
) {
    use crate::client::Subscription;
    use crate::codec::QoS;

    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
    let options = ConnectOptions::new(url)
        .receive_maximum(NonZeroU16::new(8).expect("nz"))
        .ack_timeout(Duration::from_millis(500));
    let (tx, _rx) = flume::bounded::<InboundMessage>(4);
    let (client, connection, mut phase) = connect(
        options,
        static_test_credentials(),
        DelayedReconnect { delay: Duration::ZERO },
        tx,
        NoopLifecycle,
    )
    .expect("connect");
    let run = tokio::spawn(connection.run());
    await_connected(&mut phase).await;
    client
        .subscribe(vec![Subscription::new(filter, QoS::AtLeastOnce)])
        .await
        .expect("subscribe");
    let _ = await_disconnected(&mut phase).await;
    await_connected(&mut phase).await;
    (client, run, phase)
}

/// A refused auto-resubscribe keeps the connection, lists the filter, and the client asks again
/// on the same connection until the broker grants it.
#[tokio::test]
async fn refused_resubscribe_keeps_the_session_and_is_retried() {
    use crate::codec::SubscribeAckReason;

    // Session 2: refuse the resubscribe and the first retry, grant the second retry.
    let (port, log) = spawn_restore_broker(|_, nth| if nth < 2 { SubackPlan::Refuse } else { SubackPlan::Grant }).await;
    let (client, run, mut phase) = connect_and_lose_session(port, "restore/acl").await;

    let mut refused = client.refused_subscriptions();
    let listed = timeout(Duration::from_secs(5), refused.wait_for(|list| !list.is_empty()))
        .await
        .expect("refusal listed")
        .expect("watch open")
        .clone();
    assert_eq!(listed.len(), 1);
    assert_eq!(&*listed[0].filter, "restore/acl");
    assert_eq!(listed[0].reason, SubscribeAckReason::NotAuthorized);
    assert_eq!(listed[0].reason_string.as_deref(), Some("ACL"));

    timeout(Duration::from_secs(5), refused.wait_for(|list| list.is_empty()))
        .await
        .expect("granted on a retry")
        .expect("watch open");
    let _ = phase.borrow_and_update();
    assert!(
        matches!(*phase.borrow(), SessionPhase::Connected { generation: 2, .. }),
        "the refusal must not end the session; got {:?}",
        *phase.borrow()
    );
    let subscribes: Vec<_> = log
        .lock()
        .unwrap()
        .iter()
        .filter(|(session, _)| *session == 2)
        .cloned()
        .collect();
    assert_eq!(
        subscribes.len(),
        3,
        "resubscribe + two retries on session 2: {subscribes:?}"
    );
    assert!(subscribes.iter().all(|(_, filters)| filters == &["restore/acl"]));

    client.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), run).await;
}

/// A retry the broker does not answer at all ends the connection (ack timeout), and the next
/// connection subscribes again.
#[tokio::test]
async fn unanswered_restore_retry_reconnects() {
    // Session 2: refuse the resubscribe, never answer the retry. Session 3 grants.
    let (port, log) = spawn_restore_broker(|session, nth| match (session, nth) {
        (2, 0) => SubackPlan::Refuse,
        (2, _) => SubackPlan::Silent,
        _ => SubackPlan::Grant,
    })
    .await;
    let (client, run, mut phase) = connect_and_lose_session(port, "restore/silent").await;

    let lost = timeout(Duration::from_secs(5), await_disconnected(&mut phase))
        .await
        .expect("missing SUBACK must end the session");
    assert!(
        matches!(&lost, SessionPhase::Disconnected(ConnectionOutcome::Protocol(_))),
        "got {lost:?}"
    );
    await_connected(&mut phase).await;
    let mut refused = client.refused_subscriptions();
    timeout(Duration::from_secs(5), refused.wait_for(|list| list.is_empty()))
        .await
        .expect("session 3 grants the resubscribe")
        .expect("watch open");
    assert!(log.lock().unwrap().iter().any(|(session, _)| *session == 3));

    client.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), run).await;
}

/// Unsubscribing a refused filter takes it off the list and stops the retries.
#[tokio::test]
async fn unsubscribe_stops_restore_retries() {
    let (port, log) = spawn_restore_broker(|_, _| SubackPlan::Refuse).await;
    let (client, run, _phase) = connect_and_lose_session(port, "restore/stop").await;

    let mut refused = client.refused_subscriptions();
    timeout(Duration::from_secs(5), refused.wait_for(|list| !list.is_empty()))
        .await
        .expect("refusal listed")
        .expect("watch open");
    client
        .unsubscribe(vec![bytestring::ByteString::from_static("restore/stop")])
        .await
        .expect("unsubscribe");
    assert!(refused.borrow().is_empty(), "UNSUBACK clears the list");

    let before = log.lock().unwrap().len();
    tokio::time::sleep(super::subscriptions::restore_retry_delay(8) * 2).await;
    assert_eq!(log.lock().unwrap().len(), before, "no SUBSCRIBE after the unsubscribe");

    client.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), run).await;
}

/// Restoring subscriptions repeats the application's SUBSCRIBEs one by one: a broker that
/// caps the filters per SUBSCRIBE (AWS IoT Core at 8) took each of them once already. They
/// are pipelined: the broker here answers only once ten are waiting.
#[tokio::test]
async fn resubscribe_repeats_each_subscribe_and_pipelines_them() {
    use crate::client::Subscription;
    use crate::codec::{QoS, SubscribeAckReason};
    use std::sync::atomic::{AtomicU32, Ordering};

    const CALLS: usize = 12;
    let sessions = Arc::new(AtomicU32::new(0));
    let log: SubscribeLog = Arc::default();
    let port = spawn_broker_loop({
        let log = Arc::clone(&log);
        move |mut framed_read, mut framed_write| {
            let session = sessions.fetch_add(1, Ordering::SeqCst) + 1;
            let log = Arc::clone(&log);
            async move {
                if !accept_connect_send_connack(&mut framed_read, &mut framed_write).await {
                    return;
                }
                let mut held = Vec::new();
                while let Some(Ok((frame, _))) = framed_read.next().await {
                    let DecodedFrame::Control(Packet::Subscribe(sub)) = frame else {
                        continue;
                    };
                    let filters = sub.topic_filters.iter().map(|(filter, _)| filter.to_string()).collect();
                    log.lock().unwrap().push((session, filters));
                    held.push(sub);
                    if session > 1 && held.len() < 10 && log.lock().unwrap().len() < 2 * CALLS {
                        continue;
                    }
                    for sub in held.drain(..) {
                        let _ = framed_write
                            .send(Packet::SubscribeAck(crate::codec::SubscribeAck {
                                packet_id: sub.packet_id,
                                properties: Vec::new(),
                                reason_string: None,
                                status: vec![SubscribeAckReason::GrantedQos1; sub.topic_filters.len()],
                            }))
                            .await;
                    }
                    if session == 1 && log.lock().unwrap().len() == CALLS {
                        return; // drop → reconnect → resubscribe
                    }
                }
            }
        }
    })
    .await;

    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
    let options = ConnectOptions::new(url).ack_timeout(Duration::from_millis(500));
    let (tx, _rx) = flume::bounded::<InboundMessage>(4);
    let (client, connection, mut phase) = connect(
        options,
        static_test_credentials(),
        DelayedReconnect { delay: Duration::ZERO },
        tx,
        NoopLifecycle,
    )
    .expect("connect");
    let run = tokio::spawn(connection.run());
    await_connected(&mut phase).await;
    for call in 0..CALLS {
        let mut filters = vec![Subscription::new(format!("calls/{call}/a"), QoS::AtLeastOnce)];
        if call == 0 {
            filters.push(Subscription::new("calls/0/b", QoS::AtLeastOnce));
        }
        client.subscribe(filters).await.expect("subscribe");
    }
    let _ = await_disconnected(&mut phase).await;
    timeout(Duration::from_secs(5), async {
        while log.lock().unwrap().len() < 2 * CALLS {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("every SUBSCRIBE repeated, without waiting for each SUBACK");
    await_connected(&mut phase).await;

    let log = log.lock().unwrap().clone();
    let per_session = |session| {
        log.iter()
            .filter(|(s, _)| *s == session)
            .map(|(_, f)| f.clone())
            .collect::<Vec<_>>()
    };
    let mut restored = per_session(2);
    for filters in &mut restored {
        filters.sort();
    }
    assert_eq!(restored, per_session(1), "the same SUBSCRIBEs, in the same order");

    client.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), run).await;
}

/// §4.13: a packet broken or out of place before the session runs (instead of the CONNACK,
/// or while subscriptions are restored) gets a DISCONNECT with its reason, as it does later.
#[rstest::rstest]
#[case::reserved_type_before_connack(false, &[0x00, 0x00], 0x81)]
#[case::pubrec_before_connack(false, &[0x50, 0x02, 0x00, 0x01], 0x82)]
#[case::reserved_type_while_restoring(true, &[0x00, 0x00], 0x81)]
#[case::pubrec_while_restoring(true, &[0x50, 0x02, 0x00, 0x01], 0x82)]
// QoS 0 PUBLISH "t" with Topic Alias 1; the client allows no inbound aliases.
#[case::topic_alias_while_restoring(true, &[0x30, 0x08, 0x00, 0x01, b't', 0x03, 0x23, 0x00, 0x01, b'x'], 0x94)]
#[tokio::test]
async fn a_bad_packet_before_the_session_runs_gets_its_disconnect_reason(
    #[case] while_restoring: bool,
    #[case] attack: &'static [u8],
    #[case] reason: u8,
) {
    use crate::client::Subscription;
    use crate::codec::{QoS, SubscribeAckReason};
    use std::sync::atomic::{AtomicU32, Ordering};
    use tokio::io::AsyncWriteExt;

    let sessions = Arc::new(AtomicU32::new(0));
    let (seen_tx, seen_rx) = tokio::sync::mpsc::unbounded_channel();
    let port = spawn_broker_loop(move |mut framed_read, mut framed_write| {
        let session = sessions.fetch_add(1, Ordering::SeqCst) + 1;
        let seen_tx = seen_tx.clone();
        async move {
            let attack_now = !while_restoring || session > 1;
            if !while_restoring {
                let _ = framed_read.next().await; // CONNECT
                let _ = framed_write.get_mut().write_all(attack).await;
            } else if !accept_connect_send_connack(&mut framed_read, &mut framed_write).await {
                return;
            }
            while let Some(Ok((frame, _))) = framed_read.next().await {
                match frame {
                    DecodedFrame::Control(Packet::Subscribe(_)) if attack_now => {
                        let _ = framed_write.get_mut().write_all(attack).await;
                    }
                    DecodedFrame::Control(Packet::Subscribe(sub)) => {
                        let _ = framed_write
                            .send(Packet::SubscribeAck(crate::codec::SubscribeAck {
                                packet_id: sub.packet_id,
                                properties: Vec::new(),
                                reason_string: None,
                                status: vec![SubscribeAckReason::GrantedQos1],
                            }))
                            .await;
                        return; // drop → reconnect → resubscribe
                    }
                    DecodedFrame::Control(Packet::Disconnect(disconnect)) => {
                        let _ = seen_tx.send(disconnect.reason_code as u8);
                        return;
                    }
                    _ => {}
                }
            }
        }
    })
    .await;

    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
    let (tx, _rx) = flume::bounded::<InboundMessage>(4);
    let (client, connection, mut phase) = connect(
        ConnectOptions::new(url),
        static_test_credentials(),
        DelayedReconnect {
            delay: Duration::from_millis(50),
        },
        tx,
        NoopLifecycle,
    )
    .expect("connect");
    let run = tokio::spawn(connection.run());
    if while_restoring {
        await_connected(&mut phase).await;
        client
            .subscribe(vec![Subscription::new("restore/me", QoS::AtLeastOnce)])
            .await
            .expect("subscribe");
    }
    let mut seen_rx = seen_rx;
    let seen = timeout(Duration::from_secs(5), seen_rx.recv())
        .await
        .expect("DISCONNECT in time")
        .expect("broker running");
    assert_eq!(seen, reason, "DISCONNECT reason");

    client.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), run).await;
}

/// after connect+run, watch shows Connected and QoS0 publish works
/// (session start is not blocked by lifecycle consumers).
#[tokio::test]
async fn session_start_sets_watch_connected_and_allows_qos0() {
    use bytes::Bytes;

    let (client, connection, mut phase) = test_connect().await;
    let run = tokio::spawn(connection.run());

    await_connected(&mut phase).await;
    assert!(matches!(
        *phase.borrow(),
        SessionPhase::Connected {
            session: SessionParams { from_connack: true, .. },
            ..
        }
    ));

    let published = timeout(
        Duration::from_secs(3),
        client.send(Publish::new("f10/go", Bytes::from_static(b"go"))),
    )
    .await
    .expect("session start must not block QoS 0 publish (watch Connected is non-blocking)");
    assert!(
        published.is_ok(),
        "QoS 0 publish confirms write task is running; got {published:?}"
    );

    client.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), run).await;
}

/// Recording [`LifecycleHook`] sees Connected then Disconnected (order + delivery).
#[tokio::test]
async fn lifecycle_hook_records_connected_then_disconnected() {
    use std::sync::Mutex;

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Recorded {
        Connected { generation: u64 },
        Disconnected,
    }

    struct RecordingHook {
        events: Mutex<Vec<Recorded>>,
    }

    impl LifecycleHook for RecordingHook {
        fn on_lifecycle(&self, phase: &SessionPhase) {
            let recorded = match phase {
                SessionPhase::Connected { generation, .. } => Recorded::Connected {
                    generation: *generation,
                },
                SessionPhase::Disconnected(_) => Recorded::Disconnected,
                SessionPhase::NeverConnected => {
                    panic!("hook must not receive SessionPhase::NeverConnected")
                }
            };
            self.events.lock().expect("recording hook lock").push(recorded);
        }
    }

    let hook = Arc::new(RecordingHook {
        events: Mutex::new(Vec::new()),
    });
    let port = spawn_mock_broker().await;
    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
    let options = ConnectOptions::new(url).receive_maximum(NonZeroU16::new(8).expect("nz"));
    let (tx, _rx) = flume::bounded::<InboundMessage>(4);
    let (client, connection, mut phase) =
        connect(options, static_test_credentials(), NoReconnect, tx, Arc::clone(&hook)).expect("connect wiring");

    let run = tokio::spawn(connection.run());
    await_connected(&mut phase).await;
    client.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), run).await;

    let events = hook.events.lock().expect("recording hook lock").clone();
    assert!(
        matches!(
            events.as_slice(),
            [Recorded::Connected { generation }, Recorded::Disconnected] if *generation >= 1
        ),
        "hook must see Connected then Disconnected in order, got {events:?}"
    );
}

/// handoff-accepted publishes remain available to the consumer after shutdown.
///
/// Also asserts wire order PUBACK before DISCONNECT (requires the mock broker below).
#[tokio::test]
async fn handoff_survives_shutdown() {
    use tokio::sync::mpsc as tokio_mpsc;
    use tokio::sync::oneshot;

    let (ready_tx, mut ready_rx) = tokio_mpsc::channel::<()>(1);
    let (order_tx, order_rx) = oneshot::channel::<Vec<(&'static str, u16)>>();
    let port = spawn_broker_once(move |mut framed_read, mut framed_write| async move {
        if !accept_connect_send_connack(&mut framed_read, &mut framed_write).await {
            return;
        }
        let _ = ready_tx.send(()).await;
        send_inbound_qos1_publish(
            &mut framed_write,
            "in/handoff",
            b"keep-me",
            NonZeroU16::new(9).expect("nz"),
            crate::codec::PublishProperties::default(),
        )
        .await;
        let mut order = Vec::new();
        while let Some(frame) = framed_read.next().await {
            match frame {
                Ok((DecodedFrame::Control(Packet::PublishAck(ack)), _)) => {
                    order.push(("puback", ack.packet_id.get()));
                }
                Ok((DecodedFrame::Control(Packet::Disconnect(_)), _)) => {
                    order.push(("disconnect", 0));
                    break;
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }
        let _ = order_tx.send(order);
    })
    .await;

    let (handoff_tx, handoff_rx) = flume::bounded::<InboundMessage>(4);
    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
    let options = ConnectOptions::new(url).receive_maximum(NonZeroU16::new(2).expect("nz"));
    let (client, connection, mut phase) = connect(
        options,
        static_test_credentials(),
        NoReconnect,
        handoff_tx,
        NoopLifecycle,
    )
    .expect("connect");

    let run = tokio::spawn(connection.run());
    await_connected(&mut phase).await;
    ready_rx.recv().await.expect("broker ready");

    timeout(Duration::from_secs(2), async {
        while handoff_rx.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("message must land in handoff before shutdown");

    client.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), run).await;

    let got = timeout(Duration::from_secs(2), handoff_rx.recv_async())
        .await
        .expect("handoff recv after shutdown must not hang")
        .expect("handoff must still deliver the accepted publish after shutdown");
    assert_eq!(&*got.payload, b"keep-me");

    let order = timeout(Duration::from_secs(2), order_rx)
        .await
        .expect("peer order timeout")
        .expect("peer order");
    let puback_pos = order.iter().position(|(k, _)| *k == "puback");
    let disconnect_pos = order.iter().position(|(k, _)| *k == "disconnect");
    assert!(
        matches!((puback_pos, disconnect_pos), (Some(p), Some(d)) if p < d),
        "PUBACK must precede DISCONNECT after handoff-accepted QoS1 (got {order:?})"
    );
    assert_eq!(order[puback_pos.expect("puback")].1, 9);
}
