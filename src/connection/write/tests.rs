use super::*;
use crate::codec::{
    PublishAckReason, PublishDelivery, PublishMessage, PublishProperties, QoS, V5Codec, encode_publish,
};
use crate::connection::{ControlMessage, Generation};
use bytes::Bytes;
use bytestring::ByteString;
use futures_util::StreamExt;
use nonzero_ext::nonzero;
use rstest::rstest;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWrite;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::codec::{FramedRead, FramedWrite};
use tokio_util::sync::CancellationToken;

/// Channels, cancel token, and shared registry for a write task spawned under test.
///
/// Every unit test below drives the write task through the same handles; only a few
/// [`WriteSession`] fields vary per test (see [`WriteSessionOverrides`]).
struct WriteTaskHandle {
    control_tx: mpsc::Sender<ControlMessage>,
    command_tx: mpsc::Sender<ClientCommand>,
    cancel: CancellationToken,
    subscription_registry: Arc<RwLock<SubscriptionRegistry>>,
    join: JoinHandle<WriteOutcome>,
    /// Kept alive so `stub_exit_rx.recv()` does not resolve early (biased select in unit tests).
    _stub_tx: mpsc::Sender<()>,
}

/// [`WriteSession`] fields that vary across write-task unit tests; everything else uses a
/// fixed skeleton (generation 1, 3600s keep-alive so the ping timer never fires within a
/// test, 2s graceful-disconnect timeout, no bench counter).
struct WriteSessionOverrides {
    max_qos: QoS,
    retain_available: bool,
    receive_maximum: NonZeroU16,
    inflight_capacity: NonZeroU16,
}

impl Default for WriteSessionOverrides {
    fn default() -> Self {
        Self {
            max_qos: QoS::AtLeastOnce,
            retain_available: true,
            receive_maximum: nonzero!(1024u16),
            inflight_capacity: nonzero!(4u16),
        }
    }
}

const DUPLEX_BUFFER: usize = 4096;

/// In-memory byte pipe for write-task unit tests (task end framed for egress).
fn duplex_framed_write() -> (FramedWrite<tokio::io::DuplexStream, V5Codec>, tokio::io::DuplexStream) {
    let (task_end, peer_end) = tokio::io::duplex(DUPLEX_BUFFER);
    let framed = FramedWrite::new(task_end, V5Codec::with_max_packet_size(0));
    (framed, peer_end)
}

/// Spawns [`run`] over `framed` with a fresh set of test channels and a [`WriteSession`]
/// built from `overrides` (see [`WriteSessionOverrides::default`] for the shared skeleton).
///
/// No paired read task exists in these unit tests, so `read_done` is completed immediately.
fn spawn_write_task<W>(framed: FramedWrite<W, V5Codec>, overrides: WriteSessionOverrides) -> WriteTaskHandle
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    let (control_tx, control_rx) = mpsc::channel(8);
    let (command_tx, command_rx) = mpsc::channel(8);
    let (stub_tx, stub_rx) = mpsc::channel(1);
    let cancel = CancellationToken::new();
    let (read_done_tx, read_done_rx) = oneshot::channel();
    let _ = read_done_tx.send(());
    let subscription_registry = Arc::new(RwLock::new(SubscriptionRegistry::default()));

    let join = tokio::spawn(run(
        control_rx,
        command_rx,
        stub_rx,
        framed,
        WriteSession {
            generation: Generation(1),
            keep_alive: Duration::from_secs(3600),
            subscription_registry: Arc::clone(&subscription_registry),
            cancel: cancel.clone(),
            shutdown_notify: CancellationToken::new(),
            graceful_disconnect_timeout: Duration::from_secs(2),
            max_qos: overrides.max_qos,
            retain_available: overrides.retain_available,
            receive_maximum: overrides.receive_maximum,
            handoff_stalled: Arc::new(AtomicBool::new(false)),
            inbound_activity: Arc::new(AtomicBool::new(false)),
            bench_inbound_publish_ack_counter: None,
        },
        InflightPots::new(overrides.inflight_capacity),
        read_done_rx,
    ));

    WriteTaskHandle {
        control_tx,
        command_tx,
        cancel,
        subscription_registry,
        join,
        _stub_tx: stub_tx,
    }
}

#[test]
fn late_ack_from_stale_generation_is_discarded() {
    assert!(!ack_generation_applies(Generation(2), Generation(1)));
    assert!(!ack_generation_applies(Generation(1), Generation(0)));
    assert!(ack_generation_applies(Generation(3), Generation(3)));
}

/// [MQTT-2.2.1-3]: app timeout / cancel after SUBSCRIBE is sent must not drop the waiter —
/// late SUBACK still updates the registry and frees the packet id.
#[tokio::test]
async fn timed_out_subscribe_keeps_waiter_until_suback() {
    use crate::codec::{DecodedFrame, Packet, SubscribeAck, SubscribeAckReason};

    let (wired_tx, wired_rx) = oneshot::channel::<NonZeroU16>();
    let (framed, peer_end) = duplex_framed_write();
    let peer = tokio::spawn(async move {
        let mut framed = FramedRead::new(peer_end, V5Codec::with_max_packet_size(0));
        let mut wired_tx = Some(wired_tx);
        while let Some(frame) = framed.next().await {
            match frame {
                Ok((DecodedFrame::Control(Packet::Subscribe(sub)), _)) => {
                    if let Some(tx) = wired_tx.take() {
                        let _ = tx.send(sub.packet_id);
                    }
                }
                Ok((DecodedFrame::Control(Packet::Disconnect(_)), _)) => break,
                _ => {}
            }
        }
    });

    let harness = spawn_write_task(
        framed,
        WriteSessionOverrides {
            receive_maximum: nonzero!(8u16),
            inflight_capacity: nonzero!(8u16),
            ..Default::default()
        },
    );

    let (reply_tx, reply_rx) = oneshot::channel();
    harness
        .command_tx
        .try_send(ClientCommand::Subscribe {
            filters: vec![(ByteString::from_static("t/late"), SubscriptionOptions::default())],
            reply: reply_tx,
        })
        .expect("subscribe command");

    let packet_id = timeout(Duration::from_secs(2), wired_rx)
        .await
        .expect("wire timeout")
        .expect("wired packet id");

    // App SubscribeAckTimeout / cancel must not drop the waiter or free the packet id early.
    drop(reply_rx);
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    harness
        .control_tx
        .try_send(ControlMessage::SubscribeAck {
            ack: SubscribeAck {
                packet_id,
                properties: Vec::new(),
                reason_string: None,
                status: vec![SubscribeAckReason::GrantedQos1],
            },
            generation: Generation(1),
        })
        .expect("suback");

    // Allow write task to apply SUBACK.
    tokio::task::yield_now().await;
    tokio::time::sleep(Duration::from_millis(30)).await;

    let snap = harness.subscription_registry.read().expect("registry").snapshot();
    assert_eq!(
        snap.len(),
        1,
        "late SUBACK after app timeout must still update the subscription registry"
    );
    assert_eq!(&*snap[0].0, "t/late");

    drop(harness.command_tx);
    drop(harness.control_tx);
    harness.cancel.cancel();
    let _ = timeout(Duration::from_secs(2), harness.join).await;
    let _ = timeout(Duration::from_secs(2), peer).await;
}

/// WriteOutcome → AppError mapping for waiter failure paths.
#[test]
fn app_error_from_write_outcome_mapping() {
    assert!(matches!(
        app_error_from_write_outcome(&WriteOutcome::Transport(TransportError::Connection(
            "broken pipe".into()
        ))),
        AppError::SendFailed(_)
    ));
    assert!(matches!(
        app_error_from_write_outcome(&WriteOutcome::Protocol(ProtocolError::Violation("encode".into()))),
        AppError::SendFailed(_)
    ));
    assert_eq!(
        app_error_from_write_outcome(&WriteOutcome::CommandChannelClosed),
        AppError::CommandChannelClosed
    );
    assert_eq!(
        app_error_from_write_outcome(&WriteOutcome::ControlChannelClosed),
        AppError::CommandChannelClosed
    );
    assert_eq!(
        app_error_from_write_outcome(&WriteOutcome::Cancelled),
        AppError::ConnectionLost
    );
    assert_eq!(
        app_error_from_write_outcome(&WriteOutcome::Shutdown),
        AppError::ConnectionLost
    );
    assert_eq!(
        app_error_from_write_outcome(&WriteOutcome::StubExit),
        AppError::CommandChannelClosed
    );
}

/// transport/encode outcomes must fail QoS1 waiters as SendFailed, not channel-closed.
#[tokio::test]
async fn fail_waiters_for_outcome_maps_transport_not_command_channel_closed() {
    let mut waiters = SubUnsubWaiters {
        subscribe: HashMap::new(),
        unsubscribe: HashMap::new(),
    };
    let mut publish_waiters = PublishWaiters::new();
    let mut inflight = InflightPots::new(nonzero!(4u16));
    let id = inflight.alloc_publish().expect("alloc");
    let (tx, rx) = oneshot::channel();
    publish_waiters.insert(id, tx);

    let outcome = WriteOutcome::Transport(TransportError::Connection("broken pipe".into()));
    fail_waiters_for_outcome(&mut waiters, &mut publish_waiters, &mut inflight, &outcome);

    assert!(publish_waiters.is_empty());
    assert!(!inflight.publish_contains(id));
    let err = rx.await.expect("oneshot").expect_err("waiter must fail");
    assert!(
        matches!(err, AppError::SendFailed(_)),
        "transport write failure must map to SendFailed, got {err:?}"
    );
    assert!(
        !matches!(err, AppError::CommandChannelClosed),
        "must not report CommandChannelClosed for transport failure"
    );
}

/// graceful/shutdown waiter failure uses `ConnectionLost`, not channel-closed.
#[tokio::test]
async fn graceful_shutdown_fail_all_waiters_uses_connection_lost() {
    let mut waiters = SubUnsubWaiters {
        subscribe: HashMap::new(),
        unsubscribe: HashMap::new(),
    };
    let mut publish_waiters = PublishWaiters::new();
    let mut inflight = InflightPots::new(nonzero!(4u16));
    let id = inflight.alloc_publish().expect("alloc");
    let (tx, rx) = oneshot::channel();
    publish_waiters.insert(id, tx);

    // Same error class as `graceful_disconnect` / `app_error_from_write_outcome(Shutdown)`.
    assert_eq!(
        app_error_from_write_outcome(&WriteOutcome::Shutdown),
        AppError::ConnectionLost
    );
    fail_all_waiters(
        &mut waiters,
        &mut publish_waiters,
        &mut inflight,
        &AppError::ConnectionLost,
    );

    assert!(publish_waiters.is_empty());
    assert!(!inflight.publish_contains(id));
    let err = rx.await.expect("oneshot").expect_err("waiter must fail");
    assert_eq!(err, AppError::ConnectionLost);
    assert!(
        !matches!(err, AppError::CommandChannelClosed),
        "graceful disconnect must not report CommandChannelClosed"
    );
}

#[test]
fn validate_subscribe_request_rejects_no_local_on_shared_filter() {
    let filters = [(
        ByteString::from_static("$share/g/t/1"),
        SubscriptionOptions {
            no_local: true,
            ..SubscriptionOptions::default()
        },
    )];
    let err = validate_subscribe_request(&filters).expect_err("no_local on $share must fail");
    assert!(matches!(err, AppError::InvalidRequest(_)));
}

/// [MQTT-4.7.3-1] Topic Filters MUST be at least one character long (subscribe + unsubscribe).
#[rstest]
#[case::subscribe(FilterOp::Subscribe)]
#[case::unsubscribe(FilterOp::Unsubscribe)]
fn validate_request_rejects_empty_filter_string(#[case] op: FilterOp) {
    let err = match op {
        FilterOp::Subscribe => {
            let filters = [(ByteString::from_static(""), SubscriptionOptions::default())];
            validate_subscribe_request(&filters).expect_err("empty filter must fail")
        }
        FilterOp::Unsubscribe => {
            let filters = [ByteString::from_static("")];
            validate_unsubscribe_request(&filters).expect_err("empty filter must fail")
        }
    };
    assert!(matches!(err, AppError::InvalidRequest(_)));
}

#[derive(Clone, Copy)]
enum FilterOp {
    Subscribe,
    Unsubscribe,
}

#[test]
fn validate_subscribe_request_accepts_shared_filter_without_no_local() {
    let filters = [(ByteString::from_static("$share/g/t/1"), SubscriptionOptions::default())];
    validate_subscribe_request(&filters).expect("default options on shared filter");
}

#[rstest]
#[case::empty_share_name("$share//topic")]
#[case::plus_in_share_name("$share/a+b/topic")]
#[case::missing_topic_segment("$share/g")]
#[case::empty_topic_after_slash("$share/g/")]
#[case::nul_in_plain_filter("a\0b")]
#[case::nul_in_shared_topic("$share/g/a\0b")]
fn validate_subscribe_request_rejects_invalid_share_name(#[case] filter: &str) {
    let filters = [(ByteString::from(filter), SubscriptionOptions::default())];
    let err = validate_subscribe_request(&filters).expect_err(filter);
    assert!(
        matches!(err, AppError::InvalidRequest(_)),
        "{filter}: expected InvalidRequest, got {err:?}"
    );
}

/// Wiring smoke: placement rules reach subscribe/unsubscribe as InvalidRequest.
#[rstest]
#[case::hash_glued("sport/tennis#")]
#[case::plus_glued("sport+")]
fn validate_request_rejects_misplaced_wildcards(#[case] filter: &str) {
    let sub = [(ByteString::from(filter), SubscriptionOptions::default())];
    let sub_err = validate_subscribe_request(&sub).expect_err(filter);
    assert!(
        matches!(sub_err, AppError::InvalidRequest(_)),
        "subscribe {filter}: {sub_err:?}"
    );

    let unsub = [ByteString::from(filter)];
    let unsub_err = validate_unsubscribe_request(&unsub).expect_err(filter);
    assert!(
        matches!(unsub_err, AppError::InvalidRequest(_)),
        "unsubscribe {filter}: {unsub_err:?}"
    );
}

#[test]
fn validate_request_accepts_valid_wildcard_filters() {
    let filter = "$share/g/a/+/b/#";
    let sub = [(ByteString::from_static(filter), SubscriptionOptions::default())];
    validate_subscribe_request(&sub).unwrap_or_else(|e| panic!("subscribe {filter}: {e:?}"));

    let unsub = [ByteString::from_static(filter)];
    validate_unsubscribe_request(&unsub).unwrap_or_else(|e| panic!("unsubscribe {filter}: {e:?}"));
}

#[rstest]
#[case::empty("", "publish topic must not be empty")]
#[case::plus_wildcard("a/+", "publish topic must not contain wildcard characters")]
#[case::hash_wildcard("a/#", "publish topic must not contain wildcard characters")]
#[case::embedded_nul("a\0b", "publish topic must not contain U+0000")]
fn finish_publish_encode_error_maps_bad_topic_to_invalid_request(#[case] topic: &str, #[case] expected: &str) {
    use bytes::BytesMut;

    let delivery = PublishDelivery {
        dup: false,
        retain: false,
        qos: QoS::AtMostOnce,
        packet_id: None,
    };
    let message = PublishMessage {
        topic: ByteString::from(topic),
        payload: Bytes::from_static(b"x"),
        properties: PublishProperties::default(),
    };
    let mut buf = BytesMut::new();
    let err = encode_publish(&message, delivery, u32::MAX, &mut buf).expect_err(topic);
    let (reply_tx, reply_rx) = oneshot::channel();
    finish_publish_encode_error(Some(reply_tx), &message, err).expect("session must stay up");
    let app_err = reply_rx.blocking_recv().expect("reply").expect_err("must reject");
    match app_err {
        AppError::InvalidRequest(detail) => assert_eq!(detail, expected),
        other => panic!("{topic:?}: expected InvalidRequest({expected:?}), got {other:?}"),
    }
}

#[tokio::test]
async fn handle_publish_rejects_invalid_topic_without_wire_publish() {
    use crate::codec::{DecodedFrame, Packet};

    let (framed, peer_end) = duplex_framed_write();
    let peer = tokio::spawn(async move {
        let mut framed = FramedRead::new(peer_end, V5Codec::with_max_packet_size(0));
        let mut saw_publish = false;
        let _ = timeout(Duration::from_millis(100), async {
            while let Some(frame) = framed.next().await {
                if let Ok((DecodedFrame::Publish(_), _)) = frame {
                    saw_publish = true;
                    break;
                }
                if let Ok((DecodedFrame::Control(Packet::Disconnect(_)), _)) = frame {
                    break;
                }
            }
        })
        .await;
        saw_publish
    });

    let harness = spawn_write_task(framed, WriteSessionOverrides::default());

    let (tx, rx) = oneshot::channel();
    harness
        .command_tx
        .send(ClientCommand::Publish {
            message: PublishMessage {
                topic: ByteString::from_static("a/+"),
                payload: Bytes::from_static(b"x"),
                properties: PublishProperties::default(),
            },
            qos: QoS::AtMostOnce,
            retain: false,
            reply: tx,
        })
        .await
        .expect("publish command");
    let err = timeout(Duration::from_secs(1), rx)
        .await
        .expect("reply timeout")
        .expect("oneshot")
        .expect_err("wildcard topic must fail");
    match err {
        AppError::InvalidRequest(detail) => {
            assert_eq!(detail, "publish topic must not contain wildcard characters");
        }
        other => panic!("expected InvalidRequest(wildcard), got {other:?}"),
    }

    drop(harness.command_tx);
    drop(harness.control_tx);
    harness.cancel.cancel();
    let _ = timeout(Duration::from_secs(2), harness.join).await;

    let saw_publish = timeout(Duration::from_secs(2), peer)
        .await
        .expect("peer timeout")
        .expect("peer join");
    assert!(!saw_publish, "rejected publish must not appear on the wire");
}

/// [MQTT-3.14.4-1/2]: protocol-error DISCONNECT is last frame; write half closes (peer EOF)
/// before the write task waits for supervisor cancel.
#[tokio::test]
async fn protocol_disconnect_is_terminal_no_frames_after() {
    use crate::codec::{DecodedFrame, Packet};

    let (framed, peer_end) = duplex_framed_write();
    let peer = tokio::spawn(async move {
        let mut framed = FramedRead::new(peer_end, V5Codec::with_max_packet_size(0));
        let mut frames = Vec::new();
        while let Some(frame) = framed.next().await {
            match frame {
                Ok((DecodedFrame::Control(Packet::Disconnect(d)), _)) => {
                    frames.push(format!("disconnect:{:?}", d.reason_code));
                }
                Ok((DecodedFrame::Control(Packet::PingRequest), _)) => {
                    frames.push("ping".into());
                }
                Ok((DecodedFrame::Control(Packet::PublishAck(_)), _)) => {
                    frames.push("puback".into());
                }
                Ok(_) => frames.push("other".into()),
                Err(_) => break,
            }
        }
        // Stream ended → peer observed Network Connection close ([MQTT-3.14.4-2]).
        frames
    });

    let harness = spawn_write_task(
        framed,
        WriteSessionOverrides {
            max_qos: QoS::ExactlyOnce,
            ..Default::default()
        },
    );

    harness
        .control_tx
        .send(ControlMessage::ProtocolDisconnect)
        .await
        .expect("protocol disconnect");
    harness
        .control_tx
        .send(ControlMessage::PingReq)
        .await
        .expect("late ping");
    let packet_id = NonZeroU16::new(7).expect("nz");
    harness
        .control_tx
        .send(ControlMessage::AckInboundPublish { packet_id })
        .await
        .expect("late puback");
    drop(harness.control_tx);

    // Peer must see DISCONNECT then EOF without waiting for supervisor cancel.
    let frames = timeout(Duration::from_secs(2), peer)
        .await
        .expect("peer must EOF after protocol DISCONNECT closes the write half")
        .expect("peer join");
    assert_eq!(
        frames,
        vec!["disconnect:ProtocolError".to_string()],
        "only protocol DISCONNECT may appear on the wire; got {frames:?}"
    );

    harness.cancel.cancel();
    let outcome = timeout(Duration::from_secs(2), harness.join)
        .await
        .expect("write join timeout")
        .expect("write join");
    assert_eq!(
        outcome,
        WriteOutcome::Cancelled,
        "write waits for supervisor cancel after closing the socket"
    );
}

/// [MQTT-3.2.2-11/14]: broker limits reject on the write path — no PUBLISH on the wire.
#[tokio::test]
async fn handle_publish_rejects_above_broker_limits_without_wire_publish() {
    use crate::codec::{DecodedFrame, Packet, PublishMessage, PublishProperties};
    use crate::connection::ClientCommand;
    use bytes::Bytes;

    let (framed, peer_end) = duplex_framed_write();
    let peer = tokio::spawn(async move {
        let mut framed = FramedRead::new(peer_end, V5Codec::with_max_packet_size(0));
        let mut saw_publish = false;
        // Brief window: rejected publishes must not appear; then EOF when we drop write.
        let _ = timeout(Duration::from_millis(100), async {
            while let Some(frame) = framed.next().await {
                if let Ok((DecodedFrame::Publish(_), _)) = frame {
                    saw_publish = true;
                    break;
                }
                if let Ok((DecodedFrame::Control(Packet::Disconnect(_)), _)) = frame {
                    break;
                }
            }
        })
        .await;
        saw_publish
    });

    let harness = spawn_write_task(
        framed,
        WriteSessionOverrides {
            max_qos: QoS::AtMostOnce,
            retain_available: false,
            ..Default::default()
        },
    );

    let message = PublishMessage {
        topic: ByteString::from_static("t/1"),
        payload: Bytes::from_static(b"x"),
        properties: PublishProperties::default(),
    };

    let (tx_qos, rx_qos) = oneshot::channel();
    harness
        .command_tx
        .send(ClientCommand::Publish {
            message: message.clone(),
            qos: QoS::AtLeastOnce,
            retain: false,
            reply: tx_qos,
        })
        .await
        .expect("qos publish");
    let err = timeout(Duration::from_secs(1), rx_qos)
        .await
        .expect("qos reply timeout")
        .expect("qos oneshot")
        .expect_err("QoS above Maximum must fail");
    assert!(matches!(err, AppError::InvalidRequest(_)), "{err:?}");

    let (tx_ret, rx_ret) = oneshot::channel();
    harness
        .command_tx
        .send(ClientCommand::Publish {
            message,
            qos: QoS::AtMostOnce,
            retain: true,
            reply: tx_ret,
        })
        .await
        .expect("retain publish");
    let err = timeout(Duration::from_secs(1), rx_ret)
        .await
        .expect("retain reply timeout")
        .expect("retain oneshot")
        .expect_err("retain when unavailable must fail");
    assert!(matches!(err, AppError::InvalidRequest(_)), "{err:?}");

    drop(harness.command_tx);
    drop(harness.control_tx);
    harness.cancel.cancel();
    let _ = timeout(Duration::from_secs(2), harness.join).await;

    let saw_publish = timeout(Duration::from_secs(2), peer)
        .await
        .expect("peer timeout")
        .expect("peer join");
    assert!(!saw_publish, "rejected publishes must not appear on the wire");
}

#[tokio::test]
async fn complete_publish_ack_ignores_stale_generation_without_releasing_waiter() {
    let mut waiters = PublishWaiters::new();
    let mut inflight = InflightPots::new(nonzero!(4u16));
    let id = inflight.alloc_publish().expect("alloc");
    let (tx, mut rx) = oneshot::channel();
    waiters.insert(id, tx);

    complete_publish_ack(
        &mut waiters,
        &mut inflight,
        Generation(2),
        Generation(1),
        PublishAck {
            packet_id: id,
            reason_code: PublishAckReason::Success,
            properties: Vec::new(),
            reason_string: None,
        },
    );

    assert!(
        !waiters.is_empty(),
        "stale-generation PUBACK must leave the production PublishWaiters entry in place"
    );
    assert!(inflight.publish_contains(id));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut rx).await.is_err(),
        "waiter must stay pending after stale-generation PUBACK"
    );

    complete_publish_ack(
        &mut waiters,
        &mut inflight,
        Generation(2),
        Generation(2),
        PublishAck {
            packet_id: id,
            reason_code: PublishAckReason::Success,
            properties: Vec::new(),
            reason_string: None,
        },
    );
    assert!(waiters.is_empty());
    assert!(!inflight.publish_contains(id));
    assert!(matches!(rx.await, Ok(Ok(()))));
}

#[tokio::test]
async fn complete_publish_ack_maps_failure_reason_to_publish_rejected() {
    let mut waiters = PublishWaiters::new();
    let mut inflight = InflightPots::new(nonzero!(4u16));
    let id = inflight.alloc_publish().expect("alloc");
    let (tx, rx) = oneshot::channel();
    waiters.insert(id, tx);

    complete_publish_ack(
        &mut waiters,
        &mut inflight,
        Generation(1),
        Generation(1),
        PublishAck {
            packet_id: id,
            reason_code: PublishAckReason::NotAuthorized,
            properties: Vec::new(),
            reason_string: None,
        },
    );

    assert!(waiters.is_empty());
    assert!(!inflight.publish_contains(id));
    let err = rx.await.expect("oneshot").expect_err("failure PUBACK");
    assert!(
        matches!(err, AppError::PublishRejected(_)),
        "expected PublishRejected, got {err:?}"
    );
}

/// [MQTT-2.2.1-3]: after QoS1 PUBLISH is on the wire, dropping the caller oneshot must not
/// free the packet id — a second publish at capacity waits (parked) until that PUBACK.
#[tokio::test]
async fn cancelled_qos1_publish_keeps_packet_id_until_puback() {
    use crate::codec::{DecodedFrame, Packet, PublishAck, PublishAckReason};

    let (wired_tx, mut wired_rx) = mpsc::unbounded_channel::<NonZeroU16>();
    let (framed, peer_end) = duplex_framed_write();
    let peer = tokio::spawn(async move {
        let mut framed = FramedRead::new(peer_end, V5Codec::with_max_packet_size(0));
        while let Some(frame) = framed.next().await {
            match frame {
                Ok((DecodedFrame::Publish(p), _)) => {
                    let _ = wired_tx.send(p.meta.packet_id.expect("QoS1 packet id"));
                }
                Ok((DecodedFrame::Control(Packet::Disconnect(_)), _)) => break,
                _ => {}
            }
        }
    });

    let harness = spawn_write_task(
        framed,
        WriteSessionOverrides {
            receive_maximum: nonzero!(1u16),
            inflight_capacity: nonzero!(1u16),
            ..Default::default()
        },
    );

    let message = PublishMessage {
        topic: ByteString::from_static("t/cancel"),
        payload: Bytes::from_static(b"x"),
        properties: PublishProperties::default(),
    };
    let (reply_tx, reply_rx) = oneshot::channel();
    harness
        .command_tx
        .try_send(ClientCommand::Publish {
            message: message.clone(),
            qos: QoS::AtLeastOnce,
            retain: false,
            reply: reply_tx,
        })
        .expect("first publish command");

    let packet_id = timeout(Duration::from_secs(2), wired_rx.recv())
        .await
        .expect("wire timeout")
        .expect("wired packet id");

    // Caller cancels after encode must not free the packet id until PUBACK.
    drop(reply_rx);
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    let (reply2_tx, mut reply2_rx) = oneshot::channel();
    harness
        .command_tx
        .try_send(ClientCommand::Publish {
            message: message.clone(),
            qos: QoS::AtLeastOnce,
            retain: false,
            reply: reply2_tx,
        })
        .expect("second publish command");
    assert!(
        timeout(Duration::from_millis(200), &mut reply2_rx).await.is_err(),
        "second publish must wait (parked) while the cancelled publish still holds the only slot"
    );
    assert!(
        wired_rx.try_recv().is_err(),
        "parked publish must not reach the wire before the slot is freed"
    );

    harness
        .control_tx
        .try_send(ControlMessage::PublishAck {
            ack: PublishAck {
                packet_id,
                reason_code: PublishAckReason::Success,
                properties: Vec::new(),
                reason_string: None,
            },
            generation: Generation(1),
        })
        .expect("puback");

    // PUBACK frees the slot: the parked publish goes out and now awaits its own PUBACK.
    let second_id = timeout(Duration::from_secs(2), wired_rx.recv())
        .await
        .expect("parked publish must reach the wire after PUBACK")
        .expect("wired packet id");
    assert_eq!(second_id, packet_id, "freed id is reused for the parked publish");
    assert!(
        timeout(Duration::from_millis(50), reply2_rx).await.is_err(),
        "parked publish completes only on its own PUBACK"
    );

    drop(harness.command_tx);
    drop(harness.control_tx);
    harness.cancel.cancel();
    let _ = timeout(Duration::from_secs(2), harness.join).await;
    let _ = timeout(Duration::from_secs(2), peer).await;
}

/// Pre-encode cancel: closed oneshot skips alloc so capacity stays available.
#[tokio::test]
async fn closed_publish_reply_before_encode_skips_packet_id_alloc() {
    use crate::codec::{DecodedFrame, Packet};

    let (framed, peer_end) = duplex_framed_write();
    let peer = tokio::spawn(async move {
        let mut framed = FramedRead::new(peer_end, V5Codec::with_max_packet_size(0));
        let mut saw_publish = false;
        let _ = timeout(Duration::from_millis(150), async {
            while let Some(frame) = framed.next().await {
                if let Ok((DecodedFrame::Publish(_), _)) = frame {
                    saw_publish = true;
                    break;
                }
                if let Ok((DecodedFrame::Control(Packet::Disconnect(_)), _)) = frame {
                    break;
                }
            }
        })
        .await;
        saw_publish
    });

    let harness = spawn_write_task(
        framed,
        WriteSessionOverrides {
            receive_maximum: nonzero!(1u16),
            inflight_capacity: nonzero!(1u16),
            ..Default::default()
        },
    );

    let message = PublishMessage {
        topic: ByteString::from_static("t/pre-cancel"),
        payload: Bytes::from_static(b"x"),
        properties: PublishProperties::default(),
    };
    let (reply_tx, reply_rx) = oneshot::channel();
    drop(reply_rx);
    harness
        .command_tx
        .try_send(ClientCommand::Publish {
            message: message.clone(),
            qos: QoS::AtLeastOnce,
            retain: false,
            reply: reply_tx,
        })
        .expect("cancelled publish command");

    // Give the write task a turn to observe the closed oneshot and skip alloc.
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    let (reply2_tx, reply2_rx) = oneshot::channel();
    harness
        .command_tx
        .try_send(ClientCommand::Publish {
            message,
            qos: QoS::AtLeastOnce,
            retain: false,
            reply: reply2_tx,
        })
        .expect("follow-up publish command");
    assert!(
        timeout(Duration::from_millis(100), reply2_rx).await.is_err(),
        "pre-encode cancel must leave publish capacity free for a subsequent QoS1 publish"
    );

    drop(harness.command_tx);
    drop(harness.control_tx);
    harness.cancel.cancel();
    let _ = timeout(Duration::from_secs(2), harness.join).await;
    let saw = timeout(Duration::from_secs(2), peer)
        .await
        .expect("peer timeout")
        .expect("peer join");
    assert!(saw, "follow-up publish after pre-encode cancel must reach the wire");
}

/// MQTT §7.2: pending inbound PUBACKs must egress (flushed) before DISCONNECT.
///
/// Uses a test gate so `AckInboundPublish` arrives while Shutdown is already entered;
/// shutdown must flush pending inbound PUBACKs before DISCONNECT is sent.
#[tokio::test]
async fn shutdown_flushes_pending_inbound_publish_acks_before_disconnect() {
    use crate::codec::{DecodedFrame, Packet};

    let (framed, peer_end) = duplex_framed_write();
    let peer = tokio::spawn(async move {
        let mut framed = FramedRead::new(peer_end, V5Codec::with_max_packet_size(0));
        let mut order = Vec::new();
        while let Some(frame) = framed.next().await {
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
        order
    });

    let gate = Arc::new(shutdown_test_hook::Gate::default());
    shutdown_test_hook::install(Arc::clone(&gate));

    let harness = spawn_write_task(
        framed,
        WriteSessionOverrides {
            max_qos: QoS::ExactlyOnce,
            ..Default::default()
        },
    );

    harness
        .command_tx
        .send(ClientCommand::Shutdown)
        .await
        .expect("queue Shutdown");
    timeout(Duration::from_secs(2), gate.entered.notified())
        .await
        .expect("write must enter Shutdown arm");

    let packet_id = NonZeroU16::new(42).expect("nz");
    harness
        .control_tx
        .send(ControlMessage::AckInboundPublish { packet_id })
        .await
        .expect("queue late inbound PUBACK while Shutdown is paused");
    gate.release.notify_waiters();

    let outcome = timeout(Duration::from_secs(2), harness.join)
        .await
        .expect("write join timeout")
        .expect("write join");
    assert_eq!(outcome, WriteOutcome::Shutdown);

    let order = timeout(Duration::from_secs(2), peer)
        .await
        .expect("peer timeout")
        .expect("peer join");
    assert!(
        order.iter().any(|(kind, _)| *kind == "puback") && order.iter().any(|(kind, _)| *kind == "disconnect"),
        "expected PUBACK and DISCONNECT on the wire, got {order:?}"
    );
    let puback_pos = order.iter().position(|(kind, _)| *kind == "puback").expect("puback");
    let disconnect_pos = order
        .iter()
        .position(|(kind, _)| *kind == "disconnect")
        .expect("disconnect");
    assert!(
        puback_pos < disconnect_pos,
        "ack means flushed: PUBACK must precede DISCONNECT on the wire (got {order:?})"
    );
    assert_eq!(order[puback_pos].1, 42);
}
