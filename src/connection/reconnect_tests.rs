//! Driver behaviour across reconnects and degraded connections, against scripted brokers.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::time::timeout;

use super::test_support::*;
use crate::NoopLifecycle;
use crate::Publish;
use crate::client::connect;
use crate::codec::{DecodedFrame, InboundMessage, Packet, PublishAck, PublishAckReason, QoS};
use crate::options::ConnectOptions;
use crate::util::BrokerUrl;

/// Acks every QoS 1 PUBLISH until the client goes away.
async fn ack_publishes(framed_read: &mut MockFramedRead, framed_write: &mut MockFramedWrite) {
    while let Some(Ok(frame)) = framed_read.next().await {
        if let (DecodedFrame::Publish(packet), _) = frame
            && let Some(packet_id) = packet.meta.packet_id
        {
            let ack = PublishAck {
                packet_id,
                reason_code: PublishAckReason::Success,
                properties: Vec::new(),
                reason_string: None,
            };
            if framed_write.send(Packet::PublishAck(ack)).await.is_err() {
                return;
            }
        }
    }
}

/// A publish issued while disconnected waits in the command channel through a reconnect
/// attempt that fails before CONNACK, and goes out on the session after it.
#[tokio::test]
async fn queued_publish_survives_a_failed_reconnect_attempt() {
    let connections = Arc::new(AtomicUsize::new(0));
    let port = spawn_broker_loop({
        let connections = Arc::clone(&connections);
        move |mut framed_read, mut framed_write| {
            let connections = Arc::clone(&connections);
            async move {
                match connections.fetch_add(1, Ordering::SeqCst) {
                    // Session, then the connection drops.
                    0 => {
                        let _ = accept_connect_send_connack(&mut framed_read, &mut framed_write).await;
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                    // Reconnect attempt that dies before CONNACK.
                    1 => {
                        let _ = framed_read.next().await;
                    }
                    _ => {
                        if accept_connect_send_connack(&mut framed_read, &mut framed_write).await {
                            ack_publishes(&mut framed_read, &mut framed_write).await;
                        }
                    }
                }
            }
        }
    })
    .await;

    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("url");
    let options = ConnectOptions::new(url).ack_timeout(Duration::from_secs(5));
    let (tx, _rx) = flume::bounded::<InboundMessage>(4);
    let reconnect = DelayedReconnect {
        delay: Duration::from_millis(300),
    };
    let (client, connection, mut phase) =
        connect(options, static_test_credentials(), reconnect, tx, NoopLifecycle).expect("wiring");
    let run = tokio::spawn(connection.run());
    await_connected(&mut phase).await;
    await_disconnected(&mut phase).await;

    let published = client
        .try_start_publish(Publish::new("t/q", "x").qos(QoS::AtLeastOnce))
        .expect("queued while disconnected");
    let result = timeout(Duration::from_secs(8), published.wait())
        .await
        .expect("publish resolves");
    assert!(result.is_ok(), "queued publish lost: {result:?}");
    assert!(
        connections.load(Ordering::SeqCst) >= 3,
        "went through the failed attempt"
    );

    client.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(3), run).await;
}

/// A broker that stops reading but keeps the connection open: once the socket is full the
/// write task cannot send PINGREQ either, so the blocked write itself ends the session after
/// 1.5 x Keep Alive.
#[tokio::test]
async fn broker_that_stops_reading_ends_the_session() {
    let port = spawn_broker_once(|mut framed_read, mut framed_write| async move {
        if !accept_connect_send_connack(&mut framed_read, &mut framed_write).await {
            return;
        }
        tokio::time::sleep(Duration::from_secs(30)).await;
        drop((framed_read, framed_write));
    })
    .await;
    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("url");
    let options = ConnectOptions::new(url).keep_alive(Duration::from_secs(1));
    let (tx, _rx) = flume::bounded::<InboundMessage>(4);
    let (client, connection, mut phase) = connect(
        options,
        static_test_credentials(),
        crate::NoReconnect,
        tx,
        NoopLifecycle,
    )
    .expect("wiring");
    let run = tokio::spawn(connection.run());
    await_connected(&mut phase).await;

    // Fill the socket buffers in both directions' kernels.
    let payload = bytes::Bytes::from(vec![0u8; 60 * 1024]);
    let mut pending = Vec::new();
    for _ in 0..2000 {
        if let Ok(completion) = client.try_start_publish(Publish::new("t/big", payload.clone())) {
            pending.push(completion);
        }
        tokio::task::yield_now().await;
    }
    let ended = timeout(Duration::from_secs(8), await_disconnected(&mut phase)).await;
    match ended {
        Ok(crate::SessionPhase::Disconnected(crate::ConnectionOutcome::Transport(error))) => {
            assert!(error.to_string().contains("stopped reading"), "{error}");
        }
        other => panic!("expected a transport loss within 8 s, got {other:?}"),
    }
    client.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(3), run).await;
}

/// Runs `rounds` sessions against a broker that sends `bad_packet` after CONNACK, and returns
/// the DISCONNECT reason the client sent in each (`None`: it closed without one).
async fn disconnect_reasons_after(
    bad_packet: &'static [u8],
    rounds: usize,
) -> Vec<Option<crate::codec::DisconnectReasonCode>> {
    disconnect_reasons_with(|options| options, bad_packet, rounds).await
}

/// Like [`disconnect_reasons_after`], with the client's options adjusted by `configure`.
async fn disconnect_reasons_with(
    configure: impl Fn(ConnectOptions) -> ConnectOptions,
    bad_packet: &'static [u8],
    rounds: usize,
) -> Vec<Option<crate::codec::DisconnectReasonCode>> {
    let (seen_tx, seen_rx) = flume::unbounded();
    let port = spawn_broker_loop(move |mut framed_read, mut framed_write| {
        let seen_tx = seen_tx.clone();
        async move {
            use tokio::io::AsyncWriteExt;
            if !accept_connect_send_connack(&mut framed_read, &mut framed_write).await {
                return;
            }
            if framed_write.get_mut().write_all(bad_packet).await.is_err() {
                let _ = seen_tx.send(None);
                return;
            }
            let mut reason = None;
            while let Ok(Some(frame)) = timeout(Duration::from_secs(2), framed_read.next()).await {
                if let Ok((DecodedFrame::Control(Packet::Disconnect(disconnect)), _)) = frame {
                    reason = Some(disconnect.reason_code);
                }
            }
            let _ = seen_tx.send(reason);
        }
    })
    .await;
    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("url");
    let mut reasons = Vec::with_capacity(rounds);
    for _ in 0..rounds {
        let (tx, _rx) = flume::bounded::<InboundMessage>(4);
        let (_client, connection, _phase) = connect(
            configure(ConnectOptions::new(url.clone())),
            static_test_credentials(),
            crate::NoReconnect,
            tx,
            NoopLifecycle,
        )
        .expect("wiring");
        let _ = timeout(Duration::from_secs(5), connection.run()).await;
        reasons.push(seen_rx.recv_async().await.expect("broker result"));
    }
    reasons
}

/// [MQTT-3.14.1-1]: the DISCONNECT with the reason reaches the broker every time, also on a
/// multi-threaded runtime where the supervisor can cancel the write task before it ran.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn malformed_packet_disconnect_always_reaches_the_broker() {
    // Packet type 0 is forbidden: Malformed Packet.
    let reasons = disconnect_reasons_after(&[0x00, 0x00], 100).await;
    let missing = reasons
        .iter()
        .filter(|reason| **reason != Some(crate::codec::DisconnectReasonCode::MalformedPacket))
        .count();
    assert_eq!(missing, 0, "DISCONNECT 0x81 missing or wrong in {missing}/100 sessions");
}

/// A packet a client must never receive (here PUBREL, part of QoS 2) is a Protocol Error and
/// is answered with DISCONNECT 0x82 before the connection closes.
#[tokio::test]
async fn unexpected_control_packet_gets_protocol_error_disconnect() {
    // PUBREL, packet id 1.
    let reasons = disconnect_reasons_after(&[0x62, 0x02, 0x00, 0x01], 1).await;
    assert_eq!(reasons, [Some(crate::codec::DisconnectReasonCode::ProtocolError)]);
}

/// An invalid inbound Topic Alias is answered with DISCONNECT 0x94 on the wire, not only
/// reported locally (§3.3.2.3.4): an alias above the Topic Alias Maximum we advertised,
/// one nobody bound yet, and alias 0, which fails decoding.
#[rstest::rstest]
#[case::unbound(4, &[0x30, 0x07, 0x00, 0x00, 0x03, 0x23, 0x00, 0x01, b'd'])]
#[case::alias_zero(4, &[0x30, 0x07, 0x00, 0x00, 0x03, 0x23, 0x00, 0x00, b'd'])]
#[tokio::test]
async fn invalid_inbound_topic_alias_gets_topic_alias_invalid_disconnect(
    #[case] topic_alias_maximum: u16,
    #[case] bad_packet: &'static [u8],
) {
    let reasons = disconnect_reasons_with(
        |options| options.topic_alias_maximum(topic_alias_maximum),
        bad_packet,
        1,
    )
    .await;
    assert_eq!(reasons, [Some(crate::codec::DisconnectReasonCode::TopicAliasInvalid)]);
}

/// Inbound Topic Alias bindings do not survive a reconnect: an alias bound on one connection
/// is unknown on the next, and using it is answered with DISCONNECT 0x94 ([MQTT-3.3.2-7]).
#[tokio::test]
async fn inbound_topic_alias_binding_does_not_survive_a_reconnect() {
    use tokio::io::AsyncWriteExt;

    let connections = Arc::new(AtomicUsize::new(0));
    let (reason_tx, reason_rx) = flume::unbounded::<Option<crate::codec::DisconnectReasonCode>>();
    let port = spawn_broker_loop(move |mut framed_read, mut framed_write| {
        let connections = Arc::clone(&connections);
        let reason_tx = reason_tx.clone();
        async move {
            let first = connections.fetch_add(1, Ordering::SeqCst) == 0;
            if !accept_connect_send_connack(&mut framed_read, &mut framed_write).await {
                return;
            }
            if first {
                // QoS 1 PUBLISH "a/b" binding alias 1, packet id 7; wait for its PUBACK so the
                // client has certainly processed the binding, then drop the connection.
                let bind = [
                    0x32, 0x0c, 0x00, 0x03, b'a', b'/', b'b', 0x00, 0x07, 0x03, 0x23, 0x00, 0x01, b'd',
                ];
                if framed_write.get_mut().write_all(&bind).await.is_err() {
                    return;
                }
                while let Some(Ok(frame)) = framed_read.next().await {
                    if matches!(frame, (DecodedFrame::Control(Packet::PublishAck(_)), _)) {
                        return;
                    }
                }
                return;
            }
            // Empty topic + alias 1, which the first connection bound.
            let reuse = [0x30, 0x07, 0x00, 0x00, 0x03, 0x23, 0x00, 0x01, b'd'];
            if framed_write.get_mut().write_all(&reuse).await.is_err() {
                return;
            }
            let mut reason = None;
            while let Ok(Some(frame)) = timeout(Duration::from_secs(1), framed_read.next()).await {
                if let Ok((DecodedFrame::Control(Packet::Disconnect(disconnect)), _)) = frame {
                    reason = Some(disconnect.reason_code);
                }
            }
            let _ = reason_tx.send(reason);
        }
    })
    .await;

    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("url");
    let options = ConnectOptions::new(url).topic_alias_maximum(4);
    let (tx, _rx) = flume::bounded::<InboundMessage>(4);
    let reconnect = DelayedReconnect {
        delay: Duration::from_millis(50),
    };
    let (client, connection, _phase) =
        connect(options, static_test_credentials(), reconnect, tx, NoopLifecycle).expect("wiring");
    let run = tokio::spawn(connection.run());

    let reason = timeout(Duration::from_secs(5), reason_rx.recv_async())
        .await
        .expect("second connection result")
        .expect("broker result");
    assert_eq!(reason, Some(crate::codec::DisconnectReasonCode::TopicAliasInvalid));

    client.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(3), run).await;
}

/// Outbound Topic Aliases are reset per connection: after a reconnect the first PUBLISH of a
/// topic carries its full Topic Name together with the alias again, because the broker's
/// bindings are gone ([MQTT-3.3.2-7]).
#[tokio::test]
async fn outbound_topic_aliases_are_reset_on_reconnect() {
    let connections = Arc::new(AtomicUsize::new(0));
    // (connection index, topic, wire topic alias) of every PUBLISH the broker received.
    let (seen_tx, seen_rx) = flume::unbounded::<(usize, String, Option<u16>)>();
    let (connected_tx, connected_rx) = flume::unbounded::<usize>();
    let port = spawn_broker_loop(move |mut framed_read, mut framed_write| {
        let connections = Arc::clone(&connections);
        let seen_tx = seen_tx.clone();
        let connected_tx = connected_tx.clone();
        async move {
            let index = connections.fetch_add(1, Ordering::SeqCst);
            let ack = crate::codec::ConnectAck {
                topic_alias_max: 4,
                ..Default::default()
            };
            if !accept_connect_send_connack_with(&mut framed_read, &mut framed_write, ack).await {
                return;
            }
            let _ = connected_tx.send(index);
            let mut publishes = 0;
            while let Some(Ok(frame)) = framed_read.next().await {
                if let (DecodedFrame::Publish(packet), _) = frame {
                    let alias = packet.wire_topic_alias.map(std::num::NonZeroU16::get);
                    let _ = seen_tx.send((index, packet.message.topic.to_string(), alias));
                    publishes += 1;
                    // The first connection ends after its two PUBLISHes.
                    if index == 0 && publishes == 2 {
                        return;
                    }
                }
            }
        }
    })
    .await;

    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("url");
    let options = ConnectOptions::new(url).outbound_topic_aliases(4);
    let (tx, _rx) = flume::bounded::<InboundMessage>(4);
    let reconnect = DelayedReconnect {
        delay: Duration::from_millis(50),
    };
    let (client, connection, _phase) =
        connect(options, static_test_credentials(), reconnect, tx, NoopLifecycle).expect("wiring");
    let run = tokio::spawn(connection.run());

    let next = || async {
        timeout(Duration::from_secs(5), seen_rx.recv_async())
            .await
            .expect("publish in time")
            .expect("broker open")
    };
    assert_eq!(connected_rx.recv_async().await.expect("first connection"), 0);
    client.publish("a/b", "1").await.expect("first publish");
    client.publish("a/b", "2").await.expect("second publish");
    assert_eq!(next().await, (0, "a/b".to_owned(), Some(1)), "binds the alias");
    assert_eq!(next().await, (0, String::new(), Some(1)), "reuses it");

    assert_eq!(connected_rx.recv_async().await.expect("reconnect"), 1);
    client.publish("a/b", "3").await.expect("publish after reconnect");
    assert_eq!(
        next().await,
        (1, "a/b".to_owned(), Some(1)),
        "the new connection starts without bindings"
    );

    client.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(3), run).await;
}
