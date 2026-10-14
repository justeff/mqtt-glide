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
        .start_publish(Publish::new("t/q", "x").qos(QoS::AtLeastOnce))
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
        if let Ok(completion) = client.start_publish(Publish::new("t/big", payload.clone())) {
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
            ConnectOptions::new(url.clone()),
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
