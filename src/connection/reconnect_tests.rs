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
    assert!(connections.load(Ordering::SeqCst) >= 3, "went through the failed attempt");

    client.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(3), run).await;
}
