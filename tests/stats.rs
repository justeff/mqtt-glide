//! `MqttClient::stats()` against the in-process test broker.

use std::time::Duration;

use mqtt_glide::codec::{ConnectAckReason, PublishAckReason};
use mqtt_glide::testing::{FaultProxy, TestBroker};
use mqtt_glide::{ClientBuilder, ClientStats, InboundMessage, MqttClient, Publish, QoS, StandardReconnectPolicy};

const WAIT: Duration = Duration::from_secs(2);

async fn client(url: String, id: &str, sink_capacity: usize) -> (MqttClient, flume::Receiver<InboundMessage>) {
    let (tx, rx) = flume::bounded(sink_capacity);
    let client = ClientBuilder::new(url)
        .expect("url")
        .client_id(id)
        .expect("client id")
        .inbound_sink(tx)
        .reconnect(StandardReconnectPolicy::new(
            Duration::from_millis(20),
            Duration::from_millis(100),
        ))
        .connect()
        .await
        .expect("connect");
    (client, rx)
}

/// Polls `client.stats()` until `done` holds (gauges and the read side settle a moment after
/// the caller's await returns).
async fn stats_until(client: &MqttClient, done: impl Fn(&ClientStats) -> bool) -> ClientStats {
    tokio::time::timeout(WAIT, async {
        loop {
            let stats = client.stats();
            if done(&stats) {
                return stats;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("stats did not settle: {:?}", client.stats()))
}

#[tokio::test]
async fn counts_outbound_publishes_acks_and_bytes() {
    let broker = TestBroker::start().await.expect("broker");
    let (app, _rx) = client(broker.url(), "app", 16).await;

    for _ in 0..3 {
        app.publish("t/qos0", "x").await.expect("qos 0");
    }
    for _ in 0..4 {
        app.publish("t/qos1", "x").qos(QoS::AtLeastOnce).await.expect("qos 1");
    }
    broker.reject_publishes(Some(PublishAckReason::NotAuthorized));
    app.publish("t/denied", "x")
        .qos(QoS::AtLeastOnce)
        .await
        .expect_err("rejected");

    let stats = stats_until(&app, |stats| stats.outbound.awaiting_ack == 0).await;
    let out = stats.outbound;
    assert_eq!(
        (out.publishes_qos0, out.publishes_qos1, out.publishes_resent),
        (3, 5, 0)
    );
    assert_eq!((out.publishes_acked, out.publishes_rejected), (4, 1));
    assert_eq!((out.awaiting_quota, out.commands_queued), (0, 0));
    assert!(out.bytes > 0 && stats.inbound.bytes > 0, "{stats:?}");
    assert_eq!(stats.connection.connects, 1);
    assert_eq!(
        (stats.connection.connect_failures, stats.connection.connection_losses),
        (0, 0)
    );
}

#[tokio::test]
async fn counts_inbound_publishes_and_sink_stalls() {
    let broker = TestBroker::start().await.expect("broker");
    let (app, rx) = client(broker.url(), "app", 1).await;
    app.subscribe([("in/#", QoS::AtLeastOnce)]).await.expect("subscribe");

    broker.publish(Publish::new("in/a", "0"));
    broker.publish(Publish::new("in/b", "1").qos(QoS::AtLeastOnce));
    broker.publish(Publish::new("in/c", "2").qos(QoS::AtLeastOnce));
    // The sink holds one message: the next one waits until the test reads.
    stats_until(&app, |stats| stats.inbound.handoff_stalls >= 1).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    for _ in 0..3 {
        tokio::time::timeout(WAIT, rx.recv_async())
            .await
            .expect("message in time")
            .expect("open");
    }

    let stats = stats_until(&app, |stats| stats.inbound.publishes_qos1 == 2).await;
    let inbound = stats.inbound;
    assert_eq!((inbound.publishes_qos0, inbound.duplicates), (1, 0));
    assert!(inbound.handoff_stalls >= 1);
    assert!(inbound.handoff_stall_time >= Duration::from_millis(10), "{inbound:?}");
}

#[tokio::test]
async fn counts_reconnects_and_failed_attempts() {
    let broker = TestBroker::start().await.expect("broker");
    let (app, _rx) = client(broker.url(), "app", 16).await;

    assert!(broker.drop_client("app"));
    let stats = stats_until(&app, |stats| stats.connection.connects == 2).await;
    assert_eq!(stats.connection.connection_losses, 1);

    broker.refuse_connections(Some(ConnectAckReason::ServerUnavailable));
    assert!(broker.drop_client("app"));
    stats_until(&app, |stats| stats.connection.connect_failures >= 2).await;
    broker.refuse_connections(None);
    let stats = stats_until(&app, |stats| stats.connection.connects == 3).await;
    assert_eq!(stats.connection.connection_losses, 2);

    app.shutdown().await.expect("shutdown");
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(app.stats().connection, stats.connection, "a shutdown is not a loss");
}

#[tokio::test]
async fn shows_publishes_waiting_for_their_ack() {
    let broker = TestBroker::start().await.expect("broker");
    let proxy = FaultProxy::start(([127, 0, 0, 1], broker.port()).into())
        .await
        .expect("proxy");
    let (app, _rx) = client(proxy.url(), "app", 16).await;

    proxy.set_stalled(true);
    let pending: Vec<_> = (0..3)
        .map(|_| {
            app.start_publish(Publish::new("t", "x").qos(QoS::AtLeastOnce))
                .expect("queued")
        })
        .collect();
    stats_until(&app, |stats| stats.outbound.awaiting_ack == 3).await;

    proxy.set_stalled(false);
    for completion in pending {
        completion.wait().await.expect("acked");
    }
    let stats = stats_until(&app, |stats| stats.outbound.awaiting_ack == 0).await;
    assert_eq!(stats.outbound.publishes_acked, 3);
}
