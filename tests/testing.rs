//! The `testing` helpers used the way an application test would use them.

use std::time::Duration;

use mqtt_glide::codec::{ConnectAckReason, PublishAckReason};
use mqtt_glide::testing::{FaultProxy, TestBroker};
use mqtt_glide::{
    AppError, ClientBuilder, InboundMessage, MqttClient, Publish, QoS, StandardReconnectPolicy, Subscription,
};

const WAIT: Duration = Duration::from_secs(2);

async fn client(url: &str, id: &str) -> (MqttClient, flume::Receiver<InboundMessage>) {
    let (tx, rx) = flume::bounded(64);
    let client = ClientBuilder::new(url)
        .expect("url")
        .client_id(id)
        .expect("client id")
        .inbound_sink(tx)
        .reconnect(StandardReconnectPolicy::new(
            Duration::from_millis(20),
            Duration::from_millis(200),
        ))
        .connect()
        .await
        .expect("connect");
    (client, rx)
}

async fn next(rx: &flume::Receiver<InboundMessage>) -> InboundMessage {
    tokio::time::timeout(WAIT, rx.recv_async())
        .await
        .expect("message in time")
        .expect("channel open")
}

#[tokio::test]
async fn records_what_the_application_publishes() {
    let broker = TestBroker::start().await.expect("broker");
    let (app, _rx) = client(&broker.url(), "app").await;

    app.publish("plant/line-1/state", "running")
        .qos(QoS::AtLeastOnce)
        .retain(true)
        .user_property("source", "test")
        .await
        .expect("publish");
    app.publish("plant/line-1/count", "7").await.expect("publish");

    let received = broker.wait_for_publishes(2, WAIT).await;
    assert_eq!(received.len(), 2);
    assert_eq!(received[0].client_id, "app");
    assert_eq!(&*received[0].topic, "plant/line-1/state");
    assert_eq!(&received[0].payload[..], b"running");
    assert_eq!((received[0].qos, received[0].retain), (QoS::AtLeastOnce, true));
    assert_eq!(&*received[0].properties.user_properties[0].value, "test");
    assert_eq!(received[1].qos, QoS::AtMostOnce);
    assert_eq!(broker.connected_clients(), ["app"]);
}

#[tokio::test]
async fn routes_between_clients_with_wildcards_retained_messages_and_no_local() {
    let broker = TestBroker::start().await.expect("broker");
    let (publisher, publisher_rx) = client(&broker.url(), "publisher").await;
    let (subscriber, subscriber_rx) = client(&broker.url(), "subscriber").await;

    publisher
        .publish("sensors/t1", "21.5")
        .retain(true)
        .await
        .expect("retained publish");
    broker.wait_for_publishes(1, WAIT).await;

    subscriber
        .subscribe([("sensors/+", QoS::AtLeastOnce)])
        .await
        .expect("subscribe");
    let retained = next(&subscriber_rx).await;
    assert!(retained.retain, "stored value arrives flagged as retained");
    assert_eq!(&retained.payload[..], b"21.5");

    publisher
        .subscribe([Subscription::new("sensors/#", QoS::AtMostOnce).no_local(true)])
        .await
        .expect("subscribe");
    // The retained message for the publisher's own subscription, then nothing of its own.
    assert_eq!(&next(&publisher_rx).await.payload[..], b"21.5");
    publisher
        .publish("sensors/t2", "19.0")
        .qos(QoS::AtLeastOnce)
        .await
        .expect("publish");
    let live = next(&subscriber_rx).await;
    assert_eq!(
        (&*live.topic, live.retain, live.qos),
        ("sensors/t2", false, QoS::AtLeastOnce)
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(200), publisher_rx.recv_async())
            .await
            .is_err(),
        "No Local: the publisher does not get its own message"
    );
    assert_eq!(broker.subscriptions("subscriber"), ["sensors/+"]);
}

#[tokio::test]
async fn injects_messages_and_balances_shared_subscriptions() {
    let broker = TestBroker::start().await.expect("broker");
    let (a, a_rx) = client(&broker.url(), "worker-a").await;
    let (b, b_rx) = client(&broker.url(), "worker-b").await;
    for worker in [&a, &b] {
        worker
            .subscribe([("$share/workers/jobs", QoS::AtLeastOnce)])
            .await
            .expect("subscribe");
    }

    for job in 0..4 {
        broker.publish(Publish::new("jobs", format!("job-{job}")).qos(QoS::AtLeastOnce));
    }
    let mut got = [0, 0];
    for _ in 0..4 {
        tokio::select! {
            Ok(_) = a_rx.recv_async() => got[0] += 1,
            Ok(_) = b_rx.recv_async() => got[1] += 1,
        }
    }
    assert_eq!(got, [2, 2], "each job goes to exactly one worker, in turn");
}

#[tokio::test]
async fn a_dropped_client_reconnects_resubscribes_and_its_will_goes_out() {
    let broker = TestBroker::start().await.expect("broker");
    let (observer, observer_rx) = client(&broker.url(), "observer").await;
    observer
        .subscribe([("status/#", QoS::AtMostOnce)])
        .await
        .expect("subscribe");

    let (tx, device_rx) = flume::bounded(16);
    let device = ClientBuilder::new(broker.url())
        .expect("url")
        .client_id("device")
        .expect("client id")
        .will("status/device", "offline")
        .inbound_sink(tx)
        .reconnect(StandardReconnectPolicy::new(
            Duration::from_millis(20),
            Duration::from_millis(200),
        ))
        .connect()
        .await
        .expect("connect");
    device
        .subscribe([("cmd/device", QoS::AtLeastOnce)])
        .await
        .expect("subscribe");

    assert!(broker.drop_client("device"));
    let will = next(&observer_rx).await;
    assert_eq!((&*will.topic, &will.payload[..]), ("status/device", &b"offline"[..]));

    assert!(broker.wait_for_connects(3, WAIT).await, "the device reconnected");
    assert!(
        broker.wait_for_subscription("device", "cmd/device", WAIT).await,
        "subscription restored"
    );
    broker.publish(Publish::new("cmd/device", "reboot").qos(QoS::AtLeastOnce));
    assert_eq!(&next(&device_rx).await.payload[..], b"reboot");
}

#[tokio::test]
async fn dropping_every_connection_makes_all_clients_reconnect() {
    let broker = TestBroker::start().await.expect("broker");
    let (_a, _a_rx) = client(&broker.url(), "a").await;
    let (_b, _b_rx) = client(&broker.url(), "b").await;
    broker.drop_connections();
    assert!(broker.wait_for_connects(4, WAIT).await, "both reconnected");
    tokio::time::timeout(WAIT, async {
        while broker.connected_clients() != ["a", "b"] {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("both connected again");
}

#[tokio::test]
async fn refusals_and_rejections_reach_the_application() {
    let broker = TestBroker::start().await.expect("broker");
    let (app, _rx) = client(&broker.url(), "app").await;

    broker.reject_publishes(Some(PublishAckReason::NotAuthorized));
    let error = app
        .publish("forbidden", "x")
        .qos(QoS::AtLeastOnce)
        .await
        .expect_err("rejected");
    match error {
        AppError::PublishRejected(rejection) => assert_eq!(rejection.reason, PublishAckReason::NotAuthorized),
        other => panic!("expected PublishRejected, got {other:?}"),
    }
    assert!(broker.received().is_empty(), "a rejected publish is not recorded");

    broker.refuse_connections(Some(ConnectAckReason::ServerUnavailable));
    let refused = ClientBuilder::new(broker.url())
        .expect("url")
        .discard_inbound()
        .connect()
        .await;
    assert!(refused.is_err(), "CONNACK refusal surfaces as a connect error");
}

#[tokio::test]
async fn fault_proxy_resets_connections_on_demand() {
    let broker = TestBroker::start().await.expect("broker");
    let proxy = FaultProxy::start(([127, 0, 0, 1], broker.port()).into())
        .await
        .expect("proxy");
    let (app, _rx) = client(&proxy.url(), "behind-proxy").await;
    app.publish("t", "before").qos(QoS::AtLeastOnce).await.expect("publish");

    proxy.reset_all();
    assert!(
        broker.wait_for_connects(2, WAIT).await,
        "client reconnected through the proxy"
    );
    app.publish("t", "after")
        .qos(QoS::AtLeastOnce)
        .await
        .expect("publish after reset");
    let payloads: Vec<_> = broker
        .wait_for_publishes(2, WAIT)
        .await
        .into_iter()
        .map(|publish| publish.payload)
        .collect();
    assert_eq!(payloads, ["before", "after"]);
    assert_eq!(proxy.accepted(), 2);
}
