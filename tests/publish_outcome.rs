//! What a publish reports: the broker's PUBACK when it completes, and when it fails whether
//! the broker can have the message.

use std::time::Duration;

use mqtt_glide::testing::{FaultProxy, PubAck, TestBroker};
use mqtt_glide::{
    AppError, ClientBuilder, ConnectAckReason, Delivery, MqttClient, PublishAckReason, QoS, StandardReconnectPolicy,
};

const WAIT: Duration = Duration::from_secs(2);

async fn client(url: &str, ack_timeout: Duration) -> MqttClient {
    ClientBuilder::new(url)
        .expect("url")
        .discard_inbound()
        .reconnect(StandardReconnectPolicy::new(
            Duration::from_millis(20),
            Duration::from_millis(200),
        ))
        .options(|options| options.ack_timeout(ack_timeout))
        .connect()
        .await
        .expect("connect")
}

#[tokio::test]
async fn a_completed_publish_returns_the_whole_puback() {
    let broker = TestBroker::start().await.expect("broker");
    broker.ack_publishes(|publish| match &*publish.topic {
        "job/online" => PubAck::new(PublishAckReason::Success)
            .reason_string("S")
            .user_property("outcome", "S"),
        "job/queued" => PubAck::new(PublishAckReason::Success).reason_string("Q"),
        _ => PubAck::new(PublishAckReason::NoMatchingSubscribers),
    });
    let app = client(&broker.url(), WAIT).await;

    let online = app
        .publish("job/online", "go")
        .qos(QoS::AtLeastOnce)
        .await
        .expect("online");
    assert_eq!(online.reason(), Some(PublishAckReason::Success));
    assert_eq!(online.reason_string(), Some("S"));
    assert_eq!(online.user_properties().len(), 1);
    assert_eq!(
        (&*online.user_properties()[0].name, &*online.user_properties()[0].value),
        ("outcome", "S")
    );
    assert!(!online.resent());

    let queued = app
        .publish("job/queued", "go")
        .qos(QoS::AtLeastOnce)
        .await
        .expect("queued");
    assert_eq!(queued.reason(), Some(PublishAckReason::Success));
    assert_eq!(queued.reason_string(), Some("Q"));

    let nobody = app
        .publish("job/nobody", "go")
        .qos(QoS::AtLeastOnce)
        .await
        .expect("nobody");
    assert_eq!(nobody.reason(), Some(PublishAckReason::NoMatchingSubscribers));
    assert_eq!(nobody.reason_string(), None);

    let qos0 = app.publish("job/online", "go").await.expect("qos 0");
    assert_eq!(qos0.reason(), None, "QoS 0 has no acknowledgement");

    // 0x10 accepts the message like 0x00 does.
    assert_eq!(broker.wait_for_publishes(4, WAIT).await.len(), 4);
}

#[tokio::test]
async fn a_rejected_publish_says_so_and_is_not_kept() {
    let broker = TestBroker::start().await.expect("broker");
    broker.ack_publishes(|_| PubAck::new(PublishAckReason::QuotaExceeded).reason_string("VIN queue full"));
    let app = client(&broker.url(), WAIT).await;

    let error = app
        .publish("job/1", "go")
        .qos(QoS::AtLeastOnce)
        .await
        .expect_err("rejected");
    assert_eq!(error.delivery, Delivery::Rejected);
    assert!(!error.may_have_been_delivered());
    let AppError::PublishRejected(rejection) = &error.error else {
        panic!("expected PublishRejected, got {error:?}");
    };
    assert_eq!(rejection.reason, PublishAckReason::QuotaExceeded);
    assert_eq!(rejection.reason_string.as_deref(), Some("VIN queue full"));
    assert!(broker.received().is_empty());
}

/// A QoS 1 publish whose caller gave up before it was written reports `NotSent`, and the
/// client keeps that promise: it does not send it once the connection is back. QoS 0 keeps no
/// write state, so its timeout says `Unknown`, but it is not sent afterwards either.
#[tokio::test]
async fn a_publish_timed_out_before_the_write_is_never_sent() {
    let broker = TestBroker::start().await.expect("broker");
    let app = client(&broker.url(), Duration::from_millis(300)).await;

    broker.refuse_connections(Some(ConnectAckReason::ServerUnavailable));
    broker.drop_connections();
    tokio::time::timeout(WAIT, async {
        while app.is_connected() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("disconnected");

    let error = app
        .publish("job/stale", "go")
        .qos(QoS::AtLeastOnce)
        .await
        .expect_err("no connection");
    assert!(matches!(error.error, AppError::PublishTimeout(_)), "{error:?}");
    assert_eq!(error.delivery, Delivery::NotSent);
    let error = app.publish("job/stale-qos0", "go").await.expect_err("no connection");
    assert!(matches!(error.error, AppError::PublishTimeout(_)), "{error:?}");
    assert_eq!(error.delivery, Delivery::Unknown);

    broker.refuse_connections(None);
    app.publish("job/fresh", "go")
        .qos(QoS::AtLeastOnce)
        .ack_timeout(Some(WAIT))
        .await
        .expect("after reconnect");
    let topics: Vec<_> = broker.received().into_iter().map(|publish| publish.topic).collect();
    assert_eq!(topics, ["job/fresh"]);
}

/// A publish written to the connection but never acknowledged reports `Unknown`: here the
/// broker gets it after all, once the stalled link moves again.
#[tokio::test]
async fn a_publish_timed_out_after_the_write_may_have_been_delivered() {
    let broker = TestBroker::start().await.expect("broker");
    let proxy = FaultProxy::start(([127, 0, 0, 1], broker.port()).into())
        .await
        .expect("proxy");
    let app = client(&proxy.url(), Duration::from_millis(300)).await;

    proxy.set_stalled(true);
    let error = app
        .publish("job/1", "go")
        .qos(QoS::AtLeastOnce)
        .await
        .expect_err("stalled");
    assert!(matches!(error.error, AppError::PublishTimeout(_)), "{error:?}");
    assert_eq!(error.delivery, Delivery::Unknown);
    assert!(error.may_have_been_delivered());

    proxy.set_stalled(false);
    let received = broker.wait_for_publishes(1, WAIT).await;
    assert_eq!(received.len(), 1, "the unknown publish did reach the broker");
}

#[tokio::test]
async fn no_ack_timeout_waits_for_a_slow_puback() {
    let broker = TestBroker::start().await.expect("broker");
    let proxy = FaultProxy::start(([127, 0, 0, 1], broker.port()).into())
        .await
        .expect("proxy");
    let app = client(&proxy.url(), Duration::from_millis(100)).await;
    proxy.set_latency(Duration::from_millis(300));

    let response = app
        .publish("job/slow", "go")
        .qos(QoS::AtLeastOnce)
        .ack_timeout(None)
        .await
        .expect("waits past the client's ack timeout");
    assert_eq!(response.reason(), Some(PublishAckReason::Success));

    let completion = app
        .publish("job/slow", "again")
        .qos(QoS::AtLeastOnce)
        .start()
        .await
        .expect("queued");
    completion.wait_for(None).await.expect("waits too");

    let error = app
        .publish("job/slow", "default")
        .qos(QoS::AtLeastOnce)
        .await
        .expect_err("client default times out");
    assert_eq!(error.delivery, Delivery::Unknown);
}
