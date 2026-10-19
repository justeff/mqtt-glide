//! The `testing` helpers used the way an application test would use them.

use std::time::Duration;

use mqtt_glide::testing::{FaultProxy, TestBroker};
use mqtt_glide::{
    AppError, ClientBuilder, ConnectError, InboundMessage, MqttClient, Publish, QoS, StandardReconnectPolicy,
    Subscription,
};
use mqtt_glide::{ConnectAckReason, PublishAckReason, SubscribeAckReason, TransportError};
use tokio::sync::mpsc;

const WAIT: Duration = Duration::from_secs(2);

async fn client(url: &str, id: &str) -> (MqttClient, mpsc::Receiver<InboundMessage>) {
    let (tx, rx) = mpsc::channel(64);
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

async fn next(rx: &mut mpsc::Receiver<InboundMessage>) -> InboundMessage {
    tokio::time::timeout(WAIT, rx.recv())
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
    let (publisher, mut publisher_rx) = client(&broker.url(), "publisher").await;
    let (subscriber, mut subscriber_rx) = client(&broker.url(), "subscriber").await;

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
    let retained = next(&mut subscriber_rx).await;
    assert!(retained.retain, "stored value arrives flagged as retained");
    assert_eq!(&retained.payload[..], b"21.5");

    publisher
        .subscribe([Subscription::new("sensors/#", QoS::AtMostOnce).no_local(true)])
        .await
        .expect("subscribe");
    // The retained message for the publisher's own subscription, then nothing of its own.
    assert_eq!(&next(&mut publisher_rx).await.payload[..], b"21.5");
    publisher
        .publish("sensors/t2", "19.0")
        .qos(QoS::AtLeastOnce)
        .await
        .expect("publish");
    let live = next(&mut subscriber_rx).await;
    assert_eq!(
        (&*live.topic, live.retain, live.qos),
        ("sensors/t2", false, QoS::AtLeastOnce)
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(200), publisher_rx.recv())
            .await
            .is_err(),
        "No Local: the publisher does not get its own message"
    );
    assert_eq!(broker.subscriptions("subscriber"), ["sensors/+"]);
}

#[tokio::test]
async fn injects_messages_and_balances_shared_subscriptions() {
    let broker = TestBroker::start().await.expect("broker");
    let (a, mut a_rx) = client(&broker.url(), "worker-a").await;
    let (b, mut b_rx) = client(&broker.url(), "worker-b").await;
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
            Some(_) = a_rx.recv() => got[0] += 1,
            Some(_) = b_rx.recv() => got[1] += 1,
        }
    }
    assert_eq!(got, [2, 2], "each job goes to exactly one worker, in turn");
}

/// The shutdown sequence of `examples/graceful_shutdown.rs`: a worker leaving a shared group
/// unsubscribes, shuts down, and its inbound channel ends once the driver has finished, while
/// the rest of the group takes over.
#[tokio::test]
async fn a_worker_leaving_a_shared_group_hands_over_and_its_channel_ends() {
    let broker = TestBroker::start().await.expect("broker");
    let (a, mut a_rx) = client(&broker.url(), "worker-a").await;
    let (b, mut b_rx) = client(&broker.url(), "worker-b").await;
    for worker in [&a, &b] {
        worker
            .subscribe([("$share/workers/jobs", QoS::AtLeastOnce)])
            .await
            .expect("subscribe");
    }

    a.unsubscribe(["$share/workers/jobs"]).await.expect("unsubscribe");
    a.shutdown().await.expect("shutdown");
    tokio::time::timeout(WAIT, async { while a_rx.recv().await.is_some() {} })
        .await
        .expect("the channel ends once the driver has finished");

    for job in 0..4 {
        broker.publish(Publish::new("jobs", format!("job-{job}")).qos(QoS::AtLeastOnce));
    }
    for _ in 0..4 {
        next(&mut b_rx).await;
    }
    assert_eq!(broker.connected_clients(), ["worker-b"]);
}

#[tokio::test]
async fn a_dropped_client_reconnects_resubscribes_and_its_will_goes_out() {
    let broker = TestBroker::start().await.expect("broker");
    let (observer, mut observer_rx) = client(&broker.url(), "observer").await;
    observer
        .subscribe([("status/#", QoS::AtMostOnce)])
        .await
        .expect("subscribe");

    let (tx, mut device_rx) = mpsc::channel(16);
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
    let will = next(&mut observer_rx).await;
    assert_eq!((&*will.topic, &will.payload[..]), ("status/device", &b"offline"[..]));

    assert!(broker.wait_for_connects(3, WAIT).await, "the device reconnected");
    assert!(
        broker.wait_for_subscription("device", "cmd/device", WAIT).await,
        "subscription restored"
    );
    broker.publish(Publish::new("cmd/device", "reboot").qos(QoS::AtLeastOnce));
    assert_eq!(&next(&mut device_rx).await.payload[..], b"reboot");
}

/// A broker that refuses a subscription after a reconnect (an ACL revoked meanwhile) leaves
/// the client connected and publishing; the client lists the refusal and gets the
/// subscription back on its own once the broker allows it again.
#[tokio::test]
async fn a_subscription_refused_after_reconnect_comes_back_when_allowed_again() {
    let broker = TestBroker::start().await.expect("broker");
    let (device, mut device_rx) = client(&broker.url(), "device").await;
    device
        .subscribe([("cmd/device", QoS::AtLeastOnce), ("cfg/device", QoS::AtLeastOnce)])
        .await
        .expect("subscribe");

    broker.refuse_subscription("cmd/device", Some(SubscribeAckReason::NotAuthorized));
    broker.drop_connections();
    let mut refused = device.refused_subscriptions();
    let listed = tokio::time::timeout(WAIT, refused.wait_for(|list| !list.is_empty()))
        .await
        .expect("refusal listed")
        .expect("watch open")
        .clone();
    assert_eq!(listed.len(), 1);
    assert_eq!(
        (&*listed[0].filter, listed[0].reason),
        ("cmd/device", SubscribeAckReason::NotAuthorized)
    );
    assert!(device.is_connected(), "a refused filter does not end the connection");
    assert!(broker.wait_for_subscription("device", "cfg/device", WAIT).await);
    device
        .publish("status/device", "online")
        .qos(QoS::AtLeastOnce)
        .await
        .expect("publishing keeps working");

    broker.refuse_subscription("cmd/device", None);
    // First retry after 2.5 to 5 s.
    assert!(
        broker
            .wait_for_subscription("device", "cmd/device", Duration::from_secs(8))
            .await,
        "retried on the same connection"
    );
    tokio::time::timeout(WAIT, refused.wait_for(|list| list.is_empty()))
        .await
        .expect("list empty again")
        .expect("watch open");
    assert_eq!(broker.connect_count(), 2, "no reconnect for the retry");
    broker.publish(Publish::new("cmd/device", "reboot").qos(QoS::AtLeastOnce));
    assert_eq!(&next(&mut device_rx).await.payload[..], b"reboot");
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
    match error.error {
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
    assert!(
        matches!(
            &refused,
            Err(ConnectError::Transport(TransportError::ConnectRefused {
                reason: ConnectAckReason::ServerUnavailable,
                ..
            }))
        ),
        "a CONNACK refusal surfaces as a connect error with its reason code: {:?}",
        refused.err()
    );
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

/// A second client with the same Client Identifier takes the session (0x8E). The first one
/// sees the typed reason and, with the standard policy, waits its long delay instead of
/// taking the session straight back.
#[tokio::test]
async fn a_client_whose_session_was_taken_over_does_not_take_it_back() {
    use mqtt_glide::{ConnectionOutcome, DisconnectReasonCode, SessionPhase};

    let broker = TestBroker::start().await.expect("broker");
    // Reconnect delay 20 ms, long delay (taken after a takeover) 5 s.
    let first = ClientBuilder::new(broker.url())
        .expect("url")
        .client_id("vcom-dispatch")
        .expect("client id")
        .discard_inbound()
        .reconnect(StandardReconnectPolicy::new(
            Duration::from_millis(20),
            Duration::from_secs(5),
        ))
        .connect()
        .await
        .expect("connect");
    let mut phase = first.session_phase();
    let (second, _rx2) = client(&broker.url(), "vcom-dispatch").await;

    let outcome = tokio::time::timeout(WAIT, async {
        loop {
            if let SessionPhase::Disconnected(outcome) = phase.borrow_and_update().clone() {
                return outcome;
            }
            phase.changed().await.expect("phase");
        }
    })
    .await
    .expect("first client disconnected");
    assert!(
        matches!(
            outcome,
            ConnectionOutcome::Transport(TransportError::BrokerDisconnect {
                reason: DisconnectReasonCode::SessionTakenOver,
                ..
            })
        ),
        "{outcome:?}"
    );

    // Without the long delay the first client is back within 20 ms and takes the session.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(broker.connect_count(), 2, "no immediate reconnect");
    assert!(second.is_connected());
}

async fn persistent_client(url: &str, id: &str) -> (MqttClient, mpsc::Receiver<InboundMessage>) {
    let (tx, rx) = mpsc::channel(64);
    let client = ClientBuilder::new(url)
        .expect("url")
        .client_id(id)
        .expect("client id")
        .inbound_sink(tx)
        .reconnect(StandardReconnectPolicy::new(
            Duration::from_millis(20),
            Duration::from_millis(200),
        ))
        .options(|options| options.clean_start(false).session_expiry(Some(Duration::from_secs(60))))
        .connect()
        .await
        .expect("connect");
    (client, rx)
}

/// A client with a session gets the QoS 1 messages that arrived while it was away, without
/// subscribing again; QoS 0 ones are not kept.
#[tokio::test]
async fn a_resumed_session_receives_what_arrived_while_away() {
    let broker = TestBroker::start().await.expect("broker");
    let (worker, mut inbox) = persistent_client(&broker.url(), "worker").await;
    worker
        .subscribe([("jobs/#", QoS::AtLeastOnce)])
        .await
        .expect("subscribe");

    broker.refuse_connections(Some(ConnectAckReason::ServerUnavailable));
    broker.drop_client("worker");
    assert!(broker.has_session("worker"));
    broker.publish(Publish::new("jobs/1", "kept").qos(QoS::AtLeastOnce));
    broker.publish(Publish::new("jobs/2", "not kept"));
    broker.refuse_connections(None);

    let message = next(&mut inbox).await;
    assert_eq!((&*message.topic, &message.payload[..]), ("jobs/1", &b"kept"[..]));
    assert!(
        tokio::time::timeout(Duration::from_millis(200), inbox.recv())
            .await
            .is_err(),
        "a QoS 0 message is not kept for an absent client"
    );
    assert_eq!(broker.subscriptions("worker"), ["jobs/#"], "restored with the session");
}

/// A PUBACK lost on the way: the client resends the publish with DUP into its resumed
/// session, and the broker sees it twice.
#[tokio::test]
async fn a_publish_whose_puback_was_lost_is_resent_with_dup() {
    let broker = TestBroker::start().await.expect("broker");
    let (dispatcher, _rx) = persistent_client(&broker.url(), "dispatcher").await;

    broker.withhold_pubacks(true);
    let completion = dispatcher
        .publish("vehicle/1/cmd", "unlock")
        .qos(QoS::AtLeastOnce)
        .ack_timeout(None)
        .start()
        .await
        .expect("queued");
    assert_eq!(broker.wait_for_publishes(1, WAIT).await.len(), 1);

    broker.withhold_pubacks(false);
    broker.drop_client("dispatcher");
    let response = tokio::time::timeout(WAIT, completion.wait_for(None))
        .await
        .expect("acknowledged after the resend")
        .expect("accepted");
    assert!(response.resent());
    let received = broker.received();
    assert_eq!(received.len(), 2, "the broker got it twice");
    assert_eq!((received[0].dup, received[1].dup), (false, true));
    assert_eq!(received[0].payload, received[1].payload);
}

/// Without a Session Expiry Interval the session ends with the connection; Clean Start
/// discards a kept one.
#[tokio::test]
async fn sessions_end_without_expiry_or_with_clean_start() {
    let broker = TestBroker::start().await.expect("broker");
    let (_plain, _rx) = client(&broker.url(), "plain").await;
    broker.drop_client("plain");
    assert!(!broker.has_session("plain"));

    let (persistent, _rx) = persistent_client(&broker.url(), "fresh").await;
    persistent
        .subscribe([("a/#", QoS::AtLeastOnce)])
        .await
        .expect("subscribe");
    persistent.shutdown().await.expect("shutdown");
    tokio::time::timeout(WAIT, async {
        while !broker.has_session("fresh") {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("session kept after a clean DISCONNECT");
    let (_restarted, _rx) = client(&broker.url(), "fresh").await;
    assert!(
        broker.subscriptions("fresh").is_empty(),
        "Clean Start dropped the session"
    );
}
