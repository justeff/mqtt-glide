//! Interop against HiveMQ CE: a second broker implementation with tight flow-control
//! defaults (CONNACK Receive Maximum 10, Topic Alias Maximum 5).

use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::time::Duration;

use mqtt_glide::{
    BrokerUrl, ConnectOptions, InboundMessage, MqttClient, NoopLifecycle, Publish, QoS, SessionParams, SessionPhase,
    connect, shared_filter,
};
use tokio::time::timeout;

use crate::common::{NoReconnect, static_credentials, wait_for_phase};
use crate::helpers::start_hivemq;

struct Client {
    client: MqttClient,
    session: SessionParams,
    inbound: flume::Receiver<InboundMessage>,
    run: tokio::task::JoinHandle<Result<(), mqtt_glide::ConnectionOutcome>>,
}

async fn client(port: u16) -> Client {
    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
    let (tx, inbound) = flume::bounded::<InboundMessage>(10_000);
    let (client, connection, mut phase) = connect(
        ConnectOptions::new(url),
        static_credentials(),
        NoReconnect,
        tx,
        NoopLifecycle,
    )
    .expect("connect");
    let run = tokio::spawn(connection.run());
    let SessionPhase::Connected { session, .. } = wait_for_phase(&mut phase, Duration::from_secs(15), |p| {
        matches!(p, SessionPhase::Connected { .. })
    })
    .await
    else {
        unreachable!("waited for Connected");
    };
    Client {
        client,
        session,
        inbound,
        run,
    }
}

async fn stop(client: Client) {
    client.client.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), client.run).await;
}

/// HiveMQ allows 10 unacknowledged QoS 1 publishes; a window of 64 must queue, not fail.
#[tokio::test]
async fn hivemq_qos1_burst_beyond_receive_maximum() {
    let Some((_broker, port)) = start_hivemq().await else {
        return;
    };
    let publisher = client(port).await;
    assert!(
        publisher.session.server_receive_maximum.get() < 64,
        "test needs a narrow broker window, got {}",
        publisher.session.server_receive_maximum
    );
    let publishes = (0..1_000u32).map(|i| Publish::new("interop/burst", i.to_string()).qos(QoS::AtLeastOnce));
    timeout(
        Duration::from_secs(30),
        publisher
            .client
            .publish_batch(publishes, NonZeroUsize::new(64).expect("nz")),
    )
    .await
    .expect("burst must not hang")
    .expect("burst beyond Receive Maximum must succeed");
    stop(publisher).await;
}

/// A retained message arrives flagged `retain` on subscribe; a later live one does not.
#[tokio::test]
async fn hivemq_retain_flag_distinguishes_stored_from_live() {
    let Some((_broker, port)) = start_hivemq().await else {
        return;
    };
    let publisher = client(port).await;
    publisher
        .client
        .publish("interop/state", "stored")
        .qos(QoS::AtLeastOnce)
        .retain(true)
        .await
        .expect("retained publish");

    let subscriber = client(port).await;
    subscriber
        .client
        .subscribe([("interop/state", QoS::AtLeastOnce)])
        .await
        .expect("subscribe");
    let stored = timeout(Duration::from_secs(5), subscriber.inbound.recv_async())
        .await
        .expect("retained delivery")
        .expect("open");
    assert_eq!(&*stored.payload, b"stored");
    assert!(
        stored.retain,
        "message sent because of the subscription must carry RETAIN"
    );

    publisher
        .client
        .publish("interop/state", "live")
        .qos(QoS::AtLeastOnce)
        .retain(true)
        .await
        .expect("live publish");
    let live = timeout(Duration::from_secs(5), subscriber.inbound.recv_async())
        .await
        .expect("live delivery")
        .expect("open");
    assert_eq!(&*live.payload, b"live");
    assert!(
        !live.retain,
        "a live delivery must not carry RETAIN (Retain As Published is off)"
    );
    assert_eq!(live.qos, QoS::AtLeastOnce);

    // Clear the retained message so reruns start clean.
    publisher
        .client
        .publish("interop/state", "")
        .retain(true)
        .await
        .expect("clear retained");
    stop(subscriber).await;
    stop(publisher).await;
}

/// Shared subscription: two members of one group together receive every message once.
#[tokio::test]
async fn hivemq_shared_subscription_splits_messages() {
    let Some((_broker, port)) = start_hivemq().await else {
        return;
    };
    let filter = shared_filter("workers", "interop/jobs").expect("share name");
    let a = client(port).await;
    let b = client(port).await;
    for member in [&a, &b] {
        member
            .client
            .subscribe([(filter.clone(), QoS::AtLeastOnce)])
            .await
            .expect("shared subscribe");
    }
    let publisher = client(port).await;
    let publishes = (0..200u32).map(|i| Publish::new("interop/jobs", i.to_string()).qos(QoS::AtLeastOnce));
    publisher
        .client
        .publish_batch(publishes, NonZeroUsize::new(32).expect("nz"))
        .await
        .expect("publish jobs");

    let mut seen = BTreeSet::new();
    let mut total = 0;
    while total < 200 {
        let message = tokio::select! {
            m = a.inbound.recv_async() => m,
            m = b.inbound.recv_async() => m,
            () = tokio::time::sleep(Duration::from_secs(5)) => panic!("only {total} of 200 jobs delivered"),
        }
        .expect("open");
        seen.insert(String::from_utf8(message.payload.to_vec()).expect("utf8"));
        total += 1;
    }
    assert_eq!(seen.len(), 200, "every job exactly once across the group");

    stop(publisher).await;
    stop(a).await;
    stop(b).await;
}

/// Shutdown right after a burst still gets every PUBACK from HiveMQ before DISCONNECT.
#[tokio::test]
async fn hivemq_shutdown_drains_outbound() {
    let Some((_broker, port)) = start_hivemq().await else {
        return;
    };
    let publisher = client(port).await;
    let mut completions = Vec::new();
    for i in 0..50u32 {
        let publish = Publish::new("interop/drain", i.to_string()).qos(QoS::AtLeastOnce);
        completions.push(publisher.client.start_publish(publish).await.expect("enqueue"));
    }
    publisher.client.shutdown().await.expect("shutdown");
    for completion in completions {
        completion
            .wait()
            .await
            .expect("publish queued before shutdown is acknowledged");
    }
    let _ = timeout(Duration::from_secs(5), publisher.run).await;
}

/// Outbound Topic Aliases against brokers with small alias limits (HiveMQ: 5): eight topics
/// in rotation force rebinding; a plain subscriber must still see every message under its
/// own topic.
pub(crate) async fn outbound_aliases_roundtrip(port: u16, topic_prefix: &str) {
    let subscriber = client(port).await;
    subscriber
        .client
        .subscribe([(format!("{topic_prefix}/#"), QoS::AtLeastOnce)])
        .await
        .expect("subscribe");

    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
    let (tx, _rx) = flume::bounded::<InboundMessage>(16);
    let (publisher, connection, mut phase) = connect(
        ConnectOptions::new(url).outbound_topic_aliases(64),
        static_credentials(),
        NoReconnect,
        tx,
        NoopLifecycle,
    )
    .expect("connect");
    let run = tokio::spawn(connection.run());
    let SessionPhase::Connected { session, .. } = wait_for_phase(&mut phase, Duration::from_secs(15), |p| {
        matches!(p, SessionPhase::Connected { .. })
    })
    .await
    else {
        unreachable!("waited for Connected");
    };
    assert!(
        session.server_topic_alias_maximum > 0,
        "broker must accept aliases for this test"
    );

    let mut expected = Vec::new();
    for round in 0..5 {
        for topic in 0..8 {
            let topic = format!("{topic_prefix}/device-{topic}/telemetry");
            let payload = format!("{round}");
            publisher
                .publish(topic.clone(), payload.clone())
                .qos(QoS::AtLeastOnce)
                .await
                .expect("aliased publish accepted");
            expected.push((topic, payload));
        }
    }
    for (topic, payload) in expected {
        let message = timeout(Duration::from_secs(5), subscriber.inbound.recv_async())
            .await
            .expect("delivery")
            .expect("open");
        assert_eq!(
            (&*message.topic, &*message.payload),
            (topic.as_str(), payload.as_bytes())
        );
    }
    assert!(
        publisher.is_connected(),
        "broker must not have dropped the aliasing client"
    );

    publisher.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), run).await;
    stop(subscriber).await;
}

#[tokio::test]
async fn hivemq_outbound_topic_aliases() {
    let Some((_broker, port)) = start_hivemq().await else {
        return;
    };
    outbound_aliases_roundtrip(port, "interop/alias").await;
}
