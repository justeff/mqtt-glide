//! Interop against HiveMQ CE: a second broker implementation with tight flow-control
//! defaults (CONNACK Receive Maximum 10, Topic Alias Maximum 5).

use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::time::Duration;

use mqtt_glide::{ConnectOptions, Publish, QoS, shared_filter};
use tokio::time::timeout;

use crate::common::NoReconnect;
use crate::helpers::{Client, connected, start_hivemq, tcp_url};

async fn client(port: u16) -> Client {
    connected(ConnectOptions::new(tcp_url(port)), NoReconnect, 10_000).await
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
    publisher.stop().await;
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

    let mut subscriber = client(port).await;
    subscriber
        .client
        .subscribe([("interop/state", QoS::AtLeastOnce)])
        .await
        .expect("subscribe");
    let stored = timeout(Duration::from_secs(5), subscriber.inbound.recv())
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
    let live = timeout(Duration::from_secs(5), subscriber.inbound.recv())
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
    subscriber.stop().await;
    publisher.stop().await;
}

/// Shared subscription: two members of one group together receive every message once.
#[tokio::test]
async fn hivemq_shared_subscription_splits_messages() {
    let Some((_broker, port)) = start_hivemq().await else {
        return;
    };
    let filter = shared_filter("workers", "interop/jobs").expect("share name");
    let mut a = client(port).await;
    let mut b = client(port).await;
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
            m = a.inbound.recv() => m,
            m = b.inbound.recv() => m,
            () = tokio::time::sleep(Duration::from_secs(5)) => panic!("only {total} of 200 jobs delivered"),
        }
        .expect("open");
        seen.insert(String::from_utf8(message.payload.to_vec()).expect("utf8"));
        total += 1;
    }
    assert_eq!(seen.len(), 200, "every job exactly once across the group");

    publisher.stop().await;
    a.stop().await;
    b.stop().await;
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
    crate::helpers::join_run_after_shutdown(publisher.run).await;
}

/// Outbound Topic Aliases against brokers with small alias limits (HiveMQ: 5): eight topics
/// in rotation force rebinding; a plain subscriber must still see every message under its
/// own topic.
pub(crate) async fn outbound_aliases_roundtrip(port: u16, topic_prefix: &str) {
    let mut subscriber = client(port).await;
    subscriber
        .client
        .subscribe([(format!("{topic_prefix}/#"), QoS::AtLeastOnce)])
        .await
        .expect("subscribe");

    let publisher = connected(
        ConnectOptions::new(tcp_url(port)).outbound_topic_aliases(64),
        NoReconnect,
        16,
    )
    .await;
    assert!(
        publisher.session.server_topic_alias_maximum > 0,
        "broker must accept aliases for this test"
    );

    let mut expected = Vec::new();
    for round in 0..5 {
        for topic in 0..8 {
            let topic = format!("{topic_prefix}/device-{topic}/telemetry");
            let payload = format!("{round}");
            publisher
                .client
                .publish(topic.clone(), payload.clone())
                .qos(QoS::AtLeastOnce)
                .await
                .expect("aliased publish accepted");
            expected.push((topic, payload));
        }
    }
    for (topic, payload) in expected {
        let message = timeout(Duration::from_secs(5), subscriber.inbound.recv())
            .await
            .expect("delivery")
            .expect("open");
        assert_eq!(
            (&*message.topic, &*message.payload),
            (topic.as_str(), payload.as_bytes())
        );
    }
    assert!(
        publisher.client.is_connected(),
        "broker must not have dropped the aliasing client"
    );

    publisher.stop().await;
    subscriber.stop().await;
}

#[tokio::test]
async fn hivemq_outbound_topic_aliases() {
    let Some((_broker, port)) = start_hivemq().await else {
        return;
    };
    outbound_aliases_roundtrip(port, "interop/alias").await;
}
