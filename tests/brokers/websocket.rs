//! MQTT over WebSocket against Mosquitto's `protocol websockets` listener.

use std::num::NonZeroUsize;
use std::time::Duration;

use mqtt_glide::{BrokerUrl, ConnectOptions, Publish, QoS};
use tokio::time::timeout;

use crate::common::NoReconnect;
use crate::helpers::{Client, connected, start_mosquitto_websockets};

async fn client(url: &str) -> Client {
    connected(
        ConnectOptions::new(BrokerUrl::parse(url).expect("valid url")),
        NoReconnect,
        10_000,
    )
    .await
}

/// Subscribe and publish over WebSocket: a QoS 1 burst plus payloads far larger than one
/// write, all delivered back intact. MQTT runs over WebSocket as a second byte-stream
/// transport ([MQTT-4.2.0-1]).
#[tokio::test]
async fn websocket_roundtrip_burst_and_large_payloads() {
    let Some((_broker, _tcp_port, ws_port)) = start_mosquitto_websockets().await else {
        return;
    };
    let mut ws = client(&format!("ws://127.0.0.1:{ws_port}/mqtt")).await;
    ws.client
        .subscribe([("ws/#", QoS::AtLeastOnce)])
        .await
        .expect("subscribe over WebSocket");

    let publishes = (0..300u32).map(|i| Publish::new("ws/burst", i.to_string()).qos(QoS::AtLeastOnce));
    ws.client
        .publish_batch(publishes, NonZeroUsize::new(64).expect("nz"))
        .await
        .expect("burst over WebSocket");
    let big: Vec<u8> = (0..300_000usize).map(|i| (i % 251) as u8).collect();
    ws.client
        .publish("ws/big", big.clone())
        .qos(QoS::AtLeastOnce)
        .await
        .expect("large publish over WebSocket");

    for i in 0..300u32 {
        let message = timeout(Duration::from_secs(5), ws.inbound.recv())
            .await
            .expect("burst")
            .expect("open");
        assert_eq!(&*message.payload, i.to_string().as_bytes());
    }
    let message = timeout(Duration::from_secs(5), ws.inbound.recv())
        .await
        .expect("big")
        .expect("open");
    assert_eq!(&*message.topic, "ws/big");
    assert_eq!(message.payload.len(), big.len());
    assert!(message.payload == big, "large payload must arrive intact");
    ws.stop().await;
}

/// The two transports interoperate on one broker: WebSocket publisher, TCP subscriber.
#[tokio::test]
async fn websocket_publisher_reaches_tcp_subscriber() {
    let Some((_broker, tcp_port, ws_port)) = start_mosquitto_websockets().await else {
        return;
    };
    let mut tcp = client(&format!("mqtt://127.0.0.1:{tcp_port}")).await;
    tcp.client
        .subscribe([("mixed/t", QoS::AtLeastOnce)])
        .await
        .expect("tcp subscribe");
    let ws = client(&format!("ws://127.0.0.1:{ws_port}/")).await;
    ws.client
        .publish("mixed/t", "over ws")
        .qos(QoS::AtLeastOnce)
        .await
        .expect("ws publish");
    let message = timeout(Duration::from_secs(5), tcp.inbound.recv())
        .await
        .expect("delivery")
        .expect("open");
    assert_eq!(&*message.payload, b"over ws");
    ws.stop().await;
    tcp.stop().await;
}
