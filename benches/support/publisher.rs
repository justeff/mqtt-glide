//! Persistent MQTT 5 publisher for ingress benches (reusable TCP session).

use std::collections::VecDeque;
use std::num::NonZeroU16;
use std::time::Duration;

use bytes::BytesMut;
use bytestring::ByteString;
use futures_util::{FutureExt, SinkExt, StreamExt};
use mqtt_glide::codec::{Connect, DecodedFrame, Disconnect, Packet, PublishDelivery, V5Codec, encode_publish};
use mqtt_glide::{ConnectAckReason, DEFAULT_MAX_PACKET_SIZE, PublishMessage, QoS};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::compare::harness::BenchQos;

type FramedRd = FramedRead<tokio::net::tcp::OwnedReadHalf, mqtt_glide::codec::V5Codec>;
type FramedWr = FramedWrite<tokio::net::tcp::OwnedWriteHalf, mqtt_glide::codec::V5Codec>;

/// Long-lived publisher session used for latency + sustained-load ingress.
pub struct PersistentPublisher {
    framed_read: FramedRd,
    framed_write: FramedWr,
    next_packet_id: u16,
    in_flight: VecDeque<NonZeroU16>,
    /// The broker's CONNACK Receive Maximum: most QoS 1 publishes it accepts in flight.
    receive_max: usize,
}

impl PersistentPublisher {
    /// CONNECT + CONNACK on a fresh TCP session (`TCP_NODELAY`).
    pub async fn connect(port: u16, client_id: &str) -> Self {
        let stream = TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("ingress publisher connect");
        stream.set_nodelay(true).expect("publisher tcp_nodelay");
        let (read_half, write_half) = stream.into_split();
        let mut framed_read = FramedRead::new(read_half, V5Codec::with_max_packet_size(0));
        let mut framed_write = FramedWrite::new(write_half, V5Codec::with_max_packet_size(0));

        let connect = Connect {
            clean_start: true,
            keep_alive: 60,
            client_id: ByteString::from(client_id.to_owned()),
            topic_alias_max: 0,
            ..Connect::default()
        };
        framed_write
            .send(Packet::Connect(Box::new(connect)))
            .await
            .expect("send CONNECT");

        let connack = timeout(Duration::from_secs(5), framed_read.next())
            .await
            .expect("CONNACK wait")
            .expect("stream closed")
            .expect("decode CONNACK");
        let receive_max = match connack {
            (DecodedFrame::Control(Packet::ConnectAck(ack)), _) if ack.reason_code == ConnectAckReason::Success => {
                usize::from(ack.receive_max.get())
            }
            other => panic!("ingress publisher expected Success CONNACK, got {other:?}"),
        };

        Self {
            framed_read,
            framed_write,
            next_packet_id: 1,
            in_flight: VecDeque::new(),
            receive_max,
        }
    }

    /// The broker's Receive Maximum from CONNACK.
    pub fn receive_max(&self) -> usize {
        self.receive_max
    }

    /// QoS 1 publishes awaiting PUBACK.
    pub fn in_flight(&self) -> usize {
        self.in_flight.len()
    }

    /// Sends one PUBLISH at `qos` (QoS1 tracked for PUBACK; QoS0 fire-and-forget).
    pub async fn send(&mut self, qos: BenchQos, topic: &str, payload: &[u8]) {
        match qos {
            BenchQos::AtLeastOnce => {
                let packet_id = self.alloc_packet_id();
                self.write_publish(topic, payload, QoS::AtLeastOnce, Some(packet_id))
                    .await;
                self.in_flight.push_back(packet_id);
            }
            BenchQos::AtMostOnce => {
                self.write_publish(topic, payload, QoS::AtMostOnce, None).await;
            }
        }
    }

    /// Sends `count` PUBLISHes in one write, as a broker sends them, so this one-thread
    /// publisher does not cap the subscriber's rate with a syscall per message. QoS 1
    /// publishes are tracked for PUBACK; the caller keeps them within the window.
    pub async fn send_burst(&mut self, qos: BenchQos, topic: &str, payload: &[u8], count: usize) {
        let message = PublishMessage {
            topic: ByteString::from(topic.to_owned()),
            payload: payload.to_vec().into(),
            properties: Default::default(),
        };
        let mut buf = BytesMut::new();
        for _ in 0..count {
            let (qos, packet_id) = match qos {
                BenchQos::AtMostOnce => (QoS::AtMostOnce, None),
                BenchQos::AtLeastOnce => {
                    let packet_id = self.alloc_packet_id();
                    self.in_flight.push_back(packet_id);
                    (QoS::AtLeastOnce, Some(packet_id))
                }
            };
            let delivery = PublishDelivery {
                dup: false,
                retain: false,
                qos,
                packet_id,
            };
            encode_publish(&message, delivery, DEFAULT_MAX_PACKET_SIZE, &mut buf).expect("encode publish");
        }
        self.framed_write
            .get_mut()
            .write_all(&buf)
            .await
            .expect("write PUBLISH bytes");
    }

    /// Takes every PUBACK already received; waits for one first when `wait` is set.
    pub async fn take_pubacks(&mut self, wait: bool) {
        if wait && !self.in_flight.is_empty() {
            self.recv_one_puback().await;
        }
        while !self.in_flight.is_empty() {
            match self.framed_read.next().now_or_never() {
                Some(frame) => self.check_puback(frame),
                None => break,
            }
        }
    }

    /// Drains outstanding QoS1 PUBACKs (no-op for QoS0).
    pub async fn drain_completions(&mut self, qos: BenchQos) {
        if qos == BenchQos::AtLeastOnce {
            while !self.in_flight.is_empty() {
                self.recv_one_puback().await;
            }
        }
    }

    /// Graceful DISCONNECT (best-effort).
    pub async fn disconnect(mut self) {
        while !self.in_flight.is_empty() {
            self.recv_one_puback().await;
        }
        let _ = self.framed_write.send(Packet::Disconnect(Disconnect::default())).await;
    }

    fn alloc_packet_id(&mut self) -> NonZeroU16 {
        let id = NonZeroU16::new(self.next_packet_id).expect("non-zero packet id");
        self.next_packet_id = if self.next_packet_id == u16::MAX {
            1
        } else {
            self.next_packet_id + 1
        };
        id
    }

    async fn write_publish(&mut self, topic: &str, payload: &[u8], qos: QoS, packet_id: Option<NonZeroU16>) {
        let message = PublishMessage {
            topic: ByteString::from(topic.to_owned()),
            payload: payload.to_vec().into(),
            properties: Default::default(),
        };
        let delivery = PublishDelivery {
            dup: false,
            retain: false,
            qos,
            packet_id,
        };
        let mut buf = BytesMut::new();
        encode_publish(&message, delivery, DEFAULT_MAX_PACKET_SIZE, &mut buf).expect("encode publish");
        self.framed_write
            .get_mut()
            .write_all(&buf)
            .await
            .expect("write PUBLISH bytes");
        self.framed_write.flush().await.expect("flush PUBLISH");
    }

    async fn recv_one_puback(&mut self) {
        let frame = timeout(Duration::from_secs(10), self.framed_read.next())
            .await
            .expect("PUBACK wait");
        self.check_puback(frame);
    }

    fn check_puback(&mut self, frame: Option<Result<(DecodedFrame, u32), mqtt_glide::codec::DecodeError>>) {
        match frame.expect("stream closed").expect("decode PUBACK") {
            (DecodedFrame::Control(Packet::PublishAck(ack)), _) => {
                let expected = self.in_flight.pop_front().expect("PUBACK with empty in-flight window");
                assert_eq!(
                    ack.packet_id, expected,
                    "publisher PUBACK packet id mismatch (broker should ACK in order)"
                );
            }
            other => panic!("ingress publisher expected PUBACK, got {other:?}"),
        }
    }
}

/// Most PUBLISHes the publisher child writes at once.
const MAX_BURST: usize = 64;

/// Child-process entry: publish burst against an already-running broker.
///
/// Env: `MQTT_COMPARE_PORT`, `MQTT_COMPARE_MSGS`, `MQTT_COMPARE_WINDOW`, `MQTT_COMPARE_TOPIC`,
/// `MQTT_COMPARE_PAYLOAD_SIZE`, optional `MQTT_COMPARE_CLIENT_ID`, optional `MQTT_COMPARE_PUB_QOS` (0|1),
/// optional `MQTT_COMPARE_PUB_RATE` (msg/s).
///
/// Without a rate the publisher sends as fast as the window allows (closed loop). With a rate it
/// releases messages on a 1 ms schedule and stamps the first 8 payload bytes with the
/// [`crate::support::cpu::monotonic_ns`] time they were written, so a subscriber that falls
/// behind shows up as latency instead of slowing the publisher down.
pub fn run_publisher_process() {
    let port: u16 = std::env::var("MQTT_COMPARE_PORT")
        .expect("MQTT_COMPARE_PORT")
        .parse()
        .expect("MQTT_COMPARE_PORT u16");
    let messages: usize = std::env::var("MQTT_COMPARE_MSGS")
        .expect("MQTT_COMPARE_MSGS")
        .parse()
        .expect("MQTT_COMPARE_MSGS usize");
    let window: usize = std::env::var("MQTT_COMPARE_WINDOW")
        .unwrap_or_else(|_| "64".into())
        .parse()
        .expect("MQTT_COMPARE_WINDOW usize");
    let topic = std::env::var("MQTT_COMPARE_TOPIC").unwrap_or_else(|_| "bench/compare/ingress".into());
    let payload_size: usize = std::env::var("MQTT_COMPARE_PAYLOAD_SIZE")
        .unwrap_or_else(|_| "256".into())
        .parse()
        .expect("MQTT_COMPARE_PAYLOAD_SIZE usize");
    let qos = match std::env::var("MQTT_COMPARE_PUB_QOS")
        .unwrap_or_else(|_| "1".into())
        .as_str()
    {
        "0" => BenchQos::AtMostOnce,
        "1" => BenchQos::AtLeastOnce,
        other => panic!("invalid MQTT_COMPARE_PUB_QOS={other:?}"),
    };
    let rate: Option<f64> = std::env::var("MQTT_COMPARE_PUB_RATE")
        .ok()
        .map(|rate| rate.parse().expect("MQTT_COMPARE_PUB_RATE msg/s"));
    let client_id = std::env::var("MQTT_COMPARE_CLIENT_ID").unwrap_or_else(|_| {
        format!(
            "compare-pub-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        )
    });
    let payload: Vec<u8> = (0..payload_size).map(|index| (index % 251) as u8).collect();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("publisher runtime");
    rt.block_on(async move {
        let mut publisher = PersistentPublisher::connect(port, &client_id).await;
        let window = window.min(publisher.receive_max()).max(1);
        match rate {
            None => publish_closed_loop(&mut publisher, qos, &topic, &payload, messages, window).await,
            Some(rate) => publish_open_loop(&mut publisher, qos, &topic, payload, messages, window, rate).await,
        }
        publisher.drain_completions(qos).await;
        publisher.disconnect().await;
    });
}

async fn publish_closed_loop(
    publisher: &mut PersistentPublisher,
    qos: BenchQos,
    topic: &str,
    payload: &[u8],
    messages: usize,
    window: usize,
) {
    let mut sent = 0;
    while sent < messages {
        let free = match qos {
            BenchQos::AtMostOnce => window,
            BenchQos::AtLeastOnce => window.saturating_sub(publisher.in_flight()),
        };
        if free == 0 {
            publisher.take_pubacks(true).await;
            continue;
        }
        let burst = free.min(MAX_BURST).min(messages - sent);
        publisher.send_burst(qos, topic, payload, burst).await;
        sent += burst;
        if qos == BenchQos::AtLeastOnce {
            publisher.take_pubacks(false).await;
        }
    }
}

async fn publish_open_loop(
    publisher: &mut PersistentPublisher,
    qos: BenchQos,
    topic: &str,
    mut payload: Vec<u8>,
    messages: usize,
    window: usize,
    rate: f64,
) {
    assert!(
        payload.len() >= 8,
        "open-loop ingress needs a payload of at least 8 bytes"
    );
    let start = crate::support::cpu::monotonic_ns();
    let mut ticker = tokio::time::interval(Duration::from_millis(1));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
    let mut sent = 0usize;
    while sent < messages {
        ticker.tick().await;
        let elapsed_ns = crate::support::cpu::monotonic_ns().saturating_sub(start);
        let due = ((elapsed_ns as f64 * rate / 1e9) as usize).min(messages);
        while sent < due {
            if qos == BenchQos::AtLeastOnce && publisher.in_flight() >= window {
                publisher.take_pubacks(true).await;
                continue;
            }
            let free = match qos {
                BenchQos::AtMostOnce => MAX_BURST,
                BenchQos::AtLeastOnce => (window - publisher.in_flight()).min(MAX_BURST),
            };
            let burst = free.min(due - sent);
            // Stamped when written, not when due: the 1 ms schedule would otherwise add up to
            // 1 ms to every sample. The mock queues without bound, so a slow subscriber never
            // holds this write back and still shows up as latency.
            let now_ns = crate::support::cpu::monotonic_ns();
            payload[..8].copy_from_slice(&now_ns.to_le_bytes());
            publisher.send_burst(qos, topic, &payload, burst).await;
            sent += burst;
        }
        if qos == BenchQos::AtLeastOnce {
            publisher.take_pubacks(false).await;
        }
    }
}
