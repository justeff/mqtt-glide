//! Persistent MQTT 5 publisher for ingress benches (reusable TCP session).

use std::collections::VecDeque;
use std::num::NonZeroU16;
use std::time::Duration;

use bytes::BytesMut;
use bytestring::ByteString;
use futures_util::{SinkExt, StreamExt};
use mqtt_glide::{
    Connect, ConnectAckReason, DEFAULT_MAX_PACKET_SIZE, DecodedFrame, Disconnect, Packet, PublishDelivery,
    PublishMessage, PublishProperties, QoS, V5Codec, encode_publish,
};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::compare::harness::BenchQos;

type FramedRd = FramedRead<tokio::net::tcp::OwnedReadHalf, mqtt_glide::V5Codec>;
type FramedWr = FramedWrite<tokio::net::tcp::OwnedWriteHalf, mqtt_glide::V5Codec>;

/// Long-lived publisher session used for latency + sustained-load ingress.
pub struct PersistentPublisher {
    framed_read: FramedRd,
    framed_write: FramedWr,
    next_packet_id: u16,
    in_flight: VecDeque<NonZeroU16>,
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
        match connack {
            (DecodedFrame::Control(Packet::ConnectAck(ack)), _) if ack.reason_code == ConnectAckReason::Success => {}
            other => panic!("ingress publisher expected Success CONNACK, got {other:?}"),
        }

        Self {
            framed_read,
            framed_write,
            next_packet_id: 1,
            in_flight: VecDeque::new(),
        }
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

    /// Waits until fewer than `max_in_flight` QoS1 publishes are outstanding.
    pub async fn wait_window(&mut self, max_in_flight: usize) {
        while self.in_flight.len() >= max_in_flight {
            self.recv_one_puback().await;
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
            properties: PublishProperties::default(),
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
        let puback = timeout(Duration::from_secs(10), self.framed_read.next())
            .await
            .expect("PUBACK wait")
            .expect("stream closed")
            .expect("decode PUBACK");
        match puback {
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

/// Child-process entry: windowed publish burst against an already-running broker.
///
/// Env: `MQTT_COMPARE_PORT`, `MQTT_COMPARE_MSGS`, `MQTT_COMPARE_WINDOW`, `MQTT_COMPARE_TOPIC`,
/// `MQTT_COMPARE_PAYLOAD_SIZE`, optional `MQTT_COMPARE_CLIENT_ID`, optional `MQTT_COMPARE_PUB_QOS` (0|1).
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
        for _ in 0..messages {
            if qos == BenchQos::AtLeastOnce {
                publisher.wait_window(window).await;
            }
            publisher.send(qos, &topic, &payload).await;
        }
        publisher.drain_completions(qos).await;
        publisher.disconnect().await;
    });
}
