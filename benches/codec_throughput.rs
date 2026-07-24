//! Encode microbench (no broker) — attribution for outbound wire work.
//!
//! Not part of Mosquitto compare. Run:
//!
//! ```shell
//! cargo bench --bench codec_throughput
//! ```

use std::num::NonZeroU16;
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use bytestring::ByteString;
use mqtt_glide::{PublishDelivery, PublishMessage, PublishProperties, QoS, encode_publish};

const ITERS: u32 = 50_000;
const PAYLOAD_LEN: usize = 256;

fn main() {
    let message = PublishMessage {
        topic: ByteString::from_static("bench/topic"),
        payload: Bytes::from(vec![0u8; PAYLOAD_LEN]),
        properties: PublishProperties::default(),
    };
    // QoS 1 is the production outbound path; QoS 0 encode cost is indistinguishable here.
    let delivery = PublishDelivery {
        dup: false,
        retain: false,
        qos: QoS::AtLeastOnce,
        packet_id: Some(NonZeroU16::new(1).expect("non-zero")),
    };
    let mut buf = BytesMut::with_capacity(512);

    for _ in 0..1_000 {
        buf.clear();
        encode_publish(&message, delivery, 0, &mut buf).expect("encode");
    }

    let start = Instant::now();
    for _ in 0..ITERS {
        buf.clear();
        encode_publish(&message, delivery, 0, &mut buf).expect("encode");
    }
    let elapsed = start.elapsed();
    let ns_per = elapsed.as_nanos() / u128::from(ITERS);
    let msg_s = (f64::from(ITERS) / elapsed.as_secs_f64()).round() as u64;

    println!("=== mqtt-glide codec_throughput (no broker) ===");
    println!("encode PUBLISH: {ns_per} ns/msg, {msg_s} msg/s (iters={ITERS}, payload={PAYLOAD_LEN} B, QoS 1)");
}
