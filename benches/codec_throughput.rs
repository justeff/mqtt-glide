//! Codec microbench (no broker) — attribution for wire encode/decode work.
//!
//! Not part of Mosquitto compare. Run:
//!
//! ```shell
//! cargo bench --bench codec_throughput
//! ```
//!
//! Decode rows include the per-fill `extend_from_slice` refill (same shape as the
//! real ingress path, where the socket read copies into the codec buffer).

use std::hint::black_box;
use std::num::NonZeroU16;
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use bytestring::ByteString;
use mqtt_glide::codec::{Packet, PublishAck, PublishDelivery, V5Codec, encode_publish};
use mqtt_glide::{PublishMessage, PublishProperties, QoS};

const ITERS: u32 = 200_000;
const PAYLOAD_LEN: usize = 256;
/// Frames per decode-buffer refill (amortizes the template memcpy like a socket read).
const FRAMES_PER_FILL: u32 = 64;

const SHORT_TOPIC: &str = "bench/topic";
const LONG_TOPIC: &str = "plant-north/hall-7/line-03/station-11/plc-4711/telemetry/vibration/rms";

fn publish_message(topic: &'static str, properties: PublishProperties) -> PublishMessage {
    PublishMessage {
        topic: ByteString::from_static(topic),
        payload: Bytes::from(vec![0u8; PAYLOAD_LEN]),
        properties: properties.into(),
    }
}

fn qos1_delivery(packet_id: u16) -> PublishDelivery {
    PublishDelivery {
        dup: false,
        retain: false,
        qos: QoS::AtLeastOnce,
        packet_id: Some(NonZeroU16::new(packet_id).expect("non-zero")),
    }
}

fn report(label: &str, elapsed: std::time::Duration, iters: u32) {
    let ns_per = elapsed.as_nanos() / u128::from(iters);
    let msg_s = (f64::from(iters) / elapsed.as_secs_f64()).round() as u64;
    println!("{label}: {ns_per} ns/msg, {msg_s} msg/s");
}

fn bench_encode(label: &str, message: &PublishMessage) {
    let delivery = qos1_delivery(1);
    let mut buf = BytesMut::with_capacity(4096);

    for _ in 0..1_000 {
        buf.clear();
        encode_publish(message, delivery, 0, &mut buf).expect("encode");
    }

    let start = Instant::now();
    for _ in 0..ITERS {
        buf.clear();
        encode_publish(message, delivery, 0, &mut buf).expect("encode");
    }
    report(label, start.elapsed(), ITERS);
}

/// One buffer refill worth of identical frames, pre-encoded.
fn frame_template(mut frame: impl FnMut(&mut BytesMut)) -> Bytes {
    let mut template = BytesMut::new();
    for _ in 0..FRAMES_PER_FILL {
        frame(&mut template);
    }
    template.freeze()
}

fn bench_decode(label: &str, template: &Bytes) {
    let mut codec = V5Codec::default();
    let mut buf = BytesMut::with_capacity(template.len() * 2);
    let fills = ITERS / FRAMES_PER_FILL;

    // Validate framing once outside the timed loop so the assert does not skew results.
    buf.extend_from_slice(template);
    for _ in 0..FRAMES_PER_FILL {
        let frame = codec.decode_frame(&mut buf).expect("decode").expect("frame");
        black_box(&frame);
    }
    assert!(buf.is_empty(), "template must split into whole frames");

    let mut run = |fills: u32| {
        for _ in 0..fills {
            buf.extend_from_slice(template);
            for _ in 0..FRAMES_PER_FILL {
                let frame = codec.decode_frame(&mut buf).expect("decode").expect("frame");
                black_box(&frame);
            }
        }
    };

    run(1_000 / FRAMES_PER_FILL + 1); // warmup
    let start = Instant::now();
    run(fills);
    report(label, start.elapsed(), fills * FRAMES_PER_FILL);
}

fn main() {
    println!("=== mqtt-glide codec_throughput (no broker) ===");
    println!("iters={ITERS}, payload={PAYLOAD_LEN} B, QoS 1 unless stated");

    // QoS 1 is the production outbound path; QoS 0 encode cost is indistinguishable here.
    bench_encode(
        "encode PUBLISH",
        &publish_message(SHORT_TOPIC, PublishProperties::default()),
    );
    bench_encode(
        "encode PUBLISH (topic 71 B)",
        &publish_message(LONG_TOPIC, PublishProperties::default()),
    );
    bench_encode(
        "encode PUBLISH (props)",
        &publish_message(
            SHORT_TOPIC,
            PublishProperties::default()
                .content_type("application/octet-stream")
                .user_property("device-id", "bench-device-4711")
                .user_property("trace-id", "00000000-0000-0000-0000-000000000000"),
        ),
    );

    let publish_template = |message: PublishMessage| {
        let mut packet_id = 0u16;
        frame_template(move |buf| {
            packet_id += 1;
            encode_publish(&message, qos1_delivery(packet_id), 0, buf).expect("encode");
        })
    };

    bench_decode(
        "decode PUBLISH",
        &publish_template(publish_message(SHORT_TOPIC, PublishProperties::default())),
    );
    bench_decode(
        "decode PUBLISH (topic 71 B)",
        &publish_template(publish_message(LONG_TOPIC, PublishProperties::default())),
    );
    bench_decode(
        "decode PUBLISH (props)",
        &publish_template(publish_message(
            SHORT_TOPIC,
            PublishProperties::default()
                .content_type("application/octet-stream")
                .user_property("device-id", "bench-device-4711")
                .user_property("trace-id", "00000000-0000-0000-0000-000000000000"),
        )),
    );

    let puback_template = {
        let mut codec = V5Codec::default();
        let mut packet_id = 0u16;
        frame_template(move |buf| {
            packet_id += 1;
            let ack = Packet::PublishAck(PublishAck {
                packet_id: NonZeroU16::new(packet_id).expect("non-zero"),
                ..Default::default()
            });
            codec.encode_packet(ack, buf).expect("encode");
        })
    };
    bench_decode("decode PUBACK (minimal)", &puback_template);
}
