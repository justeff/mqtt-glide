//! Writes the seed corpus for the `decode_frames` fuzz target to `seeds/decode_frames/`.
//!
//! Regenerate from the `fuzz/` directory (stable toolchain is enough):
//!
//! ```shell
//! cargo run --example gen_seeds
//! ```
//!
//! The wire bytes are spelled out here, byte by byte, on purpose: the seeds should not depend
//! on the encoder they help to test. Each seed is still run through the real decoder before it
//! is written, so a codec change that invalidates a seed fails this program instead of silently
//! shrinking the corpus.
//!
//! A seed is the target's input: one byte inbound size limit (x64, 0 = unlimited), one byte
//! read chunk size seed, then the wire bytes.

use std::fs;
use std::path::PathBuf;

use bytes::BytesMut;
use mqtt_glide::codec::{DecodedFrame, Packet, V5Codec};

/// Length-prefixed UTF-8 string or binary data.
fn lp(data: &[u8]) -> Vec<u8> {
    let mut out = u16::try_from(data.len()).unwrap().to_be_bytes().to_vec();
    out.extend_from_slice(data);
    out
}

/// Variable byte integer.
fn vbi(mut n: usize) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let mut byte = (n % 128) as u8;
        n /= 128;
        if n > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if n == 0 {
            return out;
        }
    }
}

/// Fixed header plus body.
fn frame(first: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![first];
    out.extend(vbi(body.len()));
    out.extend_from_slice(body);
    out
}

/// A property block: variable length prefix, then the already encoded properties.
fn props(items: &[&[u8]]) -> Vec<u8> {
    let body: Vec<u8> = items.concat();
    let mut out = vbi(body.len());
    out.extend(body);
    out
}

fn prop_u8(id: u8, v: u8) -> Vec<u8> {
    vec![id, v]
}
fn prop_u16(id: u8, v: u16) -> Vec<u8> {
    [&[id][..], &v.to_be_bytes()].concat()
}
fn prop_u32(id: u8, v: u32) -> Vec<u8> {
    [&[id][..], &v.to_be_bytes()].concat()
}
fn prop_lp(id: u8, v: &[u8]) -> Vec<u8> {
    [&[id][..], &lp(v)].concat()
}
fn prop_user(k: &str, v: &str) -> Vec<u8> {
    [&[0x26][..], &lp(k.as_bytes()), &lp(v.as_bytes())].concat()
}
fn prop_vbi(id: u8, v: usize) -> Vec<u8> {
    [&[id][..], &vbi(v)].concat()
}

fn connect(flags: u8, connect_props: Vec<u8>, payload: Vec<u8>) -> Vec<u8> {
    let body = [&lp(b"MQTT")[..], &[5, flags, 0x00, 0x3C], &connect_props, &payload].concat();
    frame(0x10, &body)
}

fn publish(first: u8, topic: &str, packet_id: Option<u16>, properties: Vec<u8>, payload: &[u8]) -> Vec<u8> {
    let mut body = lp(topic.as_bytes());
    if let Some(id) = packet_id {
        body.extend(id.to_be_bytes());
    }
    body.extend(properties);
    body.extend_from_slice(payload);
    frame(first, &body)
}

/// PUBACK / PUBREC / PUBREL / PUBCOMP in the full form: id, reason, properties.
fn ack(first: u8, id: u16, reason: u8, properties: Vec<u8>) -> Vec<u8> {
    frame(first, &[&id.to_be_bytes()[..], &[reason], &properties].concat())
}

fn subscribe(id: u16, properties: Vec<u8>, filters: &[(&str, u8)]) -> Vec<u8> {
    let mut body = [&id.to_be_bytes()[..], &properties].concat();
    for (filter, options) in filters {
        body.extend(lp(filter.as_bytes()));
        body.push(*options);
    }
    frame(0x82, &body)
}

fn unsubscribe(id: u16, properties: Vec<u8>, filters: &[&str]) -> Vec<u8> {
    let mut body = [&id.to_be_bytes()[..], &properties].concat();
    for filter in filters {
        body.extend(lp(filter.as_bytes()));
    }
    frame(0xA2, &body)
}

fn acks(first: u8, id: u16, properties: Vec<u8>, reasons: &[u8]) -> Vec<u8> {
    frame(first, &[&id.to_be_bytes()[..], &properties, reasons].concat())
}

fn seeds() -> Vec<(&'static str, Vec<Vec<u8>>)> {
    let connect_min = connect(0x02, props(&[]), lp(b"c1"));
    let connect_full = connect(
        0xCE, // username, password, will QoS 1, will flag, clean start
        props(&[
            &prop_u32(0x11, 3600),
            &prop_u16(0x21, 10),
            &prop_u32(0x27, 4096),
            &prop_u16(0x22, 8),
            &prop_u8(0x19, 1),
            &prop_u8(0x17, 1),
            &prop_user("k", "v"),
        ]),
        [
            lp(b"client-full"),
            props(&[
                &prop_u32(0x18, 30),
                &prop_u8(0x01, 1),
                &prop_u32(0x02, 60),
                &prop_lp(0x03, b"text/plain"),
                &prop_lp(0x08, b"reply/to"),
                &prop_lp(0x09, b"corr"),
                &prop_user("wk", "wv"),
            ]),
            lp(b"status/c1"),
            lp(b"offline"),
            lp(b"user"),
            lp(b"secret"),
        ]
        .concat(),
    );
    let connect_auth = connect(
        0x02,
        props(&[
            &prop_lp(0x15, b"SCRAM-SHA-256"),
            &prop_lp(0x16, &[0xDE, 0xAD, 0xBE, 0xEF]),
        ]),
        lp(b""),
    );
    let connack_ok = frame(0x20, &[0x00, 0x00, 0x00]);
    let connack_full = frame(
        0x20,
        &[
            &[0x01, 0x00][..],
            &props(&[
                &prop_u32(0x11, 120),
                &prop_u16(0x21, 20),
                &prop_u8(0x24, 1),
                &prop_u8(0x25, 1),
                &prop_u32(0x27, 65536),
                &prop_lp(0x12, b"assigned-id"),
                &prop_u16(0x22, 10),
                &prop_lp(0x1F, b"welcome"),
                &prop_user("srv", "1"),
                &prop_u8(0x28, 1),
                &prop_u8(0x29, 1),
                &prop_u8(0x2A, 1),
                &prop_u16(0x13, 45),
                &prop_lp(0x1A, b"info"),
                &prop_lp(0x1C, b"other:1883"),
                &prop_lp(0x15, b"SCRAM-SHA-256"),
                &prop_lp(0x16, b"data"),
            ]),
        ]
        .concat(),
    );
    let connack_refused = frame(
        0x20,
        &[&[0x00, 0x87][..], &props(&[&prop_lp(0x1F, b"not authorized")])].concat(),
    );

    let publish_q0 = publish(0x30, "a/b", None, props(&[]), b"hi");
    let publish_q0_empty = publish(0x30, "t", None, props(&[]), b"");
    let publish_q1 = publish(0x32, "sensors/temp", Some(1), props(&[]), b"21.5");
    let publish_q1_flags = publish(0x3B, "retained/dup", Some(0xFFFF), props(&[]), b"x"); // dup, QoS 1, retain
    let publish_q2 = publish(0x34, "q2/topic", Some(7), props(&[]), b"two");
    let publish_props = publish(
        0x32,
        "props/topic",
        Some(300),
        props(&[
            &prop_u8(0x01, 1),
            &prop_u32(0x02, 120),
            &prop_lp(0x03, b"application/json"),
            &prop_lp(0x08, b"resp/topic"),
            &prop_lp(0x09, b"\x00\x01\x02"),
            &prop_user("a", "b"),
            &prop_user("a", "c"),
            &prop_vbi(0x0B, 5),
            &prop_vbi(0x0B, 200),
        ]),
        b"{\"v\":1}",
    );
    let publish_alias_set = publish(0x30, "alias/topic", None, props(&[&prop_u16(0x23, 1)]), b"first");
    let publish_alias_use = publish(0x30, "", None, props(&[&prop_u16(0x23, 1)]), b"again");
    let publish_alias_q1 = publish(
        0x32,
        "",
        Some(9),
        props(&[&prop_u16(0x23, 2), &prop_user("x", "y")]),
        b"p",
    );

    let puback_short = frame(0x40, &[0x00, 0x01]);
    let puback_reason = frame(0x40, &[0x00, 0x02, 0x10]); // no matching subscribers
    let puback_long = ack(
        0x40,
        3,
        0x97,
        props(&[&prop_lp(0x1F, b"quota exceeded"), &prop_user("q", "1")]),
    );
    let pubrec_short = frame(0x50, &[0x00, 0x04]);
    let pubrec_long = ack(0x50, 5, 0x80, props(&[&prop_lp(0x1F, b"unspecified")]));
    let pubrel_short = frame(0x62, &[0x00, 0x06]);
    let pubrel_long = ack(0x62, 6, 0x92, props(&[&prop_lp(0x1F, b"packet id not found")])); // packet id not found
    let pubcomp_short = frame(0x70, &[0x00, 0x06]);
    let pubcomp_long = ack(0x70, 6, 0x92, props(&[&prop_user("done", "no")]));

    let subscribe_one = subscribe(1, props(&[]), &[("a/b", 0x01)]);
    let subscribe_many = subscribe(
        2,
        props(&[&prop_vbi(0x0B, 42), &prop_user("s", "t")]),
        &[
            ("sensors/+/temp", 0x02),
            ("$share/group/jobs/#", 0x01),
            ("opts/all", 0x1E), // QoS 2, no local, retain as published, retain handling 1
            ("opts/rh2", 0x20),
        ],
    );
    let suback = acks(0x90, 1, props(&[]), &[0x00, 0x01, 0x02, 0x80]);
    let suback_props = acks(
        0x90,
        2,
        props(&[&prop_lp(0x1F, b"mixed"), &prop_user("s", "a")]),
        &[0x00, 0x8F, 0x9E, 0xA1, 0xA2],
    );
    let unsubscribe_one = unsubscribe(3, props(&[]), &["a/b"]);
    let unsubscribe_many = unsubscribe(
        4,
        props(&[&prop_user("u", "v")]),
        &["sensors/+/temp", "$share/group/jobs/#"],
    );
    let unsuback = acks(0xB0, 3, props(&[]), &[0x00, 0x11]);
    let unsuback_props = acks(0xB0, 4, props(&[&prop_lp(0x1F, b"ok")]), &[0x00, 0x8F]);

    let pingreq = frame(0xC0, &[]);
    let pingresp = frame(0xD0, &[]);

    let disconnect_empty = frame(0xE0, &[]);
    let disconnect_reason = frame(0xE0, &[0x04]); // disconnect with will message
    let disconnect_props = frame(
        0xE0,
        &[
            &[0x9C][..], // use another server
            &props(&[
                &prop_lp(0x1C, b"other:1883"),
                &prop_lp(0x1F, b"moved"),
                &prop_user("d", "1"),
            ]),
        ]
        .concat(),
    );
    let disconnect_session_expiry = frame(0xE0, &[&[0x00][..], &props(&[&prop_u32(0x11, 0)])].concat());

    let auth_empty = frame(0xF0, &[]);
    let auth_continue = frame(
        0xF0,
        &[
            &[0x18][..], // continue authentication
            &props(&[
                &prop_lp(0x15, b"SCRAM-SHA-256"),
                &prop_lp(0x16, b"challenge"),
                &prop_lp(0x1F, b"step 2"),
            ]),
        ]
        .concat(),
    );
    let auth_reauth = frame(
        0xF0,
        &[
            &[0x19][..],
            &props(&[&prop_lp(0x15, b"SCRAM-SHA-256"), &prop_user("r", "1")]),
        ]
        .concat(),
    );

    let single = |f: &Vec<u8>| vec![f.clone()];
    let mut truncated = [publish_q1.clone(), pingresp.clone()].concat();
    truncated.truncate(truncated.len() - 1);

    vec![
        ("connect_min", single(&connect_min)),
        ("connect_full", single(&connect_full)),
        ("connect_auth", single(&connect_auth)),
        ("connack_ok", single(&connack_ok)),
        ("connack_full", single(&connack_full)),
        ("connack_refused", single(&connack_refused)),
        ("publish_q0", single(&publish_q0)),
        ("publish_q0_empty", single(&publish_q0_empty)),
        ("publish_q1", single(&publish_q1)),
        ("publish_q1_dup_retain", single(&publish_q1_flags)),
        ("publish_q2", single(&publish_q2)),
        ("publish_props", single(&publish_props)),
        ("publish_alias_set", single(&publish_alias_set)),
        ("publish_alias_use", single(&publish_alias_use)),
        ("publish_alias_q1", single(&publish_alias_q1)),
        ("puback_short", single(&puback_short)),
        ("puback_reason", single(&puback_reason)),
        ("puback_long", single(&puback_long)),
        ("pubrec_short", single(&pubrec_short)),
        ("pubrec_long", single(&pubrec_long)),
        ("pubrel_short", single(&pubrel_short)),
        ("pubrel_long", single(&pubrel_long)),
        ("pubcomp_short", single(&pubcomp_short)),
        ("pubcomp_long", single(&pubcomp_long)),
        ("subscribe_one", single(&subscribe_one)),
        ("subscribe_many", single(&subscribe_many)),
        ("suback", single(&suback)),
        ("suback_props", single(&suback_props)),
        ("unsubscribe_one", single(&unsubscribe_one)),
        ("unsubscribe_many", single(&unsubscribe_many)),
        ("unsuback", single(&unsuback)),
        ("unsuback_props", single(&unsuback_props)),
        ("pingreq", single(&pingreq)),
        ("pingresp", single(&pingresp)),
        ("disconnect_empty", single(&disconnect_empty)),
        ("disconnect_reason", single(&disconnect_reason)),
        ("disconnect_props", single(&disconnect_props)),
        ("disconnect_session_expiry", single(&disconnect_session_expiry)),
        ("auth_empty", single(&auth_empty)),
        ("auth_continue", single(&auth_continue)),
        ("auth_reauth", single(&auth_reauth)),
        (
            "multi_handshake",
            vec![
                connect_full.clone(),
                connack_full.clone(),
                subscribe_many.clone(),
                suback_props.clone(),
            ],
        ),
        (
            "multi_publish_flow",
            vec![
                publish_q1.clone(),
                puback_short.clone(),
                publish_q2.clone(),
                pubrec_short.clone(),
                pubrel_short.clone(),
                pubcomp_short.clone(),
            ],
        ),
        (
            "multi_alias",
            vec![
                publish_alias_set.clone(),
                publish_alias_use.clone(),
                publish_alias_use.clone(),
                pingresp.clone(),
            ],
        ),
        (
            "multi_pings",
            vec![
                pingreq.clone(),
                pingresp.clone(),
                pingreq.clone(),
                pingresp.clone(),
                pingreq.clone(),
            ],
        ),
        (
            "multi_shutdown",
            vec![
                unsubscribe_one.clone(),
                unsuback.clone(),
                auth_continue.clone(),
                disconnect_props.clone(),
            ],
        ),
        ("multi_truncated_tail", vec![truncated]),
    ]
}

/// Chunk size seed for the read-splitting half of the target, varied so seeds start from
/// different read boundaries.
fn chunk_seed(index: usize) -> u8 {
    [0xFF, 0x00, 0x02, 0x06, 0x0C][index % 5]
}

/// Decodes `wire` completely and returns the frames; panics if any of the seed is invalid.
fn check(name: &str, wire: &[u8], expect_tail: bool) -> Vec<DecodedFrame> {
    let mut codec = V5Codec::new(0, 0);
    let mut buf = BytesMut::from(wire);
    let mut frames = Vec::new();
    loop {
        match codec.decode_frame(&mut buf) {
            Ok(Some(frame)) => frames.push(frame),
            Ok(None) => break,
            Err(error) => panic!("seed {name} does not decode: {error}"),
        }
    }
    assert_eq!(!buf.is_empty(), expect_tail, "seed {name}: unexpected trailing bytes");
    assert!(!frames.is_empty(), "seed {name}: no frame decoded");
    frames
}

fn main() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("seeds/decode_frames");
    fs::create_dir_all(&dir).unwrap();
    // Stale files from a renamed seed would silently stay in the corpus.
    for entry in fs::read_dir(&dir).unwrap() {
        fs::remove_file(entry.unwrap().path()).unwrap();
    }

    let mut kinds = std::collections::BTreeSet::new();
    let mut total = 0;
    let seeds = seeds();
    for (index, (name, frames)) in seeds.iter().enumerate() {
        let wire = frames.concat();
        for decoded in check(name, &wire, name.ends_with("truncated_tail")) {
            kinds.insert(match decoded {
                DecodedFrame::Publish(_) => "PUBLISH",
                DecodedFrame::Control(Packet::Connect(_)) => "CONNECT",
                DecodedFrame::Control(Packet::ConnectAck(_)) => "CONNACK",
                DecodedFrame::Control(Packet::PublishAck(_)) => "PUBACK",
                DecodedFrame::Control(Packet::PublishReceived(_)) => "PUBREC",
                DecodedFrame::Control(Packet::PublishRelease(_)) => "PUBREL",
                DecodedFrame::Control(Packet::PublishComplete(_)) => "PUBCOMP",
                DecodedFrame::Control(Packet::Subscribe(_)) => "SUBSCRIBE",
                DecodedFrame::Control(Packet::SubscribeAck(_)) => "SUBACK",
                DecodedFrame::Control(Packet::Unsubscribe(_)) => "UNSUBSCRIBE",
                DecodedFrame::Control(Packet::UnsubscribeAck(_)) => "UNSUBACK",
                DecodedFrame::Control(Packet::PingRequest) => "PINGREQ",
                DecodedFrame::Control(Packet::PingResponse) => "PINGRESP",
                DecodedFrame::Control(Packet::Disconnect(_)) => "DISCONNECT",
                DecodedFrame::Control(Packet::Auth(_)) => "AUTH",
            });
        }
        let input = [&[0u8, chunk_seed(index)][..], &wire].concat();
        total += input.len();
        fs::write(dir.join(name), input).unwrap();
    }
    assert_eq!(kinds.len(), 15, "every packet type is seeded: {kinds:?}");
    println!("wrote {} seeds ({total} bytes) to {}", seeds.len(), dir.display());
}
