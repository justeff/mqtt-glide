//! Arbitrary bytes from a peer, split into arbitrary reads.
//!
//! Checks, for whatever the input is:
//! - decoding never panics (malformed input is a `DecodeError`),
//! - the frames decoded do not depend on how the bytes were split across reads,
//! - every frame that decodes re-encodes, and the re-encoded bytes decode to the same frame.
#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use mqtt_glide::codec::{DecodedFrame, PublishDelivery, V5Codec, encode_publish};

/// Bound on frames per input, so a run of tiny frames cannot make one input slow.
const MAX_FRAMES: usize = 256;

fuzz_target!(|data: &[u8]| {
    // First byte: inbound size limit (0 = unlimited). Second: read chunk size seed.
    let [limit, chunk, wire @ ..] = data else {
        return;
    };
    let max_in = u32::from(*limit) * 64;
    let whole = decode_all(max_in, wire, wire.len().max(1));
    let split = decode_all(max_in, wire, usize::from(*chunk % 16) + 1);
    assert_eq!(whole, split, "framing must not depend on read boundaries");

    for frame in whole.frames {
        let mut buf = BytesMut::new();
        let encoded = match &frame {
            DecodedFrame::Control(packet) => V5Codec::default().encode_packet(packet.clone(), &mut buf),
            // An empty topic only makes sense with a connection-local alias; skip those.
            DecodedFrame::Publish(publish) if publish.wire_topic_alias.is_some() => continue,
            DecodedFrame::Publish(publish) => {
                let delivery = PublishDelivery {
                    dup: publish.meta.dup,
                    retain: publish.meta.retain,
                    qos: publish.meta.qos,
                    packet_id: publish.meta.packet_id,
                };
                encode_publish(&publish.message, delivery, 0, &mut buf)
            }
        };
        // Some decodable packets are not ours to send (e.g. a client never encodes QoS 2
        // or Subscription Identifiers on PUBLISH); refusing them is fine, panicking is not.
        if encoded.is_err() {
            continue;
        }
        let again = V5Codec::default()
            .decode_frame(&mut buf)
            .expect("re-encoded frame decodes")
            .expect("re-encoded frame is complete");
        assert_eq!(again, without_server_only_fields(frame), "decode(encode(frame)) == frame");
        assert!(buf.is_empty(), "re-encoded frame has no trailing bytes");
    }
});

#[derive(Debug, PartialEq)]
struct Decoded {
    frames: Vec<DecodedFrame>,
    error: Option<String>,
}

fn decode_all(max_in: u32, wire: &[u8], chunk: usize) -> Decoded {
    let mut codec = V5Codec::new(max_in, 0);
    let mut buf = BytesMut::new();
    let mut frames = Vec::new();
    for piece in wire.chunks(chunk) {
        buf.extend_from_slice(piece);
        loop {
            if frames.len() >= MAX_FRAMES {
                return Decoded { frames, error: None };
            }
            match codec.decode_frame(&mut buf) {
                Ok(Some(frame)) => frames.push(frame),
                Ok(None) => break,
                Err(error) => {
                    return Decoded {
                        frames,
                        error: Some(error.to_string()),
                    };
                }
            }
        }
    }
    Decoded { frames, error: None }
}

/// Inbound-only data the encoder legitimately drops: Subscription Identifiers on PUBLISH
/// are set by the server and never encoded by a client.
fn without_server_only_fields(frame: DecodedFrame) -> DecodedFrame {
    match frame {
        DecodedFrame::Publish(mut publish) => {
            publish.subscription_ids.clear();
            DecodedFrame::Publish(publish)
        }
        other => other,
    }
}
