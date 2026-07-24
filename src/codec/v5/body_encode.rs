use bytes::BytesMut;

use crate::codec::error::EncodeError;

/// Trait for encoding MQTT packet bodies with optional packet size limit enforcement.
///
/// Body-only: fixed header and Remaining Length varint are written by `Packet` orchestration.
/// `max_out` is the remaining body budget. Property trimming decisions (v5) must match between
/// `encoded_body_len` and `encode_body`.
pub(crate) trait WireBodyEncode {
    /// Exact body length for Remaining Length (after any property trimming decisions).
    fn encoded_body_len(&self, max_out: u32) -> Result<usize, EncodeError>;

    /// Write packet body bytes only (no fixed header).
    fn encode_body(&self, buf: &mut BytesMut, max_out: u32) -> Result<(), EncodeError>;
}
