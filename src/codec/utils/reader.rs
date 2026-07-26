//! Cursor over a remaining MQTT packet body for decode helpers.

use bytes::{Buf, Bytes};

use crate::codec::error::DecodeError;

pub(crate) struct WireReader<'a> {
    src: &'a mut Bytes,
}

impl<'a> WireReader<'a> {
    #[inline]
    pub(crate) fn new(src: &'a mut Bytes) -> Self {
        Self { src }
    }

    #[inline]
    pub(crate) fn remaining(&self) -> usize {
        self.src.remaining()
    }

    #[inline]
    pub(crate) fn read_u8(&mut self) -> Result<u8, DecodeError> {
        ensure!(self.src.has_remaining(), DecodeError::InvalidLength);
        Ok(self.src.get_u8())
    }

    #[inline]
    pub(crate) fn read_u16(&mut self) -> Result<u16, DecodeError> {
        ensure!(self.src.remaining() >= 2, DecodeError::InvalidLength);
        Ok(self.src.get_u16())
    }

    #[inline]
    pub(crate) fn read_u32(&mut self) -> Result<u32, DecodeError> {
        ensure!(self.src.remaining() >= 4, DecodeError::InvalidLength);
        Ok(self.src.get_u32())
    }

    pub(crate) fn ensure_empty(&self) -> Result<(), DecodeError> {
        ensure!(
            !self.src.has_remaining(),
            DecodeError::MalformedPacket("invalid encoding")
        );
        Ok(())
    }

    pub(crate) fn into_inner(self) -> &'a mut Bytes {
        self.src
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::utils::Decode;

    #[test]
    fn read_primitives_advances_cursor() {
        let mut src = Bytes::from_static(&[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07]);
        let mut reader = WireReader::new(&mut src);
        assert_eq!(reader.read_u8().unwrap(), 0x01);
        assert_eq!(reader.read_u16().unwrap(), 0x0203);
        assert_eq!(reader.read_u32().unwrap(), 0x04050607);
        assert_eq!(reader.remaining(), 0);
    }

    #[test]
    fn read_underflow_returns_invalid_length() {
        let mut src = Bytes::from_static(&[0x01]);
        let mut reader = WireReader::new(&mut src);
        assert_eq!(reader.read_u8().unwrap(), 0x01);
        assert!(matches!(reader.read_u16(), Err(DecodeError::InvalidLength)));
        assert!(matches!(reader.read_u32(), Err(DecodeError::InvalidLength)));
    }

    #[test]
    fn ensure_empty_rejects_trailing_bytes() {
        let mut src = Bytes::from_static(&[0x00]);
        let reader = WireReader::new(&mut src);
        assert!(matches!(reader.ensure_empty(), Err(DecodeError::MalformedPacket(_))));
    }

    #[test]
    fn ensure_empty_accepts_empty_buffer() {
        let mut src = Bytes::new();
        let reader = WireReader::new(&mut src);
        assert!(reader.ensure_empty().is_ok());
    }

    #[test]
    fn into_inner_passes_through_for_decode_trait() {
        let mut src = Bytes::from_static(&[0x12, 0x34]);
        let reader = WireReader::new(&mut src);
        assert_eq!(u16::decode(reader.into_inner()).unwrap(), 0x1234);
    }
}
