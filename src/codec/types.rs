use std::convert::TryFrom;

use crate::codec::error::DecodeError;

/// Protocol Name bytes in CONNECT (`"MQTT"`, §3.1.2.1).
pub(crate) const MQTT: &[u8] = b"MQTT";
/// Protocol Version for MQTT 5.0 (§3.1.2.2).
pub const MQTT_LEVEL_5: u8 = 5;
/// Bit shift for Will QoS in the Connect Flags byte (§3.1.2.6).
pub(crate) const WILL_QOS_SHIFT: u8 = 3;

/// Maximum Remaining Length value (268,435,455; §1.5.5 / §2.1.4).
pub(crate) const MAX_PACKET_SIZE: u32 = 0xF_FF_FF_FF;

/// Quality of Service (§4.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum QoS {
    /// At most once (§4.3.1).
    AtMostOnce = 0,

    /// At least once (§4.3.2).
    AtLeastOnce = 1,

    /// Exactly once (§4.3.3). This crate rejects QoS 2 at the public API.
    ExactlyOnce = 2,
}

impl TryFrom<u8> for QoS {
    type Error = DecodeError;

    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(Self::AtMostOnce),
            1 => Ok(Self::AtLeastOnce),
            2 => Ok(Self::ExactlyOnce),
            _ => Err(DecodeError::MalformedPacket("unknown property or reason code")),
        }
    }
}

impl From<QoS> for u8 {
    fn from(v: QoS) -> Self {
        v as u8
    }
}

impl QoS {
    /// Numeric QoS value (`0`–`2`).
    #[inline]
    pub fn value(self) -> u8 {
        u8::from(self)
    }

    /// Returns the lower of two QoS levels.
    ///
    /// # Example
    /// ```
    /// use mqtt_glide::codec::QoS;
    ///
    /// let lower = QoS::ExactlyOnce.min(QoS::AtLeastOnce);
    /// assert_eq!(lower, QoS::AtLeastOnce);
    /// ```
    #[inline]
    pub fn min(self, qos: QoS) -> QoS {
        if self.value() < qos.value() { self } else { qos }
    }
}

bitflags::bitflags! {
    /// Connect Flags byte (§3.1.2.3).
    #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct ConnectFlags: u8 {
        /// User Name Flag (bit 7).
        const USERNAME    = 0b1000_0000;
        /// Password Flag (bit 6).
        const PASSWORD    = 0b0100_0000;
        /// Will Retain (bit 5).
        const WILL_RETAIN = 0b0010_0000;
        /// Will QoS mask (bits 4–3).
        const WILL_QOS    = 0b0001_1000;
        /// Will Flag (bit 2).
        const WILL        = 0b0000_0100;
        /// Clean Start (bit 1).
        const CLEAN_START = 0b0000_0010;
    }
}

bitflags::bitflags! {
    /// CONNACK Acknowledge Flags (§3.2.2.1).
    #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct ConnectAckFlags: u8 {
        /// Session Present (bit 0).
        const SESSION_PRESENT = 0b0000_0001;
    }
}

/// Fixed-header MQTT Control Packet type values (§2.1.2).
pub(super) mod packet_type {
    /// CONNECT (`0x10`).
    pub(crate) const CONNECT: u8 = 0b0001_0000;
    /// CONNACK (`0x20`).
    pub(crate) const CONNACK: u8 = 0b0010_0000;
    /// PUBLISH type nibble start (`0x30`).
    pub(crate) const PUBLISH_START: u8 = 0b0011_0000;
    /// PUBLISH type nibble end (`0x3F`).
    pub(crate) const PUBLISH_END: u8 = 0b0011_1111;
    /// PUBACK (`0x40`).
    pub(crate) const PUBACK: u8 = 0b0100_0000;
    /// PUBREC (`0x50`).
    pub(crate) const PUBREC: u8 = 0b0101_0000;
    /// PUBREL (`0x62`).
    pub(crate) const PUBREL: u8 = 0b0110_0010;
    /// PUBCOMP (`0x70`).
    pub(crate) const PUBCOMP: u8 = 0b0111_0000;
    /// SUBSCRIBE (`0x82`).
    pub(crate) const SUBSCRIBE: u8 = 0b1000_0010;
    /// SUBACK (`0x90`).
    pub(crate) const SUBACK: u8 = 0b1001_0000;
    /// UNSUBSCRIBE (`0xA2`).
    pub(crate) const UNSUBSCRIBE: u8 = 0b1010_0010;
    /// UNSUBACK (`0xB0`).
    pub(crate) const UNSUBACK: u8 = 0b1011_0000;
    /// PINGREQ (`0xC0`).
    pub(crate) const PINGREQ: u8 = 0b1100_0000;
    /// PINGRESP (`0xD0`).
    pub(crate) const PINGRESP: u8 = 0b1101_0000;
    /// DISCONNECT (`0xE0`).
    pub(crate) const DISCONNECT: u8 = 0b1110_0000;
    /// AUTH (`0xF0`, MQTT 5.0).
    pub(crate) const AUTH: u8 = 0b1111_0000;
}

#[cfg(test)]
mod tests {
    use super::QoS;

    #[test]
    fn qos_value_and_min() {
        assert_eq!(QoS::AtMostOnce.value(), 0);
        assert_eq!(QoS::AtLeastOnce.value(), 1);
        assert_eq!(QoS::ExactlyOnce.value(), 2);
        for (a, b, lower) in [
            (QoS::AtMostOnce, QoS::AtLeastOnce, QoS::AtMostOnce),
            (QoS::AtLeastOnce, QoS::AtMostOnce, QoS::AtMostOnce),
            (QoS::ExactlyOnce, QoS::AtLeastOnce, QoS::AtLeastOnce),
            (QoS::AtLeastOnce, QoS::AtLeastOnce, QoS::AtLeastOnce),
        ] {
            assert_eq!(a.min(b), lower, "{a:?}.min({b:?})");
        }
    }
}
