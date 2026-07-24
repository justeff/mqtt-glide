use std::convert::TryFrom;

use crate::codec::error::DecodeError;

/// MQTT protocol name for CONNECT (`"MQTT"`).
pub(crate) const MQTT: &[u8] = b"MQTT";
/// Protocol level for MQTT 5.0
pub const MQTT_LEVEL_5: u8 = 5;
/// Bit shift position for Will QoS in Connect flags
pub(crate) const WILL_QOS_SHIFT: u8 = 3;

/// Maximum allowed packet size (268,435,455 bytes)
pub(crate) const MAX_PACKET_SIZE: u32 = 0xF_FF_FF_FF;

/// Quality of Service levels for message delivery.
///
/// Determines the guarantee of message delivery between client and broker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum QoS {
    /// At most once delivery (Fire and Forget)
    ///
    /// # Example
    /// Used for non-critical sensor data where occasional loss is acceptable
    AtMostOnce = 0,

    /// At least once delivery (Acknowledged Delivery)
    ///
    /// # Example
    /// Used for important notifications that must be received but can tolerate duplicates
    AtLeastOnce = 1,

    /// Exactly once delivery (Assured Delivery)
    ///
    /// # Example
    /// Used for critical commands where duplication could cause harmful effects
    ExactlyOnce = 2,
}

impl TryFrom<u8> for QoS {
    type Error = DecodeError;

    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(Self::AtMostOnce),
            1 => Ok(Self::AtLeastOnce),
            2 => Ok(Self::ExactlyOnce),
            _ => Err(DecodeError::MalformedPacket),
        }
    }
}

impl From<QoS> for u8 {
    fn from(v: QoS) -> Self {
        v as u8
    }
}

impl QoS {
    /// Gets the numeric value of the QoS level
    #[inline]
    pub fn value(self) -> u8 {
        u8::from(self)
    }

    /// Returns the lower of two QoS levels
    ///
    /// # Example
    /// ```
    /// use mqtt_glide::codec::QoS;
    ///
    /// let lower = QoS::ExactlyOnce.less_value(QoS::AtLeastOnce);
    /// assert_eq!(lower, QoS::AtLeastOnce);
    /// ```
    #[inline]
    pub fn less_value(self, qos: QoS) -> QoS {
        if self.value() < qos.value() { self } else { qos }
    }
}

bitflags::bitflags! {
    /// Connection flags for MQTT CONNECT packet
    #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct ConnectFlags: u8 {
        /// Username flag (bit 7)
        const USERNAME    = 0b1000_0000;
        /// Password flag (bit 6)
        const PASSWORD    = 0b0100_0000;
        /// Will retain flag (bit 5)
        const WILL_RETAIN = 0b0010_0000;
        /// Will QoS mask (bits 4-3)
        const WILL_QOS    = 0b0001_1000;
        /// Will flag (bit 2)
        const WILL        = 0b0000_0100;
        /// Clean session flag (bit 1)
        const CLEAN_START = 0b0000_0010;
    }
}

bitflags::bitflags! {
    /// Connection acknowledgment flags for MQTT CONNACK packet
    #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct ConnectAckFlags: u8 {
        /// Session present flag (bit 0)
        const SESSION_PRESENT = 0b0000_0001;
    }
}

/// Packet type identifiers and masks
pub(super) mod packet_type {
    /// CONNECT packet type (0x10)
    pub(crate) const CONNECT: u8 = 0b0001_0000;
    /// CONNACK packet type (0x20)
    pub(crate) const CONNACK: u8 = 0b0010_0000;
    /// PUBLISH packet type range start (0x30)
    pub(crate) const PUBLISH_START: u8 = 0b0011_0000;
    /// PUBLISH packet type range end (0x3F)
    pub(crate) const PUBLISH_END: u8 = 0b0011_1111;
    /// PUBACK packet type (0x40)
    pub(crate) const PUBACK: u8 = 0b0100_0000;
    /// PUBREC packet type (0x50)
    pub(crate) const PUBREC: u8 = 0b0101_0000;
    /// PUBREL packet type (0x62)
    pub(crate) const PUBREL: u8 = 0b0110_0010;
    /// PUBCOMP packet type (0x70)
    pub(crate) const PUBCOMP: u8 = 0b0111_0000;
    /// SUBSCRIBE packet type (0x82)
    pub(crate) const SUBSCRIBE: u8 = 0b1000_0010;
    /// SUBACK packet type (0x90)
    pub(crate) const SUBACK: u8 = 0b1001_0000;
    /// UNSUBSCRIBE packet type (0xA2)
    pub(crate) const UNSUBSCRIBE: u8 = 0b1010_0010;
    /// UNSUBACK packet type (0xB0)
    pub(crate) const UNSUBACK: u8 = 0b1011_0000;
    /// PINGREQ packet type (0xC0)
    pub(crate) const PINGREQ: u8 = 0b1100_0000;
    /// PINGRESP packet type (0xD0)
    pub(crate) const PINGRESP: u8 = 0b1101_0000;
    /// DISCONNECT packet type (0xE0)
    pub(crate) const DISCONNECT: u8 = 0b1110_0000;
    /// AUTH packet type (0xF0) - MQTT 5.0 only
    pub(crate) const AUTH: u8 = 0b1111_0000;
}
