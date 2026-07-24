//! MQTT v5 Protocol codec

use std::num::NonZeroU16;

use bytestring::ByteString;
use nonzero_ext::nonzero;

mod body_encode;
mod codec;
mod decode;
mod encode;
mod packet;

pub use self::codec::V5Codec;
pub use self::encode::encode_publish;
pub(crate) use self::encode::publish_wire_len;
pub(crate) use self::packet::property_type;
pub use self::packet::*;

/// A key-value pair representing a single user property
pub type UserProperty = (ByteString, ByteString);
/// A list of user properties for MQTT v5 packets
pub type UserProperties = Vec<UserProperty>;

/// Default maximum receive value (65535) when CONNECT/CONNACK omit Receive Maximum (MQTT v5).
pub const RECEIVE_MAX_DEFAULT: NonZeroU16 = nonzero!(65535u16);
