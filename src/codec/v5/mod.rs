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

/// One MQTT User Property (§2.2.2.2 / UTF-8 String Pair §1.5.7).
///
/// Construct with [`From`]: `("name", "value").into()` for any `Into<ByteString>` pair.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UserProperty {
    /// Name (first field of the UTF-8 String Pair).
    pub name: ByteString,
    /// Value (second field of the UTF-8 String Pair).
    pub value: ByteString,
}

impl<K, V> From<(K, V)> for UserProperty
where
    K: Into<ByteString>,
    V: Into<ByteString>,
{
    #[inline]
    fn from((name, value): (K, V)) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
        }
    }
}

impl From<UserProperty> for (ByteString, ByteString) {
    #[inline]
    fn from(prop: UserProperty) -> Self {
        (prop.name, prop.value)
    }
}

/// List of User Properties on an MQTT v5 packet.
pub type UserProperties = Vec<UserProperty>;

/// Default maximum receive value (65535) when CONNECT/CONNACK omit Receive Maximum (MQTT v5).
pub const RECEIVE_MAX_DEFAULT: NonZeroU16 = nonzero!(65535u16);
