//! MQTT SUBSCRIBE (§3.8), SUBACK (§3.9), UNSUBSCRIBE (§3.10), and UNSUBACK (§3.11).
//!
//! Subscription control packets per OASIS MQTT Version 5.0.
//!
//! # Errors
//! Decode rejects empty topic filter lists, malformed Subscription Options, unknown
//! Reason Codes, and trailing bytes after the payload (§2.1.4).

use std::convert::TryFrom;
use std::num::{NonZeroU16, NonZeroU32};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use bytestring::ByteString;

use crate::codec::error::{DecodeError, EncodeError, UnknownCode};
use crate::codec::properties::{ack_props, reduce_limit, var_int_len};
use crate::codec::types::QoS;
use crate::codec::utils::{self, Decode, Encode, decode_non_empty_topic};
use crate::codec::v5::body_encode::WireBodyEncode;
use crate::codec::v5::{UserProperties, UserProperty, property_type as pt};

/// SUBSCRIBE packet body (§3.8).
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Subscribe {
    /// Packet Identifier (§2.2.1, §3.8.2).
    pub packet_id: NonZeroU16,
    /// Subscription Identifier property (§3.8.2.1.2).
    pub id: Option<NonZeroU32>,
    pub user_properties: UserProperties,
    /// Topic Filters and Subscription Options (§3.8.3).
    pub topic_filters: Vec<(ByteString, SubscriptionOptions)>,
}

/// Per-filter Subscription Options byte (§3.8.3.1).
///
/// Encodes QoS, No Local, Retain As Published, and Retain Handling.
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub struct SubscriptionOptions {
    pub qos: QoS,
    pub no_local: bool,
    pub retain_as_published: bool,
    pub retain_handling: RetainHandling,
}

impl Default for SubscriptionOptions {
    fn default() -> Self {
        Self {
            qos: QoS::AtMostOnce,
            no_local: false,
            retain_as_published: false,
            retain_handling: RetainHandling::AtSubscribe,
        }
    }
}

/// Retain Handling values in the Subscription Options byte (§3.8.3.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RetainHandling {
    AtSubscribe = 0,
    AtSubscribeNew = 1,
    NoAtSubscribe = 2,
}

impl TryFrom<u8> for RetainHandling {
    type Error = UnknownCode;

    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(Self::AtSubscribe),
            1 => Ok(Self::AtSubscribeNew),
            2 => Ok(Self::NoAtSubscribe),
            _ => Err(UnknownCode(v)),
        }
    }
}

impl From<RetainHandling> for u8 {
    fn from(v: RetainHandling) -> Self {
        v as u8
    }
}

/// SUBACK packet body (§3.9).
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SubscribeAck {
    pub packet_id: NonZeroU16,
    pub properties: UserProperties,
    pub reason_string: Option<ByteString>,
    /// One Reason Code per Topic Filter in the matching SUBSCRIBE (§3.9.3).
    pub status: Vec<SubscribeAckReason>,
}

/// UNSUBSCRIBE packet body (§3.10).
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Unsubscribe {
    /// Packet Identifier (§2.2.1, §3.10.2).
    pub packet_id: NonZeroU16,
    pub user_properties: UserProperties,
    /// Topic Filters to remove (§3.10.3).
    pub topic_filters: Vec<ByteString>,
}

/// UNSUBACK packet body (§3.11).
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct UnsubscribeAck {
    /// Packet Identifier (§2.2.1, §3.11.2).
    pub packet_id: NonZeroU16,
    pub properties: UserProperties,
    pub reason_string: Option<ByteString>,
    pub status: Vec<UnsubscribeAckReason>,
}

/// SUBACK Reason Codes (§3.9.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SubscribeAckReason {
    GrantedQos0 = 0,
    GrantedQos1 = 1,
    GrantedQos2 = 2,
    UnspecifiedError = 128,
    ImplementationSpecificError = 131,
    NotAuthorized = 135,
    TopicFilterInvalid = 143,
    PacketIdentifierInUse = 145,
    QuotaExceeded = 151,
    SharedSubscriptionNotSupported = 158,
    SubscriptionIdentifiersNotSupported = 161,
    WildcardSubscriptionsNotSupported = 162,
}

impl SubscribeAckReason {
    /// Whether the broker granted the subscription (any `GrantedQos*`, i.e. code below 0x80).
    #[inline]
    pub fn is_granted(self) -> bool {
        (self as u8) < 0x80
    }
}

impl TryFrom<u8> for SubscribeAckReason {
    type Error = UnknownCode;

    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(Self::GrantedQos0),
            1 => Ok(Self::GrantedQos1),
            2 => Ok(Self::GrantedQos2),
            128 => Ok(Self::UnspecifiedError),
            131 => Ok(Self::ImplementationSpecificError),
            135 => Ok(Self::NotAuthorized),
            143 => Ok(Self::TopicFilterInvalid),
            145 => Ok(Self::PacketIdentifierInUse),
            151 => Ok(Self::QuotaExceeded),
            158 => Ok(Self::SharedSubscriptionNotSupported),
            161 => Ok(Self::SubscriptionIdentifiersNotSupported),
            162 => Ok(Self::WildcardSubscriptionsNotSupported),
            _ => Err(UnknownCode(v)),
        }
    }
}

impl From<SubscribeAckReason> for u8 {
    fn from(v: SubscribeAckReason) -> Self {
        v as u8
    }
}

/// UNSUBACK Reason Codes (§3.11.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum UnsubscribeAckReason {
    Success = 0,
    NoSubscriptionExisted = 17,
    UnspecifiedError = 128,
    ImplementationSpecificError = 131,
    NotAuthorized = 135,
    TopicFilterInvalid = 143,
    PacketIdentifierInUse = 145,
}

impl TryFrom<u8> for UnsubscribeAckReason {
    type Error = UnknownCode;

    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(Self::Success),
            17 => Ok(Self::NoSubscriptionExisted),
            128 => Ok(Self::UnspecifiedError),
            131 => Ok(Self::ImplementationSpecificError),
            135 => Ok(Self::NotAuthorized),
            143 => Ok(Self::TopicFilterInvalid),
            145 => Ok(Self::PacketIdentifierInUse),
            _ => Err(UnknownCode(v)),
        }
    }
}

impl From<UnsubscribeAckReason> for u8 {
    fn from(v: UnsubscribeAckReason) -> Self {
        v as u8
    }
}

impl Subscribe {
    pub(crate) fn decode(src: &mut Bytes) -> Result<Self, DecodeError> {
        let packet_id = NonZeroU16::decode(src)?;
        let prop_src = &mut utils::take_properties(src)?;
        let mut sub_id = None;
        let mut user_properties = Vec::new();
        while prop_src.has_remaining() {
            let prop_id = prop_src.get_u8();
            match prop_id {
                pt::SUB_ID => {
                    ensure!(sub_id.is_none(), DecodeError::MalformedPacket("invalid encoding")); // can't appear twice
                    let val = utils::decode_variable_length_cursor(prop_src)?;
                    sub_id = Some(NonZeroU32::new(val).ok_or(DecodeError::MalformedPacket("invalid encoding"))?);
                }
                pt::USER => user_properties.push(UserProperty::decode(prop_src)?),
                _ => return Err(DecodeError::MalformedPacket("unknown property or reason code")),
            }
        }

        let mut topic_filters = Vec::new();
        while src.has_remaining() {
            let topic = decode_non_empty_topic(src)?;
            let opts = SubscriptionOptions::decode(src)?;
            topic_filters.push((topic, opts));
        }

        // [MQTT-3.8.3-2] SUBSCRIBE payload must contain at least one Topic Filter
        ensure!(
            !topic_filters.is_empty(),
            DecodeError::MalformedPacket("invalid encoding")
        );

        Ok(Self {
            packet_id,
            id: sub_id,
            user_properties,
            topic_filters,
        })
    }
}

impl SubscribeAck {
    pub(crate) fn decode(src: &mut Bytes) -> Result<Self, DecodeError> {
        let packet_id = NonZeroU16::decode(src)?;
        let (properties, reason_string) = ack_props::decode(src)?;
        ensure!(src.has_remaining(), DecodeError::MalformedPacket("invalid encoding"));
        let mut status = Vec::with_capacity(src.remaining());
        for code in src.as_ref().iter().copied() {
            status.push(code.try_into()?);
        }
        Ok(Self {
            packet_id,
            properties,
            reason_string,
            status,
        })
    }
}

impl Unsubscribe {
    pub(crate) fn decode(src: &mut Bytes) -> Result<Self, DecodeError> {
        let packet_id = NonZeroU16::decode(src)?;

        let prop_src = &mut utils::take_properties(src)?;
        let mut user_properties = Vec::new();
        while prop_src.has_remaining() {
            let prop_id = prop_src.get_u8();
            match prop_id {
                pt::USER => user_properties.push(UserProperty::decode(prop_src)?),
                _ => return Err(DecodeError::MalformedPacket("unknown property or reason code")),
            }
        }

        let mut topic_filters = Vec::new();
        while src.remaining() > 0 {
            topic_filters.push(decode_non_empty_topic(src)?);
        }

        // [MQTT-3.10.3-2] UNSUBSCRIBE payload must contain at least one Topic Filter
        ensure!(
            !topic_filters.is_empty(),
            DecodeError::MalformedPacket("invalid encoding")
        );

        Ok(Self {
            packet_id,
            user_properties,
            topic_filters,
        })
    }
}

impl UnsubscribeAck {
    pub(crate) fn decode(src: &mut Bytes) -> Result<Self, DecodeError> {
        let packet_id = NonZeroU16::decode(src)?;
        let (properties, reason_string) = ack_props::decode(src)?;
        ensure!(src.has_remaining(), DecodeError::MalformedPacket("invalid encoding"));
        let mut status = Vec::with_capacity(src.remaining());
        for code in src.as_ref().iter().copied() {
            status.push(code.try_into()?);
        }
        Ok(Self {
            packet_id,
            properties,
            reason_string,
            status,
        })
    }
}

impl WireBodyEncode for Subscribe {
    fn encoded_body_len(&self, _max_out: u32) -> Result<usize, EncodeError> {
        let prop_len = self.id.map_or(0, |v| 1 + var_int_len(v.get() as usize) as usize) // +1 to account for property type byte
            + self.user_properties.encoded_size();
        let payload_len = self
            .topic_filters
            .iter()
            .fold(0, |acc, (filter, _opts)| acc + filter.encoded_size() + 1);
        Ok(self.packet_id.encoded_size() + var_int_len(prop_len) as usize + prop_len + payload_len)
    }

    fn encode_body(&self, buf: &mut BytesMut, _: u32) -> Result<(), EncodeError> {
        self.packet_id.encode(buf)?;

        // encode properties
        let prop_len =
            self.id.map_or(0, |v| 1 + var_int_len(v.get() as usize)) + self.user_properties.encoded_size() as u32; // safe: size was already checked against maximum
        utils::write_variable_length(prop_len, buf)?;

        if let Some(id) = self.id {
            buf.put_u8(pt::SUB_ID);
            utils::write_variable_length(id.get(), buf)?;
        }

        self.user_properties.encode(buf)?;

        // payload
        for (filter, opts) in self.topic_filters.iter() {
            if filter.is_empty() {
                return Err(EncodeError::MalformedPacket);
            }
            filter.encode(buf)?;
            opts.encode(buf)?;
        }

        Ok(())
    }
}

impl Decode for SubscriptionOptions {
    fn decode(src: &mut Bytes) -> Result<Self, DecodeError> {
        ensure!(src.has_remaining(), DecodeError::InvalidLength);
        let val = src.get_u8();
        // [MQTT-3.8.3-5] Subscription Options bits 6-7 are reserved
        ensure!(
            (val & 0b1100_0000) == 0,
            DecodeError::MalformedPacket("invalid encoding")
        );
        let qos = (val & 0b0000_0011).try_into()?;
        let retain_handling = ((val & 0b0011_0000) >> 4).try_into()?;
        Ok(SubscriptionOptions {
            qos,
            no_local: val & 0b0000_0100 != 0,
            retain_as_published: val & 0b0000_1000 != 0,
            retain_handling,
        })
    }
}

impl Encode for SubscriptionOptions {
    fn encoded_size(&self) -> usize {
        1
    }
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodeError> {
        buf.put_u8(
            u8::from(self.qos)
                | ((self.no_local as u8) << 2)
                | ((self.retain_as_published as u8) << 3)
                | (u8::from(self.retain_handling) << 4),
        );
        Ok(())
    }
}

fn status_ack_encoded_size(
    status_len: usize,
    properties: &UserProperties,
    reason_string: &Option<ByteString>,
    limit: u32,
) -> usize {
    let payload_len = 2 + status_len;
    payload_len + ack_props::encoded_size(properties, reason_string, reduce_limit(limit, payload_len))
}

fn encode_status_ack<R>(
    packet_id: NonZeroU16,
    status: &[R],
    properties: &UserProperties,
    reason_string: &Option<ByteString>,
    buf: &mut BytesMut,
    max_out: u32,
) -> Result<(), EncodeError>
where
    R: Copy + Into<u8>,
{
    packet_id.encode(buf)?;
    ack_props::encode(properties, reason_string, buf, reduce_limit(max_out, 2 + status.len()))?;
    for reason in status {
        buf.put_u8((*reason).into());
    }
    Ok(())
}

impl WireBodyEncode for SubscribeAck {
    fn encoded_body_len(&self, max_out: u32) -> Result<usize, EncodeError> {
        Ok(status_ack_encoded_size(
            self.status.len(),
            &self.properties,
            &self.reason_string,
            max_out,
        ))
    }

    fn encode_body(&self, buf: &mut BytesMut, max_out: u32) -> Result<(), EncodeError> {
        encode_status_ack(
            self.packet_id,
            &self.status,
            &self.properties,
            &self.reason_string,
            buf,
            max_out,
        )
    }
}

impl WireBodyEncode for Unsubscribe {
    fn encoded_body_len(&self, _max_out: u32) -> Result<usize, EncodeError> {
        let prop_len = self.user_properties.encoded_size();
        Ok(2 + var_int_len(prop_len) as usize
            + prop_len
            + self.topic_filters.iter().fold(0, |acc, filter| acc + 2 + filter.len()))
    }

    fn encode_body(&self, buf: &mut BytesMut, _max_out: u32) -> Result<(), EncodeError> {
        self.packet_id.encode(buf)?;

        // properties
        let prop_len = self.user_properties.encoded_size();
        utils::write_variable_length(prop_len as u32, buf)?; // safe: max size check is done already
        self.user_properties.encode(buf)?;

        // payload
        for filter in self.topic_filters.iter() {
            if filter.is_empty() {
                return Err(EncodeError::MalformedPacket);
            }
            filter.encode(buf)?;
        }
        Ok(())
    }
}

impl WireBodyEncode for UnsubscribeAck {
    fn encoded_body_len(&self, max_out: u32) -> Result<usize, EncodeError> {
        Ok(status_ack_encoded_size(
            self.status.len(),
            &self.properties,
            &self.reason_string,
            max_out,
        ))
    }

    fn encode_body(&self, buf: &mut BytesMut, max_out: u32) -> Result<(), EncodeError> {
        encode_status_ack(
            self.packet_id,
            &self.status,
            &self.properties,
            &self.reason_string,
            buf,
            max_out,
        )
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    /// Dispatch target for [`subscribe_family_decode_rejects_malformed_packet`]: SUBSCRIBE,
    /// UNSUBSCRIBE, and their acks share identical malformed-payload rules but decode to
    /// distinct types, so cases select which `decode` to exercise.
    #[derive(Clone, Copy)]
    enum SubscribeFamily {
        Subscribe,
        Unsubscribe,
        SubscribeAck,
        UnsubscribeAck,
    }

    #[rstest]
    // [MQTT-3.8.3-2] / [MQTT-3.10.3-2] payload must contain at least one Topic Filter
    #[case::subscribe_empty_topic_filters(SubscribeFamily::Subscribe, vec![0x12, 0x34, 0x00])]
    #[case::unsubscribe_empty_topic_filters(SubscribeFamily::Unsubscribe, vec![0x12, 0x34, 0x00])]
    // [MQTT-3.8.3-1] / [MQTT-3.10.3-1] Topic Filters must be UTF-8 Encoded Strings
    #[case::subscribe_invalid_utf8_topic_filter(SubscribeFamily::Subscribe, vec![0x12, 0x34, 0x00, 0x00, 0x02, 0xC0, 0x80, 0x00])]
    #[case::unsubscribe_invalid_utf8_topic_filter(SubscribeFamily::Unsubscribe, vec![0x12, 0x34, 0x00, 0x00, 0x02, 0xC0, 0x80])]
    // A Topic Filter is at least one character long (§4.7.3)
    #[case::subscribe_empty_topic_filter_string(SubscribeFamily::Subscribe, vec![0x12, 0x34, 0x00, 0x00, 0x00])]
    // SUBACK / UNSUBACK carry one reason code per Topic Filter, so at least one
    #[case::suback_empty_reason_codes(SubscribeFamily::SubscribeAck, vec![0x12, 0x34, 0x00])]
    #[case::unsuback_empty_reason_codes(SubscribeFamily::UnsubscribeAck, vec![0x12, 0x34, 0x00])]
    // Reason code 0x7F is not a defined SUBACK/UNSUBACK reason
    #[case::suback_invalid_reason_code(SubscribeFamily::SubscribeAck, vec![0x12, 0x34, 0x00, 0x7F])]
    #[case::unsuback_invalid_reason_code(SubscribeFamily::UnsubscribeAck, vec![0x12, 0x34, 0x00, 0x7F])]
    fn subscribe_family_decode_rejects_malformed_packet(#[case] target: SubscribeFamily, #[case] wire: Vec<u8>) {
        let mut src = Bytes::from(wire);
        let rejected_as_malformed = match target {
            SubscribeFamily::Subscribe => {
                matches!(Subscribe::decode(&mut src), Err(DecodeError::MalformedPacket(_)))
            }
            SubscribeFamily::Unsubscribe => {
                matches!(Unsubscribe::decode(&mut src), Err(DecodeError::MalformedPacket(_)))
            }
            SubscribeFamily::SubscribeAck => {
                matches!(SubscribeAck::decode(&mut src), Err(DecodeError::MalformedPacket(_)))
            }
            SubscribeFamily::UnsubscribeAck => {
                matches!(UnsubscribeAck::decode(&mut src), Err(DecodeError::MalformedPacket(_)))
            }
        };
        assert!(rejected_as_malformed);
    }

    #[test]
    fn subscription_options_rejects_reserved_bits() {
        let mut src = Bytes::from_static(&[0xC0]);
        assert!(matches!(
            SubscriptionOptions::decode(&mut src),
            Err(DecodeError::MalformedPacket("invalid encoding"))
        ));
    }

    #[test]
    fn suback_decode_preserves_reason_code_order() {
        let mut src = Bytes::from_static(&[0x12, 0x34, 0x00, 0x01, 0x87]);
        let ack = SubscribeAck::decode(&mut src).expect("decode");
        assert_eq!(
            ack.status,
            vec![SubscribeAckReason::GrantedQos1, SubscribeAckReason::NotAuthorized]
        );
    }

    #[test]
    fn unsuback_decode_preserves_reason_code_order() {
        let mut src = Bytes::from_static(&[0x12, 0x34, 0x00, 0x00, 0x11]);
        let ack = UnsubscribeAck::decode(&mut src).expect("decode");
        assert_eq!(
            ack.status,
            vec![
                UnsubscribeAckReason::Success,
                UnsubscribeAckReason::NoSubscriptionExisted
            ]
        );
    }

    /// Only the three Granted QoS codes grant a subscription; every failure code (0x80 and up) does not.
    #[test]
    fn only_granted_qos_codes_grant_the_subscription() {
        for byte in 0..=u8::MAX {
            if let Ok(reason) = SubscribeAckReason::try_from(byte) {
                assert_eq!(reason.is_granted(), byte <= 2, "{reason:?}");
            }
        }
    }

    #[test]
    fn subscription_options_byte_follows_the_spec_layout() {
        // §3.8.3.1: bits 0-1 QoS, bit 2 No Local, bit 3 Retain As Published, bits 4-5 Retain Handling.
        for qos in [QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce] {
            for no_local in [false, true] {
                for retain_as_published in [false, true] {
                    for (retain_handling, rh) in [
                        (RetainHandling::AtSubscribe, 0u8),
                        (RetainHandling::AtSubscribeNew, 1),
                        (RetainHandling::NoAtSubscribe, 2),
                    ] {
                        let options = SubscriptionOptions {
                            qos,
                            no_local,
                            retain_as_published,
                            retain_handling,
                        };
                        let byte = qos as u8 | (no_local as u8) << 2 | (retain_as_published as u8) << 3 | rh << 4;
                        let mut buf = BytesMut::new();
                        options.encode(&mut buf).unwrap();
                        assert_eq!(buf.as_ref(), &[byte], "{options:?}");
                        assert_eq!(SubscriptionOptions::decode(&mut buf.freeze()).unwrap(), options);
                    }
                }
            }
        }
    }
}
