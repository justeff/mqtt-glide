//! MQTT **SUBSCRIBE**, **SUBACK**, **UNSUBSCRIBE**, and **UNSUBACK** control packets.
//!
//! - OASIS MQTT v5.0: §3.8 SUBSCRIBE — Subscribe request
//! - OASIS MQTT v5.0: §3.9 SUBACK — Subscribe acknowledgement
//! - OASIS MQTT v5.0: §3.10 UNSUBSCRIBE — Unsubscribe request
//! - OASIS MQTT v5.0: §3.11 UNSUBACK — Unsubscribe acknowledgement

use std::convert::TryFrom;
use std::num::{NonZeroU16, NonZeroU32};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use bytestring::ByteString;

use crate::codec::error::{DecodeError, EncodeError};
use crate::codec::properties::{ack_props, reduce_limit, var_int_len};
use crate::codec::types::QoS;
use crate::codec::utils::{self, Decode, Encode, decode_non_empty_topic};
use crate::codec::v5::body_encode::WireBodyEncode;
use crate::codec::v5::{UserProperties, UserProperty, property_type as pt};

/// Represents SUBSCRIBE packet
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Subscribe {
    /// Packet Identifier
    pub packet_id: NonZeroU16,
    /// Subscription Identifier
    pub id: Option<NonZeroU32>,
    pub user_properties: UserProperties,
    /// the list of Topic Filters and QoS to which the Client wants to subscribe.
    pub topic_filters: Vec<(ByteString, SubscriptionOptions)>,
}

/// Subscription options for MQTT v5 SUBSCRIBE packets
///
/// Contains per-subscription options including QoS, no-local, retain-as-published,
/// and retain handling as defined in the MQTT v5.0 specification.
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

/// Retain handling options for MQTT v5 subscriptions.
///
/// Controls how retained messages are sent when a subscription is established.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RetainHandling {
    AtSubscribe = 0,
    AtSubscribeNew = 1,
    NoAtSubscribe = 2,
}

impl TryFrom<u8> for RetainHandling {
    type Error = DecodeError;

    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(Self::AtSubscribe),
            1 => Ok(Self::AtSubscribeNew),
            2 => Ok(Self::NoAtSubscribe),
            _ => Err(DecodeError::MalformedPacket),
        }
    }
}

impl From<RetainHandling> for u8 {
    fn from(v: RetainHandling) -> Self {
        v as u8
    }
}

/// Represents SUBACK packet
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SubscribeAck {
    pub packet_id: NonZeroU16,
    pub properties: UserProperties,
    pub reason_string: Option<ByteString>,
    /// corresponds to a Topic Filter in the SUBSCRIBE Packet being acknowledged.
    pub status: Vec<SubscribeAckReason>,
}

/// Represents UNSUBSCRIBE packet
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Unsubscribe {
    /// Packet Identifier
    pub packet_id: NonZeroU16,
    pub user_properties: UserProperties,
    /// the list of Topic Filters that the Client wishes to unsubscribe from.
    pub topic_filters: Vec<ByteString>,
}

/// Represents UNSUBACK packet
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct UnsubscribeAck {
    /// Packet Identifier
    pub packet_id: NonZeroU16,
    pub properties: UserProperties,
    pub reason_string: Option<ByteString>,
    pub status: Vec<UnsubscribeAckReason>,
}

/// SUBACK reason codes
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

impl TryFrom<u8> for SubscribeAckReason {
    type Error = DecodeError;

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
            _ => Err(DecodeError::MalformedPacket),
        }
    }
}

impl From<SubscribeAckReason> for u8 {
    fn from(v: SubscribeAckReason) -> Self {
        v as u8
    }
}

/// UNSUBACK reason codes
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
    type Error = DecodeError;

    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(Self::Success),
            17 => Ok(Self::NoSubscriptionExisted),
            128 => Ok(Self::UnspecifiedError),
            131 => Ok(Self::ImplementationSpecificError),
            135 => Ok(Self::NotAuthorized),
            143 => Ok(Self::TopicFilterInvalid),
            145 => Ok(Self::PacketIdentifierInUse),
            _ => Err(DecodeError::MalformedPacket),
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
                    ensure!(sub_id.is_none(), DecodeError::MalformedPacket); // can't appear twice
                    let val = utils::decode_variable_length_cursor(prop_src)?;
                    sub_id = Some(NonZeroU32::new(val).ok_or(DecodeError::MalformedPacket)?);
                }
                pt::USER => user_properties.push(UserProperty::decode(prop_src)?),
                _ => return Err(DecodeError::MalformedPacket),
            }
        }

        let mut topic_filters = Vec::new();
        while src.has_remaining() {
            let topic = decode_non_empty_topic(src)?;
            let opts = SubscriptionOptions::decode(src)?;
            topic_filters.push((topic, opts));
        }

        // [MQTT-3.8.3-2] SUBSCRIBE payload must contain at least one Topic Filter
        ensure!(!topic_filters.is_empty(), DecodeError::MalformedPacket);

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
        ensure!(src.has_remaining(), DecodeError::MalformedPacket);
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
                _ => return Err(DecodeError::MalformedPacket),
            }
        }

        let mut topic_filters = Vec::new();
        while src.remaining() > 0 {
            topic_filters.push(decode_non_empty_topic(src)?);
        }

        // [MQTT-3.10.3-2] UNSUBSCRIBE payload must contain at least one Topic Filter
        ensure!(!topic_filters.is_empty(), DecodeError::MalformedPacket);

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
        ensure!(src.has_remaining(), DecodeError::MalformedPacket);
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
        ensure!((val & 0b1100_0000) == 0, DecodeError::MalformedPacket);
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
    use crate::codec::v5::{DecodedFrame, Packet, V5Codec};
    use tokio_util::codec::Decoder;
    use tokio_util::codec::Encoder;

    #[test]
    fn test_sub() {
        let pkt = Subscribe {
            packet_id: 12.try_into().unwrap(),
            id: Some(10.try_into().unwrap()),
            user_properties: vec![("a".into(), "1".into())],
            topic_filters: vec![("test".into(), SubscriptionOptions::default())],
        };

        let body_len = pkt.encoded_body_len(99999).unwrap() as u32;
        let mut buf = BytesMut::with_capacity(body_len as usize);
        pkt.encode_body(&mut buf, body_len).unwrap();
        assert_eq!(buf.len(), body_len as usize);
        assert_eq!(pkt, Subscribe::decode(&mut buf.freeze()).unwrap());

        let pkt = Unsubscribe {
            packet_id: 12.try_into().unwrap(),
            user_properties: vec![("a".into(), "1".into())],
            topic_filters: vec!["test".into()],
        };

        let body_len = pkt.encoded_body_len(99999).unwrap() as u32;
        let mut buf = BytesMut::with_capacity(body_len as usize);
        pkt.encode_body(&mut buf, body_len).unwrap();
        assert_eq!(buf.len(), body_len as usize);
        assert_eq!(pkt, Unsubscribe::decode(&mut buf.freeze()).unwrap());
    }

    #[test]
    fn test_sub_pkt() {
        let pkt = Packet::Subscribe(Subscribe {
            packet_id: 12.try_into().unwrap(),
            id: None,
            user_properties: vec![("a".into(), "1".into())],
            topic_filters: vec![("test".into(), SubscriptionOptions::default())],
        });
        let mut codec = V5Codec::default();

        let mut buf = BytesMut::new();
        codec.encode(pkt.clone(), &mut buf).unwrap();

        assert_eq!(
            DecodedFrame::Control(pkt.clone()),
            codec.decode(&mut buf).unwrap().unwrap().0
        );
    }

    #[test]
    fn test_sub_ack() {
        let ack = SubscribeAck {
            packet_id: NonZeroU16::new(1).unwrap(),
            properties: vec![("prop1".into(), "val1".into()), ("prop2".into(), "val2".into())],
            reason_string: None,
            status: vec![SubscribeAckReason::GrantedQos0],
        };
        let body_len = ack.encoded_body_len(99999).unwrap() as u32;
        let mut buf = BytesMut::with_capacity(body_len as usize);
        ack.encode_body(&mut buf, body_len).unwrap();
        assert_eq!(ack, SubscribeAck::decode(&mut buf.freeze()).unwrap());

        let ack = UnsubscribeAck {
            packet_id: NonZeroU16::new(1).unwrap(),
            properties: vec![("prop1".into(), "val1".into()), ("prop2".into(), "val2".into())],
            reason_string: None,
            status: vec![UnsubscribeAckReason::Success],
        };
        let body_len = ack.encoded_body_len(99999).unwrap() as u32;
        let mut buf = BytesMut::with_capacity(body_len as usize);
        ack.encode_body(&mut buf, body_len).unwrap();
        assert_eq!(ack, UnsubscribeAck::decode(&mut buf.freeze()).unwrap());
    }

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
    // Topic Filter must be well-formed UTF-8
    #[case::subscribe_invalid_utf8_topic_filter(SubscribeFamily::Subscribe, vec![0x12, 0x34, 0x00, 0x00, 0x02, 0xC0, 0x80, 0x00])]
    #[case::unsubscribe_invalid_utf8_topic_filter(SubscribeFamily::Unsubscribe, vec![0x12, 0x34, 0x00, 0x00, 0x02, 0xC0, 0x80])]
    // Reason code 0x7F is not a defined SUBACK/UNSUBACK reason
    #[case::suback_invalid_reason_code(SubscribeFamily::SubscribeAck, vec![0x12, 0x34, 0x00, 0x7F])]
    #[case::unsuback_invalid_reason_code(SubscribeFamily::UnsubscribeAck, vec![0x12, 0x34, 0x00, 0x7F])]
    fn subscribe_family_decode_rejects_malformed_packet(#[case] target: SubscribeFamily, #[case] wire: Vec<u8>) {
        let mut src = Bytes::from(wire);
        let rejected_as_malformed = match target {
            SubscribeFamily::Subscribe => {
                matches!(Subscribe::decode(&mut src), Err(DecodeError::MalformedPacket))
            }
            SubscribeFamily::Unsubscribe => {
                matches!(Unsubscribe::decode(&mut src), Err(DecodeError::MalformedPacket))
            }
            SubscribeFamily::SubscribeAck => {
                matches!(SubscribeAck::decode(&mut src), Err(DecodeError::MalformedPacket))
            }
            SubscribeFamily::UnsubscribeAck => {
                matches!(UnsubscribeAck::decode(&mut src), Err(DecodeError::MalformedPacket))
            }
        };
        assert!(rejected_as_malformed);
    }

    #[test]
    fn subscribe_rejects_empty_filter_string() {
        let mut src = Bytes::from_static(&[0x12, 0x34, 0x00, 0x00, 0x00]);
        assert!(matches!(Subscribe::decode(&mut src), Err(DecodeError::MalformedPacket)));
    }

    #[test]
    fn suback_rejects_empty_reason_codes() {
        let mut src = Bytes::from_static(&[0x12, 0x34, 0x00]);
        assert!(matches!(
            SubscribeAck::decode(&mut src),
            Err(DecodeError::MalformedPacket)
        ));
    }

    #[test]
    fn unsuback_rejects_empty_reason_codes() {
        let mut src = Bytes::from_static(&[0x12, 0x34, 0x00]);
        assert!(matches!(
            UnsubscribeAck::decode(&mut src),
            Err(DecodeError::MalformedPacket)
        ));
    }

    #[test]
    fn subscription_options_rejects_reserved_bits() {
        let mut src = Bytes::from_static(&[0xC0]);
        assert!(matches!(
            SubscriptionOptions::decode(&mut src),
            Err(DecodeError::MalformedPacket)
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
}
