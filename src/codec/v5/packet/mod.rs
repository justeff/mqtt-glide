//! MQTT v5 control-packet enum and property identifiers.
//!
//! PUBLISH is decoded separately as [`DecodedFrame::Publish`]; this module covers the
//! remaining control packets from OASIS MQTT Version 5.0 §3.

use crate::codec::publish::PublishPacket;
use crate::codec::types::packet_type;

mod auth;
mod connack;
mod connect;
mod disconnect;
mod pubacks;
mod publish;
mod subscribe;

pub use auth::*;
pub use connack::*;
pub use connect::*;
pub use disconnect::*;
pub use pubacks::*;
pub use subscribe::*;

use crate::codec::error::{DecodeError, UnknownCode};

/// Decodes an acknowledgement Reason Code, reading an undefined failure code (0x80 and up) as
/// `unspecified` instead of failing the packet as malformed.
///
/// The broker has refused either way, and a refusal is final for that request. Rejecting the
/// packet instead ended the connection, and a refused subscription restored on every reconnect
/// was refused the same way again. Codes below 0x80 stay strict: they claim success.
pub(crate) fn lenient_failure_code<T: TryFrom<u8, Error = UnknownCode>>(
    byte: u8,
    unspecified: T,
) -> Result<T, DecodeError> {
    match T::try_from(byte) {
        Ok(reason) => Ok(reason),
        Err(_) if byte >= 0x80 => Ok(unspecified),
        Err(unknown) => Err(unknown.into()),
    }
}

/// Decoded inbound MQTT frame: PUBLISH data path or a control packet.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum DecodedFrame {
    /// PUBLISH (§3.3).
    Publish(PublishPacket),
    /// Any other MQTT control packet.
    Control(Packet),
}

/// MQTT control packets excluding PUBLISH (see [`DecodedFrame::Publish`]).
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Packet {
    /// CONNECT (§3.1).
    Connect(Box<Connect>),
    /// CONNACK (§3.2).
    ConnectAck(Box<ConnectAck>),
    /// PUBACK (§3.4).
    PublishAck(PublishAck),
    /// PUBREC (§3.5).
    PublishReceived(PublishAck),
    /// PUBREL (§3.6).
    PublishRelease(PubRelComp),
    /// PUBCOMP (§3.7).
    PublishComplete(PubRelComp),
    /// SUBSCRIBE (§3.8).
    Subscribe(Subscribe),
    /// SUBACK (§3.9).
    SubscribeAck(SubscribeAck),
    /// UNSUBSCRIBE (§3.10).
    Unsubscribe(Unsubscribe),
    /// UNSUBACK (§3.11).
    UnsubscribeAck(UnsubscribeAck),
    /// PINGREQ (§3.12).
    PingRequest,
    /// PINGRESP (§3.13).
    PingResponse,
    /// DISCONNECT (§3.14).
    Disconnect(Disconnect),
    /// AUTH (§3.15).
    Auth(Auth),
}

impl Packet {
    /// Fixed-header packet type nibble as a full first-byte value (§2.1.2).
    pub fn packet_type(&self) -> u8 {
        match self {
            Packet::Connect(_) => packet_type::CONNECT,
            Packet::ConnectAck(_) => packet_type::CONNACK,
            Packet::PublishAck(_) => packet_type::PUBACK,
            Packet::PublishReceived(_) => packet_type::PUBREC,
            Packet::PublishRelease(_) => packet_type::PUBREL,
            Packet::PublishComplete(_) => packet_type::PUBCOMP,
            Packet::Subscribe(_) => packet_type::SUBSCRIBE,
            Packet::SubscribeAck(_) => packet_type::SUBACK,
            Packet::Unsubscribe(_) => packet_type::UNSUBSCRIBE,
            Packet::UnsubscribeAck(_) => packet_type::UNSUBACK,
            Packet::PingRequest => packet_type::PINGREQ,
            Packet::PingResponse => packet_type::PINGRESP,
            Packet::Disconnect(_) => packet_type::DISCONNECT,
            Packet::Auth(_) => packet_type::AUTH,
        }
    }
}

impl From<Connect> for Packet {
    fn from(pkt: Connect) -> Self {
        Self::Connect(Box::new(pkt))
    }
}

impl From<Box<Connect>> for Packet {
    fn from(pkt: Box<Connect>) -> Self {
        Self::Connect(pkt)
    }
}

impl From<ConnectAck> for Packet {
    fn from(pkt: ConnectAck) -> Self {
        Self::ConnectAck(Box::new(pkt))
    }
}

impl From<Box<ConnectAck>> for Packet {
    fn from(pkt: Box<ConnectAck>) -> Self {
        Self::ConnectAck(pkt)
    }
}

impl From<PublishAck> for Packet {
    fn from(pkt: PublishAck) -> Self {
        Self::PublishAck(pkt)
    }
}

impl From<Subscribe> for Packet {
    fn from(pkt: Subscribe) -> Self {
        Self::Subscribe(pkt)
    }
}

impl From<SubscribeAck> for Packet {
    fn from(pkt: SubscribeAck) -> Self {
        Self::SubscribeAck(pkt)
    }
}

impl From<Unsubscribe> for Packet {
    fn from(pkt: Unsubscribe) -> Self {
        Self::Unsubscribe(pkt)
    }
}

impl From<UnsubscribeAck> for Packet {
    fn from(pkt: UnsubscribeAck) -> Self {
        Self::UnsubscribeAck(pkt)
    }
}

impl From<Disconnect> for Packet {
    fn from(pkt: Disconnect) -> Self {
        Self::Disconnect(pkt)
    }
}

impl From<Auth> for Packet {
    fn from(pkt: Auth) -> Self {
        Self::Auth(pkt)
    }
}

/// Property identifiers from OASIS MQTT Version 5.0 §2.2.2.2.
pub(crate) mod property_type {
    pub(crate) const UTF8_PAYLOAD: u8 = 0x01;
    pub(crate) const MSG_EXPIRY_INT: u8 = 0x02;
    pub(crate) const CONTENT_TYPE: u8 = 0x03;
    pub(crate) const RESP_TOPIC: u8 = 0x08;
    pub(crate) const CORR_DATA: u8 = 0x09;
    pub(crate) const SUB_ID: u8 = 0x0B;
    pub(crate) const SESS_EXPIRY_INT: u8 = 0x11;
    pub(crate) const ASSND_CLIENT_ID: u8 = 0x12;
    pub(crate) const SERVER_KA: u8 = 0x13;
    pub(crate) const AUTH_METHOD: u8 = 0x15;
    pub(crate) const AUTH_DATA: u8 = 0x16;
    pub(crate) const REQ_PROB_INFO: u8 = 0x17;
    pub(crate) const WILL_DELAY_INT: u8 = 0x18;
    pub(crate) const REQ_RESP_INFO: u8 = 0x19;
    pub(crate) const RESP_INFO: u8 = 0x1A;
    pub(crate) const SERVER_REF: u8 = 0x1C;
    pub(crate) const REASON_STRING: u8 = 0x1F;
    pub(crate) const RECEIVE_MAX: u8 = 0x21;
    pub(crate) const TOPIC_ALIAS_MAX: u8 = 0x22;
    pub(crate) const TOPIC_ALIAS: u8 = 0x23;
    pub(crate) const MAX_QOS: u8 = 0x24;
    pub(crate) const RETAIN_AVAIL: u8 = 0x25;
    pub(crate) const USER: u8 = 0x26;
    pub(crate) const MAX_PACKET_SIZE: u8 = 0x27;
    pub(crate) const WILDCARD_SUB_AVAIL: u8 = 0x28;
    pub(crate) const SUB_IDS_AVAIL: u8 = 0x29;
    pub(crate) const SHARED_SUB_AVAIL: u8 = 0x2A;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::types::QoS;

    /// Every byte in `spec` decodes and encodes back to itself; every other byte is refused.
    fn assert_code_table<T>(spec: &[u8])
    where
        T: TryFrom<u8> + Copy + std::fmt::Debug,
        u8: From<T>,
    {
        for byte in 0..=u8::MAX {
            match T::try_from(byte) {
                Ok(code) => {
                    assert!(
                        spec.contains(&byte),
                        "{byte:#04x} decodes to {code:?} but is not in the table"
                    );
                    assert_eq!(u8::from(code), byte, "{code:?} encodes to a different byte");
                }
                Err(_) => assert!(!spec.contains(&byte), "{byte:#04x} is in the table but refused"),
            }
        }
    }

    #[test]
    fn reason_codes_match_the_spec_tables() {
        // §3.4.2.1 PUBACK and §3.5.2.1 PUBREC share one table.
        assert_code_table::<PublishAckReason>(&[0x00, 0x10, 0x80, 0x83, 0x87, 0x90, 0x91, 0x97, 0x99]);
        // §3.6.2.1 PUBREL and §3.7.2.1 PUBCOMP.
        assert_code_table::<PubRelCompReason>(&[0x00, 0x92]);
        // §3.9.3 SUBACK.
        assert_code_table::<SubscribeAckReason>(&[
            0x00, 0x01, 0x02, 0x80, 0x83, 0x87, 0x8F, 0x91, 0x97, 0x9E, 0xA1, 0xA2,
        ]);
        // §3.11.3 UNSUBACK.
        assert_code_table::<UnsubscribeAckReason>(&[0x00, 0x11, 0x80, 0x83, 0x87, 0x8F, 0x91]);
        // §3.14.2.1 DISCONNECT.
        assert_code_table::<DisconnectReasonCode>(&[
            0x00, 0x04, 0x80, 0x81, 0x82, 0x83, 0x87, 0x89, 0x8B, 0x8C, 0x8D, 0x8E, 0x8F, 0x90, 0x93, 0x94, 0x95, 0x96,
            0x97, 0x98, 0x99, 0x9A, 0x9B, 0x9C, 0x9D, 0x9E, 0x9F, 0xA0, 0xA1, 0xA2,
        ]);
        // §3.15.2.1 AUTH.
        assert_code_table::<AuthReasonCode>(&[0x00, 0x18, 0x19]);
        // §3.8.3.1 Retain Handling and §4.3 QoS.
        assert_code_table::<RetainHandling>(&[0, 1, 2]);
        assert_code_table::<QoS>(&[0, 1, 2]);
    }

    /// What a decoder made of one reason-code byte.
    #[derive(Debug, PartialEq)]
    enum Read {
        /// The packet decoded and carries this code.
        Code(u8),
        /// The packet was refused as malformed.
        Malformed,
    }

    /// The (first) reason code of a decoded ack, DISCONNECT or AUTH.
    fn code_of(packet: Packet) -> u8 {
        match packet {
            Packet::ConnectAck(ack) => ack.reason_code.into(),
            Packet::PublishAck(ack) | Packet::PublishReceived(ack) => ack.reason_code.into(),
            Packet::PublishRelease(ack) | Packet::PublishComplete(ack) => ack.reason_code.into(),
            Packet::SubscribeAck(ack) => ack.status[0].into(),
            Packet::UnsubscribeAck(ack) => ack.status[0].into(),
            Packet::Disconnect(packet) => packet.reason_code.into(),
            Packet::Auth(packet) => packet.reason_code.into(),
            other => panic!("no reason code in {other:?}"),
        }
    }

    fn read_code(first_byte: u8, body: Vec<u8>) -> Read {
        match super::super::decode::decode_packet(bytes::Bytes::from(body), first_byte) {
            Ok(DecodedFrame::Control(packet)) => Read::Code(code_of(packet)),
            Ok(DecodedFrame::Publish(_)) => panic!("{first_byte:#04x} decoded as PUBLISH"),
            Err(DecodeError::MalformedPacket(_)) => Read::Malformed,
            Err(other) => panic!("{first_byte:#04x}: unexpected {other:?}"),
        }
    }

    /// Every reason-code byte through the real packet decoders (§2.4): a defined code decodes
    /// to itself; an undefined one below 0x80 claims success and makes the packet malformed
    /// (for CONNACK [MQTT-3.2.2-8]); an undefined one from 0x80 is read as a failure where the
    /// packet has failure codes (`lenient`), and refused where it has none.
    #[test]
    fn every_reason_code_byte_is_classified_by_the_decoders() {
        // (name, first byte, body around the code, defined?, lenient?)
        type Case = (&'static str, u8, fn(u8) -> Vec<u8>, fn(u8) -> bool, bool);
        let cases: [Case; 9] = [
            (
                "CONNACK",
                0x20,
                |c| vec![0x00, c, 0x00],
                |b| !matches!(ConnectAckReason::from(b), ConnectAckReason::Unknown(_)),
                true,
            ),
            (
                "PUBACK",
                0x40,
                |c| vec![0x00, 0x01, c, 0x00],
                |b| PublishAckReason::try_from(b).is_ok(),
                true,
            ),
            (
                "PUBREC",
                0x50,
                |c| vec![0x00, 0x01, c, 0x00],
                |b| PublishAckReason::try_from(b).is_ok(),
                true,
            ),
            (
                "PUBREL",
                0x62,
                |c| vec![0x00, 0x01, c, 0x00],
                |b| PubRelCompReason::try_from(b).is_ok(),
                false,
            ),
            (
                "PUBCOMP",
                0x70,
                |c| vec![0x00, 0x01, c, 0x00],
                |b| PubRelCompReason::try_from(b).is_ok(),
                false,
            ),
            (
                "SUBACK",
                0x90,
                |c| vec![0x00, 0x01, 0x00, c],
                |b| SubscribeAckReason::try_from(b).is_ok(),
                true,
            ),
            (
                "UNSUBACK",
                0xB0,
                |c| vec![0x00, 0x01, 0x00, c],
                |b| UnsubscribeAckReason::try_from(b).is_ok(),
                true,
            ),
            (
                "DISCONNECT",
                0xE0,
                |c| vec![c, 0x00],
                |b| DisconnectReasonCode::try_from(b).is_ok(),
                true,
            ),
            // With an Authentication Method, which Continue and Re-authenticate need.
            (
                "AUTH",
                0xF0,
                |c| vec![c, 0x04, 0x15, 0x00, 0x01, b'm'],
                |b| AuthReasonCode::try_from(b).is_ok(),
                false,
            ),
        ];
        for (name, first_byte, body, defined, lenient) in cases {
            for byte in 0..=u8::MAX {
                let read = read_code(first_byte, body(byte));
                if defined(byte) {
                    assert_eq!(read, Read::Code(byte), "{name} {byte:#04x}");
                } else if byte >= 0x80 && lenient {
                    assert!(
                        matches!(read, Read::Code(code) if code >= 0x80),
                        "{name} {byte:#04x} must read as a failure, got {read:?}"
                    );
                } else {
                    assert_eq!(read, Read::Malformed, "{name} {byte:#04x}");
                }
            }
        }
    }

    #[test]
    fn connack_reason_codes_match_the_spec_table() {
        // §3.2.2.2: codes outside the table are kept as `Unknown` instead of refused.
        const SPEC: &[u8] = &[
            0x00, 0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8A, 0x8C, 0x90, 0x95, 0x97, 0x99, 0x9A,
            0x9B, 0x9C, 0x9D, 0x9F,
        ];
        for byte in 0..=u8::MAX {
            let code = ConnectAckReason::from(byte);
            assert_eq!(u8::from(code), byte, "{code:?} encodes to a different byte");
            assert_eq!(
                matches!(code, ConnectAckReason::Unknown(_)),
                !SPEC.contains(&byte),
                "{byte:#04x} decodes to {code:?}"
            );
        }
    }
}
