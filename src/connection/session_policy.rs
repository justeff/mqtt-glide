//! Session policy: topic-alias Maximum 0, inbound PUBLISH gates, outbound broker limits (§3.5).

use std::num::NonZeroU16;

use crate::codec::{PublishPacket, QoS};
use crate::error::{AppError, ProtocolError};

/// Rejects outbound PUBLISH that violates CONNACK Maximum QoS or Retain Available ([MQTT-3.2.2-11], [MQTT-3.2.2-14]).
pub(crate) fn validate_publish_against_broker_limits(
    qos: QoS,
    retain: bool,
    max_qos: QoS,
    retain_available: bool,
) -> Result<(), AppError> {
    if qos > max_qos {
        return Err(AppError::InvalidRequest(format!(
            "publish QoS {} exceeds broker Maximum QoS {}",
            qos.value(),
            max_qos.value()
        )));
    }
    if retain && !retain_available {
        return Err(AppError::InvalidRequest(
            "retain publish is not available on this broker session".into(),
        ));
    }
    Ok(())
}

/// Inbound PUBLISH Topic Alias policy: Maximum = 0 → any alias is a protocol error.
pub(crate) fn reject_inbound_topic_alias(packet: &PublishPacket) -> Result<(), ProtocolError> {
    if packet.wire_topic_alias.is_some() {
        Err(ProtocolError::TopicAliasNotSupported)
    } else {
        Ok(())
    }
}

/// Shared inbound PUBLISH policy for the read task and resubscribe wait path.
///
/// Rejects topic alias and QoS 2. On success returns the packet id that **MUST** be
/// PUBACKed only after successful flume accept (enqueue-before-ack), or `None` for QoS 0.
///
/// Callers differ only in handoff cut-off and PUBACK emission: the live read path
/// uses the control channel (write task owns the socket); resubscribe writes PUBACK
/// inline because read/write tasks are not running yet.
pub(crate) fn inbound_publish_policy(packet: &PublishPacket) -> Result<Option<NonZeroU16>, ProtocolError> {
    reject_inbound_topic_alias(packet)?;
    match packet.meta.qos {
        QoS::ExactlyOnce => Err(ProtocolError::Violation("QoS 2 PUBLISH is not supported".into())),
        QoS::AtLeastOnce => {
            let Some(packet_id) = packet.meta.packet_id else {
                return Err(ProtocolError::Violation(
                    "QoS 1 PUBLISH missing packet identifier".into(),
                ));
            };
            Ok(Some(packet_id))
        }
        QoS::AtMostOnce => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU16;

    use bytes::Bytes;
    use bytestring::ByteString;

    use super::*;
    use crate::codec::{PublishMessage, PublishPacket, PublishPacketMeta, PublishProperties, QoS};
    use crate::error::{AppError, ProtocolError};

    fn publish_with_alias(alias: Option<NonZeroU16>) -> PublishPacket {
        PublishPacket {
            message: PublishMessage {
                topic: ByteString::from_static("t/1"),
                payload: Bytes::from_static(b"x"),
                properties: PublishProperties::default(),
            },
            meta: PublishPacketMeta {
                dup: false,
                retain: false,
                qos: QoS::AtMostOnce,
                packet_id: None,
            },
            wire_topic_alias: alias,
            subscription_ids: Vec::new(),
        }
    }

    #[test]
    fn validate_publish_rejects_qos_above_connack_maximum() {
        let err = validate_publish_against_broker_limits(QoS::AtLeastOnce, false, QoS::AtMostOnce, true)
            .expect_err("QoS 1 must be rejected when Maximum QoS is 0");
        assert!(matches!(err, AppError::InvalidRequest(_)));
    }

    /// [MQTT-3.2.2-14] No RETAIN publish once the broker announced Retain Available 0.
    #[test]
    fn validate_publish_rejects_retain_when_unavailable() {
        let err = validate_publish_against_broker_limits(QoS::AtMostOnce, true, QoS::AtLeastOnce, false)
            .expect_err("retain must be rejected when Retain Available is 0");
        assert!(matches!(err, AppError::InvalidRequest(_)));
    }

    #[test]
    fn validate_publish_accepts_within_broker_limits() {
        validate_publish_against_broker_limits(QoS::AtMostOnce, false, QoS::AtLeastOnce, true)
            .expect("QoS 0 retain=false within limits");
        validate_publish_against_broker_limits(QoS::AtLeastOnce, true, QoS::AtLeastOnce, true)
            .expect("QoS 1 retain=true when allowed");
    }

    #[test]
    fn inbound_publish_policy_rejects_qos2() {
        let mut packet = publish_with_alias(None);
        packet.meta.qos = QoS::ExactlyOnce;
        packet.meta.packet_id = NonZeroU16::new(1);
        let err = inbound_publish_policy(&packet).expect_err("QoS 2 must be rejected");
        assert!(matches!(err, ProtocolError::Violation(_)));
    }

    #[test]
    fn inbound_publish_policy_qos1_requires_packet_id() {
        let mut packet = publish_with_alias(None);
        packet.meta.qos = QoS::AtLeastOnce;
        let err = inbound_publish_policy(&packet).expect_err("missing packet id");
        assert!(matches!(err, ProtocolError::Violation(_)));
    }

    #[test]
    fn inbound_publish_policy_qos0_returns_none() {
        let packet = publish_with_alias(None);
        assert_eq!(inbound_publish_policy(&packet).expect("qos0"), None);
    }
}
