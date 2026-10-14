//! What the broker said in its acknowledgements: reason codes plus the optional
//! human-readable Reason String and User Properties (MQTT 5 §3.4.2.2, §3.9.2.1, §3.11.2.1).

use std::fmt;
use std::ops::Deref;

use bytestring::ByteString;

use crate::codec::{PublishAckReason, SubscribeAckReason, UnsubscribeAckReason, UserProperties};

/// A QoS 1 PUBLISH the broker refused (PUBACK reason code 0x80 or higher), carried by
/// [`AppError::PublishRejected`](crate::AppError::PublishRejected).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishRejection {
    /// PUBACK reason code, e.g. [`PublishAckReason::NotAuthorized`] or `QuotaExceeded`.
    pub reason: PublishAckReason,
    /// Broker's diagnostic text, if it sent one (not meant to be parsed).
    pub reason_string: Option<ByteString>,
    /// User Properties of the PUBACK.
    pub user_properties: UserProperties,
}

impl fmt::Display for PublishRejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.reason)?;
        if let Some(reason_string) = &self.reason_string {
            write!(f, " ({reason_string})")?;
        }
        Ok(())
    }
}

/// Generates the SUBACK / UNSUBACK response types: per-filter reason codes plus the
/// packet's Reason String and User Properties. Derefs to the reason codes, so it reads
/// like the plain list (`resp[0]`, `resp.iter()`, `resp == vec![…]`).
macro_rules! ack_response {
    ($(#[$doc:meta])* $name:ident, $reason:ty) => {
        $(#[$doc])*
        #[derive(Debug, Clone, PartialEq, Eq)]
        pub struct $name {
            /// One reason code per topic filter, in request order.
            pub reasons: Vec<$reason>,
            /// Broker's diagnostic text, if it sent one (not meant to be parsed).
            pub reason_string: Option<ByteString>,
            /// User Properties of the acknowledgement.
            pub user_properties: UserProperties,
        }

        impl $name {
            /// The per-filter reason codes (same as `&*self`), like `Vec::as_slice`.
            pub fn as_slice(&self) -> &[$reason] {
                &self.reasons
            }
        }

        impl Deref for $name {
            type Target = [$reason];

            fn deref(&self) -> &[$reason] {
                &self.reasons
            }
        }

        impl IntoIterator for $name {
            type Item = $reason;
            type IntoIter = std::vec::IntoIter<$reason>;

            fn into_iter(self) -> Self::IntoIter {
                self.reasons.into_iter()
            }
        }

        impl<'a> IntoIterator for &'a $name {
            type Item = &'a $reason;
            type IntoIter = std::slice::Iter<'a, $reason>;

            fn into_iter(self) -> Self::IntoIter {
                self.reasons.iter()
            }
        }

        impl PartialEq<Vec<$reason>> for $name {
            fn eq(&self, other: &Vec<$reason>) -> bool {
                &self.reasons == other
            }
        }
    };
}

ack_response!(
    /// Result of [`MqttClient::subscribe`](crate::MqttClient::subscribe): the SUBACK.
    SubscribeResponse,
    SubscribeAckReason
);

ack_response!(
    /// Result of [`MqttClient::unsubscribe`](crate::MqttClient::unsubscribe): the UNSUBACK.
    UnsubscribeResponse,
    UnsubscribeAckReason
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscribe_response_reads_like_the_reason_list() {
        let response = SubscribeResponse {
            reasons: vec![SubscribeAckReason::GrantedQos1, SubscribeAckReason::NotAuthorized],
            reason_string: Some("ACL denied for b".into()),
            user_properties: Vec::new(),
        };
        assert_eq!(
            response,
            vec![SubscribeAckReason::GrantedQos1, SubscribeAckReason::NotAuthorized]
        );
        assert_eq!(response[1], SubscribeAckReason::NotAuthorized);
        assert_eq!(response.iter().count(), 2);
        assert_eq!(response.reason_string.as_deref(), Some("ACL denied for b"));
    }

    #[test]
    fn publish_rejection_display_includes_reason_string() {
        let rejection = PublishRejection {
            reason: PublishAckReason::QuotaExceeded,
            reason_string: Some("too many messages".into()),
            user_properties: Vec::new(),
        };
        assert_eq!(rejection.to_string(), "QuotaExceeded (too many messages)");
    }
}
