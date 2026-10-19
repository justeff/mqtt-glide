//! What the broker said in its acknowledgements: reason codes plus the optional
//! human-readable Reason String and User Properties (MQTT 5 §3.4.2.2, §3.9.2.1, §3.11.2.1).

use std::fmt;
use std::ops::Deref;

use bytestring::ByteString;

use crate::codec::{PublishAckReason, SubscribeAckReason, UnsubscribeAckReason, UserProperties, UserProperty};
use crate::error::AppError;

/// A QoS 1 PUBLISH the broker refused (PUBACK reason code 0x80 or higher), carried by
/// [`AppError::PublishRejected`](crate::AppError::PublishRejected).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
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

/// What the broker answered to a publish that completed: the PUBACK of a QoS 1 publish
/// (MQTT 5 §3.4.2), or nothing for QoS 0, which has no acknowledgement.
///
/// Read through methods: the Reason String and User Properties are kept out of line, so a
/// PUBACK without them (the common case) costs no allocation and the value stays small.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishResponse {
    reason: Option<PublishAckReason>,
    resent: bool,
    details: Option<Box<AckDetails>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AckDetails {
    reason_string: Option<ByteString>,
    user_properties: UserProperties,
}

impl PublishResponse {
    /// A QoS 0 publish: written, no acknowledgement exists.
    pub(crate) fn written() -> Self {
        Self {
            reason: None,
            resent: false,
            details: None,
        }
    }

    /// A QoS 1 publish the broker accepted.
    pub(crate) fn acked(
        reason: PublishAckReason,
        reason_string: Option<ByteString>,
        user_properties: UserProperties,
        resent: bool,
    ) -> Self {
        let details = (reason_string.is_some() || !user_properties.is_empty()).then(|| {
            Box::new(AckDetails {
                reason_string,
                user_properties,
            })
        });
        Self {
            reason: Some(reason),
            resent,
            details,
        }
    }

    /// PUBACK reason code: [`PublishAckReason::Success`] or
    /// [`PublishAckReason::NoMatchingSubscribers`] (codes 0x80 and above fail the publish with
    /// [`AppError::PublishRejected`](crate::AppError::PublishRejected)). `None` for QoS 0,
    /// which completes once written.
    pub fn reason(&self) -> Option<PublishAckReason> {
        self.reason
    }

    /// Broker's diagnostic text, if it sent one (not meant to be parsed, §3.4.2.2.2).
    pub fn reason_string(&self) -> Option<&str> {
        self.details.as_ref()?.reason_string.as_deref()
    }

    /// User Properties of the PUBACK.
    pub fn user_properties(&self) -> &[UserProperty] {
        self.details.as_ref().map_or(&[], |details| &details.user_properties)
    }

    /// The PUBACK answers a resend (DUP) after a reconnect: the first transmission may also
    /// have reached the broker, so subscribers may get the message twice. `false` does not
    /// rule out duplicates further down the line (the broker's own resends to subscribers).
    pub fn resent(&self) -> bool {
        self.resent
    }
}

/// Whether a failed publish can have reached the broker, carried by [`PublishError`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Delivery {
    /// Never written to a connection: the broker does not have it, and publishing it again
    /// does not duplicate it.
    NotSent,
    /// Written, but no PUBACK arrived (connection lost, session ended, timeout after the
    /// write): the broker may or may not have it. A QoS 1 publish of a resumed session is
    /// still resent by the client; publishing it again yourself may duplicate it. Also every
    /// QoS 0 timeout: QoS 0 keeps no write state, so the client cannot tell whether it was
    /// written (one that was not is not written afterwards).
    Unknown,
    /// The broker refused it ([`AppError::PublishRejected`](crate::AppError::PublishRejected)):
    /// it is not delivered.
    Rejected,
}

/// A publish that did not complete: the cause plus whether the broker can have it.
///
/// Converts into [`AppError`](crate::AppError) (`?` in a function returning `AppError`), so
/// callers that do not care about [`Delivery`] are unaffected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{error}")]
#[non_exhaustive]
pub struct PublishError {
    /// Why the publish failed.
    pub error: AppError,
    /// Whether the broker can have the message.
    pub delivery: Delivery,
}

impl PublishError {
    /// A publish that never reached a connection.
    pub(crate) fn not_sent(error: AppError) -> Self {
        Self {
            error,
            delivery: Delivery::NotSent,
        }
    }

    /// Whether the broker may have accepted the message ([`Delivery::Unknown`]): if so,
    /// publishing it again may deliver it twice.
    pub fn may_have_been_delivered(&self) -> bool {
        self.delivery == Delivery::Unknown
    }
}

impl From<PublishError> for AppError {
    fn from(error: PublishError) -> Self {
        error.error
    }
}

/// A subscription the broker refused to restore after a reconnect, as listed by
/// [`MqttClient::refused_subscriptions`](crate::MqttClient::refused_subscriptions).
///
/// The client keeps asking for it on the same connection, with backoff, until the broker
/// grants it or the application unsubscribes.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct RefusedSubscription {
    /// The topic filter.
    pub filter: ByteString,
    /// SUBACK reason code of the last refusal, e.g. [`SubscribeAckReason::NotAuthorized`].
    pub reason: SubscribeAckReason,
    /// Broker's diagnostic text of the last refusal, if it sent one.
    pub reason_string: Option<ByteString>,
}

/// Generates the SUBACK / UNSUBACK response types: per-filter reason codes plus the
/// packet's Reason String and User Properties. Derefs to the reason codes, so it reads
/// like the plain list (`resp[0]`, `resp.iter()`, `resp == vec![…]`).
macro_rules! ack_response {
    ($(#[$doc:meta])* $name:ident, $reason:ty) => {
        $(#[$doc])*
        #[derive(Debug, Clone, PartialEq, Eq)]
        #[non_exhaustive]
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
