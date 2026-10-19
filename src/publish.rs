//! Outbound PUBLISH: the owned [`Publish`] value and its builder.

use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::time::Duration;

use bytes::Bytes;
use bytestring::ByteString;

use crate::ack::{PublishError, PublishResponse};
use crate::client::{MqttClient, PublishCompletion};
use crate::codec::{InboundMessage, PublishMessage, PublishProperties, QoS};

/// One outbound PUBLISH: topic, payload, QoS, retain flag and MQTT 5 properties.
///
/// Build with [`Publish::new`] and the chained setters; defaults are QoS 0,
/// retain `false` and no properties. Converts from a [`PublishMessage`] (for example to
/// republish an inbound message) with the same defaults.
///
/// Built once, it can be handed to [`MqttClient::send`], [`MqttClient::try_start_publish`],
/// [`PublishWindow::push`](crate::PublishWindow::push) or [`MqttClient::publish_batch`];
/// [`MqttClient::publish`] returns the fluent [`PublishBuilder`] instead.
///
/// # Examples
///
/// ```no_run
/// use mqtt_glide::{MqttClient, Publish, QoS};
///
/// # async fn demo(client: MqttClient) -> Result<(), mqtt_glide::AppError> {
/// // Fluent, awaited directly:
/// client.publish("sensors/temp", "22.5").qos(QoS::AtLeastOnce).retain(true).await?;
///
/// // Or build the value first and send it later:
/// let status = Publish::new("status", "online").qos(QoS::AtLeastOnce).retain(true);
/// client.send(status).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Publish {
    pub(crate) message: PublishMessage,
    pub(crate) qos: QoS,
    pub(crate) retain: bool,
}

/// Setters shared by [`Publish`] and [`PublishBuilder`] (the builder forwards to its value).
macro_rules! publish_setters {
    () => {
        /// Sets the QoS (default [`QoS::AtMostOnce`]). QoS 2 is rejected when sent
        /// ([`AppError::InvalidRequest`](crate::AppError::InvalidRequest)).
        #[inline]
        pub fn qos(mut self, qos: QoS) -> Self {
            self.inner_mut().qos = qos;
            self
        }

        /// Sets the RETAIN flag (default `false`).
        #[inline]
        pub fn retain(mut self, retain: bool) -> Self {
            self.inner_mut().retain = retain;
            self
        }

        /// Appends one MQTT User Property.
        #[inline]
        pub fn user_property(mut self, name: impl Into<ByteString>, value: impl Into<ByteString>) -> Self {
            self.inner_mut()
                .message
                .properties
                .user_properties
                .push((name, value).into());
            self
        }

        /// Appends MQTT User Properties from any iterator of `(name, value)` pairs.
        #[inline]
        pub fn user_properties<K, V>(mut self, properties: impl IntoIterator<Item = (K, V)>) -> Self
        where
            K: Into<ByteString>,
            V: Into<ByteString>,
        {
            self.inner_mut()
                .message
                .properties
                .user_properties
                .extend(properties.into_iter().map(Into::into));
            self
        }

        /// Sets Message Expiry Interval in **seconds** (§3.3.2.3.3); `0` omits the property.
        #[inline]
        pub fn message_expiry(mut self, seconds: u32) -> Self {
            self.inner_mut().message.properties.message_expiry_interval = (seconds != 0).then_some(seconds);
            self
        }

        /// Sets Content Type (§3.3.2.3.9).
        #[inline]
        pub fn content_type(mut self, content_type: impl Into<ByteString>) -> Self {
            self.inner_mut().message.properties.content_type = Some(content_type.into());
            self
        }

        /// Sets Response Topic (§3.3.2.3.5) for request/response.
        #[inline]
        pub fn response_topic(mut self, response_topic: impl Into<ByteString>) -> Self {
            self.inner_mut().message.properties.response_topic = Some(response_topic.into());
            self
        }

        /// Sets Correlation Data (§3.3.2.3.6) for request/response.
        #[inline]
        pub fn correlation_data(mut self, data: impl Into<Bytes>) -> Self {
            self.inner_mut().message.properties.correlation_data = Some(data.into());
            self
        }

        /// Replaces all MQTT 5 properties, including User Properties and expiry set earlier.
        #[inline]
        pub fn properties(mut self, properties: PublishProperties) -> Self {
            self.inner_mut().message.properties = properties.into();
            self
        }
    };
}

impl Publish {
    /// Builds a QoS 0, non-retained PUBLISH without properties.
    ///
    /// `topic` accepts any [`Into<ByteString>`] (`&str`, `String`, [`ByteString`]).
    /// `payload` accepts any [`Into<Bytes>`]: `&'static str` / `&'static [u8]` without a copy,
    /// and [`Bytes`], `Vec<u8>`, `String`, `Box<[u8]>` by move. Borrowed non-`'static` bytes
    /// are not accepted, so a copy is always visible at the call site (`.to_vec()`,
    /// `Bytes::copy_from_slice`, `.to_owned()`).
    #[inline]
    pub fn new(topic: impl Into<ByteString>, payload: impl Into<Bytes>) -> Self {
        Self {
            message: PublishMessage::new(topic, payload),
            qos: QoS::AtMostOnce,
            retain: false,
        }
    }

    publish_setters!();

    #[inline]
    fn inner_mut(&mut self) -> &mut Publish {
        self
    }

    /// Topic name.
    #[inline]
    pub fn topic(&self) -> &ByteString {
        &self.message.topic
    }

    /// Payload bytes.
    #[inline]
    pub fn payload(&self) -> &Bytes {
        &self.message.payload
    }

    /// QoS this PUBLISH is sent with.
    #[inline]
    pub fn qos_level(&self) -> QoS {
        self.qos
    }

    /// Whether the RETAIN flag is set.
    #[inline]
    pub fn is_retained(&self) -> bool {
        self.retain
    }

    /// MQTT 5 properties.
    #[inline]
    pub fn publish_properties(&self) -> &PublishProperties {
        &self.message.properties
    }

    /// Splits into the wire message, QoS and retain flag.
    #[inline]
    pub fn into_parts(self) -> (PublishMessage, QoS, bool) {
        (self.message, self.qos, self.retain)
    }
}

impl Publish {
    /// The response to a request (§4.10): published to the request's Response Topic, with its
    /// Correlation Data copied, QoS 0 unless set. `None` when `request` carries no Response
    /// Topic (it does not expect an answer).
    pub fn reply_to(request: &InboundMessage, payload: impl Into<Bytes>) -> Option<Self> {
        let topic = request.properties.response_topic.clone()?;
        let mut reply = Self::new(topic, payload);
        if let Some(correlation) = request.properties.correlation_data.clone() {
            reply = reply.correlation_data(correlation);
        }
        Some(reply)
    }
}

impl From<InboundMessage> for Publish {
    /// Republish an inbound message: topic, payload and properties are kept; QoS 0 and
    /// retain `false` like [`Publish::new`] (set them explicitly to forward the flags).
    #[inline]
    fn from(message: InboundMessage) -> Self {
        Self::from(message.into_message())
    }
}

impl From<PublishMessage> for Publish {
    /// QoS 0, retain `false`; topic, payload and properties are kept.
    #[inline]
    fn from(message: PublishMessage) -> Self {
        Self {
            message,
            qos: QoS::AtMostOnce,
            retain: false,
        }
    }
}

/// Fluent PUBLISH returned by [`MqttClient::publish`](crate::MqttClient::publish).
///
/// Chain the same setters as [`Publish`], then `.await` it (or [`.send()`](Self::send)).
/// Awaiting waits for room in the command channel, then for completion: encode into the
/// write buffer for QoS 0, the matching `PUBACK` for QoS 1. Both waits are bounded by
/// [`ConnectOptions::ack_timeout`](crate::ConnectOptions::ack_timeout).
/// [`.start()`](Self::start) waits only for room in the command channel and returns a
/// [`PublishCompletion`]; [`.try_start()`](Self::try_start) does not wait at all.
///
/// Bare `.await` boxes the future ([`IntoFuture`]); `.send().await` does not.
#[derive(Debug)]
#[must_use = "a PUBLISH does nothing until it is awaited, sent or started"]
pub struct PublishBuilder<'a> {
    client: &'a MqttClient,
    publish: Publish,
    /// `None`: the client's [`ConnectOptions::ack_timeout`](crate::ConnectOptions::ack_timeout).
    ack_timeout: Option<Option<Duration>>,
}

impl<'a> PublishBuilder<'a> {
    #[inline]
    pub(crate) fn new(client: &'a MqttClient, publish: Publish) -> Self {
        Self {
            client,
            publish,
            ack_timeout: None,
        }
    }

    publish_setters!();

    #[inline]
    fn inner_mut(&mut self) -> &mut Publish {
        &mut self.publish
    }

    /// How long to wait for completion (the PUBACK for QoS 1) instead of
    /// [`ConnectOptions::ack_timeout`](crate::ConnectOptions::ack_timeout); `None` waits until
    /// the PUBACK arrives or the publish fails for good. Waiting for room in the command channel
    /// stays bounded by the client's timeout.
    #[inline]
    pub fn ack_timeout(mut self, ack_timeout: Option<Duration>) -> Self {
        self.ack_timeout = Some(ack_timeout);
        self
    }

    #[inline]
    fn effective_ack_timeout(&self) -> Option<Duration> {
        self.ack_timeout.unwrap_or(Some(self.client.ack_timeout))
    }

    /// Sends and waits for completion (same as `.await`, without boxing).
    #[inline]
    pub fn send(self) -> impl Future<Output = Result<PublishResponse, PublishError>> + Send + 'a {
        let ack_timeout = self.effective_ack_timeout();
        self.client.send_with(self.publish, ack_timeout)
    }

    /// Enqueues, waiting for room in the command channel, and returns the completion without
    /// waiting for it; see [`MqttClient::start_publish`](crate::MqttClient::start_publish).
    /// An error here is always [`Delivery::NotSent`](crate::Delivery::NotSent).
    #[inline]
    pub fn start(self) -> impl Future<Output = Result<PublishCompletion, PublishError>> + Send + 'a {
        let ack_timeout = self.effective_ack_timeout();
        self.client.start_publish_with(self.publish, ack_timeout)
    }

    /// Enqueues without waiting at all; see
    /// [`MqttClient::try_start_publish`](crate::MqttClient::try_start_publish).
    /// An error here is always [`Delivery::NotSent`](crate::Delivery::NotSent).
    #[inline]
    pub fn try_start(self) -> Result<PublishCompletion, PublishError> {
        let ack_timeout = self.effective_ack_timeout();
        self.client.try_start_publish_with(self.publish, ack_timeout)
    }

    /// Returns the built [`Publish`] without sending it.
    #[inline]
    pub fn into_publish(self) -> Publish {
        self.publish
    }
}

impl<'a> IntoFuture for PublishBuilder<'a> {
    type Output = Result<PublishResponse, PublishError>;
    type IntoFuture = Pin<Box<dyn Future<Output = Result<PublishResponse, PublishError>> + Send + 'a>>;

    #[inline]
    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.send())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::num::NonZeroU16;

    use crate::test_util::{options_for, qos_id_payload, spawn_puback_broker, start};

    #[test]
    fn publish_defaults_and_setters() {
        let p = Publish::new("t/a", "x");
        assert_eq!(p.qos_level(), QoS::AtMostOnce);
        assert!(!p.is_retained());
        assert!(p.publish_properties().is_wire_empty());

        let p = p
            .qos(QoS::AtLeastOnce)
            .retain(true)
            .user_property("a", "1")
            .user_properties([("b", "2"), ("c", "3")])
            .message_expiry(30)
            .content_type("text/plain")
            .response_topic("reply/t")
            .correlation_data("id-1");
        assert_eq!(p.qos_level(), QoS::AtLeastOnce);
        assert!(p.is_retained());
        let names: Vec<&str> = p
            .publish_properties()
            .user_properties
            .iter()
            .map(|u| &*u.name)
            .collect();
        assert_eq!(names, ["a", "b", "c"]);
        assert_eq!(p.publish_properties().message_expiry_interval, Some(30));
        assert_eq!(p.publish_properties().content_type.as_deref(), Some("text/plain"));
        assert_eq!(p.publish_properties().response_topic.as_deref(), Some("reply/t"));
        assert_eq!(p.publish_properties().correlation_data.as_deref(), Some(&b"id-1"[..]));
    }

    #[test]
    fn properties_replaces_everything_set_before() {
        let p = Publish::new("t", "x")
            .user_property("a", "1")
            .properties(PublishProperties::default().content_type("c"));
        assert!(p.publish_properties().user_properties.is_empty());
        assert_eq!(p.publish_properties().content_type.as_deref(), Some("c"));
    }

    #[test]
    fn from_publish_message_keeps_properties() {
        let message = PublishMessage::new("in/t", "payload").properties(PublishProperties::default().content_type("c"));
        let p = Publish::from(message.clone());
        assert_eq!(p.qos_level(), QoS::AtMostOnce);
        let (back, qos, retain) = p.into_parts();
        assert_eq!(back, message);
        assert_eq!(qos, QoS::AtMostOnce);
        assert!(!retain);
    }

    /// `publish(topic, payload).qos(AtLeastOnce)` via `.send().await`, bare `.await` and a `Bytes` payload.
    #[tokio::test]
    async fn publish_with_qos_at_least_once_roundtrip() {
        let (port, observed) = spawn_puback_broker(NonZeroU16::new(8).expect("nz"), qos_id_payload).await;
        let (client, _rx, run) = start(options_for(port).receive_maximum(NonZeroU16::new(8).expect("nz"))).await;

        client
            .publish("t/publish-with", "fluent-qos1")
            .qos(QoS::AtLeastOnce)
            .send()
            .await
            .expect("publish qos1");

        client
            .publish("t/publish-bytes", Bytes::from_static(b"owned"))
            .qos(QoS::AtLeastOnce)
            .send()
            .await
            .expect("payload Bytes move");

        // Bare `.await` uses `IntoFuture` (boxes); prefer `.send().await`.
        client
            .publish("t/into-future", "via-await")
            .qos(QoS::AtLeastOnce)
            .await
            .expect("into_future bare await");

        let frames = observed.lock().await.clone();
        assert_eq!(frames.len(), 3);
        assert_eq!(frames[0].0, QoS::AtLeastOnce);
        assert!(frames[0].1.is_some());
        assert_eq!(&*frames[0].2, b"fluent-qos1");
        assert_eq!(&*frames[1].2, b"owned");
        assert_eq!(&*frames[2].2, b"via-await");

        client.shutdown().await.expect("shutdown");
        let _ = run.await;
    }
}
