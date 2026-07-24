//! Fluent [`PublishWith`] builder for [`MqttClient::publish_with`](crate::MqttClient::publish_with).
//!
//! Type-state requires [`.topic`](PublishWith::topic) and [`.payload`](PublishWith::payload)
//! before `.await` / [`.send`](PublishWith::send).
//! Default QoS is [`QoS::AtMostOnce`]; override with [`.qos`](PublishWith::qos).
//! Retain defaults to `false`.

use std::future::{Future, IntoFuture};
use std::pin::Pin;

use bytes::Bytes;
use bytestring::ByteString;

use crate::client::MqttClient;
use crate::codec::{PublishMessage, PublishProperties, QoS};
use crate::error::AppError;

/// Marker: topic not yet set on [`PublishWith`].
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct TopicUnset;

/// Marker: topic set on [`PublishWith`].
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct TopicSet(pub(crate) ByteString);

/// Marker: payload not yet set on [`PublishWith`].
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct PayloadUnset;

/// Marker: payload set on [`PublishWith`].
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct PayloadSet(pub(crate) Bytes);

/// Fluent PUBLISH builder returned by [`MqttClient::publish_with`](crate::MqttClient::publish_with).
///
/// Only [`PublishWith`] with topic + payload set implements [`.send`](PublishWith::send) /
/// [`IntoFuture`] — call `.topic(...)` and `.payload(...)` before `.await` or `.send().await`.
/// Default QoS is [`QoS::AtMostOnce`]; retain defaults to `false`.
///
/// Prefer [`.send`](PublishWith::send).await; bare `.await` uses [`IntoFuture`] and boxes the future.
///
/// # Examples
///
/// ```no_run
/// use mqtt_glide::{ClientBuilder, QoS};
///
/// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// let client = ClientBuilder::new("mqtt://127.0.0.1:1883")?
///     .discard_inbound()
///     .connect()
///     .await?;
///
/// client
///     .publish_with()
///     .topic("sensors/temp")
///     .payload("22.5")
///     .qos(QoS::AtLeastOnce)
///     .send()
///     .await?;
///
/// client.shutdown().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct PublishWith<'a, T = TopicUnset, P = PayloadUnset> {
    client: &'a MqttClient,
    topic: T,
    payload: P,
    qos: QoS,
    retain: bool,
    properties: PublishProperties,
}

impl<'a> PublishWith<'a, TopicUnset, PayloadUnset> {
    /// Creates a builder bound to `client` with QoS 0 and `retain = false`.
    #[inline]
    pub(crate) fn new(client: &'a MqttClient) -> Self {
        Self {
            client,
            topic: TopicUnset,
            payload: PayloadUnset,
            qos: QoS::AtMostOnce,
            retain: false,
            properties: PublishProperties::default(),
        }
    }
}

impl<'a, T, P> PublishWith<'a, T, P> {
    /// Sets the PUBLISH topic name.
    #[inline]
    pub fn topic(self, topic: impl Into<ByteString>) -> PublishWith<'a, TopicSet, P> {
        PublishWith {
            client: self.client,
            topic: TopicSet(topic.into()),
            payload: self.payload,
            qos: self.qos,
            retain: self.retain,
            properties: self.properties,
        }
    }

    /// Sets the PUBLISH payload via [`Into<Bytes>`].
    ///
    /// - `&'static str` / `&'static [u8]`: [`Bytes::from_static`] (no copy) — prefer
    ///   plain `"..."` when the payload is just text
    /// - [`Bytes`], `Vec<u8>`, `String`, `Box<[u8]>`: move / take ownership (no extra copy)
    /// - non-`'static` `&[u8]`: not accepted — use `Bytes::copy_from_slice(...)` or
    ///   `.to_vec()` at the call site when a copy is intentional
    ///
    /// ```no_run
    /// # use bytes::Bytes;
    /// # use mqtt_glide::MqttClient;
    /// # async fn demo(client: &MqttClient) -> Result<(), mqtt_glide::AppError> {
    /// client.publish_with().topic("t").payload("hello").send().await?;
    /// let owned = Bytes::from_static(b"hello");
    /// client.publish_with().topic("t").payload(owned).send().await?;
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    pub fn payload(self, payload: impl Into<Bytes>) -> PublishWith<'a, T, PayloadSet> {
        PublishWith {
            client: self.client,
            topic: self.topic,
            payload: PayloadSet(payload.into()),
            qos: self.qos,
            retain: self.retain,
            properties: self.properties,
        }
    }

    /// Sets the PUBLISH QoS. QoS 2 is rejected by [`MqttClient::publish`](crate::MqttClient::publish).
    ///
    /// Default is [`QoS::AtMostOnce`] when unset.
    #[inline]
    pub fn qos(mut self, qos: QoS) -> Self {
        self.qos = qos;
        self
    }

    /// Sets the RETAIN flag (default `false`).
    #[inline]
    pub fn retain(mut self, retain: bool) -> Self {
        self.retain = retain;
        self
    }

    /// Sets Message Expiry Interval in **seconds**.
    ///
    /// `0` clears expiry (property omitted). Non-zero values map to
    /// [`PublishProperties::message_expiry_interval`].
    #[inline]
    pub fn message_expiry(mut self, seconds: u32) -> Self {
        self.properties.message_expiry_interval = std::num::NonZeroU32::new(seconds);
        self
    }

    /// Replaces transferable MQTT 5 PUBLISH properties.
    ///
    /// **Warning:** this overwrites the entire [`PublishProperties`] value, including
    /// any User Properties previously appended with [`.user_property`](Self::user_property)
    /// and [`.message_expiry`](Self::message_expiry).
    /// Call `.properties(...)` first, then fluent property helpers, or include those
    /// fields in the replacement value.
    #[inline]
    pub fn properties(mut self, properties: PublishProperties) -> Self {
        self.properties = properties;
        self
    }

    /// Appends one MQTT User Property to the PUBLISH properties.
    ///
    /// Subsequent [`.properties`](Self::properties) calls replace the whole properties
    /// struct and **drop** user properties accumulated here.
    #[inline]
    pub fn user_property(mut self, key: impl Into<ByteString>, value: impl Into<ByteString>) -> Self {
        self.properties.user_properties.push((key.into(), value.into()));
        self
    }
}

impl<'a> PublishWith<'a, TopicSet, PayloadSet> {
    /// Builds a [`PublishMessage`] and returns the [`MqttClient::publish`](crate::MqttClient::publish) future.
    ///
    /// Prefer `.send().await` over awaiting the builder; bare `.await` boxes via [`IntoFuture`].
    #[inline]
    pub fn send(self) -> impl Future<Output = Result<(), AppError>> + Send + 'a {
        let message = PublishMessage {
            topic: self.topic.0,
            payload: self.payload.0,
            properties: self.properties,
        };
        self.client.publish(message, self.qos, self.retain)
    }
}

impl<'a> IntoFuture for PublishWith<'a, TopicSet, PayloadSet> {
    type Output = Result<(), AppError>;
    type IntoFuture = Pin<Box<dyn Future<Output = Result<(), AppError>> + Send + 'a>>;

    #[inline]
    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.send())
    }
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use std::num::NonZeroU16;
    use std::sync::Arc;

    use bytes::Bytes;
    use futures_util::{SinkExt, StreamExt};
    use tokio::sync::Mutex;

    use crate::client::{NoReconnect, connect};
    use crate::codec::{ConnectAck, DecodedFrame, Packet, PublishAck, PublishAckReason, PublishMessage, QoS};
    use crate::connection::NoopLifecycle;
    use crate::connection::test_support::{
        accept_connect_send_connack_with, await_connected, spawn_broker_once, static_test_credentials,
    };
    use crate::options::ConnectOptions;
    use crate::util::BrokerUrl;

    /// `publish_with().topic().payload().qos(AtLeastOnce).send().await` roundtrip.
    #[tokio::test]
    async fn publish_with_qos_at_least_once_roundtrip() {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let observed_broker = Arc::clone(&observed);
        let port = spawn_broker_once(move |mut framed_read, mut framed_write| async move {
            let mut ack = ConnectAck::default();
            ack.receive_max = NonZeroU16::new(8).expect("nz");
            if !accept_connect_send_connack_with(&mut framed_read, &mut framed_write, ack).await {
                return;
            }
            loop {
                match framed_read.next().await {
                    Some(Ok((DecodedFrame::Publish(packet), _))) => {
                        observed_broker.lock().await.push((
                            packet.meta.qos,
                            packet.meta.packet_id,
                            packet.message.payload.clone(),
                        ));
                        if packet.meta.qos == QoS::AtLeastOnce {
                            let Some(id) = packet.meta.packet_id else {
                                continue;
                            };
                            let _ = framed_write
                                .send(Packet::PublishAck(PublishAck {
                                    packet_id: id,
                                    reason_code: PublishAckReason::Success,
                                    properties: Vec::new(),
                                    reason_string: None,
                                }))
                                .await;
                        }
                    }
                    Some(Ok((DecodedFrame::Control(Packet::Disconnect(_)), _))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(_)) => break,
                }
            }
        })
        .await;

        let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
        let options = ConnectOptions::new(url).receive_maximum(NonZeroU16::new(8).expect("nz"));
        let (tx, _rx) = flume::bounded::<PublishMessage>(4);
        let (client, connection, mut phase) =
            connect(options, static_test_credentials(), NoReconnect, tx, NoopLifecycle).expect("connect");
        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        client
            .publish_with()
            .topic("t/publish-with")
            .payload("fluent-qos1")
            .qos(QoS::AtLeastOnce)
            .send()
            .await
            .expect("publish_with qos1");

        client
            .publish_with()
            .topic("t/publish-bytes")
            .payload(Bytes::from_static(b"owned"))
            .qos(QoS::AtLeastOnce)
            .send()
            .await
            .expect("payload Bytes move");

        // Bare `.await` uses `IntoFuture` (boxes); prefer `.send().await`.
        client
            .publish_with()
            .topic("t/into-future")
            .payload("via-await")
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
