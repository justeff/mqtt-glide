//! Sliding-window outbound publish helper ([`PublishWindow`]).
//!
//! Wraps [`MqttClient::start_publish`](crate::MqttClient::start_publish) +
//! [`PublishCompletion`](crate::PublishCompletion) without spawning a task per message.
//! Prefer [`PublishWindow::publish_with`] for fluent topic/payload; call
//! [`PublishWindow::drain`] after the burst. For a uniform QoS/retain burst, see
//! [`MqttClient::publish_batch`](crate::MqttClient::publish_batch).
//!
//! # Example
//!
//! ```no_run
//! use std::num::NonZeroUsize;
//!
//! use mqtt_glide::{ClientBuilder, QoS};
//!
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! let client = ClientBuilder::new("mqtt://127.0.0.1:1883")?
//!     .discard_inbound()
//!     .connect()
//!     .await?;
//!
//! let mut pipe = client.publish_window(NonZeroUsize::new(64).expect("nz"));
//! for i in 0..1_000 {
//!     pipe.publish_with()
//!         .topic(format!("out/{i}"))
//!         .payload("x")
//!         .qos(QoS::AtLeastOnce)
//!         .push_retry()
//!         .await?;
//! }
//! pipe.drain().await?;
//! client.shutdown().await?;
//! # Ok(())
//! # }
//! ```

use std::collections::VecDeque;
use std::future::Future;
use std::num::NonZeroUsize;

use bytes::Bytes;
use bytestring::ByteString;

use crate::client::{MqttClient, PublishCompletion};
use crate::codec::{PublishMessage, PublishProperties, QoS};
use crate::error::AppError;
use crate::publish_builder::{PayloadSet, PayloadUnset, TopicSet, TopicUnset};

/// Bounded in-flight window over [`MqttClient::start_publish`](crate::MqttClient::start_publish).
///
/// Keeps at most `capacity` [`PublishCompletion`]s open. [`Self::push`] / [`Self::push_retry`]
/// wait on the oldest completion when full, then enqueue. After the burst, call [`Self::drain`].
///
/// Dropping without [`Self::drain`] does not cancel already-enqueued publishes (same as
/// dropping [`PublishCompletion`] without [`PublishCompletion::wait`](crate::PublishCompletion::wait)).
#[derive(Debug)]
pub struct PublishWindow<'a> {
    client: &'a MqttClient,
    in_flight: VecDeque<PublishCompletion>,
    capacity: usize,
}

impl<'a> PublishWindow<'a> {
    /// Creates a window bound to `client` with the given in-flight capacity.
    #[inline]
    pub(crate) fn new(client: &'a MqttClient, capacity: NonZeroUsize) -> Self {
        let capacity = capacity.get();
        Self {
            client,
            in_flight: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    /// Maximum number of open completions.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Number of open completions awaiting [`PublishCompletion::wait`](crate::PublishCompletion::wait).
    #[inline]
    pub fn len(&self) -> usize {
        self.in_flight.len()
    }

    /// Whether no completions are open.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.in_flight.is_empty()
    }

    /// Fluent builder that [`WindowPublishWith::push`]es / [`WindowPublishWith::push_retry`]es
    /// into this window.
    ///
    /// Default QoS is [`QoS::AtMostOnce`]; retain defaults to `false`. Topic and payload are
    /// required before [`.push`](WindowPublishWith::push) (type-state).
    #[inline]
    pub fn publish_with(&mut self) -> WindowPublishWith<'_, 'a, TopicUnset, PayloadUnset> {
        WindowPublishWith::new(self)
    }

    /// Enqueues one PUBLISH, waiting on the oldest in-flight completion when the window is full.
    ///
    /// Full command channel → [`AppError::Backpressure`] (no completion is stored). QoS 2 →
    /// [`AppError::InvalidRequest`].
    pub async fn push(&mut self, message: PublishMessage, qos: QoS, retain: bool) -> Result<(), AppError> {
        self.ensure_slot().await?;
        let completion = self.client.start_publish(message, qos, retain)?;
        self.in_flight.push_back(completion);
        Ok(())
    }

    /// Like [`Self::push`], but retries on [`AppError::Backpressure`] with
    /// [`tokio::task::yield_now`] between attempts. Other errors return immediately.
    ///
    /// Each attempt [`Clone`]s `message` so a dropped full-channel send can be retried.
    /// Prefer this for sustained pipeline bursts; use [`Self::push`] when the caller wants
    /// to handle backpressure explicitly.
    #[inline]
    pub async fn push_retry(&mut self, message: PublishMessage, qos: QoS, retain: bool) -> Result<(), AppError> {
        loop {
            match self.push(message.clone(), qos, retain).await {
                Ok(()) => return Ok(()),
                Err(AppError::Backpressure(_)) => tokio::task::yield_now().await,
                Err(error) => return Err(error),
            }
        }
    }

    /// Awaits and removes the oldest in-flight completion.
    ///
    /// Returns [`AppError::Internal`] when the window is empty.
    pub async fn wait_one(&mut self) -> Result<(), AppError> {
        let completion = self
            .in_flight
            .pop_front()
            .ok_or_else(|| AppError::Internal("publish window wait_one called with no in-flight publishes".into()))?;
        completion.wait().await
    }

    /// Awaits all remaining in-flight completions (FIFO). Consumes the window.
    ///
    /// Stops at the first error; remaining completions are dropped (enqueued publishes continue
    /// in the driver).
    pub async fn drain(mut self) -> Result<(), AppError> {
        while let Some(completion) = self.in_flight.pop_front() {
            completion.wait().await?;
        }
        Ok(())
    }

    async fn ensure_slot(&mut self) -> Result<(), AppError> {
        while self.in_flight.len() >= self.capacity {
            self.wait_one().await?;
        }
        Ok(())
    }
}

/// Fluent PUBLISH builder for [`PublishWindow::publish_with`].
///
/// Same type-state as [`crate::PublishWith`] (topic + payload required). Use
/// [`.push`](Self::push) instead of [`.send`](crate::PublishWith::send) so the message
/// enters the sliding window.
#[derive(Debug)]
pub struct WindowPublishWith<'w, 'a, T = TopicUnset, P = PayloadUnset> {
    window: &'w mut PublishWindow<'a>,
    topic: T,
    payload: P,
    qos: QoS,
    retain: bool,
    properties: PublishProperties,
}

impl<'w, 'a> WindowPublishWith<'w, 'a, TopicUnset, PayloadUnset> {
    #[inline]
    fn new(window: &'w mut PublishWindow<'a>) -> Self {
        Self {
            window,
            topic: TopicUnset,
            payload: PayloadUnset,
            qos: QoS::AtMostOnce,
            retain: false,
            properties: PublishProperties::default(),
        }
    }
}

impl<'w, 'a, T, P> WindowPublishWith<'w, 'a, T, P> {
    /// Sets the PUBLISH topic name.
    #[inline]
    pub fn topic(self, topic: impl Into<ByteString>) -> WindowPublishWith<'w, 'a, TopicSet, P> {
        WindowPublishWith {
            window: self.window,
            topic: TopicSet(topic.into()),
            payload: self.payload,
            qos: self.qos,
            retain: self.retain,
            properties: self.properties,
        }
    }

    /// Sets the PUBLISH payload via [`Into<Bytes>`] (same rules as [`crate::PublishWith::payload`]).
    #[inline]
    pub fn payload(self, payload: impl Into<Bytes>) -> WindowPublishWith<'w, 'a, T, PayloadSet> {
        WindowPublishWith {
            window: self.window,
            topic: self.topic,
            payload: PayloadSet(payload.into()),
            qos: self.qos,
            retain: self.retain,
            properties: self.properties,
        }
    }

    /// Sets the PUBLISH QoS. QoS 2 is rejected by [`PublishWindow::push`].
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

    /// Sets Message Expiry Interval in **seconds** (same rules as [`crate::PublishWith::message_expiry`]).
    #[inline]
    pub fn message_expiry(mut self, seconds: u32) -> Self {
        self.properties.message_expiry_interval = std::num::NonZeroU32::new(seconds);
        self
    }

    /// Replaces transferable MQTT 5 PUBLISH properties.
    ///
    /// **Warning:** overwrites the entire [`PublishProperties`] value, including any User
    /// Properties previously appended with [`.user_property`](Self::user_property) and
    /// [`.message_expiry`](Self::message_expiry).
    #[inline]
    pub fn properties(mut self, properties: PublishProperties) -> Self {
        self.properties = properties;
        self
    }

    /// Appends one MQTT User Property to the PUBLISH properties.
    #[inline]
    pub fn user_property(mut self, name: impl Into<ByteString>, value: impl Into<ByteString>) -> Self {
        self.properties.user_properties.push((name, value).into());
        self
    }
}

impl<'w, 'a> WindowPublishWith<'w, 'a, TopicSet, PayloadSet> {
    /// Builds a [`PublishMessage`] and [`PublishWindow::push`]es it into the window.
    pub fn push(self) -> impl Future<Output = Result<(), AppError>> + Send + 'w {
        let message = PublishMessage {
            topic: self.topic.0,
            payload: self.payload.0,
            properties: self.properties,
        };
        let qos = self.qos;
        let retain = self.retain;
        let window = self.window;
        async move { window.push(message, qos, retain).await }
    }

    /// Builds a [`PublishMessage`] and [`PublishWindow::push_retry`]es it into the window.
    pub fn push_retry(self) -> impl Future<Output = Result<(), AppError>> + Send + 'w {
        let message = PublishMessage {
            topic: self.topic.0,
            payload: self.payload.0,
            properties: self.properties,
        };
        let qos = self.qos;
        let retain = self.retain;
        let window = self.window;
        async move { window.push_retry(message, qos, retain).await }
    }
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use std::num::{NonZeroU16, NonZeroUsize};
    use std::sync::Arc;

    use bytes::Bytes;
    use bytestring::ByteString;
    use futures_util::{SinkExt, StreamExt};
    use tokio::sync::Mutex;

    use super::*;
    use crate::client::{NoReconnect, connect};
    use crate::codec::{ConnectAck, DecodedFrame, Packet, PublishAck, PublishAckReason};
    use crate::connection::NoopLifecycle;
    use crate::connection::test_support::{
        accept_connect_send_connack_with, await_connected, spawn_broker_once, static_test_credentials,
    };
    use crate::options::ConnectOptions;
    use crate::util::BrokerUrl;

    #[tokio::test]
    async fn publish_window_fluent_qos1_pipeline_roundtrip() {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let observed_broker = Arc::clone(&observed);
        let port = spawn_broker_once(move |mut framed_read, mut framed_write| async move {
            let mut ack = ConnectAck::default();
            ack.receive_max = NonZeroU16::new(16).expect("nz");
            if !accept_connect_send_connack_with(&mut framed_read, &mut framed_write, ack).await {
                return;
            }
            loop {
                match framed_read.next().await {
                    Some(Ok((DecodedFrame::Publish(packet), _))) => {
                        observed_broker.lock().await.push(packet.message.payload.clone());
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
        let options = ConnectOptions::new(url).receive_maximum(NonZeroU16::new(16).expect("nz"));
        let (tx, _rx) = flume::bounded::<PublishMessage>(4);
        let (client, connection, mut phase) =
            connect(options, static_test_credentials(), NoReconnect, tx, NoopLifecycle).expect("connect");
        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        let mut pipe = client.publish_window(NonZeroUsize::new(4).expect("nz"));
        for i in 0..10u8 {
            pipe.publish_with()
                .topic("t/window")
                .payload(Bytes::from(vec![i]))
                .qos(QoS::AtLeastOnce)
                .push_retry()
                .await
                .expect("push_retry");
        }
        pipe.drain().await.expect("drain");

        let frames = observed.lock().await.clone();
        assert_eq!(frames.len(), 10);
        for (i, payload) in frames.iter().enumerate() {
            assert_eq!(&**payload, &[i as u8]);
        }

        client.shutdown().await.expect("shutdown");
        let _ = run.await;
    }

    #[tokio::test]
    async fn publish_window_push_message_api() {
        let port = crate::connection::test_support::spawn_mock_broker().await;
        let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
        let options = ConnectOptions::new(url);
        let (tx, _rx) = flume::bounded::<PublishMessage>(4);
        let (client, connection, mut phase) =
            connect(options, static_test_credentials(), NoReconnect, tx, NoopLifecycle).expect("connect");
        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        let mut pipe = client.publish_window(NonZeroUsize::new(2).expect("nz"));
        let msg = PublishMessage {
            topic: ByteString::from_static("t/raw"),
            payload: Bytes::from_static(b"a"),
            properties: PublishProperties::default(),
        };
        pipe.push(msg, QoS::AtMostOnce, false).await.expect("push");
        assert_eq!(pipe.len(), 1);
        assert!(!pipe.is_empty());
        pipe.drain().await.expect("drain");

        client.shutdown().await.expect("shutdown");
        let _ = run.await;
    }

    #[tokio::test]
    async fn publish_batch_qos1_roundtrip() {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let observed_broker = Arc::clone(&observed);
        let port = spawn_broker_once(move |mut framed_read, mut framed_write| async move {
            let mut ack = ConnectAck::default();
            ack.receive_max = NonZeroU16::new(16).expect("nz");
            if !accept_connect_send_connack_with(&mut framed_read, &mut framed_write, ack).await {
                return;
            }
            loop {
                match framed_read.next().await {
                    Some(Ok((DecodedFrame::Publish(packet), _))) => {
                        observed_broker.lock().await.push(packet.message.payload.clone());
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
        let options = ConnectOptions::new(url).receive_maximum(NonZeroU16::new(16).expect("nz"));
        let (tx, _rx) = flume::bounded::<PublishMessage>(4);
        let (client, connection, mut phase) =
            connect(options, static_test_credentials(), NoReconnect, tx, NoopLifecycle).expect("connect");
        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        let messages = (0..5u8).map(|i| PublishMessage {
            topic: ByteString::from_static("t/batch"),
            payload: Bytes::from(vec![i]),
            properties: PublishProperties::default(),
        });
        client
            .publish_batch(messages, QoS::AtLeastOnce, false, NonZeroUsize::new(4).expect("nz"))
            .await
            .expect("publish_batch");

        let frames = observed.lock().await.clone();
        assert_eq!(frames.len(), 5);
        for (i, payload) in frames.iter().enumerate() {
            assert_eq!(&**payload, &[i as u8]);
        }

        client.shutdown().await.expect("shutdown");
        let _ = run.await;
    }
}
