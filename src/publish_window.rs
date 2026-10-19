//! Sliding-window outbound publish helper ([`PublishWindow`]).

use std::collections::VecDeque;
use std::num::NonZeroUsize;

use crate::ack::{PublishError, PublishResponse};
use crate::client::{MqttClient, PublishCompletion};
use crate::error::AppError;
use crate::publish::Publish;

/// Bounded in-flight window over [`MqttClient::start_publish`](crate::MqttClient::start_publish).
///
/// Keeps at most `capacity` [`PublishCompletion`]s open. [`Self::push`] waits on the oldest
/// completion when full, then enqueues (waiting for command-channel room). After the
/// burst, call [`Self::drain`]. The window may be wider than the broker's Receive
/// Maximum: QoS 1 publishes beyond it wait inside the driver for a free slot.
///
/// Dropping without [`Self::drain`] discards the open completions (same as dropping
/// [`PublishCompletion`] without [`PublishCompletion::wait`](crate::PublishCompletion::wait)):
/// publishes the write task has already encoded proceed, still-queued ones may be skipped.
///
/// # Example
///
/// ```no_run
/// use std::num::NonZeroUsize;
///
/// use mqtt_glide::{ClientBuilder, Publish, QoS};
///
/// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// let client = ClientBuilder::new("mqtt://127.0.0.1:1883")?
///     .discard_inbound()
///     .connect()
///     .await?;
///
/// let mut pipe = client.publish_window(NonZeroUsize::new(64).expect("nz"));
/// for i in 0..1_000 {
///     pipe.push(Publish::new(format!("out/{i}"), "x").qos(QoS::AtLeastOnce)).await?;
/// }
/// pipe.drain().await?;
/// client.shutdown().await?;
/// # Ok(())
/// # }
/// ```
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

    /// Enqueues one PUBLISH, waiting on the oldest in-flight completion when the window is full.
    ///
    /// Accepts a [`Publish`] (or a [`PublishMessage`](crate::PublishMessage) as QoS 0).
    /// Errors from the oldest completion surface here; QoS 2 → [`AppError::InvalidRequest`];
    /// no command-channel room within the ack timeout → [`AppError::Backpressure`].
    ///
    /// On an error, `publish` was not enqueued. The error's
    /// [`delivery`](crate::PublishError::delivery) describes the publish that failed, which
    /// can be an earlier one waited on to make room. Where each message's outcome matters, start
    /// the publishes yourself ([`PublishBuilder::start`](crate::PublishBuilder::start)) and wait
    /// on each completion.
    pub async fn push(&mut self, publish: impl Into<Publish>) -> Result<(), PublishError> {
        let publish = publish.into();
        self.ensure_slot().await?;
        let completion = self.client.start_publish(publish).await?;
        self.in_flight.push_back(completion);
        Ok(())
    }

    /// Awaits and removes the oldest in-flight completion.
    ///
    /// Returns [`AppError::Internal`] when the window is empty.
    pub async fn wait_one(&mut self) -> Result<PublishResponse, PublishError> {
        let completion = self.in_flight.pop_front().ok_or_else(|| {
            PublishError::not_sent(AppError::Internal(
                "publish window wait_one called with no in-flight publishes".into(),
            ))
        })?;
        completion.wait().await
    }

    /// Awaits all remaining in-flight completions (FIFO). Consumes the window.
    ///
    /// Stops at the first error; remaining completions are dropped (enqueued publishes continue
    /// in the driver), so the error says nothing about the publishes after the failed one.
    pub async fn drain(mut self) -> Result<(), PublishError> {
        while let Some(completion) = self.in_flight.pop_front() {
            completion.wait().await?;
        }
        Ok(())
    }

    async fn ensure_slot(&mut self) -> Result<(), PublishError> {
        while self.in_flight.len() >= self.capacity {
            self.wait_one().await?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use std::num::{NonZeroU16, NonZeroUsize};

    use bytes::Bytes;
    use futures_util::{SinkExt, StreamExt};

    use super::*;
    use crate::codec::{ConnectAck, DecodedFrame, Packet, QoS};
    use crate::connection::test_support::{accept_connect_send_connack_with, spawn_broker_once};
    use crate::test_util::{options_for, puback, spawn_puback_broker, start};

    #[tokio::test]
    async fn publish_window_fluent_qos1_pipeline_roundtrip() {
        let (port, observed) = spawn_puback_broker(NonZeroU16::new(16).expect("nz"), |packet| {
            packet.message.payload.clone()
        })
        .await;
        let (client, _rx, run) = start(options_for(port).receive_maximum(NonZeroU16::new(16).expect("nz"))).await;

        let mut pipe = client.publish_window(NonZeroUsize::new(4).expect("nz"));
        for i in 0..10u8 {
            pipe.push(Publish::new("t/window", Bytes::from(vec![i])).qos(QoS::AtLeastOnce))
                .await
                .expect("push");
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

    /// Window wider than the broker's Receive Maximum must wait for PUBACKs, not fail.
    /// (Default mosquitto advertises 20; examples use a window of 64.)
    #[tokio::test]
    async fn publish_window_wider_than_server_receive_maximum() {
        let port = spawn_broker_once(move |mut framed_read, mut framed_write| async move {
            let mut ack = ConnectAck::default();
            ack.receive_max = NonZeroU16::new(2).expect("nz");
            if !accept_connect_send_connack_with(&mut framed_read, &mut framed_write, ack).await {
                return;
            }
            loop {
                match framed_read.next().await {
                    Some(Ok((DecodedFrame::Publish(packet), _))) => {
                        let Some(id) = packet.meta.packet_id else {
                            continue;
                        };
                        // Delay so several publishes are queued while the window is full.
                        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                        let _ = framed_write.send(puback(id)).await;
                    }
                    Some(Ok((DecodedFrame::Control(Packet::Disconnect(_)), _))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(_)) => break,
                }
            }
        })
        .await;

        let (client, _rx, run) = start(options_for(port)).await;

        let mut pipe = client.publish_window(NonZeroUsize::new(8).expect("nz"));
        for i in 0..20u8 {
            pipe.push(Publish::new("t/window-rm", Bytes::from(vec![i])).qos(QoS::AtLeastOnce))
                .await
                .expect("push must not fail on broker Receive Maximum");
        }
        pipe.drain()
            .await
            .expect("drain must not fail on broker Receive Maximum");

        client.shutdown().await.expect("shutdown");
        let _ = run.await;
    }

    #[tokio::test]
    async fn publish_window_push_message_api() {
        let port = crate::connection::test_support::spawn_mock_broker().await;
        let (client, _rx, run) = start(options_for(port)).await;

        let mut pipe = client.publish_window(NonZeroUsize::new(2).expect("nz"));
        pipe.push(Publish::new("t/raw", Bytes::from_static(b"a")))
            .await
            .expect("push");
        assert_eq!(pipe.len(), 1);
        assert!(!pipe.is_empty());
        pipe.drain().await.expect("drain");

        client.shutdown().await.expect("shutdown");
        let _ = run.await;
    }

    #[tokio::test]
    async fn publish_batch_qos1_roundtrip() {
        let (port, observed) = spawn_puback_broker(NonZeroU16::new(16).expect("nz"), |packet| {
            packet.message.payload.clone()
        })
        .await;
        let (client, _rx, run) = start(options_for(port).receive_maximum(NonZeroU16::new(16).expect("nz"))).await;

        let messages = (0..5u8).map(|i| Publish::new("t/batch", Bytes::from(vec![i])).qos(QoS::AtLeastOnce));
        client
            .publish_batch(messages, NonZeroUsize::new(4).expect("nz"))
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
