//! MQTT 5 request / response (§4.10): [`Requester`] on the asking side,
//! [`Publish::reply_to`](crate::Publish::reply_to) on the answering side.

use std::collections::HashMap;
use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use bytestring::ByteString;
use tokio::sync::oneshot;

use crate::client::MqttClient;
use crate::codec::{InboundMessage, QoS};
use crate::error::AppError;
use crate::publish::Publish;

/// How long [`RequestBuilder`] waits for the response unless [`RequestBuilder::timeout`] says
/// otherwise.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Routes responses (messages on a requester's response topic with Correlation Data) to the
/// request waiting for them. Shared by the client handles and the driver's read path.
#[derive(Debug, Default)]
pub(crate) struct ResponseRouter {
    /// Fast path: no requester exists, nothing to route.
    active: AtomicBool,
    state: std::sync::Mutex<RouterState>,
}

#[derive(Debug, Default)]
struct RouterState {
    /// Response topics, with how many requesters use each.
    topics: HashMap<ByteString, usize>,
    pending: HashMap<Bytes, oneshot::Sender<InboundMessage>>,
}

impl ResponseRouter {
    fn lock(&self) -> std::sync::MutexGuard<'_, RouterState> {
        self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn add_topic(&self, topic: ByteString) {
        *self.lock().topics.entry(topic).or_default() += 1;
        self.active.store(true, Ordering::Release);
    }

    /// Drops one requester's use of `topic`; returns whether it was the last one.
    fn remove_topic(&self, topic: &ByteString) -> bool {
        let mut state = self.lock();
        let last = match state.topics.get_mut(topic) {
            Some(users) if *users > 1 => {
                *users -= 1;
                false
            }
            Some(_) => {
                state.topics.remove(topic);
                true
            }
            None => false,
        };
        self.active.store(!state.topics.is_empty(), Ordering::Release);
        last
    }

    fn register(&self, correlation: Bytes, reply: oneshot::Sender<InboundMessage>) {
        self.lock().pending.insert(correlation, reply);
    }

    fn forget(&self, correlation: &Bytes) {
        self.lock().pending.remove(correlation);
    }

    /// Takes `message` if it is a response on a requester topic (handing it to its request,
    /// or dropping a late one); otherwise gives it back for normal delivery.
    #[inline]
    pub(crate) fn route(&self, message: InboundMessage) -> Option<InboundMessage> {
        if !self.active.load(Ordering::Acquire) {
            return Some(message);
        }
        let Some(correlation) = message.properties.correlation_data.as_ref() else {
            return Some(message);
        };
        let mut state = self.lock();
        if !state.topics.contains_key(&message.topic) {
            return Some(message);
        }
        match state.pending.remove(correlation) {
            Some(reply) => {
                drop(state);
                let _ = reply.send(message);
            }
            None => tracing::debug!(topic = %message.topic, "dropping response nobody waits for (late or unknown)"),
        }
        None
    }
}

/// Removes a request's routing entry however its future ends (response, timeout, drop).
struct PendingGuard {
    router: Arc<ResponseRouter>,
    correlation: Bytes,
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        self.router.forget(&self.correlation);
    }
}

/// Sends requests and awaits their responses on one response topic. Cheap to clone and
/// safe to use from many tasks at once.
///
/// A requester subscribes once to its own response topic. Every request carries that topic as
/// Response Topic plus fresh Correlation Data; the driver hands a matching response straight
/// to the waiting request instead of the application's inbound sink.
///
/// # Examples
///
/// ```no_run
/// use std::time::Duration;
///
/// use mqtt_glide::{MqttClient, Publish, QoS};
///
/// # async fn demo(client: MqttClient) -> Result<(), mqtt_glide::AppError> {
/// // Asking side.
/// let requester = client.requester("svc/replies/edge-01").await?;
/// let reply = requester
///     .request("svc/time", "now?")
///     .qos(QoS::AtLeastOnce)
///     .timeout(Duration::from_secs(2))
///     .await?;
/// println!("{:?}", reply.payload);
/// # Ok(())
/// # }
///
/// # async fn answer(client: MqttClient, request: mqtt_glide::InboundMessage) -> Result<(), mqtt_glide::AppError> {
/// // Answering side, for each request taken from the inbound sink.
/// if let Some(reply) = Publish::reply_to(&request, "12:00") {
///     client.send(reply).await?;
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Requester {
    client: MqttClient,
    response_topic: ByteString,
}

impl Requester {
    pub(crate) async fn subscribe(client: &MqttClient, response_topic: ByteString) -> Result<Self, AppError> {
        if response_topic.is_empty() || response_topic.contains(['+', '#']) || response_topic.contains('\0') {
            return Err(AppError::InvalidRequest(
                "response topic must be a non-empty topic name without wildcards".into(),
            ));
        }
        let router = client.response_router();
        // Route before the subscription exists, so an early response is never missed.
        router.add_topic(response_topic.clone());
        let subscribed = client.subscribe([(response_topic.clone(), QoS::AtLeastOnce)]).await;
        let refused = match &subscribed {
            Ok(response) => response.iter().find(|reason| !reason.is_granted()).copied(),
            Err(_) => None,
        };
        if subscribed.is_err() || refused.is_some() {
            router.remove_topic(&response_topic);
        }
        subscribed?;
        if let Some(reason) = refused {
            return Err(AppError::InvalidRequest(format!(
                "broker refused the response topic subscription: {reason:?}"
            )));
        }
        Ok(Self {
            client: client.clone(),
            response_topic,
        })
    }

    /// The response topic this requester listens on.
    pub fn response_topic(&self) -> &str {
        &self.response_topic
    }

    /// Starts a request of `payload` to `topic`; `.await` it for the response.
    ///
    /// QoS 0 by default; the builder sets QoS, User Properties, Content Type, Message Expiry
    /// and [`RequestBuilder::timeout`]. Response Topic and Correlation Data are set by the
    /// requester.
    pub fn request(&self, topic: impl Into<ByteString>, payload: impl Into<Bytes>) -> RequestBuilder<'_> {
        RequestBuilder {
            requester: self,
            publish: Publish::new(topic, payload),
            timeout: DEFAULT_REQUEST_TIMEOUT,
        }
    }

    /// Stops routing the response topic and unsubscribes from it. Requests still in flight
    /// on other clones of this requester will time out. Another requester the client made for
    /// the same topic keeps working: the topic stays routed and subscribed until the last one
    /// closes.
    pub async fn close(self) -> Result<(), AppError> {
        if !self.client.response_router().remove_topic(&self.response_topic) {
            return Ok(());
        }
        self.client.unsubscribe([self.response_topic]).await.map(|_| ())
    }
}

/// One request, returned by [`Requester::request`]. Awaiting it publishes the request and
/// resolves with the response.
#[derive(Debug)]
#[must_use = "a request does nothing until it is awaited or sent"]
pub struct RequestBuilder<'a> {
    requester: &'a Requester,
    publish: Publish,
    timeout: Duration,
}

impl RequestBuilder<'_> {
    /// How long to wait for the response after the request went out (default
    /// [`DEFAULT_REQUEST_TIMEOUT`]); then [`AppError::ResponseTimeout`].
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Sets the request's QoS (default [`QoS::AtMostOnce`]).
    pub fn qos(mut self, qos: QoS) -> Self {
        self.publish = self.publish.qos(qos);
        self
    }

    /// Appends one MQTT User Property to the request.
    pub fn user_property(mut self, name: impl Into<ByteString>, value: impl Into<ByteString>) -> Self {
        self.publish = self.publish.user_property(name, value);
        self
    }

    /// Appends MQTT User Properties to the request.
    pub fn user_properties<K, V>(mut self, properties: impl IntoIterator<Item = (K, V)>) -> Self
    where
        K: Into<ByteString>,
        V: Into<ByteString>,
    {
        self.publish = self.publish.user_properties(properties);
        self
    }

    /// Sets the request's Content Type.
    pub fn content_type(mut self, content_type: impl Into<ByteString>) -> Self {
        self.publish = self.publish.content_type(content_type);
        self
    }

    /// Sets the request's Message Expiry Interval in seconds.
    pub fn message_expiry(mut self, seconds: u32) -> Self {
        self.publish = self.publish.message_expiry(seconds);
        self
    }

    /// Publishes the request and waits for its response (same as `.await`, without boxing).
    pub async fn send(self) -> Result<InboundMessage, AppError> {
        let requester = self.requester;
        let router = requester.client.response_router();
        let correlation = Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        let (reply_tx, reply_rx) = oneshot::channel();
        // Registered before the request leaves, so even an instant response finds it.
        router.register(correlation.clone(), reply_tx);
        let _pending = PendingGuard {
            router,
            correlation: correlation.clone(),
        };
        let publish = self
            .publish
            .response_topic(requester.response_topic.clone())
            .correlation_data(correlation);
        requester.client.send(publish).await?;
        match tokio::time::timeout(self.timeout, reply_rx).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(_closed)) => Err(AppError::ConnectionLost),
            Err(_elapsed) => Err(AppError::ResponseTimeout(self.timeout)),
        }
    }
}

impl<'a> IntoFuture for RequestBuilder<'a> {
    type Output = Result<InboundMessage, AppError>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.send())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{PublishProperties, QoS};

    fn response(topic: &'static str, correlation: Option<&'static [u8]>) -> InboundMessage {
        InboundMessage {
            topic: topic.into(),
            payload: Bytes::from_static(b"r"),
            properties: PublishProperties {
                correlation_data: correlation.map(Bytes::from_static),
                ..PublishProperties::default()
            },
            qos: QoS::AtMostOnce,
            retain: false,
            dup: false,
            subscription_ids: Vec::new(),
        }
    }

    #[test]
    fn router_passes_everything_through_until_a_requester_exists() {
        let router = ResponseRouter::default();
        assert!(router.route(response("rr/replies", Some(b"c"))).is_some());
    }

    #[test]
    fn router_takes_responses_and_drops_late_ones() {
        let router = ResponseRouter::default();
        router.add_topic("rr/replies".into());
        let (tx, mut rx) = oneshot::channel();
        router.register(Bytes::from_static(b"c1"), tx);

        assert!(
            router.route(response("other/topic", Some(b"c1"))).is_some(),
            "other topics pass"
        );
        assert!(
            router.route(response("rr/replies", None)).is_some(),
            "no correlation passes"
        );
        assert!(router.route(response("rr/replies", Some(b"c1"))).is_none(), "routed");
        assert_eq!(&*rx.try_recv().expect("delivered").payload, b"r");
        assert!(
            router.route(response("rr/replies", Some(b"c1"))).is_none(),
            "late duplicate dropped"
        );

        router.remove_topic(&"rr/replies".into());
        assert!(
            router.route(response("rr/replies", Some(b"c2"))).is_some(),
            "routing off again"
        );
    }

    /// Two requesters on one topic: closing or failing one leaves the other routed.
    #[test]
    fn router_keeps_a_topic_until_its_last_requester_leaves() {
        let router = ResponseRouter::default();
        let topic: ByteString = "rr/replies".into();
        router.add_topic(topic.clone());
        router.add_topic(topic.clone());
        assert!(!router.remove_topic(&topic), "another requester still uses it");
        let (tx, mut rx) = oneshot::channel();
        router.register(Bytes::from_static(b"c1"), tx);
        assert!(
            router.route(response("rr/replies", Some(b"c1"))).is_none(),
            "still routed"
        );
        assert!(rx.try_recv().is_ok());
        assert!(router.remove_topic(&topic), "last requester");
        assert!(router.route(response("rr/replies", Some(b"c2"))).is_some());
    }

    #[test]
    fn reply_to_copies_response_topic_and_correlation() {
        let mut request = response("svc/time", Some(b"id-9"));
        assert!(
            Publish::reply_to(&request, "x").is_none(),
            "no response topic: no reply"
        );
        request.properties.response_topic = Some("rr/replies".into());
        let reply = Publish::reply_to(&request, "12:00").expect("reply");
        assert_eq!(&**reply.topic(), "rr/replies");
        assert_eq!(
            reply.publish_properties().correlation_data.as_deref(),
            Some(&b"id-9"[..])
        );
    }
}
