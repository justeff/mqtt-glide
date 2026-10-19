//! Minimal in-process MQTT 5 broker for application tests.

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroU16;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use bytestring::ByteString;
use futures_util::StreamExt;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;
use tokio_util::codec::FramedRead;

use crate::Publish;
use crate::codec::{
    ConnectAck, ConnectAckReason, DecodedFrame, Disconnect, DisconnectReasonCode, Packet, PublishAck, PublishAckReason,
    PublishDelivery, PublishMessage, PublishProperties, QoS, SubscribeAck, SubscribeAckReason, UnsubscribeAck,
    UnsubscribeAckReason, UserProperties, UserProperty, V5Codec, WillMessage, encode_publish,
};
use crate::util::topic_matches;

/// The PUBACK a [`TestBroker`] answers a QoS 1 PUBLISH with; see [`TestBroker::ack_publishes`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct PubAck {
    /// Reason code: 0x00 or 0x10 accept the message, 0x80 and above reject it.
    pub reason: PublishAckReason,
    /// Reason String.
    pub reason_string: Option<ByteString>,
    /// User Properties.
    pub user_properties: UserProperties,
}

impl PubAck {
    /// A PUBACK with `reason` and no properties.
    pub fn new(reason: PublishAckReason) -> Self {
        Self {
            reason,
            reason_string: None,
            user_properties: Vec::new(),
        }
    }

    /// Sets the Reason String.
    #[must_use]
    pub fn reason_string(mut self, reason_string: impl Into<ByteString>) -> Self {
        self.reason_string = Some(reason_string.into());
        self
    }

    /// Adds a User Property.
    #[must_use]
    pub fn user_property(mut self, name: impl Into<ByteString>, value: impl Into<ByteString>) -> Self {
        self.user_properties
            .push(UserProperty::from((name.into(), value.into())));
        self
    }

    /// Whether the broker keeps the message (reason code below 0x80).
    fn accepts(&self) -> bool {
        u8::from(self.reason) < 0x80
    }
}

type AckFn = Arc<dyn Fn(&ReceivedPublish) -> PubAck + Send + Sync>;

/// A PUBLISH a client sent to the [`TestBroker`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ReceivedPublish {
    /// Client Identifier of the publisher (the assigned one if it connected without).
    pub client_id: String,
    /// Topic Name.
    pub topic: ByteString,
    /// Payload.
    pub payload: Bytes,
    /// QoS it was published with.
    pub qos: QoS,
    /// RETAIN flag.
    pub retain: bool,
    /// MQTT 5 properties (User Properties, Content Type, Correlation Data, ...).
    pub properties: PublishProperties,
}

/// In-process MQTT 5 broker on `127.0.0.1` for tests; see the [module docs](super).
///
/// Dropping it stops the broker and closes every connection.
pub struct TestBroker {
    port: u16,
    shared: Arc<Shared>,
    accept_task: JoinHandle<()>,
}

#[derive(Default)]
struct Shared {
    state: Mutex<State>,
    /// Woken whenever a publish is recorded or a client connects.
    changed: Notify,
}

#[derive(Default)]
struct State {
    next_connection: u64,
    connections: HashMap<u64, Conn>,
    retained: BTreeMap<ByteString, Delivery>,
    received: Vec<ReceivedPublish>,
    connects: usize,
    refuse_with: Option<ConnectAckReason>,
    /// Answer to QoS 1 PUBLISHes; `None`: Success.
    ack_with: Option<AckFn>,
    /// Topic filters every SUBSCRIBE gets refused, with the reason code to answer.
    refused_filters: HashMap<String, SubscribeAckReason>,
    shared_cursor: usize,
}

struct Conn {
    client_id: String,
    tx: mpsc::UnboundedSender<Outgoing>,
    subscriptions: Vec<Subscription>,
    will: Option<WillMessage>,
    task: Option<JoinHandle<()>>,
}

#[derive(Clone)]
struct Subscription {
    filter: ByteString,
    /// `$share/<group>/` stripped; `Some(group)` for a shared subscription.
    group: Option<ByteString>,
    qos: QoS,
    no_local: bool,
}

#[derive(Clone)]
struct Delivery {
    message: PublishMessage,
    qos: QoS,
    retain: bool,
}

enum Outgoing {
    Packet(Packet),
    Publish(Delivery),
    Close,
}

fn lock(shared: &Shared) -> MutexGuard<'_, State> {
    shared.state.lock().unwrap_or_else(PoisonError::into_inner)
}

impl TestBroker {
    /// Starts the broker on an ephemeral local port.
    pub async fn start() -> std::io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let shared = Arc::new(Shared::default());
        let accept_task = tokio::spawn({
            let shared = Arc::clone(&shared);
            async move {
                while let Ok((stream, _)) = listener.accept().await {
                    let _ = stream.set_nodelay(true);
                    let id = {
                        let mut state = lock(&shared);
                        state.next_connection += 1;
                        state.next_connection
                    };
                    let task = tokio::spawn(serve(Arc::clone(&shared), id, stream));
                    // The connection registers itself on CONNECT; keep the task to abort it.
                    let mut state = lock(&shared);
                    match state.connections.get_mut(&id) {
                        Some(conn) => conn.task = Some(task),
                        None => {
                            state.connections.insert(
                                id,
                                Conn {
                                    client_id: String::new(),
                                    tx: mpsc::unbounded_channel().0,
                                    subscriptions: Vec::new(),
                                    will: None,
                                    task: Some(task),
                                },
                            );
                        }
                    }
                }
            }
        });
        Ok(Self {
            port,
            shared,
            accept_task,
        })
    }

    /// Broker URL for [`ClientBuilder::new`](crate::ClientBuilder::new) /
    /// [`BrokerUrl::parse`](crate::BrokerUrl::parse), e.g. `mqtt://127.0.0.1:41234`.
    pub fn url(&self) -> String {
        format!("mqtt://127.0.0.1:{}", self.port)
    }

    /// The local port the broker listens on.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Publishes `publish` to every matching subscriber, as if another client had sent it
    /// (a retained one is also stored for later subscribers).
    pub fn publish(&self, publish: impl Into<Publish>) {
        let (message, qos, retain) = publish.into().into_parts();
        route(&self.shared, None, Delivery { message, qos, retain });
    }

    /// Everything clients have published so far, in arrival order.
    pub fn received(&self) -> Vec<ReceivedPublish> {
        lock(&self.shared).received.clone()
    }

    /// Waits until clients have published at least `count` messages or `timeout` passes, and
    /// returns what arrived by then (possibly fewer than `count`).
    pub async fn wait_for_publishes(&self, count: usize, timeout: Duration) -> Vec<ReceivedPublish> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let changed = self.shared.changed.notified();
            {
                let state = lock(&self.shared);
                if state.received.len() >= count {
                    return state.received.clone();
                }
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                return self.received();
            }
        }
    }

    /// Client Identifiers of the clients connected right now.
    pub fn connected_clients(&self) -> Vec<String> {
        let state = lock(&self.shared);
        let mut ids: Vec<_> = state
            .connections
            .values()
            .filter(|conn| !conn.client_id.is_empty())
            .map(|conn| conn.client_id.clone())
            .collect();
        ids.sort();
        ids
    }

    /// CONNECT packets accepted so far (reconnects count again).
    pub fn connect_count(&self) -> usize {
        lock(&self.shared).connects
    }

    /// Waits until at least `count` CONNECTs were accepted or `timeout` passes; returns whether
    /// they were.
    pub async fn wait_for_connects(&self, count: usize, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let changed = self.shared.changed.notified();
            if self.connect_count() >= count {
                return true;
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                return self.connect_count() >= count;
            }
        }
    }

    /// Topic Filters `client_id` is subscribed to right now (shared ones with their
    /// `$share/<group>/` prefix).
    pub fn subscriptions(&self, client_id: &str) -> Vec<String> {
        let state = lock(&self.shared);
        state
            .connections
            .values()
            .filter(|conn| conn.client_id == client_id)
            .flat_map(|conn| conn.subscriptions.iter())
            .map(|sub| match &sub.group {
                Some(group) => format!("$share/{group}/{}", sub.filter),
                None => sub.filter.to_string(),
            })
            .collect()
    }

    /// Waits until `client_id` is subscribed to `filter` (written as in
    /// [`subscriptions`](Self::subscriptions)) or `timeout` passes; returns whether it is. Use it
    /// before [`publish`](Self::publish) so the message does not go out before the code under
    /// test has subscribed, also after a reconnect.
    pub async fn wait_for_subscription(&self, client_id: &str, filter: &str, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let changed = self.shared.changed.notified();
            if self.subscriptions(client_id).iter().any(|sub| sub == filter) {
                return true;
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                return self.subscriptions(client_id).iter().any(|sub| sub == filter);
            }
        }
    }

    /// Refuses every following CONNECT with `reason` (`None`: accept again).
    pub fn refuse_connections(&self, reason: Option<ConnectAckReason>) {
        lock(&self.shared).refuse_with = reason;
    }

    /// Answers every following QoS 1 PUBLISH with the PUBACK `answer` returns for it, for
    /// example to test how an application reads a Reason String, User Properties or 0x10
    /// No Matching Subscribers. A reason code of 0x80 or above rejects the message: it is
    /// neither recorded nor routed. Replaces any earlier answer; [`Self::reject_publishes`]
    /// with `None` goes back to plain Success.
    ///
    /// ```
    /// use mqtt_glide::PublishAckReason;
    /// use mqtt_glide::testing::{PubAck, TestBroker};
    ///
    /// # async fn demo(broker: TestBroker) {
    /// broker.ack_publishes(|publish| {
    ///     if publish.topic.starts_with("vehicle/offline/") {
    ///         PubAck::new(PublishAckReason::Success).reason_string("Q")
    ///     } else {
    ///         PubAck::new(PublishAckReason::NoMatchingSubscribers)
    ///     }
    /// });
    /// # }
    /// ```
    pub fn ack_publishes(&self, answer: impl Fn(&ReceivedPublish) -> PubAck + Send + Sync + 'static) {
        lock(&self.shared).ack_with = Some(Arc::new(answer));
    }

    /// Answers every following QoS 1 PUBLISH with `reason` instead of Success (`None`: accept
    /// again). A code of 0x80 or above rejects: the message is neither recorded nor routed.
    pub fn reject_publishes(&self, reason: Option<PublishAckReason>) {
        lock(&self.shared).ack_with = reason.map(|reason| -> AckFn { Arc::new(move |_| PubAck::new(reason)) });
    }

    /// Answers every following SUBSCRIBE to exactly `filter` (as the client sends it, with any
    /// `$share/<group>/` prefix) with `reason` and does not add the subscription, like a broker
    /// whose ACL denies it (`None`: grant it again). Use a code of 0x80 or above, such as
    /// [`SubscribeAckReason::NotAuthorized`].
    pub fn refuse_subscription(&self, filter: &str, reason: Option<SubscribeAckReason>) {
        let mut state = lock(&self.shared);
        match reason {
            Some(reason) => state.refused_filters.insert(filter.to_owned(), reason),
            None => state.refused_filters.remove(filter),
        };
    }

    /// Drops every connection without a DISCONNECT, like a broker restart or a network cut.
    /// Nobody is left to receive the dropped clients' Wills; use [`Self::drop_client`] to test
    /// a Will.
    pub fn drop_connections(&self) {
        self.drop_where(|_| true);
    }

    /// Drops the connection of `client_id` without a DISCONNECT, as if that client's network
    /// failed, and publishes its Will to the remaining clients. Returns whether it was
    /// connected.
    pub fn drop_client(&self, client_id: &str) -> bool {
        self.drop_where(|conn| conn.client_id == client_id) > 0
    }

    fn drop_where(&self, mut select: impl FnMut(&Conn) -> bool) -> usize {
        let dropped: Vec<Conn> = {
            let mut state = lock(&self.shared);
            let ids: Vec<u64> = state
                .connections
                .iter()
                .filter(|(_, conn)| select(conn))
                .map(|(id, _)| *id)
                .collect();
            ids.iter().filter_map(|id| state.connections.remove(id)).collect()
        };
        let count = dropped.iter().filter(|conn| !conn.client_id.is_empty()).count();
        for mut conn in dropped {
            if let Some(task) = conn.task.take() {
                task.abort();
            }
            if let Some(will) = conn.will.take() {
                route(&self.shared, None, will_delivery(will));
            }
        }
        count
    }

    /// Sends every connected client a DISCONNECT with `reason`, then closes the connections.
    pub fn disconnect_all(&self, reason: DisconnectReasonCode) {
        let state = lock(&self.shared);
        for conn in state.connections.values() {
            let _ = conn
                .tx
                .send(Outgoing::Packet(Packet::Disconnect(Disconnect::new(reason))));
            let _ = conn.tx.send(Outgoing::Close);
        }
    }
}

impl Drop for TestBroker {
    fn drop(&mut self) {
        self.accept_task.abort();
        let mut state = lock(&self.shared);
        for (_, mut conn) in state.connections.drain() {
            if let Some(task) = conn.task.take() {
                task.abort();
            }
        }
    }
}

impl std::fmt::Debug for TestBroker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestBroker")
            .field("port", &self.port)
            .finish_non_exhaustive()
    }
}

fn will_delivery(will: WillMessage) -> Delivery {
    Delivery {
        message: PublishMessage {
            topic: will.topic,
            payload: will.payload,
            properties: PublishProperties {
                correlation_data: will.correlation_data,
                message_expiry_interval: will.message_expiry_interval,
                content_type: will.content_type,
                user_properties: will.user_properties,
                payload_format_indicator: will.payload_format_indicator.unwrap_or(false),
                response_topic: will.response_topic,
            }
            .into(),
        },
        qos: will.qos,
        retain: will.retain,
    }
}

/// One client connection: CONNECT handshake, then packets until it closes.
async fn serve(shared: Arc<Shared>, id: u64, stream: TcpStream) {
    let (read, write) = stream.into_split();
    let mut frames = FramedRead::new(read, V5Codec::default());
    let Some(Ok((DecodedFrame::Control(Packet::Connect(connect)), _))) = frames.next().await else {
        unregister(&shared, id);
        return;
    };
    let (tx, rx) = mpsc::unbounded_channel();
    let writer = tokio::spawn(write_loop(write, rx));

    let refuse = lock(&shared).refuse_with;
    if let Some(reason) = refuse {
        let _ = tx.send(Outgoing::Packet(Packet::ConnectAck(Box::new(ConnectAck {
            reason_code: reason,
            ..ConnectAck::default()
        }))));
        let _ = tx.send(Outgoing::Close);
        let _ = writer.await;
        unregister(&shared, id);
        return;
    }

    let assigned = connect.client_id.is_empty().then(|| format!("test-broker-{id}"));
    let client_id = assigned.clone().unwrap_or_else(|| connect.client_id.to_string());
    {
        let mut state = lock(&shared);
        let task = state.connections.remove(&id).and_then(|mut conn| conn.task.take());
        // A new connection with the same Client Identifier takes the session over (§3.1.4).
        let taken_over: Vec<u64> = state
            .connections
            .iter()
            .filter(|(_, conn)| conn.client_id == client_id)
            .map(|(other, _)| *other)
            .collect();
        for other in taken_over {
            if let Some(conn) = state.connections.remove(&other) {
                let _ = conn.tx.send(Outgoing::Packet(Packet::Disconnect(Disconnect::new(
                    DisconnectReasonCode::SessionTakenOver,
                ))));
                let _ = conn.tx.send(Outgoing::Close);
            }
        }
        state.connections.insert(
            id,
            Conn {
                client_id: client_id.clone(),
                tx: tx.clone(),
                subscriptions: Vec::new(),
                will: connect.will_message.clone(),
                task,
            },
        );
        state.connects += 1;
    }
    let _ = tx.send(Outgoing::Packet(Packet::ConnectAck(Box::new(ConnectAck {
        max_qos: QoS::AtLeastOnce,
        assigned_client_id: assigned.map(ByteString::from),
        ..ConnectAck::default()
    }))));
    shared.changed.notify_waiters();

    let mut clean_disconnect = false;
    while let Some(Ok((frame, _))) = frames.next().await {
        match frame {
            DecodedFrame::Publish(packet) => {
                let delivery = Delivery {
                    message: packet.message,
                    qos: packet.meta.qos,
                    retain: packet.meta.retain,
                };
                let received = ReceivedPublish {
                    client_id: client_id.clone(),
                    topic: delivery.message.topic.clone(),
                    payload: delivery.message.payload.clone(),
                    qos: delivery.qos,
                    retain: delivery.retain,
                    properties: delivery.message.properties.clone().into(),
                };
                if let Some(packet_id) = packet.meta.packet_id {
                    let answer = lock(&shared).ack_with.clone();
                    let ack = answer.map_or_else(|| PubAck::new(PublishAckReason::Success), |answer| answer(&received));
                    let accepted = ack.accepts();
                    let _ = tx.send(Outgoing::Packet(Packet::PublishAck(PublishAck {
                        packet_id,
                        reason_code: ack.reason,
                        reason_string: ack.reason_string,
                        properties: ack.user_properties,
                    })));
                    if !accepted {
                        continue;
                    }
                }
                lock(&shared).received.push(received);
                route(&shared, Some(id), delivery);
                shared.changed.notify_waiters();
            }
            DecodedFrame::Control(Packet::Subscribe(subscribe)) => {
                let mut status = Vec::with_capacity(subscribe.topic_filters.len());
                let mut retained = Vec::new();
                {
                    let mut state = lock(&shared);
                    for (filter, options) in &subscribe.topic_filters {
                        if let Some(reason) = state.refused_filters.get(&**filter) {
                            status.push(*reason);
                            continue;
                        }
                        let (group, filter) = split_shared(filter);
                        let qos = options.qos.min(QoS::AtLeastOnce);
                        status.push(if qos == QoS::AtMostOnce {
                            SubscribeAckReason::GrantedQos0
                        } else {
                            SubscribeAckReason::GrantedQos1
                        });
                        if group.is_none() {
                            retained.extend(
                                state
                                    .retained
                                    .iter()
                                    .filter(|(topic, _)| topic_matches(&filter, topic))
                                    .map(|(_, delivery)| Delivery {
                                        qos: delivery.qos.min(qos),
                                        ..delivery.clone()
                                    }),
                            );
                        }
                        if let Some(conn) = state.connections.get_mut(&id) {
                            conn.subscriptions
                                .retain(|sub| !(sub.filter == filter && sub.group == group));
                            conn.subscriptions.push(Subscription {
                                filter,
                                group,
                                qos,
                                no_local: options.no_local,
                            });
                        }
                    }
                }
                let _ = tx.send(Outgoing::Packet(Packet::SubscribeAck(SubscribeAck {
                    packet_id: subscribe.packet_id,
                    status,
                    properties: Vec::new(),
                    reason_string: None,
                })));
                for delivery in retained {
                    let _ = tx.send(Outgoing::Publish(delivery));
                }
                shared.changed.notify_waiters();
            }
            DecodedFrame::Control(Packet::Unsubscribe(unsubscribe)) => {
                let status = {
                    let mut state = lock(&shared);
                    let conn = state.connections.get_mut(&id);
                    let mut status = Vec::new();
                    if let Some(conn) = conn {
                        for filter in &unsubscribe.topic_filters {
                            let (group, filter) = split_shared(filter);
                            let before = conn.subscriptions.len();
                            conn.subscriptions
                                .retain(|sub| !(sub.filter == filter && sub.group == group));
                            status.push(if conn.subscriptions.len() < before {
                                UnsubscribeAckReason::Success
                            } else {
                                UnsubscribeAckReason::NoSubscriptionExisted
                            });
                        }
                    }
                    status
                };
                let _ = tx.send(Outgoing::Packet(Packet::UnsubscribeAck(UnsubscribeAck {
                    packet_id: unsubscribe.packet_id,
                    status,
                    properties: Vec::new(),
                    reason_string: None,
                })));
                shared.changed.notify_waiters();
            }
            DecodedFrame::Control(Packet::PingRequest) => {
                let _ = tx.send(Outgoing::Packet(Packet::PingResponse));
            }
            DecodedFrame::Control(Packet::Disconnect(disconnect)) => {
                clean_disconnect = disconnect.reason_code != DisconnectReasonCode::DisconnectWithWillMessage;
                break;
            }
            // PUBACKs for deliveries need no bookkeeping here (no redelivery).
            DecodedFrame::Control(_) => {}
        }
    }

    let will = {
        let mut state = lock(&shared);
        state.connections.remove(&id).and_then(|conn| conn.will)
    };
    if let Some(will) = will.filter(|_| !clean_disconnect) {
        route(&shared, Some(id), will_delivery(will));
    }
    let _ = tx.send(Outgoing::Close);
    let _ = writer.await;
}

fn unregister(shared: &Shared, id: u64) {
    lock(shared).connections.remove(&id);
}

async fn write_loop(mut write: OwnedWriteHalf, mut rx: mpsc::UnboundedReceiver<Outgoing>) {
    let mut codec = V5Codec::default();
    let mut buf = BytesMut::new();
    let mut next_id: u16 = 0;
    while let Some(outgoing) = rx.recv().await {
        buf.clear();
        let encoded = match outgoing {
            Outgoing::Packet(packet) => codec.encode_packet(packet, &mut buf).is_ok(),
            Outgoing::Publish(delivery) => {
                let packet_id = (delivery.qos != QoS::AtMostOnce).then(|| {
                    next_id = next_id.checked_add(1).unwrap_or(1);
                    NonZeroU16::new(next_id).expect("non-zero")
                });
                let wire = PublishDelivery {
                    dup: false,
                    retain: delivery.retain,
                    qos: delivery.qos,
                    packet_id,
                };
                encode_publish(&delivery.message, wire, 0, &mut buf).is_ok()
            }
            Outgoing::Close => break,
        };
        if encoded && write.write_all(&buf).await.is_err() {
            break;
        }
    }
    let _ = write.shutdown().await;
}

/// Delivers `delivery` to every matching subscription (one member per shared group) and
/// updates the retained store. `from` is the publishing connection, for No Local.
fn route(shared: &Shared, from: Option<u64>, delivery: Delivery) {
    let mut state = lock(shared);
    if delivery.retain {
        if delivery.message.payload.is_empty() {
            state.retained.remove(&delivery.message.topic);
        } else {
            state.retained.insert(delivery.message.topic.clone(), delivery.clone());
        }
    }
    let topic = delivery.message.topic.clone();
    let mut groups: BTreeMap<ByteString, Vec<(u64, QoS)>> = BTreeMap::new();
    let mut targets = Vec::new();
    for (id, conn) in &state.connections {
        // Several matching subscriptions of one client: one copy at the highest QoS.
        let mut best: Option<QoS> = None;
        for sub in &conn.subscriptions {
            if !topic_matches(&sub.filter, &topic) || (sub.no_local && from == Some(*id)) {
                continue;
            }
            match &sub.group {
                Some(group) => groups.entry(group.clone()).or_default().push((*id, sub.qos)),
                None => best = Some(best.map_or(sub.qos, |qos| qos.max(sub.qos))),
            }
        }
        if let Some(qos) = best {
            targets.push((*id, qos));
        }
    }
    for members in groups.values_mut() {
        members.sort_by_key(|(id, _)| *id);
        let pick = state.shared_cursor % members.len();
        targets.push(members[pick]);
    }
    if !groups.is_empty() {
        state.shared_cursor += 1;
    }
    for (id, qos) in targets {
        if let Some(conn) = state.connections.get(&id) {
            let _ = conn.tx.send(Outgoing::Publish(Delivery {
                qos: delivery.qos.min(qos),
                // Forwarded as a live message: RETAIN only on deliveries caused by a subscribe.
                retain: false,
                message: delivery.message.clone(),
            }));
        }
    }
}

/// `$share/<group>/<filter>` → `(Some(group), filter)`; anything else → `(None, filter)`.
fn split_shared(filter: &ByteString) -> (Option<ByteString>, ByteString) {
    if let Some(rest) = filter.strip_prefix("$share/")
        && let Some((group, inner)) = rest.split_once('/')
    {
        return (Some(group.into()), inner.into());
    }
    (None, filter.clone())
}
