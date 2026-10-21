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
    Auth, AuthReasonCode, ConnectAck, ConnectAckReason, DecodedFrame, Disconnect, DisconnectReasonCode, Packet,
    PublishAck, PublishAckReason, PublishDelivery, PublishMessage, PublishProperties, QoS, SubscribeAck,
    SubscribeAckReason, UnsubscribeAck, UnsubscribeAckReason, UserProperties, UserProperty, V5Codec, WillMessage,
    encode_publish,
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

/// One step of enhanced authentication a [`TestBroker`] is asked to answer; see
/// [`TestBroker::enhanced_auth`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct AuthRequest {
    /// Client Identifier (the assigned one if it connected without).
    pub client_id: String,
    /// The client's Authentication Data: from CONNECT, AUTH 0x19 (Re-authenticate) or its
    /// AUTH 0x18 answer to a challenge.
    pub data: Option<Bytes>,
    /// Part of a re-authentication on an open connection (§4.12.1), not of CONNECT.
    pub reauthentication: bool,
}

/// How a [`TestBroker`] answers an [`AuthRequest`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AuthStep {
    /// AUTH 0x18 (Continue authentication) with this challenge; the client's answer is the
    /// next request.
    Continue(Bytes),
    /// Accept: CONNACK 0x00, or AUTH 0x00 (Success) ending a re-authentication, with this
    /// Authentication Data.
    Accept(Option<Bytes>),
    /// Refuse with 0x87 Not authorized: in the CONNACK, or in a DISCONNECT during a
    /// re-authentication.
    Refuse,
}

type AuthFn = Arc<dyn Fn(&AuthRequest) -> AuthStep + Send + Sync>;

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
    /// DUP flag: the client sent it again after a reconnect into a resumed session.
    pub dup: bool,
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
    /// Sessions of disconnected clients, by Client Identifier.
    sessions: HashMap<String, Session>,
    /// Accept QoS 1 PUBLISHes without answering them (PUBACKs lost on the way).
    withhold_pubacks: bool,
    /// Enhanced authentication: the method it accepts and how it answers.
    enhanced_auth: Option<(ByteString, AuthFn)>,
}

struct Conn {
    client_id: String,
    tx: mpsc::UnboundedSender<Outgoing>,
    subscriptions: Vec<Subscription>,
    will: Option<WillMessage>,
    task: Option<JoinHandle<()>>,
    /// QoS 1 deliveries to this client not yet acknowledged, in send order.
    unacked: Vec<(NonZeroU16, Delivery)>,
    next_packet_id: u16,
    /// Session Expiry Interval from CONNECT (or a later DISCONNECT); 0 ends the session with
    /// the connection.
    session_expiry: u32,
}

/// What a broker keeps of a client while it is disconnected (§4.1).
struct Session {
    subscriptions: Vec<Subscription>,
    unacked: Vec<(NonZeroU16, Delivery)>,
    next_packet_id: u16,
    /// QoS 1 messages that matched while the client was away.
    queued: Vec<Delivery>,
    /// `None`: never (Session Expiry Interval 0xFFFFFFFF).
    expires_at: Option<tokio::time::Instant>,
}

impl Session {
    fn expired(&self, now: tokio::time::Instant) -> bool {
        self.expires_at.is_some_and(|at| at <= now)
    }
}

impl Conn {
    fn new(client_id: String, tx: mpsc::UnboundedSender<Outgoing>, task: Option<JoinHandle<()>>) -> Self {
        Self {
            client_id,
            tx,
            subscriptions: Vec::new(),
            will: None,
            task,
            unacked: Vec::new(),
            next_packet_id: 0,
            session_expiry: 0,
        }
    }

    /// Sends a PUBLISH; a QoS 1 one gets a free Packet Identifier and waits for its PUBACK.
    fn deliver(&mut self, delivery: Delivery) {
        let packet_id = (delivery.qos != QoS::AtMostOnce).then(|| {
            loop {
                self.next_packet_id = self.next_packet_id.checked_add(1).unwrap_or(1);
                let id = NonZeroU16::new(self.next_packet_id).expect("non-zero");
                if !self.unacked.iter().any(|(used, _)| *used == id) {
                    break id;
                }
            }
        });
        if let Some(id) = packet_id {
            self.unacked.push((id, delivery.clone()));
        }
        let _ = self.tx.send(Outgoing::Publish {
            delivery,
            packet_id,
            dup: false,
        });
    }

    /// Ends the connection; returns the session to keep, if its expiry allows one.
    fn into_session(self, now: tokio::time::Instant) -> Option<(String, Session)> {
        if self.session_expiry == 0 || self.client_id.is_empty() {
            return None;
        }
        let expires_at =
            (self.session_expiry != u32::MAX).then(|| now + Duration::from_secs(u64::from(self.session_expiry)));
        Some((
            self.client_id,
            Session {
                subscriptions: self.subscriptions,
                unacked: self.unacked,
                next_packet_id: self.next_packet_id,
                queued: Vec::new(),
                expires_at,
            },
        ))
    }
}

impl State {
    /// Removes connection `id`, keeping its session; returns its Will and task.
    fn end_connection(&mut self, id: u64) -> Option<(Option<WillMessage>, Option<JoinHandle<()>>)> {
        let mut conn = self.connections.remove(&id)?;
        let will = conn.will.take();
        let task = conn.task.take();
        if let Some((client_id, session)) = conn.into_session(tokio::time::Instant::now()) {
            self.sessions.insert(client_id, session);
        }
        Some((will, task))
    }
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
    Publish {
        delivery: Delivery,
        packet_id: Option<NonZeroU16>,
        dup: bool,
    },
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
                            let conn = Conn::new(String::new(), mpsc::unbounded_channel().0, Some(task));
                            state.connections.insert(id, conn);
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
        let (count, dropped) = {
            let mut state = lock(&self.shared);
            let ids: Vec<u64> = state
                .connections
                .iter()
                .filter(|(_, conn)| select(conn))
                .map(|(id, _)| *id)
                .collect();
            let count = ids
                .iter()
                .filter(|id| state.connections.get(id).is_some_and(|conn| !conn.client_id.is_empty()))
                .count();
            let dropped: Vec<_> = ids.iter().filter_map(|id| state.end_connection(*id)).collect();
            (count, dropped)
        };
        for (will, task) in dropped {
            if let Some(task) = task {
                task.abort();
            }
            if let Some(will) = will {
                route(&self.shared, None, will_delivery(will));
            }
        }
        count
    }

    /// Keeps accepting QoS 1 PUBLISHes but stops answering them (`false`: answer again), as if
    /// every PUBACK got lost on the way. Messages are still recorded and routed. Drop the
    /// client afterwards to see it resend them, with DUP, into its resumed session.
    pub fn withhold_pubacks(&self, withhold: bool) {
        lock(&self.shared).withhold_pubacks = withhold;
    }

    /// Whether the broker keeps a session for the disconnected client `client_id`: it
    /// connected with a Session Expiry Interval above 0, and that interval has not run out.
    pub fn has_session(&self, client_id: &str) -> bool {
        lock(&self.shared)
            .sessions
            .get(client_id)
            .is_some_and(|session| !session.expired(tokio::time::Instant::now()))
    }

    /// Runs enhanced authentication (§4.12) for clients whose CONNECT names `method`: `answer`
    /// gets each step's client data and decides how the broker replies, for the CONNECT and
    /// for re-authentications on an open connection. A CONNECT with another method is refused
    /// with 0x8C (Bad authentication method), as without this call; one without a method still
    /// connects. Replaces any earlier answer.
    ///
    /// ```
    /// use bytes::Bytes;
    /// use mqtt_glide::testing::{AuthStep, TestBroker};
    ///
    /// # async fn demo(broker: TestBroker) {
    /// // One challenge, then the client must have answered it with "signed:nonce".
    /// broker.enhanced_auth("example-nonce-token", |request| match request.data.as_deref() {
    ///     None => AuthStep::Continue(Bytes::from_static(b"nonce")),
    ///     Some(b"signed:nonce") => AuthStep::Accept(None),
    ///     Some(_) => AuthStep::Refuse,
    /// });
    /// # }
    /// ```
    pub fn enhanced_auth(
        &self,
        method: impl Into<ByteString>,
        answer: impl Fn(&AuthRequest) -> AuthStep + Send + Sync + 'static,
    ) {
        lock(&self.shared).enhanced_auth = Some((method.into(), Arc::new(answer)));
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
    // Enhanced authentication before the CONNACK (§4.12).
    let mut accepted_auth = None;
    if let Some(method) = connect.auth_method.clone() {
        let configured = lock(&shared).enhanced_auth.clone();
        let refusal = match configured.filter(|(accepted, _)| *accepted == method) {
            None => Some(ConnectAckReason::BadAuthenticationMethod),
            Some((_, answer)) => {
                match authenticate_connect(
                    &answer,
                    &method,
                    &client_id,
                    connect.auth_data.clone(),
                    &tx,
                    &mut frames,
                )
                .await
                {
                    Ok(data) => {
                        accepted_auth = Some((method, data));
                        None
                    }
                    Err(reason) => Some(reason),
                }
            }
        };
        if let Some(reason) = refusal {
            let _ = tx.send(Outgoing::Packet(Packet::ConnectAck(Box::new(ConnectAck {
                reason_code: reason,
                ..ConnectAck::default()
            }))));
            let _ = tx.send(Outgoing::Close);
            let _ = writer.await;
            unregister(&shared, id);
            return;
        }
    }
    let (auth_method, auth_data) = accepted_auth.map_or((None, None), |(method, data)| (Some(method), data));
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
            if let Some(conn) = state.connections.get(&other) {
                let _ = conn.tx.send(Outgoing::Packet(Packet::Disconnect(Disconnect::new(
                    DisconnectReasonCode::SessionTakenOver,
                ))));
                let _ = conn.tx.send(Outgoing::Close);
            }
            // Its Will is not published: the client is still there (§3.1.4).
            state.end_connection(other);
        }
        // Clean Start discards a kept session; otherwise an unexpired one resumes (§3.1.2.4).
        let kept = state.sessions.remove(&client_id);
        let resumed = kept.filter(|session| !connect.clean_start && !session.expired(tokio::time::Instant::now()));
        let mut conn = Conn::new(client_id.clone(), tx.clone(), task);
        conn.will = connect.will_message.clone();
        conn.session_expiry = connect.session_expiry_interval;
        let _ = tx.send(Outgoing::Packet(Packet::ConnectAck(Box::new(ConnectAck {
            session_present: resumed.is_some(),
            max_qos: QoS::AtLeastOnce,
            assigned_client_id: assigned.map(ByteString::from),
            response_info: connect
                .request_response_info
                .then(|| ByteString::from(format!("responses/{client_id}"))),
            auth_method: auth_method.clone(),
            auth_data,
            ..ConnectAck::default()
        }))));
        if let Some(session) = resumed {
            conn.subscriptions = session.subscriptions;
            conn.next_packet_id = session.next_packet_id;
            // Unacknowledged deliveries again with their Packet Identifiers and DUP, in
            // order, then what queued up meanwhile ([MQTT-4.4.0-1]).
            conn.unacked = session.unacked;
            for (packet_id, delivery) in &conn.unacked {
                let _ = tx.send(Outgoing::Publish {
                    delivery: delivery.clone(),
                    packet_id: Some(*packet_id),
                    dup: true,
                });
            }
            for delivery in session.queued {
                conn.deliver(delivery);
            }
        }
        state.connections.insert(id, conn);
        state.connects += 1;
    }
    shared.changed.notify_waiters();

    let mut clean_disconnect = false;
    let mut reauthenticating = false;
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
                    dup: packet.meta.dup,
                    properties: delivery.message.properties.clone().into(),
                };
                if let Some(packet_id) = packet.meta.packet_id {
                    let (answer, withhold) = {
                        let state = lock(&shared);
                        (state.ack_with.clone(), state.withhold_pubacks)
                    };
                    let ack = answer.map_or_else(|| PubAck::new(PublishAckReason::Success), |answer| answer(&received));
                    let accepted = ack.accepts();
                    if !withhold {
                        let _ = tx.send(Outgoing::Packet(Packet::PublishAck(PublishAck {
                            packet_id,
                            reason_code: ack.reason,
                            reason_string: ack.reason_string,
                            properties: ack.user_properties,
                        })));
                    }
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
                if let Some(conn) = lock(&shared).connections.get_mut(&id) {
                    for delivery in retained {
                        conn.deliver(delivery);
                    }
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
            DecodedFrame::Control(Packet::Auth(auth)) => {
                let answer = lock(&shared).enhanced_auth.clone();
                let expected = if reauthenticating {
                    AuthReasonCode::ContinueAuth
                } else {
                    AuthReasonCode::ReAuth
                };
                let valid =
                    auth.reason_code == expected && auth.auth_method.is_some() && auth.auth_method == auth_method;
                // Configured meanwhile for another method: this connection's is not accepted any more.
                if valid
                    && answer
                        .as_ref()
                        .is_none_or(|(method, _)| Some(method) != auth_method.as_ref())
                {
                    let _ = tx.send(Outgoing::Packet(Packet::Disconnect(Disconnect::new(
                        DisconnectReasonCode::BadAuthenticationMethod,
                    ))));
                    break;
                }
                let step = answer.filter(|_| valid).map(|(_, answer)| {
                    answer(&AuthRequest {
                        client_id: client_id.clone(),
                        data: auth.auth_data,
                        reauthentication: true,
                    })
                });
                let method = auth.auth_method;
                match step {
                    Some(AuthStep::Continue(challenge)) => {
                        reauthenticating = true;
                        let _ = tx.send(Outgoing::Packet(auth_packet(
                            AuthReasonCode::ContinueAuth,
                            method,
                            Some(challenge),
                        )));
                    }
                    Some(AuthStep::Accept(data)) => {
                        reauthenticating = false;
                        let _ = tx.send(Outgoing::Packet(auth_packet(AuthReasonCode::Success, method, data)));
                    }
                    Some(AuthStep::Refuse) => {
                        let _ = tx.send(Outgoing::Packet(Packet::Disconnect(Disconnect::new(
                            DisconnectReasonCode::NotAuthorized,
                        ))));
                        break;
                    }
                    // AUTH without an exchange to belong to ([MQTT-4.12.0-7], [MQTT-4.12.1-1]).
                    None => {
                        let _ = tx.send(Outgoing::Packet(Packet::Disconnect(Disconnect::new(
                            DisconnectReasonCode::ProtocolError,
                        ))));
                        break;
                    }
                }
            }
            DecodedFrame::Control(Packet::PublishAck(ack)) => {
                if let Some(conn) = lock(&shared).connections.get_mut(&id) {
                    conn.unacked.retain(|(packet_id, _)| *packet_id != ack.packet_id);
                }
            }
            DecodedFrame::Control(Packet::Disconnect(disconnect)) => {
                clean_disconnect = disconnect.reason_code != DisconnectReasonCode::DisconnectWithWillMessage;
                // The client may change its Session Expiry Interval when it leaves (§3.14.2.2.2).
                if let Some(expiry) = disconnect.session_expiry_interval
                    && let Some(conn) = lock(&shared).connections.get_mut(&id)
                {
                    conn.session_expiry = expiry;
                }
                break;
            }
            DecodedFrame::Control(_) => {}
        }
    }

    let will = {
        let mut state = lock(&shared);
        state.end_connection(id).and_then(|(will, _)| will)
    };
    if let Some(will) = will.filter(|_| !clean_disconnect) {
        route(&shared, Some(id), will_delivery(will));
    }
    let _ = tx.send(Outgoing::Close);
    let _ = writer.await;
}

/// The CONNECT part of enhanced authentication: challenges until `answer` accepts (its
/// CONNACK data) or refuses (0x87), or the client sends anything but the AUTH answer (0x82,
/// [MQTT-3.1.2-30]).
async fn authenticate_connect(
    answer: &AuthFn,
    method: &ByteString,
    client_id: &str,
    mut data: Option<Bytes>,
    tx: &mpsc::UnboundedSender<Outgoing>,
    frames: &mut FramedRead<tokio::net::tcp::OwnedReadHalf, V5Codec>,
) -> Result<Option<Bytes>, ConnectAckReason> {
    loop {
        let request = AuthRequest {
            client_id: client_id.to_owned(),
            data,
            reauthentication: false,
        };
        match answer(&request) {
            AuthStep::Accept(data) => return Ok(data),
            AuthStep::Refuse => return Err(ConnectAckReason::NotAuthorized),
            AuthStep::Continue(challenge) => {
                let challenge = auth_packet(AuthReasonCode::ContinueAuth, Some(method.clone()), Some(challenge));
                let _ = tx.send(Outgoing::Packet(challenge));
                match frames.next().await {
                    Some(Ok((DecodedFrame::Control(Packet::Auth(auth)), _)))
                        if auth.reason_code == AuthReasonCode::ContinueAuth
                            && auth.auth_method.as_ref() == Some(method) =>
                    {
                        data = auth.auth_data;
                    }
                    _ => return Err(ConnectAckReason::ProtocolError),
                }
            }
        }
    }
}

fn auth_packet(reason_code: AuthReasonCode, method: Option<ByteString>, data: Option<Bytes>) -> Packet {
    Packet::Auth(Auth {
        reason_code,
        auth_method: method,
        auth_data: data,
        ..Auth::default()
    })
}

fn unregister(shared: &Shared, id: u64) {
    lock(shared).connections.remove(&id);
}

async fn write_loop(mut write: OwnedWriteHalf, mut rx: mpsc::UnboundedReceiver<Outgoing>) {
    let mut codec = V5Codec::default();
    let mut buf = BytesMut::new();
    while let Some(outgoing) = rx.recv().await {
        buf.clear();
        let encoded = match outgoing {
            Outgoing::Packet(packet) => codec.encode_packet(packet, &mut buf).is_ok(),
            Outgoing::Publish {
                delivery,
                packet_id,
                dup,
            } => {
                let wire = PublishDelivery {
                    dup,
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
    // Forwarded as a live message: RETAIN only on deliveries caused by a subscribe.
    let forward = |qos: QoS| Delivery {
        qos: delivery.qos.min(qos),
        retain: false,
        message: delivery.message.clone(),
    };
    for (id, qos) in targets {
        if let Some(conn) = state.connections.get_mut(&id) {
            conn.deliver(forward(qos));
        }
    }
    // Disconnected clients with a session keep their QoS 1 messages until they return
    // (shared groups deliver to their connected members only).
    let now = tokio::time::Instant::now();
    state.sessions.retain(|_, session| !session.expired(now));
    for session in state.sessions.values_mut() {
        let best = session
            .subscriptions
            .iter()
            .filter(|sub| sub.group.is_none() && topic_matches(&sub.filter, &topic))
            .map(|sub| sub.qos)
            .max();
        if let Some(qos) = best
            && delivery.qos.min(qos) == QoS::AtLeastOnce
        {
            session.queued.push(forward(qos));
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
