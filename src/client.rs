//! [`MqttClient`] handle and [`connect`] entry point.
//!
//! Subscribe / unsubscribe await SUBACK / UNSUBACK with per-filter reason codes.
//! Publish: QoS 0 fire-and-forget; QoS 1 awaits PUBACK.
//! Prefer [`MqttClient::publish_with`] for the fluent builder, [`MqttClient::publish_window`] for
//! pipelined sliding-window publish, or [`MqttClient::publish`] with a ready
//! [`PublishMessage`](crate::PublishMessage).
//!
//! For a higher-level entry point that applies defaults and connects (spawns the driver), see
//! [`ClientBuilder`].

use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use bytestring::ByteString;
use secrecy::SecretString;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;

use crate::codec::{
    PublishMessage, QoS, RetainHandling, SubscribeAckReason, SubscriptionOptions, UnsubscribeAckReason,
};
use crate::connection::{
    ClientCommand, CommandSender, Connection, ConnectionChannels, LifecycleHook, SessionPhase, control_channel_capacity,
};
use crate::error::{AppError, AuthError, ConnectError, ConnectionOutcome};
use crate::inbound_sink::InboundSink;
use crate::options::ConnectOptions;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

pub use crate::builder::ClientBuilder;
pub use crate::publish_builder::PublishWith;
use crate::publish_builder::{PayloadUnset, TopicUnset};
pub use crate::publish_window::{PublishWindow, WindowPublishWith};

/// One topic filter + options for [`MqttClient::subscribe`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subscription {
    /// Topic filter (may be a shared filter such as `$share/group/topic`).
    pub filter: ByteString,
    /// Per-filter subscription options (QoS, retain handling, …).
    pub options: SubscriptionOptions,
}

impl Subscription {
    /// Builds a subscription with the given filter and QoS; other options use defaults.
    pub fn new(filter: impl Into<ByteString>, qos: QoS) -> Self {
        Self {
            filter: filter.into(),
            options: SubscriptionOptions {
                qos,
                ..SubscriptionOptions::default()
            },
        }
    }

    /// Sets the No Local subscription option ([MQTT-3.8.3-3](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901169)).
    #[inline]
    pub fn no_local(mut self, no_local: bool) -> Self {
        self.options.no_local = no_local;
        self
    }

    /// Sets Retain As Published ([MQTT-3.8.3-3](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901169)).
    #[inline]
    pub fn retain_as_published(mut self, retain_as_published: bool) -> Self {
        self.options.retain_as_published = retain_as_published;
        self
    }

    /// Sets Retain Handling ([MQTT-3.8.3-3](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901169)).
    #[inline]
    pub fn retain_handling(mut self, retain_handling: RetainHandling) -> Self {
        self.options.retain_handling = retain_handling;
        self
    }
}

impl<S: Into<ByteString>> From<(S, QoS)> for Subscription {
    /// `(filter, qos)` → [`Subscription::new`] (other options default).
    #[inline]
    fn from((filter, qos): (S, QoS)) -> Self {
        Self::new(filter, qos)
    }
}

/// Supplies MQTT username and password for CONNECT.
///
/// Implementations must be [`Send`] + [`Sync`] so the driver can share them across tasks.
/// For fixed passwords see [`StaticCredentials`]; apps with rotating tokens implement this
/// trait themselves (this crate does not embed OAuth).
///
/// Uses RPITIT (`impl Future + Send`) instead of the `async_trait` crate so futures
/// are `Send` and can be erased for storage in [`Connection`].
pub trait CredentialsProvider: Send + Sync {
    /// Returns credentials for the next CONNECT attempt.
    fn credentials(&self) -> impl Future<Output = Result<(String, SecretString), AuthError>> + Send;

    /// Invalidates cached credentials after a broker auth rejection.
    ///
    /// Default is a no-op so static-password providers can omit this method.
    /// The driver calls this only for CONNECT-level [`AuthError::NotAuthorized`]
    /// (never SUBACK ACL; never [`AuthError::Provider`]).
    fn invalidate(&self) -> impl Future<Output = ()> + Send {
        async {}
    }
}

/// Fixed username/password credentials that never invalidate.
///
/// Convenience helper for tests and simple deployments. Uses the default no-op
/// [`CredentialsProvider::invalidate`].
#[derive(Debug, Clone)]
pub struct StaticCredentials {
    username: String,
    password: SecretString,
}

impl StaticCredentials {
    /// Builds a provider that always returns the same `username` and `password`.
    pub fn new(username: impl Into<String>, password: impl Into<SecretString>) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
        }
    }
}

impl CredentialsProvider for StaticCredentials {
    async fn credentials(&self) -> Result<(String, SecretString), AuthError> {
        Ok((self.username.clone(), self.password.clone()))
    }
}

/// CONNECT without username or password (anonymous session).
///
/// Returns empty username/password; the driver omits both fields on CONNECT
/// (no USERNAME / PASSWORD flags). Default for [`ClientBuilder`].
#[derive(Debug, Default, Clone, Copy)]
pub struct AnonymousCredentials;

impl CredentialsProvider for AnonymousCredentials {
    async fn credentials(&self) -> Result<(String, SecretString), AuthError> {
        Ok((String::new(), SecretString::from(String::new())))
    }
}

/// Decides reconnect delay after a connection outcome.
///
/// Return `None` to stop reconnecting (shutdown). For a ready-made fixed-delay /
/// auth-backoff policy see [`crate::StandardReconnectPolicy`].
pub trait ReconnectPolicy: Send + Sync {
    /// Delay before the next connect attempt after `outcome`.
    ///
    /// `None` means stop (no further reconnect).
    fn delay_after(&self, outcome: &ConnectionOutcome) -> Option<Duration>;
}

impl<T: ReconnectPolicy + ?Sized> ReconnectPolicy for Arc<T> {
    fn delay_after(&self, outcome: &ConnectionOutcome) -> Option<Duration> {
        (**self).delay_after(outcome)
    }
}

/// Never reconnects — a single connect attempt then stop.
///
/// Default [`ReconnectPolicy`] for [`ClientBuilder`]. Useful for tests and
/// one-shot connections.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoReconnect;

impl ReconnectPolicy for NoReconnect {
    fn delay_after(&self, _outcome: &ConnectionOutcome) -> Option<Duration> {
        None
    }
}

/// Erased credentials provider stored inside [`Connection`].
#[derive(Clone)]
pub(crate) struct SharedCredentials(Arc<dyn CredentialsErased>);

impl std::fmt::Debug for SharedCredentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("SharedCredentials").finish_non_exhaustive()
    }
}

type CredentialFuture<'a> = Pin<Box<dyn Future<Output = Result<(String, SecretString), AuthError>> + Send + 'a>>;
type InvalidateFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

trait CredentialsErased: Send + Sync {
    fn credentials(&self) -> CredentialFuture<'_>;

    fn invalidate(&self) -> InvalidateFuture<'_>;
}

impl<T> CredentialsErased for T
where
    T: CredentialsProvider + Send + Sync + 'static,
{
    fn credentials(&self) -> CredentialFuture<'_> {
        Box::pin(CredentialsProvider::credentials(self))
    }

    fn invalidate(&self) -> InvalidateFuture<'_> {
        Box::pin(CredentialsProvider::invalidate(self))
    }
}

impl SharedCredentials {
    /// Boxes any [`CredentialsProvider`] for storage in the driver.
    pub(crate) fn new(provider: impl CredentialsProvider + 'static) -> Self {
        Self(Arc::new(provider))
    }

    /// Fetches credentials for the next CONNECT.
    pub(crate) async fn credentials(&self) -> Result<(String, SecretString), AuthError> {
        self.0.credentials().await
    }

    /// Invalidates cached credentials after a broker auth rejection.
    ///
    /// Single-attempt connect calls this on `AuthError::NotAuthorized` — no silent reconnect loop.
    pub(crate) async fn invalidate(&self) {
        self.0.invalidate().await;
    }
}

/// Completion handle for an outbound PUBLISH started by [`MqttClient::start_publish`].
///
/// [`Self::wait`] completes after the frame is **encoded into the connection write buffer**
/// for QoS 0 (not necessarily flushed to the socket or received by the broker), or after
/// the matching PUBACK for QoS 1.
///
/// # Drop behavior
///
/// Dropping this handle without waiting does not cancel the already-enqueued PUBLISH.
/// For QoS 1, the driver-side waiter remains until PUBACK or session teardown, matching
/// the behavior of dropping a [`MqttClient::publish`] future while it awaits completion.
/// The acknowledgement timeout starts only when [`Self::wait`] is polled.
#[derive(Debug)]
pub struct PublishCompletion {
    reply_rx: oneshot::Receiver<Result<(), AppError>>,
    ack_timeout: Duration,
}

impl PublishCompletion {
    /// Waits for the outbound PUBLISH to complete.
    ///
    /// QoS 0 completes after the PUBLISH is encoded into the connection write buffer (not
    /// necessarily flushed or broker-received). QoS 1 completes after the matching PUBACK.
    /// If completion does not arrive before the configured acknowledgement timeout, this
    /// returns [`AppError::PublishAckTimeout`].
    #[inline]
    pub async fn wait(self) -> Result<(), AppError> {
        await_ack(self.ack_timeout, self.reply_rx, AckKind::Publish).await
    }
}

/// Cloneable command handle — sends application commands over a bounded channel.
#[derive(Debug, Clone)]
pub struct MqttClient {
    /// Shared sender replaced by the driver on each reconnect session (lock-free load).
    command_tx: Arc<ArcSwap<CommandSender>>,
    /// Cancels the driver during connect/backoff when no write task is listening.
    cancel: CancellationToken,
    /// Priority graceful-shutdown signal — not starved by a full command channel.
    shutdown_notify: CancellationToken,
    /// Deadline waiting for PUBACK / SUBACK / UNSUBACK (from [`ConnectOptions::ack_timeout`]).
    ack_timeout: Duration,
}

impl MqttClient {
    /// Creates a handle wired to the driver's bounded command channel.
    fn new(
        command_tx: Arc<ArcSwap<CommandSender>>,
        cancel: CancellationToken,
        shutdown_notify: CancellationToken,
        ack_timeout: Duration,
    ) -> Self {
        Self {
            command_tx,
            cancel,
            shutdown_notify,
            ack_timeout,
        }
    }

    /// Sends a command, mapping a full channel to [`AppError::Backpressure`].
    #[inline]
    fn try_send_command(&self, command: ClientCommand) -> Result<(), AppError> {
        // ArcSwap load — no lock across await; sender is only swapped on reconnect.
        self.command_tx.load().try_send(command).map_err(|error| match error {
            TrySendError::Full(_) => AppError::Backpressure("command channel full".into()),
            TrySendError::Closed(_) => AppError::CommandChannelClosed,
        })
    }

    /// Publishes an application message.
    ///
    /// - **QoS 0:** no packet identifier, no publish-inflight permit, no PUBACK wait — completes
    ///   after the PUBLISH is encoded into the connection write buffer (not necessarily flushed
    ///   to the socket or received by the broker).
    /// - **QoS 1:** allocates a publish packet id (capped by CONNACK `server_receive_maximum`) and
    ///   resolves only after a matching PUBACK.
    ///
    /// `packet_id` / `dup` are assigned by the connection driver, not the caller.
    /// Full command channel → [`AppError::Backpressure`].
    ///
    /// QoS 1 awaits PUBACK until [`ConnectOptions::ack_timeout`] elapses, then
    /// returns [`AppError::PublishAckTimeout`]. QoS 0 uses the same deadline for encode
    /// completion.
    pub async fn publish(&self, message: PublishMessage, qos: QoS, retain: bool) -> Result<(), AppError> {
        self.start_publish(message, qos, retain)?.wait().await
    }

    /// Enqueues an application PUBLISH and returns its completion handle without waiting.
    ///
    /// - **QoS 0:** completion resolves after the PUBLISH is encoded into the connection write
    ///   buffer (not necessarily flushed or broker-received).
    /// - **QoS 1:** completion resolves after the matching PUBACK.
    ///
    /// `packet_id` / `dup` are assigned by the connection driver, not the caller.
    /// QoS 2 returns [`AppError::InvalidRequest`]. A full command channel returns
    /// [`AppError::Backpressure`] without creating a completion handle.
    ///
    /// Call [`PublishCompletion::wait`] to observe write/PUBACK errors and enforce
    /// [`ConnectOptions::ack_timeout`].
    #[inline]
    pub fn start_publish(
        &self,
        message: PublishMessage,
        qos: QoS,
        retain: bool,
    ) -> Result<PublishCompletion, AppError> {
        if qos == QoS::ExactlyOnce {
            return Err(AppError::InvalidRequest("QoS 2 publish is not supported".into()));
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        self.try_send_command(ClientCommand::Publish {
            message,
            qos,
            retain,
            reply: reply_tx,
        })?;
        Ok(PublishCompletion {
            reply_rx,
            ack_timeout: self.ack_timeout,
        })
    }

    /// Starts a fluent PUBLISH builder.
    ///
    /// Default QoS is [`QoS::AtMostOnce`]; retain defaults to `false`. Topic and payload are
    /// required before [`.send`](PublishWith::send) / `.await` (type-state). See [`PublishWith`].
    ///
    /// Prefer [`.send()`](PublishWith::send).await; bare `.await` boxes via
    /// [`IntoFuture`](std::future::IntoFuture).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use mqtt_glide::QoS;
    ///
    /// # async fn demo(client: mqtt_glide::MqttClient) -> Result<(), mqtt_glide::AppError> {
    /// client
    ///     .publish_with()
    ///     .topic("t/example")
    ///     .payload("hello")
    ///     .qos(QoS::AtLeastOnce)
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    pub fn publish_with(&self) -> PublishWith<'_, TopicUnset, PayloadUnset> {
        PublishWith::new(self)
    }

    /// Starts a sliding-window pipeline over [`Self::start_publish`] (no task per message).
    ///
    /// Use [`PublishWindow::push`] / [`PublishWindow::publish_with`] to enqueue, then
    /// [`PublishWindow::drain`] after the burst. See [`PublishWindow`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::num::NonZeroUsize;
    ///
    /// use mqtt_glide::QoS;
    ///
    /// # async fn demo(client: mqtt_glide::MqttClient) -> Result<(), mqtt_glide::AppError> {
    /// let mut pipe = client.publish_window(NonZeroUsize::new(64).expect("nz"));
    /// pipe.publish_with()
    ///     .topic("t/example")
    ///     .payload("hello")
    ///     .qos(QoS::AtLeastOnce)
    ///     .push()
    ///     .await?;
    /// pipe.drain().await?;
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    pub fn publish_window(&self, capacity: NonZeroUsize) -> PublishWindow<'_> {
        PublishWindow::new(self, capacity)
    }

    /// Publishes every message with the same `qos` / `retain` through an internal
    /// [`PublishWindow`] of `window` capacity ([`PublishWindow::push_retry`] +
    /// [`PublishWindow::drain`]).
    ///
    /// Prefer [`Self::publish_window`] when messages need mixed QoS/retain or custom
    /// wait ordering. An empty iterator completes immediately. Stops at the first
    /// error (same as [`PublishWindow::drain`]); QoS 2 → [`AppError::InvalidRequest`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::num::NonZeroUsize;
    ///
    /// use mqtt_glide::{PublishMessage, QoS};
    ///
    /// # async fn demo(client: mqtt_glide::MqttClient) -> Result<(), mqtt_glide::AppError> {
    /// let messages = [
    ///     PublishMessage::new("out/1", "a"),
    ///     PublishMessage::new("out/2", "b"),
    /// ];
    /// client
    ///     .publish_batch(
    ///         messages,
    ///         QoS::AtLeastOnce,
    ///         false,
    ///         NonZeroUsize::new(64).expect("nz"),
    ///     )
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn publish_batch(
        &self,
        messages: impl IntoIterator<Item = PublishMessage>,
        qos: QoS,
        retain: bool,
        window: NonZeroUsize,
    ) -> Result<(), AppError> {
        let mut pipe = self.publish_window(window);
        for message in messages {
            pipe.push_retry(message, qos, retain).await?;
        }
        pipe.drain().await
    }

    /// Subscribes to one or more topic filters and returns **per-filter** SUBACK reason codes.
    ///
    /// Reason codes are not collapsed to a single bool — callers must inspect each entry
    /// (e.g. [`SubscribeAckReason::NotAuthorized`] vs granted QoS).
    ///
    /// Accepts any [`IntoIterator`] of values convertible to [`Subscription`] — for example
    /// `[("topic", QoS::AtLeastOnce)]`, `[Subscription::new(...)]`, or a `Vec`.
    ///
    /// Awaits a matching SUBACK until [`ConnectOptions::ack_timeout`] elapses, then
    /// returns [`AppError::SubscribeAckTimeout`].
    pub async fn subscribe(
        &self,
        filters: impl IntoIterator<Item = impl Into<Subscription>>,
    ) -> Result<Vec<SubscribeAckReason>, AppError> {
        let filters: Vec<_> = filters
            .into_iter()
            .map(|sub| {
                let sub = sub.into();
                (sub.filter, sub.options)
            })
            .collect();
        let (reply_tx, reply_rx) = oneshot::channel();
        self.try_send_command(ClientCommand::Subscribe {
            filters,
            reply: reply_tx,
        })?;
        await_ack(self.ack_timeout, reply_rx, AckKind::Subscribe).await
    }

    /// Unsubscribes from topic filters and returns **per-filter** UNSUBACK reason codes.
    ///
    /// Accepts any [`IntoIterator`] of values convertible to [`ByteString`] (e.g. `["a/b"]`,
    /// `vec![…]`).
    ///
    /// Awaits a matching UNSUBACK until [`ConnectOptions::ack_timeout`] elapses, then
    /// returns [`AppError::UnsubscribeAckTimeout`].
    pub async fn unsubscribe(
        &self,
        filters: impl IntoIterator<Item = impl Into<ByteString>>,
    ) -> Result<Vec<UnsubscribeAckReason>, AppError> {
        let filters: Vec<_> = filters.into_iter().map(Into::into).collect();
        let (reply_tx, reply_rx) = oneshot::channel();
        self.try_send_command(ClientCommand::Unsubscribe {
            filters,
            reply: reply_tx,
        })?;
        await_ack(self.ack_timeout, reply_rx, AckKind::Unsubscribe).await
    }

    /// Requests graceful shutdown of the connection driver.
    ///
    /// Signals the priority shutdown notify (not starved by a full command channel), best-effort
    /// enqueues `Shutdown` for DISCONNECT, and cancels the driver token so connect/backoff and
    /// an active session (session cancel is a child of this token) stop promptly.
    pub async fn shutdown(&self) -> Result<(), AppError> {
        self.shutdown_notify.cancel();
        let send_result = self.try_send_command(ClientCommand::Shutdown);
        self.cancel.cancel();
        match send_result {
            Ok(()) | Err(AppError::CommandChannelClosed) | Err(AppError::Backpressure(_)) => Ok(()),
            other => other,
        }
    }

    /// Test hook: fills the command channel with no-op commands.
    #[cfg(test)]
    #[inline]
    pub(crate) fn try_send_noop(&self) -> Result<(), AppError> {
        self.try_send_command(ClientCommand::Noop)
    }

    /// Test hook: registers a QoS-1 publish waiter on the live write task (no encode).
    #[cfg(test)]
    #[inline]
    pub(crate) fn try_insert_publish_waiter(
        &self,
        packet_id: std::num::NonZeroU16,
        reply: crate::connection::PublishReply,
    ) -> Result<(), AppError> {
        self.try_send_command(ClientCommand::InsertPublishWaiter { packet_id, reply })
    }

    /// Test hook: registers a subscribe waiter on the live write task (no encode).
    #[cfg(test)]
    #[inline]
    pub(crate) fn try_insert_subscribe_waiter(
        &self,
        packet_id: std::num::NonZeroU16,
        reply: crate::connection::SubscribeReply,
    ) -> Result<(), AppError> {
        self.try_send_command(ClientCommand::InsertSubscribeWaiter { packet_id, reply })
    }

    /// Test hook: registers an unsubscribe waiter on the live write task (no encode).
    #[cfg(test)]
    #[inline]
    pub(crate) fn try_insert_unsubscribe_waiter(
        &self,
        packet_id: std::num::NonZeroU16,
        reply: crate::connection::UnsubscribeReply,
    ) -> Result<(), AppError> {
        self.try_send_command(ClientCommand::InsertUnsubscribeWaiter { packet_id, reply })
    }
}

enum AckKind {
    Publish,
    Subscribe,
    Unsubscribe,
}

async fn await_ack<T>(
    deadline: Duration,
    reply_rx: oneshot::Receiver<Result<T, AppError>>,
    kind: AckKind,
) -> Result<T, AppError> {
    match timeout(deadline, reply_rx).await {
        Ok(Ok(result)) => result,
        Ok(Err(_closed)) => Err(AppError::CommandChannelClosed),
        Err(_elapsed) => match kind {
            AckKind::Publish => Err(AppError::PublishAckTimeout(deadline)),
            AckKind::Subscribe => Err(AppError::SubscribeAckTimeout(deadline)),
            AckKind::Unsubscribe => Err(AppError::UnsubscribeAckTimeout(deadline)),
        },
    }
}

/// Establishes an MQTT 5 session and returns a client handle, driver, and phase watch.
///
/// `inbound` is the injectable [`InboundSink`] for inbound PUBLISH delivery (for example
/// a bounded `flume::Sender` or `tokio::sync::mpsc::Sender`).
///
/// `lifecycle` is a required [`LifecycleHook`] (pass [`crate::NoopLifecycle`] for no side
/// effects). It receives synchronous [`SessionPhase`] callbacks (`Connected` /
/// `Disconnected` only); the returned [`watch::Receiver`] is the source of truth for
/// the current phase. Applications **SHOULD** use the hook **or** the watch for side
/// effects, not both.
///
/// Wires per-connection channels and returns immediately; the caller must spawn and
/// await [`Connection::run`], which performs TCP + CONNECT/CONNACK.
pub fn connect<H: InboundSink>(
    options: ConnectOptions,
    credentials: impl CredentialsProvider + 'static,
    reconnect: impl ReconnectPolicy + 'static,
    inbound: H,
    lifecycle: impl LifecycleHook + 'static,
) -> Result<(MqttClient, Connection<H>, watch::Receiver<SessionPhase>), ConnectError> {
    connect_with_shared(
        options,
        SharedCredentials::new(credentials),
        Arc::new(reconnect),
        inbound,
        Arc::new(lifecycle),
    )
}

/// Like [`connect`], but takes already-erased credentials / policy / lifecycle arcs.
///
/// Used by [`ClientBuilder`] to avoid double-wrapping providers that are already shared.
pub(crate) fn connect_with_shared<H: InboundSink>(
    options: ConnectOptions,
    credentials: SharedCredentials,
    reconnect: Arc<dyn ReconnectPolicy>,
    inbound: H,
    lifecycle: Arc<dyn LifecycleHook>,
) -> Result<(MqttClient, Connection<H>, watch::Receiver<SessionPhase>), ConnectError> {
    if inbound.is_closed() {
        return Err(ConnectError::Application(AppError::InboundSinkClosed));
    }

    let command_channel_capacity = options.command_channel_capacity.get();
    let (command_tx, command_rx) = mpsc::channel(command_channel_capacity);
    let command_tx = Arc::new(ArcSwap::from_pointee(command_tx));
    let (control_tx, control_rx) = mpsc::channel(control_channel_capacity(options.receive_maximum));
    let (phase_tx, phase_rx) = watch::channel(SessionPhase::initial());
    #[cfg(test)]
    let (read_stub_exit_tx, read_stub_exit_rx) = mpsc::channel(1);
    #[cfg(test)]
    let (write_stub_exit_tx, write_stub_exit_rx) = mpsc::channel(1);
    let cancel = CancellationToken::new();
    let shutdown_notify = CancellationToken::new();
    let ack_timeout = options.ack_timeout;

    let handoff_anchor = inbound.clone();
    let channels = ConnectionChannels {
        control_tx,
        control_rx,
        command_tx: Arc::clone(&command_tx),
        command_rx,
        handoff: inbound,
        handoff_anchor,
        phase_tx,
        lifecycle,
        #[cfg(test)]
        read_stub_exit_tx,
        #[cfg(test)]
        read_stub_exit_rx,
        #[cfg(test)]
        write_stub_exit_tx,
        #[cfg(test)]
        write_stub_exit_rx,
    };

    let connection = Connection::new(
        options,
        credentials,
        reconnect,
        channels,
        cancel.clone(),
        shutdown_notify.clone(),
    );

    Ok((
        MqttClient::new(command_tx, cancel, shutdown_notify, ack_timeout),
        connection,
        phase_rx,
    ))
}

#[cfg(test)]
#[allow(
    clippy::box_default,
    clippy::field_reassign_with_default,
    clippy::manual_async_fn,
    clippy::type_complexity
)]
mod tests {
    use std::num::{NonZeroU16, NonZeroUsize};
    use std::sync::Arc;

    use bytes::Bytes;
    use bytestring::ByteString;
    use futures_util::{SinkExt, StreamExt};
    use rstest::rstest;
    use tokio::time::timeout;

    use super::*;
    use crate::codec::{
        ConnectAck, DecodedFrame, Packet, PublishAck, PublishProperties, SubscribeAck, SubscribeAckReason,
        UnsubscribeAck, UnsubscribeAckReason,
    };
    use crate::connection::test_support::{
        NoReconnect, accept_connect_send_connack, accept_connect_send_connack_with, await_connected, spawn_broker_once,
        spawn_mock_broker, static_test_credentials,
    };
    use crate::connection::{COMMAND_CHANNEL_CAPACITY, ControlMessage, Generation, NoopLifecycle};
    use crate::options::ConnectOptions;
    use crate::util::BrokerUrl;

    /// `(filter, qos)` converts to [`Subscription`] for ergonomic [`MqttClient::subscribe`].
    #[test]
    fn subscription_from_filter_qos_tuple() {
        let sub = Subscription::from(("sensors/+/temp", QoS::AtLeastOnce));
        assert_eq!(&*sub.filter, "sensors/+/temp");
        assert_eq!(sub.options.qos, QoS::AtLeastOnce);
    }

    #[test]
    fn subscription_options_builders() {
        let sub = Subscription::new("t", QoS::AtLeastOnce)
            .no_local(true)
            .retain_as_published(true)
            .retain_handling(RetainHandling::NoAtSubscribe);
        assert!(sub.options.no_local);
        assert!(sub.options.retain_as_published);
        assert_eq!(sub.options.retain_handling, RetainHandling::NoAtSubscribe);
    }

    /// Reconnects immediately after transport/protocol outcomes — used by tests that never wire a
    /// live mock broker and only need `connect` to succeed (e.g. backpressure boundary checks).
    struct ImmediateReconnect;

    impl ReconnectPolicy for ImmediateReconnect {
        fn delay_after(&self, outcome: &ConnectionOutcome) -> Option<Duration> {
            match outcome {
                ConnectionOutcome::Shutdown(_) | ConnectionOutcome::Application(_) => None,
                _ => Some(Duration::ZERO),
            }
        }
    }

    /// Which of SUBACK / UNSUBACK / PUBACK a parameterized test exercises.
    #[derive(Clone, Copy, Debug)]
    enum AckKind {
        Subscribe,
        Unsubscribe,
        Publish,
    }

    /// Wires a client against `port` with `receive_maximum=8` and [`NoReconnect`] (single mock session).
    async fn connect_test_client(port: u16) -> (MqttClient, Connection, watch::Receiver<SessionPhase>) {
        let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
        let options = ConnectOptions::new(url).receive_maximum(NonZeroU16::new(8).expect("nz"));
        let (tx, _rx) = flume::bounded::<PublishMessage>(4);
        connect(options, static_test_credentials(), NoReconnect, tx, NoopLifecycle).expect("connect wiring")
    }

    /// [`connect_test_client`], then spawns `run()` and waits for the live connection generation.
    async fn connect_with_live_generation(
        port: u16,
    ) -> (
        MqttClient,
        tokio::task::JoinHandle<Result<(), ConnectionOutcome>>,
        mpsc::Sender<ControlMessage>,
        Generation,
    ) {
        let (client, connection, mut phase) = connect_test_client(port).await;
        let control = connection.control_sender();
        let run = tokio::spawn(connection.run());

        await_connected(&mut phase).await;
        let live_generation = match &*phase.borrow() {
            SessionPhase::Connected { generation, .. } => Generation(*generation),
            other => panic!("expected Connected, got {other:?}"),
        };
        assert!(
            live_generation.0 >= 1,
            "successful connect must bump the connection generation"
        );

        (client, run, control, live_generation)
    }

    /// Live Connection path: a stale-generation ack must not complete the waiter, but the same
    /// ack tagged with the live generation must.
    async fn assert_stale_generation_then_live_completes<T>(
        control: &mpsc::Sender<ControlMessage>,
        mut reply_rx: oneshot::Receiver<Result<T, AppError>>,
        stale_message: ControlMessage,
        live_message: ControlMessage,
    ) {
        control
            .send(stale_message)
            .await
            .expect("inject stale-generation ack on real control path");
        assert!(
            timeout(Duration::from_millis(100), &mut reply_rx).await.is_err(),
            "ack from a prior Connection generation must not complete the live waiter"
        );

        control.send(live_message).await.expect("inject current-generation ack");
        let completed = timeout(Duration::from_secs(2), reply_rx)
            .await
            .expect("current-generation ack must complete the waiter")
            .expect("oneshot open");
        assert!(completed.is_ok());
    }

    fn sample_message(topic: &str, payload: &'static [u8]) -> PublishMessage {
        PublishMessage {
            topic: ByteString::from(topic.to_owned()),
            payload: Bytes::from_static(payload),
            properties: PublishProperties::default(),
        }
    }

    /// CONNACK, then SUBACK/UNSUBACK with mixed per-filter reason codes echoing the request packet id.
    async fn spawn_mock_broker_suback_mixed_reasons() -> u16 {
        spawn_broker_once(|mut framed_read, mut framed_write| async move {
            if !accept_connect_send_connack(&mut framed_read, &mut framed_write).await {
                return;
            }
            loop {
                match framed_read.next().await {
                    Some(Ok((DecodedFrame::Control(Packet::Subscribe(sub)), _))) => {
                        let _ = framed_write
                            .send(Packet::SubscribeAck(SubscribeAck {
                                packet_id: sub.packet_id,
                                properties: Vec::new(),
                                reason_string: None,
                                status: vec![SubscribeAckReason::GrantedQos1, SubscribeAckReason::NotAuthorized],
                            }))
                            .await;
                    }
                    Some(Ok((DecodedFrame::Control(Packet::Unsubscribe(unsub)), _))) => {
                        let _ = framed_write
                            .send(Packet::UnsubscribeAck(UnsubscribeAck {
                                packet_id: unsub.packet_id,
                                properties: Vec::new(),
                                reason_string: None,
                                status: vec![
                                    UnsubscribeAckReason::Success,
                                    UnsubscribeAckReason::NoSubscriptionExisted,
                                ],
                            }))
                            .await;
                    }
                    Some(Ok(_)) => {}
                    Some(Err(_)) | None => break,
                }
            }
        })
        .await
    }

    /// CONNACK (with a caller-chosen `server_receive_maximum`), then record outbound PUBLISH
    /// frames and echo PUBACK for QoS 1.
    async fn spawn_mock_broker_record_publishes(
        observed: Arc<tokio::sync::Mutex<Vec<(QoS, Option<NonZeroU16>, Bytes)>>>,
        server_receive_maximum: NonZeroU16,
    ) -> u16 {
        spawn_broker_once(move |mut framed_read, mut framed_write| async move {
            let mut ack = ConnectAck::default();
            ack.receive_max = server_receive_maximum;
            if !accept_connect_send_connack_with(&mut framed_read, &mut framed_write, ack).await {
                return;
            }
            loop {
                match framed_read.next().await {
                    Some(Ok((DecodedFrame::Publish(packet), _))) => {
                        let qos = packet.meta.qos;
                        let packet_id = packet.meta.packet_id;
                        let payload = packet.message.payload.clone();
                        observed.lock().await.push((qos, packet_id, payload));
                        if qos == QoS::AtLeastOnce {
                            let Some(id) = packet_id else {
                                continue;
                            };
                            let _ = framed_write
                                .send(Packet::PublishAck(PublishAck {
                                    packet_id: id,
                                    reason_code: crate::codec::PublishAckReason::Success,
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
        .await
    }

    /// CONNACK, then duplicate the SUBACK/UNSUBACK/PUBACK for whichever request arrives — used by
    /// [`duplicate_ack_after_completion_is_harmless`] across all three ack kinds.
    async fn spawn_duplicate_ack_broker() -> u16 {
        spawn_broker_once(|mut framed_read, mut framed_write| async move {
            if !accept_connect_send_connack(&mut framed_read, &mut framed_write).await {
                return;
            }
            loop {
                match framed_read.next().await {
                    Some(Ok((DecodedFrame::Control(Packet::Subscribe(sub)), _))) => {
                        let ack = Packet::SubscribeAck(SubscribeAck {
                            packet_id: sub.packet_id,
                            properties: Vec::new(),
                            reason_string: None,
                            status: vec![SubscribeAckReason::GrantedQos1],
                        });
                        let _ = framed_write.send(ack.clone()).await;
                        // Late / duplicate ack for the same id.
                        let _ = framed_write.send(ack).await;
                    }
                    Some(Ok((DecodedFrame::Control(Packet::Unsubscribe(unsub)), _))) => {
                        let ack = Packet::UnsubscribeAck(UnsubscribeAck {
                            packet_id: unsub.packet_id,
                            properties: Vec::new(),
                            reason_string: None,
                            status: vec![UnsubscribeAckReason::Success],
                        });
                        let _ = framed_write.send(ack.clone()).await;
                        let _ = framed_write.send(ack).await;
                    }
                    Some(Ok((DecodedFrame::Publish(packet), _))) => {
                        let Some(id) = packet.meta.packet_id else {
                            continue;
                        };
                        let ack = Packet::PublishAck(PublishAck {
                            packet_id: id,
                            reason_code: crate::codec::PublishAckReason::Success,
                            properties: Vec::new(),
                            reason_string: None,
                        });
                        let _ = framed_write.send(ack.clone()).await;
                        let _ = framed_write.send(ack).await;
                    }
                    Some(Ok((DecodedFrame::Control(Packet::Disconnect(_)), _))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(_)) => break,
                }
            }
        })
        .await
    }

    /// Shutdown has priority over a saturated command channel — must not stick on
    /// [`AppError::Backpressure`] as the only outcome, and an active session must still stop
    /// (cancel alone is not enough while `session_cancel` is not a child of client cancel).
    #[tokio::test]
    async fn shutdown_wins_when_command_channel_full() {
        let port = spawn_mock_broker().await;
        let (client, connection, mut phase) = connect_test_client(port).await;

        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        for _ in 0..COMMAND_CHANNEL_CAPACITY {
            client.try_send_noop().expect("channel has capacity");
        }

        let shutdown = timeout(Duration::from_secs(3), client.shutdown())
            .await
            .expect("shutdown must not hang behind a full command channel");
        assert!(
            shutdown.is_ok(),
            "shutdown must complete despite full command channel, got {shutdown:?}"
        );

        let result = timeout(Duration::from_secs(5), run)
            .await
            .expect(
                "connection.run must finish within graceful budget when shutdown wins over a full \
                 command channel (active session must stop even if Shutdown command cannot enqueue)",
            )
            .expect("run join");
        assert!(
            matches!(result, Err(ConnectionOutcome::Shutdown(_)) | Ok(())),
            "expected shutdown outcome, got {result:?}"
        );
    }

    /// SUBACK reason codes must reach the app unchanged (no collapse to a single bool).
    #[tokio::test]
    async fn subscribe_returns_per_filter_reason_codes() {
        let port = spawn_mock_broker_suback_mixed_reasons().await;
        let (client, connection, mut phase) = connect_test_client(port).await;

        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        let reasons = client
            .subscribe([("ok/topic", QoS::AtLeastOnce), ("denied/topic", QoS::AtLeastOnce)])
            .await
            .expect("subscribe must return per-filter SUBACK reasons");

        assert_eq!(
            reasons,
            vec![SubscribeAckReason::GrantedQos1, SubscribeAckReason::NotAuthorized,],
            "reason codes must pass through unchanged (not collapsed to bool)"
        );

        client.shutdown().await.expect("shutdown");
        let _ = run.await;
    }

    /// Missing SUBACK must surface a typed timeout (not hang without a deadline).
    #[tokio::test]
    async fn subscribe_times_out_without_suback() {
        let port = spawn_mock_broker().await;
        let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
        let options = ConnectOptions::new(url)
            .receive_maximum(NonZeroU16::new(8).expect("nz"))
            .ack_timeout(Duration::from_millis(50));
        let (tx, _rx) = flume::bounded::<PublishMessage>(4);
        let (client, connection, mut phase) =
            connect(options, static_test_credentials(), NoReconnect, tx, NoopLifecycle).expect("connect wiring");

        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        let err = timeout(Duration::from_secs(2), client.subscribe([("t/1", QoS::AtMostOnce)]))
            .await
            .expect("subscribe must not hang past the outer test deadline")
            .expect_err("missing SUBACK must be a typed error");

        assert!(
            matches!(err, AppError::SubscribeAckTimeout(_)),
            "expected AppError::SubscribeAckTimeout, got {err:?}"
        );

        client.shutdown().await.expect("shutdown");
        let _ = run.await;
    }

    /// UNSUBACK reason codes must reach the app per filter when unsubscribe is used.
    #[tokio::test]
    async fn unsubscribe_returns_per_filter_reason_codes() {
        let port = spawn_mock_broker_suback_mixed_reasons().await;
        let (client, connection, mut phase) = connect_test_client(port).await;

        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        let reasons = client
            .unsubscribe(["a/b", "c/d"])
            .await
            .expect("unsubscribe must return per-filter UNSUBACK reasons");

        assert_eq!(
            reasons,
            vec![
                UnsubscribeAckReason::Success,
                UnsubscribeAckReason::NoSubscriptionExisted,
            ]
        );

        client.shutdown().await.expect("shutdown");
        let _ = run.await;
    }

    // --- publish / flow-control contracts ---

    /// QoS 0: no packet-id / publish-permit / PUBACK wait — message must reach the wire.
    #[tokio::test]
    async fn qos0_publish_reaches_wire_without_packet_id() {
        let observed = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let port = spawn_mock_broker_record_publishes(Arc::clone(&observed), NonZeroU16::new(8).expect("nz")).await;
        let (client, connection, mut phase) = connect_test_client(port).await;

        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        timeout(
            Duration::from_secs(2),
            client.publish(sample_message("t/qos0", b"fire"), QoS::AtMostOnce, false),
        )
        .await
        .expect("QoS 0 publish must not hang waiting for PUBACK")
        .expect("QoS 0 publish must succeed");

        let frames = timeout(Duration::from_secs(2), async {
            loop {
                let guard = observed.lock().await;
                if !guard.is_empty() {
                    return guard.clone();
                }
                drop(guard);
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("QoS 0 PUBLISH must appear on the wire");

        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].0, QoS::AtMostOnce);
        assert_eq!(frames[0].1, None, "QoS 0 must not carry a packet identifier");
        assert_eq!(&*frames[0].2, b"fire");

        client.shutdown().await.expect("shutdown");
        let _ = run.await;
    }

    /// QoS 1: `publish().await` resolves only after PUBACK (mock broker).
    #[tokio::test]
    async fn qos1_publish_awaits_matching_puback() {
        let observed = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let port = spawn_mock_broker_record_publishes(Arc::clone(&observed), NonZeroU16::new(8).expect("nz")).await;
        let (client, connection, mut phase) = connect_test_client(port).await;

        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        client
            .publish(sample_message("t/qos1", b"need-ack"), QoS::AtLeastOnce, false)
            .await
            .expect("QoS 1 publish must complete after PUBACK");

        let frames = observed.lock().await.clone();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].0, QoS::AtLeastOnce);
        assert!(frames[0].1.is_some(), "QoS 1 must allocate a packet identifier");

        client.shutdown().await.expect("shutdown");
        let _ = run.await;
    }

    /// QoS 1: `start_publish` enqueues immediately and its completion waits for PUBACK.
    #[tokio::test]
    async fn qos1_start_publish_completion_awaits_matching_puback() {
        let observed = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let port = spawn_mock_broker_record_publishes(Arc::clone(&observed), NonZeroU16::new(8).expect("nz")).await;
        let (client, connection, mut phase) = connect_test_client(port).await;

        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        client
            .start_publish(sample_message("t/qos1-start", b"need-ack"), QoS::AtLeastOnce, false)
            .expect("QoS 1 publish must enqueue")
            .wait()
            .await
            .expect("QoS 1 completion must resolve after PUBACK");

        let frames = observed.lock().await.clone();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].0, QoS::AtLeastOnce);
        assert!(frames[0].1.is_some(), "QoS 1 must allocate a packet identifier");

        client.shutdown().await.expect("shutdown");
        let _ = run.await;
    }

    /// Barrier: `publish().await` stays pending until the matching PUBACK is released.
    #[tokio::test]
    async fn qos1_publish_future_stays_pending_until_delayed_puback() {
        let observed = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let (release_tx, release_rx) = oneshot::channel::<()>();
        let release_rx = Arc::new(tokio::sync::Mutex::new(Some(release_rx)));

        let observed_broker = Arc::clone(&observed);
        let release_slot = Arc::clone(&release_rx);
        let port = spawn_broker_once(move |mut framed_read, mut framed_write| async move {
            if !accept_connect_send_connack(&mut framed_read, &mut framed_write).await {
                return;
            }
            loop {
                match framed_read.next().await {
                    Some(Ok((DecodedFrame::Publish(packet), _))) => {
                        let packet_id = packet.meta.packet_id;
                        observed_broker
                            .lock()
                            .await
                            .push((packet.meta.qos, packet_id, packet.message.payload.clone()));
                        if packet.meta.qos != QoS::AtLeastOnce {
                            continue;
                        }
                        let Some(id) = packet_id else {
                            continue;
                        };
                        // Hold PUBACK until the test releases the barrier.
                        if let Some(rx) = release_slot.lock().await.take() {
                            let _ = rx.await;
                        }
                        let _ = framed_write
                            .send(Packet::PublishAck(PublishAck {
                                packet_id: id,
                                reason_code: crate::codec::PublishAckReason::Success,
                                properties: Vec::new(),
                                reason_string: None,
                            }))
                            .await;
                    }
                    Some(Ok(_)) => {}
                    Some(Err(_)) | None => break,
                }
            }
        })
        .await;

        let (client, connection, mut phase) = connect_test_client(port).await;

        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        let client_shutdown = client.clone();
        let publish_task = tokio::spawn(async move {
            client
                .publish(sample_message("t/barrier", b"wait"), QoS::AtLeastOnce, false)
                .await
        });

        // Wait until the PUBLISH is on the wire (driver has sent it) while PUBACK is still held.
        timeout(Duration::from_secs(2), async {
            loop {
                if !observed.lock().await.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("QoS 1 PUBLISH must reach the wire before PUBACK");

        // publish().await must stay pending until matching PUBACK, not merely after encode.
        assert!(
            !publish_task.is_finished(),
            "publish().await must remain pending until the matching PUBACK is observed"
        );

        // Short timeout must elapse without completion while the barrier holds.
        assert!(
            timeout(Duration::from_millis(100), async {
                while !publish_task.is_finished() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .is_err(),
            "publish must not resolve while PUBACK is delayed"
        );

        release_tx.send(()).expect("release PUBACK barrier");

        timeout(Duration::from_secs(2), async {
            publish_task.await.expect("publish task join")
        })
        .await
        .expect("publish must complete after delayed PUBACK")
        .expect("QoS 1 publish after PUBACK");

        client_shutdown.shutdown().await.expect("shutdown");
        let _ = run.await;
    }

    /// Wrong Packet Identifier PUBACK must not complete the publish; matching id does.
    #[tokio::test]
    async fn qos1_publish_ignores_wrong_packet_id_puback_until_match() {
        let observed = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let (release_tx, release_rx) = oneshot::channel::<()>();
        let release_rx = Arc::new(tokio::sync::Mutex::new(Some(release_rx)));

        let observed_broker = Arc::clone(&observed);
        let release_slot = Arc::clone(&release_rx);
        let port = spawn_broker_once(move |mut framed_read, mut framed_write| async move {
            if !accept_connect_send_connack(&mut framed_read, &mut framed_write).await {
                return;
            }
            loop {
                match framed_read.next().await {
                    Some(Ok((DecodedFrame::Publish(packet), _))) => {
                        let packet_id = packet.meta.packet_id;
                        observed_broker
                            .lock()
                            .await
                            .push((packet.meta.qos, packet_id, packet.message.payload.clone()));
                        if packet.meta.qos != QoS::AtLeastOnce {
                            continue;
                        }
                        let Some(id) = packet_id else {
                            continue;
                        };
                        let wrong_id = NonZeroU16::new(u16::from(id).wrapping_add(1).max(1)).expect("nz");
                        let _ = framed_write
                            .send(Packet::PublishAck(PublishAck {
                                packet_id: wrong_id,
                                reason_code: crate::codec::PublishAckReason::Success,
                                properties: Vec::new(),
                                reason_string: None,
                            }))
                            .await;
                        // Hold matching PUBACK until the test asserts the future is still pending.
                        if let Some(rx) = release_slot.lock().await.take() {
                            let _ = rx.await;
                        }
                        let _ = framed_write
                            .send(Packet::PublishAck(PublishAck {
                                packet_id: id,
                                reason_code: crate::codec::PublishAckReason::Success,
                                properties: Vec::new(),
                                reason_string: None,
                            }))
                            .await;
                    }
                    Some(Ok(_)) => {}
                    Some(Err(_)) | None => break,
                }
            }
        })
        .await;

        let (client, connection, mut phase) = connect_test_client(port).await;

        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        let client_shutdown = client.clone();
        let publish_task = tokio::spawn(async move {
            client
                .publish(sample_message("t/wrong-id", b"wait"), QoS::AtLeastOnce, false)
                .await
        });

        timeout(Duration::from_secs(2), async {
            loop {
                if !observed.lock().await.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("QoS 1 PUBLISH must reach the wire before PUBACK");

        // Wrong-id PUBACK must not complete the future.
        assert!(
            !publish_task.is_finished(),
            "publish().await must remain pending until the matching PUBACK is observed"
        );
        assert!(
            timeout(Duration::from_millis(100), async {
                while !publish_task.is_finished() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .is_err(),
            "publish must not resolve on a PUBACK with a non-matching packet id"
        );

        release_tx.send(()).expect("release matching PUBACK");

        timeout(Duration::from_secs(2), async {
            publish_task.await.expect("publish task join")
        })
        .await
        .expect("publish must complete after matching PUBACK")
        .expect("QoS 1 publish after matching PUBACK");

        client_shutdown.shutdown().await.expect("shutdown");
        let _ = run.await;
    }

    /// QoS 0 needs no publish-inflight permit: still reaches the wire when QoS 1 capacity is full.
    #[tokio::test]
    async fn qos0_publish_reaches_wire_while_qos1_inflight_capacity_exhausted() {
        let observed = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let observed_broker = Arc::clone(&observed);
        let port = spawn_broker_once(move |mut framed_read, mut framed_write| async move {
            let mut ack = ConnectAck::default();
            ack.receive_max = NonZeroU16::new(1).expect("nz");
            if !accept_connect_send_connack_with(&mut framed_read, &mut framed_write, ack).await {
                return;
            }
            // Never PUBACK — keep the single QoS1 permit exhausted.
            while let Some(Ok(frame)) = framed_read.next().await {
                if let (DecodedFrame::Publish(packet), _) = frame {
                    observed_broker.lock().await.push((
                        packet.meta.qos,
                        packet.meta.packet_id,
                        packet.message.payload.clone(),
                    ));
                }
            }
        })
        .await;

        let (client, connection, mut phase) = connect_test_client(port).await;

        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        let client_qos0 = client.clone();
        let qos1 = tokio::spawn(async move {
            client
                .publish(sample_message("t/qos1-hold", b"hold"), QoS::AtLeastOnce, false)
                .await
        });

        timeout(Duration::from_secs(2), async {
            loop {
                let frames = observed.lock().await;
                if frames.iter().any(|(qos, _, _)| *qos == QoS::AtLeastOnce) {
                    break;
                }
                drop(frames);
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("QoS 1 PUBLISH must occupy the single server_receive_maximum slot");

        timeout(
            Duration::from_secs(2),
            client_qos0.publish(sample_message("t/qos0-free", b"free"), QoS::AtMostOnce, false),
        )
        .await
        .expect("QoS 0 must not block on publish permits")
        .expect("QoS 0 publish must succeed without a QoS1 permit");

        let frames = timeout(Duration::from_secs(2), async {
            loop {
                let guard = observed.lock().await;
                if guard
                    .iter()
                    .any(|(qos, _, payload)| *qos == QoS::AtMostOnce && payload.as_ref() == b"free")
                {
                    return guard.clone();
                }
                drop(guard);
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("QoS 0 PUBLISH must appear on the wire while QoS1 capacity is exhausted");

        let qos0 = frames
            .iter()
            .find(|(qos, _, payload)| *qos == QoS::AtMostOnce && payload.as_ref() == b"free")
            .expect("qos0 frame");
        assert_eq!(qos0.1, None, "QoS 0 must not take a packet identifier / permit");

        qos1.abort();
        client_qos0.shutdown().await.expect("shutdown");
        let _ = run.await;
    }

    /// Full command queue → `AppError::Backpressure` on publish (no unbounded growth).
    #[tokio::test]
    async fn publish_returns_backpressure_when_command_queue_full() {
        let url = BrokerUrl::parse("mqtt://localhost").expect("valid url");
        let options = ConnectOptions::new(url).receive_maximum(NonZeroU16::new(8).expect("nz"));
        let (tx, _rx) = flume::bounded::<PublishMessage>(4);
        let (client, _connection, _phase) = connect(
            options,
            static_test_credentials(),
            ImmediateReconnect,
            tx,
            NoopLifecycle,
        )
        .expect("connect");

        for _ in 0..COMMAND_CHANNEL_CAPACITY {
            client.try_send_noop().expect("channel has capacity");
        }

        let err = client
            .publish(sample_message("t/bp", b"x"), QoS::AtMostOnce, false)
            .await
            .expect_err("full command channel must backpressure publish");
        assert!(
            matches!(err, AppError::Backpressure(_)),
            "expected Backpressure, got {err:?}"
        );
    }

    /// Builder override of command channel capacity applies to backpressure boundary.
    #[tokio::test]
    async fn publish_returns_backpressure_at_custom_command_queue_capacity() {
        const CUSTOM_CAPACITY: usize = 4;
        let url = BrokerUrl::parse("mqtt://localhost").expect("valid url");
        let options = ConnectOptions::new(url)
            .receive_maximum(NonZeroU16::new(8).expect("nz"))
            .command_channel_capacity(NonZeroUsize::new(CUSTOM_CAPACITY).expect("nz"));
        let (tx, _rx) = flume::bounded::<PublishMessage>(4);
        let (client, _connection, _phase) = connect(
            options,
            static_test_credentials(),
            ImmediateReconnect,
            tx,
            NoopLifecycle,
        )
        .expect("connect");

        for _ in 0..CUSTOM_CAPACITY {
            client.try_send_noop().expect("channel has capacity");
        }

        let err = client
            .publish(sample_message("t/bp-custom", b"x"), QoS::AtMostOnce, false)
            .await
            .expect_err("full custom command channel must backpressure publish");
        assert!(
            matches!(err, AppError::Backpressure(_)),
            "expected Backpressure, got {err:?}"
        );
    }

    /// CONNACK `server_receive_maximum` caps outbound QoS 1 inflight (not only ConnectOptions).
    #[tokio::test]
    async fn outbound_qos1_inflight_capped_by_connack_server_receive_maximum() {
        let observed = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        // Broker advertises Receive Maximum = 1 and withholds PUBACK so the window stays full.
        let observed_broker = Arc::clone(&observed);
        let port = spawn_broker_once(move |mut framed_read, mut framed_write| async move {
            let mut ack = ConnectAck::default();
            ack.receive_max = NonZeroU16::new(1).expect("nz");
            if !accept_connect_send_connack_with(&mut framed_read, &mut framed_write, ack).await {
                return;
            }
            // Intentionally no PUBACK — keep the server Receive Maximum window full.
            while let Some(Ok(frame)) = framed_read.next().await {
                if let (DecodedFrame::Publish(packet), _) = frame {
                    observed_broker.lock().await.push((
                        packet.meta.qos,
                        packet.meta.packet_id,
                        packet.message.payload.clone(),
                    ));
                }
            }
        })
        .await;

        let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
        // Client CONNECT Receive Maximum is large; CONNACK server window must win for outbound.
        let options = ConnectOptions::new(url).receive_maximum(NonZeroU16::new(64).expect("nz"));
        let (tx, _rx) = flume::bounded::<PublishMessage>(4);
        let (client, connection, mut phase) =
            connect(options, static_test_credentials(), NoReconnect, tx, NoopLifecycle).expect("connect wiring");

        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        let client_shutdown = client.clone();
        let client2 = client.clone();
        let first = tokio::spawn(async move {
            client
                .publish(sample_message("t/a", b"1"), QoS::AtLeastOnce, false)
                .await
        });
        tokio::task::yield_now().await;
        let second = tokio::spawn(async move {
            client2
                .publish(sample_message("t/b", b"2"), QoS::AtLeastOnce, false)
                .await
        });

        tokio::time::sleep(Duration::from_millis(200)).await;
        let on_wire = observed.lock().await.len();
        assert_eq!(
            on_wire, 1,
            "with CONNACK server_receive_maximum=1 and withheld PUBACK, exactly one QoS1 PUBLISH may be on the wire; saw {on_wire}"
        );

        first.abort();
        second.abort();
        client_shutdown.shutdown().await.expect("shutdown");
        let _ = run.await;
    }

    /// CONNACK `server_receive_maximum=1` with withheld PUBACK: second QoS1 `publish` returns
    /// [`AppError::Backpressure`] promptly (no hang / abort-without-assert).
    #[tokio::test]
    async fn qos1_publish_returns_backpressure_when_inflight_exhausted() {
        use tokio::sync::Notify;

        let first_publish_seen = Arc::new(Notify::new());
        let first_publish_seen_broker = Arc::clone(&first_publish_seen);
        let port = spawn_broker_once(move |mut framed_read, mut framed_write| async move {
            let mut ack = ConnectAck::default();
            ack.receive_max = NonZeroU16::new(1).expect("nz");
            if !accept_connect_send_connack_with(&mut framed_read, &mut framed_write, ack).await {
                return;
            }
            // Withhold PUBACKs so the outbound QoS1 window stays full; signal first PUBLISH on wire.
            while let Some(Ok(frame)) = framed_read.next().await {
                if let (DecodedFrame::Publish(_), _) = frame {
                    first_publish_seen_broker.notify_one();
                }
            }
        })
        .await;

        let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
        let options = ConnectOptions::new(url).receive_maximum(NonZeroU16::new(64).expect("nz"));
        let (tx, _rx) = flume::bounded::<PublishMessage>(4);
        let (client, connection, mut phase) =
            connect(options, static_test_credentials(), NoReconnect, tx, NoopLifecycle).expect("connect wiring");

        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        let first = tokio::spawn({
            let client = client.clone();
            async move {
                client
                    .publish(sample_message("t/a", b"1"), QoS::AtLeastOnce, false)
                    .await
            }
        });
        // Wait until the broker observes the first QoS1 PUBLISH (slot occupied).
        timeout(Duration::from_secs(2), first_publish_seen.notified())
            .await
            .expect("first QoS1 PUBLISH must reach the broker before second publish");

        let err = timeout(
            Duration::from_secs(2),
            client.publish(sample_message("t/b", b"2"), QoS::AtLeastOnce, false),
        )
        .await
        .expect("second QoS1 publish must not hang when inflight is exhausted")
        .expect_err("second QoS1 publish must fail with typed Backpressure");

        assert!(
            matches!(err, AppError::Backpressure(_)),
            "expected AppError::Backpressure when CONNACK receive_max=1 window is full, got {err:?}"
        );

        first.abort();
        client.shutdown().await.expect("shutdown");
        let _ = run.await;
    }

    /// Rumqttc #1057 class: subscribe must complete while the outbound QoS1 window is full.
    #[tokio::test]
    async fn subscribe_completes_while_many_qos1_publishes_in_flight() {
        let observed = Arc::new(tokio::sync::Mutex::new(Vec::<(u8, NonZeroU16)>::new()));
        // Tuple: (1 = PUBLISH, 2 = SUBSCRIBE, packet_id)
        let observed_broker = Arc::clone(&observed);
        let port = spawn_broker_once(move |mut framed_read, mut framed_write| async move {
            let mut ack = ConnectAck::default();
            ack.receive_max = NonZeroU16::new(1).expect("nz");
            if !accept_connect_send_connack_with(&mut framed_read, &mut framed_write, ack).await {
                return;
            }
            loop {
                match framed_read.next().await {
                    Some(Ok((DecodedFrame::Publish(packet), _))) => {
                        if let Some(id) = packet.meta.packet_id {
                            observed_broker.lock().await.push((1, id));
                        }
                        // Withhold PUBACK — keep server Receive Maximum window full.
                    }
                    Some(Ok((DecodedFrame::Control(Packet::Subscribe(sub)), _))) => {
                        observed_broker.lock().await.push((2, sub.packet_id));
                        let _ = framed_write
                            .send(Packet::SubscribeAck(SubscribeAck {
                                packet_id: sub.packet_id,
                                properties: Vec::new(),
                                reason_string: None,
                                status: vec![SubscribeAckReason::GrantedQos1],
                            }))
                            .await;
                    }
                    Some(Ok((DecodedFrame::Control(Packet::Disconnect(_)), _))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(_)) => break,
                }
            }
        })
        .await;

        let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
        let options = ConnectOptions::new(url)
            .receive_maximum(NonZeroU16::new(64).expect("nz"))
            .ack_timeout(Duration::from_secs(2));
        let (tx, _rx) = flume::bounded::<PublishMessage>(4);
        let (client, connection, mut phase) =
            connect(options, static_test_credentials(), NoReconnect, tx, NoopLifecycle).expect("connect wiring");

        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        let publish_task = tokio::spawn({
            let client = client.clone();
            async move {
                client
                    .publish(sample_message("t/inflight", b"1"), QoS::AtLeastOnce, false)
                    .await
            }
        });
        // Fill the outbound QoS1 window before subscribe.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            observed.lock().await.iter().filter(|(kind, _)| *kind == 1).count(),
            1,
            "expected exactly one QoS1 PUBLISH on the wire before subscribe"
        );

        let reasons = timeout(
            Duration::from_secs(2),
            client.subscribe(vec![Subscription::new("t/sub", QoS::AtLeastOnce)]),
        )
        .await
        .expect("subscribe must complete with SUBACK while QoS1 publish window is full (no write-task starvation)")
        .expect("subscribe must succeed");
        assert_eq!(reasons, vec![SubscribeAckReason::GrantedQos1]);

        let frames = observed.lock().await.clone();
        let publish_id = frames
            .iter()
            .find_map(|(kind, id)| (*kind == 1).then_some(*id))
            .expect("PUBLISH packet id observed");
        let subscribe_id = frames
            .iter()
            .find_map(|(kind, id)| (*kind == 2).then_some(*id))
            .expect("SUBSCRIBE packet id observed");
        assert_ne!(
            publish_id, subscribe_id,
            "PUBLISH and SUBSCRIBE must use distinct packet identifiers"
        );

        publish_task.abort();
        client.shutdown().await.expect("shutdown");
        let _ = run.await;
    }

    /// A duplicate/late ack for an already-completed packet id must be ignored; the id stays reusable.
    #[rstest]
    #[case::subscribe(AckKind::Subscribe)]
    #[case::unsubscribe(AckKind::Unsubscribe)]
    #[case::publish(AckKind::Publish)]
    #[tokio::test]
    async fn duplicate_ack_after_completion_is_harmless(#[case] kind: AckKind) {
        let port = spawn_duplicate_ack_broker().await;
        let (client, connection, mut phase) = connect_test_client(port).await;

        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        match kind {
            AckKind::Subscribe => {
                let first = client
                    .subscribe(vec![Subscription::new("t/dup", QoS::AtLeastOnce)])
                    .await
                    .expect("first SUBACK completes subscribe");
                assert_eq!(first, vec![SubscribeAckReason::GrantedQos1]);

                // Second subscribe must still work (id freed after definitive completion; duplicate ignored).
                let second = client
                    .subscribe(vec![Subscription::new("t/dup2", QoS::AtLeastOnce)])
                    .await
                    .expect("packet id must be reusable after completion despite duplicate SUBACK");
                assert_eq!(second, vec![SubscribeAckReason::GrantedQos1]);
            }
            AckKind::Unsubscribe => {
                let first = client
                    .unsubscribe(["t/dup"])
                    .await
                    .expect("first UNSUBACK completes unsubscribe");
                assert_eq!(first, vec![UnsubscribeAckReason::Success]);

                // Second unsubscribe must still work (id freed after definitive completion; duplicate ignored).
                let second = client
                    .unsubscribe(["t/dup2"])
                    .await
                    .expect("packet id must be reusable after completion despite duplicate UNSUBACK");
                assert_eq!(second, vec![UnsubscribeAckReason::Success]);
            }
            AckKind::Publish => {
                client
                    .publish(sample_message("t/dup", b"once"), QoS::AtLeastOnce, false)
                    .await
                    .expect("first PUBACK completes publish");

                // Second publish must still work (id freed after definitive completion; duplicate ignored).
                client
                    .publish(sample_message("t/dup2", b"twice"), QoS::AtLeastOnce, false)
                    .await
                    .expect("packet id must be reusable after completion despite duplicate PUBACK");
            }
        }

        client.shutdown().await.expect("shutdown");
        let _ = run.await;
    }

    /// Live Connection path: an ack tagged with a prior generation must not complete a waiter
    /// bound to the write task's current generation.
    #[rstest]
    #[case::subscribe(AckKind::Subscribe)]
    #[case::unsubscribe(AckKind::Unsubscribe)]
    #[case::publish(AckKind::Publish)]
    #[tokio::test]
    async fn stale_generation_ack_does_not_complete_live_waiter(#[case] kind: AckKind) {
        let port = spawn_mock_broker().await;
        let (client, run, control, live_generation) = connect_with_live_generation(port).await;

        let packet_id = NonZeroU16::new(42).expect("nz");
        let stale = Generation(live_generation.0.saturating_sub(1));
        assert_ne!(stale, live_generation);

        match kind {
            AckKind::Subscribe => {
                let (reply_tx, reply_rx) = oneshot::channel();
                client
                    .try_insert_subscribe_waiter(packet_id, reply_tx)
                    .expect("insert subscribe waiter on live write task");
                // Allow the write task to process InsertSubscribeWaiter.
                tokio::task::yield_now().await;
                tokio::time::sleep(Duration::from_millis(30)).await;

                let ack = SubscribeAck {
                    packet_id,
                    properties: Vec::new(),
                    reason_string: None,
                    status: vec![SubscribeAckReason::GrantedQos1],
                };
                assert_stale_generation_then_live_completes(
                    &control,
                    reply_rx,
                    ControlMessage::SubscribeAck {
                        ack: ack.clone(),
                        generation: stale,
                    },
                    ControlMessage::SubscribeAck {
                        ack,
                        generation: live_generation,
                    },
                )
                .await;
            }
            AckKind::Unsubscribe => {
                let (reply_tx, reply_rx) = oneshot::channel();
                client
                    .try_insert_unsubscribe_waiter(packet_id, reply_tx)
                    .expect("insert unsubscribe waiter on live write task");
                // Allow the write task to process InsertUnsubscribeWaiter.
                tokio::task::yield_now().await;
                tokio::time::sleep(Duration::from_millis(30)).await;

                let ack = UnsubscribeAck {
                    packet_id,
                    properties: Vec::new(),
                    reason_string: None,
                    status: vec![UnsubscribeAckReason::Success],
                };
                assert_stale_generation_then_live_completes(
                    &control,
                    reply_rx,
                    ControlMessage::UnsubscribeAck {
                        ack: ack.clone(),
                        generation: stale,
                    },
                    ControlMessage::UnsubscribeAck {
                        ack,
                        generation: live_generation,
                    },
                )
                .await;
            }
            AckKind::Publish => {
                use crate::codec::PublishAckReason;

                let (reply_tx, reply_rx) = oneshot::channel();
                client
                    .try_insert_publish_waiter(packet_id, reply_tx)
                    .expect("insert publish waiter on live write task");
                // Allow the write task to process InsertPublishWaiter.
                tokio::task::yield_now().await;
                tokio::time::sleep(Duration::from_millis(30)).await;

                let ack = PublishAck {
                    packet_id,
                    reason_code: PublishAckReason::Success,
                    properties: Vec::new(),
                    reason_string: None,
                };
                assert_stale_generation_then_live_completes(
                    &control,
                    reply_rx,
                    ControlMessage::PublishAck {
                        ack: ack.clone(),
                        generation: stale,
                    },
                    ControlMessage::PublishAck {
                        ack,
                        generation: live_generation,
                    },
                )
                .await;
            }
        }

        client.shutdown().await.expect("shutdown");
        let _ = run.await;
    }

    /// Peer close while QoS 1 publish is outstanding → typed error, no hang.
    #[tokio::test]
    async fn qos1_publish_waiter_gets_typed_error_on_connection_loss() {
        let port = spawn_broker_once(|mut framed_read, mut framed_write| async move {
            if !accept_connect_send_connack(&mut framed_read, &mut framed_write).await {
                return;
            }
            // Accept one PUBLISH then drop the socket (no PUBACK).
            let _ = framed_read.next().await;
            drop(framed_write);
            drop(framed_read);
        })
        .await;

        let (client, connection, mut phase) = connect_test_client(port).await;

        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        let err = timeout(
            Duration::from_secs(3),
            client.publish(sample_message("t/lost", b"x"), QoS::AtLeastOnce, false),
        )
        .await
        .expect("QoS 1 waiter must not hang after connection loss")
        .expect_err("open QoS 1 publish must fail with a typed error");

        assert!(
            matches!(err, AppError::ConnectionLost),
            "expected AppError::ConnectionLost after peer drop, got {err:?}"
        );
        assert!(
            !matches!(err, AppError::CommandChannelClosed),
            "peer drop must not surface as CommandChannelClosed, got {err:?}"
        );

        let _ = run.await;
    }

    /// Graceful `shutdown` with an in-flight QoS1 waiter → `ConnectionLost` (live `graceful_disconnect`).
    #[tokio::test]
    async fn qos1_publish_waiter_gets_connection_lost_on_graceful_shutdown() {
        use tokio::sync::Notify;

        let first_publish = Arc::new(Notify::new());
        let first_publish_broker = Arc::clone(&first_publish);
        let port = spawn_broker_once(move |mut framed_read, mut framed_write| async move {
            if !accept_connect_send_connack(&mut framed_read, &mut framed_write).await {
                return;
            }
            // Withhold PUBACK so the QoS1 waiter stays open until graceful_disconnect.
            while let Some(Ok(frame)) = framed_read.next().await {
                if matches!(frame, (DecodedFrame::Publish(_), _)) {
                    first_publish_broker.notify_one();
                }
            }
        })
        .await;

        let (client, connection, mut phase) = connect_test_client(port).await;

        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        let publish = tokio::spawn({
            let client = client.clone();
            async move {
                client
                    .publish(sample_message("t/shutdown", b"x"), QoS::AtLeastOnce, false)
                    .await
            }
        });
        timeout(Duration::from_secs(2), first_publish.notified())
            .await
            .expect("broker must observe first QoS1 PUBLISH before shutdown");

        client.shutdown().await.expect("shutdown");
        let err = timeout(Duration::from_secs(3), publish)
            .await
            .expect("QoS1 waiter must complete on graceful shutdown")
            .expect("publish task join")
            .expect_err("open QoS1 publish must fail when shutdown drains waiters");

        assert_eq!(
            err,
            AppError::ConnectionLost,
            "graceful_disconnect must fail waiters as ConnectionLost, got {err:?}"
        );
        assert!(
            !matches!(err, AppError::CommandChannelClosed),
            "graceful shutdown must not surface as CommandChannelClosed"
        );

        let _ = run.await;
    }
}
