//! [`MqttClient`] handle and [`connect`] entry point.
//!
//! Subscribe / unsubscribe await SUBACK / UNSUBACK with per-filter reason codes.
//! Publish: QoS 0 completes on encode; QoS 1 awaits PUBACK.
//! [`MqttClient::publish`] is the fluent entry point; [`MqttClient::send`] takes a ready
//! [`Publish`]; [`MqttClient::publish_window`] pipelines bursts.
//!
//! For a higher-level entry point that applies defaults and connects (spawns the driver), see
//! [`ClientBuilder`].

use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use arc_swap::ArcSwap;
use bytes::Bytes;
use bytestring::ByteString;
use secrecy::SecretString;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;

use crate::codec::{QoS, RetainHandling, SubscriptionOptions};
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
pub use crate::publish::{Publish, PublishBuilder};
pub use crate::publish_window::PublishWindow;

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

/// Completion handle for an outbound PUBLISH started by [`MqttClient::try_start_publish`].
///
/// [`Self::wait`] completes after the frame is **encoded into the connection write buffer**
/// for QoS 0 (not necessarily flushed to the socket or received by the broker), or after
/// the matching PUBACK for QoS 1.
///
/// # Drop behavior
///
/// Dropping this handle without waiting means the publish result is discarded. A PUBLISH
/// still queued on the command channel at that point may be skipped without a packet id
/// being allocated; once the write task has encoded the frame, the publish proceeds —
/// for QoS 1 the driver-side waiter remains until PUBACK or session teardown, matching
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
    /// Set by the driver between Connected and Disconnected.
    connected: Arc<AtomicBool>,
    /// Set by [`Self::shutdown`]. Checked on every command, so it is a plain atomic:
    /// `CancellationToken::is_cancelled` takes a mutex.
    shutting_down: Arc<AtomicBool>,
    /// [`ConnectOptions::fail_fast_when_disconnected`].
    fail_fast_when_disconnected: bool,
    /// Session phase watch (the same one [`connect`] returns), for [`Self::session_phase`].
    phase: watch::Receiver<SessionPhase>,
    /// For [`Self::refused_subscriptions`].
    refused: watch::Receiver<Vec<crate::RefusedSubscription>>,
    /// Response routing for [`crate::Requester`] (shared with the driver's read path).
    router: Arc<crate::request::ResponseRouter>,
    /// Counters kept by the driver.
    #[cfg(feature = "stats")]
    stats: crate::stats::Stats,
}

/// The [`ConnectOptions`] a client handle needs after the options moved into the driver.
struct ClientOptions {
    ack_timeout: Duration,
    fail_fast_when_disconnected: bool,
}

/// State the driver shares with every client handle.
struct DriverShared {
    connected: Arc<AtomicBool>,
    phase: watch::Receiver<SessionPhase>,
    refused: watch::Receiver<Vec<crate::RefusedSubscription>>,
    router: Arc<crate::request::ResponseRouter>,
    #[cfg(feature = "stats")]
    stats: crate::stats::Stats,
}

impl MqttClient {
    /// Creates a handle wired to the driver's bounded command channel.
    fn new(
        command_tx: Arc<ArcSwap<CommandSender>>,
        cancel: CancellationToken,
        shutdown_notify: CancellationToken,
        options: &ClientOptions,
        shared: DriverShared,
    ) -> Self {
        Self {
            command_tx,
            cancel,
            shutdown_notify,
            ack_timeout: options.ack_timeout,
            connected: shared.connected,
            shutting_down: Arc::default(),
            fail_fast_when_disconnected: options.fail_fast_when_disconnected,
            phase: shared.phase,
            refused: shared.refused,
            router: shared.router,
            #[cfg(feature = "stats")]
            stats: shared.stats,
        }
    }

    /// Subscribes to `response_topic` (QoS 1) and returns a [`crate::Requester`] that sends
    /// requests and awaits their responses there (MQTT 5 request / response, §4.10).
    ///
    /// Pick a topic only this client uses (for example including its client id) that the
    /// broker's ACLs allow it to subscribe to; no wildcards. Responses on it are routed to
    /// the waiting requests and never reach the inbound sink. The subscription is restored
    /// on reconnect like any other.
    pub async fn requester(&self, response_topic: impl Into<ByteString>) -> Result<crate::Requester, AppError> {
        crate::Requester::subscribe(self, response_topic.into()).await
    }

    pub(crate) fn response_router(&self) -> Arc<crate::request::ResponseRouter> {
        Arc::clone(&self.router)
    }

    /// A receiver for the session phase: the current value plus every change.
    ///
    /// The same watch [`connect`] returns, also reachable when the client came from
    /// [`ClientBuilder::connect`]. Wait for readiness with
    /// `client.session_phase().wait_for(|p| matches!(p, SessionPhase::Connected { .. }))`.
    /// For a cheap snapshot use [`Self::is_connected`].
    pub fn session_phase(&self) -> watch::Receiver<SessionPhase> {
        self.phase.clone()
    }

    /// Subscriptions the broker refused to restore after a reconnect: the current list plus
    /// every change. Empty while every subscription is active.
    ///
    /// When the broker has no session for the client (Clean Start, or the session expired), the
    /// client subscribes again to everything the application subscribed to. A filter the broker
    /// refuses then (an ACL revoked meanwhile, a quota) does not end the connection: it stays
    /// listed here with the SUBACK reason code, and the client asks again on the same
    /// connection, first after about 5 s, then doubling up to every 5 min, until the broker
    /// grants it or the application calls [`Self::unsubscribe`] for it. Each refusal is also
    /// logged at `warn`.
    ///
    /// A non-empty list means messages on those filters are not arriving, which makes it a
    /// natural health check:
    ///
    /// ```no_run
    /// # fn report_degraded(_: &[mqtt_glide::RefusedSubscription]) {}
    /// # async fn example(client: mqtt_glide::MqttClient) {
    /// let mut refused = client.refused_subscriptions();
    /// while refused.changed().await.is_ok() {
    ///     report_degraded(&refused.borrow_and_update());
    /// }
    /// # }
    /// ```
    ///
    /// A refusal of [`Self::subscribe`] itself is not listed: that answer goes to the caller.
    pub fn refused_subscriptions(&self) -> watch::Receiver<Vec<crate::RefusedSubscription>> {
        self.refused.clone()
    }

    /// Whether a session is currently up (between `Connected` and `Disconnected`).
    ///
    /// A snapshot: the connection can drop right after this returns. For readiness waits use
    /// the [`SessionPhase`] watch or a [`LifecycleHook`].
    #[inline]
    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    /// What the connection has done since [`connect`], across reconnects: sessions,
    /// publishes in and out, acknowledgements, the outbound window, bytes (`stats` feature).
    ///
    /// Cheap enough to call on every metrics scrape: a few dozen relaxed atomic loads. The
    /// crate does not pick a metrics system; export the fields to whichever the application
    /// uses. Errors the caller already sees (a publish ack timeout, backpressure) are not
    /// counted here.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo(client: mqtt_glide::MqttClient) {
    /// let mut every = tokio::time::interval(std::time::Duration::from_secs(15));
    /// loop {
    ///     every.tick().await;
    ///     let stats = client.stats();
    ///     tracing::info!(
    ///         reconnects = stats.connection.connects.saturating_sub(1),
    ///         sent = stats.outbound.publishes_qos1,
    ///         rejected = stats.outbound.publishes_rejected,
    ///         awaiting_ack = stats.outbound.awaiting_ack,
    ///         sink_stalls = stats.inbound.handoff_stalls,
    ///         "mqtt"
    ///     );
    /// }
    /// # }
    /// ```
    #[cfg(feature = "stats")]
    pub fn stats(&self) -> crate::ClientStats {
        let commands = self.command_tx.load();
        self.stats.snapshot(commands.max_capacity() - commands.capacity())
    }

    /// Refuses a command before it is queued: shutting down, or disconnected in fail-fast mode.
    #[inline]
    fn check_accepting(&self) -> Result<(), AppError> {
        if self.shutting_down.load(Ordering::Acquire) {
            return Err(AppError::ShuttingDown);
        }
        if self.fail_fast_when_disconnected && !self.is_connected() {
            return Err(AppError::NotConnected);
        }
        Ok(())
    }

    /// Sends a command without waiting, mapping a full channel to [`AppError::Backpressure`].
    #[inline]
    fn try_send_command(&self, command: ClientCommand) -> Result<(), AppError> {
        self.check_accepting()?;
        // ArcSwap load — no lock across await; sender is only swapped on reconnect.
        self.command_tx.load().try_send(command).map_err(|error| match error {
            TrySendError::Full(_) => AppError::Backpressure("command channel full".into()),
            TrySendError::Closed(_) => AppError::CommandChannelClosed,
        })
    }

    /// Sends a command, waiting up to [`ConnectOptions::ack_timeout`] for channel room.
    ///
    /// Fast path is a plain `try_send`. When full, reserves a slot on the current sender;
    /// if that sender closes because the driver swapped it on reconnect, retries on the
    /// new one. Times out with [`AppError::Backpressure`].
    async fn send_command(&self, command: ClientCommand) -> Result<(), AppError> {
        match self.try_send_or_return(command)? {
            None => Ok(()),
            // Boxed so the rare wait state (timer + reserve) does not inflate every
            // publish future on the hot path.
            Some(command) => Box::pin(self.wait_for_room(command)).await,
        }
    }

    /// `try_send`, handing the command back when the channel is full.
    #[inline]
    fn try_send_or_return(&self, command: ClientCommand) -> Result<Option<ClientCommand>, AppError> {
        self.check_accepting()?;
        match self.command_tx.load().try_send(command) {
            Ok(()) => Ok(None),
            Err(TrySendError::Full(command)) => Ok(Some(command)),
            Err(TrySendError::Closed(_)) => Err(AppError::CommandChannelClosed),
        }
    }

    #[cold]
    async fn wait_for_room(&self, command: ClientCommand) -> Result<(), AppError> {
        let deadline = tokio::time::Instant::now() + self.ack_timeout;
        loop {
            let sender = self.command_tx.load_full();
            // No select on the shutdown token: registering on it takes its mutex for every
            // waiter. Shutdown closes the channel instead, which ends `reserve` here.
            match tokio::time::timeout_at(deadline, sender.reserve()).await {
                Ok(Ok(permit)) => {
                    permit.send(command);
                    return Ok(());
                }
                Ok(Err(_closed)) if self.shutting_down.load(Ordering::Acquire) => return Err(AppError::ShuttingDown),
                Ok(Err(_closed)) if !Arc::ptr_eq(&sender, &self.command_tx.load_full()) => continue,
                Ok(Err(_closed)) => return Err(AppError::CommandChannelClosed),
                Err(_elapsed) => {
                    return Err(AppError::Backpressure(format!(
                        "command channel full for {:?}",
                        self.ack_timeout
                    )));
                }
            }
        }
    }

    /// Starts a fluent PUBLISH of `payload` to `topic`; `.await` it to send.
    ///
    /// Defaults: QoS 0, retain `false`, no properties. Awaiting waits for room in the
    /// command channel, then for completion — encoded into the write buffer for QoS 0
    /// (not necessarily flushed or broker-received), the matching `PUBACK` for QoS 1.
    /// QoS 1 publishes beyond the broker's Receive Maximum wait for a free slot.
    /// Both waits are bounded by [`ConnectOptions::ack_timeout`]
    /// ([`AppError::Backpressure`] / [`AppError::PublishAckTimeout`]).
    ///
    /// `topic` / `payload` take the same types as [`Publish::new`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use mqtt_glide::QoS;
    ///
    /// # async fn demo(client: mqtt_glide::MqttClient) -> Result<(), mqtt_glide::AppError> {
    /// client.publish("t/example", "hello").await?;
    /// client
    ///     .publish("t/status", "online")
    ///     .qos(QoS::AtLeastOnce)
    ///     .retain(true)
    ///     .user_property("source", "edge-01")
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    pub fn publish(&self, topic: impl Into<ByteString>, payload: impl Into<Bytes>) -> PublishBuilder<'_> {
        PublishBuilder::new(self, Publish::new(topic, payload))
    }

    /// Sends a ready [`Publish`] (or a [`PublishMessage`](crate::PublishMessage) as QoS 0) and waits for completion.
    ///
    /// Same waiting and errors as awaiting [`Self::publish`].
    pub async fn send(&self, publish: impl Into<Publish>) -> Result<(), AppError> {
        // Flat on purpose: one future layer and a sync `try_send` on the hot path.
        let (command, completion) = self.publish_command(publish.into())?;
        if let Some(command) = self.try_send_or_return(command)? {
            Box::pin(self.wait_for_room(command)).await?;
        }
        completion.wait().await
    }

    /// Enqueues a PUBLISH without waiting at all and returns its completion handle; the
    /// non-blocking form of [`Self::start_publish`] (like `try_send` and `send` on a channel).
    ///
    /// A full command channel returns [`AppError::Backpressure`] immediately (no handle is
    /// created). QoS 2 returns
    /// [`AppError::InvalidRequest`]. Call [`PublishCompletion::wait`] to observe the
    /// result; `packet_id` / `dup` are assigned by the connection driver.
    #[inline]
    pub fn try_start_publish(&self, publish: impl Into<Publish>) -> Result<PublishCompletion, AppError> {
        let (command, completion) = self.publish_command(publish.into())?;
        self.try_send_command(command)?;
        Ok(completion)
    }

    /// Enqueues a PUBLISH and returns its completion handle without waiting for the
    /// completion itself: waits up to [`ConnectOptions::ack_timeout`] for room in the command
    /// channel ([`AppError::Backpressure`] after that), then returns. Call
    /// [`PublishCompletion::wait`] to observe the result. [`Self::try_start_publish`] fails at
    /// once on a full channel instead.
    pub async fn start_publish(&self, publish: impl Into<Publish>) -> Result<PublishCompletion, AppError> {
        let (command, completion) = self.publish_command(publish.into())?;
        if let Some(command) = self.try_send_or_return(command)? {
            Box::pin(self.wait_for_room(command)).await?;
        }
        Ok(completion)
    }

    fn publish_command(&self, publish: Publish) -> Result<(ClientCommand, PublishCompletion), AppError> {
        if publish.qos == QoS::ExactlyOnce {
            return Err(AppError::InvalidRequest("QoS 2 publish is not supported".into()));
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        let command = ClientCommand::Publish {
            message: publish.message,
            qos: publish.qos,
            retain: publish.retain,
            reply: reply_tx,
        };
        Ok((
            command,
            PublishCompletion {
                reply_rx,
                ack_timeout: self.ack_timeout,
            },
        ))
    }

    /// Starts a sliding-window pipeline over [`Self::start_publish`] (no task per message).
    ///
    /// [`PublishWindow::push`] enqueues, then [`PublishWindow::drain`] after the burst.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::num::NonZeroUsize;
    ///
    /// use mqtt_glide::{Publish, QoS};
    ///
    /// # async fn demo(client: mqtt_glide::MqttClient) -> Result<(), mqtt_glide::AppError> {
    /// let mut pipe = client.publish_window(NonZeroUsize::new(64).expect("nz"));
    /// for i in 0..1_000 {
    ///     pipe.push(Publish::new(format!("out/{i}"), "x").qos(QoS::AtLeastOnce)).await?;
    /// }
    /// pipe.drain().await?;
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    pub fn publish_window(&self, capacity: NonZeroUsize) -> PublishWindow<'_> {
        PublishWindow::new(self, capacity)
    }

    /// Publishes every message through an internal [`PublishWindow`] of `window` capacity.
    ///
    /// Each item carries its own QoS / retain / properties ([`Publish`]; a bare
    /// [`PublishMessage`](crate::PublishMessage) is QoS 0). An empty iterator completes immediately. Stops at the
    /// first error (same as [`PublishWindow::drain`]).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::num::NonZeroUsize;
    ///
    /// use mqtt_glide::{Publish, QoS};
    ///
    /// # async fn demo(client: mqtt_glide::MqttClient) -> Result<(), mqtt_glide::AppError> {
    /// let messages = ["a", "b"].map(|p| Publish::new("out", p).qos(QoS::AtLeastOnce));
    /// client.publish_batch(messages, NonZeroUsize::new(64).expect("nz")).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn publish_batch(
        &self,
        messages: impl IntoIterator<Item = impl Into<Publish>>,
        window: NonZeroUsize,
    ) -> Result<(), AppError> {
        let mut pipe = self.publish_window(window);
        for message in messages {
            pipe.push(message).await?;
        }
        pipe.drain().await
    }

    /// Subscribes to one or more topic filters and returns the SUBACK: **per-filter** reason
    /// codes plus the broker's Reason String and User Properties
    /// ([`SubscribeResponse`](crate::SubscribeResponse), which reads like the reason list).
    ///
    /// Reason codes are not collapsed to a single bool — callers must inspect each entry
    /// (e.g. [`SubscribeAckReason::NotAuthorized`](crate::SubscribeAckReason::NotAuthorized) vs granted QoS).
    ///
    /// Accepts any [`IntoIterator`] of values convertible to [`Subscription`] — for example
    /// `[("topic", QoS::AtLeastOnce)]`, `[Subscription::new(...)]`, or a `Vec`.
    ///
    /// Awaits a matching SUBACK until [`ConnectOptions::ack_timeout`] elapses, then
    /// returns [`AppError::SubscribeAckTimeout`].
    ///
    /// On timeout the broker may still apply the subscription; a late SUBACK is then
    /// still recorded in the resubscribe registry (the packet id stays allocated until
    /// the ack, §2.2.1). Treat a timeout as "unknown outcome", not as "failed".
    pub async fn subscribe(
        &self,
        filters: impl IntoIterator<Item = impl Into<Subscription>>,
    ) -> Result<crate::ack::SubscribeResponse, AppError> {
        self.subscribe_inner(filters, None).await
    }

    /// Like [`Self::subscribe`], tagging the subscriptions with Subscription Identifier `id`
    /// (§3.8.2.1.2): every inbound message they match carries `id` in
    /// [`InboundMessage::subscription_ids`](crate::InboundMessage::subscription_ids), so an
    /// application can route by subscription instead of re-matching topics. One identifier
    /// covers all `filters` of the call. The identifier survives reconnects (restored
    /// subscriptions keep it).
    ///
    /// `id` must be in `1..=268_435_455`; a broker that announced no support for
    /// identifiers (CONNACK) makes this fail with [`AppError::InvalidRequest`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use mqtt_glide::QoS;
    ///
    /// # async fn demo(client: mqtt_glide::MqttClient) -> Result<(), mqtt_glide::AppError> {
    /// const ALERTS: u32 = 1;
    /// client.subscribe_with_id(ALERTS, [("site/+/alarm", QoS::AtLeastOnce)]).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn subscribe_with_id(
        &self,
        id: u32,
        filters: impl IntoIterator<Item = impl Into<Subscription>>,
    ) -> Result<crate::ack::SubscribeResponse, AppError> {
        /// Largest Variable Byte Integer (§1.5.5), the identifier's wire type.
        const MAX_SUBSCRIPTION_ID: u32 = 268_435_455;
        let id = std::num::NonZeroU32::new(id)
            .filter(|id| id.get() <= MAX_SUBSCRIPTION_ID)
            .ok_or_else(|| AppError::InvalidRequest(format!("Subscription Identifier {id} not in 1..=268435455")))?;
        self.subscribe_inner(filters, Some(id)).await
    }

    async fn subscribe_inner(
        &self,
        filters: impl IntoIterator<Item = impl Into<Subscription>>,
        id: Option<std::num::NonZeroU32>,
    ) -> Result<crate::ack::SubscribeResponse, AppError> {
        let filters: Vec<_> = filters
            .into_iter()
            .map(|sub| {
                let sub = sub.into();
                (sub.filter, sub.options)
            })
            .collect();
        let (reply_tx, reply_rx) = oneshot::channel();
        self.send_command(ClientCommand::Subscribe {
            filters,
            id,
            reply: reply_tx,
        })
        .await?;
        await_ack(self.ack_timeout, reply_rx, AckKind::Subscribe).await
    }

    /// Unsubscribes from topic filters and returns the UNSUBACK: **per-filter** reason codes
    /// plus Reason String and User Properties ([`UnsubscribeResponse`](crate::UnsubscribeResponse)).
    ///
    /// Accepts any [`IntoIterator`] of values convertible to [`ByteString`] (e.g. `["a/b"]`,
    /// `vec![…]`).
    ///
    /// Awaits a matching UNSUBACK until [`ConnectOptions::ack_timeout`] elapses, then
    /// returns [`AppError::UnsubscribeAckTimeout`].
    ///
    /// On timeout the broker may still apply the removal; a late UNSUBACK is then still
    /// recorded in the resubscribe registry. Treat a timeout as "unknown outcome".
    pub async fn unsubscribe(
        &self,
        filters: impl IntoIterator<Item = impl Into<ByteString>>,
    ) -> Result<crate::ack::UnsubscribeResponse, AppError> {
        let filters: Vec<_> = filters.into_iter().map(Into::into).collect();
        let (reply_tx, reply_rx) = oneshot::channel();
        self.send_command(ClientCommand::Unsubscribe {
            filters,
            reply: reply_tx,
        })
        .await?;
        await_ack(self.ack_timeout, reply_rx, AckKind::Unsubscribe).await
    }

    /// Requests graceful shutdown of the connection driver.
    ///
    /// From this call on, every client command fails with [`AppError::ShuttingDown`]. An
    /// active session then drains, bounded by
    /// [`ConnectOptions::graceful_disconnect_timeout`]: commands already queued are sent,
    /// QoS 1 publishes get their `PUBACK`s, and the read task keeps delivering (and
    /// acknowledging) inbound messages until that is done. Then the client sends DISCONNECT
    /// and closes the connection. Whatever is still unacknowledged when the budget runs out
    /// fails with [`AppError::ConnectionLost`].
    ///
    /// Returns once shutdown is requested, not when the driver finished; await the
    /// [`Connection::run`] handle (or watch the session phase) for that. Connect / backoff
    /// phases stop at once.
    pub async fn shutdown(&self) -> Result<(), AppError> {
        // Signalled via tokens, not the command channel, so a full channel cannot delay it.
        self.shutting_down.store(true, Ordering::Release);
        self.shutdown_notify.cancel();
        self.cancel.cancel();
        Ok(())
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
    // [MQTT-3.1.3-7]: Clean Start = 0 with a Zero-length ClientID is a Protocol Error.
    if !options.clean_start {
        let empty_id = options
            .client_id
            .as_ref()
            .is_none_or(|client_id| client_id.as_str().is_empty());
        if empty_id {
            return Err(ConnectError::Application(AppError::InvalidRequest(
                "clean_start = false requires a non-empty client_id ([MQTT-3.1.3-7])".into(),
            )));
        }
    }
    if let Some(will) = options.will_message.as_ref() {
        crate::connection::validate_will_message(will)?;
    }

    let command_channel_capacity = options.command_channel_capacity.get();
    let (command_tx, command_rx) = mpsc::channel(command_channel_capacity);
    let command_tx = Arc::new(ArcSwap::from_pointee(command_tx));
    let (control_tx, control_rx) = mpsc::channel(control_channel_capacity(options.receive_maximum));
    let (phase_tx, phase_rx) = watch::channel(SessionPhase::initial());
    let (refused_tx, refused_rx) = watch::channel(Vec::new());
    #[cfg(test)]
    let (read_stub_exit_tx, read_stub_exit_rx) = mpsc::channel(1);
    #[cfg(test)]
    let (write_stub_exit_tx, write_stub_exit_rx) = mpsc::channel(1);
    let cancel = CancellationToken::new();
    let shutdown_notify = CancellationToken::new();
    let connected = Arc::new(AtomicBool::new(false));
    let router = Arc::new(crate::request::ResponseRouter::default());
    let stats = crate::stats::Stats::default();
    let client_options = ClientOptions {
        ack_timeout: options.ack_timeout,
        fail_fast_when_disconnected: options.fail_fast_when_disconnected,
    };

    let handoff_anchor = inbound.clone();
    let channels = ConnectionChannels {
        control_tx,
        control_rx,
        command_tx: Arc::clone(&command_tx),
        command_rx: Some(command_rx),
        handoff: inbound,
        handoff_anchor,
        phase_tx,
        lifecycle,
        connected: Arc::clone(&connected),
        response_router: Arc::clone(&router),
        refused_subscriptions: refused_tx,
        #[cfg(feature = "stats")]
        stats: stats.clone(),
        #[cfg(not(feature = "stats"))]
        stats,
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
        MqttClient::new(
            command_tx,
            cancel,
            shutdown_notify,
            &client_options,
            DriverShared {
                connected,
                phase: phase_rx.clone(),
                refused: refused_rx,
                router,
                #[cfg(feature = "stats")]
                stats,
            },
        ),
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
        ConnectAck, DecodedFrame, InboundMessage, Packet, PublishAck, PublishMessage, PublishProperties, QoS,
        SubscribeAck, SubscribeAckReason, UnsubscribeAck, UnsubscribeAckReason,
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

    #[test]
    fn connect_rejects_invalid_will_message_before_dial() {
        let url = BrokerUrl::parse("mqtt://127.0.0.1:9").expect("url");
        let options = ConnectOptions::new(url).will("wild/+/card", "x");
        let (tx, _rx) = flume::bounded(1);
        let err = connect(options, AnonymousCredentials, NoReconnect, tx, NoopLifecycle)
            .expect_err("wildcard will topic must fail before dial");
        assert!(matches!(err, ConnectError::Application(AppError::InvalidRequest(_))));
    }

    #[test]
    fn connect_rejects_clean_start_false_without_client_id() {
        let url = BrokerUrl::parse("mqtt://127.0.0.1:9").expect("url");
        let options = ConnectOptions::new(url).clean_start(false);
        let (tx, _rx) = flume::bounded(1);
        let err = connect(options, AnonymousCredentials, NoReconnect, tx, NoopLifecycle)
            .expect_err("empty client id with clean_start=false");
        assert!(matches!(err, ConnectError::Application(AppError::InvalidRequest(_))));
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
        let (tx, _rx) = flume::bounded::<InboundMessage>(4);
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
        let (tx, _rx) = flume::bounded::<InboundMessage>(4);
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

    /// QoS 0: no packet-id / publish-permit / PUBACK wait — message must reach the wire, and
    /// without a Packet Identifier ([MQTT-2.2.1-2]).
    #[tokio::test]
    async fn qos0_publish_reaches_wire_without_packet_id() {
        let observed = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let port = spawn_mock_broker_record_publishes(Arc::clone(&observed), NonZeroU16::new(8).expect("nz")).await;
        let (client, connection, mut phase) = connect_test_client(port).await;

        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        timeout(
            Duration::from_secs(2),
            client.send(Publish::from(sample_message("t/qos0", b"fire"))),
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

    /// QoS 1: `publish().await` resolves only after PUBACK (mock broker). The PUBLISH goes out
    /// as QoS 1 with a Packet Identifier ([MQTT-4.3.2-2]).
    #[tokio::test]
    async fn qos1_publish_awaits_matching_puback() {
        let observed = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let port = spawn_mock_broker_record_publishes(Arc::clone(&observed), NonZeroU16::new(8).expect("nz")).await;
        let (client, connection, mut phase) = connect_test_client(port).await;

        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        client
            .send(Publish::from(sample_message("t/qos1", b"need-ack")).qos(QoS::AtLeastOnce))
            .await
            .expect("QoS 1 publish must complete after PUBACK");

        let frames = observed.lock().await.clone();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].0, QoS::AtLeastOnce);
        assert!(frames[0].1.is_some(), "QoS 1 must allocate a packet identifier");

        client.shutdown().await.expect("shutdown");
        let _ = run.await;
    }

    /// QoS 1: `try_start_publish` enqueues immediately and its completion waits for PUBACK.
    #[tokio::test]
    async fn qos1_start_publish_completion_awaits_matching_puback() {
        let observed = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let port = spawn_mock_broker_record_publishes(Arc::clone(&observed), NonZeroU16::new(8).expect("nz")).await;
        let (client, connection, mut phase) = connect_test_client(port).await;

        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        client
            .try_start_publish(Publish::from(sample_message("t/qos1-start", b"need-ack")).qos(QoS::AtLeastOnce))
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

    /// QoS 2 is refused by the client itself, before the command is queued: the error comes
    /// back without a connection (the driver is never run here), also from the non-blocking
    /// forms.
    #[tokio::test]
    async fn qos2_publish_is_rejected_before_it_is_queued() {
        let port = spawn_mock_broker().await;
        let (client, _connection, _phase) = connect_test_client(port).await;

        let publish = || Publish::new("t/qos2", "x").qos(QoS::ExactlyOnce);
        assert!(matches!(
            client.try_start_publish(publish()),
            Err(AppError::InvalidRequest(_))
        ));
        let started = timeout(Duration::from_secs(1), client.start_publish(publish()))
            .await
            .expect("rejected without waiting for a connection");
        assert!(matches!(started, Err(AppError::InvalidRequest(_))));
        let sent = timeout(
            Duration::from_secs(1),
            client.publish("t/qos2", "x").qos(QoS::ExactlyOnce),
        )
        .await
        .expect("rejected without waiting for a connection");
        assert!(matches!(sent, Err(AppError::InvalidRequest(_))));
    }

    /// A QoS 2 publish on a live connection fails with `InvalidRequest` and nothing reaches the
    /// wire: the next PUBLISH the broker sees is the QoS 0 one sent after it.
    #[tokio::test]
    async fn qos2_publish_never_reaches_the_wire() {
        let observed = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let port = spawn_mock_broker_record_publishes(Arc::clone(&observed), NonZeroU16::new(8).expect("nz")).await;
        let (client, connection, mut phase) = connect_test_client(port).await;
        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        let result = timeout(
            Duration::from_secs(2),
            client.publish("t/qos2", "refused").qos(QoS::ExactlyOnce),
        )
        .await
        .expect("a QoS 2 publish fails at once");
        assert!(matches!(result, Err(AppError::InvalidRequest(_))), "{result:?}");
        client.publish("t/after", "marker").await.expect("qos 0 publish");
        client.shutdown().await.expect("shutdown");
        let _ = run.await;

        let frames = observed.lock().await.clone();
        assert_eq!(frames, [(QoS::AtMostOnce, None, Bytes::from_static(b"marker"))]);
    }

    /// With a window larger than one, every QoS 1 PUBLISH in flight has its own Packet
    /// Identifier, and the one freed by a PUBACK is the one the next PUBLISH gets
    /// ([MQTT-2.2.1-3], [MQTT-4.3.2-1]).
    #[tokio::test]
    async fn in_flight_qos1_publishes_have_distinct_packet_ids_until_puback() {
        use crate::codec::PublishAckReason;

        let (ids_tx, ids_rx) = oneshot::channel::<(Vec<NonZeroU16>, bool, Option<NonZeroU16>)>();
        let port = spawn_broker_once(move |mut framed_read, mut framed_write| async move {
            let mut ack = ConnectAck::default();
            ack.receive_max = NonZeroU16::new(4).expect("nz");
            if !accept_connect_send_connack_with(&mut framed_read, &mut framed_write, ack).await {
                return;
            }
            async fn next_publish_id(framed_read: &mut crate::connection::test_support::MockFramedRead) -> NonZeroU16 {
                loop {
                    if let Some(Ok((DecodedFrame::Publish(packet), _))) = framed_read.next().await {
                        return packet.meta.packet_id.expect("QoS 1 packet id");
                    }
                }
            }
            let puback = |packet_id| {
                Packet::PublishAck(PublishAck {
                    packet_id,
                    reason_code: PublishAckReason::Success,
                    properties: Vec::new(),
                    reason_string: None,
                })
            };
            // The window of 4 fills; the fifth PUBLISH must wait for a PUBACK.
            let mut first_window = Vec::new();
            for _ in 0..4 {
                first_window.push(next_publish_id(&mut framed_read).await);
            }
            let early_fifth = timeout(Duration::from_millis(200), framed_read.next()).await.is_ok();
            // Free one slot that is neither the first nor the last allocated.
            let freed = first_window[1];
            if framed_write.send(puback(freed)).await.is_err() {
                return;
            }
            let fifth = timeout(Duration::from_secs(2), next_publish_id(&mut framed_read))
                .await
                .ok();
            for id in first_window.iter().copied().filter(|id| *id != freed).chain(fifth) {
                let _ = framed_write.send(puback(id)).await;
            }
            let _ = ids_tx.send((first_window, early_fifth, fifth));
            while framed_read.next().await.is_some() {}
        })
        .await;
        let (client, connection, mut phase) = connect_test_client(port).await;
        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        let completions: Vec<_> = (0..5u8)
            .map(|n| {
                client
                    .try_start_publish(Publish::new("t/window", vec![n]).qos(QoS::AtLeastOnce))
                    .expect("queued")
            })
            .collect();
        let (first_window, early_fifth, fifth) = timeout(Duration::from_secs(5), ids_rx)
            .await
            .expect("broker result in time")
            .expect("broker result");
        let distinct: std::collections::HashSet<_> = first_window.iter().collect();
        assert_eq!(distinct.len(), 4, "ids in flight must differ: {first_window:?}");
        assert!(!early_fifth, "the fifth PUBLISH must wait for a PUBACK");
        assert_eq!(
            fifth,
            Some(first_window[1]),
            "the identifier freed by the PUBACK is the only one available"
        );

        for completion in completions {
            timeout(Duration::from_secs(5), completion.wait())
                .await
                .expect("acked in time")
                .expect("acked");
        }

        client.shutdown().await.expect("shutdown");
        let _ = run.await;
    }

    /// Barrier: `publish().await` stays pending until the matching PUBACK is released; the
    /// PUBLISH stays unacknowledged until then ([MQTT-4.3.2-3]).
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
                .send(Publish::from(sample_message("t/barrier", b"wait")).qos(QoS::AtLeastOnce))
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

    /// Wrong Packet Identifier PUBACK must not complete the publish; matching id does
    /// ([MQTT-4.3.2-3]).
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
                .send(Publish::from(sample_message("t/wrong-id", b"wait")).qos(QoS::AtLeastOnce))
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
                .send(Publish::from(sample_message("t/qos1-hold", b"hold")).qos(QoS::AtLeastOnce))
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
            client_qos0.send(Publish::from(sample_message("t/qos0-free", b"free"))),
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

    /// Full command queue → immediate `AppError::Backpressure` on `try_start_publish` (no unbounded growth).
    #[tokio::test]
    async fn publish_returns_backpressure_when_command_queue_full() {
        let url = BrokerUrl::parse("mqtt://localhost").expect("valid url");
        let options = ConnectOptions::new(url).receive_maximum(NonZeroU16::new(8).expect("nz"));
        let (tx, _rx) = flume::bounded::<InboundMessage>(4);
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
            .try_start_publish(Publish::from(sample_message("t/bp", b"x")))
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
        let (tx, _rx) = flume::bounded::<InboundMessage>(4);
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
            .try_start_publish(Publish::from(sample_message("t/bp-custom", b"x")))
            .expect_err("full custom command channel must backpressure publish");
        assert!(
            matches!(err, AppError::Backpressure(_)),
            "expected Backpressure, got {err:?}"
        );
    }

    /// CONNACK `server_receive_maximum` caps outbound QoS 1 inflight (not only ConnectOptions).
    /// With Receive Maximum 1 exactly one PUBLISH goes out: the initial send quota is non-zero
    /// and no larger than Receive Maximum ([MQTT-4.9.0-1]), and the client never has more
    /// unacknowledged QoS 1 PUBLISHes than that ([MQTT-3.3.4-7], [MQTT-4.9.0-2]).
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
        let (tx, _rx) = flume::bounded::<InboundMessage>(4);
        let (client, connection, mut phase) =
            connect(options, static_test_credentials(), NoReconnect, tx, NoopLifecycle).expect("connect wiring");

        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        let client_shutdown = client.clone();
        let client2 = client.clone();
        let first = tokio::spawn(async move {
            client
                .send(Publish::from(sample_message("t/a", b"1")).qos(QoS::AtLeastOnce))
                .await
        });
        tokio::task::yield_now().await;
        let second = tokio::spawn(async move {
            client2
                .send(Publish::from(sample_message("t/b", b"2")).qos(QoS::AtLeastOnce))
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

    /// CONNACK `server_receive_maximum=1` with withheld PUBACK: a second QoS1 `publish` waits
    /// for a window slot ([MQTT-4.9.0-2], [MQTT-3.3.4-7]) instead of failing, and stays off the wire.
    #[tokio::test]
    async fn qos1_publish_waits_for_window_when_inflight_exhausted() {
        use tokio::sync::Notify;

        let first_publish_seen = Arc::new(Notify::new());
        let first_publish_seen_broker = Arc::clone(&first_publish_seen);
        let publishes_on_wire = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let publishes_on_wire_broker = Arc::clone(&publishes_on_wire);
        let port = spawn_broker_once(move |mut framed_read, mut framed_write| async move {
            let mut ack = ConnectAck::default();
            ack.receive_max = NonZeroU16::new(1).expect("nz");
            if !accept_connect_send_connack_with(&mut framed_read, &mut framed_write, ack).await {
                return;
            }
            // Withhold PUBACKs so the outbound QoS1 window stays full; signal each PUBLISH on wire.
            while let Some(Ok(frame)) = framed_read.next().await {
                if let (DecodedFrame::Publish(_), _) = frame {
                    publishes_on_wire_broker.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    first_publish_seen_broker.notify_one();
                }
            }
        })
        .await;

        let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
        let options = ConnectOptions::new(url).receive_maximum(NonZeroU16::new(64).expect("nz"));
        let (tx, _rx) = flume::bounded::<InboundMessage>(4);
        let (client, connection, mut phase) =
            connect(options, static_test_credentials(), NoReconnect, tx, NoopLifecycle).expect("connect wiring");

        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        let first = tokio::spawn({
            let client = client.clone();
            async move {
                client
                    .send(Publish::from(sample_message("t/a", b"1")).qos(QoS::AtLeastOnce))
                    .await
            }
        });
        // Wait until the broker observes the first QoS1 PUBLISH (slot occupied).
        timeout(Duration::from_secs(2), first_publish_seen.notified())
            .await
            .expect("first QoS1 PUBLISH must reach the broker before second publish");

        let second = timeout(
            Duration::from_millis(300),
            client.send(Publish::from(sample_message("t/b", b"2")).qos(QoS::AtLeastOnce)),
        )
        .await;
        assert!(
            second.is_err(),
            "second QoS1 publish must wait for a window slot, not fail; got {second:?}"
        );
        assert_eq!(
            publishes_on_wire.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "parked publish must not exceed the broker Receive Maximum on the wire"
        );

        first.abort();
        client.shutdown().await.expect("shutdown");
        let _ = run.await;
    }

    /// Rumqttc #1057 class: subscribe must complete while the outbound QoS1 window is full.
    /// SUBSCRIBE is not delayed by the exhausted quota ([MQTT-3.3.4-8]) and its SUBACK is still
    /// processed ([MQTT-4.9.0-3]).
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
        let (tx, _rx) = flume::bounded::<InboundMessage>(4);
        let (client, connection, mut phase) =
            connect(options, static_test_credentials(), NoReconnect, tx, NoopLifecycle).expect("connect wiring");

        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        let publish_task = tokio::spawn({
            let client = client.clone();
            async move {
                client
                    .send(Publish::from(sample_message("t/inflight", b"1")).qos(QoS::AtLeastOnce))
                    .await
            }
        });
        // Fill the outbound QoS1 window before subscribe.
        tokio::time::sleep(Duration::from_millis(100)).await;
        // A second publish parks behind the full window; subscribe must still get past it.
        let parked_task = tokio::spawn({
            let client = client.clone();
            async move {
                client
                    .send(Publish::from(sample_message("t/parked", b"2")).qos(QoS::AtLeastOnce))
                    .await
            }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            observed.lock().await.iter().filter(|(kind, _)| *kind == 1).count(),
            1,
            "expected exactly one QoS1 PUBLISH on the wire before subscribe (second is parked)"
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
        parked_task.abort();
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
                    .send(Publish::from(sample_message("t/dup", b"once")).qos(QoS::AtLeastOnce))
                    .await
                    .expect("first PUBACK completes publish");

                // Second publish must still work (id freed after definitive completion; duplicate ignored).
                client
                    .send(Publish::from(sample_message("t/dup2", b"twice")).qos(QoS::AtLeastOnce))
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
            client.send(Publish::from(sample_message("t/lost", b"x")).qos(QoS::AtLeastOnce)),
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

    /// Graceful `shutdown` with a QoS1 waiter the broker never acknowledges → `ConnectionLost`
    /// once the graceful budget runs out (live `graceful_disconnect`).
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

        let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
        let options = ConnectOptions::new(url).graceful_disconnect_timeout(Duration::from_millis(100));
        let (tx, _rx) = flume::bounded::<InboundMessage>(4);
        let (client, connection, mut phase) =
            connect(options, static_test_credentials(), NoReconnect, tx, NoopLifecycle).expect("connect wiring");

        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        let publish = tokio::spawn({
            let client = client.clone();
            async move {
                client
                    .send(Publish::from(sample_message("t/shutdown", b"x")).qos(QoS::AtLeastOnce))
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

    // --- resend of unacknowledged QoS 1 on session resume ---

    /// Frames a resume-test broker saw: `(connection index, packet id, dup, payload)`.
    type SeenPublishes = Arc<tokio::sync::Mutex<Vec<(usize, NonZeroU16, bool, Bytes)>>>;

    /// Connection 0: CONNACK without session, swallow two QoS 1 PUBLISHes, then drop TCP.
    /// Connection 1: CONNACK with `second_session_present`, PUBACK everything.
    async fn spawn_resume_broker(seen: SeenPublishes, second_session_present: bool) -> u16 {
        let connections = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        crate::connection::test_support::spawn_broker_loop(move |mut framed_read, mut framed_write| {
            let seen = Arc::clone(&seen);
            let connections = Arc::clone(&connections);
            async move {
                let index = connections.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let mut ack = ConnectAck::default();
                ack.session_present = index > 0 && second_session_present;
                if !accept_connect_send_connack_with(&mut framed_read, &mut framed_write, ack).await {
                    return;
                }
                let mut swallowed = 0;
                while let Some(Ok(frame)) = framed_read.next().await {
                    let (DecodedFrame::Publish(packet), _) = frame else {
                        continue;
                    };
                    let id = packet.meta.packet_id.expect("QoS 1 packet id");
                    seen.lock()
                        .await
                        .push((index, id, packet.meta.dup, packet.message.payload.clone()));
                    if index == 0 {
                        swallowed += 1;
                        if swallowed == 2 {
                            return; // drop the connection with both PUBACKs outstanding
                        }
                        continue;
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
            }
        })
        .await
    }

    fn resumable_options(port: u16) -> ConnectOptions {
        let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
        ConnectOptions::new(url)
            .client_id(crate::ClientId::try_new("resume-test").expect("client id"))
            .clean_start(false)
            .session_expiry(Some(Duration::from_secs(60)))
    }

    /// Session Present after reconnect: both PUBLISHes are resent in order, with their
    /// original ids and DUP set, and the callers' completions resolve on the new PUBACKs.
    /// Covers [MQTT-4.4.0-1] (resend with original ids), [MQTT-4.6.0-1] (original order),
    /// [MQTT-3.3.1-1] (DUP 1 on re-delivery), [MQTT-4.3.2-2] (first transmission has DUP 0) and
    /// [MQTT-3.1.2-23] (the unacknowledged PUBLISHes survive the closed connection).
    #[tokio::test]
    async fn unacked_qos1_is_resent_with_dup_on_session_resume() {
        let seen: SeenPublishes = Arc::default();
        let port = spawn_resume_broker(Arc::clone(&seen), true).await;
        let (tx, _rx) = flume::bounded::<InboundMessage>(4);
        let (client, connection, mut phase) = connect(
            resumable_options(port),
            static_test_credentials(),
            ImmediateReconnect,
            tx,
            NoopLifecycle,
        )
        .expect("connect wiring");
        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        let first = client
            .try_start_publish(Publish::new("t/resume", "a").qos(QoS::AtLeastOnce))
            .expect("enqueue a");
        let second = client
            .try_start_publish(Publish::new("t/resume", "b").qos(QoS::AtLeastOnce))
            .expect("enqueue b");
        timeout(Duration::from_secs(3), async {
            first.wait().await?;
            second.wait().await
        })
        .await
        .expect("completions must resolve after resend")
        .expect("resent publishes must be acknowledged");

        let frames = seen.lock().await.clone();
        let first_conn: Vec<_> = frames.iter().filter(|f| f.0 == 0).collect();
        let second_conn: Vec<_> = frames.iter().filter(|f| f.0 == 1).collect();
        assert_eq!(first_conn.len(), 2);
        assert_eq!(second_conn.len(), 2, "both unacknowledged publishes must be resent");
        for (original, resent) in first_conn.iter().zip(&second_conn) {
            assert!(!original.2, "first transmission must not set DUP");
            assert!(resent.2, "resend must set DUP ([MQTT-3.3.1-1])");
            assert_eq!(original.1, resent.1, "resend must reuse the original packet id");
            assert_eq!(original.3, resent.3, "resend keeps send order ([MQTT-4.6.0-1])");
        }

        client.shutdown().await.expect("shutdown");
        let _ = run.await;
    }

    /// The resumed connection allows fewer publishes in flight than are unacknowledged: all of
    /// them are still resent ([MQTT-4.4.0-1]) with DUP and their original ids, but never more
    /// at once than the new Receive Maximum ([MQTT-4.9.0-2]); the next goes out after a PUBACK.
    #[tokio::test]
    async fn unacked_qos1_beyond_a_smaller_receive_maximum_waits_and_is_resent() {
        let seen: SeenPublishes = Arc::default();
        let connections = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let port = crate::connection::test_support::spawn_broker_loop({
            let seen = Arc::clone(&seen);
            move |mut framed_read, mut framed_write| {
                let seen = Arc::clone(&seen);
                let connections = Arc::clone(&connections);
                async move {
                    let index = connections.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let mut ack = ConnectAck::default();
                    if index > 0 {
                        ack.session_present = true;
                        ack.receive_max = NonZeroU16::new(1).expect("nz");
                    }
                    if !accept_connect_send_connack_with(&mut framed_read, &mut framed_write, ack).await {
                        return;
                    }
                    let mut swallowed = 0;
                    loop {
                        // On the resumed connection a second PUBLISH before our PUBACK would
                        // overrun Receive Maximum 1.
                        let next = if index > 0 {
                            match timeout(Duration::from_millis(100), framed_read.next()).await {
                                Ok(next) => next,
                                Err(_) => continue,
                            }
                        } else {
                            framed_read.next().await
                        };
                        let Some(Ok((DecodedFrame::Publish(packet), _))) = next else {
                            return;
                        };
                        let id = packet.meta.packet_id.expect("QoS 1 packet id");
                        seen.lock()
                            .await
                            .push((index, id, packet.meta.dup, packet.message.payload.clone()));
                        if index == 0 {
                            swallowed += 1;
                            if swallowed == 3 {
                                return;
                            }
                            continue;
                        }
                        // Hold the PUBACK: nothing else may arrive meanwhile.
                        let early = timeout(Duration::from_millis(100), framed_read.next()).await;
                        assert!(early.is_err(), "PUBLISH beyond Receive Maximum 1: {early:?}");
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
            }
        })
        .await;
        let (tx, _rx) = flume::bounded::<InboundMessage>(4);
        let (client, connection, mut phase) = connect(
            resumable_options(port),
            static_test_credentials(),
            ImmediateReconnect,
            tx,
            NoopLifecycle,
        )
        .expect("connect wiring");
        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        let completions: Vec<_> = ["a", "b", "c"]
            .into_iter()
            .map(|payload| {
                client
                    .try_start_publish(Publish::new("t/resume", payload).qos(QoS::AtLeastOnce))
                    .expect("enqueue")
            })
            .collect();
        for completion in completions {
            timeout(Duration::from_secs(5), completion.wait())
                .await
                .expect("completion resolves")
                .expect("resent and acknowledged");
        }

        let frames = seen.lock().await.clone();
        let original: Vec<_> = frames.iter().filter(|f| f.0 == 0).collect();
        let resent: Vec<_> = frames.iter().filter(|f| f.0 == 1).collect();
        assert_eq!(resent.len(), 3, "every unacknowledged publish is resent: {frames:?}");
        for (original, resent) in original.iter().zip(&resent) {
            assert!(resent.2, "DUP set");
            assert_eq!((original.1, &original.3), (resent.1, &resent.3), "same id and order");
        }

        client.shutdown().await.expect("shutdown");
        let _ = run.await;
    }

    /// No session on the broker after reconnect: the client discards its own state
    /// ([MQTT-3.2.2-5]) — nothing is resent ([MQTT-4.4.0-1]: no resend at any other time) and the
    /// callers see `ConnectionLost`.
    #[tokio::test]
    async fn unacked_qos1_fails_when_broker_lost_the_session() {
        let seen: SeenPublishes = Arc::default();
        let port = spawn_resume_broker(Arc::clone(&seen), false).await;
        let (tx, _rx) = flume::bounded::<InboundMessage>(4);
        let (client, connection, mut phase) = connect(
            resumable_options(port),
            static_test_credentials(),
            ImmediateReconnect,
            tx,
            NoopLifecycle,
        )
        .expect("connect wiring");
        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        let first = client
            .try_start_publish(Publish::new("t/resume", "a").qos(QoS::AtLeastOnce))
            .expect("enqueue a");
        let second = client
            .try_start_publish(Publish::new("t/resume", "b").qos(QoS::AtLeastOnce))
            .expect("enqueue b");
        for completion in [first, second] {
            let err = timeout(Duration::from_secs(3), completion.wait())
                .await
                .expect("completion must resolve")
                .expect_err("publish from a discarded session must fail");
            assert!(matches!(err, AppError::ConnectionLost), "got {err:?}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            seen.lock().await.iter().all(|f| f.0 == 0),
            "nothing may be resent into a fresh session"
        );

        client.shutdown().await.expect("shutdown");
        let _ = run.await;
    }

    /// Clean Start sessions keep the old behaviour: connection loss fails in-flight QoS 1
    /// (Clean Start 1 discards the old session, [MQTT-3.1.2-4]).
    #[tokio::test]
    async fn unacked_qos1_fails_on_connection_loss_with_clean_start() {
        let seen: SeenPublishes = Arc::default();
        let port = spawn_resume_broker(Arc::clone(&seen), false).await;
        let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
        let (tx, _rx) = flume::bounded::<InboundMessage>(4);
        let (client, connection, mut phase) = connect(
            ConnectOptions::new(url),
            static_test_credentials(),
            ImmediateReconnect,
            tx,
            NoopLifecycle,
        )
        .expect("connect wiring");
        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        let first = client
            .try_start_publish(Publish::new("t/clean", "a").qos(QoS::AtLeastOnce))
            .expect("enqueue a");
        let _second = client
            .try_start_publish(Publish::new("t/clean", "b").qos(QoS::AtLeastOnce))
            .expect("enqueue b");
        let err = timeout(Duration::from_secs(3), first.wait())
            .await
            .expect("completion must resolve")
            .expect_err("clean session loss must fail the publish");
        assert!(matches!(err, AppError::ConnectionLost), "got {err:?}");

        client.shutdown().await.expect("shutdown");
        let _ = run.await;
    }

    /// A PUBACK with a Reason Code of 0x80 or more still acknowledges the PUBLISH: in a
    /// persistent session that is resumed after the connection drops, the rejected PUBLISH is
    /// not resent ([MQTT-4.4.0-2]). Only the follow-up PUBLISH sent on the new connection goes
    /// out, and it goes out as a first transmission.
    #[tokio::test]
    async fn rejected_qos1_is_not_resent_on_session_resume() {
        let seen: SeenPublishes = Arc::default();
        let seen_broker = Arc::clone(&seen);
        let connections = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (resumed_tx, resumed_rx) = flume::bounded::<()>(1);
        let port = crate::connection::test_support::spawn_broker_loop(move |mut framed_read, mut framed_write| {
            let seen = Arc::clone(&seen_broker);
            let connections = Arc::clone(&connections);
            let resumed_tx = resumed_tx.clone();
            async move {
                let index = connections.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let mut ack = ConnectAck::default();
                ack.session_present = index > 0;
                if !accept_connect_send_connack_with(&mut framed_read, &mut framed_write, ack).await {
                    return;
                }
                if index > 0 {
                    let _ = resumed_tx.try_send(());
                }
                while let Some(Ok(frame)) = framed_read.next().await {
                    let (DecodedFrame::Publish(packet), _) = frame else {
                        continue;
                    };
                    let id = packet.meta.packet_id.expect("QoS 1 packet id");
                    seen.lock()
                        .await
                        .push((index, id, packet.meta.dup, packet.message.payload.clone()));
                    let reason_code = if index == 0 {
                        crate::codec::PublishAckReason::NotAuthorized
                    } else {
                        crate::codec::PublishAckReason::Success
                    };
                    let _ = framed_write
                        .send(Packet::PublishAck(PublishAck {
                            packet_id: id,
                            reason_code,
                            properties: Vec::new(),
                            reason_string: None,
                        }))
                        .await;
                    if index == 0 {
                        return; // drop the connection right after the rejection
                    }
                }
            }
        })
        .await;
        let (tx, _rx) = flume::bounded::<InboundMessage>(4);
        let (client, connection, mut phase) = connect(
            resumable_options(port),
            static_test_credentials(),
            ImmediateReconnect,
            tx,
            NoopLifecycle,
        )
        .expect("connect wiring");
        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        let err = timeout(
            Duration::from_secs(3),
            client.publish("t/rejected", "rejected").qos(QoS::AtLeastOnce),
        )
        .await
        .expect("PUBACK arrives")
        .expect_err("broker rejects the publish");
        let AppError::PublishRejected(rejection) = err else {
            panic!("expected PublishRejected, got {err:?}");
        };
        assert_eq!(rejection.reason, crate::codec::PublishAckReason::NotAuthorized);

        // The broker has dropped the connection and accepted the reconnect with Session Present 1.
        timeout(Duration::from_secs(3), resumed_rx.recv_async())
            .await
            .expect("client reconnects")
            .expect("broker alive");

        // A marker PUBLISH proves the resume has run: a resend would precede or follow it.
        timeout(
            Duration::from_secs(3),
            client.publish("t/rejected", "marker").qos(QoS::AtLeastOnce),
        )
        .await
        .expect("marker acknowledged")
        .expect("marker accepted");
        tokio::time::sleep(Duration::from_millis(100)).await;

        let frames = seen.lock().await.clone();
        assert_eq!(frames.iter().filter(|f| f.0 == 0).count(), 1, "one first transmission");
        let resumed: Vec<_> = frames.iter().filter(|f| f.0 == 1).collect();
        assert_eq!(
            resumed.len(),
            1,
            "only the marker may appear on the resumed connection, got {resumed:?}"
        );
        assert_eq!(resumed[0].3.as_ref(), b"marker");
        assert!(!resumed[0].2, "the marker is a first transmission, not DUP");

        client.shutdown().await.expect("shutdown");
        let _ = run.await;
    }

    // --- graceful shutdown drain ---

    /// Broker for drain tests: on each QoS 1 PUBLISH, first pushes `inbound_burst` QoS 0
    /// PUBLISHes to the client, then (unless `never_ack`) waits `ack_delay` and PUBACKs.
    /// Records the order of PUBACK-relevant events and whether DISCONNECT arrived.
    async fn spawn_drain_broker(
        inbound_burst: usize,
        ack_delay: Duration,
        never_ack: bool,
        events: Arc<tokio::sync::Mutex<Vec<&'static str>>>,
    ) -> u16 {
        spawn_broker_once(move |mut framed_read, mut framed_write| async move {
            if !accept_connect_send_connack(&mut framed_read, &mut framed_write).await {
                return;
            }
            while let Some(Ok(frame)) = framed_read.next().await {
                match frame {
                    (DecodedFrame::Publish(packet), _) => {
                        events.lock().await.push("publish");
                        let mut burst = bytes::BytesMut::new();
                        for i in 0..inbound_burst {
                            let inbound = PublishMessage::new("in/drain", format!("m{i}"));
                            let delivery = crate::codec::PublishDelivery {
                                dup: false,
                                retain: false,
                                qos: QoS::AtMostOnce,
                                packet_id: None,
                            };
                            crate::codec::encode_publish(
                                &inbound,
                                delivery,
                                crate::codec::DEFAULT_MAX_PACKET_SIZE,
                                &mut burst,
                            )
                            .expect("encode inbound");
                        }
                        let _ = tokio::io::AsyncWriteExt::write_all(framed_write.get_mut(), &burst).await;
                        let _ = framed_write.flush().await;
                        let Some(id) = packet.meta.packet_id else {
                            continue;
                        };
                        if never_ack {
                            continue;
                        }
                        tokio::time::sleep(ack_delay).await;
                        let _ = framed_write
                            .send(Packet::PublishAck(PublishAck {
                                packet_id: id,
                                reason_code: crate::codec::PublishAckReason::Success,
                                properties: Vec::new(),
                                reason_string: None,
                            }))
                            .await;
                        events.lock().await.push("puback");
                    }
                    (DecodedFrame::Control(Packet::Disconnect(_)), _) => {
                        events.lock().await.push("disconnect");
                        break;
                    }
                    _ => {}
                }
            }
        })
        .await
    }

    /// Waits until the drain broker has recorded `n` events (it runs on its own task).
    async fn drain_events(events: &tokio::sync::Mutex<Vec<&'static str>>, n: usize) -> Vec<&'static str> {
        timeout(Duration::from_secs(2), async {
            loop {
                let seen = events.lock().await.clone();
                if seen.len() >= n {
                    return seen;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("broker did not record {n} events"))
    }

    /// Shutdown right after a QoS 1 publish waits for its delayed PUBACK instead of failing it,
    /// and DISCONNECT goes out only after that.
    #[tokio::test]
    async fn shutdown_waits_for_outbound_puback() {
        let events = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let port = spawn_drain_broker(0, Duration::from_millis(100), false, Arc::clone(&events)).await;
        let (client, connection, mut phase) = connect_test_client(port).await;
        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        let completion = client
            .try_start_publish(Publish::new("t/drain", "x").qos(QoS::AtLeastOnce))
            .expect("enqueue");
        client.shutdown().await.expect("shutdown");
        completion
            .wait()
            .await
            .expect("publish enqueued before shutdown must complete on its PUBACK");
        let _ = timeout(Duration::from_secs(3), run).await.expect("driver stops");
        assert_eq!(drain_events(&events, 3).await, ["publish", "puback", "disconnect"]);
    }

    /// Inbound messages the broker sends while the client drains reach the app instead of
    /// being dropped with the socket buffer.
    #[tokio::test]
    async fn shutdown_delivers_inbound_received_during_drain() {
        let events = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let port = spawn_drain_broker(100, Duration::from_millis(20), false, Arc::clone(&events)).await;
        let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
        let (tx, rx) = flume::bounded::<InboundMessage>(256);
        let (client, connection, mut phase) = connect(
            ConnectOptions::new(url),
            static_test_credentials(),
            NoReconnect,
            tx,
            NoopLifecycle,
        )
        .expect("connect wiring");
        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        let completion = client
            .try_start_publish(Publish::new("t/drain", "x").qos(QoS::AtLeastOnce))
            .expect("enqueue");
        client.shutdown().await.expect("shutdown");
        completion.wait().await.expect("PUBACK during drain");
        let _ = timeout(Duration::from_secs(3), run).await.expect("driver stops");
        assert_eq!(
            rx.len(),
            100,
            "every inbound message sent before the PUBACK must be delivered"
        );
    }

    /// Commands still queued when shutdown is requested are sent, and new ones are refused.
    #[tokio::test]
    async fn shutdown_sends_queued_publishes_and_refuses_new_ones() {
        let observed = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let port = spawn_mock_broker_record_publishes(Arc::clone(&observed), NonZeroU16::new(8).expect("nz")).await;
        let (client, connection, mut phase) = connect_test_client(port).await;
        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        // Queue without yielding, so the write task has not seen them before shutdown.
        let completions: Vec<_> = (0..5u8)
            .map(|i| {
                client
                    .try_start_publish(Publish::new("t/queued", Bytes::from(vec![i])).qos(QoS::AtLeastOnce))
                    .expect("enqueue")
            })
            .collect();
        client.shutdown().await.expect("shutdown");
        let refused = client.publish("t/late", "x").await;
        assert!(matches!(refused, Err(AppError::ShuttingDown)), "got {refused:?}");
        for completion in completions {
            completion
                .wait()
                .await
                .expect("queued publish must be sent and acknowledged");
        }
        let _ = timeout(Duration::from_secs(3), run).await.expect("driver stops");
        assert_eq!(observed.lock().await.len(), 5);
    }

    /// A broker that never acknowledges cannot hold shutdown past the graceful budget; the
    /// publish fails with `ConnectionLost` and DISCONNECT is still attempted.
    #[tokio::test]
    async fn shutdown_drain_is_bounded_by_graceful_budget() {
        let events = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let port = spawn_drain_broker(0, Duration::ZERO, true, Arc::clone(&events)).await;
        let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
        let options = ConnectOptions::new(url).graceful_disconnect_timeout(Duration::from_millis(200));
        let (tx, _rx) = flume::bounded::<InboundMessage>(4);
        let (client, connection, mut phase) =
            connect(options, static_test_credentials(), NoReconnect, tx, NoopLifecycle).expect("connect wiring");
        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        let completion = client
            .try_start_publish(Publish::new("t/drain", "x").qos(QoS::AtLeastOnce))
            .expect("enqueue");
        let started = std::time::Instant::now();
        client.shutdown().await.expect("shutdown");
        let err = completion.wait().await.expect_err("never-acked publish must fail");
        assert!(matches!(err, AppError::ConnectionLost), "got {err:?}");
        let _ = timeout(Duration::from_secs(3), run).await.expect("driver stops");
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "drain must respect the budget"
        );
        assert_eq!(drain_events(&events, 2).await, ["publish", "disconnect"]);
    }

    /// Publishes that never reached the wire (parked behind a full Receive Maximum window, or
    /// still queued in the command channel) survive a connection loss and go out on the next
    /// connection; only the one already sent fails in a Clean Start session.
    #[tokio::test]
    async fn unsent_publishes_are_sent_after_reconnect() {
        let seen: SeenPublishes = Arc::default();
        let seen_broker = Arc::clone(&seen);
        let connections = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let port = crate::connection::test_support::spawn_broker_loop(move |mut framed_read, mut framed_write| {
            let seen = Arc::clone(&seen_broker);
            let connections = Arc::clone(&connections);
            async move {
                let index = connections.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let mut ack = ConnectAck::default();
                ack.receive_max = NonZeroU16::new(1).expect("nz");
                if !accept_connect_send_connack_with(&mut framed_read, &mut framed_write, ack).await {
                    return;
                }
                while let Some(Ok(frame)) = framed_read.next().await {
                    let (DecodedFrame::Publish(packet), _) = frame else {
                        continue;
                    };
                    let id = packet.meta.packet_id.expect("QoS 1 packet id");
                    seen.lock()
                        .await
                        .push((index, id, packet.meta.dup, packet.message.payload.clone()));
                    if index == 0 {
                        // Let the client park / queue the rest, then drop with no PUBACK.
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        return;
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
            }
        })
        .await;

        let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
        let options = ConnectOptions::new(url).command_channel_capacity(NonZeroUsize::new(64).expect("nz"));
        let (tx, _rx) = flume::bounded::<InboundMessage>(4);
        let (client, connection, mut phase) = connect(
            options,
            static_test_credentials(),
            ImmediateReconnect,
            tx,
            NoopLifecycle,
        )
        .expect("connect wiring");
        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        let completions: Vec<_> = (0..40u8)
            .map(|i| {
                client
                    .try_start_publish(Publish::new("t/unsent", Bytes::from(vec![i])).qos(QoS::AtLeastOnce))
                    .expect("enqueue")
            })
            .collect();
        let mut results = Vec::new();
        for completion in completions {
            results.push(
                timeout(Duration::from_secs(5), completion.wait())
                    .await
                    .expect("resolves"),
            );
        }

        assert!(
            matches!(results[0], Err(AppError::ConnectionLost)),
            "the publish sent on the lost connection fails in a Clean Start session: {:?}",
            results[0]
        );
        for (i, result) in results.iter().enumerate().skip(1) {
            assert!(
                result.is_ok(),
                "publish {i} was never sent and must go out after reconnect: {result:?}"
            );
        }
        let frames = seen.lock().await.clone();
        let resent: Vec<u8> = frames.iter().filter(|f| f.0 == 1).map(|f| f.3[0]).collect();
        assert_eq!(resent, (1..40u8).collect::<Vec<_>>(), "sent once each, in order");
        assert!(frames.iter().all(|f| !f.2), "never-sent publishes are new, not DUP");

        client.shutdown().await.expect("shutdown");
        let _ = run.await;
    }

    /// `is_connected` follows the session; with `fail_fast_when_disconnected` commands issued
    /// while no connection is up fail at once with `NotConnected` instead of queueing.
    #[tokio::test]
    async fn fail_fast_when_disconnected_rejects_during_reconnect() {
        // Every connection: CONNACK, then close after the first PUBLISH.
        let port = crate::connection::test_support::spawn_broker_loop(|mut framed_read, mut framed_write| async move {
            if !accept_connect_send_connack(&mut framed_read, &mut framed_write).await {
                return;
            }
            while let Some(Ok(frame)) = framed_read.next().await {
                if matches!(frame, (DecodedFrame::Publish(_), _)) {
                    return;
                }
            }
        })
        .await;

        /// Long backoff so the test can observe the disconnected window.
        struct SlowReconnect;
        impl ReconnectPolicy for SlowReconnect {
            fn delay_after(&self, outcome: &ConnectionOutcome) -> Option<Duration> {
                match outcome {
                    ConnectionOutcome::Shutdown(_) | ConnectionOutcome::Application(_) => None,
                    _ => Some(Duration::from_secs(30)),
                }
            }
        }

        let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
        let options = ConnectOptions::new(url).fail_fast_when_disconnected(true);
        let (tx, _rx) = flume::bounded::<InboundMessage>(4);
        let (client, connection, mut phase) =
            connect(options, static_test_credentials(), SlowReconnect, tx, NoopLifecycle).expect("connect wiring");
        assert!(!client.is_connected(), "not connected before the driver runs");
        assert!(matches!(client.publish("t/ff", "x").await, Err(AppError::NotConnected)));

        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;
        assert!(client.is_connected());
        client
            .publish("t/ff", "x")
            .await
            .expect("QoS 0 publish while connected");

        crate::connection::test_support::await_disconnected(&mut phase).await;
        assert!(!client.is_connected());
        let started = std::time::Instant::now();
        let err = client
            .publish("t/ff", "y")
            .qos(QoS::AtLeastOnce)
            .await
            .expect_err("publish during backoff must fail fast");
        assert!(matches!(err, AppError::NotConnected), "got {err:?}");
        assert!(started.elapsed() < Duration::from_millis(100), "no ack_timeout wait");
        let err = client
            .subscribe([("t/#", QoS::AtMostOnce)])
            .await
            .expect_err("subscribe too");
        assert!(matches!(err, AppError::NotConnected), "got {err:?}");

        client.shutdown().await.expect("shutdown");
        let _ = timeout(Duration::from_secs(3), run).await;
    }

    /// Default (queue): a publish issued during backoff is not rejected but waits.
    #[tokio::test]
    async fn default_queues_while_disconnected() {
        let url = BrokerUrl::parse("mqtt://127.0.0.1:9").expect("valid url");
        let options = ConnectOptions::new(url).ack_timeout(Duration::from_millis(50));
        let (tx, _rx) = flume::bounded::<InboundMessage>(4);
        let (client, _connection, _phase) =
            connect(options, static_test_credentials(), NoReconnect, tx, NoopLifecycle).expect("connect wiring");
        assert!(!client.is_connected());
        let err = client.publish("t/q", "x").await.expect_err("nobody encodes it");
        assert!(
            matches!(err, AppError::PublishAckTimeout(_)),
            "queued, then timed out: {err:?}"
        );
    }

    /// A caller waiting for command-channel room is released by `shutdown` with
    /// `ShuttingDown`, not after its ack timeout.
    #[tokio::test]
    async fn shutdown_releases_callers_waiting_for_channel_room() {
        // Accept TCP but never CONNACK: no write task drains the channel.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        tokio::spawn(async move {
            let _held = listener.accept().await;
            std::future::pending::<()>().await;
        });
        let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
        let options = ConnectOptions::new(url)
            .command_channel_capacity(NonZeroUsize::new(1).expect("nz"))
            .ack_timeout(Duration::from_secs(10))
            .connect_timeout(Duration::from_secs(10));
        let (tx, _rx) = flume::bounded::<InboundMessage>(4);
        let (client, connection, _phase) =
            connect(options, static_test_credentials(), NoReconnect, tx, NoopLifecycle).expect("connect wiring");
        let run = tokio::spawn(connection.run());

        let _queued = client
            .try_start_publish(Publish::new("t/w", "fills the channel"))
            .expect("first fits");
        let waiter = tokio::spawn({
            let client = client.clone();
            async move { client.publish("t/w", "waits for room").await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!waiter.is_finished(), "second publish must be waiting for room");

        client.shutdown().await.expect("shutdown");
        let result = timeout(Duration::from_secs(2), waiter)
            .await
            .expect("waiter released promptly")
            .expect("join");
        assert!(matches!(result, Err(AppError::ShuttingDown)), "got {result:?}");
        let _ = timeout(Duration::from_secs(2), run).await;
    }

    /// The inbound sink sees the delivery flags: retained vs live, DUP, delivered QoS.
    #[tokio::test]
    async fn inbound_message_carries_retain_dup_and_qos() {
        let port = spawn_broker_once(|mut framed_read, mut framed_write| async move {
            if !accept_connect_send_connack(&mut framed_read, &mut framed_write).await {
                return;
            }
            let mut wire = bytes::BytesMut::new();
            let retained = PublishMessage::new("status/dev1", "online");
            let delivery = crate::codec::PublishDelivery {
                dup: true,
                retain: true,
                qos: QoS::AtLeastOnce,
                packet_id: Some(NonZeroU16::new(7).expect("nz")),
            };
            crate::codec::encode_publish(&retained, delivery, crate::codec::DEFAULT_MAX_PACKET_SIZE, &mut wire)
                .expect("encode retained");
            let live = PublishMessage::new("status/dev1", "offline");
            let delivery = crate::codec::PublishDelivery {
                dup: false,
                retain: false,
                qos: QoS::AtMostOnce,
                packet_id: None,
            };
            crate::codec::encode_publish(&live, delivery, crate::codec::DEFAULT_MAX_PACKET_SIZE, &mut wire)
                .expect("encode live");
            let _ = tokio::io::AsyncWriteExt::write_all(framed_write.get_mut(), &wire).await;
            while framed_read.next().await.is_some() {}
        })
        .await;

        let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
        let (tx, rx) = flume::bounded::<InboundMessage>(4);
        let (client, connection, mut phase) = connect(
            ConnectOptions::new(url),
            static_test_credentials(),
            NoReconnect,
            tx,
            NoopLifecycle,
        )
        .expect("connect wiring");
        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        let first = timeout(Duration::from_secs(2), rx.recv_async())
            .await
            .expect("first")
            .expect("open");
        assert_eq!(&*first.payload, b"online");
        assert!(first.retain && first.dup);
        assert_eq!(first.qos, QoS::AtLeastOnce);
        let second = timeout(Duration::from_secs(2), rx.recv_async())
            .await
            .expect("second")
            .expect("open");
        assert_eq!(&*second.payload, b"offline");
        assert!(!second.retain && !second.dup);
        assert_eq!(second.qos, QoS::AtMostOnce);

        client.shutdown().await.expect("shutdown");
        let _ = run.await;
    }

    // --- Topic Aliases ---

    /// Outbound aliases: the first PUBLISH per topic binds an alias (topic + alias), later
    /// ones send an empty topic + alias; with more topics than aliases the least recently
    /// used binding is rebound. Never more aliases than the broker's maximum ([MQTT-3.3.2-9],
    /// [MQTT-3.2.2-17]) and never alias 0 ([MQTT-3.3.2-8]).
    #[tokio::test]
    async fn outbound_topic_aliases_bind_reuse_and_rebind() {
        let seen = Arc::new(tokio::sync::Mutex::new(Vec::<(String, Option<u16>)>::new()));
        let seen_broker = Arc::clone(&seen);
        let port = spawn_broker_once(move |mut framed_read, mut framed_write| async move {
            let mut ack = ConnectAck::default();
            ack.topic_alias_max = 2;
            if !accept_connect_send_connack_with(&mut framed_read, &mut framed_write, ack).await {
                return;
            }
            while let Some(Ok((frame, _))) = framed_read.next().await {
                if let DecodedFrame::Publish(packet) = frame {
                    seen_broker.lock().await.push((
                        packet.message.topic.to_string(),
                        packet.wire_topic_alias.map(|a| a.get()),
                    ));
                }
            }
        })
        .await;
        let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
        let (tx, _rx) = flume::bounded::<InboundMessage>(4);
        let options = ConnectOptions::new(url).outbound_topic_aliases(8);
        let (client, connection, mut phase) =
            connect(options, static_test_credentials(), NoReconnect, tx, NoopLifecycle).expect("connect wiring");
        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        for topic in ["dev/a", "dev/a", "dev/b", "dev/a", "dev/c", "dev/c"] {
            client.publish(topic, "x").await.expect("publish");
        }
        let frames = timeout(Duration::from_secs(2), async {
            loop {
                let frames = seen.lock().await.clone();
                if frames.len() == 6 {
                    return frames;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("all frames");
        let expected = [
            ("dev/a", Some(1)), // bind a→1
            ("", Some(1)),      // reuse
            ("dev/b", Some(2)), // bind b→2
            ("", Some(1)),      // reuse a (b is now least recently used)
            ("dev/c", Some(2)), // capped at the broker's 2: rebind b's alias to c
            ("", Some(2)),      // reuse c
        ];
        let frames: Vec<(&str, Option<u16>)> = frames.iter().map(|(t, a)| (t.as_str(), *a)).collect();
        assert_eq!(frames, expected);

        client.shutdown().await.expect("shutdown");
        let _ = run.await;
    }

    /// A broker whose CONNACK carries no Topic Alias Maximum gets no Topic Alias, even with
    /// outbound aliases configured ([MQTT-3.2.2-18]).
    #[tokio::test]
    async fn no_outbound_topic_aliases_when_the_broker_offers_none() {
        let seen = Arc::new(tokio::sync::Mutex::new(Vec::<(String, Option<u16>)>::new()));
        let seen_broker = Arc::clone(&seen);
        let port = spawn_broker_once(move |mut framed_read, mut framed_write| async move {
            if !accept_connect_send_connack_with(&mut framed_read, &mut framed_write, ConnectAck::default()).await {
                return;
            }
            while let Some(Ok((frame, _))) = framed_read.next().await {
                if let DecodedFrame::Publish(packet) = frame {
                    seen_broker.lock().await.push((
                        packet.message.topic.to_string(),
                        packet.wire_topic_alias.map(|a| a.get()),
                    ));
                }
            }
        })
        .await;
        let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
        let (tx, _rx) = flume::bounded::<InboundMessage>(4);
        let options = ConnectOptions::new(url).outbound_topic_aliases(8);
        let (client, connection, mut phase) =
            connect(options, static_test_credentials(), NoReconnect, tx, NoopLifecycle).expect("connect wiring");
        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        for _ in 0..2 {
            client.publish("dev/a", "x").await.expect("publish");
        }
        let frames = timeout(Duration::from_secs(2), async {
            loop {
                let frames = seen.lock().await.clone();
                if frames.len() == 2 {
                    return frames;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("both frames");
        assert_eq!(frames, [("dev/a".to_owned(), None), ("dev/a".to_owned(), None)]);

        client.shutdown().await.expect("shutdown");
        let _ = run.await;
    }

    /// [MQTT-4.7.3-3]: a Topic Name over 65535 bytes is refused before it reaches the wire,
    /// and the session stays up.
    #[tokio::test]
    async fn publish_refuses_a_topic_longer_than_65535_bytes() {
        let port = spawn_mock_broker().await;
        let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
        let (tx, _rx) = flume::bounded::<InboundMessage>(4);
        let (client, connection, mut phase) = connect(
            ConnectOptions::new(url),
            static_test_credentials(),
            NoReconnect,
            tx,
            NoopLifecycle,
        )
        .expect("connect wiring");
        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        let error = client
            .publish("t".repeat(65_536), "x")
            .await
            .expect_err("oversized topic");
        assert!(matches!(error, AppError::InvalidRequest(_)), "{error:?}");
        assert!(client.is_connected());
        client.publish("t", "x").await.expect("session still usable");

        client.shutdown().await.expect("shutdown");
        let _ = run.await;
    }

    /// CONNACK `ack`, then answers every SUBSCRIBE / UNSUBSCRIBE with `extra` more (or fewer)
    /// granted / success reason codes than it has filters, and reports the client's DISCONNECT.
    async fn spawn_reason_count_broker(
        ack: ConnectAck,
        extra: isize,
        disconnect: oneshot::Sender<crate::codec::DisconnectReasonCode>,
    ) -> u16 {
        spawn_broker_once(move |mut framed_read, mut framed_write| async move {
            if !accept_connect_send_connack_with(&mut framed_read, &mut framed_write, ack).await {
                return;
            }
            let count = |filters: usize| filters.saturating_add_signed(extra);
            while let Some(Ok((frame, _))) = framed_read.next().await {
                let reply = match frame {
                    DecodedFrame::Control(Packet::Subscribe(sub)) => Packet::SubscribeAck(SubscribeAck {
                        packet_id: sub.packet_id,
                        properties: Vec::new(),
                        reason_string: None,
                        status: vec![SubscribeAckReason::GrantedQos1; count(sub.topic_filters.len())],
                    }),
                    DecodedFrame::Control(Packet::Unsubscribe(unsub)) => Packet::UnsubscribeAck(UnsubscribeAck {
                        packet_id: unsub.packet_id,
                        properties: Vec::new(),
                        reason_string: None,
                        status: vec![UnsubscribeAckReason::Success; count(unsub.topic_filters.len())],
                    }),
                    DecodedFrame::Control(Packet::Disconnect(packet)) => {
                        let _ = disconnect.send(packet.reason_code);
                        return;
                    }
                    _ => continue,
                };
                let _ = framed_write.send(reply).await;
            }
        })
        .await
    }

    async fn start_against(port: u16) -> (MqttClient, tokio::task::JoinHandle<Result<(), ConnectionOutcome>>) {
        let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
        let (tx, _rx) = flume::bounded::<InboundMessage>(4);
        let (client, connection, mut phase) = connect(
            ConnectOptions::new(url),
            static_test_credentials(),
            NoReconnect,
            tx,
            NoopLifecycle,
        )
        .expect("connect wiring");
        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;
        (client, run)
    }

    /// [MQTT-3.2.2-15]: a SUBSCRIBE or UNSUBSCRIBE over the broker's Maximum Packet Size, or
    /// with a topic filter over 65535 bytes, is refused like such a publish; the session stays.
    #[tokio::test]
    async fn subscribe_and_unsubscribe_the_broker_cannot_take_are_refused_and_the_session_stays() {
        let mut ack = ConnectAck::default();
        ack.max_packet_size = Some(64);
        let (disconnect_tx, _disconnect_rx) = oneshot::channel();
        let port = spawn_reason_count_broker(ack, 0, disconnect_tx).await;
        let (client, run) = start_against(port).await;

        for filter in ["t".repeat(100), "t".repeat(65_536)] {
            let error = client
                .subscribe([(filter.as_str(), QoS::AtLeastOnce)])
                .await
                .expect_err("subscribe too big");
            assert!(matches!(error, AppError::InvalidRequest(_)), "{error:?}");
            let error = client
                .unsubscribe([filter.as_str()])
                .await
                .expect_err("unsubscribe too big");
            assert!(matches!(error, AppError::InvalidRequest(_)), "{error:?}");
        }
        assert!(client.is_connected());
        client
            .subscribe([("t", QoS::AtLeastOnce)])
            .await
            .expect("session still usable");

        client.shutdown().await.expect("shutdown");
        let _ = run.await;
    }

    /// [MQTT-3.9.3-2], [MQTT-3.11.3-2]: a SUBACK or UNSUBACK that does not carry one reason code
    /// per filter is not an answer; the request fails and the client leaves with 0x82.
    #[rstest]
    #[case::suback_short(AckKind::Subscribe, -1)]
    #[case::suback_long(AckKind::Subscribe, 1)]
    #[case::unsuback_short(AckKind::Unsubscribe, -1)]
    #[case::unsuback_long(AckKind::Unsubscribe, 1)]
    #[tokio::test]
    async fn an_ack_with_the_wrong_number_of_reason_codes_ends_the_connection(
        #[case] kind: AckKind,
        #[case] extra: isize,
    ) {
        let (disconnect_tx, disconnect_rx) = oneshot::channel();
        let port = spawn_reason_count_broker(ConnectAck::default(), extra, disconnect_tx).await;
        let (client, run) = start_against(port).await;

        let filters = [("a", QoS::AtLeastOnce), ("b", QoS::AtLeastOnce)];
        let result = match kind {
            AckKind::Subscribe => client.subscribe(filters).await.map(drop),
            _ => client.unsubscribe(["a", "b"]).await.map(drop),
        };
        assert_eq!(result, Err(AppError::ConnectionLost));
        let reason = timeout(Duration::from_secs(2), disconnect_rx)
            .await
            .expect("DISCONNECT in time")
            .expect("broker saw a DISCONNECT");
        assert_eq!(reason, crate::codec::DisconnectReasonCode::ProtocolError);
        let outcome = timeout(Duration::from_secs(2), run)
            .await
            .expect("driver ends")
            .expect("driver task");
        assert!(matches!(outcome, Err(ConnectionOutcome::Protocol(_))), "{outcome:?}");
    }

    /// Inbound aliases: the broker binds an alias, then sends only the alias; the
    /// application sees the full topic both times ([MQTT-3.3.2-10]).
    #[tokio::test]
    async fn inbound_topic_aliases_are_resolved_before_delivery() {
        let port = spawn_broker_once(|mut framed_read, mut framed_write| async move {
            let Some(Ok((DecodedFrame::Control(Packet::Connect(connect)), _))) = framed_read.next().await else {
                return;
            };
            assert_eq!(connect.topic_alias_max, 4, "client advertises its inbound maximum");
            let _ = framed_write.send(Packet::ConnectAck(Box::default())).await;
            let mut wire = bytes::BytesMut::new();
            let delivery = crate::codec::PublishDelivery {
                dup: false,
                retain: false,
                qos: QoS::AtMostOnce,
                packet_id: None,
            };
            let alias = NonZeroU16::new(3).expect("nz");
            for (payload, mode) in [
                ("first", crate::codec::OutboundTopicAlias::Assign(alias)),
                ("second", crate::codec::OutboundTopicAlias::Reuse(alias)),
            ] {
                let message = PublishMessage::new("sensors/hall/temperature", payload);
                crate::codec::encode_publish_with_alias(&message, delivery, Some(mode), u32::MAX, &mut wire)
                    .expect("encode aliased publish");
            }
            let _ = tokio::io::AsyncWriteExt::write_all(framed_write.get_mut(), &wire).await;
            while framed_read.next().await.is_some() {}
        })
        .await;
        let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
        let (tx, rx) = flume::bounded::<InboundMessage>(4);
        let options = ConnectOptions::new(url).topic_alias_maximum(4);
        let (client, connection, mut phase) =
            connect(options, static_test_credentials(), NoReconnect, tx, NoopLifecycle).expect("connect wiring");
        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        for payload in ["first", "second"] {
            let message = timeout(Duration::from_secs(2), rx.recv_async())
                .await
                .expect("delivery")
                .expect("open");
            assert_eq!(&*message.topic, "sensors/hall/temperature");
            assert_eq!(&*message.payload, payload.as_bytes());
        }
        client.shutdown().await.expect("shutdown");
        let _ = run.await;
    }

    /// Broker diagnostics reach the application: PUBACK rejection (reason, Reason String,
    /// User Properties), SUBACK Reason String, and the DISCONNECT Reason String / Server
    /// Reference in the session outcome.
    #[tokio::test]
    async fn broker_reason_strings_and_properties_reach_the_application() {
        let port = spawn_broker_once(|mut framed_read, mut framed_write| async move {
            if !accept_connect_send_connack(&mut framed_read, &mut framed_write).await {
                return;
            }
            while let Some(Ok((frame, _))) = framed_read.next().await {
                match frame {
                    DecodedFrame::Publish(packet) => {
                        let _ = framed_write
                            .send(Packet::PublishAck(PublishAck {
                                packet_id: packet.meta.packet_id.expect("QoS 1"),
                                reason_code: crate::codec::PublishAckReason::QuotaExceeded,
                                properties: vec![("limit", "100/s").into()],
                                reason_string: Some("rate limit for tenant".into()),
                            }))
                            .await;
                    }
                    DecodedFrame::Control(Packet::Subscribe(sub)) => {
                        let _ = framed_write
                            .send(Packet::SubscribeAck(SubscribeAck {
                                packet_id: sub.packet_id,
                                properties: Vec::new(),
                                reason_string: Some("no ACL for secret/#".into()),
                                status: vec![SubscribeAckReason::NotAuthorized],
                            }))
                            .await;
                        let disconnect = crate::codec::Disconnect {
                            reason_code: crate::codec::DisconnectReasonCode::ServerMoved,
                            reason_string: Some("maintenance".into()),
                            server_reference: Some("mqtt2.example.com".into()),
                            ..crate::codec::Disconnect::default()
                        };
                        let _ = framed_write.send(Packet::Disconnect(disconnect)).await;
                        return;
                    }
                    _ => {}
                }
            }
        })
        .await;
        let (client, connection, mut phase) = connect_test_client(port).await;
        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        let err = client
            .publish("t/limited", "x")
            .qos(QoS::AtLeastOnce)
            .await
            .expect_err("broker rejects");
        let AppError::PublishRejected(rejection) = err else {
            panic!("expected PublishRejected, got {err:?}");
        };
        assert_eq!(rejection.reason, crate::codec::PublishAckReason::QuotaExceeded);
        assert_eq!(rejection.reason_string.as_deref(), Some("rate limit for tenant"));
        assert_eq!(&*rejection.user_properties[0].value, "100/s");

        let response = client
            .subscribe([("secret/#", QoS::AtLeastOnce)])
            .await
            .expect("SUBACK");
        assert_eq!(response, vec![SubscribeAckReason::NotAuthorized]);
        assert_eq!(response.reason_string.as_deref(), Some("no ACL for secret/#"));

        let outcome = timeout(Duration::from_secs(2), run)
            .await
            .expect("run ends")
            .expect("join");
        let message = format!("{outcome:?}");
        assert!(
            message.contains("maintenance") && message.contains("mqtt2.example.com"),
            "DISCONNECT diagnostics in the outcome: {message}"
        );
    }

    // --- Subscription Identifiers ---

    /// Raw PUBLISH (QoS 0) carrying Subscription Identifiers, as a broker sends it
    /// (the client's own encoder never writes them, [MQTT-3.3.4-6]).
    fn raw_publish_with_subscription_ids(topic: &str, payload: &[u8], ids: &[u8]) -> Vec<u8> {
        let mut properties = Vec::new();
        for id in ids {
            properties.extend_from_slice(&[0x0B, *id]); // Subscription Identifier, 1-byte varint
        }
        let mut body = Vec::new();
        body.extend_from_slice(&u16::try_from(topic.len()).expect("short topic").to_be_bytes());
        body.extend_from_slice(topic.as_bytes());
        body.push(u8::try_from(properties.len()).expect("short properties"));
        body.extend_from_slice(&properties);
        body.extend_from_slice(payload);
        let mut frame = vec![0x30, u8::try_from(body.len()).expect("short body")];
        frame.extend_from_slice(&body);
        frame
    }

    #[tokio::test]
    async fn subscription_ids_go_out_and_come_back() {
        let seen_id = Arc::new(tokio::sync::Mutex::new(None));
        let seen = Arc::clone(&seen_id);
        let port = spawn_broker_once(move |mut framed_read, mut framed_write| async move {
            if !accept_connect_send_connack(&mut framed_read, &mut framed_write).await {
                return;
            }
            while let Some(Ok((frame, _))) = framed_read.next().await {
                if let DecodedFrame::Control(Packet::Subscribe(sub)) = frame {
                    *seen.lock().await = sub.id;
                    let _ = framed_write
                        .send(Packet::SubscribeAck(SubscribeAck {
                            packet_id: sub.packet_id,
                            properties: Vec::new(),
                            reason_string: None,
                            status: vec![SubscribeAckReason::GrantedQos0],
                        }))
                        .await;
                    let frame = raw_publish_with_subscription_ids("plant/1/alarm", b"hot", &[7, 9]);
                    let _ = tokio::io::AsyncWriteExt::write_all(framed_write.get_mut(), &frame).await;
                }
            }
        })
        .await;
        let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
        let (tx, rx) = flume::bounded::<InboundMessage>(4);
        let (client, connection, mut phase) = connect(
            ConnectOptions::new(url),
            static_test_credentials(),
            NoReconnect,
            tx,
            NoopLifecycle,
        )
        .expect("connect wiring");
        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        client
            .subscribe_with_id(7, [("plant/+/alarm", QoS::AtMostOnce)])
            .await
            .expect("subscribe with id");
        assert_eq!(
            *seen_id.lock().await,
            std::num::NonZeroU32::new(7),
            "SUBSCRIBE carries the id"
        );
        let message = timeout(Duration::from_secs(2), rx.recv_async())
            .await
            .expect("delivery")
            .expect("open");
        let ids: Vec<u32> = message.subscription_ids.iter().map(|id| id.get()).collect();
        assert_eq!(ids, [7, 9], "every matching subscription's id, in wire order");

        for bad in [0, 268_435_456] {
            let err = client
                .subscribe_with_id(bad, [("x/#", QoS::AtMostOnce)])
                .await
                .expect_err("out-of-range id");
            assert!(matches!(err, AppError::InvalidRequest(_)), "{bad}: {err:?}");
        }
        client.shutdown().await.expect("shutdown");
        let _ = run.await;
    }

    /// A broker announcing no Subscription Identifier support gets none: the call fails
    /// instead of risking a protocol error, plain subscribes still work.
    #[tokio::test]
    async fn subscription_ids_refused_when_broker_lacks_support() {
        let port = spawn_broker_once(|mut framed_read, mut framed_write| async move {
            let mut ack = ConnectAck::default();
            ack.subscription_identifiers_available = false;
            if !accept_connect_send_connack_with(&mut framed_read, &mut framed_write, ack).await {
                return;
            }
            while let Some(Ok((frame, _))) = framed_read.next().await {
                if let DecodedFrame::Control(Packet::Subscribe(sub)) = frame {
                    assert_eq!(sub.id, None, "no id may reach this broker");
                    let _ = framed_write
                        .send(Packet::SubscribeAck(SubscribeAck {
                            packet_id: sub.packet_id,
                            properties: Vec::new(),
                            reason_string: None,
                            status: vec![SubscribeAckReason::GrantedQos0],
                        }))
                        .await;
                }
            }
        })
        .await;
        let (client, connection, mut phase) = connect_test_client(port).await;
        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        let err = client
            .subscribe_with_id(3, [("a/#", QoS::AtMostOnce)])
            .await
            .expect_err("unsupported");
        assert!(matches!(err, AppError::InvalidRequest(_)), "{err:?}");
        client
            .subscribe([("a/#", QoS::AtMostOnce)])
            .await
            .expect("plain subscribe still works");
        client.shutdown().await.expect("shutdown");
        let _ = run.await;
    }

    /// Restored subscriptions keep their identifier: after a reconnect into a fresh session
    /// the driver re-issues one SUBSCRIBE per identifier.
    #[tokio::test]
    async fn subscription_ids_survive_resubscribe() {
        let seen = Arc::new(tokio::sync::Mutex::new(Vec::<(usize, Option<u32>, Vec<String>)>::new()));
        let seen_broker = Arc::clone(&seen);
        let connections = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let port = crate::connection::test_support::spawn_broker_loop(move |mut framed_read, mut framed_write| {
            let seen = Arc::clone(&seen_broker);
            let connections = Arc::clone(&connections);
            async move {
                let index = connections.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if !accept_connect_send_connack(&mut framed_read, &mut framed_write).await {
                    return;
                }
                let mut subscribes = 0;
                while let Some(Ok((frame, _))) = framed_read.next().await {
                    if let DecodedFrame::Control(Packet::Subscribe(sub)) = frame {
                        let filters = sub.topic_filters.iter().map(|(f, _)| f.to_string()).collect();
                        seen.lock().await.push((index, sub.id.map(|id| id.get()), filters));
                        let _ = framed_write
                            .send(Packet::SubscribeAck(SubscribeAck {
                                packet_id: sub.packet_id,
                                properties: Vec::new(),
                                reason_string: None,
                                status: vec![SubscribeAckReason::GrantedQos0; sub.topic_filters.len()],
                            }))
                            .await;
                        subscribes += 1;
                        if index == 0 && subscribes == 3 {
                            return; // drop the first connection once everything is subscribed
                        }
                    }
                }
            }
        })
        .await;
        let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url");
        let (tx, _rx) = flume::bounded::<InboundMessage>(4);
        let (client, connection, mut phase) = connect(
            ConnectOptions::new(url),
            static_test_credentials(),
            ImmediateReconnect,
            tx,
            NoopLifecycle,
        )
        .expect("connect wiring");
        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        client
            .subscribe_with_id(1, [("a/#", QoS::AtMostOnce)])
            .await
            .expect("id 1");
        client
            .subscribe_with_id(2, [("b/#", QoS::AtMostOnce)])
            .await
            .expect("id 2");
        client.subscribe([("c/#", QoS::AtMostOnce)]).await.expect("no id");

        let restored = timeout(Duration::from_secs(3), async {
            loop {
                let restored: Vec<_> = seen.lock().await.iter().filter(|s| s.0 == 1).cloned().collect();
                if restored.len() == 3 {
                    return restored;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("three resubscribes on the second connection");
        let mut by_id: Vec<(Option<u32>, Vec<String>)> = restored.into_iter().map(|(_, id, f)| (id, f)).collect();
        by_id.sort();
        assert_eq!(
            by_id,
            [
                (None, vec!["c/#".to_owned()]),
                (Some(1), vec!["a/#".to_owned()]),
                (Some(2), vec!["b/#".to_owned()]),
            ]
        );
        client.shutdown().await.expect("shutdown");
        let _ = run.await;
    }

    // --- acknowledgement, AUTH and ClientID conformance ---

    /// The client acknowledges every inbound QoS 1 PUBLISH, whatever the application does with
    /// it: here the messages are discarded without ever being read ([MQTT-4.5.0-2]).
    #[tokio::test]
    async fn inbound_qos1_is_acknowledged_when_the_application_discards_it() {
        let (acked_tx, acked_rx) = flume::bounded::<Vec<NonZeroU16>>(1);
        let port = spawn_broker_once(move |mut framed_read, mut framed_write| async move {
            if !accept_connect_send_connack(&mut framed_read, &mut framed_write).await {
                return;
            }
            let ids = [NonZeroU16::new(7).expect("nz"), NonZeroU16::new(8).expect("nz")];
            let mut wire = bytes::BytesMut::new();
            for id in ids {
                let message = PublishMessage::new("t/discarded", "payload");
                let delivery = crate::codec::PublishDelivery {
                    dup: false,
                    retain: false,
                    qos: QoS::AtLeastOnce,
                    packet_id: Some(id),
                };
                crate::codec::encode_publish(&message, delivery, crate::codec::DEFAULT_MAX_PACKET_SIZE, &mut wire)
                    .expect("encode inbound publish");
            }
            let _ = tokio::io::AsyncWriteExt::write_all(framed_write.get_mut(), &wire).await;

            let mut acked = Vec::new();
            while acked.len() < ids.len() {
                match framed_read.next().await {
                    Some(Ok((DecodedFrame::Control(Packet::PublishAck(ack)), _))) => acked.push(ack.packet_id),
                    Some(Ok(_)) => {}
                    _ => break,
                }
            }
            let _ = acked_tx.send(acked);
            while framed_read.next().await.is_some() {}
        })
        .await;

        let client = crate::ClientBuilder::new(format!("mqtt://127.0.0.1:{port}"))
            .expect("url")
            .credentials(static_test_credentials())
            .discard_inbound()
            .connect()
            .await
            .expect("connect");

        let acked = timeout(Duration::from_secs(3), acked_rx.recv_async())
            .await
            .expect("broker must receive a PUBACK for each QoS 1 PUBLISH")
            .expect("broker alive");
        assert_eq!(
            acked,
            [NonZeroU16::new(7).expect("nz"), NonZeroU16::new(8).expect("nz")],
            "one PUBACK per inbound packet id"
        );

        client.shutdown().await.expect("shutdown");
    }

    /// Without an Authentication Method the client never sends AUTH ([MQTT-4.12.0-7]): not for
    /// connect, subscribe, QoS 0 and QoS 1 publishes, an AUTH the broker sends anyway (ignored),
    /// or graceful shutdown. Every packet the broker receives is recorded by type.
    #[tokio::test]
    async fn client_never_sends_auth_without_authentication_method() {
        const AUTH_TYPE: u8 = 15;
        const PUBLISH_TYPE: u8 = 3;
        let received = Arc::new(tokio::sync::Mutex::new(Vec::<u8>::new()));
        let received_broker = Arc::clone(&received);
        let (done_tx, done_rx) = flume::bounded::<()>(1);
        let port = spawn_broker_once(move |mut framed_read, mut framed_write| async move {
            // CONNECT is the first packet; record it like the rest.
            let Some(Ok((frame, _))) = framed_read.next().await else {
                return;
            };
            let DecodedFrame::Control(connect @ Packet::Connect(_)) = frame else {
                return;
            };
            received_broker.lock().await.push(connect.packet_type() >> 4);
            if framed_write.send(Packet::ConnectAck(Box::default())).await.is_err() {
                return;
            }
            while let Some(Ok((frame, _))) = framed_read.next().await {
                let (kind, reply) = match &frame {
                    DecodedFrame::Publish(packet) => (
                        PUBLISH_TYPE,
                        packet.meta.packet_id.map(|packet_id| {
                            Packet::PublishAck(PublishAck {
                                packet_id,
                                reason_code: crate::codec::PublishAckReason::Success,
                                properties: Vec::new(),
                                reason_string: None,
                            })
                        }),
                    ),
                    DecodedFrame::Control(Packet::Subscribe(sub)) => (
                        Packet::Subscribe(sub.clone()).packet_type() >> 4,
                        Some(Packet::SubscribeAck(SubscribeAck {
                            packet_id: sub.packet_id,
                            properties: Vec::new(),
                            reason_string: None,
                            status: sub
                                .topic_filters
                                .iter()
                                .map(|_| SubscribeAckReason::GrantedQos1)
                                .collect(),
                        })),
                    ),
                    DecodedFrame::Control(packet) => (packet.packet_type() >> 4, None),
                };
                received_broker.lock().await.push(kind);
                if let Some(reply) = reply {
                    let is_suback = matches!(reply, Packet::SubscribeAck(_));
                    if framed_write.send(reply).await.is_err() {
                        return;
                    }
                    if is_suback {
                        // An AUTH the client did not ask for: ignored, not answered.
                        let _ = framed_write.send(Packet::Auth(crate::codec::Auth::default())).await;
                    }
                }
            }
            let _ = done_tx.send(());
        })
        .await;

        let (client, connection, mut phase) = connect_test_client(port).await;
        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;

        client
            .subscribe([("t/auth", QoS::AtLeastOnce)])
            .await
            .expect("subscribe");
        client.publish("t/auth", "q0").await.expect("QoS 0 publish");
        // Acknowledged after the broker's AUTH on the wire, so the client has read (and ignored) it.
        client
            .publish("t/auth", "q1")
            .qos(QoS::AtLeastOnce)
            .await
            .expect("QoS 1 publish");
        client.shutdown().await.expect("shutdown");
        let _ = timeout(Duration::from_secs(3), run).await;
        timeout(Duration::from_secs(3), done_rx.recv_async())
            .await
            .expect("broker sees the connection end")
            .expect("broker alive");

        let received = received.lock().await.clone();
        // CONNECT, SUBSCRIBE, PUBLISH (QoS 0), PUBLISH (QoS 1), DISCONNECT.
        assert_eq!(
            received,
            [1, 8, PUBLISH_TYPE, PUBLISH_TYPE, 14],
            "session was recorded in full"
        );
        assert!(
            !received.contains(&AUTH_TYPE),
            "client sent AUTH without an Authentication Method: {received:?}"
        );
    }

    /// Broker that records the Client Identifier of every CONNECT and, for the first
    /// `drops` connections, closes the transport right after CONNACK.
    async fn spawn_client_id_recording_broker(drops: usize) -> (u16, Arc<tokio::sync::Mutex<Vec<(String, bool)>>>) {
        let seen: Arc<tokio::sync::Mutex<Vec<(String, bool)>>> = Arc::default();
        let seen_broker = Arc::clone(&seen);
        let port = crate::connection::test_support::spawn_broker_loop(move |mut framed_read, mut framed_write| {
            let seen = Arc::clone(&seen_broker);
            async move {
                let Some(Ok((DecodedFrame::Control(Packet::Connect(connect)), _))) = framed_read.next().await else {
                    return;
                };
                let index = {
                    let mut seen = seen.lock().await;
                    seen.push((connect.client_id.to_string(), connect.clean_start));
                    seen.len() - 1
                };
                if framed_write.send(Packet::ConnectAck(Box::default())).await.is_err() {
                    return;
                }
                if index >= drops {
                    while framed_read.next().await.is_some() {}
                }
            }
        })
        .await;
        (port, seen)
    }

    async fn wait_for_connects(seen: &tokio::sync::Mutex<Vec<(String, bool)>>, n: usize) -> Vec<(String, bool)> {
        timeout(Duration::from_secs(5), async {
            loop {
                let connects = seen.lock().await.clone();
                if connects.len() >= n {
                    return connects;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("client reconnects")
    }

    /// The Client Identifier names the session ([MQTT-3.1.3-2]): a client that keeps its session
    /// (Clean Start 0) presents the same ID on the first connection and on both reconnects.
    #[tokio::test]
    async fn clean_start_false_reconnects_present_the_same_client_id() {
        let (port, seen) = spawn_client_id_recording_broker(2).await;
        let (tx, _rx) = flume::bounded::<InboundMessage>(4);
        let (client, connection, _phase) = connect(
            resumable_options(port),
            static_test_credentials(),
            ImmediateReconnect,
            tx,
            NoopLifecycle,
        )
        .expect("connect wiring");
        let run = tokio::spawn(connection.run());

        let connects = wait_for_connects(&seen, 3).await;
        assert_eq!(connects.len(), 3, "first connection plus two reconnects");
        for (client_id, clean_start) in &connects {
            assert_eq!(client_id, "resume-test", "every CONNECT carries the session's ClientID");
            assert!(!clean_start, "the session is kept across reconnects");
        }

        client.shutdown().await.expect("shutdown");
        let _ = run.await;
    }

    /// A ClientID the builder generates (no `client_id` set, Clean Start 0) is minted once and
    /// reused by every reconnect, so the generated ID keeps naming the same session
    /// ([MQTT-3.1.3-2]).
    #[tokio::test]
    async fn generated_client_id_is_the_same_across_reconnects() {
        let (port, seen) = spawn_client_id_recording_broker(2).await;
        let client = crate::ClientBuilder::new(format!("mqtt://127.0.0.1:{port}"))
            .expect("url")
            .credentials(static_test_credentials())
            .options(|options| options.clean_start(false).session_expiry(Some(Duration::from_secs(60))))
            .reconnect(ImmediateReconnect)
            .wait_connected(false) // the broker drops the first connections at once
            .discard_inbound()
            .connect()
            .await
            .expect("connect");

        let connects = wait_for_connects(&seen, 3).await;
        assert_eq!(connects.len(), 3, "first connection plus two reconnects");
        let first = &connects[0].0;
        assert!(first.starts_with("glide-"), "generated ClientID, got {first}");
        for (client_id, clean_start) in &connects {
            assert_eq!(client_id, first, "generated ClientID must not change on reconnect");
            assert!(!clean_start, "the session is kept across reconnects");
        }

        client.shutdown().await.expect("shutdown");
    }

    /// `MqttClient::stats()` counters against scripted brokers, asserted to exact values.
    #[cfg(feature = "stats")]
    mod stats {
        use std::sync::atomic::AtomicU64;

        use tokio::io::AsyncWriteExt;
        use tokio_util::codec::Encoder;

        use super::*;
        use crate::ClientStats;
        use crate::codec::{PublishAckReason, PublishDelivery, V5Codec, encode_publish};
        use crate::connection::test_support::{send_inbound_qos1_publish_dup, spawn_mock_broker_keepalive_enforcer};

        /// Polls `client.stats()` until `done` holds; the gauges settle a moment after the
        /// caller's await returns.
        async fn stats_until(client: &MqttClient, done: impl Fn(&ClientStats) -> bool) -> ClientStats {
            timeout(Duration::from_secs(3), async {
                loop {
                    let stats = client.stats();
                    if done(&stats) {
                        return stats;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("stats did not settle: {:?}", client.stats()))
        }

        /// Wires a client with a roomy inbound sink and spawns its driver.
        async fn start(
            options: ConnectOptions,
        ) -> (
            MqttClient,
            flume::Receiver<InboundMessage>,
            tokio::task::JoinHandle<Result<(), ConnectionOutcome>>,
        ) {
            let (tx, rx) = flume::bounded::<InboundMessage>(8);
            let (client, connection, mut phase) =
                connect(options, static_test_credentials(), NoReconnect, tx, NoopLifecycle).expect("wiring");
            let run = tokio::spawn(connection.run());
            await_connected(&mut phase).await;
            (client, rx, run)
        }

        fn options_for(port: u16) -> ConnectOptions {
            ConnectOptions::new(BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url"))
        }

        /// Only PUBLISHes carrying the DUP flag are duplicates; the others are not.
        #[tokio::test]
        async fn inbound_duplicates_counts_only_publishes_with_dup_set() {
            let port = spawn_broker_once(|mut framed_read, mut framed_write| async move {
                if !accept_connect_send_connack(&mut framed_read, &mut framed_write).await {
                    return;
                }
                for (id, dup) in [(1, false), (2, true), (3, false)] {
                    send_inbound_qos1_publish_dup(
                        &mut framed_write,
                        "in/dup",
                        b"x",
                        NonZeroU16::new(id).expect("nz"),
                        PublishProperties::default(),
                        dup,
                    )
                    .await;
                }
                while framed_read.next().await.is_some() {}
            })
            .await;
            let (client, rx, run) = start(options_for(port)).await;

            for _ in 0..3 {
                timeout(Duration::from_secs(2), rx.recv_async())
                    .await
                    .expect("message in time")
                    .expect("sink open");
            }
            let inbound = client.stats().inbound;
            assert_eq!((inbound.publishes_qos1, inbound.duplicates), (3, 1), "{inbound:?}");

            client.shutdown().await.expect("shutdown");
            let _ = run.await;
        }

        /// Resending the unacknowledged QoS 1 publishes of a resumed session counts each one as
        /// resent, and not again as a first transmission.
        #[tokio::test]
        async fn resumed_session_counts_every_resent_publish() {
            let seen: SeenPublishes = Arc::default();
            let port = spawn_resume_broker(Arc::clone(&seen), true).await;
            let (tx, _rx) = flume::bounded::<InboundMessage>(4);
            let (client, connection, mut phase) = connect(
                resumable_options(port),
                static_test_credentials(),
                ImmediateReconnect,
                tx,
                NoopLifecycle,
            )
            .expect("connect wiring");
            let run = tokio::spawn(connection.run());
            await_connected(&mut phase).await;

            let completions: Vec<_> = ["a", "b"]
                .into_iter()
                .map(|payload| {
                    client
                        .try_start_publish(Publish::new("t/resume", payload).qos(QoS::AtLeastOnce))
                        .expect("enqueue")
                })
                .collect();
            for completion in completions {
                timeout(Duration::from_secs(3), completion.wait())
                    .await
                    .expect("resolves after the resend")
                    .expect("acknowledged");
            }

            let stats = stats_until(&client, |stats| stats.outbound.publishes_acked == 2).await;
            let outbound = stats.outbound;
            assert_eq!(
                (
                    outbound.publishes_qos1,
                    outbound.publishes_resent,
                    outbound.publishes_acked
                ),
                (2, 2, 2),
                "{outbound:?}"
            );

            client.shutdown().await.expect("shutdown");
            let _ = run.await;
        }

        /// With the broker's Receive Maximum at 1 and its PUBACK withheld, one publish waits for
        /// its ack and the rest wait for quota; a PUBACK moves one from the second to the first.
        #[tokio::test]
        async fn publishes_beyond_receive_maximum_count_as_awaiting_quota() {
            let (ack_tx, ack_rx) = oneshot::channel::<()>();
            let port = spawn_broker_once(|mut framed_read, mut framed_write| async move {
                let mut ack = ConnectAck::default();
                ack.receive_max = NonZeroU16::new(1).expect("nz");
                if !accept_connect_send_connack_with(&mut framed_read, &mut framed_write, ack).await {
                    return;
                }
                let Some(Ok((DecodedFrame::Publish(first), _))) = framed_read.next().await else {
                    return;
                };
                let _ = ack_rx.await;
                let puback = Packet::PublishAck(PublishAck {
                    packet_id: first.meta.packet_id.expect("QoS 1 packet id"),
                    reason_code: PublishAckReason::Success,
                    properties: Vec::new(),
                    reason_string: None,
                });
                let _ = framed_write.send(puback).await;
                while framed_read.next().await.is_some() {}
            })
            .await;
            let (client, _rx, run) = start(options_for(port)).await;

            let _completions: Vec<_> = (0..3u8)
                .map(|n| {
                    client
                        .try_start_publish(Publish::new("t/quota", vec![n]).qos(QoS::AtLeastOnce))
                        .expect("queued")
                })
                .collect();
            let outbound = stats_until(&client, |stats| stats.outbound.awaiting_quota == 2)
                .await
                .outbound;
            assert_eq!((outbound.awaiting_ack, outbound.awaiting_quota), (1, 2), "{outbound:?}");

            let _ = ack_tx.send(());
            let outbound = stats_until(&client, |stats| stats.outbound.publishes_acked == 1)
                .await
                .outbound;
            assert_eq!((outbound.awaiting_ack, outbound.awaiting_quota), (1, 1), "{outbound:?}");

            client.shutdown().await.expect("shutdown");
            let _ = run.await;
        }

        /// An idle connection pings once per Keep Alive, and the pings are counted.
        #[tokio::test]
        async fn idle_connection_counts_the_pings_it_sends() {
            let pings = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let port =
                spawn_mock_broker_keepalive_enforcer(Duration::from_secs(5), false, true, false, Arc::clone(&pings))
                    .await;
            let (client, _rx, run) = start(options_for(port).keep_alive(Duration::from_secs(1))).await;

            let stats = stats_until(&client, |stats| stats.connection.pings_sent >= 1).await;
            assert!(stats.connection.pings_sent >= 1, "{stats:?}");
            assert!(
                pings.load(std::sync::atomic::Ordering::SeqCst) >= 1,
                "the broker saw a PINGREQ"
            );

            client.shutdown().await.expect("shutdown");
            let _ = run.await;
        }

        /// PUBACK reason No Matching Subscribers (0x10) means the broker took the message, so it
        /// counts as acknowledged; an error reason code counts as rejected.
        #[tokio::test]
        async fn no_matching_subscribers_puback_counts_as_acked() {
            let reasons = [
                PublishAckReason::Success,
                PublishAckReason::NoMatchingSubscribers,
                PublishAckReason::NotAuthorized,
            ];
            let port = spawn_broker_once(move |mut framed_read, mut framed_write| async move {
                if !accept_connect_send_connack(&mut framed_read, &mut framed_write).await {
                    return;
                }
                let mut reasons = reasons.into_iter();
                while let Some(Ok(frame)) = framed_read.next().await {
                    if let (DecodedFrame::Publish(packet), _) = frame {
                        let puback = Packet::PublishAck(PublishAck {
                            packet_id: packet.meta.packet_id.expect("QoS 1 packet id"),
                            reason_code: reasons.next().expect("three publishes"),
                            properties: Vec::new(),
                            reason_string: None,
                        });
                        if framed_write.send(puback).await.is_err() {
                            return;
                        }
                    }
                }
            })
            .await;
            let (client, _rx, run) = start(options_for(port)).await;

            client
                .publish("t/ack", "1")
                .qos(QoS::AtLeastOnce)
                .await
                .expect("success");
            client
                .publish("t/ack", "2")
                .qos(QoS::AtLeastOnce)
                .await
                .expect("no matching subscribers is not an error");
            client
                .publish("t/ack", "3")
                .qos(QoS::AtLeastOnce)
                .await
                .expect_err("not authorized");

            let outbound = stats_until(&client, |stats| stats.outbound.awaiting_ack == 0)
                .await
                .outbound;
            assert_eq!(
                (outbound.publishes_acked, outbound.publishes_rejected),
                (2, 1),
                "{outbound:?}"
            );

            client.shutdown().await.expect("shutdown");
            let _ = run.await;
        }

        /// The byte counters equal the size of the packets that crossed the connection, as the
        /// broker measured them: CONNECT and a PUBLISH out; CONNACK, a PUBACK and a PUBLISH in.
        #[tokio::test]
        async fn byte_counters_match_the_packets_on_the_wire() {
            let read_by_broker = Arc::new(AtomicU64::new(0));
            let written_by_broker = Arc::new(AtomicU64::new(0));
            let (publish_len_tx, publish_len_rx) = oneshot::channel::<u64>();
            let port = spawn_broker_once({
                let read_by_broker = Arc::clone(&read_by_broker);
                let written_by_broker = Arc::clone(&written_by_broker);
                move |mut framed_read, mut framed_write| async move {
                    let count = |total: &AtomicU64, len: u64| {
                        total.fetch_add(len, std::sync::atomic::Ordering::SeqCst);
                    };
                    // The decoder reports the Remaining Length; add the fixed header (1 + 1 byte here).
                    let wire_len = |remaining: u32| {
                        assert!(remaining < 128, "one-byte Remaining Length expected");
                        u64::from(remaining) + 2
                    };
                    let encoded = |packet: Packet| {
                        let mut buf = bytes::BytesMut::new();
                        V5Codec::with_max_packet_size(0)
                            .encode(packet, &mut buf)
                            .expect("encode");
                        buf
                    };
                    let Some(Ok((DecodedFrame::Control(Packet::Connect(_)), len))) = framed_read.next().await else {
                        return;
                    };
                    count(&read_by_broker, wire_len(len));
                    let connack = encoded(Packet::ConnectAck(Box::default()));
                    count(&written_by_broker, connack.len() as u64);
                    if framed_write.get_mut().write_all(&connack).await.is_err() {
                        return;
                    }

                    let Some(Ok((DecodedFrame::Publish(publish), len))) = framed_read.next().await else {
                        return;
                    };
                    count(&read_by_broker, wire_len(len));
                    let _ = publish_len_tx.send(wire_len(len));
                    let puback = encoded(Packet::PublishAck(PublishAck {
                        packet_id: publish.meta.packet_id.expect("QoS 1 packet id"),
                        reason_code: PublishAckReason::Success,
                        properties: Vec::new(),
                        reason_string: None,
                    }));
                    count(&written_by_broker, puback.len() as u64);

                    let mut inbound = bytes::BytesMut::new();
                    let delivery = PublishDelivery {
                        dup: false,
                        retain: false,
                        qos: QoS::AtMostOnce,
                        packet_id: None,
                    };
                    encode_publish(
                        &sample_message("in/t", b"abc"),
                        delivery,
                        crate::codec::DEFAULT_MAX_PACKET_SIZE,
                        &mut inbound,
                    )
                    .expect("encode");
                    // 1 fixed header + 1 length + 2 + 4 topic + 1 properties + 3 payload.
                    assert_eq!(inbound.len(), 12);
                    count(&written_by_broker, inbound.len() as u64);
                    let mut wire = puback;
                    wire.extend_from_slice(&inbound);
                    if framed_write.get_mut().write_all(&wire).await.is_err() {
                        return;
                    }
                    while framed_read.next().await.is_some() {}
                }
            })
            .await;
            let (client, rx, run) = start(options_for(port)).await;

            client
                .publish("t/b", "hello")
                .qos(QoS::AtLeastOnce)
                .await
                .expect("acknowledged");
            timeout(Duration::from_secs(2), rx.recv_async())
                .await
                .expect("inbound publish in time")
                .expect("sink open");

            let publish_len = publish_len_rx.await.expect("publish length");
            // 1 fixed header + 1 length + 2 + 3 topic + 2 packet id + 1 properties + 5 payload.
            assert_eq!(publish_len, 15);
            let stats = client.stats();
            let read = read_by_broker.load(std::sync::atomic::Ordering::SeqCst);
            let written = written_by_broker.load(std::sync::atomic::Ordering::SeqCst);
            assert_eq!(stats.outbound.bytes, read, "{stats:?}");
            assert_eq!(stats.inbound.bytes, written, "{stats:?}");

            client.shutdown().await.expect("shutdown");
            let _ = run.await;
        }
    }
}
