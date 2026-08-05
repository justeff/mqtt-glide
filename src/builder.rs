//! Ergonomic [`ClientBuilder`] over the `connect_with_shared` driver entry.
//!
//! # Publish sink (type-state)
//!
//! Incoming PUBLISH delivery **MUST** be configured before [`ClientBuilder::connect`]:
//! call [`.inbound_sink`](ClientBuilder::inbound_sink) with any [`InboundSink`] (for
//! example `flume::Sender` or `tokio::sync::mpsc::Sender`),
//! [`.on_inbound`](ClientBuilder::on_inbound) for an inline handler on the read task,
//! or [`.discard_inbound`](ClientBuilder::discard_inbound) to drain/drop messages in a
//! background task. These paths are mutually exclusive (compile-time). The unset state
//! does not expose `connect`.
//!
//! # Example
//!
//! ```no_run
//! use mqtt_glide::{ClientBuilder, QoS};
//!
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! let client = ClientBuilder::new("mqtt://127.0.0.1:1883")?
//!     .discard_inbound()
//!     .connect()
//!     .await?;
//! client
//!     .publish_with()
//!     .topic("topic/a")
//!     .payload("hello")
//!     .qos(QoS::AtLeastOnce)
//!     .send()
//!     .await?;
//! client.shutdown().await?;
//! # Ok(())
//! # }
//! ```

use std::num::NonZeroU16;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::timeout;

use crate::client::{
    AnonymousCredentials, CredentialsProvider, MqttClient, NoReconnect, ReconnectPolicy, SharedCredentials,
    connect_with_shared,
};
use crate::codec::{PublishMessage, QoS};
use crate::connection::{LifecycleHook, NoopLifecycle, SessionParams, SessionPhase};
use crate::error::{AppError, ConnectError, ConnectionOutcome, TransportError};
use crate::inbound_sink::{InboundSink, OnInboundSink};
use crate::options::ConnectOptions;
use crate::util::{BrokerUrl, BrokerUrlError, ClientId};

/// Marker: handoff not configured — [`ClientBuilder::connect`] is unavailable.
#[doc(hidden)]
#[derive(Debug)]
pub struct InboundSinkUnset;

/// Marker: application [`InboundSink`] set via [`ClientBuilder::inbound_sink`].
#[doc(hidden)]
pub struct InboundSinkReady<S: InboundSink> {
    sink: S,
}

/// Marker: inbound PUBLISH discarded via [`ClientBuilder::discard_inbound`].
#[doc(hidden)]
#[derive(Debug)]
pub struct InboundSinkDiscard;

struct BuilderConfig {
    options: ConnectOptions,
    credentials: SharedCredentials,
    reconnect: Arc<dyn ReconnectPolicy>,
    lifecycle: Arc<dyn LifecycleHook>,
    wait_connected: bool,
}

/// Fluent builder that applies defaults and connects (spawns the connection driver).
///
/// Defaults (when not overridden):
/// - reconnect: [`NoReconnect`]
/// - lifecycle: [`crate::NoopLifecycle`]
/// - credentials: [`AnonymousCredentials`] (CONNECT without username/password)
/// - `client_id`: [`ClientId::from_prefix`]`("mqtt5")` when `options.client_id` is `None`
/// - `wait_connected`: `true` (await [`SessionPhase::Connected`] after [`Self::connect`])
///
/// Low-level [`crate::connect`] remains available for full control.
///
/// The `connection.run()` [`tokio::task::JoinHandle`] is **detached** after a successful
/// connect (a small supervisor logs join panics). Graceful shutdown still works via
/// [`MqttClient::shutdown`]; the handle is not returned (prefer observing phase /
/// shutdown over joining the driver). On [`Self::connect`] wait failure the builder
/// shuts the client down and joins the driver so a retrying reconnect policy cannot
/// keep running after `Err`.
pub struct ClientBuilder<H> {
    config: BuilderConfig,
    handoff: H,
}

impl ClientBuilder<InboundSinkUnset> {
    /// Parses `url` via [`BrokerUrl`] and starts a builder with default options.
    pub fn new(url: impl AsRef<str>) -> Result<Self, BrokerUrlError> {
        let broker_url = BrokerUrl::parse(url.as_ref())?;
        Ok(Self::from_options(ConnectOptions::new(broker_url)))
    }

    /// Starts a builder from existing [`ConnectOptions`].
    pub fn from_options(options: ConnectOptions) -> Self {
        Self {
            config: BuilderConfig {
                options,
                credentials: SharedCredentials::new(AnonymousCredentials),
                reconnect: Arc::new(NoReconnect),
                lifecycle: Arc::new(NoopLifecycle),
                wait_connected: true,
            },
            handoff: InboundSinkUnset,
        }
    }

    /// Sets the injectable inbound PUBLISH handoff sink ([`InboundSink`]).
    pub fn inbound_sink<S: InboundSink>(self, sink: S) -> ClientBuilder<InboundSinkReady<S>> {
        ClientBuilder {
            config: self.config,
            handoff: InboundSinkReady { sink },
        }
    }

    /// Handles each inbound PUBLISH inline on the connection read task (no app queue).
    ///
    /// Mutually exclusive with [`Self::inbound_sink`] and [`Self::discard_inbound`]
    /// (type-state). Prefer a channel sink for non-trivial work — this path blocks
    /// decode / keepalive while `handler` runs. QoS 1 `PUBACK` is sent only after
    /// `handler` returns.
    ///
    /// Equivalent to [`Self::inbound_sink`]`(`[`OnInboundSink::new`]`(handler))`.
    pub fn on_inbound<F>(self, handler: F) -> ClientBuilder<InboundSinkReady<OnInboundSink<F>>>
    where
        F: Fn(PublishMessage) + Send + Sync + 'static,
    {
        self.inbound_sink(OnInboundSink::new(handler))
    }

    /// Discards inbound PUBLISH messages (spawns a drain task on [`Self::connect`]).
    ///
    /// The discard channel is sized at [`Self::connect`] from the **final**
    /// [`ConnectOptions::receive_maximum`] (after any [`.options`](Self::options)
    /// overrides), so discard does not inflate the inbound stall ceiling beyond the
    /// CONNECT receive window.
    pub fn discard_inbound(self) -> ClientBuilder<InboundSinkDiscard> {
        ClientBuilder {
            config: self.config,
            handoff: InboundSinkDiscard,
        }
    }
}

impl<H> ClientBuilder<H> {
    /// Sets the credentials provider (replaces the default [`AnonymousCredentials`]).
    pub fn credentials(mut self, credentials: impl CredentialsProvider + 'static) -> Self {
        self.config.credentials = SharedCredentials::new(credentials);
        self
    }

    /// Sets the reconnect policy (replaces the default [`NoReconnect`]).
    pub fn reconnect(mut self, reconnect: impl ReconnectPolicy + 'static) -> Self {
        self.config.reconnect = Arc::new(reconnect);
        self
    }

    /// Sets the lifecycle hook (replaces the default [`crate::NoopLifecycle`]).
    pub fn lifecycle(mut self, lifecycle: impl LifecycleHook + 'static) -> Self {
        self.config.lifecycle = Arc::new(lifecycle);
        self
    }

    /// Transforms the current [`ConnectOptions`] (fluent overrides).
    pub fn options(mut self, f: impl FnOnce(ConnectOptions) -> ConnectOptions) -> Self {
        self.config.options = f(self.config.options);
        self
    }

    /// Sets the Will Message for CONNECT (§3.1.2.5).
    ///
    /// Builds [`WillMessage`](crate::WillMessage) internally (QoS 0, retain `false` by default).
    /// Chain [`Self::will_qos`] / [`Self::will_retain`] as needed. See [`ConnectOptions::will`].
    pub fn will(mut self, topic: impl Into<bytestring::ByteString>, payload: impl Into<bytes::Bytes>) -> Self {
        self.config.options = self.config.options.will(topic, payload);
        self
    }

    /// Sets Will QoS (§3.1.2.6). Call after [`Self::will`]; no-op otherwise. Default: QoS 0.
    pub fn will_qos(mut self, qos: QoS) -> Self {
        self.config.options = self.config.options.will_qos(qos);
        self
    }

    /// Sets Will Retain (§3.1.2.7). Call after [`Self::will`]; no-op otherwise. Default: `false`.
    pub fn will_retain(mut self, retain: bool) -> Self {
        self.config.options = self.config.options.will_retain(retain);
        self
    }

    /// Sets Will Delay Interval in **seconds**. Call after [`Self::will`]; no-op otherwise.
    pub fn will_delay_interval(mut self, seconds: u32) -> Self {
        self.config.options = self.config.options.will_delay_interval(seconds);
        self
    }

    /// When `true` (default), [`ClientBuilder::connect`] waits until
    /// [`SessionPhase::Connected`] (or a disconnect / timeout).
    pub fn wait_connected(mut self, wait_connected: bool) -> Self {
        self.config.wait_connected = wait_connected;
        self
    }
}

impl<S: InboundSink> ClientBuilder<InboundSinkReady<S>> {
    /// Wires [`crate::connect`], spawns `connection.run()`, and optionally waits
    /// for [`SessionPhase::Connected`].
    ///
    /// On success the driver [`JoinHandle`](tokio::task::JoinHandle) is detached
    /// under a panic-logging supervisor; use [`MqttClient::shutdown`] to stop the
    /// session. On wait failure this method best-effort shuts the client down and
    /// joins the driver so reconnect cannot continue after `Err`.
    pub async fn connect(self) -> Result<MqttClient, ConnectError> {
        finish_connect(self.config, self.handoff.sink, None).await
    }
}

impl ClientBuilder<InboundSinkDiscard> {
    /// Wires [`crate::connect`], spawns `connection.run()`, and optionally waits
    /// for [`SessionPhase::Connected`].
    ///
    /// On success the driver [`JoinHandle`](tokio::task::JoinHandle) is detached
    /// under a panic-logging supervisor; use [`MqttClient::shutdown`] to stop the
    /// session. On wait failure this method best-effort shuts the client down and
    /// joins the driver so reconnect cannot continue after `Err`.
    pub async fn connect(self) -> Result<MqttClient, ConnectError> {
        let receive_maximum = self.config.options.receive_maximum;
        let (tx, rx) = make_discard_channel(receive_maximum);
        finish_connect(self.config, tx, Some(rx)).await
    }
}

async fn finish_connect<S: InboundSink>(
    config: BuilderConfig,
    sink: S,
    discard_rx: Option<flume::Receiver<PublishMessage>>,
) -> Result<MqttClient, ConnectError> {
    let BuilderConfig {
        mut options,
        credentials,
        reconnect,
        lifecycle,
        wait_connected,
    } = config;

    if options.client_id.is_none() {
        options = options.client_id(ClientId::from_prefix("mqtt5"));
    }

    let connect_timeout = options.connect_timeout;

    if let Some(rx) = discard_rx {
        tokio::spawn(async move { while rx.recv_async().await.is_ok() {} });
    }

    let (client, connection, mut phase) = connect_with_shared(options, credentials, reconnect, sink, lifecycle)?;

    let run_handle = tokio::spawn(connection.run());

    if wait_connected && let Err(err) = wait_until_connected(&mut phase, connect_timeout).await {
        // `finish_session` notifies Disconnected before reconnect delay — wait can
        // fail while a retrying policy would still loop. Stop the driver first.
        let _ = client.shutdown().await;
        let _ = timeout(Duration::from_secs(5), run_handle).await;
        return Err(err);
    }

    tokio::spawn(supervise_driver(run_handle));
    Ok(client)
}

/// Bounded discard handoff capacity = CONNECT `Receive Maximum`.
fn make_discard_channel(
    receive_maximum: NonZeroU16,
) -> (flume::Sender<PublishMessage>, flume::Receiver<PublishMessage>) {
    flume::bounded(usize::from(receive_maximum.get()))
}

/// Awaits the detached driver and logs join panics.
async fn supervise_driver(handle: tokio::task::JoinHandle<Result<(), ConnectionOutcome>>) {
    match handle.await {
        Ok(_) => {}
        Err(join_err) if join_err.is_panic() => {
            tracing::error!(error = %join_err, "mqtt-glide connection driver panicked");
        }
        Err(join_err) => {
            tracing::error!(error = %join_err, "mqtt-glide connection driver join failed");
        }
    }
}

async fn wait_until_connected(
    phase: &mut tokio::sync::watch::Receiver<SessionPhase>,
    deadline: Duration,
) -> Result<(), ConnectError> {
    let wait = phase.wait_for(|p| {
        matches!(
            p,
            SessionPhase::Connected {
                session: SessionParams { from_connack: true, .. },
                ..
            } | SessionPhase::Disconnected(_)
        )
    });

    match timeout(deadline, wait).await {
        Ok(Ok(guard)) => match &*guard {
            SessionPhase::Connected {
                session: SessionParams { from_connack: true, .. },
                ..
            } => Ok(()),
            SessionPhase::Disconnected(outcome) => Err(ConnectError::from(outcome.clone())),
            other => Err(ConnectError::Application(AppError::Internal(format!(
                "unexpected session phase while waiting for Connected: {other:?}"
            )))),
        },
        Ok(Err(_)) => Err(ConnectError::Application(AppError::Internal(
            "session phase watch closed before Connected".into(),
        ))),
        Err(_elapsed) => Err(ConnectError::Transport(TransportError::Connection(
            "timed out waiting for Connected".into(),
        ))),
    }
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use std::num::NonZeroU16;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use bytes::Bytes;
    use bytestring::ByteString;
    use futures_util::{SinkExt, StreamExt};
    use tokio::sync::Mutex;

    use super::*;
    use crate::client::{StaticCredentials, connect};
    use crate::codec::{
        ConnectAck, DecodedFrame, Packet, PublishAck, PublishAckReason, PublishMessage, PublishProperties, QoS,
    };
    use crate::connection::NoopLifecycle;
    use crate::connection::test_support::{
        accept_connect_send_connack_with, await_connected, spawn_broker_loop, spawn_broker_once, spawn_mock_broker,
        static_test_credentials,
    };
    use crate::error::ConnectionOutcome;
    use crate::util::BrokerUrl;

    /// Builder defaults: anonymous creds, NoReconnect, NoopLifecycle, wait_connected, mqtt5 client id.
    #[tokio::test]
    async fn builder_applies_defaults_and_connects() {
        let observed_client_id = Arc::new(Mutex::new(None));
        let observed = Arc::clone(&observed_client_id);
        let port = spawn_broker_once(move |mut framed_read, mut framed_write| async move {
            if let Some(Ok((DecodedFrame::Control(Packet::Connect(connect)), _))) = framed_read.next().await {
                *observed.lock().await = Some(connect.client_id.to_string());
                assert!(connect.username.is_none(), "default credentials must be anonymous");
                assert!(connect.password.is_none(), "default credentials must be anonymous");
                let _ = framed_write.send(Packet::ConnectAck(Box::default())).await;
                while framed_read.next().await.is_some() {}
            }
        })
        .await;

        let client = ClientBuilder::new(format!("mqtt://127.0.0.1:{port}"))
            .expect("url")
            .discard_inbound()
            .connect()
            .await
            .expect("connect");

        let id = observed_client_id.lock().await.clone().expect("CONNECT seen");
        assert!(id.starts_with("mqtt5-"), "default client_id prefix mqtt5, got {id}");

        client.shutdown().await.expect("shutdown");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    /// Minimal publisher: `discard_inbound` + `connect` + `publish_with` QoS 1.
    #[tokio::test]
    async fn builder_discard_inbound_publish_qos_at_least_once_roundtrip() {
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

        let client = ClientBuilder::new(format!("mqtt://127.0.0.1:{port}"))
            .expect("url")
            .credentials(static_test_credentials())
            .options(|o| o.receive_maximum(NonZeroU16::new(8).expect("nz")))
            .discard_inbound()
            .connect()
            .await
            .expect("connect");

        client
            .publish_with()
            .topic("t/discard-pub")
            .payload("minimal-publisher")
            .qos(QoS::AtLeastOnce)
            .send()
            .await
            .expect("publish qos1");

        let frames = observed.lock().await.clone();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].0, QoS::AtLeastOnce);
        assert!(frames[0].1.is_some());
        assert_eq!(&*frames[0].2, b"minimal-publisher");

        client.shutdown().await.expect("shutdown");
    }

    /// Explicit handoff + `publish_with` QoS 1 roundtrip through the builder.
    #[tokio::test]
    async fn builder_publish_with_qos_at_least_once_roundtrip() {
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

        let (tx, _rx) = flume::bounded::<PublishMessage>(4);
        let client = ClientBuilder::new(format!("mqtt://127.0.0.1:{port}"))
            .expect("url")
            .credentials(static_test_credentials())
            .options(|o| o.receive_maximum(NonZeroU16::new(8).expect("nz")))
            .inbound_sink(tx)
            .connect()
            .await
            .expect("connect");

        client
            .publish_with()
            .topic("t/builder")
            .payload("need-ack")
            .qos(QoS::AtLeastOnce)
            .send()
            .await
            .expect("publish qos1");

        client
            .publish_with()
            .topic("t/builder-props")
            .payload("with-props")
            .qos(QoS::AtLeastOnce)
            .user_property("k", "v")
            .send()
            .await
            .expect("publish with props");

        let frames = observed.lock().await.clone();
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].0, QoS::AtLeastOnce);
        assert!(frames[0].1.is_some());
        assert_eq!(&*frames[0].2, b"need-ack");
        assert_eq!(&*frames[1].2, b"with-props");

        client.shutdown().await.expect("shutdown");
    }

    /// `from_options` + `wait_connected(false)` returns without waiting for CONNACK.
    #[tokio::test]
    async fn builder_from_options_without_wait_connected() {
        let port = spawn_mock_broker().await;
        let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("url");
        let options = ConnectOptions::new(url).client_id(ClientId::from_prefix("custom"));

        let client = ClientBuilder::from_options(options)
            .wait_connected(false)
            .discard_inbound()
            .connect()
            .await
            .expect("connect without wait");

        // Handle is usable; may or may not be Connected yet — just shutdown cleanly.
        client.shutdown().await.expect("shutdown");
    }

    /// Low-level `connect` still works alongside the builder (sanity).
    #[tokio::test]
    async fn low_level_connect_still_available() {
        let port = spawn_mock_broker().await;
        let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("url");
        let options = ConnectOptions::new(url).receive_maximum(NonZeroU16::new(8).expect("nz"));
        let (tx, _rx) = flume::bounded::<PublishMessage>(4);
        let (client, connection, mut phase) = connect(
            options,
            StaticCredentials::new("user", "pass"),
            NoReconnect,
            tx,
            NoopLifecycle,
        )
        .expect("connect");
        let run = tokio::spawn(connection.run());
        await_connected(&mut phase).await;
        client
            .publish_with()
            .topic("t/q0")
            .payload("fire")
            .send()
            .await
            .expect("publish qos0 default");
        client.shutdown().await.expect("shutdown");
        let _ = run.await;
    }

    /// Tokio `mpsc` is a valid [`crate::InboundSink`] for builder connect.
    #[tokio::test]
    async fn builder_accepts_tokio_mpsc_handoff() {
        let port = spawn_mock_broker().await;
        let (tx, _rx) = tokio::sync::mpsc::channel::<PublishMessage>(4);
        let client = ClientBuilder::new(format!("mqtt://127.0.0.1:{port}"))
            .expect("url")
            .credentials(static_test_credentials())
            .inbound_sink(tx)
            .connect()
            .await
            .expect("connect with mpsc handoff");
        client.shutdown().await.expect("shutdown");
    }

    /// `on_inbound` delivers inbound QoS 1 and PUBACKs only after the handler returns.
    #[tokio::test]
    async fn builder_on_inbound_inbound_qos1_invokes_handler_then_puback() {
        let puback_ids = Arc::new(Mutex::new(Vec::<NonZeroU16>::new()));
        let pubacks = Arc::clone(&puback_ids);
        let port = spawn_broker_once(move |mut framed_read, mut framed_write| async move {
            if !accept_connect_send_connack_with(&mut framed_read, &mut framed_write, ConnectAck::default()).await {
                return;
            }
            crate::connection::test_support::send_inbound_qos1_publish(
                &mut framed_write,
                "in/on-publish",
                b"inline-payload",
                NonZeroU16::new(9).expect("nz"),
                PublishProperties::default(),
            )
            .await;
            loop {
                match framed_read.next().await {
                    Some(Ok((DecodedFrame::Control(Packet::PublishAck(ack)), _))) => {
                        pubacks.lock().await.push(ack.packet_id);
                    }
                    Some(Ok(_)) => {}
                    Some(Err(_)) | None => break,
                }
            }
        })
        .await;

        let seen = Arc::new(AtomicUsize::new(0));
        let client = ClientBuilder::new(format!("mqtt://127.0.0.1:{port}"))
            .expect("url")
            .credentials(static_test_credentials())
            .on_inbound({
                let seen = Arc::clone(&seen);
                move |msg: PublishMessage| {
                    assert_eq!(&*msg.payload, b"inline-payload");
                    seen.fetch_add(1, Ordering::SeqCst);
                }
            })
            .connect()
            .await
            .expect("connect");

        let acked = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Some(id) = puback_ids.lock().await.first().copied() {
                    break id;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("PUBACK after on_inbound");
        assert_eq!(acked, NonZeroU16::new(9).expect("nz"));
        assert_eq!(seen.load(Ordering::SeqCst), 1);

        client.shutdown().await.expect("shutdown");
    }

    /// Discard channel capacity follows final `receive_maximum` (not a hardcoded default).
    #[test]
    fn discard_channel_capacity_matches_receive_maximum() {
        let receive_maximum = NonZeroU16::new(7).expect("nz");
        let (tx, rx) = make_discard_channel(receive_maximum);
        let stub = || PublishMessage {
            topic: ByteString::from_static("t"),
            payload: Bytes::new(),
            properties: PublishProperties::default(),
        };
        for _ in 0..7 {
            assert!(tx.try_send(stub()).is_ok());
        }
        assert!(tx.try_send(stub()).is_err(), "8th send must fail on capacity 7");
        drop(rx);
    }

    /// Retrying reconnect + wait failure must shut the driver down (no orphan reconnect loop).
    #[tokio::test]
    async fn connect_wait_failure_shuts_down_retrying_driver() {
        let connect_count = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&connect_count);
        let port = spawn_broker_loop(move |mut framed_read, framed_write| {
            let count = Arc::clone(&count);
            async move {
                if let Some(Ok((DecodedFrame::Control(Packet::Connect(_)), _))) = framed_read.next().await {
                    count.fetch_add(1, Ordering::SeqCst);
                    // Drop without CONNACK → transport/protocol failure + reconnect.
                    drop(framed_write);
                }
            }
        })
        .await;

        /// Retries forever on transport/protocol (would orphan without shutdown-on-err).
        struct AlwaysReconnect;

        impl ReconnectPolicy for AlwaysReconnect {
            fn delay_after(&self, outcome: &ConnectionOutcome) -> Option<Duration> {
                match outcome {
                    ConnectionOutcome::Shutdown(_) | ConnectionOutcome::Application(_) => None,
                    _ => Some(Duration::from_millis(20)),
                }
            }
        }

        let result = ClientBuilder::new(format!("mqtt://127.0.0.1:{port}"))
            .expect("url")
            .reconnect(AlwaysReconnect)
            .options(|o| o.connect_timeout(Duration::from_millis(500)))
            .discard_inbound()
            .connect()
            .await;

        assert!(result.is_err(), "connect must Err when broker never CONNACKs");

        let after_err = connect_count.load(Ordering::SeqCst);
        assert!(after_err >= 1, "at least one CONNECT before connect Err");
        tokio::time::sleep(Duration::from_millis(250)).await;
        let later = connect_count.load(Ordering::SeqCst);
        assert_eq!(
            after_err, later,
            "driver must not keep reconnecting after connect returns Err (got {after_err} then {later})"
        );
    }
}
