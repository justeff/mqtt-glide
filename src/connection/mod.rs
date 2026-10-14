//! Connection coordinator: `run()`, state machine, channels, task supervision.

mod channels;
mod handshake;
mod io_batch;
mod lifecycle;
pub(crate) mod packet_id;
pub(crate) mod read;
pub(crate) mod session_policy;
mod state;
pub(crate) mod subscriptions;
pub(crate) mod topic_alias;
mod transport;
#[cfg(feature = "websocket")]
mod websocket;
pub(crate) mod write;

use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use futures_util::SinkExt;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_util::codec::{FramedRead, FramedWrite};

use tokio_util::sync::CancellationToken;

use self::write::WriteOutcome;
use crate::client::{ReconnectPolicy, SharedCredentials};
use crate::codec::{Disconnect, DisconnectReasonCode, Packet, V5Codec};
use crate::error::{AppError, AuthError, ConnectionOutcome, ProtocolError, ShutdownError, TransportError};
use crate::inbound_sink::InboundSink;
use crate::options::ConnectOptions;

pub use crate::options::COMMAND_CHANNEL_CAPACITY;
pub use channels::{CONTROL_CHANNEL_CAPACITY, control_channel_capacity};
pub(crate) use channels::{
    ClientCommand, CommandReceiver, CommandSender, ControlMessage, ControlReceiver, PublishReply, SubscribeReply,
    UnsubscribeReply,
};
pub use handshake::SessionParams;
pub(crate) use handshake::validate_will_message;
pub(crate) use handshake::{TlsMode, resolve_tls_mode};
pub(crate) use io_batch::effective_io_batch;
pub use lifecycle::{LifecycleHook, NoopLifecycle, SessionPhase};
pub(crate) use session_policy::{inbound_publish_policy, validate_publish_against_broker_limits};
pub(crate) use state::{ConnectionState, Generation};
pub(crate) use subscriptions::SubscriptionRegistry;

use self::read::ReadOutcome;

/// Per-connection channel bundle created by [`crate::connect`].
pub(crate) struct ConnectionChannels<H: InboundSink> {
    pub control_tx: channels::ControlSender,
    pub control_rx: ControlReceiver,
    /// Shared with [`crate::MqttClient`]; replaced on each reconnect session (lock-free load).
    pub command_tx: Arc<ArcSwap<CommandSender>>,
    pub command_rx: CommandReceiver,
    pub handoff: H,
    /// Extra sink handle kept for the [`Connection`] lifetime so the injectable handoff
    /// never closes when a read-task sender is dropped/respawned.
    /// Single handoff channel; the anchor sender is not the application consumer.
    #[allow(dead_code)] // held for channel lifetime, not sent on
    pub(crate) handoff_anchor: H,
    /// Latest session phase for application readiness (source of truth).
    pub phase_tx: watch::Sender<SessionPhase>,
    /// Optional sync side effects for lifecycle transitions.
    pub lifecycle: Arc<dyn LifecycleHook>,
    /// Shared with [`crate::MqttClient::is_connected`]; true between Connected and Disconnected.
    pub connected: Arc<std::sync::atomic::AtomicBool>,
    /// Test/supervision hooks: clone senders before [`Connection::run`].
    #[cfg(test)]
    #[allow(dead_code)]
    pub read_stub_exit_tx: mpsc::Sender<()>,
    #[cfg(test)]
    pub read_stub_exit_rx: mpsc::Receiver<()>,
    #[cfg(test)]
    #[allow(dead_code)]
    pub write_stub_exit_tx: mpsc::Sender<()>,
    #[cfg(test)]
    pub write_stub_exit_rx: mpsc::Receiver<()>,
}

impl<H: InboundSink> std::fmt::Debug for ConnectionChannels<H> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectionChannels")
            .field("handoff", &std::any::type_name::<H>())
            .finish_non_exhaustive()
    }
}

impl<H: InboundSink> ConnectionChannels<H> {
    /// Recreates per-session control/command channels after a connection loss.
    ///
    /// Updates the shared command sender so [`crate::MqttClient`] keeps working across reconnects.
    /// Under `cfg(test)`, stub-exit channels are also replenished so supervision hooks that clone
    /// senders before [`Connection::run`] remain valid across reconnect sessions.
    pub(crate) fn replenish_session_channels(
        &mut self,
        command_channel_capacity: usize,
        control_channel_capacity: usize,
    ) {
        let (control_tx, control_rx) = mpsc::channel(control_channel_capacity);
        let (command_tx, command_rx) = mpsc::channel(command_channel_capacity);
        self.control_tx = control_tx;
        self.control_rx = control_rx;
        self.command_tx.store(Arc::new(command_tx));
        self.command_rx = command_rx;

        #[cfg(test)]
        {
            let (read_stub_exit_tx, read_stub_exit_rx) = mpsc::channel(1);
            self.read_stub_exit_tx = read_stub_exit_tx;
            self.read_stub_exit_rx = read_stub_exit_rx;
            let (write_stub_exit_tx, write_stub_exit_rx) = mpsc::channel(1);
            self.write_stub_exit_tx = write_stub_exit_tx;
            self.write_stub_exit_rx = write_stub_exit_rx;
        }
    }
}

/// I/O driver: connect, read/write tasks, reconnect until shutdown.
pub struct Connection<H: InboundSink = flume::Sender<crate::codec::InboundMessage>> {
    options: ConnectOptions,
    credentials: SharedCredentials,
    reconnect: Arc<dyn ReconnectPolicy>,
    channels: ConnectionChannels<H>,
    cancel: CancellationToken,
    /// Priority graceful-shutdown notify shared with [`crate::MqttClient`].
    shutdown_notify: CancellationToken,
    generation: Generation,
    state: ConnectionState,
    /// Publish vs subscribe/unsubscribe Packet-ID pots.
    ///
    /// Moved into the write task when [`Connection::run`] starts.
    inflight: packet_id::InflightPots,
    /// Topic filters to restore after Clean Start reconnect.
    subscription_registry: std::sync::Arc<std::sync::RwLock<SubscriptionRegistry>>,
    /// Whether this process has established a resumeable MQTT session (`clean_start = false`
    /// past a successful CONNACK). Used for [MQTT-3.2.2-4]; independent of subscription count.
    session_affinity: subscriptions::SessionAffinity,
    /// What one connection still owes the application, handed to the next connection
    /// (never-sent publishes always; unacknowledged QoS 1 when the session is resumable).
    session_carry: write::SessionCarry,
}

impl<H: InboundSink> std::fmt::Debug for Connection<H> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Connection")
            .field("generation", &self.generation)
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

impl<H: InboundSink> Connection<H> {
    /// Builds a connection driver with per-connection channels (no global state).
    pub(crate) fn new(
        options: ConnectOptions,
        credentials: SharedCredentials,
        reconnect: Arc<dyn ReconnectPolicy>,
        channels: ConnectionChannels<H>,
        cancel: CancellationToken,
        shutdown_notify: CancellationToken,
    ) -> Self {
        let inflight = packet_id::InflightPots::new(options.receive_maximum);
        let resumable = !options.clean_start && handshake::session_expiry_secs(options.session_expiry) > 0;
        let session_carry = Arc::new(std::sync::Mutex::new(write::Carry::new(resumable)));
        Self {
            options,
            credentials,
            reconnect,
            channels,
            cancel,
            shutdown_notify,
            generation: Generation(0),
            state: ConnectionState::Stopped,
            inflight,
            subscription_registry: std::sync::Arc::new(std::sync::RwLock::new(SubscriptionRegistry::default())),
            session_affinity: subscriptions::SessionAffinity::None,
            session_carry,
        }
    }

    /// Fails everything still carried (driver stopping).
    fn fail_carried(&self) {
        let (unacked, unsent) = self
            .session_carry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take_all();
        for entry in unacked {
            let _ = entry.reply.send(Err(AppError::ConnectionLost));
        }
        for publish in unsent {
            let _ = publish.reply.send(Err(AppError::ConnectionLost));
        }
    }

    /// Fails carried *unacknowledged* publishes: the broker has no session to resend into.
    fn fail_carried_unacked(&self) {
        let unacked = std::mem::take(
            &mut self
                .session_carry
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .unacked,
        );
        for entry in unacked {
            let _ = entry.reply.send(Err(AppError::ConnectionLost));
        }
    }

    /// Returns a clone of the read-stub exit signal sender (test/supervision hook).
    #[cfg(test)]
    pub(crate) fn read_stub_exit_sender(&self) -> mpsc::Sender<()> {
        self.channels.read_stub_exit_tx.clone()
    }

    /// Returns a clone of the write-stub exit signal sender (test/supervision hook).
    #[cfg(test)]
    pub(crate) fn write_stub_exit_sender(&self) -> mpsc::Sender<()> {
        self.channels.write_stub_exit_tx.clone()
    }

    /// Returns a clone of the control-path sender (test hook for dual-task isolation tests).
    #[cfg(test)]
    pub(crate) fn control_sender(&self) -> channels::ControlSender {
        self.channels.control_tx.clone()
    }

    /// Publishes `Connected` on the phase watch and invokes the lifecycle hook (sync).
    fn notify_connected(&self, generation: u64, session: SessionParams) {
        let phase = SessionPhase::Connected { generation, session };
        self.channels
            .connected
            .store(true, std::sync::atomic::Ordering::Release);
        self.channels.phase_tx.send_replace(phase.clone());
        self.channels.lifecycle.on_lifecycle(&phase);
    }

    /// Publishes `Disconnected` on the phase watch and invokes the lifecycle hook (sync).
    fn notify_disconnected(&self, outcome: ConnectionOutcome) {
        let phase = SessionPhase::Disconnected(outcome);
        self.channels
            .connected
            .store(false, std::sync::atomic::Ordering::Release);
        self.channels.phase_tx.send_replace(phase.clone());
        self.channels.lifecycle.on_lifecycle(&phase);
    }

    /// Runs the connection driver until shutdown or a fatal outcome.
    ///
    /// Spawn on a Tokio runtime (for example `tokio::spawn(connection.run())`) and
    /// **await** the returned future — it is not fire-and-forget.
    ///
    /// Reconnects on transport/protocol loss per [`ReconnectPolicy`]; auth and shutdown stop.
    pub async fn run(mut self) -> Result<(), ConnectionOutcome> {
        loop {
            if self.cancel.is_cancelled() {
                self.state = ConnectionState::Stopped;
                self.fail_carried();
                return Err(ConnectionOutcome::Shutdown(ShutdownError::Cancelled));
            }

            // First session uses channels from `connect()`; reconnect sessions already
            // replenished command/control in [`Self::backoff_then_retry`] so the shared
            // sender stays live across the reconnect/backoff gap.
            match self.run_one_session().await {
                SessionDisposition::Retry => continue,
                SessionDisposition::Stop(result) => {
                    self.fail_carried();
                    return result;
                }
            }
        }
    }

    /// One TCP connect + supervised read/write session.
    async fn run_one_session(&mut self) -> SessionDisposition {
        self.state = ConnectionState::Connecting;
        let deadline = Instant::now() + self.options.connect_timeout;

        // Dial under `select!` with borrowed `options` (no per-attempt `ConnectOptions` clone);
        // the borrow ends before `&mut self` resumes in `session_after_dial`.
        let websocket = self.options.broker_url.websocket_path().is_some();
        match (resolve_tls_mode(&self.options), websocket) {
            (TlsMode::PlainTcp, false) => {
                let dialed = dial_or_cancel(&self.cancel, transport::dial_tcp(&self.options, deadline)).await;
                self.session_after_dial(dialed, deadline).await
            }
            (TlsMode::RustlsSystemRoots, false) => {
                let dialed = dial_or_cancel(&self.cancel, transport::dial_tls(&self.options, deadline)).await;
                self.session_after_dial(dialed, deadline).await
            }
            #[cfg(feature = "websocket")]
            (TlsMode::PlainTcp, true) => {
                let dialed = dial_or_cancel(&self.cancel, transport::dial_ws(&self.options, deadline)).await;
                self.session_after_dial(dialed, deadline).await
            }
            #[cfg(feature = "websocket")]
            (TlsMode::RustlsSystemRoots, true) => {
                let dialed = dial_or_cancel(&self.cancel, transport::dial_wss(&self.options, deadline)).await;
                self.session_after_dial(dialed, deadline).await
            }
            // `BrokerUrl::parse` rejects ws:// / wss:// without the feature.
            #[cfg(not(feature = "websocket"))]
            (_, true) => unreachable!("WebSocket URL without the `websocket` feature"),
        }
    }

    /// Continues after a dial attempt: handshake + session, or the dial's failure / cancel.
    async fn session_after_dial<R, W>(
        &mut self,
        dialed: Option<Result<(R, W), ConnectionOutcome>>,
        deadline: Instant,
    ) -> SessionDisposition
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        match dialed {
            Some(Ok((read_half, write_half))) => {
                self.run_one_session_from_halves(read_half, write_half, deadline).await
            }
            Some(Err(outcome)) => self.finish_session(outcome, false).await,
            None => {
                self.finish_session(ConnectionOutcome::Shutdown(ShutdownError::Cancelled), false)
                    .await
            }
        }
    }

    /// Runs CONNECT/CONNACK on dialed halves, then supervises the session.
    async fn run_one_session_from_halves<R, W>(
        &mut self,
        read_half: R,
        write_half: W,
        deadline: Instant,
    ) -> SessionDisposition
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let establish =
            transport::establish_session_on(read_half, write_half, &self.options, &self.credentials, deadline);

        let (framed_read, framed_write, session) = tokio::select! {
            result = establish => {
                match result {
                    Ok(established) => established,
                    Err(outcome) => return self.finish_session(outcome, false).await,
                }
            }
            () = self.cancel.cancelled() => {
                return self
                    .finish_session(ConnectionOutcome::Shutdown(ShutdownError::Cancelled), false)
                    .await;
            }
        };

        self.run_supervised_session(framed_read, framed_write, session).await
    }

    /// Post-handshake session: resubscribe, spawn read/write tasks, supervise until exit.
    async fn run_supervised_session<R, W>(
        &mut self,
        mut framed_read: FramedRead<R, V5Codec>,
        mut framed_write: FramedWrite<W, V5Codec>,
        session: SessionParams,
    ) -> SessionDisposition
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        self.generation = self.generation.next();
        let keep_alive = session.keep_alive;
        let subscription_identifiers_available = session.subscription_identifiers_available;
        // Topic Aliases are per network connection ([MQTT-3.3.2-7]): fresh maps every time.
        let mut inbound_topic_aliases = topic_alias::InboundTopicAliases::new(self.options.topic_alias_maximum);
        let outbound_topic_aliases = self
            .options
            .outbound_topic_aliases
            .min(session.server_topic_alias_maximum);
        let max_qos = session.max_qos;
        let retain_available = session.retain_available;
        self.inflight.set_publish_capacity(session.server_receive_maximum);

        let pending = match subscriptions::snapshot_for_resubscribe(&self.subscription_registry) {
            Ok(filters) => filters,
            Err(outcome) => return self.finish_session(outcome, false).await,
        };

        // Session Present must match Clean Start / local session affinity. Reconnecting into
        // the same claim would loop, so this stops (Application → no retry). Prefer a
        // Protocol Error DISCONNECT before dropping TCP when the write half is still open.
        if !subscriptions::session_resume_consistent(
            self.options.clean_start,
            session.session_present,
            self.session_affinity,
        ) {
            let message = if self.options.clean_start {
                "broker set Session Present = 1 after Clean Start = 1 ([MQTT-3.2.2-2])"
            } else {
                "broker reports Session Present but the client holds no session state ([MQTT-3.2.2-4])"
            };
            let _ = framed_write
                .send(Packet::Disconnect(Disconnect::new(DisconnectReasonCode::ProtocolError)))
                .await;
            return self
                .finish_session(
                    ConnectionOutcome::Application(AppError::SessionPresentInconsistent(message.into())),
                    false,
                )
                .await;
        }
        // Affinity follows Clean Start: a clean session clears it; a resume path marks us as
        // holding local session state even when the subscription registry is empty.
        self.session_affinity = if self.options.clean_start {
            subscriptions::SessionAffinity::None
        } else {
            subscriptions::SessionAffinity::Held
        };

        // No session on the broker: the client MUST discard its own state too
        // ([MQTT-3.2.2-5]), so unacknowledged publishes are not resent. Never-sent ones still go.
        if !session.session_present {
            self.fail_carried_unacked();
        }

        // Clean Start discards broker-side state; a broker answering `session_present = false`
        // to a resume attempt proves it holds none — rebuild the subscriptions in both cases.
        if subscriptions::needs_resubscribe(self.options.clean_start, session.session_present)
            && !pending.is_empty()
            && let Err(outcome) = subscriptions::resubscribe_pending(
                &mut framed_read,
                &mut framed_write,
                &mut self.inflight,
                pending,
                self.options.ack_timeout,
                &self.cancel,
                subscriptions::ResubscribeInbound {
                    handoff: &self.channels.handoff,
                    topic_aliases: &mut inbound_topic_aliases,
                    subscription_identifiers_available,
                },
            )
            .await
        {
            return self.finish_session(outcome, false).await;
        }

        // The session is not a child of the driver cancel: `MqttClient::shutdown` cancels that
        // too, but an active session must stay up to drain. The write task sees
        // `shutdown_notify`, drains (bounded by the graceful budget) and only then stops the
        // read task via `read_stop`; the supervisor cancels the rest.
        let session_cancel = CancellationToken::new();
        let read_cancel = session_cancel.child_token();
        let write_cancel = session_cancel.child_token();
        let read_stop = CancellationToken::new();

        let control_tx = self.channels.control_tx.clone();
        let handoff = self.channels.handoff.clone();
        let generation = self.generation;

        #[cfg(test)]
        let read_stub_exit_rx = std::mem::replace(&mut self.channels.read_stub_exit_rx, mpsc::channel(1).1);
        #[cfg(test)]
        let write_stub_exit_rx = std::mem::replace(&mut self.channels.write_stub_exit_rx, mpsc::channel(1).1);
        let control_rx = std::mem::replace(
            &mut self.channels.control_rx,
            mpsc::channel(control_channel_capacity(self.options.receive_maximum)).1,
        );
        let command_rx = std::mem::replace(
            &mut self.channels.command_rx,
            mpsc::channel(self.options.command_channel_capacity.get()).1,
        );
        let inflight = std::mem::replace(
            &mut self.inflight,
            packet_id::InflightPots::new(self.options.receive_maximum),
        );

        // Connected is published on the watch (never dropped for readiness) before
        // read/write spawn so session start cannot stall on a consumer.
        self.notify_connected(self.generation.0, session);
        if self.cancel.is_cancelled() {
            return self
                .finish_session(ConnectionOutcome::Shutdown(ShutdownError::Cancelled), false)
                .await;
        }
        self.state = ConnectionState::Connected;

        // Read-join before DISCONNECT: write waits for this oneshot (fired when read drops).
        let (read_done_tx, read_done_rx) = tokio::sync::oneshot::channel();
        // Shared with write-task keepalive watchdog: stall skip + any-inbound liveness.
        let handoff_stalled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let inbound_activity = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let read_handle = tokio::spawn(read::run(
            read::ReadSession {
                cancel: read_cancel.clone(),
                stop: read_stop.clone(),
                generation,
                receive_maximum: self.options.receive_maximum,
                handoff_stalled: Arc::clone(&handoff_stalled),
                inbound_activity: Arc::clone(&inbound_activity),
                topic_aliases: inbound_topic_aliases,
            },
            #[cfg(test)]
            read_stub_exit_rx,
            framed_read,
            handoff,
            control_tx,
            read::ReadDoneSignal::new(read_done_tx),
        ));

        let write_handle = tokio::spawn(write::run(
            control_rx,
            command_rx,
            #[cfg(test)]
            write_stub_exit_rx,
            framed_write,
            write::WriteSession {
                generation,
                keep_alive,
                subscription_registry: Arc::clone(&self.subscription_registry),
                cancel: write_cancel.clone(),
                shutdown_notify: self.shutdown_notify.clone(),
                graceful_disconnect_timeout: self.options.graceful_disconnect_timeout,
                max_qos,
                retain_available,
                receive_maximum: self.options.receive_maximum,
                handoff_stalled,
                inbound_activity,
                bench_inbound_publish_ack_counter: self.options.bench_inbound_publish_ack_counter.clone(),
                session_carry: Some(Arc::clone(&self.session_carry)),
                read_stop,
                outbound_topic_aliases,
                subscription_identifiers_available,
            },
            inflight,
            read_done_rx,
        ));

        let outcome = Self::supervise_tasks(read_handle, write_handle, read_cancel, write_cancel).await;
        // Drop session token so any lingering child work observes cancel.
        drop(session_cancel);
        self.finish_session(outcome, true).await
    }

    /// Emits `Disconnected`, applies reconnect policy, and decides whether to retry.
    async fn finish_session(&mut self, outcome: ConnectionOutcome, from_active_session: bool) -> SessionDisposition {
        self.state = ConnectionState::Draining;
        self.notify_disconnected(outcome.clone());

        let invalidate_credentials =
            matches!(&outcome, ConnectionOutcome::Auth(AuthError::NotAuthorized)) && from_active_session;

        match &outcome {
            ConnectionOutcome::Shutdown(_) | ConnectionOutcome::Application(_) => {
                self.state = ConnectionState::Stopped;
                SessionDisposition::Stop(Self::map_final_result(outcome))
            }
            ConnectionOutcome::Auth(_) => match self.reconnect.delay_after(&outcome) {
                None => {
                    if invalidate_credentials {
                        self.credentials.invalidate().await;
                    }
                    self.state = ConnectionState::Stopped;
                    SessionDisposition::Stop(Err(outcome))
                }
                Some(delay) => {
                    let disposition = self.backoff_then_retry(delay).await;
                    // Invalidate after the channels are replenished (in backoff_then_retry):
                    // app commands must not see a closed command channel while the
                    // credentials provider is refreshing. Runs on every disposition —
                    // including a backoff cancelled by shutdown — so a rejected credential
                    // is never kept cached.
                    if invalidate_credentials {
                        self.credentials.invalidate().await;
                    }
                    disposition
                }
            },
            ConnectionOutcome::Transport(_) | ConnectionOutcome::Protocol(_) => {
                match self.reconnect.delay_after(&outcome) {
                    None => {
                        self.state = ConnectionState::Stopped;
                        SessionDisposition::Stop(Err(outcome))
                    }
                    Some(delay) => self.backoff_then_retry(delay).await,
                }
            }
        }
    }

    async fn backoff_then_retry(&mut self, delay: Duration) -> SessionDisposition {
        // Replenish before sleeping so `MqttClient`'s shared `command_tx` stays open while
        // the previous write task (and its command receiver) is already gone.
        self.channels.replenish_session_channels(
            self.options.command_channel_capacity.get(),
            control_channel_capacity(self.options.receive_maximum),
        );
        self.inflight = packet_id::InflightPots::new(self.options.receive_maximum);

        self.state = ConnectionState::Backoff;
        if self.cancel.is_cancelled() {
            self.state = ConnectionState::Stopped;
            return SessionDisposition::Stop(Err(ConnectionOutcome::Shutdown(ShutdownError::Cancelled)));
        }
        if delay.is_zero() {
            return SessionDisposition::Retry;
        }

        tokio::select! {
            () = tokio::time::sleep(delay) => SessionDisposition::Retry,
            () = self.cancel.cancelled() => {
                self.state = ConnectionState::Stopped;
                SessionDisposition::Stop(Err(ConnectionOutcome::Shutdown(ShutdownError::Cancelled)))
            }
        }
    }

    fn map_final_result(outcome: ConnectionOutcome) -> Result<(), ConnectionOutcome> {
        match &outcome {
            ConnectionOutcome::Shutdown(ShutdownError::Requested) => Err(outcome),
            ConnectionOutcome::Application(AppError::CommandChannelClosed) => Ok(()),
            ConnectionOutcome::Shutdown(ShutdownError::Cancelled) => Ok(()),
            _ => Err(outcome),
        }
    }

    async fn supervise_tasks(
        read_handle: JoinHandle<ReadOutcome>,
        write_handle: JoinHandle<WriteOutcome>,
        read_cancel: CancellationToken,
        write_cancel: CancellationToken,
    ) -> ConnectionOutcome {
        tokio::pin!(read_handle);
        tokio::pin!(write_handle);

        enum FirstResult {
            Read(Result<ReadOutcome, tokio::task::JoinError>),
            Write(Result<WriteOutcome, tokio::task::JoinError>),
        }

        let first = tokio::select! {
            read_result = read_handle.as_mut() => FirstResult::Read(read_result),
            write_result = write_handle.as_mut() => FirstResult::Write(write_result),
        };

        match first {
            FirstResult::Read(read_result) => {
                write_cancel.cancel();
                let write_result = write_handle.as_mut().await;
                // Prefer write's graceful DISCONNECT over read cut-off Cancelled.
                if matches!(&write_result, Ok(WriteOutcome::Shutdown)) {
                    return Self::map_write_outcome(write_result);
                }
                Self::map_read_outcome(read_result)
            }
            FirstResult::Write(write_result) => {
                read_cancel.cancel();
                let _ = read_handle.as_mut().await;
                Self::map_write_outcome(write_result)
            }
        }
    }

    fn map_read_outcome(result: Result<ReadOutcome, tokio::task::JoinError>) -> ConnectionOutcome {
        match result {
            Ok(ReadOutcome::Cancelled) => ConnectionOutcome::Shutdown(ShutdownError::Cancelled),
            #[cfg(test)]
            Ok(ReadOutcome::StubExit) => {
                ConnectionOutcome::Application(AppError::Internal("test stub read exit".into()))
            }
            Ok(ReadOutcome::InboundSinkClosed) => ConnectionOutcome::Application(AppError::InboundSinkClosed),
            Ok(ReadOutcome::PeerClosed) => {
                ConnectionOutcome::Transport(TransportError::Connection("peer closed".into()))
            }
            Ok(ReadOutcome::BrokerDisconnect(disconnect)) => {
                let detail = broker_detail(
                    disconnect.reason_string.as_deref(),
                    disconnect.server_reference.as_deref(),
                );
                match map_broker_disconnect_outcome(disconnect.reason_code) {
                    ConnectionOutcome::Transport(TransportError::Connection(message)) => {
                        ConnectionOutcome::Transport(TransportError::Connection(message + &detail))
                    }
                    ConnectionOutcome::Protocol(ProtocolError::Violation(message)) => {
                        ConnectionOutcome::Protocol(ProtocolError::Violation(message + &detail))
                    }
                    other => {
                        if !detail.is_empty() {
                            tracing::warn!(reason = ?disconnect.reason_code, detail = %detail, "broker DISCONNECT");
                        }
                        other
                    }
                }
            }
            Ok(ReadOutcome::Transport(error)) => ConnectionOutcome::Transport(error),
            Ok(ReadOutcome::Protocol(error)) => ConnectionOutcome::Protocol(error),
            Err(join_err) => ConnectionOutcome::Application(AppError::TaskJoin(join_err.to_string())),
        }
    }

    fn map_write_outcome(result: Result<WriteOutcome, tokio::task::JoinError>) -> ConnectionOutcome {
        match result {
            Ok(WriteOutcome::Cancelled) => ConnectionOutcome::Shutdown(ShutdownError::Cancelled),
            Ok(WriteOutcome::Shutdown) => ConnectionOutcome::Shutdown(ShutdownError::Requested),
            #[cfg(test)]
            Ok(WriteOutcome::StubExit) => {
                ConnectionOutcome::Application(AppError::Internal("test stub write exit".into()))
            }
            Ok(WriteOutcome::CommandChannelClosed) => ConnectionOutcome::Application(AppError::CommandChannelClosed),
            Ok(WriteOutcome::ControlChannelClosed) => ConnectionOutcome::Application(AppError::CommandChannelClosed),
            Ok(WriteOutcome::Transport(error)) => ConnectionOutcome::Transport(error),
            Ok(WriteOutcome::Protocol(error)) => ConnectionOutcome::Protocol(error),
            Err(join_err) => ConnectionOutcome::Application(AppError::TaskJoin(join_err.to_string())),
        }
    }
}

/// Races a dial against the driver cancel; `None` = cancelled.
async fn dial_or_cancel<T>(
    cancel: &CancellationToken,
    dial: impl std::future::Future<Output = Result<T, ConnectionOutcome>>,
) -> Option<Result<T, ConnectionOutcome>> {
    tokio::select! {
        result = dial => Some(result),
        () = cancel.cancelled() => None,
    }
}

/// Whether [`Connection::run`] should attempt another session after a disconnect.
enum SessionDisposition {
    /// Backoff elapsed (or zero delay) — start a new session.
    Retry,
    /// Driver finished; return this result to the caller.
    Stop(Result<(), ConnectionOutcome>),
}

/// Broker-supplied diagnostics for error messages: Reason String and Server Reference.
pub(crate) fn broker_detail(reason_string: Option<&str>, server_reference: Option<&str>) -> String {
    let mut detail = String::new();
    if let Some(reason_string) = reason_string {
        detail.push_str(&format!(" ({reason_string})"));
    }
    if let Some(server_reference) = server_reference {
        detail.push_str(&format!(" [server reference: {server_reference}]"));
    }
    detail
}

/// Maps a broker DISCONNECT reason to a typed connection outcome.
pub(crate) fn map_broker_disconnect_outcome(reason: DisconnectReasonCode) -> ConnectionOutcome {
    match reason {
        DisconnectReasonCode::NotAuthorized | DisconnectReasonCode::BadAuthenticationMethod => {
            ConnectionOutcome::Auth(AuthError::NotAuthorized)
        }
        DisconnectReasonCode::KeepAliveTimeout => {
            ConnectionOutcome::Transport(TransportError::Connection("broker keep-alive timeout".into()))
        }
        DisconnectReasonCode::ProtocolError
        | DisconnectReasonCode::MalformedPacket
        | DisconnectReasonCode::TopicAliasInvalid
        | DisconnectReasonCode::PacketTooLarge => {
            ConnectionOutcome::Protocol(ProtocolError::Violation(format!("broker DISCONNECT: {reason:?}")))
        }
        _ => ConnectionOutcome::Transport(TransportError::Connection(format!("broker DISCONNECT: {reason:?}"))),
    }
}

#[cfg(test)]
pub(crate) mod test_support;

#[cfg(test)]
mod io_mock_tests;

#[cfg(test)]
#[allow(clippy::box_default, clippy::manual_async_fn)]
mod tests;
