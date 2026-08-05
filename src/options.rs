//! Connect options: broker URL, client ID, keep-alive, receive maximum, TLS trust.

use std::num::{NonZeroU16, NonZeroUsize};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use nonzero_ext::nonzero;

use crate::codec::{QoS, WillMessage};
use crate::util::{BrokerUrl, ClientId};

/// Default capacity of the bounded application command/data path to the write task.
///
/// App → write-task admission queue (not PUBACK inflight). Sized for typical
/// single-flight / low-concurrency callers. Raise explicitly when keeping many
/// concurrent `publish().await` (or queued commands) in flight — otherwise the
/// queue fills and callers see [`crate::AppError::Backpressure`].
///
/// Used as [`ConnectOptions::command_channel_capacity`] when not overridden.
pub const COMMAND_CHANNEL_CAPACITY: usize = 32;

/// Default inbound QoS-1 pipelining window ([`ConnectOptions::receive_maximum`]).
///
/// Bounded enough to limit memory under handoff stall / a misbehaving broker;
/// raise explicitly for high-throughput pipelines.
pub const DEFAULT_RECEIVE_MAXIMUM: NonZeroU16 = nonzero!(32u16);

/// Default MQTT keep-alive interval when not overridden.
const DEFAULT_KEEP_ALIVE: Duration = Duration::from_secs(60);

/// Default deadline waiting for PUBACK / SUBACK / UNSUBACK after the matching request.
pub const DEFAULT_ACK_TIMEOUT: Duration = Duration::from_secs(5);

/// Default budget for TCP dial + optional TLS + CONNACK ([`ConnectOptions::connect_timeout`]).
///
/// Default **5 s**. One shared deadline; stage-specific timeout errors still
/// surface (TCP / TLS / CONNACK).
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Default budget for PUBACK + DISCONNECT flush during graceful shutdown.
///
/// Default **2 s**. Applied as the write-task read-join + flush budget via
/// [`ConnectOptions::graceful_disconnect_timeout`].
pub const DEFAULT_GRACEFUL_DISCONNECT_TIMEOUT: Duration = Duration::from_secs(2);

/// TLS trust configuration for broker connections.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TlsConfig {
    /// Use the operating system's trusted root certificate store.
    SystemRoots,
}

/// Broker endpoint and session parameters for [`crate::connect`].
///
/// Prefer the fluent setters to build. Fields are `pub` so the driver
/// and callers can read values without `get_*` accessors; they are also writable
/// (no extra validation beyond the field types).
#[derive(Debug, Clone)]
pub struct ConnectOptions {
    /// Parsed broker endpoint (host, port, TLS requirement).
    pub broker_url: BrokerUrl,
    /// MQTT client identifier sent in CONNECT, if set.
    pub client_id: Option<ClientId>,
    /// MQTT keep-alive interval.
    pub keep_alive: Duration,
    /// Client **Receive Maximum** (inbound QoS-1 pipelining window).
    pub receive_maximum: NonZeroU16,
    /// MQTT v5 **Clean Start** flag.
    ///
    /// `false` asks the broker to resume a stored session. The driver keeps no local
    /// session store (no QoS 1 DUP redelivery); subscriptions are re-issued whenever the
    /// broker answers `session_present = false`, so a lost session is rebuilt rather than
    /// silently dropped.
    pub clean_start: bool,
    /// Optional **Session Expiry Interval** (omit property when `None`).
    pub session_expiry: Option<Duration>,
    /// Whether `TCP_NODELAY` is enabled on the underlying socket.
    pub tcp_nodelay: bool,
    /// Explicit TLS trust configuration, if set (overrides scheme-only hint).
    pub tls: Option<TlsConfig>,
    /// Deadline for awaiting PUBACK / SUBACK / UNSUBACK ([`DEFAULT_ACK_TIMEOUT`] when unset).
    pub ack_timeout: Duration,
    /// Shared budget for TCP dial + optional TLS + waiting for CONNACK
    /// ([`DEFAULT_CONNECT_TIMEOUT`] when unset). Stage-specific timeout messages still apply.
    pub connect_timeout: Duration,
    /// Budget for flushing pending inbound PUBACKs + DISCONNECT on [`crate::MqttClient::shutdown`].
    ///
    /// Default: [`DEFAULT_GRACEFUL_DISCONNECT_TIMEOUT`] (2 s). Caps the write-task
    /// read-join + drain (`AckInboundPublish*` then `DISCONNECT`) so shutdown cannot hang
    /// on a stalled flush.
    pub graceful_disconnect_timeout: Duration,
    /// Bounded capacity of the application → write-task command channel.
    ///
    /// When full, [`crate::MqttClient::publish`], `subscribe`, and `unsubscribe` return
    /// [`crate::AppError::Backpressure`]. This queue is independent of MQTT **Receive Maximum**
    /// (inbound QoS-1 pipelining) and outbound publish inflight limits.
    ///
    /// Default: [`COMMAND_CHANNEL_CAPACITY`] (32). Zero is rejected (`NonZeroUsize`).
    pub command_channel_capacity: NonZeroUsize,
    /// Optional Will Message published by the broker on abnormal disconnect (§3.1.2.5).
    ///
    /// Default `None` (no Will Flag). Validated when connecting: Topic Name rules, QoS 0|1 only.
    /// CONNACK Maximum QoS / Retain Available apply to later PUBLISH, not to this CONNECT field
    /// (the Will is already on the wire before CONNACK).
    pub will_message: Option<WillMessage>,
    /// Optional counter incremented when the write task feeds an inbound QoS-1 PUBACK.
    ///
    /// Bench / diagnostic hook only (`None` in production). Does not change enqueue-before-ack semantics.
    #[doc(hidden)]
    pub bench_inbound_publish_ack_counter: Option<Arc<AtomicUsize>>,
}

impl ConnectOptions {
    /// Creates options for `broker_url` with documented defaults.
    ///
    /// Defaults: keep-alive 60 s, [`DEFAULT_RECEIVE_MAXIMUM`] (32), `clean_start`
    /// true, `tcp_nodelay` true, no explicit TLS config (see [`BrokerUrl`] scheme).
    pub fn new(broker_url: BrokerUrl) -> Self {
        Self {
            broker_url,
            client_id: None,
            keep_alive: DEFAULT_KEEP_ALIVE,
            receive_maximum: DEFAULT_RECEIVE_MAXIMUM,
            clean_start: true,
            session_expiry: None,
            tcp_nodelay: true,
            tls: None,
            ack_timeout: DEFAULT_ACK_TIMEOUT,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            graceful_disconnect_timeout: DEFAULT_GRACEFUL_DISCONNECT_TIMEOUT,
            command_channel_capacity: NonZeroUsize::new(COMMAND_CHANNEL_CAPACITY)
                .expect("COMMAND_CHANNEL_CAPACITY is non-zero"),
            will_message: None,
            bench_inbound_publish_ack_counter: None,
        }
    }

    /// Increments `counter` each time the write task feeds an inbound QoS-1 PUBACK (bench hook).
    #[doc(hidden)]
    pub fn bench_inbound_publish_ack_counter(mut self, counter: Arc<AtomicUsize>) -> Self {
        self.bench_inbound_publish_ack_counter = Some(counter);
        self
    }

    /// Sets the MQTT client identifier sent in CONNECT.
    pub fn client_id(mut self, client_id: ClientId) -> Self {
        self.client_id = Some(client_id);
        self
    }

    /// Sets the MQTT keep-alive interval.
    pub fn keep_alive(mut self, keep_alive: Duration) -> Self {
        self.keep_alive = keep_alive;
        self
    }

    /// Sets the client **Receive Maximum** (inbound QoS-1 pipelining window).
    ///
    /// Default: [`DEFAULT_RECEIVE_MAXIMUM`] (32).
    pub fn receive_maximum(mut self, receive_maximum: NonZeroU16) -> Self {
        self.receive_maximum = receive_maximum;
        self
    }

    /// Sets the MQTT v5 **Clean Start** flag.
    pub fn clean_start(mut self, clean_start: bool) -> Self {
        self.clean_start = clean_start;
        self
    }

    /// Sets optional **Session Expiry Interval** (omit property when `None`).
    pub fn session_expiry(mut self, session_expiry: Option<Duration>) -> Self {
        self.session_expiry = session_expiry;
        self
    }

    /// Enables or disables `TCP_NODELAY` on the underlying socket.
    pub fn tcp_nodelay(mut self, tcp_nodelay: bool) -> Self {
        self.tcp_nodelay = tcp_nodelay;
        self
    }

    /// Sets explicit TLS trust configuration (overrides scheme-only hint).
    pub fn tls(mut self, tls: TlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }

    /// Sets the PUBACK / SUBACK / UNSUBACK await deadline (default: [`DEFAULT_ACK_TIMEOUT`]).
    pub fn ack_timeout(mut self, ack_timeout: Duration) -> Self {
        self.ack_timeout = ack_timeout;
        self
    }

    /// Sets the TCP + TLS + CONNACK handshake budget (default: [`DEFAULT_CONNECT_TIMEOUT`]).
    pub fn connect_timeout(mut self, connect_timeout: Duration) -> Self {
        self.connect_timeout = connect_timeout;
        self
    }

    /// Sets the graceful-disconnect flush budget (default: [`DEFAULT_GRACEFUL_DISCONNECT_TIMEOUT`]).
    pub fn graceful_disconnect_timeout(mut self, graceful_disconnect_timeout: Duration) -> Self {
        self.graceful_disconnect_timeout = graceful_disconnect_timeout;
        self
    }

    /// Sets the bounded application → write-task command channel capacity.
    ///
    /// Default: [`COMMAND_CHANNEL_CAPACITY`] (32). Zero is rejected (`NonZeroUsize`).
    pub fn command_channel_capacity(mut self, command_channel_capacity: NonZeroUsize) -> Self {
        self.command_channel_capacity = command_channel_capacity;
        self
    }

    /// Sets the Will Message for CONNECT (§3.1.2.5).
    ///
    /// Builds [`WillMessage`] internally with defaults: QoS 0, retain `false`. Chain
    /// [`Self::will_qos`] / [`Self::will_retain`] (and other `will_*` helpers) as needed.
    /// Topic Name / QoS rules are enforced at [`crate::connect`] /
    /// [`crate::ClientBuilder::connect`] time.
    ///
    /// For a pre-built [`WillMessage`], use [`Self::will_message`].
    pub fn will(mut self, topic: impl Into<bytestring::ByteString>, payload: impl Into<bytes::Bytes>) -> Self {
        self.will_message = Some(WillMessage::new(topic, payload));
        self
    }

    /// Sets Will QoS on an existing Will (§3.1.2.6). No-op if [`Self::will`] was not called.
    ///
    /// Default when unset: [`QoS::AtMostOnce`]. QoS 2 is rejected at connect.
    pub fn will_qos(mut self, qos: QoS) -> Self {
        if let Some(will) = self.will_message.as_mut() {
            will.qos = qos;
        }
        self
    }

    /// Sets Will Retain on an existing Will (§3.1.2.7). No-op if [`Self::will`] was not called.
    ///
    /// Default when unset: `false`.
    pub fn will_retain(mut self, retain: bool) -> Self {
        if let Some(will) = self.will_message.as_mut() {
            will.retain = retain;
        }
        self
    }

    /// Sets Will Delay Interval in **seconds** on an existing Will (§3.1.3.2.2).
    ///
    /// No-op if [`Self::will`] was not called. Only meaningful with non-zero Session Expiry.
    pub fn will_delay_interval(mut self, seconds: u32) -> Self {
        if let Some(will) = self.will_message.as_mut() {
            will.will_delay_interval = Some(seconds);
        }
        self
    }

    /// Replaces the Will Message with a pre-built [`WillMessage`] (power path), or clears it with [`None`].
    pub fn will_message(mut self, will_message: Option<WillMessage>) -> Self {
        self.will_message = will_message;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// [`ConnectOptions::new`] must apply every documented default, and each named
    /// constant must match the values asserted below (including the 2 s graceful
    /// disconnect budget; keep-alive / receive-maximum / timeouts elsewhere).
    #[test]
    fn connect_options_defaults_match_spec_constants() {
        let url = BrokerUrl::parse("ssl://broker.example").expect("valid url");
        let options = ConnectOptions::new(url);

        assert_eq!(options.keep_alive, Duration::from_secs(60));
        assert!(options.clean_start);
        assert!(options.tcp_nodelay);
        assert!(options.session_expiry.is_none());
        assert!(options.tls.is_none());
        assert!(options.will_message.is_none());

        assert_eq!(options.receive_maximum, DEFAULT_RECEIVE_MAXIMUM);
        assert_eq!(DEFAULT_RECEIVE_MAXIMUM.get(), 32);

        assert_eq!(options.ack_timeout, DEFAULT_ACK_TIMEOUT);
        assert_eq!(DEFAULT_ACK_TIMEOUT, Duration::from_secs(5));

        assert_eq!(options.connect_timeout, DEFAULT_CONNECT_TIMEOUT);
        assert_eq!(DEFAULT_CONNECT_TIMEOUT, Duration::from_secs(5));

        assert_eq!(options.graceful_disconnect_timeout, DEFAULT_GRACEFUL_DISCONNECT_TIMEOUT);
        assert_eq!(DEFAULT_GRACEFUL_DISCONNECT_TIMEOUT, Duration::from_secs(2));

        assert_eq!(options.command_channel_capacity.get(), COMMAND_CHANNEL_CAPACITY);
        assert_eq!(COMMAND_CHANNEL_CAPACITY, 32);
    }

    #[test]
    fn builder_overrides_receive_maximum() {
        let url = BrokerUrl::parse("mqtt://broker.example").expect("valid url");
        let custom = NonZeroU16::new(256).expect("non-zero");
        let options = ConnectOptions::new(url).receive_maximum(custom);
        assert_eq!(options.receive_maximum, custom);
    }

    #[test]
    fn builder_overrides_ack_timeout() {
        let url = BrokerUrl::parse("mqtt://broker.example").expect("valid url");
        let options = ConnectOptions::new(url).ack_timeout(Duration::from_millis(50));
        assert_eq!(options.ack_timeout, Duration::from_millis(50));
    }

    #[test]
    fn builder_overrides_connect_timeout() {
        let url = BrokerUrl::parse("mqtt://broker.example").expect("valid url");
        let options = ConnectOptions::new(url).connect_timeout(Duration::from_millis(25));
        assert_eq!(options.connect_timeout, Duration::from_millis(25));
    }

    #[test]
    fn builder_overrides_graceful_disconnect_timeout() {
        let url = BrokerUrl::parse("mqtt://broker.example").expect("valid url");
        let options = ConnectOptions::new(url).graceful_disconnect_timeout(Duration::from_millis(100));
        assert_eq!(options.graceful_disconnect_timeout, Duration::from_millis(100));
    }

    #[test]
    fn builder_overrides_command_channel_capacity() {
        let url = BrokerUrl::parse("mqtt://broker.example").expect("valid url");
        let custom = NonZeroUsize::new(128).expect("non-zero");
        let options = ConnectOptions::new(url).command_channel_capacity(custom);
        assert_eq!(options.command_channel_capacity, custom);
    }

    #[test]
    fn will_fluent_sets_defaults_and_overrides() {
        let url = BrokerUrl::parse("mqtt://broker.example").expect("valid url");
        let options = ConnectOptions::new(url)
            .will("status/offline", "gone")
            .will_qos(QoS::AtLeastOnce)
            .will_retain(true);
        let will = options.will_message.expect("will");
        assert_eq!(&*will.topic, "status/offline");
        assert_eq!(will.payload.as_ref(), b"gone");
        assert_eq!(will.qos, QoS::AtLeastOnce);
        assert!(will.retain);

        let url = BrokerUrl::parse("mqtt://broker.example").expect("valid url");
        let will = ConnectOptions::new(url).will("t", "p").will_message.expect("will");
        assert_eq!(will.qos, QoS::AtMostOnce);
        assert!(!will.retain);
    }
}
