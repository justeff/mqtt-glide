//! Connect options: broker URL, client ID, keep-alive, receive maximum, TLS trust.

use std::num::{NonZeroU16, NonZeroU32, NonZeroUsize};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use nonzero_ext::nonzero;

use crate::codec::{DEFAULT_MAX_PACKET_SIZE, QoS, WillMessage};
use crate::util::{BrokerUrl, ClientId, HttpProxy};

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
    /// `false` asks the broker to resume a stored session. Within the process, unacknowledged
    /// QoS 1 publishes are then resent with DUP after a reconnect ([MQTT-4.4.0-1]; in memory,
    /// not across restarts), and subscriptions are re-issued whenever the broker answers
    /// `session_present = false`. Resume also needs a non-zero [`Self::session_expiry`] —
    /// with the default (`None` → 0 on the wire) the broker ends the session on disconnect.
    /// Cross-process resume requires a stable [`ClientId`] (via [`ClientId::try_new`]).
    /// [`ClientId::from_prefix`] always mints a fresh UUID.
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
    /// Budget for the [`crate::MqttClient::shutdown`] drain.
    ///
    /// Default: [`DEFAULT_GRACEFUL_DISCONNECT_TIMEOUT`] (2 s). Caps sending queued commands,
    /// waiting for outbound `PUBACK`s while inbound is still delivered, the final inbound
    /// `PUBACK` flush and `DISCONNECT`, so shutdown cannot hang on a stalled broker or socket.
    pub graceful_disconnect_timeout: Duration,
    /// Bounded capacity of the application → write-task command channel.
    ///
    /// When full, awaited calls ([`crate::MqttClient::publish`], `send`, `subscribe`,
    /// `unsubscribe`, `PublishWindow::push`) wait up to [`Self::ack_timeout`] for room, then
    /// return [`crate::AppError::Backpressure`]; [`crate::MqttClient::try_start_publish`] fails at
    /// once. This queue is independent of MQTT **Receive Maximum** (inbound QoS-1 pipelining)
    /// and outbound publish inflight limits.
    ///
    /// Default: [`COMMAND_CHANNEL_CAPACITY`] (32). Zero is rejected (`NonZeroUsize`).
    pub command_channel_capacity: NonZeroUsize,
    /// Optional Will Message published by the broker on abnormal disconnect (§3.1.2.5).
    ///
    /// Default `None` (no Will Flag). Validated when connecting: Topic Name rules, QoS 0|1 only.
    /// CONNACK Maximum QoS / Retain Available apply to later PUBLISH, not to this CONNECT field
    /// (the Will is already on the wire before CONNACK).
    pub will_message: Option<WillMessage>,
    /// Client **Maximum Packet Size** advertised in CONNECT and enforced on the inbound decoder
    /// ([MQTT-3.1.2-24]). Default: [`DEFAULT_MAX_PACKET_SIZE`] (1 MiB). Broker CONNACK limits
    /// still apply only to outbound encode.
    pub max_packet_size: NonZeroU32,
    /// Inbound **Topic Alias Maximum** advertised in CONNECT: how many Topic Aliases the
    /// broker may use towards this client (§3.1.2.11.5). Default 0 (none). Aliases are
    /// resolved before delivery, so the application always sees the full topic.
    pub topic_alias_maximum: u16,
    /// Outbound Topic Aliases this client uses at most, further capped by the broker's
    /// CONNACK Topic Alias Maximum. Default 0 (off). The least recently used topic is
    /// rebound when all are taken; worth it for long topics published often.
    pub outbound_topic_aliases: u16,
    /// Reject commands with [`crate::AppError::NotConnected`] while no connection is up
    /// (connecting, reconnect backoff) instead of queueing them for the next connection.
    ///
    /// Default `false`: queueing is what a resumable session wants, and the caller's wait is
    /// still bounded by [`Self::ack_timeout`]. Set it when a publish that cannot go out now is
    /// worthless (live telemetry) or the application keeps its own buffer.
    pub fail_fast_when_disconnected: bool,
    /// Follow a broker's redirect (§4.11): a CONNACK or DISCONNECT with Reason Code 0x9C
    /// (Use another server) or 0x9D (Server moved) and a Server Reference.
    ///
    /// When set, the next connection goes to the referenced server at once, whatever the
    /// [`ReconnectPolicy`](crate::ReconnectPolicy) says; 0x9D keeps the new server for good,
    /// 0x9C only for that connection. The reference's host and port replace the configured
    /// ones; scheme, TLS and WebSocket path stay, and TLS verifies the new host's certificate.
    /// After five redirects in a row (a session that ran for a minute before its redirect ends
    /// the streak) the client stops following and leaves the next attempt to the reconnect
    /// policy, towards the configured broker or the last 0x9D target.
    ///
    /// Default `false`: a redirect is then reported like any other refusal or disconnect (its
    /// Server Reference is in the error text).
    pub follow_server_redirects: bool,
    /// Reach the broker through an HTTP proxy (`CONNECT` tunnel); see [`HttpProxy`].
    ///
    /// Default `None`: connect directly. Not read from `HTTP(S)_PROXY`; set it explicitly.
    pub http_proxy: Option<HttpProxy>,
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
            max_packet_size: NonZeroU32::new(DEFAULT_MAX_PACKET_SIZE).expect("DEFAULT_MAX_PACKET_SIZE non-zero"),
            fail_fast_when_disconnected: false,
            follow_server_redirects: false,
            http_proxy: None,
            topic_alias_maximum: 0,
            outbound_topic_aliases: 0,
            bench_inbound_publish_ack_counter: None,
        }
    }

    /// Sets [`Self::topic_alias_maximum`] (inbound aliases the broker may use).
    pub fn topic_alias_maximum(mut self, maximum: u16) -> Self {
        self.topic_alias_maximum = maximum;
        self
    }

    /// Sets [`Self::outbound_topic_aliases`] (aliases this client may use when publishing).
    pub fn outbound_topic_aliases(mut self, count: u16) -> Self {
        self.outbound_topic_aliases = count;
        self
    }

    /// Sets [`Self::fail_fast_when_disconnected`].
    pub fn fail_fast_when_disconnected(mut self, fail_fast: bool) -> Self {
        self.fail_fast_when_disconnected = fail_fast;
        self
    }

    /// Sets [`Self::follow_server_redirects`].
    pub fn follow_server_redirects(mut self, follow: bool) -> Self {
        self.follow_server_redirects = follow;
        self
    }

    /// Sets [`Self::http_proxy`].
    pub fn http_proxy(mut self, proxy: HttpProxy) -> Self {
        self.http_proxy = Some(proxy);
        self
    }

    /// Increments `counter` each time the write task feeds an inbound QoS-1 PUBACK (bench hook).
    #[doc(hidden)]
    pub fn bench_inbound_publish_ack_counter(mut self, counter: Arc<AtomicUsize>) -> Self {
        self.bench_inbound_publish_ack_counter = Some(counter);
        self
    }

    /// Sets the MQTT client identifier sent in CONNECT.
    ///
    /// Use [`ClientId::try_new`] for a stable ID across process restarts, or
    /// [`ClientId::from_prefix`] for a fresh random ID.
    pub fn client_id(mut self, client_id: ClientId) -> Self {
        self.client_id = Some(client_id);
        self
    }

    /// Sets the MQTT keep-alive interval: whole seconds, fractions dropped (under one second
    /// turns keep-alive off), at most 65 535 s.
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

    /// Sets the client **Maximum Packet Size** advertised in CONNECT / enforced inbound.
    ///
    /// Default: [`DEFAULT_MAX_PACKET_SIZE`] (1 MiB).
    pub fn max_packet_size(mut self, max_packet_size: NonZeroU32) -> Self {
        self.max_packet_size = max_packet_size;
        self
    }

    /// Sets the MQTT v5 **Clean Start** flag.
    ///
    /// `false` together with a non-zero [`Self::session_expiry`] makes the session outlive the
    /// connection: unacknowledged QoS 1 publishes are resent after a reconnect into it
    /// ([MQTT-4.4.0-1]). Needs a stable [`Self::client_id`].
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
    ///
    /// Also bounds waiting for command-channel room. The deadline ends the caller's wait, not
    /// the publish: during a reconnect an unacknowledged publish of a resumed session is still
    /// resent after it, so keep it above the reconnect delay when duplicates matter.
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
