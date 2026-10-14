//! Measurement contract: thin adapters, not a lowest-common-denominator API.

use async_trait::async_trait;

/// Selectable client under test.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ClientId {
    /// This crate (`mqtt-glide`).
    Glide,
    /// `rumqttc-next`.
    Rumqttc,
    /// LabOverWire `mqtt5` crate.
    Mqtt5,
}

impl ClientId {
    /// Stable file / JSON id.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Glide => "glide",
            Self::Rumqttc => "rumqttc",
            Self::Mqtt5 => "mqtt5",
        }
    }
}

/// QoS under test (QoS 2 is never used).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BenchQos {
    /// QoS 0.
    AtMostOnce,
    /// QoS 1.
    AtLeastOnce,
}

impl BenchQos {
    /// Wire / JSON value.
    pub const fn as_u8(self) -> u8 {
        match self {
            Self::AtMostOnce => 0,
            Self::AtLeastOnce => 1,
        }
    }
}

/// Ingress QoS 1 acknowledgement policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AckPolicy {
    /// PUBACK only after app handoff accept (enqueue-before-ack / manual ack after flume).
    AfterEnqueue,
    /// PUBACK before (or without waiting for) app handoff (auto-ack).
    Before,
}

impl AckPolicy {
    /// JSON label.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AfterEnqueue => "after_enqueue",
            Self::Before => "before",
        }
    }
}

/// What a client can honestly participate in (no fake paths).
#[derive(Debug, Clone, Copy)]
pub struct ClientCaps {
    /// Supports QoS 0 publish/subscribe paths used by the matrix.
    pub qos0: bool,
    /// Supports QoS 1 publish/subscribe paths used by the matrix.
    pub qos1: bool,
    /// Can delay inbound PUBACK until after handoff (`after_enqueue`).
    pub ack_after_enqueue: bool,
    /// Has an auto-ack / PUBACK-before-handoff path (`before`).
    pub ack_before: bool,
}

impl ClientCaps {
    /// Why this client cannot fill an ingress QoS1 ack cell (if any).
    pub fn ingress_ack_unsupported(self, ack: AckPolicy) -> Option<&'static str> {
        match ack {
            AckPolicy::AfterEnqueue if !self.ack_after_enqueue => Some("no_manual_ack"),
            AckPolicy::Before if !self.ack_before => Some("no_auto_ack"),
            _ => None,
        }
    }

    /// Whether the client supports the given QoS.
    pub fn supports_qos(self, qos: BenchQos) -> bool {
        match qos {
            BenchQos::AtMostOnce => self.qos0,
            BenchQos::AtLeastOnce => self.qos1,
        }
    }
}

/// Per-session connect / subscribe parameters.
#[derive(Debug, Clone)]
pub struct SessionCfg {
    /// Broker TCP port.
    pub port: u16,
    /// MQTT client id.
    pub client_id: String,
    /// QoS for subscribe / publish.
    pub qos: BenchQos,
    /// Ingress ack policy (QoS 1 only; ignored for QoS 0).
    pub ack: Option<AckPolicy>,
}

/// Client adapter: caps + session setup. Implementations stay idiomatic per crate.
// async_trait expansion triggers clippy::double_must_use on returned futures.
#[allow(clippy::double_must_use)]
#[async_trait]
pub trait BenchClient: Send + Sync {
    /// Stable id (`glide`, `rumqttc`, `mqtt5`).
    fn id(&self) -> ClientId;

    /// Honest capability flags.
    fn caps(&self) -> ClientCaps;

    /// Subscribe + handoff path for ingress scenarios.
    async fn setup_ingress(&self, cfg: &SessionCfg) -> Box<dyn IngressSession>;

    /// Publish path for outbound scenarios.
    async fn setup_outbound(&self, cfg: &SessionCfg) -> Box<dyn OutboundSession>;
}

/// Ingress session: receive handoff, wait for ack/handoff barrier.
// async_trait expansion triggers clippy::double_must_use on returned futures.
#[allow(clippy::double_must_use)]
#[async_trait]
pub trait IngressSession: Send + Sync {
    /// Subscribed topic (unique per session).
    fn topic(&self) -> &str;

    /// Reset barrier counters before a load burst.
    fn reset_barrier(&self);

    /// Drain any leftover handoffs without counting.
    fn drain(&self);

    /// Block until one message is handed off and touched.
    async fn recv_touch(&self);

    /// Wait until barrier reaches `target` (PUBACKs or handoffs, depending on ack policy).
    async fn wait_barrier(&self, target: usize);

    /// Release broker resources (disconnect). Default: no-op.
    async fn close(&self) {}
}

/// Outbound session: serial, windowed, and harness-driven concurrent publish.
///
/// `Sync` is required so outbound concurrent load can share one session across
/// caller tasks (`publish_serial` only).
// async_trait expansion triggers clippy::double_must_use on returned futures.
#[allow(clippy::double_must_use)]
#[async_trait]
pub trait OutboundSession: Send + Sync {
    /// One publish; QoS 1 awaits PUBACK, QoS 0 awaits write completion as defined by the client.
    ///
    /// Also the sole feed API for outbound concurrent (HTTP-style fan-out) load.
    async fn publish_serial(&self, payload: &[u8]);

    /// Up to `window` in flight; await oldest first; `n` total messages.
    async fn publish_pipelined(&self, payload: &[u8], window: usize, n: usize);

    /// Release broker resources (disconnect). Default: no-op.
    async fn close(&self) {}
}
