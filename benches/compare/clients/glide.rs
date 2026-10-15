//! mqtt-glide adapter (enqueue-before-ack only — no auto-ack path).

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use bytestring::ByteString;
use mqtt_glide::Password;
use mqtt_glide::{
    AuthError, BrokerUrl, ClientId as GlideClientId, ConnectOptions, ConnectionOutcome, CredentialsProvider,
    InboundMessage, MqttClient, NoopLifecycle, Publish, PublishMessage, PublishProperties, QoS, ReconnectPolicy,
    SessionParams, SessionPhase, connect,
};
use tokio::task::JoinHandle;

use crate::compare::fixtures::{
    HANDOFF_CAPACITY, INGRESS_TOPIC, KEEP_ALIVE, OUTBOUND_TOPIC, RECEIVE_MAXIMUM, REQUEST_CAPACITY,
};
use crate::compare::harness::{
    AckPolicy, BenchClient, BenchQos, ClientCaps, ClientId, IngressSession, OutboundSession, SessionCfg,
};
use crate::support::handoff::touch_publish;

struct BenchCredentials;

impl CredentialsProvider for BenchCredentials {
    async fn credentials(&self) -> Result<(String, Password), AuthError> {
        Ok(("bench".into(), Password::from("bench")))
    }
}

struct NoReconnect;

impl ReconnectPolicy for NoReconnect {
    fn delay_after(&self, _outcome: &ConnectionOutcome) -> Option<Duration> {
        None
    }
}

/// mqtt-glide bench client.
pub struct GlideClient;

#[async_trait]
impl BenchClient for GlideClient {
    fn id(&self) -> ClientId {
        ClientId::Glide
    }

    fn caps(&self) -> ClientCaps {
        ClientCaps {
            qos0: true,
            qos1: true,
            ack_after_enqueue: true,
            ack_before: false,
        }
    }

    async fn setup_ingress(&self, cfg: &SessionCfg) -> Box<dyn IngressSession> {
        if matches!(cfg.ack, Some(AckPolicy::Before)) {
            panic!("glide has no ack-before path; matrix should mark n/a");
        }
        Box::new(setup_ingress(cfg).await)
    }

    async fn setup_outbound(&self, cfg: &SessionCfg) -> Box<dyn OutboundSession> {
        Box::new(setup_outbound(cfg).await)
    }
}

struct GlideIngress {
    handoff_rx: flume::Receiver<InboundMessage>,
    handed_off: Arc<AtomicUsize>,
    acked: Arc<AtomicUsize>,
    qos: BenchQos,
    topic: String,
    _client: MqttClient,
    _run: JoinHandle<Result<(), ConnectionOutcome>>,
}

struct GlideOutbound {
    client: MqttClient,
    qos: BenchQos,
    _run: JoinHandle<Result<(), ConnectionOutcome>>,
}

async fn setup_ingress(cfg: &SessionCfg) -> GlideIngress {
    let topic = format!("{INGRESS_TOPIC}/{}", cfg.client_id);
    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{}", cfg.port)).expect("broker url");
    let acked = Arc::new(AtomicUsize::new(0));
    let handed_off = Arc::new(AtomicUsize::new(0));
    let qos = wire_qos(cfg.qos);
    let mut options = ConnectOptions::new(url)
        .client_id(GlideClientId::from_prefix(&cfg.client_id))
        .keep_alive(KEEP_ALIVE)
        .receive_maximum(RECEIVE_MAXIMUM)
        .command_channel_capacity(NonZeroUsize::new(REQUEST_CAPACITY).expect("non-zero"));
    if cfg.qos == BenchQos::AtLeastOnce {
        options = options.bench_inbound_publish_ack_counter(Arc::clone(&acked));
    }
    let (handoff_tx, handoff_rx) = flume::bounded(HANDOFF_CAPACITY);
    let (client, connection, mut phase) =
        connect(options, BenchCredentials, NoReconnect, handoff_tx, NoopLifecycle).expect("glide connect");
    let run = tokio::spawn(connection.run());
    wait_connected(&mut phase).await;
    client
        .subscribe([(topic.as_str(), qos)])
        .await
        .expect("glide subscribe");
    GlideIngress {
        handoff_rx,
        handed_off,
        acked,
        qos: cfg.qos,
        topic,
        _client: client,
        _run: run,
    }
}

async fn setup_outbound(cfg: &SessionCfg) -> GlideOutbound {
    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{}", cfg.port)).expect("broker url");
    let options = ConnectOptions::new(url)
        .client_id(GlideClientId::from_prefix(&cfg.client_id))
        .keep_alive(KEEP_ALIVE)
        .receive_maximum(RECEIVE_MAXIMUM)
        .command_channel_capacity(NonZeroUsize::new(REQUEST_CAPACITY).expect("non-zero"));
    let (handoff_tx, _handoff_rx) = flume::bounded(HANDOFF_CAPACITY);
    let (client, connection, mut phase) =
        connect(options, BenchCredentials, NoReconnect, handoff_tx, NoopLifecycle).expect("glide connect");
    let run = tokio::spawn(connection.run());
    wait_connected(&mut phase).await;
    GlideOutbound {
        client,
        qos: cfg.qos,
        _run: run,
    }
}

async fn wait_connected(phase: &mut tokio::sync::watch::Receiver<SessionPhase>) {
    let _ = tokio::time::timeout(
        Duration::from_secs(30),
        phase.wait_for(|p| {
            matches!(
                p,
                SessionPhase::Connected {
                    session: SessionParams { from_connack: true, .. },
                    ..
                }
            )
        }),
    )
    .await
    .expect("glide Connected timeout")
    .expect("glide phase watch closed");
}

async fn wait_counter(counter: &AtomicUsize, target: usize, label: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while counter.load(Ordering::Relaxed) < target {
        if Instant::now() > deadline {
            panic!(
                "timeout waiting for {label}: have {} want {target}",
                counter.load(Ordering::Relaxed)
            );
        }
        tokio::task::yield_now().await;
    }
}

#[async_trait]
impl IngressSession for GlideIngress {
    fn topic(&self) -> &str {
        &self.topic
    }

    fn reset_barrier(&self) {
        self.acked.store(0, Ordering::Relaxed);
        self.handed_off.store(0, Ordering::Relaxed);
    }

    fn drain(&self) {
        while self.handoff_rx.try_recv().is_ok() {}
    }

    async fn recv_touch(&self) {
        let msg = self.handoff_rx.recv_async().await.expect("glide handoff");
        touch_publish(&msg);
        self.handed_off.fetch_add(1, Ordering::Relaxed);
    }

    async fn wait_barrier(&self, target: usize) {
        if self.qos == BenchQos::AtLeastOnce {
            wait_counter(&self.acked, target, "glide inbound PUBACKs").await;
        } else {
            wait_counter(&self.handed_off, target, "glide handoffs").await;
        }
    }
}

fn publish_message(payload: &[u8]) -> PublishMessage {
    PublishMessage {
        topic: ByteString::from_static(OUTBOUND_TOPIC),
        payload: Bytes::copy_from_slice(payload),
        properties: PublishProperties::default(),
    }
}

fn wire_qos(qos: BenchQos) -> QoS {
    match qos {
        BenchQos::AtMostOnce => QoS::AtMostOnce,
        BenchQos::AtLeastOnce => QoS::AtLeastOnce,
    }
}

#[async_trait]
impl OutboundSession for GlideOutbound {
    async fn publish_serial(&self, payload: &[u8]) {
        self.client
            .send(Publish::from(publish_message(payload)).qos(wire_qos(self.qos)))
            .await
            .expect("glide outbound publish");
    }

    async fn publish_pipelined(&self, payload: &[u8], window: usize, n: usize) {
        let qos = wire_qos(self.qos);
        let capacity = NonZeroUsize::new(window).expect("pipeline window > 0");
        let mut pipe = self.client.publish_window(capacity);
        for _ in 0..n {
            pipe.push(Publish::from(publish_message(payload)).qos(qos))
                .await
                .expect("glide pipelined publish");
        }
        pipe.drain().await.expect("glide pipelined drain");
    }
}
