//! LabOverWire `mqtt5` adapter (auto-ack only — no manual ack).

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use mqtt5::{ConnectOptions, MqttClient, PublishOptions, QoS, SubscribeOptions};
use tokio::task::JoinHandle;

use crate::compare::fixtures::{HANDOFF_CAPACITY, INGRESS_TOPIC, KEEP_ALIVE, OUTBOUND_TOPIC, RECEIVE_MAXIMUM};
use crate::compare::harness::{
    AckPolicy, BenchClient, BenchQos, ClientCaps, ClientId, IngressSession, OutboundSession, SessionCfg,
};
use crate::support::handoff::BenchHandoff;

/// LabOverWire mqtt5 bench client.
pub struct Mqtt5Client;

#[async_trait]
impl BenchClient for Mqtt5Client {
    fn id(&self) -> ClientId {
        ClientId::Mqtt5
    }

    fn caps(&self) -> ClientCaps {
        ClientCaps {
            qos0: true,
            qos1: true,
            ack_after_enqueue: false,
            ack_before: true,
        }
    }

    async fn setup_ingress(&self, cfg: &SessionCfg) -> Box<dyn IngressSession> {
        if matches!(cfg.ack, Some(AckPolicy::AfterEnqueue)) {
            panic!("mqtt5 has no after_enqueue path; matrix should mark n/a");
        }
        Box::new(setup_ingress(cfg).await)
    }

    async fn setup_outbound(&self, cfg: &SessionCfg) -> Box<dyn OutboundSession> {
        Box::new(setup_outbound(cfg).await)
    }
}

struct PeerIngress {
    handoff_rx: flume::Receiver<BenchHandoff>,
    handed_off: Arc<AtomicUsize>,
    topic: String,
    client: MqttClient,
}

struct PeerOutbound {
    client: MqttClient,
    qos: BenchQos,
}

fn is_transient_io(err: &mqtt5::MqttError) -> bool {
    let text = err.to_string();
    text.contains("No buffer space available") || text.contains("os error 55")
}

async fn publish_with_retry(client: &MqttClient, payload: Vec<u8>, opts: PublishOptions) {
    let mut delay = Duration::from_millis(1);
    for attempt in 0..32 {
        match client
            .publish_with_options(OUTBOUND_TOPIC, payload.clone(), opts.clone())
            .await
        {
            Ok(_) => return,
            Err(error) if is_transient_io(&error) && attempt + 1 < 32 => {
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_millis(50));
            }
            Err(error) => panic!("peer mqtt5 publish: {error}"),
        }
    }
}

fn peer_connect_options(client_id: &str) -> ConnectOptions {
    ConnectOptions::new(client_id)
        .with_clean_start(true)
        .with_keep_alive(KEEP_ALIVE)
        .with_receive_maximum(RECEIVE_MAXIMUM.get())
        .with_automatic_reconnect(false)
}

fn broker_url(port: u16) -> String {
    format!("mqtt://127.0.0.1:{port}")
}

fn wire_qos(qos: BenchQos) -> QoS {
    match qos {
        BenchQos::AtMostOnce => QoS::AtMostOnce,
        BenchQos::AtLeastOnce => QoS::AtLeastOnce,
    }
}

async fn setup_ingress(cfg: &SessionCfg) -> PeerIngress {
    let topic = format!("{INGRESS_TOPIC}/{}", cfg.client_id);
    let (handoff_tx, handoff_rx) = flume::bounded(HANDOFF_CAPACITY);
    let handed_off = Arc::new(AtomicUsize::new(0));
    let client = MqttClient::with_options(peer_connect_options(&cfg.client_id));
    client.connect(&broker_url(cfg.port)).await.expect("peer mqtt5 connect");

    let handed_off_counter = Arc::clone(&handed_off);
    let sub_opts = SubscribeOptions {
        qos: wire_qos(cfg.qos),
        ..Default::default()
    };
    client
        .subscribe_with_options(topic.clone(), sub_opts, move |message| {
            let handoff = BenchHandoff::from_peer_mqtt5(&message);
            if handoff_tx.send(handoff).is_err() {
                return;
            }
            handed_off_counter.fetch_add(1, Ordering::Relaxed);
        })
        .await
        .expect("peer mqtt5 subscribe");

    PeerIngress {
        handoff_rx,
        handed_off,
        topic,
        client,
    }
}

async fn setup_outbound(cfg: &SessionCfg) -> PeerOutbound {
    let client = MqttClient::with_options(peer_connect_options(&cfg.client_id));
    client
        .connect(&broker_url(cfg.port))
        .await
        .expect("peer mqtt5 outbound connect");
    PeerOutbound { client, qos: cfg.qos }
}

async fn wait_handed_off(handed_off: &AtomicUsize, target: usize) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while handed_off.load(Ordering::Relaxed) < target {
        if Instant::now() > deadline {
            panic!(
                "timeout waiting for peer mqtt5 handoffs: have {} want {target}",
                handed_off.load(Ordering::Relaxed)
            );
        }
        tokio::task::yield_now().await;
    }
}

#[async_trait]
impl IngressSession for PeerIngress {
    fn topic(&self) -> &str {
        &self.topic
    }

    fn reset_barrier(&self) {
        self.handed_off.store(0, Ordering::Relaxed);
    }

    fn drain(&self) {
        while self.handoff_rx.try_recv().is_ok() {}
    }

    async fn recv_touch(&self) {
        let msg = self.handoff_rx.recv_async().await.expect("peer mqtt5 handoff");
        msg.touch();
    }

    async fn wait_barrier(&self, target: usize) {
        wait_handed_off(&self.handed_off, target).await;
    }

    async fn close(&self) {
        let _ = self.client.disconnect().await;
    }
}

#[async_trait]
impl OutboundSession for PeerOutbound {
    async fn publish_serial(&self, payload: &[u8]) {
        let opts = PublishOptions {
            qos: wire_qos(self.qos),
            ..Default::default()
        };
        publish_with_retry(&self.client, payload.to_vec(), opts).await;
    }

    async fn publish_pipelined(&self, payload: &[u8], window: usize, n: usize) {
        let opts = PublishOptions {
            qos: wire_qos(self.qos),
            ..Default::default()
        };
        let mut in_flight: VecDeque<JoinHandle<()>> = VecDeque::with_capacity(window);
        for _ in 0..n {
            while in_flight.len() >= window {
                in_flight.pop_front().expect("peer mqtt5 window").await.expect("join");
            }
            let client = self.client.clone();
            let body = payload.to_vec();
            let publish_opts = opts.clone();
            in_flight.push_back(tokio::spawn(async move {
                publish_with_retry(&client, body, publish_opts).await;
            }));
        }
        while let Some(handle) = in_flight.pop_front() {
            handle.await.expect("peer mqtt5 drain");
        }
    }

    async fn close(&self) {
        let _ = self.client.disconnect().await;
    }
}
