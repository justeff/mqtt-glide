//! rumqttc-next adapter (`manual_acks` after flume and auto-ack).

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use rumqttc::{
    AsyncClient, ConnectReturnCode, Event, MqttOptions, NetworkOptions, Packet, PublishNotice, QoS as RumqttQoS,
};
use tokio::task::JoinHandle;

use crate::compare::fixtures::{
    HANDOFF_CAPACITY, INGRESS_TOPIC, KEEP_ALIVE, OUTBOUND_TOPIC, RECEIVE_MAXIMUM, REQUEST_CAPACITY,
};
use crate::compare::harness::{
    AckPolicy, BenchClient, BenchQos, ClientCaps, ClientId, IngressSession, OutboundSession, SessionCfg,
};
use crate::support::handoff::BenchHandoff;

/// rumqttc-next bench client.
pub struct RumqttcClient;

#[async_trait]
impl BenchClient for RumqttcClient {
    fn id(&self) -> ClientId {
        ClientId::Rumqttc
    }

    fn caps(&self) -> ClientCaps {
        ClientCaps {
            qos0: true,
            qos1: true,
            ack_after_enqueue: true,
            ack_before: true,
        }
    }

    async fn setup_ingress(&self, cfg: &SessionCfg) -> Box<dyn IngressSession> {
        Box::new(setup_ingress(cfg).await)
    }

    async fn setup_outbound(&self, cfg: &SessionCfg) -> Box<dyn OutboundSession> {
        Box::new(setup_outbound(cfg).await)
    }
}

struct RumqttcIngress {
    handoff_rx: flume::Receiver<BenchHandoff>,
    handed_off: Arc<AtomicUsize>,
    acked: Arc<AtomicUsize>,
    after_enqueue: bool,
    qos: BenchQos,
    topic: String,
    _client: AsyncClient,
    _poll: JoinHandle<()>,
}

struct RumqttcOutbound {
    client: AsyncClient,
    qos: BenchQos,
    _poll: JoinHandle<()>,
}

fn base_opts(client_id: &str, port: u16) -> MqttOptions {
    let mut opts = MqttOptions::new(client_id, ("127.0.0.1", port));
    opts.set_keep_alive(KEEP_ALIVE.as_secs() as u16);
    opts.set_receive_maximum(Some(RECEIVE_MAXIMUM.get()));
    opts.set_network_options({
        let mut network = NetworkOptions::new();
        network.set_tcp_nodelay(true);
        network
    });
    opts
}

fn wire_qos(qos: BenchQos) -> RumqttQoS {
    match qos {
        BenchQos::AtMostOnce => RumqttQoS::AtMostOnce,
        BenchQos::AtLeastOnce => RumqttQoS::AtLeastOnce,
    }
}

async fn wait_connack(eventloop: &mut rumqttc::EventLoop) {
    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::ConnAck(ack))) if ack.code == ConnectReturnCode::Success => {
                return;
            }
            Ok(_) => continue,
            Err(error) => panic!("rumqttc CONNACK failed: {error:?}"),
        }
    }
}

async fn wait_suback(eventloop: &mut rumqttc::EventLoop) {
    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::SubAck(_))) => return,
            Ok(_) => continue,
            Err(error) => panic!("rumqttc SUBACK failed: {error:?}"),
        }
    }
}

async fn setup_ingress(cfg: &SessionCfg) -> RumqttcIngress {
    let topic = format!("{INGRESS_TOPIC}/{}", cfg.client_id);
    let (handoff_tx, handoff_rx) = flume::bounded(HANDOFF_CAPACITY);
    let acked = Arc::new(AtomicUsize::new(0));
    let handed_off = Arc::new(AtomicUsize::new(0));
    let after_enqueue = matches!(cfg.ack, Some(AckPolicy::AfterEnqueue)) && cfg.qos == BenchQos::AtLeastOnce;
    let mut opts = base_opts(&cfg.client_id, cfg.port);
    if cfg.qos == BenchQos::AtLeastOnce {
        opts.set_manual_acks(after_enqueue);
    }
    let (client, mut eventloop) = AsyncClient::builder(opts).capacity(REQUEST_CAPACITY).build();
    let ack_client = client.clone();
    wait_connack(&mut eventloop).await;
    client
        .subscribe(topic.as_str(), wire_qos(cfg.qos))
        .await
        .expect("rumqttc subscribe");
    wait_suback(&mut eventloop).await;

    let acked_counter = Arc::clone(&acked);
    let handed_off_counter = Arc::clone(&handed_off);
    let poll = tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::Publish(publish))) => {
                    let handoff = BenchHandoff::from_rumqttc(&publish);
                    if handoff_tx.send_async(handoff).await.is_err() {
                        break;
                    }
                    handed_off_counter.fetch_add(1, Ordering::Relaxed);
                    if after_enqueue && ack_client.ack(&publish).await.is_err() {
                        break;
                    }
                    if after_enqueue {
                        acked_counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }
    });

    RumqttcIngress {
        handoff_rx,
        handed_off,
        acked,
        after_enqueue,
        qos: cfg.qos,
        topic,
        _client: client,
        _poll: poll,
    }
}

async fn setup_outbound(cfg: &SessionCfg) -> RumqttcOutbound {
    let opts = base_opts(&cfg.client_id, cfg.port);
    let (client, mut eventloop) = AsyncClient::builder(opts).capacity(REQUEST_CAPACITY).build();
    wait_connack(&mut eventloop).await;
    let poll = tokio::spawn(async move { while eventloop.poll().await.is_ok() {} });
    RumqttcOutbound {
        client,
        qos: cfg.qos,
        _poll: poll,
    }
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
impl IngressSession for RumqttcIngress {
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
        let msg = self.handoff_rx.recv_async().await.expect("rumqttc handoff");
        msg.touch();
    }

    async fn wait_barrier(&self, target: usize) {
        if self.after_enqueue && self.qos == BenchQos::AtLeastOnce {
            wait_counter(&self.acked, target, "rumqttc manual acks").await;
        } else {
            wait_counter(&self.handed_off, target, "rumqttc handoffs").await;
        }
    }
}

#[async_trait]
impl OutboundSession for RumqttcOutbound {
    async fn publish_serial(&self, payload: &[u8]) {
        match self.qos {
            BenchQos::AtLeastOnce => {
                let notice = self
                    .client
                    .publish_tracked(OUTBOUND_TOPIC, wire_qos(self.qos), false, payload.to_vec())
                    .await
                    .expect("rumqttc outbound publish");
                notice.wait_completion_async().await.expect("rumqttc outbound PUBACK");
            }
            BenchQos::AtMostOnce => {
                self.client
                    .publish(OUTBOUND_TOPIC, wire_qos(self.qos), false, payload.to_vec())
                    .await
                    .expect("rumqttc qos0 publish");
            }
        }
    }

    async fn publish_pipelined(&self, payload: &[u8], window: usize, n: usize) {
        match self.qos {
            BenchQos::AtLeastOnce => {
                let mut in_flight: VecDeque<PublishNotice> = VecDeque::with_capacity(window);
                for _ in 0..n {
                    while in_flight.len() >= window {
                        let notice = in_flight.pop_front().expect("rumqttc pipelined window");
                        notice.wait_completion_async().await.expect("rumqttc pipelined PUBACK");
                    }
                    let notice = self
                        .client
                        .publish_tracked(OUTBOUND_TOPIC, RumqttQoS::AtLeastOnce, false, payload.to_vec())
                        .await
                        .expect("rumqttc pipelined publish");
                    in_flight.push_back(notice);
                }
                while let Some(notice) = in_flight.pop_front() {
                    notice.wait_completion_async().await.expect("rumqttc pipelined drain");
                }
            }
            BenchQos::AtMostOnce => {
                // No PUBACK; still bound concurrency with a join window of fire-and-forget tasks.
                let mut in_flight: VecDeque<JoinHandle<()>> = VecDeque::with_capacity(window);
                for _ in 0..n {
                    while in_flight.len() >= window {
                        in_flight.pop_front().expect("rumqttc qos0 window").await.expect("join");
                    }
                    let client = self.client.clone();
                    let body = payload.to_vec();
                    in_flight.push_back(tokio::spawn(async move {
                        client
                            .publish(OUTBOUND_TOPIC, RumqttQoS::AtMostOnce, false, body)
                            .await
                            .expect("rumqttc qos0 pipelined");
                    }));
                }
                while let Some(handle) = in_flight.pop_front() {
                    handle.await.expect("rumqttc qos0 drain");
                }
            }
        }
    }
}
