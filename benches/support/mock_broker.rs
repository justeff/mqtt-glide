//! Minimal MQTT 5 broker for benches (production V5 codec on the wire).
//!
//! Bench parent spawns this as a child process (`MQTT_COMPARE_ROLE=mock_broker`) so
//! `getrusage(RUSAGE_SELF)` excludes broker encode/decode/fanout.
//!
//! Protocol needs: CONNACK (`receive_max`), SUBACK, QoS1 PUBACK, fanout of
//! every inbound PUBLISH to all subscribed connections at the lower of the publish and
//! subscription QoS (no topic-filter matching —
//! ingress uses one exact topic per session; outbound has no subscribers). QoS 1 deliveries
//! respect the subscriber's CONNECT Receive Maximum; the rest wait in order until PUBACKs free
//! the window.
//!
//! A second TCP **stats** port exposes `RESET` / `GET` / `WAIT <n>` for the inbound
//! PUBLISH counter used as the outbound wire-load stopwatch (`RECEIVED` / `TIMEOUT`).
//!
//! Performance knobs (ceiling attribution, not a production broker):
//! - multi-thread Tokio runtime
//! - `TCP_NODELAY`
//! - subscriber list under a short `std` mutex; fanout sends `Arc` payloads outside the lock

use std::collections::VecDeque;
use std::io::{BufRead, Write as _};
use std::num::NonZeroU16;
use std::process::Child;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};
use std::time::Duration;

use arc_swap::ArcSwap;
use bytes::BytesMut;
use futures_util::{FutureExt, SinkExt, StreamExt};
use mqtt_glide::codec::{
    ConnectAck, DecodedFrame, Packet, PublishAck, PublishDelivery, PublishPacket, SubscribeAck, V5Codec, encode_publish,
};
use mqtt_glide::{
    ConnectAckReason, DEFAULT_MAX_PACKET_SIZE, PublishAckReason, PublishMessage, QoS, SubscribeAckReason,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, mpsc};
use tokio_util::codec::{FramedRead, FramedWrite};

type FramedWr = FramedWrite<tokio::net::tcp::OwnedWriteHalf, mqtt_glide::codec::V5Codec>;

/// Ready line printed to stdout by the child (`MQTT_COMPARE_ROLE=mock_broker`).
///
/// Format: `MQTT_COMPARE_MOCK_BROKER_READY port=<mqtt> stats=<stats>`.
pub const READY_PREFIX: &str = "MQTT_COMPARE_MOCK_BROKER_READY ";

/// How long the parent waits for [`READY_PREFIX`] before killing the child.
pub const READY_TIMEOUT: Duration = Duration::from_secs(10);

/// How long outbound wire waits for the mock publish counter to reach the burst size.
pub const WIRE_COUNT_TIMEOUT: Duration = Duration::from_secs(60);

/// Parsed ready line: MQTT listen port + stats control port.
#[derive(Debug, Clone, Copy)]
pub struct MockBrokerReady {
    /// MQTT TCP port.
    pub mqtt_port: u16,
    /// Line-oriented stats port (`RESET` / `GET` / `WAIT <n>`).
    pub stats_port: u16,
}

/// Fanout target: any connection that completed SUBSCRIBE.
#[derive(Clone)]
struct Subscriber {
    conn_id: u16,
    sender: mpsc::UnboundedSender<(Arc<PublishMessage>, QoS)>,
}

struct BrokerState {
    /// Read lock-free on every PUBLISH; replaced on (un)subscribe.
    subscribers: ArcSwap<Vec<Subscriber>>,
    next_conn_id: AtomicU16,
    /// Inbound PUBLISH frames decoded (QoS 0 and 1), for wire-load stopwatches.
    publishes_received: AtomicU64,
    /// Wakes `WAIT` stats clients when the counter advances.
    publish_notify: Notify,
}

impl BrokerState {
    fn new() -> Self {
        Self {
            subscribers: ArcSwap::from_pointee(Vec::new()),
            next_conn_id: AtomicU16::new(1),
            publishes_received: AtomicU64::new(0),
            publish_notify: Notify::new(),
        }
    }

    /// Counts `count` decoded PUBLISHes at once: `notify_waiters` takes a lock, so it runs once
    /// per batch of frames, not per message.
    fn note_publishes_received(&self, count: u64) {
        self.publishes_received.fetch_add(count, Ordering::Relaxed);
        self.publish_notify.notify_waiters();
    }

    fn publish_count(&self) -> u64 {
        self.publishes_received.load(Ordering::Relaxed)
    }

    fn reset_publish_count(&self) {
        self.publishes_received.store(0, Ordering::Relaxed);
        // Wake waiters so they re-check (e.g. WAIT after RESET).
        self.publish_notify.notify_waiters();
    }

    fn next_conn_id(&self) -> u16 {
        self.next_conn_id.fetch_add(1, Ordering::Relaxed)
    }

    fn register_subscriber(&self, conn_id: u16, sender: mpsc::UnboundedSender<(Arc<PublishMessage>, QoS)>) {
        let subscriber = Subscriber { conn_id, sender };
        self.subscribers.rcu(|subs| {
            let mut subs = Vec::clone(subs);
            subs.push(subscriber.clone());
            subs
        });
    }

    fn unregister_conn(&self, conn_id: u16) {
        self.subscribers.rcu(|subs| {
            let mut subs = Vec::clone(subs);
            subs.retain(|sub| sub.conn_id != conn_id);
            subs
        });
    }

    /// Forward to every subscribed connection (no topic matching), sharing one [`Arc`] payload.
    fn fanout(&self, message: PublishMessage, qos: QoS) {
        let subs = self.subscribers.load();
        if subs.is_empty() {
            return;
        }
        let shared = Arc::new(message);
        for sub in subs.iter() {
            let _ = sub.sender.send((Arc::clone(&shared), qos));
        }
    }
}

/// Runs the mock MQTT accept loop until the listener fails (or the process is killed).
async fn run_mock_broker(listener: TcpListener, state: Arc<BrokerState>, receive_max: u16) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            break;
        };
        let conn_id = state.next_conn_id();
        let conn_state = Arc::clone(&state);
        tokio::spawn(async move {
            if let Err(error) = serve_connection(stream, conn_state, conn_id, receive_max).await {
                eprintln!("mock broker connection {conn_id} ended: {error}");
            }
        });
    }
}

/// Line protocol on the stats port:
/// - `RESET` → `OK`
/// - `GET` → decimal count
/// - `WAIT <n>` → `RECEIVED <count>` when count ≥ n, else `TIMEOUT <count>` after [`WIRE_COUNT_TIMEOUT`]
async fn run_stats_server(listener: TcpListener, state: Arc<BrokerState>) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            break;
        };
        let conn_state = Arc::clone(&state);
        tokio::spawn(async move {
            if let Err(error) = serve_stats_connection(stream, conn_state).await {
                eprintln!("mock broker stats connection ended: {error}");
            }
        });
    }
}

async fn serve_stats_connection(stream: TcpStream, state: Arc<BrokerState>) -> Result<(), &'static str> {
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();
    while let Some(line) = lines.next_line().await.map_err(|_| "stats read")? {
        let cmd = line.trim();
        if cmd.is_empty() {
            continue;
        }
        if cmd == "RESET" {
            state.reset_publish_count();
            writer.write_all(b"OK\n").await.map_err(|_| "stats write")?;
            continue;
        }
        if cmd == "GET" {
            let body = format!("{}\n", state.publish_count());
            writer.write_all(body.as_bytes()).await.map_err(|_| "stats write")?;
            continue;
        }
        if let Some(target_str) = cmd.strip_prefix("WAIT ") {
            let target: u64 = target_str.trim().parse().map_err(|_| "stats WAIT target")?;
            let reply = wait_for_publishes(&state, target).await;
            writer.write_all(reply.as_bytes()).await.map_err(|_| "stats write")?;
            continue;
        }
        writer.write_all(b"ERR\n").await.map_err(|_| "stats write")?;
    }
    Ok(())
}

/// Blocks until `publishes_received >= target` or [`WIRE_COUNT_TIMEOUT`].
async fn wait_for_publishes(state: &BrokerState, target: u64) -> String {
    let deadline = tokio::time::Instant::now() + WIRE_COUNT_TIMEOUT;
    loop {
        // Register interest before reading the counter (Notify race).
        let notified = state.publish_notify.notified();
        let count = state.publish_count();
        if count >= target {
            return format!("RECEIVED {count}\n");
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return format!("TIMEOUT {count}\n");
        }
        tokio::select! {
            () = notified => {}
            () = tokio::time::sleep_until(deadline) => {
                let count = state.publish_count();
                if count >= target {
                    return format!("RECEIVED {count}\n");
                }
                return format!("TIMEOUT {count}\n");
            }
        }
    }
}

/// Outcome of [`MockStatsPort::wait_received`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireWaitOutcome {
    /// Mock reports it has seen at least `target` PUBLISH frames (`count` may equal target).
    Received {
        /// Counter value when the mock replied.
        count: u64,
    },
    /// [`WIRE_COUNT_TIMEOUT`] elapsed before the target was reached.
    Timeout {
        /// Counter value at timeout.
        count: u64,
    },
}

/// Parent-side handle for the mock stats port (wire-load counter).
#[derive(Debug, Clone, Copy)]
pub struct MockStatsPort {
    port: u16,
}

impl MockStatsPort {
    /// Creates a handle for `127.0.0.1:port`.
    pub fn new(port: u16) -> Self {
        Self { port }
    }

    /// Opens a long-lived stats connection (reuse across many RESET/WAIT calls).
    pub async fn connect(self) -> MockStatsSession {
        let stream = TcpStream::connect(("127.0.0.1", self.port))
            .await
            .expect("connect mock stats");
        let (reader, writer) = stream.into_split();
        MockStatsSession {
            writer,
            lines: BufReader::new(reader).lines(),
        }
    }

    /// Zeros the inbound PUBLISH counter (one-shot connection).
    pub async fn reset(self) {
        self.connect().await.reset().await;
    }

    /// Waits until the mock reports it received exactly `target` PUBLISH frames (one-shot).
    pub async fn wait_exact(self, target: u64) -> Result<u64, String> {
        self.connect().await.wait_exact(target).await
    }
}

/// Persistent stats TCP session — prefer this for latency loops (avoids ephemeral-port exhaustion).
pub struct MockStatsSession {
    writer: tokio::net::tcp::OwnedWriteHalf,
    lines: tokio::io::Lines<BufReader<tokio::net::tcp::OwnedReadHalf>>,
}

impl MockStatsSession {
    /// Zeros the inbound PUBLISH counter.
    pub async fn reset(&mut self) {
        self.writer.write_all(b"RESET\n").await.expect("stats RESET");
        let line = self
            .lines
            .next_line()
            .await
            .expect("stats RESET read")
            .expect("stats RESET eof");
        assert_eq!(line.trim(), "OK", "mock stats RESET reply: {line:?}");
    }

    /// Asks the mock to wait until it has received at least `target` PUBLISH frames.
    pub async fn wait_received(&mut self, target: u64) -> WireWaitOutcome {
        let request = format!("WAIT {target}\n");
        self.writer.write_all(request.as_bytes()).await.expect("stats WAIT");
        let line = self
            .lines
            .next_line()
            .await
            .expect("stats WAIT read")
            .expect("stats WAIT eof");
        let line = line.trim();
        if let Some(count_str) = line.strip_prefix("RECEIVED ") {
            let count: u64 = count_str
                .parse()
                .unwrap_or_else(|_| panic!("mock stats RECEIVED reply: {line:?}"));
            return WireWaitOutcome::Received { count };
        }
        if let Some(count_str) = line.strip_prefix("TIMEOUT ") {
            let count: u64 = count_str
                .parse()
                .unwrap_or_else(|_| panic!("mock stats TIMEOUT reply: {line:?}"));
            return WireWaitOutcome::Timeout { count };
        }
        panic!("mock stats WAIT reply: {line:?}");
    }

    /// Waits until the mock reports it received exactly `target` PUBLISH frames.
    pub async fn wait_exact(&mut self, target: u64) -> Result<u64, String> {
        match self.wait_received(target).await {
            WireWaitOutcome::Received { count } if count == target => Ok(count),
            WireWaitOutcome::Received { count } => Err(format!(
                "mock received {count} PUBLISH frames, expected exactly {target} (extras/lost)"
            )),
            WireWaitOutcome::Timeout { count } => Err(format!(
                "mock TIMEOUT after {WIRE_COUNT_TIMEOUT:?}: received {count}/{target} PUBLISH frames"
            )),
        }
    }
}

/// Child-process entry: bind MQTT + stats, print ready line, run until killed.
///
/// Env: optional `MQTT_COMPARE_RECEIVE_MAX` (default [`crate::compare::fixtures::RECEIVE_MAXIMUM`]).
pub fn run_mock_broker_process() {
    let receive_max: u16 = std::env::var("MQTT_COMPARE_RECEIVE_MAX")
        .unwrap_or_else(|_| crate::compare::fixtures::RECEIVE_MAXIMUM.get().to_string())
        .parse()
        .expect("MQTT_COMPARE_RECEIVE_MAX u16");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("mock-broker")
        .build()
        .expect("mock broker runtime");
    rt.block_on(async move {
        let mqtt_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock broker mqtt");
        let stats_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock broker stats");
        let mqtt_port = mqtt_listener.local_addr().expect("mqtt local addr").port();
        let stats_port = stats_listener.local_addr().expect("stats local addr").port();
        println!("{READY_PREFIX}port={mqtt_port} stats={stats_port}");
        std::io::stdout().flush().expect("flush mock broker ready line");
        let state = Arc::new(BrokerState::new());
        let stats_state = Arc::clone(&state);
        tokio::spawn(async move {
            run_stats_server(stats_listener, stats_state).await;
        });
        run_mock_broker(mqtt_listener, state, receive_max).await;
    });
}

/// Parses the ready line emitted by [`run_mock_broker_process`].
pub fn parse_ready_line(line: &str) -> MockBrokerReady {
    let line = line.trim();
    let rest = line
        .strip_prefix(READY_PREFIX)
        .unwrap_or_else(|| panic!("mock broker ready line missing prefix: {line:?}"));
    let mut mqtt_port = None;
    let mut stats_port = None;
    for part in rest.split_whitespace() {
        if let Some(port) = part.strip_prefix("port=") {
            mqtt_port = Some(port.parse().expect("mock broker mqtt port u16"));
        } else if let Some(port) = part.strip_prefix("stats=") {
            stats_port = Some(port.parse().expect("mock broker stats port u16"));
        }
    }
    MockBrokerReady {
        mqtt_port: mqtt_port.unwrap_or_else(|| panic!("mock broker ready missing port=: {line:?}")),
        stats_port: stats_port.unwrap_or_else(|| panic!("mock broker ready missing stats=: {line:?}")),
    }
}

/// Reads the ready line from a spawned mock-broker child stdout.
///
/// On timeout or I/O failure the child is killed and reaped before panicking.
pub fn read_mock_broker_ready(child: &mut Child) -> MockBrokerReady {
    let stdout = child.stdout.take().expect("mock broker child stdout pipe");
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut reader = std::io::BufReader::new(stdout);
        let mut line = String::new();
        let result = reader.read_line(&mut line).map(|_| line);
        let _ = tx.send(result);
    });

    match rx.recv_timeout(READY_TIMEOUT) {
        Ok(Ok(line)) => {
            if line.is_empty() {
                kill_and_reap(child);
                panic!("mock broker exited before ready line");
            }
            parse_ready_line(&line)
        }
        Ok(Err(error)) => {
            kill_and_reap(child);
            panic!("read mock broker ready line: {error}");
        }
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            kill_and_reap(child);
            panic!("mock broker ready timed out after {READY_TIMEOUT:?}");
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            kill_and_reap(child);
            panic!("mock broker ready reader disconnected");
        }
    }
}

fn kill_and_reap(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

async fn serve_connection(
    stream: TcpStream,
    state: Arc<BrokerState>,
    conn_id: u16,
    receive_max: u16,
) -> Result<(), &'static str> {
    stream.set_nodelay(true).map_err(|_| "tcp_nodelay")?;
    let (read_half, write_half) = stream.into_split();
    let mut framed_read = FramedRead::new(read_half, V5Codec::with_max_packet_size(0));
    let mut framed_write = FramedWrite::new(write_half, V5Codec::with_max_packet_size(0));

    let first = framed_read
        .next()
        .await
        .ok_or("client closed before CONNECT")?
        .map_err(|_| "decode CONNECT")?;
    // Absent Receive Maximum means 65 535 (§3.1.2.11.3).
    let client_receive_max = match first {
        (DecodedFrame::Control(Packet::Connect(connect)), _) => connect.receive_max.map_or(u16::MAX, NonZeroU16::get),
        _ => return Err("expected CONNECT"),
    };

    let connack = ConnectAck {
        reason_code: ConnectAckReason::Success,
        receive_max: NonZeroU16::new(receive_max).ok_or("receive_max zero")?,
        ..Default::default()
    };
    framed_write
        .send(Packet::ConnectAck(Box::new(connack)))
        .await
        .map_err(|_| "send CONNACK")?;

    let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel::<(Arc<PublishMessage>, QoS)>();
    let mut subscribed = false;
    let mut delivery = Delivery {
        granted: QoS::AtMostOnce,
        next_packet_id: 1,
        in_flight: 0,
        // Like a broker with its own per-client cap on deliveries in flight (EMQX `max_inflight`).
        receive_max: client_receive_max.min(delivery_inflight_cap()),
        parked: VecDeque::new(),
    };
    loop {
        tokio::select! {
            inbound = framed_read.next() => {
                // Handle every frame already buffered, then write once, as a broker reading a
                // full socket buffer does: one write per batch of PUBACKs to a publisher, and
                // one delivery flush per batch of PUBACKs from a subscriber.
                let mut next = Some(inbound);
                let mut batch = FrameBatch::default();
                let mut handled = 0;
                while let Some(inbound) = next.take() {
                    let frame = match inbound {
                        None => {
                            batch.finish(&mut framed_write, &state, &mut delivery).await?;
                            state.unregister_conn(conn_id);
                            return Ok(());
                        }
                        Some(Err(_)) => return Err("decode client frame"),
                        Some(Ok((frame, _))) => frame,
                    };
                    match frame {
                        DecodedFrame::Publish(publish) => batch.publish(&mut framed_write, &state, publish).await?,
                        DecodedFrame::Control(Packet::PublishAck(_)) => {
                            delivery.in_flight = delivery.in_flight.saturating_sub(1);
                            batch.acked = true;
                        }
                        DecodedFrame::Control(Packet::Subscribe(subscribe)) => {
                            let mut status = Vec::with_capacity(subscribe.topic_filters.len());
                            for (_filter, options) in &subscribe.topic_filters {
                                status.push(granted_qos(options.qos));
                                delivery.granted = delivery.granted.max(delivery_qos(options.qos));
                            }
                            if !subscribed {
                                state.register_subscriber(conn_id, outbound_tx.clone());
                                subscribed = true;
                            }
                            let suback = SubscribeAck {
                                packet_id: subscribe.packet_id,
                                properties: Vec::new(),
                                reason_string: None,
                                status,
                            };
                            framed_write.send(Packet::SubscribeAck(suback)).await.map_err(|_| "send SUBACK")?;
                        }
                        DecodedFrame::Control(Packet::PingRequest) => {
                            framed_write.send(Packet::PingResponse).await.map_err(|_| "send PINGRESP")?;
                        }
                        DecodedFrame::Control(Packet::Disconnect(_)) => {
                            batch.finish(&mut framed_write, &state, &mut delivery).await?;
                            state.unregister_conn(conn_id);
                            return Ok(());
                        }
                        DecodedFrame::Control(_) => {}
                    }
                    handled += 1;
                    if handled < FRAME_BATCH {
                        next = framed_read.next().now_or_never();
                    }
                }
                batch.finish(&mut framed_write, &state, &mut delivery).await?;
            }
            // Parked deliveries wait for PUBACKs; take no more until they are out.
            outbound = outbound_rx.recv(), if delivery.parked.is_empty() => {
                let Some(first) = outbound else { break };
                delivery.parked.push_back(first);
                while let Ok(next) = outbound_rx.try_recv() {
                    delivery.parked.push_back(next);
                }
                delivery.flush(&mut framed_write).await?;
            }
        }
    }

    state.unregister_conn(conn_id);
    Ok(())
}

/// Most frames handled from the read buffer before writing.
const FRAME_BATCH: usize = 64;

/// What one batch of inbound frames leaves to write.
#[derive(Default)]
struct FrameBatch {
    /// PUBLISHes decoded (counted toward the stats port once per batch).
    received: u64,
    /// PUBACKs to publishers fed into the write buffer, not yet flushed.
    pubacks_fed: bool,
    /// The subscriber PUBACKed deliveries, so parked ones may fit its window now.
    acked: bool,
}

impl FrameBatch {
    /// PUBACKs a QoS 1 publish (fed, flushed with the batch) and fans it out.
    async fn publish(
        &mut self,
        framed_write: &mut FramedWr,
        state: &BrokerState,
        publish: PublishPacket,
    ) -> Result<(), &'static str> {
        self.received += 1;
        let qos = delivery_qos(publish.meta.qos);
        if publish.meta.qos == QoS::AtLeastOnce {
            let packet_id = publish.meta.packet_id.ok_or("QoS1 publish without packet id")?;
            let puback = PublishAck {
                packet_id,
                reason_code: PublishAckReason::Success,
                properties: Vec::new(),
                reason_string: None,
            };
            framed_write
                .feed(Packet::PublishAck(puback))
                .await
                .map_err(|_| "send PUBACK to publisher")?;
            self.pubacks_fed = true;
        }
        state.fanout(publish.into_message(), qos);
        Ok(())
    }

    /// Counts, flushes the PUBACKs and sends deliveries that fit the subscriber's window now.
    async fn finish(
        &mut self,
        framed_write: &mut FramedWr,
        state: &BrokerState,
        delivery: &mut Delivery,
    ) -> Result<(), &'static str> {
        if self.received > 0 {
            state.note_publishes_received(self.received);
        }
        // Before deliveries: those bypass the framed write buffer.
        if self.pubacks_fed {
            framed_write.flush().await.map_err(|_| "flush PUBACKs to publisher")?;
        }
        if self.acked {
            delivery.flush(framed_write).await?;
        }
        *self = Self::default();
        Ok(())
    }
}

/// `MQTT_COMPARE_MOCK_DELIVERY_INFLIGHT`: QoS 1 deliveries the mock keeps unacknowledged per
/// subscriber at most, whatever the subscriber's Receive Maximum (default: no cap of its own).
fn delivery_inflight_cap() -> u16 {
    std::env::var("MQTT_COMPARE_MOCK_DELIVERY_INFLIGHT")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|n: &u16| *n >= 1)
        .unwrap_or(u16::MAX)
}

/// Most PUBLISHes a subscriber connection sends in one write.
const OUTBOUND_BATCH: usize = 64;

/// Per-connection delivery state toward a subscriber.
struct Delivery {
    /// Highest QoS granted to the subscription.
    granted: QoS,
    next_packet_id: u16,
    /// QoS 1 deliveries not yet PUBACKed.
    in_flight: u16,
    /// The subscriber's CONNECT Receive Maximum.
    receive_max: u16,
    /// Queued deliveries, in order.
    parked: VecDeque<(Arc<PublishMessage>, QoS)>,
}

impl Delivery {
    /// Sends parked deliveries in batches of [`OUTBOUND_BATCH`] until the queue is empty or the
    /// next QoS 1 delivery would exceed the subscriber's Receive Maximum.
    async fn flush(&mut self, framed_write: &mut FramedWr) -> Result<(), &'static str> {
        loop {
            let mut buf = BytesMut::new();
            let mut batched = 0;
            while batched < OUTBOUND_BATCH {
                // Delivered at the lower of the publish and subscription QoS (§3.8.4).
                let Some(qos) = self.parked.front().map(|(_, qos)| (*qos).min(self.granted)) else {
                    break;
                };
                let packet_id = if qos == QoS::AtMostOnce {
                    None
                } else if self.in_flight >= self.receive_max {
                    break;
                } else {
                    let id = NonZeroU16::new(self.next_packet_id).ok_or("packet id zero")?;
                    self.next_packet_id = self.next_packet_id.checked_add(1).unwrap_or(1);
                    self.in_flight += 1;
                    Some(id)
                };
                let (message, _) = self.parked.pop_front().expect("front checked");
                encode_delivery(message.as_ref(), qos, packet_id, &mut buf)?;
                batched += 1;
            }
            if batched == 0 {
                return Ok(());
            }
            framed_write
                .get_mut()
                .write_all(&buf)
                .await
                .map_err(|_| "write PUBLISH")?;
        }
    }
}

fn encode_delivery(
    message: &PublishMessage,
    qos: QoS,
    packet_id: Option<NonZeroU16>,
    buf: &mut BytesMut,
) -> Result<(), &'static str> {
    let delivery = PublishDelivery {
        dup: false,
        retain: false,
        qos,
        packet_id,
    };
    encode_publish(message, delivery, DEFAULT_MAX_PACKET_SIZE, buf).map_err(|_| "encode PUBLISH")
}

/// QoS the mock delivers at: it never runs PUBREL/PUBCOMP, so QoS 2 becomes QoS 1.
fn delivery_qos(qos: QoS) -> QoS {
    qos.min(QoS::AtLeastOnce)
}

fn granted_qos(qos: QoS) -> SubscribeAckReason {
    match qos {
        QoS::AtMostOnce => SubscribeAckReason::GrantedQos0,
        QoS::AtLeastOnce => SubscribeAckReason::GrantedQos1,
        // Mock never runs PUBREL/PUBCOMP; grant QoS1 so clients stay on the bench path.
        QoS::ExactlyOnce => SubscribeAckReason::GrantedQos1,
    }
}
