//! Minimal MQTT 5 broker for benches (production V5 codec on the wire).
//!
//! Bench parent spawns this as a child process (`MQTT_COMPARE_ROLE=mock_broker`) so
//! `getrusage(RUSAGE_SELF)` excludes broker encode/decode/fanout.
//!
//! Protocol needs: CONNACK (`receive_max`), SUBACK, QoS1 PUBACK, fanout of
//! every inbound PUBLISH to all subscribed connections (no topic-filter matching —
//! ingress uses one exact topic per session; outbound has no subscribers).
//!
//! A second TCP **stats** port exposes `RESET` / `GET` / `WAIT <n>` for the inbound
//! PUBLISH counter used as the outbound wire-load stopwatch (`RECEIVED` / `TIMEOUT`).
//!
//! Performance knobs (ceiling attribution, not a production broker):
//! - multi-thread Tokio runtime
//! - `TCP_NODELAY`
//! - subscriber list under a short `std` mutex; fanout sends `Arc` payloads outside the lock

use std::io::{BufRead, Write as _};
use std::num::NonZeroU16;
use std::process::Child;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::BytesMut;
use futures_util::{SinkExt, StreamExt};
use mqtt_glide::{
    ConnectAck, ConnectAckReason, DEFAULT_MAX_PACKET_SIZE, DecodedFrame, Packet, PublishAck, PublishAckReason,
    PublishDelivery, PublishMessage, PublishPacket, QoS, SubscribeAck, SubscribeAckReason, V5Codec, encode_publish,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, mpsc};
use tokio_util::codec::{FramedRead, FramedWrite};

type FramedWr = FramedWrite<tokio::net::tcp::OwnedWriteHalf, mqtt_glide::V5Codec>;

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
struct Subscriber {
    conn_id: u16,
    sender: mpsc::UnboundedSender<Arc<PublishMessage>>,
}

struct BrokerState {
    subscribers: Mutex<Vec<Subscriber>>,
    next_conn_id: AtomicU16,
    /// Inbound PUBLISH frames decoded (QoS 0 and 1), for wire-load stopwatches.
    publishes_received: AtomicU64,
    /// Wakes `WAIT` stats clients when the counter advances.
    publish_notify: Notify,
}

impl BrokerState {
    fn new() -> Self {
        Self {
            subscribers: Mutex::new(Vec::new()),
            next_conn_id: AtomicU16::new(1),
            publishes_received: AtomicU64::new(0),
            publish_notify: Notify::new(),
        }
    }

    fn note_publish_received(&self) {
        self.publishes_received.fetch_add(1, Ordering::Relaxed);
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

    fn register_subscriber(&self, conn_id: u16, sender: mpsc::UnboundedSender<Arc<PublishMessage>>) {
        self.subscribers
            .lock()
            .expect("mock broker subscriber lock")
            .push(Subscriber { conn_id, sender });
    }

    fn unregister_conn(&self, conn_id: u16) {
        self.subscribers
            .lock()
            .expect("mock broker subscriber lock")
            .retain(|sub| sub.conn_id != conn_id);
    }

    /// Forward to every subscribed connection (no topic matching).
    ///
    /// Snapshot senders under a short lock, then send an [`Arc`] shared payload.
    fn fanout(&self, message: PublishMessage) {
        let senders: Vec<mpsc::UnboundedSender<Arc<PublishMessage>>> = {
            let subs = self.subscribers.lock().expect("mock broker subscriber lock");
            if subs.is_empty() {
                return;
            }
            subs.iter().map(|sub| sub.sender.clone()).collect()
        };
        let shared = Arc::new(message);
        for sender in senders {
            let _ = sender.send(Arc::clone(&shared));
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
    match first {
        (DecodedFrame::Control(Packet::Connect(_)), _) => {}
        _ => return Err("expected CONNECT"),
    }

    let connack = ConnectAck {
        reason_code: ConnectAckReason::Success,
        receive_max: NonZeroU16::new(receive_max).ok_or("receive_max zero")?,
        ..Default::default()
    };
    framed_write
        .send(Packet::ConnectAck(Box::new(connack)))
        .await
        .map_err(|_| "send CONNACK")?;

    let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel::<Arc<PublishMessage>>();
    let mut subscribed = false;
    let mut next_packet_id = 1u16;
    loop {
        tokio::select! {
            inbound = framed_read.next() => {
                match inbound {
                    None => break,
                    Some(Err(_)) => return Err("decode client frame"),
                    Some(Ok((DecodedFrame::Publish(publish), _))) => {
                        handle_inbound_publish(&mut framed_write, &state, publish).await?;
                    }
                    Some(Ok((DecodedFrame::Control(packet), _))) => {
                        match packet {
                            Packet::Subscribe(subscribe) => {
                                let mut status = Vec::with_capacity(subscribe.topic_filters.len());
                                for (_filter, options) in &subscribe.topic_filters {
                                    status.push(granted_qos(options.qos));
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
                            Packet::PublishAck(_) => {
                                // Subscriber QoS1 ack — tolerate without tracking.
                            }
                            Packet::PingRequest => {
                                framed_write.send(Packet::PingResponse).await.map_err(|_| "send PINGRESP")?;
                            }
                            Packet::Disconnect(_) => break,
                            _ => {}
                        }
                    }
                }
            }
            outbound = outbound_rx.recv() => {
                let Some(message) = outbound else { break };
                let id = NonZeroU16::new(next_packet_id).ok_or("packet id zero")?;
                next_packet_id = if next_packet_id == u16::MAX { 1 } else { next_packet_id + 1 };
                send_publish(&mut framed_write, message.as_ref(), QoS::AtLeastOnce, Some(id)).await?;
            }
        }
    }

    state.unregister_conn(conn_id);
    Ok(())
}

async fn handle_inbound_publish(
    framed_write: &mut FramedWr,
    state: &BrokerState,
    publish: PublishPacket,
) -> Result<(), &'static str> {
    state.note_publish_received();
    // PUBACK first so publisher RTT is not blocked on fanout work.
    if publish.meta.qos == QoS::AtLeastOnce {
        let packet_id = publish.meta.packet_id.ok_or("QoS1 publish without packet id")?;
        let puback = PublishAck {
            packet_id,
            reason_code: PublishAckReason::Success,
            properties: Vec::new(),
            reason_string: None,
        };
        framed_write
            .send(Packet::PublishAck(puback))
            .await
            .map_err(|_| "send PUBACK to publisher")?;
    }

    state.fanout(publish.into_message());
    Ok(())
}

async fn send_publish(
    framed_write: &mut FramedWr,
    message: &PublishMessage,
    qos: QoS,
    packet_id: Option<NonZeroU16>,
) -> Result<(), &'static str> {
    let delivery = PublishDelivery {
        dup: false,
        retain: false,
        qos,
        packet_id,
    };
    let mut buf = BytesMut::new();
    encode_publish(message, delivery, DEFAULT_MAX_PACKET_SIZE, &mut buf).map_err(|_| "encode PUBLISH")?;
    // Direct socket write (codec buffer unused for this path); nodelay already set.
    framed_write
        .get_mut()
        .write_all(&buf)
        .await
        .map_err(|_| "write PUBLISH")?;
    Ok(())
}

fn granted_qos(qos: QoS) -> SubscribeAckReason {
    match qos {
        QoS::AtMostOnce => SubscribeAckReason::GrantedQos0,
        QoS::AtLeastOnce => SubscribeAckReason::GrantedQos1,
        // Mock never runs PUBREL/PUBCOMP; grant QoS1 so clients stay on the bench path.
        QoS::ExactlyOnce => SubscribeAckReason::GrantedQos1,
    }
}
