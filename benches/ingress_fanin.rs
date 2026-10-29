//! Multi-broker ingress fan-in: N mqtt-glide clients, each on its own broker, feed one handoff
//! that M consumer tasks drain (mqtt-glide only, mock brokers).
//!
//! Compares the ways an application can hand inbound messages to a worker pool:
//!
//! | sink            | consumers | how |
//! |-----------------|-----------|-----|
//! | `mpsc`          | 1         | `tokio::sync::mpsc` (single consumer) |
//! | `flume`         | M         | `flume` MPMC channel (mutex inside) |
//! | `async_channel` | M         | `async-channel` MPMC channel (lock-free queue) |
//! | `shard`         | M         | one `tokio::sync::mpsc` per consumer, the sink round-robins (no per-topic order) |
//! | `inline`        | N         | work runs in `on_inbound` on each connection's read task |
//!
//! Every consumer touches the message and spins `work_ns` per message to stand in for
//! application work. Reported per cell: msg/s, process CPU per message and the clients'
//! handoff stalls (`ClientStats::inbound.handoff_stalls`).
//!
//! ```shell
//! cargo bench --features __bench,stats --bench ingress_fanin
//! ```
//!
//! Knobs: `FANIN_BROKERS` (default `1,2,4`), `FANIN_CONSUMERS` (default `1,2,4`), `FANIN_SINKS`
//! (default all), `FANIN_WORK_NS` (default `0,1000`), `FANIN_MSGS` (total per run, default
//! 2000000), `FANIN_PAYLOAD` (default 64), plus `MQTT_COMPARE_QOS`, `MQTT_COMPARE_RUNS`,
//! `MQTT_COMPARE_CLIENT_RM`, `MQTT_COMPARE_OUT` and the CPU pinning variables of `mqtt_compare`.
//! Results go to `<out>/fanin.jsonl` and `<out>/meta.json` (default `target/ingress_fanin/<ts>`).

#![allow(dead_code, unused_imports)]

mod compare;
mod support;

use std::fs;
use std::io::Write;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mqtt_glide::{
    AuthError, BrokerUrl, ClientId as GlideClientId, ConnectOptions, ConnectionOutcome, CredentialsProvider,
    InboundMessage, InboundSink, InboundSinkClosed, InboundTrySendError, MqttClient, NoopLifecycle, OnInboundSink,
    Password, QoS, ReconnectPolicy, SessionParams, SessionPhase, connect,
};
use serde_json::json;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use compare::fixtures::{
    GLIDE_VERSION, HANDOFF_CAPACITY, KEEP_ALIVE, REQUEST_CAPACITY, bench_runs, child_cpus, client_receive_maximum,
    ingress_window, next_id, parse_qos_list,
};
use compare::harness::BenchQos;
use compare::pubchild::{spawn_publisher_child, wait_child};
use support::broker::{BenchBroker, start_mock_broker};
use support::cpu::process_cpu_time;
use support::handoff::touch_publish;

/// Messages each broker delivers before the measured runs of a cell.
const WARMUP_PER_BROKER: usize = 20_000;

/// Upper bound for one run; a hang fails the bench instead of blocking it.
const RUN_TIMEOUT: Duration = Duration::from_secs(120);

const TOPIC_PREFIX: &str = "bench/fanin";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SinkKind {
    Mpsc,
    Flume,
    AsyncChannel,
    Shard,
    Inline,
}

impl SinkKind {
    const ALL: [Self; 5] = [Self::Mpsc, Self::Flume, Self::AsyncChannel, Self::Shard, Self::Inline];

    fn as_str(self) -> &'static str {
        match self {
            Self::Mpsc => "mpsc",
            Self::Flume => "flume",
            Self::AsyncChannel => "async_channel",
            Self::Shard => "shard",
            Self::Inline => "inline",
        }
    }

    fn parse(value: &str) -> Self {
        Self::ALL
            .into_iter()
            .find(|kind| kind.as_str() == value)
            .unwrap_or_else(|| panic!("unknown FANIN_SINKS entry {value:?}"))
    }
}

#[derive(Debug, Clone, Copy)]
struct Cell {
    sink: SinkKind,
    brokers: usize,
    /// Consumer tasks; for `inline` the number of read tasks running the handler.
    consumers: usize,
    work_ns: u64,
    qos: BenchQos,
    payload: usize,
    messages: usize,
}

#[derive(Debug, Clone, Copy)]
struct Sample {
    msg_s: f64,
    cpu_ns_per_msg: f64,
    stalls_per_kmsg: f64,
    stall_ms: f64,
}

fn env_list(var: &str, default: &str) -> Vec<String> {
    std::env::var(var)
        .unwrap_or_else(|_| default.to_owned())
        .split(',')
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .collect()
}

fn env_usizes(var: &str, default: &str) -> Vec<usize> {
    env_list(var, default)
        .iter()
        .map(|v| v.parse().unwrap_or_else(|_| panic!("{var}: invalid number {v:?}")))
        .collect()
}

fn env_usize(var: &str, default: usize) -> usize {
    std::env::var(var).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn build_cells() -> Vec<Cell> {
    let brokers = env_usizes("FANIN_BROKERS", "1,2,4");
    let consumers = env_usizes("FANIN_CONSUMERS", "1,2,4");
    let sinks: Vec<SinkKind> = env_list("FANIN_SINKS", "mpsc,flume,async_channel,shard,inline")
        .iter()
        .map(|s| SinkKind::parse(s))
        .collect();
    let works = env_usizes("FANIN_WORK_NS", "0,1000");
    let messages = env_usize("FANIN_MSGS", 2_000_000);
    let payload = env_usize("FANIN_PAYLOAD", 64);
    let mut cells = Vec::new();
    for qos in parse_qos_list() {
        for &work in &works {
            for &n in &brokers {
                for &sink in &sinks {
                    let consumer_counts: Vec<usize> = match sink {
                        SinkKind::Mpsc => vec![1],
                        SinkKind::Inline => vec![n],
                        _ => consumers.clone(),
                    };
                    for m in consumer_counts {
                        cells.push(Cell {
                            sink,
                            brokers: n,
                            consumers: m,
                            work_ns: work as u64,
                            qos,
                            payload,
                            messages,
                        });
                    }
                }
            }
        }
    }
    cells
}

/// Per-consumer progress on its own cache line, so counting adds no shared write.
#[repr(align(128))]
#[derive(Default)]
struct Slot(AtomicU64);

#[derive(Default)]
struct Progress {
    slots: Vec<Slot>,
}

impl Progress {
    fn new(consumers: usize) -> Arc<Self> {
        Arc::new(Self {
            slots: (0..consumers).map(|_| Slot::default()).collect(),
        })
    }

    fn total(&self) -> u64 {
        self.slots.iter().map(|slot| slot.0.load(Ordering::Relaxed)).sum()
    }
}

/// What every consumer does with a message.
struct Consumer {
    progress: Arc<Progress>,
    index: usize,
    count: u64,
    work_ns: u64,
}

impl Consumer {
    fn handle(&mut self, message: &InboundMessage) {
        touch_publish(message);
        if self.work_ns > 0 {
            let start = Instant::now();
            while start.elapsed().as_nanos() < u128::from(self.work_ns) {
                std::hint::spin_loop();
            }
        }
        self.count += 1;
        self.progress.slots[self.index].0.store(self.count, Ordering::Relaxed);
    }
}

/// One `mpsc` per consumer. `try_send` starts at the next shard round-robin and tries each
/// once, so a single slow consumer does not stall the connection while others have room.
struct ShardSink {
    shards: Arc<[mpsc::Sender<InboundMessage>]>,
    next: AtomicUsize,
}

impl Clone for ShardSink {
    fn clone(&self) -> Self {
        Self {
            shards: Arc::clone(&self.shards),
            next: AtomicUsize::new(self.next.load(Ordering::Relaxed)),
        }
    }
}

impl ShardSink {
    fn advance(&self) -> usize {
        let index = self.next.load(Ordering::Relaxed);
        self.next.store((index + 1) % self.shards.len(), Ordering::Relaxed);
        index
    }
}

impl InboundSink for ShardSink {
    fn is_closed(&self) -> bool {
        self.shards.iter().all(mpsc::Sender::is_closed)
    }

    fn try_send(&self, mut message: InboundMessage) -> Result<(), InboundTrySendError> {
        let mut closed = 0;
        for _ in 0..self.shards.len() {
            match self.shards[self.advance()].try_send(message) {
                Ok(()) => return Ok(()),
                Err(mpsc::error::TrySendError::Full(m)) => message = m,
                Err(mpsc::error::TrySendError::Closed(m)) => {
                    closed += 1;
                    message = m;
                }
            }
        }
        if closed == self.shards.len() {
            Err(InboundTrySendError::Closed(message))
        } else {
            Err(InboundTrySendError::Full(message))
        }
    }

    fn send(&self, message: InboundMessage) -> impl Future<Output = Result<(), InboundSinkClosed>> + Send {
        let sender = self.shards[self.advance()].clone();
        async move { sender.send(message).await.map_err(|_| InboundSinkClosed) }
    }
}

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

struct Session {
    client: MqttClient,
    run: JoinHandle<Result<(), ConnectionOutcome>>,
    topic: String,
    port: u16,
}

fn wire_qos(qos: BenchQos) -> QoS {
    match qos {
        BenchQos::AtMostOnce => QoS::AtMostOnce,
        BenchQos::AtLeastOnce => QoS::AtLeastOnce,
    }
}

async fn start_session<H: InboundSink>(port: u16, index: usize, qos: BenchQos, sink: H) -> Session {
    let topic = format!("{TOPIC_PREFIX}/{index}");
    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("broker url");
    let options = ConnectOptions::new(url)
        .client_id(GlideClientId::from_prefix(&next_id("fanin")))
        .keep_alive(KEEP_ALIVE)
        .receive_maximum(client_receive_maximum())
        .command_channel_capacity(NonZeroUsize::new(REQUEST_CAPACITY).expect("non-zero"));
    let (client, connection, mut phase) =
        connect(options, BenchCredentials, NoReconnect, sink, NoopLifecycle).expect("glide connect");
    let run = tokio::spawn(connection.run());
    tokio::time::timeout(
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
    client
        .subscribe([(topic.as_str(), wire_qos(qos))])
        .await
        .expect("glide subscribe");
    Session {
        client,
        run,
        topic,
        port,
    }
}

/// Publishes `per_broker` messages on every broker and waits until the consumers saw them.
///
/// The clock starts at the first consumed message, so publisher process start-up is not
/// measured; messages consumed before that are subtracted.
async fn burst(sessions: &[Session], progress: &Progress, cell: &Cell, per_broker: usize) -> (u64, Duration) {
    let base = progress.total();
    let target = base + (per_broker * sessions.len()) as u64;
    let children: Vec<_> = sessions
        .iter()
        .map(|s| {
            spawn_publisher_child(
                s.port,
                per_broker,
                &next_id("fanin-pub"),
                &s.topic,
                cell.payload,
                cell.qos,
                None,
            )
        })
        .collect();
    let deadline = Instant::now() + RUN_TIMEOUT;
    let first = loop {
        let seen = progress.total();
        if seen > base {
            break seen;
        }
        assert!(Instant::now() < deadline, "no message arrived");
        tokio::task::yield_now().await;
    };
    let start = Instant::now();
    loop {
        let seen = progress.total();
        if seen >= target {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timeout: consumed {} of {}",
            seen - base,
            target - base
        );
        tokio::time::sleep(Duration::from_micros(200)).await;
    }
    let elapsed = start.elapsed();
    for child in children {
        wait_child(child);
    }
    (target - first, elapsed)
}

fn stall_totals(sessions: &[Session]) -> (u64, Duration) {
    sessions.iter().fold((0, Duration::ZERO), |(n, t), s| {
        let inbound = s.client.stats().inbound;
        (n + inbound.handoff_stalls, t + inbound.handoff_stall_time)
    })
}

async fn measure(sessions: &[Session], progress: &Progress, cell: &Cell, runs: usize) -> Vec<Sample> {
    burst(sessions, progress, cell, WARMUP_PER_BROKER).await;
    let per_broker = cell.messages / cell.brokers;
    let total = (per_broker * cell.brokers) as f64;
    let mut samples = Vec::with_capacity(runs);
    for _ in 0..runs {
        let (stalls0, stall_time0) = stall_totals(sessions);
        let cpu0 = process_cpu_time();
        let (counted, elapsed) = burst(sessions, progress, cell, per_broker).await;
        let cpu = process_cpu_time() - cpu0;
        let (stalls1, stall_time1) = stall_totals(sessions);
        samples.push(Sample {
            msg_s: counted as f64 / elapsed.as_secs_f64(),
            cpu_ns_per_msg: cpu.as_nanos() as f64 / total,
            stalls_per_kmsg: (stalls1 - stalls0) as f64 * 1000.0 / total,
            stall_ms: (stall_time1 - stall_time0).as_secs_f64() * 1000.0,
        });
    }
    samples
}

async fn shutdown(sessions: Vec<Session>) {
    for session in sessions {
        let _ = tokio::time::timeout(Duration::from_secs(5), session.client.shutdown()).await;
        session.run.abort();
    }
}

async fn run_channel_cell<H: InboundSink>(
    cell: &Cell,
    brokers: &[BenchBroker],
    sink: H,
    consumers: Vec<JoinHandle<()>>,
    progress: &Progress,
    runs: usize,
) -> Vec<Sample> {
    let mut sessions = Vec::with_capacity(brokers.len());
    for (index, broker) in brokers.iter().enumerate() {
        sessions.push(start_session(broker.port(), index, cell.qos, sink.clone()).await);
    }
    drop(sink);
    let samples = measure(&sessions, progress, cell, runs).await;
    shutdown(sessions).await;
    for consumer in consumers {
        let _ = tokio::time::timeout(Duration::from_secs(5), consumer).await;
    }
    samples
}

async fn run_cell(cell: &Cell, runs: usize) -> Vec<Sample> {
    let brokers: Vec<BenchBroker> = (0..cell.brokers).map(|_| start_mock_broker()).collect();
    let progress = Progress::new(cell.consumers);
    let consumer = |index: usize| Consumer {
        progress: Arc::clone(&progress),
        index,
        count: 0,
        work_ns: cell.work_ns,
    };
    match cell.sink {
        SinkKind::Mpsc => {
            let (tx, mut rx) = mpsc::channel(HANDOFF_CAPACITY);
            let mut c = consumer(0);
            let task = tokio::spawn(async move {
                while let Some(m) = rx.recv().await {
                    c.handle(&m);
                }
            });
            run_channel_cell(cell, &brokers, tx, vec![task], &progress, runs).await
        }
        SinkKind::Flume => {
            let (tx, rx) = flume::bounded(HANDOFF_CAPACITY);
            let tasks = (0..cell.consumers)
                .map(|i| {
                    let rx = rx.clone();
                    let mut c = consumer(i);
                    tokio::spawn(async move {
                        while let Ok(m) = rx.recv_async().await {
                            c.handle(&m);
                        }
                    })
                })
                .collect();
            drop(rx);
            run_channel_cell(cell, &brokers, tx, tasks, &progress, runs).await
        }
        SinkKind::AsyncChannel => {
            let (tx, rx) = async_channel::bounded(HANDOFF_CAPACITY);
            let tasks = (0..cell.consumers)
                .map(|i| {
                    let rx = rx.clone();
                    let mut c = consumer(i);
                    tokio::spawn(async move {
                        while let Ok(m) = rx.recv().await {
                            c.handle(&m);
                        }
                    })
                })
                .collect();
            drop(rx);
            run_channel_cell(cell, &brokers, tx, tasks, &progress, runs).await
        }
        SinkKind::Shard => {
            // Same total buffer as the single channels.
            let per_shard = (HANDOFF_CAPACITY / cell.consumers).max(1);
            let mut shards = Vec::with_capacity(cell.consumers);
            let mut tasks = Vec::with_capacity(cell.consumers);
            for i in 0..cell.consumers {
                let (tx, mut rx) = mpsc::channel(per_shard);
                shards.push(tx);
                let mut c = consumer(i);
                tasks.push(tokio::spawn(async move {
                    while let Some(m) = rx.recv().await {
                        c.handle(&m);
                    }
                }));
            }
            let sink = ShardSink {
                shards: shards.into(),
                next: AtomicUsize::new(0),
            };
            run_channel_cell(cell, &brokers, sink, tasks, &progress, runs).await
        }
        SinkKind::Inline => {
            let mut sessions = Vec::with_capacity(brokers.len());
            for (index, broker) in brokers.iter().enumerate() {
                let c = std::sync::Mutex::new(consumer(index));
                // Only this connection's read task calls the handler; the mutex is uncontended.
                let sink = OnInboundSink::new(move |m: InboundMessage| c.lock().expect("consumer").handle(&m));
                sessions.push(start_session(broker.port(), index, cell.qos, sink).await);
            }
            let samples = measure(&sessions, &progress, cell, runs).await;
            shutdown(sessions).await;
            samples
        }
    }
}

fn median(mut values: Vec<f64>) -> f64 {
    values.sort_by(f64::total_cmp);
    values[values.len() / 2]
}

fn output_dir() -> PathBuf {
    if let Ok(path) = std::env::var("MQTT_COMPARE_OUT") {
        return PathBuf::from(path);
    }
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    PathBuf::from(format!("target/ingress_fanin/{ts}"))
}

fn main() {
    match std::env::var("MQTT_COMPARE_ROLE").as_deref() {
        Ok("publisher") => return support::publisher::run_publisher_process(),
        Ok("mock_broker") => return support::mock_broker::run_mock_broker_process(),
        _ => {}
    }
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let cells = build_cells();
    let dir = output_dir();
    fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("create {}: {e}", dir.display()));
    let meta = json!({
        "glide_version": GLIDE_VERSION,
        "client_receive_max": client_receive_maximum().get(),
        "ingress_window": ingress_window(),
        "handoff_capacity": HANDOFF_CAPACITY,
        "runs": bench_runs(),
        "worker_threads": std::env::var("TOKIO_WORKER_THREADS").ok(),
        "broker_cpus": child_cpus("MQTT_COMPARE_BROKER_CPUS"),
        "publisher_cpus": child_cpus("MQTT_COMPARE_PUBLISHER_CPUS"),
        "warmup_per_broker": WARMUP_PER_BROKER,
        "os": std::env::consts::OS,
        "arch": std::env::consts::ARCH,
    });
    fs::write(
        dir.join("meta.json"),
        serde_json::to_vec_pretty(&meta).expect("meta json"),
    )
    .expect("write meta");
    let mut out = fs::File::create(dir.join("fanin.jsonl")).expect("create fanin.jsonl");
    eprintln!(
        "ingress_fanin: {} cells, {} runs each → {}",
        cells.len(),
        bench_runs(),
        dir.display()
    );
    // Sinks compared in one table row take turns run by run (the first one rotates), so drift
    // of the host over minutes hits each of them alike.
    let mut rows: Vec<Vec<Cell>> = Vec::new();
    for cell in cells {
        let key = |c: &Cell| (c.qos.as_u8(), c.work_ns, c.brokers, c.consumers);
        match rows.iter_mut().find(|row| key(&row[0]) == key(&cell)) {
            Some(row) => row.push(cell),
            None => rows.push(vec![cell]),
        }
    }
    let runs = bench_runs();
    for row in &rows {
        let mut row_samples: Vec<Vec<Sample>> = row.iter().map(|_| Vec::with_capacity(runs)).collect();
        for run in 0..runs {
            for turn in 0..row.len() {
                let idx = (run + turn) % row.len();
                row_samples[idx].extend(rt.block_on(run_cell(&row[idx], 1)));
            }
        }
        for (cell, samples) in row.iter().zip(row_samples) {
            write_cell(&mut out, cell, &samples);
        }
    }
    eprintln!("done — results in {}", dir.display());
}

fn write_cell(out: &mut fs::File, cell: &Cell, samples: &[Sample]) {
    let msg_s = median(samples.iter().map(|s| s.msg_s).collect());
    let cpu = median(samples.iter().map(|s| s.cpu_ns_per_msg).collect());
    let stalls = median(samples.iter().map(|s| s.stalls_per_kmsg).collect());
    eprintln!(
        "  qos={} work={}ns brokers={} {:<13} consumers={} → {:>9.0} msg/s, {:>6.0} CPU ns/msg, {:.1} stalls/kmsg",
        cell.qos.as_u8(),
        cell.work_ns,
        cell.brokers,
        cell.sink.as_str(),
        cell.consumers,
        msg_s,
        cpu,
        stalls
    );
    let row = json!({
        "sink": cell.sink.as_str(),
        "brokers": cell.brokers,
        "consumers": cell.consumers,
        "work_ns": cell.work_ns,
        "qos": cell.qos.as_u8(),
        "payload": cell.payload,
        "messages": cell.messages,
        "msg_s": msg_s,
        "cpu_ns_per_msg": cpu,
        "stalls_per_kmsg": stalls,
        "msg_s_runs": samples.iter().map(|s| s.msg_s).collect::<Vec<_>>(),
        "cpu_ns_per_msg_runs": samples.iter().map(|s| s.cpu_ns_per_msg).collect::<Vec<_>>(),
        "stall_ms_runs": samples.iter().map(|s| s.stall_ms).collect::<Vec<_>>(),
    });
    writeln!(out, "{row}").expect("write row");
}
