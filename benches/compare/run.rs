//! Execute the absolute scenario matrix for selected clients.

use std::sync::Arc;
use std::time::{Duration, Instant};

use super::clients::client_for;
use super::fixtures::{
    LOAD_WINDOW, SUMMARY_SAMPLES, WARMUP_SAMPLES, bench_runs, load_messages, next_id, payload_bytes, rate_secs,
};
use super::harness::{BenchClient, BenchQos, ClientId, OutboundSession, SessionCfg};
use super::pubchild::{spawn_publisher_child, wait_child};
use super::report::{CellKey, Metrics, ResultRow, ResultWriter, print_row_summary};
use super::scenario::{Direction, ScenarioCell};
use crate::support::broker::BenchBrokerBackend;
use crate::support::cpu::process_cpu_time;
use crate::support::mock_broker::MockStatsPort;
use crate::support::publisher::PersistentPublisher;

/// Shared context for one matrix run (broker ports / backend).
#[derive(Debug, Clone, Copy)]
pub struct MatrixCtx {
    /// MQTT broker port.
    pub port: u16,
    /// Broker backend (Mosquitto skips outbound QoS 0 load).
    pub backend: BenchBrokerBackend,
    /// Mock inbound PUBLISH counter (wire stopwatch), if mock broker.
    pub mock_stats: Option<MockStatsPort>,
}

/// Runs every `(client × cell)` and writes absolute JSONL rows.
pub async fn execute_matrix(ctx: MatrixCtx, clients: &[ClientId], cells: &[ScenarioCell], writer: &ResultWriter) {
    let runs = bench_runs();
    for &client_id in clients {
        let client = client_for(client_id);
        let caps = client.caps();
        for cell in cells {
            let row = measure_cell(ctx, client.as_ref(), cell, caps, runs).await;
            writer.write_row(client_id, &row);
            print_row_summary(&row);
        }
    }
}

async fn measure_cell(
    ctx: MatrixCtx,
    client: &dyn BenchClient,
    cell: &ScenarioCell,
    caps: super::harness::ClientCaps,
    runs: usize,
) -> ResultRow {
    let key = CellKey {
        client: client.id(),
        scenario: cell.direction.as_str(),
        qos: cell.qos.as_u8(),
        payload: cell.payload,
        ack: cell.ack.map(|a| a.as_str()),
        level: cell.level,
    };

    if !caps.supports_qos(cell.qos) {
        return ResultRow::na(key, "unsupported_qos", runs);
    }
    if let Some(ack_policy) = cell.ack
        && let Some(reason) = caps.ingress_ack_unsupported(ack_policy)
    {
        return ResultRow::na(key, reason, runs);
    }
    // Mosquitto: no publish counter → all outbound QoS 0 cells are API-only / not wire-fair.
    // Keep outbound QoS 1 (PUBACK ≈ broker saw the message; cutover throughput + RTT).
    if ctx.backend != BenchBrokerBackend::Mock && cell.qos == BenchQos::AtMostOnce && cell.direction.is_outbound() {
        return ResultRow::na(key, "qos0_no_wire_counter", runs);
    }

    let mut samples = Vec::with_capacity(runs);
    for _ in 0..runs {
        let sample = match cell.direction {
            Direction::IngressLoad => load_ingress(ctx.port, client, cell).await,
            Direction::OutboundPipelined => load_outbound_pipelined(ctx, client, cell).await,
            Direction::OutboundConcurrent => {
                load_outbound_concurrent(ctx, client, cell, cell.level.expect("concurrency level")).await
            }
            Direction::OutboundSpawn => load_outbound_spawn(ctx, client, cell).await,
            Direction::OutboundRate => load_outbound_rate(ctx, client, cell, cell.level.expect("rate level")).await,
            Direction::IngressSerial => latency_ingress(ctx.port, client, cell).await,
            Direction::OutboundSerialLatency => latency_outbound(ctx, client, cell).await,
        };
        samples.push(sample);
    }
    ResultRow::measured(key, median_metrics(&samples), runs)
}

/// Median of each metric across runs (metrics absent in a run are skipped).
fn median_metrics(samples: &[Metrics]) -> Metrics {
    fn median(values: impl Iterator<Item = Option<f64>>) -> Option<f64> {
        let mut values: Vec<f64> = values.flatten().collect();
        if values.is_empty() {
            return None;
        }
        values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        Some(values[values.len() / 2])
    }
    Metrics {
        msg_s: median(samples.iter().map(|s| s.msg_s)),
        cpu_ns_per_msg: median(samples.iter().map(|s| s.cpu_ns_per_msg)),
        p50_us: median(samples.iter().map(|s| s.p50_us)),
        p99_us: median(samples.iter().map(|s| s.p99_us)),
        p999_us: median(samples.iter().map(|s| s.p999_us)),
    }
}

fn session_cfg(port: u16, client_id: String, cell: &ScenarioCell) -> SessionCfg {
    SessionCfg {
        port,
        client_id,
        qos: cell.qos,
        ack: cell.ack,
    }
}

async fn load_ingress(port: u16, client: &dyn BenchClient, cell: &ScenarioCell) -> Metrics {
    let messages = load_messages();
    let cfg = session_cfg(port, next_id(&format!("{}-in", client.id().as_str())), cell);
    let session = client.setup_ingress(&cfg).await;
    let payload = Arc::new(payload_bytes(cell.payload));

    // Warmup
    {
        let mut warm = PersistentPublisher::connect(port, &next_id("pub-warm")).await;
        session.drain();
        session.reset_barrier();
        for _ in 0..32 {
            warm.send(cell.qos, session.topic(), &payload).await;
            session.recv_touch().await;
        }
        session.wait_barrier(32).await;
        warm.drain_completions(cell.qos).await;
        warm.disconnect().await;
        session.drain();
    }

    session.reset_barrier();
    let cpu_before = process_cpu_time();
    let wall_start = Instant::now();
    let child = spawn_publisher_child(
        port,
        messages,
        &next_id("child-pub"),
        session.topic(),
        cell.payload,
        cell.qos,
    );
    for _ in 0..messages {
        session.recv_touch().await;
    }
    session.wait_barrier(messages).await;
    wait_child(child);
    let wall = wall_start.elapsed();
    let cpu = process_cpu_time().saturating_sub(cpu_before);
    session.close().await;
    load_metrics(messages, wall, cpu, None)
}

/// Outbound load warmup publishes (drained on mock before the timed burst).
const OUTBOUND_WARMUP: usize = 32;

/// Shared outbound setup: session (shareable across tasks), payload, warmed-up broker counter.
async fn outbound_session(
    ctx: MatrixCtx,
    client: &dyn BenchClient,
    cell: &ScenarioCell,
    tag: &str,
) -> (Arc<dyn OutboundSession>, Arc<Vec<u8>>) {
    let cfg = session_cfg(ctx.port, next_id(&format!("{}-{tag}", client.id().as_str())), cell);
    let session: Arc<dyn OutboundSession> = Arc::from(client.setup_outbound(&cfg).await);
    let payload = Arc::new(payload_bytes(cell.payload));
    warmup_outbound(ctx.mock_stats, session.as_ref(), payload.as_ref()).await;
    (session, payload)
}

/// Mock: waits until the broker decoded all `messages` (QoS 0 completes before the wire).
async fn wait_wire(ctx: MatrixCtx, messages: usize, label: &str) {
    if let Some(stats) = ctx.mock_stats {
        stats
            .wait_exact(messages as u64)
            .await
            .unwrap_or_else(|error| panic!("{label} wire wait: {error}"));
    }
}

async fn load_outbound_pipelined(ctx: MatrixCtx, client: &dyn BenchClient, cell: &ScenarioCell) -> Metrics {
    let messages = load_messages();
    let (session, payload) = outbound_session(ctx, client, cell, "pipe").await;
    let cpu_before = process_cpu_time();
    let wall_start = Instant::now();
    session.publish_pipelined(&payload, LOAD_WINDOW, messages).await;
    wait_wire(ctx, messages, "outbound pipelined").await;
    let wall = wall_start.elapsed();
    let cpu = process_cpu_time().saturating_sub(cpu_before);
    session.close().await;
    load_metrics(messages, wall, cpu, None)
}

/// Service shape: `concurrency` long-lived tasks share one session, each awaiting every publish.
async fn load_outbound_concurrent(
    ctx: MatrixCtx,
    client: &dyn BenchClient,
    cell: &ScenarioCell,
    concurrency: u32,
) -> Metrics {
    let messages = load_messages();
    let workers = usize::try_from(concurrency).expect("concurrency fits usize");
    let (session, payload) = outbound_session(ctx, client, cell, "conc").await;
    let cpu_before = process_cpu_time();
    let wall_start = Instant::now();

    let per_worker = messages / workers;
    let remainder = messages % workers;
    let mut joins = tokio::task::JoinSet::new();
    for worker in 0..workers {
        let n = per_worker + usize::from(worker < remainder);
        if n == 0 {
            continue;
        }
        let session = Arc::clone(&session);
        let payload = Arc::clone(&payload);
        joins.spawn(async move {
            let mut latencies = Vec::with_capacity(n);
            for _ in 0..n {
                let start = Instant::now();
                session.publish_serial(payload.as_ref()).await;
                latencies.push(start.elapsed());
            }
            latencies
        });
    }
    let mut latencies = Vec::with_capacity(messages);
    while let Some(joined) = joins.join_next().await {
        latencies.extend(joined.expect("outbound concurrent worker"));
    }

    wait_wire(ctx, messages, "outbound concurrent").await;
    let wall = wall_start.elapsed();
    let cpu = process_cpu_time().saturating_sub(cpu_before);
    session.close().await;
    load_metrics(messages, wall, cpu, Some(&mut latencies))
}

/// Relay shape: one producer spawns a task per message that awaits its publish
/// (throughput and CPU only — the whole burst is spawned at once).
async fn load_outbound_spawn(ctx: MatrixCtx, client: &dyn BenchClient, cell: &ScenarioCell) -> Metrics {
    let messages = load_messages();
    let (session, payload) = outbound_session(ctx, client, cell, "spawn").await;
    let cpu_before = process_cpu_time();
    let wall_start = Instant::now();

    let mut joins = tokio::task::JoinSet::new();
    for _ in 0..messages {
        let session = Arc::clone(&session);
        let payload = Arc::clone(&payload);
        joins.spawn(async move { session.publish_serial(payload.as_ref()).await });
    }
    while let Some(joined) = joins.join_next().await {
        joined.expect("outbound spawn task");
    }

    wait_wire(ctx, messages, "outbound spawn").await;
    let wall = wall_start.elapsed();
    let cpu = process_cpu_time().saturating_sub(cpu_before);
    session.close().await;
    // No latency: every task starts at once, so it would only measure the burst size.
    load_metrics(messages, wall, cpu, None)
}

/// Open loop: publishes are released at `rate` msg/s (1 ms ticks), each in its own task.
/// The generator never waits on the client, so a client that cannot keep up shows growing
/// latency (measured from each task's release) and a lower achieved rate instead of
/// silently slowing the generator down.
async fn load_outbound_rate(ctx: MatrixCtx, client: &dyn BenchClient, cell: &ScenarioCell, rate: u32) -> Metrics {
    let rate_f = f64::from(rate);
    let messages = (rate_f * rate_secs()).round().max(1.0) as usize;
    let (session, payload) = outbound_session(ctx, client, cell, "rate").await;
    let cpu_before = process_cpu_time();
    let wall_start = Instant::now();

    let mut ticker = tokio::time::interval(Duration::from_millis(1));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
    let mut joins = tokio::task::JoinSet::new();
    let mut released = 0usize;
    while released < messages {
        ticker.tick().await;
        let due = ((wall_start.elapsed().as_secs_f64() * rate_f) as usize).min(messages);
        while released < due {
            let session = Arc::clone(&session);
            let payload = Arc::clone(&payload);
            let release = Instant::now();
            joins.spawn(async move {
                session.publish_serial(payload.as_ref()).await;
                release.elapsed()
            });
            released += 1;
        }
    }
    let mut latencies = Vec::with_capacity(messages);
    while let Some(joined) = joins.join_next().await {
        latencies.push(joined.expect("outbound rate task"));
    }

    wait_wire(ctx, messages, "outbound rate").await;
    let wall = wall_start.elapsed();
    let cpu = process_cpu_time().saturating_sub(cpu_before);
    session.close().await;
    load_metrics(messages, wall, cpu, Some(&mut latencies))
}

/// Warms the session; on mock, drains PUBLISHes to the broker then resets the counter.
async fn warmup_outbound(
    mock_stats: Option<MockStatsPort>,
    session: &dyn super::harness::OutboundSession,
    payload: &[u8],
) {
    if let Some(stats) = mock_stats {
        stats.reset().await;
    }
    for _ in 0..OUTBOUND_WARMUP {
        session.publish_serial(payload).await;
    }
    if let Some(stats) = mock_stats {
        // QoS 0 may complete before the broker sees the frame — drain before RESET.
        stats
            .wait_exact(OUTBOUND_WARMUP as u64)
            .await
            .unwrap_or_else(|error| panic!("outbound warmup wire wait: {error}"));
        stats.reset().await;
    }
}

async fn latency_ingress(port: u16, client: &dyn BenchClient, cell: &ScenarioCell) -> Metrics {
    let cfg = session_cfg(port, next_id(&format!("{}-lat-in", client.id().as_str())), cell);
    let session = client.setup_ingress(&cfg).await;
    let payload = payload_bytes(cell.payload);
    let mut publisher = PersistentPublisher::connect(port, &next_id("lat-pub")).await;
    let mut samples = Vec::with_capacity(SUMMARY_SAMPLES);
    for i in 0..(WARMUP_SAMPLES + SUMMARY_SAMPLES) {
        session.drain();
        session.reset_barrier();
        let start = Instant::now();
        publisher.send(cell.qos, session.topic(), &payload).await;
        session.recv_touch().await;
        let elapsed = start.elapsed();
        session.wait_barrier(1).await;
        publisher.drain_completions(cell.qos).await;
        if i >= WARMUP_SAMPLES {
            samples.push(elapsed);
        }
    }
    publisher.disconnect().await;
    session.close().await;
    latency_metrics(&mut samples)
}

async fn latency_outbound(ctx: MatrixCtx, client: &dyn BenchClient, cell: &ScenarioCell) -> Metrics {
    let cfg = session_cfg(ctx.port, next_id(&format!("{}-lat-out", client.id().as_str())), cell);
    let session = client.setup_outbound(&cfg).await;
    let payload = payload_bytes(cell.payload);
    // One stats TCP session for the whole cell — per-sample connect exhausts ephemeral ports.
    let mut stats = match ctx.mock_stats {
        Some(port) => Some(port.connect().await),
        None => None,
    };
    if let Some(stats) = stats.as_mut() {
        stats.reset().await;
        for _ in 0..8 {
            session.publish_serial(&payload).await;
        }
        stats
            .wait_exact(8)
            .await
            .unwrap_or_else(|error| panic!("outbound latency warmup wire wait: {error}"));
        stats.reset().await;
    } else {
        for _ in 0..8 {
            session.publish_serial(&payload).await;
        }
    }
    let mut samples = Vec::with_capacity(SUMMARY_SAMPLES);
    for i in 0..(WARMUP_SAMPLES + SUMMARY_SAMPLES) {
        if let Some(stats) = stats.as_mut() {
            stats.reset().await;
        }
        let start = Instant::now();
        session.publish_serial(&payload).await;
        // Mock: stopwatch ends when the broker has decoded this PUBLISH (QoS 0/1).
        // Mosquitto QoS 1: publish_serial already waited on PUBACK (≈ broker saw it).
        if let Some(stats) = stats.as_mut() {
            stats
                .wait_exact(1)
                .await
                .unwrap_or_else(|error| panic!("outbound latency wire wait: {error}"));
        }
        let elapsed = start.elapsed();
        if i >= WARMUP_SAMPLES {
            samples.push(elapsed);
        }
    }
    session.close().await;
    latency_metrics(&mut samples)
}

fn load_metrics(messages: usize, wall: Duration, cpu: Duration, latencies: Option<&mut Vec<Duration>>) -> Metrics {
    let msg_s = if wall.is_zero() {
        0.0
    } else {
        messages as f64 / wall.as_secs_f64()
    };
    let cpu_ns = cpu.as_nanos() as f64 / messages as f64;
    eprintln!(
        "    burst: {messages} msgs in {:.3}s wall / {:.3}s CPU → {:.0} msg/s, {:.0} CPU ns/msg",
        wall.as_secs_f64(),
        cpu.as_secs_f64(),
        msg_s,
        cpu_ns
    );
    let latency = latencies.map(|samples| latency_metrics(samples)).unwrap_or_default();
    Metrics {
        msg_s: Some(msg_s),
        cpu_ns_per_msg: Some(cpu_ns),
        ..latency
    }
}

fn latency_metrics(samples: &mut [Duration]) -> Metrics {
    samples.sort_unstable();
    let micros = |rank| percentile(samples, rank).as_secs_f64() * 1_000_000.0;
    Metrics {
        p50_us: Some(micros(0.50)),
        p99_us: Some(micros(0.99)),
        p999_us: Some(micros(0.999)),
        ..Metrics::default()
    }
}

fn percentile(sorted: &[Duration], rank: f64) -> Duration {
    assert!(!sorted.is_empty());
    let index = ((sorted.len() as f64 - 1.0) * rank).round() as usize;
    sorted[index.min(sorted.len() - 1)]
}
