//! Execute the absolute scenario matrix for selected clients.

use std::sync::Arc;
use std::time::{Duration, Instant};

use super::clients::client_for;
use super::fixtures::{
    LOAD_CONCURRENCY, LOAD_MESSAGES, LOAD_WINDOW, SUMMARY_SAMPLES, WARMUP_SAMPLES, bench_runs, next_id, payload_bytes,
};
use super::harness::{BenchClient, BenchQos, ClientId, OutboundSession, SessionCfg};
use super::pubchild::{spawn_publisher_child, wait_child};
use super::report::{ResultRow, ResultWriter, print_row_summary};
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
    let client_id = client.id();
    let scenario = cell.direction.as_str();
    let qos = cell.qos.as_u8();
    let ack = cell.ack.map(|a| a.as_str());

    if !caps.supports_qos(cell.qos) {
        return ResultRow::na(client_id, scenario, qos, cell.payload, ack, "unsupported_qos", runs);
    }
    if let Some(ack_policy) = cell.ack
        && let Some(reason) = caps.ingress_ack_unsupported(ack_policy)
    {
        return ResultRow::na(client_id, scenario, qos, cell.payload, ack, reason, runs);
    }
    // Mosquitto: no publish counter → all outbound QoS 0 cells are API-only / not wire-fair.
    // Keep outbound QoS 1 (PUBACK ≈ broker saw the message; cutover throughput + RTT).
    if ctx.backend == BenchBrokerBackend::Mosquitto
        && cell.qos == BenchQos::AtMostOnce
        && matches!(
            cell.direction,
            Direction::OutboundSerial
                | Direction::OutboundPipelined
                | Direction::OutboundConcurrent
                | Direction::OutboundSerialLatency
        )
    {
        return ResultRow::na(
            client_id,
            scenario,
            qos,
            cell.payload,
            ack,
            "qos0_no_wire_counter",
            runs,
        );
    }

    match cell.direction {
        Direction::IngressLoad => {
            let samples = collect_load_runs(runs, || load_ingress(ctx.port, client, cell)).await;
            median_load_row(client_id, scenario, qos, cell.payload, ack, &samples, runs)
        }
        Direction::OutboundSerial => {
            let samples = collect_load_runs(runs, || load_outbound_serial(ctx, client, cell)).await;
            median_load_row(client_id, scenario, qos, cell.payload, ack, &samples, runs)
        }
        Direction::OutboundPipelined => {
            let samples = collect_load_runs(runs, || load_outbound_pipelined(ctx, client, cell)).await;
            median_load_row(client_id, scenario, qos, cell.payload, ack, &samples, runs)
        }
        Direction::OutboundConcurrent => {
            let samples = collect_load_runs(runs, || load_outbound_concurrent(ctx, client, cell)).await;
            median_load_row(client_id, scenario, qos, cell.payload, ack, &samples, runs)
        }
        Direction::IngressSerial => {
            let samples = collect_latency_runs(runs, || latency_ingress(ctx.port, client, cell)).await;
            median_latency_row(client_id, scenario, qos, cell.payload, ack, &samples, runs)
        }
        Direction::OutboundSerialLatency => {
            let samples = collect_latency_runs(runs, || latency_outbound(ctx, client, cell)).await;
            median_latency_row(client_id, scenario, qos, cell.payload, ack, &samples, runs)
        }
    }
}

struct LoadSample {
    msg_s: f64,
    cpu_ns: f64,
}

struct LatencySample {
    p50_us: f64,
    p99_us: f64,
}

async fn collect_load_runs<F, Fut>(runs: usize, mut measure: F) -> Vec<LoadSample>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = LoadSample>,
{
    let mut out = Vec::with_capacity(runs);
    for _ in 0..runs {
        out.push(measure().await);
    }
    out
}

async fn collect_latency_runs<F, Fut>(runs: usize, mut measure: F) -> Vec<LatencySample>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = LatencySample>,
{
    let mut out = Vec::with_capacity(runs);
    for _ in 0..runs {
        out.push(measure().await);
    }
    out
}

fn median_load_row(
    client: ClientId,
    scenario: &'static str,
    qos: u8,
    payload: usize,
    ack: Option<&'static str>,
    samples: &[LoadSample],
    runs: usize,
) -> ResultRow {
    let mut rates: Vec<f64> = samples.iter().map(|s| s.msg_s).collect();
    let mut cpus: Vec<f64> = samples.iter().map(|s| s.cpu_ns).collect();
    rates.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    cpus.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mid = samples.len() / 2;
    ResultRow::load(client, scenario, qos, payload, ack, rates[mid], cpus[mid], runs)
}

fn median_latency_row(
    client: ClientId,
    scenario: &'static str,
    qos: u8,
    payload: usize,
    ack: Option<&'static str>,
    samples: &[LatencySample],
    runs: usize,
) -> ResultRow {
    let mut p50s: Vec<f64> = samples.iter().map(|s| s.p50_us).collect();
    let mut p99s: Vec<f64> = samples.iter().map(|s| s.p99_us).collect();
    p50s.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    p99s.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mid = samples.len() / 2;
    ResultRow::latency(client, scenario, qos, payload, ack, p50s[mid], p99s[mid], runs)
}

fn session_cfg(port: u16, client_id: String, cell: &ScenarioCell) -> SessionCfg {
    SessionCfg {
        port,
        client_id,
        qos: cell.qos,
        ack: cell.ack,
    }
}

async fn load_ingress(port: u16, client: &dyn BenchClient, cell: &ScenarioCell) -> LoadSample {
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
        LOAD_MESSAGES,
        &next_id("child-pub"),
        session.topic(),
        cell.payload,
        cell.qos,
    );
    for _ in 0..LOAD_MESSAGES {
        session.recv_touch().await;
    }
    session.wait_barrier(LOAD_MESSAGES).await;
    wait_child(child);
    let wall = wall_start.elapsed();
    let cpu = process_cpu_time().saturating_sub(cpu_before);
    session.close().await;
    load_sample(LOAD_MESSAGES, wall, cpu)
}

/// Outbound load warmup publishes (drained on mock before the timed burst).
const OUTBOUND_WARMUP: usize = 32;

async fn load_outbound_serial(ctx: MatrixCtx, client: &dyn BenchClient, cell: &ScenarioCell) -> LoadSample {
    let cfg = session_cfg(ctx.port, next_id(&format!("{}-out", client.id().as_str())), cell);
    let session = client.setup_outbound(&cfg).await;
    let payload = payload_bytes(cell.payload);
    warmup_outbound(ctx.mock_stats, session.as_ref(), &payload).await;
    let cpu_before = process_cpu_time();
    let wall_start = Instant::now();
    for _ in 0..LOAD_MESSAGES {
        session.publish_serial(&payload).await;
    }
    if let Some(stats) = ctx.mock_stats {
        stats
            .wait_exact(LOAD_MESSAGES as u64)
            .await
            .unwrap_or_else(|error| panic!("outbound serial wire wait: {error}"));
    }
    let wall = wall_start.elapsed();
    let cpu = process_cpu_time().saturating_sub(cpu_before);
    session.close().await;
    load_sample(LOAD_MESSAGES, wall, cpu)
}

async fn load_outbound_pipelined(ctx: MatrixCtx, client: &dyn BenchClient, cell: &ScenarioCell) -> LoadSample {
    let cfg = session_cfg(ctx.port, next_id(&format!("{}-pipe", client.id().as_str())), cell);
    let session = client.setup_outbound(&cfg).await;
    let payload = payload_bytes(cell.payload);
    warmup_outbound(ctx.mock_stats, session.as_ref(), &payload).await;
    let cpu_before = process_cpu_time();
    let wall_start = Instant::now();
    session.publish_pipelined(&payload, LOAD_WINDOW, LOAD_MESSAGES).await;
    if let Some(stats) = ctx.mock_stats {
        stats
            .wait_exact(LOAD_MESSAGES as u64)
            .await
            .unwrap_or_else(|error| panic!("outbound pipelined wire wait: {error}"));
    }
    let wall = wall_start.elapsed();
    let cpu = process_cpu_time().saturating_sub(cpu_before);
    session.close().await;
    load_sample(LOAD_MESSAGES, wall, cpu)
}

/// HTTP-style fan-out: `LOAD_CONCURRENCY` tasks share one session and only call `publish_serial`.
async fn load_outbound_concurrent(ctx: MatrixCtx, client: &dyn BenchClient, cell: &ScenarioCell) -> LoadSample {
    let cfg = session_cfg(ctx.port, next_id(&format!("{}-conc", client.id().as_str())), cell);
    let session: Arc<dyn OutboundSession> = Arc::from(client.setup_outbound(&cfg).await);
    let payload = Arc::new(payload_bytes(cell.payload));
    warmup_outbound(ctx.mock_stats, session.as_ref(), payload.as_ref()).await;
    let cpu_before = process_cpu_time();
    let wall_start = Instant::now();

    let per_worker = LOAD_MESSAGES / LOAD_CONCURRENCY;
    let remainder = LOAD_MESSAGES % LOAD_CONCURRENCY;
    let mut joins = tokio::task::JoinSet::new();
    for worker in 0..LOAD_CONCURRENCY {
        let n = per_worker + if worker < remainder { 1 } else { 0 };
        if n == 0 {
            continue;
        }
        let session = Arc::clone(&session);
        let payload = Arc::clone(&payload);
        joins.spawn(async move {
            for _ in 0..n {
                session.publish_serial(payload.as_ref()).await;
            }
        });
    }
    while let Some(joined) = joins.join_next().await {
        joined.expect("outbound concurrent worker");
    }

    if let Some(stats) = ctx.mock_stats {
        stats
            .wait_exact(LOAD_MESSAGES as u64)
            .await
            .unwrap_or_else(|error| panic!("outbound concurrent wire wait: {error}"));
    }
    let wall = wall_start.elapsed();
    let cpu = process_cpu_time().saturating_sub(cpu_before);
    session.close().await;
    load_sample(LOAD_MESSAGES, wall, cpu)
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

async fn latency_ingress(port: u16, client: &dyn BenchClient, cell: &ScenarioCell) -> LatencySample {
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
    latency_from_samples(&samples)
}

async fn latency_outbound(ctx: MatrixCtx, client: &dyn BenchClient, cell: &ScenarioCell) -> LatencySample {
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
    latency_from_samples(&samples)
}

fn load_sample(messages: usize, wall: Duration, cpu: Duration) -> LoadSample {
    let msg_s = if wall.is_zero() {
        0.0
    } else {
        messages as f64 / wall.as_secs_f64()
    };
    let cpu_ns = if messages == 0 {
        0.0
    } else {
        cpu.as_nanos() as f64 / messages as f64
    };
    eprintln!(
        "    burst: {messages} msgs in {:.3}s wall / {:.3}s CPU → {:.0} msg/s, {:.0} CPU ns/msg",
        wall.as_secs_f64(),
        cpu.as_secs_f64(),
        msg_s,
        cpu_ns
    );
    LoadSample { msg_s, cpu_ns }
}

fn latency_from_samples(samples: &[Duration]) -> LatencySample {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let p50 = percentile(&sorted, 0.50);
    let p99 = percentile(&sorted, 0.99);
    LatencySample {
        p50_us: p50.as_secs_f64() * 1_000_000.0,
        p99_us: p99.as_secs_f64() * 1_000_000.0,
    }
}

fn percentile(sorted: &[Duration], rank: f64) -> Duration {
    assert!(!sorted.is_empty());
    let index = ((sorted.len() as f64 - 1.0) * rank).round() as usize;
    sorted[index.min(sorted.len() - 1)]
}
