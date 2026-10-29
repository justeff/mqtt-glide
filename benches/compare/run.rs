//! Execute the absolute scenario matrix for selected clients.

use std::sync::Arc;
use std::time::{Duration, Instant};

use super::clients::client_for;
use super::fixtures::{LOAD_WINDOW, bench_runs, load_messages, next_id, payload_bytes, rate_secs};
use super::harness::{BenchClient, BenchQos, ClientId, IngressSession, OutboundSession, SessionCfg};
use super::pubchild::{spawn_publisher_child, wait_child};
use super::report::{CellKey, Metrics, ResultRow, ResultWriter, print_row_summary};
use super::scenario::{Direction, ScenarioCell};
use crate::support::broker::BenchBrokerBackend;
use crate::support::cpu::{monotonic_ns, process_cpu_time};
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
///
/// Clients take turns run by run within a cell (the first client rotates), so drift of the
/// host over minutes (frequency, the scheduler of a VM's vCPUs) hits every client alike
/// instead of whichever client happens to run its cells while the host is slow.
pub async fn execute_matrix(ctx: MatrixCtx, clients: &[ClientId], cells: &[ScenarioCell], writer: &ResultWriter) {
    let runs = bench_runs();
    let clients: Vec<_> = clients.iter().map(|&id| (id, client_for(id))).collect();
    for cell in cells {
        let mut skipped: Vec<Option<ResultRow>> = clients
            .iter()
            .map(|(_, client)| skip_reason(ctx, client.as_ref(), cell, runs))
            .collect();
        let mut samples: Vec<Vec<Metrics>> = clients.iter().map(|_| Vec::with_capacity(runs)).collect();
        for run in 0..runs {
            for turn in 0..clients.len() {
                let idx = (run + turn) % clients.len();
                if skipped[idx].is_none() {
                    samples[idx].push(measure_once(ctx, clients[idx].1.as_ref(), cell).await);
                }
            }
        }
        for (idx, (client_id, client)) in clients.iter().enumerate() {
            let row = skipped[idx]
                .take()
                .unwrap_or_else(|| ResultRow::measured(cell_key(client.as_ref(), cell), &samples[idx]));
            writer.write_row(*client_id, &row);
            print_row_summary(&row);
        }
    }
}

fn cell_key(client: &dyn BenchClient, cell: &ScenarioCell) -> CellKey {
    CellKey {
        client: client.id(),
        scenario: cell.direction.as_str(),
        qos: cell.qos.as_u8(),
        payload: cell.payload,
        ack: cell.ack.map(|a| a.as_str()),
        level: cell.level,
    }
}

/// The `n/a` row when this client cannot run this cell.
fn skip_reason(ctx: MatrixCtx, client: &dyn BenchClient, cell: &ScenarioCell, runs: usize) -> Option<ResultRow> {
    let caps = client.caps();
    let key = cell_key(client, cell);
    if !caps.supports_qos(cell.qos) {
        return Some(ResultRow::na(key, "unsupported_qos", runs));
    }
    if let Some(ack_policy) = cell.ack
        && let Some(reason) = caps.ingress_ack_unsupported(ack_policy)
    {
        return Some(ResultRow::na(key, reason, runs));
    }
    // Mosquitto: no publish counter → all outbound QoS 0 cells are API-only / not wire-fair.
    // Keep outbound QoS 1 (PUBACK ≈ broker saw the message; throughput + round trip).
    if ctx.backend != BenchBrokerBackend::Mock && cell.qos == BenchQos::AtMostOnce && cell.direction.is_outbound() {
        return Some(ResultRow::na(key, "qos0_no_wire_counter", runs));
    }
    None
}

async fn measure_once(ctx: MatrixCtx, client: &dyn BenchClient, cell: &ScenarioCell) -> Metrics {
    match cell.direction {
        Direction::IngressLoad => load_ingress(ctx.port, client, cell).await,
        Direction::OutboundPipelined => load_outbound_pipelined(ctx, client, cell).await,
        Direction::OutboundConcurrent => {
            load_outbound_concurrent(ctx, client, cell, cell.level.expect("concurrency level")).await
        }
        Direction::OutboundRate => load_outbound_rate(ctx, client, cell, cell.level.expect("rate level")).await,
        Direction::IngressRate => rate_ingress(ctx.port, client, cell, cell.level.expect("rate level")).await,
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

/// 32 messages through the session, then an empty queue and fresh barrier counters.
async fn warmup_ingress(port: u16, session: &mut dyn IngressSession, cell: &ScenarioCell) {
    let payload = payload_bytes(cell.payload);
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

async fn load_ingress(port: u16, client: &dyn BenchClient, cell: &ScenarioCell) -> Metrics {
    let messages = load_messages();
    let cfg = session_cfg(port, next_id(&format!("{}-in", client.id().as_str())), cell);
    let mut session = client.setup_ingress(&cfg).await;
    warmup_ingress(port, session.as_mut(), cell).await;

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
        None,
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

/// Open-loop ingress at `rate` msg/s: latency from the publisher's write to the app-queue pull.
async fn rate_ingress(port: u16, client: &dyn BenchClient, cell: &ScenarioCell, rate: u32) -> Metrics {
    let messages = (f64::from(rate) * rate_secs()).round().max(1.0) as usize;
    let cfg = session_cfg(port, next_id(&format!("{}-rate-in", client.id().as_str())), cell);
    let mut session = client.setup_ingress(&cfg).await;
    warmup_ingress(port, session.as_mut(), cell).await;

    session.reset_barrier();
    let cpu_before = process_cpu_time();
    let wall_start = Instant::now();
    let child = spawn_publisher_child(
        port,
        messages,
        &next_id("child-pub"),
        session.topic(),
        cell.payload.max(8),
        cell.qos,
        Some(rate),
    );
    let mut latencies = Vec::with_capacity(messages);
    for _ in 0..messages {
        let stamp = session.recv_touch().await;
        latencies.push(Duration::from_nanos(monotonic_ns().saturating_sub(stamp)));
    }
    session.wait_barrier(messages).await;
    wait_child(child);
    let wall = wall_start.elapsed();
    let cpu = process_cpu_time().saturating_sub(cpu_before);
    session.close().await;
    load_metrics(messages, wall, cpu, Some(&mut latencies))
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
