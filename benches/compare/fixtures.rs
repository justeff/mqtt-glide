//! Shared fixtures and env parsing for the absolute compare matrix.

use std::num::NonZeroU16;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use nonzero_ext::nonzero;

use super::harness::{AckPolicy, BenchQos, ClientId};
use super::scenario::Direction;

/// Default payload size (bytes) when `MQTT_COMPARE_PAYLOADS` is unset.
pub const DEFAULT_PAYLOAD_SIZE: usize = 256;

/// MQTT keep-alive interval shared by all clients.
pub const KEEP_ALIVE: Duration = Duration::from_secs(60);

/// Client Receive Maximum (large window; not library defaults).
pub const RECEIVE_MAXIMUM: NonZeroU16 = nonzero!(1024u16);

/// Injectable handoff capacity.
pub const HANDOFF_CAPACITY: usize = 10_000;

/// rumqttc / mqtt-glide command-queue depth for heavy publish (1024).
pub const REQUEST_CAPACITY: usize = 1024;

/// Warmup iterations before latency collection.
pub const WARMUP_SAMPLES: usize = 50;

/// Samples for serial latency p50/p99.
pub const SUMMARY_SAMPLES: usize = 200;

/// Default messages per sustained-load burst (`MQTT_COMPARE_LOAD_MSGS`).
///
/// Large enough that one burst runs for tens to hundreds of milliseconds, so scheduler
/// jitter and warmup do not dominate the measurement.
pub const DEFAULT_LOAD_MESSAGES: usize = 50_000;

/// Max in-flight for ingress publisher and outbound pipelined load.
pub const LOAD_WINDOW: usize = 64;

/// Default caller-task counts for `outbound_concurrent` (`MQTT_COMPARE_CONCURRENCY`).
pub const DEFAULT_CONCURRENCY: &str = "8,64,512";

/// Default offered rates (msg/s) for `outbound_rate` (`MQTT_COMPARE_RATES`).
pub const DEFAULT_RATES: &str = "10000,50000";

/// Default duration of one `outbound_rate` run (`MQTT_COMPARE_RATE_SECS`).
pub const DEFAULT_RATE_SECS: f64 = 2.0;

/// Default median-of runs (`MQTT_COMPARE_RUNS`).
pub const DEFAULT_BENCH_RUNS: usize = 5;

/// Ingress topic prefix (per-session suffix appended).
pub const INGRESS_TOPIC: &str = "bench/compare/ingress";

/// Outbound topic (no subscriber required).
pub const OUTBOUND_TOPIC: &str = "bench/compare/outbound";

/// This crate’s version under test (`CARGO_PKG_VERSION`).
pub const GLIDE_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Locked peer crate for published baselines (`rumqttc-next` in `Cargo.toml`).
pub const PEER_RUMQTTC_VERSION: &str = "0.33.3";

/// Locked peer crate for published baselines (LabOverWire `mqtt5` in `Cargo.toml`).
pub const PEER_MQTT5_VERSION: &str = "0.37.2";

static SEQ: AtomicUsize = AtomicUsize::new(0);

/// Unique client id with prefix.
pub fn next_id(prefix: &str) -> String {
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{seq}")
}

/// Payload pattern of `size` bytes.
pub fn payload_bytes(size: usize) -> Vec<u8> {
    (0..size).map(|index| (index % 251) as u8).collect()
}

/// `MQTT_COMPARE_LOAD_MSGS` (default [`DEFAULT_LOAD_MESSAGES`], min 1).
pub fn load_messages() -> usize {
    std::env::var("MQTT_COMPARE_LOAD_MSGS")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|n: &usize| *n >= 1)
        .unwrap_or(DEFAULT_LOAD_MESSAGES)
}

/// `MQTT_COMPARE_RATE_SECS` (default [`DEFAULT_RATE_SECS`]).
pub fn rate_secs() -> f64 {
    std::env::var("MQTT_COMPARE_RATE_SECS")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|secs: &f64| *secs > 0.0)
        .unwrap_or(DEFAULT_RATE_SECS)
}

/// `MQTT_COMPARE_CONCURRENCY` — comma list of caller-task counts (default [`DEFAULT_CONCURRENCY`]).
pub fn parse_concurrency() -> Vec<u32> {
    parse_positive_list("MQTT_COMPARE_CONCURRENCY", DEFAULT_CONCURRENCY)
}

/// `MQTT_COMPARE_RATES` — comma list of offered msg/s (default [`DEFAULT_RATES`]).
pub fn parse_rates() -> Vec<u32> {
    parse_positive_list("MQTT_COMPARE_RATES", DEFAULT_RATES)
}

fn parse_positive_list(var: &str, default: &str) -> Vec<u32> {
    let raw = std::env::var(var).unwrap_or_else(|_| default.into());
    let out: Vec<u32> = raw
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(|part| {
            part.parse()
                .ok()
                .filter(|n: &u32| *n > 0)
                .unwrap_or_else(|| panic!("invalid {var} entry {part:?}: expected a positive integer"))
        })
        .collect();
    assert!(!out.is_empty(), "{var} must list at least one value");
    out
}

/// CPU list for a bench child process (`taskset -c` syntax), e.g. `MQTT_COMPARE_BROKER_CPUS=4-7`.
///
/// Children inherit the parent's affinity; set these to keep the broker / publisher off
/// the cores the client under test runs on (run the bench itself under `taskset`).
pub fn child_cpus(var: &str) -> Option<String> {
    std::env::var(var)
        .ok()
        .map(|cpus| cpus.trim().to_owned())
        .filter(|cpus| !cpus.is_empty())
}

/// Builds a `Command` for `exe`, wrapped in `taskset -c <cpus>` when `cpus_var` is set.
pub fn pinned_command(exe: &std::path::Path, cpus_var: &str) -> std::process::Command {
    match child_cpus(cpus_var) {
        Some(cpus) => {
            let mut command = std::process::Command::new("taskset");
            command.arg("-c").arg(cpus).arg(exe);
            command
        }
        None => std::process::Command::new(exe),
    }
}

/// `MQTT_COMPARE_RUNS` (default [`DEFAULT_BENCH_RUNS`], min 1).
pub fn bench_runs() -> usize {
    std::env::var("MQTT_COMPARE_RUNS")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|n: &usize| *n >= 1)
        .unwrap_or(DEFAULT_BENCH_RUNS)
}

/// `MQTT_COMPARE_CLIENTS` — comma list (`glide,rumqttc,mqtt5`), default all.
pub fn parse_clients() -> Vec<ClientId> {
    let raw = std::env::var("MQTT_COMPARE_CLIENTS").unwrap_or_else(|_| "glide,rumqttc,mqtt5".into());
    let mut out = Vec::new();
    for part in raw.split(',') {
        let part = part.trim().to_ascii_lowercase();
        if part.is_empty() {
            continue;
        }
        out.push(match part.as_str() {
            "glide" | "mqtt-glide" | "mqtt_glide" => ClientId::Glide,
            "rumqttc" | "rumqttc-next" => ClientId::Rumqttc,
            "mqtt5" => ClientId::Mqtt5,
            other => panic!("invalid MQTT_COMPARE_CLIENTS entry {other:?}"),
        });
    }
    assert!(!out.is_empty(), "MQTT_COMPARE_CLIENTS must list at least one client");
    out
}

/// `MQTT_COMPARE_PAYLOADS` — comma list of sizes, default `256`.
pub fn parse_payloads() -> Vec<usize> {
    let raw = std::env::var("MQTT_COMPARE_PAYLOADS").unwrap_or_else(|_| DEFAULT_PAYLOAD_SIZE.to_string());
    let mut out = Vec::new();
    for part in raw.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let size: usize = part
            .parse()
            .unwrap_or_else(|_| panic!("invalid MQTT_COMPARE_PAYLOADS entry {part:?}"));
        assert!(size > 0, "payload size must be > 0");
        out.push(size);
    }
    assert!(!out.is_empty(), "MQTT_COMPARE_PAYLOADS must list at least one size");
    out
}

/// `MQTT_COMPARE_QOS` — `0`, `1`, or `0,1` (default `1`).
pub fn parse_qos_list() -> Vec<BenchQos> {
    let raw = std::env::var("MQTT_COMPARE_QOS").unwrap_or_else(|_| "1".into());
    let mut out = Vec::new();
    for part in raw.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        out.push(match part {
            "0" => BenchQos::AtMostOnce,
            "1" => BenchQos::AtLeastOnce,
            other => panic!("invalid MQTT_COMPARE_QOS entry {other:?}: expected 0 or 1"),
        });
    }
    assert!(!out.is_empty(), "MQTT_COMPARE_QOS must list at least one value");
    out
}

/// `MQTT_COMPARE_ACK` — `after_enqueue`, `before`, or both (default both).
pub fn parse_ack_policies() -> Vec<AckPolicy> {
    let raw = std::env::var("MQTT_COMPARE_ACK").unwrap_or_else(|_| "after_enqueue,before".into());
    let mut out = Vec::new();
    for part in raw.split(',') {
        let part = part.trim().to_ascii_lowercase();
        if part.is_empty() {
            continue;
        }
        out.push(match part.as_str() {
            "after_enqueue" | "after" | "enqueue" => AckPolicy::AfterEnqueue,
            "before" | "auto" => AckPolicy::Before,
            other => panic!("invalid MQTT_COMPARE_ACK entry {other:?}"),
        });
    }
    assert!(!out.is_empty(), "MQTT_COMPARE_ACK must list at least one policy");
    out
}

/// `MQTT_COMPARE_SCENARIOS` — comma list of directions (default all load+latency).
pub fn parse_directions() -> Vec<Direction> {
    let raw = std::env::var("MQTT_COMPARE_SCENARIOS").unwrap_or_else(|_| {
        "ingress_load,outbound_concurrent,outbound_spawn,outbound_rate,outbound_pipelined,ingress_serial,outbound_serial_latency"
            .into()
    });
    let mut out = Vec::new();
    for part in raw.split(',') {
        let part = part.trim().to_ascii_lowercase();
        if part.is_empty() {
            continue;
        }
        out.push(match part.as_str() {
            "ingress_load" => Direction::IngressLoad,
            "outbound_pipelined" => Direction::OutboundPipelined,
            "outbound_concurrent" => Direction::OutboundConcurrent,
            "outbound_spawn" => Direction::OutboundSpawn,
            "outbound_rate" => Direction::OutboundRate,
            "ingress_serial" => Direction::IngressSerial,
            "outbound_serial_latency" => Direction::OutboundSerialLatency,
            other => panic!("invalid MQTT_COMPARE_SCENARIOS entry {other:?}"),
        });
    }
    assert!(
        !out.is_empty(),
        "MQTT_COMPARE_SCENARIOS must list at least one scenario"
    );
    out
}
