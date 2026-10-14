//! Absolute JSONL + meta.json writers (no paired deltas).

use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

use super::harness::ClientId;

/// Run metadata written once as `meta.json`.
#[derive(Debug, Serialize)]
pub struct RunMeta<'a> {
    /// Output directory path.
    pub run_id: &'a str,
    /// Host label.
    pub host: &'a str,
    /// Broker label.
    pub broker: &'a str,
    /// Median-of runs.
    pub runs: usize,
    /// Load burst size.
    pub load_messages: usize,
    /// Pipelined in-flight window / ingress publisher window.
    pub load_window: usize,
    /// `outbound_concurrent` caller-task levels.
    pub concurrency: Vec<u32>,
    /// `outbound_rate` offered msg/s levels.
    pub rates: Vec<u32>,
    /// Seconds per `outbound_rate` run.
    pub rate_secs: f64,
    /// CPU affinity of this process (from `/proc/self/status`), e.g. `0-3`.
    pub client_cpus: String,
    /// `MQTT_COMPARE_BROKER_CPUS` (mock broker child), if set.
    pub broker_cpus: Option<String>,
    /// `MQTT_COMPARE_PUBLISHER_CPUS` (ingress publisher child), if set.
    pub publisher_cpus: Option<String>,
    /// Selected clients.
    pub clients: Vec<&'a str>,
    /// `mqtt-glide` crate version under test.
    pub glide_version: &'a str,
    /// Locked `rumqttc-next` version under test.
    pub rumqttc_version: &'a str,
    /// Locked LabOverWire `mqtt5` version under test.
    pub mqtt5_version: &'a str,
}

/// Measured values of one cell (median across runs, per metric).
#[derive(Debug, Clone, Copy, Default)]
pub struct Metrics {
    /// Sustained msg/s.
    pub msg_s: Option<f64>,
    /// Process CPU nanoseconds per message.
    pub cpu_ns_per_msg: Option<f64>,
    /// Per-message latency p50 (µs).
    pub p50_us: Option<f64>,
    /// Per-message latency p99 (µs).
    pub p99_us: Option<f64>,
    /// Per-message latency p99.9 (µs).
    pub p999_us: Option<f64>,
}

/// Identity of one cell in a result row.
#[derive(Debug, Clone, Copy)]
pub struct CellKey {
    /// Client id.
    pub client: ClientId,
    /// Scenario direction label.
    pub scenario: &'static str,
    /// QoS 0 or 1.
    pub qos: u8,
    /// Payload bytes.
    pub payload: usize,
    /// Ack policy label, if applicable.
    pub ack: Option<&'static str>,
    /// Caller tasks (concurrent) or offered msg/s (rate).
    pub level: Option<u32>,
}

/// One absolute result row (measured or N/A).
#[derive(Debug, Clone, Serialize)]
pub struct ResultRow {
    /// Client id.
    pub client: &'static str,
    /// Scenario direction.
    pub scenario: &'static str,
    /// QoS 0 or 1.
    pub qos: u8,
    /// Payload bytes.
    pub payload: usize,
    /// Ack policy label, if applicable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ack: Option<&'static str>,
    /// Caller tasks (`outbound_concurrent`) or offered msg/s (`outbound_rate`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub level: Option<u32>,
    /// `ok` or `n/a`.
    pub status: &'static str,
    /// Why skipped, if `n/a`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<&'static str>,
    /// Sustained msg/s (load scenarios).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msg_s: Option<f64>,
    /// CPU nanoseconds per message (load scenarios).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_ns_per_msg: Option<f64>,
    /// Latency p50 microseconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p50_us: Option<f64>,
    /// Latency p99 microseconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p99_us: Option<f64>,
    /// Latency p99.9 microseconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p999_us: Option<f64>,
    /// Number of runs aggregated.
    pub runs: usize,
}

impl ResultRow {
    /// Measured cell.
    pub fn measured(key: CellKey, metrics: Metrics, runs: usize) -> Self {
        Self {
            client: key.client.as_str(),
            scenario: key.scenario,
            qos: key.qos,
            payload: key.payload,
            ack: key.ack,
            level: key.level,
            status: "ok",
            reason: None,
            msg_s: metrics.msg_s,
            cpu_ns_per_msg: metrics.cpu_ns_per_msg,
            p50_us: metrics.p50_us,
            p99_us: metrics.p99_us,
            p999_us: metrics.p999_us,
            runs,
        }
    }

    /// Unsupported / skipped cell.
    pub fn na(key: CellKey, reason: &'static str, runs: usize) -> Self {
        Self {
            status: "n/a",
            reason: Some(reason),
            ..Self::measured(key, Metrics::default(), runs)
        }
    }
}

/// Resolves `MQTT_COMPARE_OUT` or `target/mqtt_compare/<timestamp>`.
pub fn output_dir() -> PathBuf {
    if let Ok(path) = std::env::var("MQTT_COMPARE_OUT") {
        return PathBuf::from(path);
    }
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    PathBuf::from(format!("target/mqtt_compare/{ts}"))
}

/// Per-client JSONL appenders + meta.
pub struct ResultWriter {
    dir: PathBuf,
}

impl ResultWriter {
    /// Creates the output directory and truncates JSONL for `clients` only.
    pub fn create(dir: PathBuf, clients: &[ClientId]) -> Self {
        fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("create {}: {e}", dir.display()));
        for client in clients {
            let path = dir.join(format!("{}.jsonl", client.as_str()));
            let _ = fs::remove_file(&path);
        }
        Self { dir }
    }

    /// Output directory.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Writes `meta.json`.
    pub fn write_meta(&self, meta: &RunMeta<'_>) {
        let path = self.dir.join("meta.json");
        let file = File::create(&path).unwrap_or_else(|e| panic!("create {}: {e}", path.display()));
        serde_json::to_writer_pretty(file, meta).expect("write meta.json");
    }

    /// Appends one JSONL row for `client`.
    pub fn write_row(&self, client: ClientId, row: &ResultRow) {
        let path = self.dir.join(format!("{}.jsonl", client.as_str()));
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .unwrap_or_else(|e| panic!("open {}: {e}", path.display()));
        let mut writer = BufWriter::new(file);
        serde_json::to_writer(&mut writer, row).expect("serialize result row");
        writer.write_all(b"\n").expect("jsonl newline");
        writer.flush().expect("flush jsonl");
    }
}

/// Prints a short human summary line for a row.
pub fn print_row_summary(row: &ResultRow) {
    let ack = row.ack.unwrap_or("-");
    let level = row.level.map(|l| format!(" level={l}")).unwrap_or_default();
    let head = format!(
        "  {} {} qos={} payload={} ack={}{}",
        row.client, row.scenario, row.qos, row.payload, ack, level
    );
    if row.status == "n/a" {
        println!("{head} → n/a ({})", row.reason.unwrap_or("unsupported"));
        return;
    }
    let mut parts = Vec::new();
    if let Some(msg_s) = row.msg_s {
        parts.push(format!("{msg_s:.0} msg/s"));
    }
    if let Some(cpu) = row.cpu_ns_per_msg {
        parts.push(format!("{cpu:.0} CPU ns/msg"));
    }
    if let (Some(p50), Some(p99)) = (row.p50_us, row.p99_us) {
        let p999 = row.p999_us.map(|v| format!(" p99.9={v:.0}µs")).unwrap_or_default();
        parts.push(format!("p50={p50:.0}µs p99={p99:.0}µs{p999}"));
    }
    println!("{head} → {}", parts.join(", "));
}
