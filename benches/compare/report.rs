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
    /// Pipelined in-flight window / concurrent caller count / ingress publisher window.
    pub load_window: usize,
    /// Selected clients.
    pub clients: Vec<&'a str>,
    /// `mqtt-glide` crate version under test.
    pub glide_version: &'a str,
    /// Locked `rumqttc-next` version under test.
    pub rumqttc_version: &'a str,
    /// Locked LabOverWire `mqtt5` version under test.
    pub mqtt5_version: &'a str,
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
    /// Number of runs aggregated.
    pub runs: usize,
}

impl ResultRow {
    /// Measured load cell.
    #[allow(clippy::too_many_arguments)]
    pub fn load(
        client: ClientId,
        scenario: &'static str,
        qos: u8,
        payload: usize,
        ack: Option<&'static str>,
        msg_s: f64,
        cpu_ns_per_msg: f64,
        runs: usize,
    ) -> Self {
        Self {
            client: client.as_str(),
            scenario,
            qos,
            payload,
            ack,
            status: "ok",
            reason: None,
            msg_s: Some(msg_s),
            cpu_ns_per_msg: Some(cpu_ns_per_msg),
            p50_us: None,
            p99_us: None,
            runs,
        }
    }

    /// Measured latency cell.
    #[allow(clippy::too_many_arguments)]
    pub fn latency(
        client: ClientId,
        scenario: &'static str,
        qos: u8,
        payload: usize,
        ack: Option<&'static str>,
        p50_us: f64,
        p99_us: f64,
        runs: usize,
    ) -> Self {
        Self {
            client: client.as_str(),
            scenario,
            qos,
            payload,
            ack,
            status: "ok",
            reason: None,
            msg_s: None,
            cpu_ns_per_msg: None,
            p50_us: Some(p50_us),
            p99_us: Some(p99_us),
            runs,
        }
    }

    /// Unsupported capability cell.
    #[allow(clippy::too_many_arguments)]
    pub fn na(
        client: ClientId,
        scenario: &'static str,
        qos: u8,
        payload: usize,
        ack: Option<&'static str>,
        reason: &'static str,
        runs: usize,
    ) -> Self {
        Self {
            client: client.as_str(),
            scenario,
            qos,
            payload,
            ack,
            status: "n/a",
            reason: Some(reason),
            msg_s: None,
            cpu_ns_per_msg: None,
            p50_us: None,
            p99_us: None,
            runs,
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
    match row.status {
        "n/a" => println!(
            "  {} {} qos={} payload={} ack={} → n/a ({})",
            row.client,
            row.scenario,
            row.qos,
            row.payload,
            ack,
            row.reason.unwrap_or("unsupported")
        ),
        _ if row.msg_s.is_some() => println!(
            "  {} {} qos={} payload={} ack={} → {:.1} msg/s, {:.0} CPU ns/msg",
            row.client,
            row.scenario,
            row.qos,
            row.payload,
            ack,
            row.msg_s.unwrap_or(0.0),
            row.cpu_ns_per_msg.unwrap_or(0.0)
        ),
        _ => println!(
            "  {} {} qos={} payload={} ack={} → p50={:.0}µs p99={:.0}µs",
            row.client,
            row.scenario,
            row.qos,
            row.payload,
            ack,
            row.p50_us.unwrap_or(0.0),
            row.p99_us.unwrap_or(0.0)
        ),
    }
}
