//! Pluggable broker backend (`MQTT_COMPARE_BROKER=mosquitto|mock`).

use std::process::{Child, Stdio};

use super::mock_broker;
use crate::compare::fixtures::{RECEIVE_MAXIMUM, pinned_command};

/// Selectable broker backend for benches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BenchBrokerBackend {
    /// eclipse-mosquitto via testcontainers (default baseline).
    Mosquitto,
    /// eclipse-mosquitto with its stock flow-control limits (Receive Maximum 20, 1000 queued).
    MosquittoStock,
    /// Out-of-process minimal MQTT 5 broker (no Podman/Docker).
    Mock,
}

impl BenchBrokerBackend {
    /// Human-readable label for comparison reports.
    pub fn report_label(self) -> &'static str {
        match self {
            Self::Mosquitto => {
                "eclipse-mosquitto:2.1.2-alpine (anonymous, bench-tuned: nodelay, inflight/queue unlimited, no $SYS/persistence; testcontainers)"
            }
            Self::MosquittoStock => {
                "eclipse-mosquitto:2.1.2-alpine (anonymous, nodelay, STOCK max_inflight_messages 20 / max_queued_messages 1000; testcontainers)"
            }
            Self::Mock => {
                "out-of-process mock MQTT 5 (receive_max 1024, multi-thread, Arc fanout, nodelay, publish counter; MQTT_COMPARE_BROKER=mock)"
            }
        }
    }
}

/// Parses `MQTT_COMPARE_BROKER` (default `mosquitto`).
pub fn bench_broker_backend() -> BenchBrokerBackend {
    match std::env::var("MQTT_COMPARE_BROKER")
        .unwrap_or_else(|_| "mosquitto".into())
        .to_ascii_lowercase()
        .as_str()
    {
        "mosquitto" => BenchBrokerBackend::Mosquitto,
        "mosquitto-stock" | "mosquitto_stock" => BenchBrokerBackend::MosquittoStock,
        "mock" => BenchBrokerBackend::Mock,
        other => panic!(
            "invalid MQTT_COMPARE_BROKER={other:?}: expected \"mosquitto\" (default), \"mosquitto-stock\" or \"mock\""
        ),
    }
}

/// Running broker for one `mqtt_compare` process (port + drop guard).
pub struct BenchBroker {
    port: u16,
    /// Mock wire-load counter (None for Mosquitto).
    mock_stats: Option<mock_broker::MockStatsPort>,
    label: &'static str,
    guard: BrokerGuard,
}

impl BenchBroker {
    /// Mapped host TCP port.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Mock inbound-PUBLISH stats port, when using [`BenchBrokerBackend::Mock`].
    pub fn mock_stats(&self) -> Option<mock_broker::MockStatsPort> {
        self.mock_stats
    }

    /// Label printed in the summary header.
    pub fn label(&self) -> &'static str {
        self.label
    }
}

enum BrokerGuard {
    /// Mosquitto container id. Its testcontainers handle is forgotten (dropping it needs a
    /// reactor on this thread), so [`BenchBroker`]'s drop removes the container itself —
    /// otherwise it would outlive the bench whenever Ryuk is disabled (rootless Podman).
    Mosquitto(String),
    /// Mock broker child process — killed and reaped on drop.
    Mock(Child),
}

/// Kills and reaps the child if dropped before ownership moves into [`BenchBroker`].
struct KillChildOnDrop(Option<Child>);

impl KillChildOnDrop {
    fn new(child: Child) -> Self {
        Self(Some(child))
    }

    fn child_mut(&mut self) -> &mut Child {
        self.0.as_mut().expect("mock broker child still owned")
    }

    fn into_child(mut self) -> Child {
        self.0.take().expect("mock broker child still owned")
    }
}

impl Drop for KillChildOnDrop {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// Starts the configured broker backend and returns port + lifetime guard.
pub async fn start_bench_broker() -> BenchBroker {
    match bench_broker_backend() {
        backend @ (BenchBrokerBackend::Mosquitto | BenchBrokerBackend::MosquittoStock) => {
            let stock = backend == BenchBrokerBackend::MosquittoStock;
            let (container, port) = super::mosquitto::start_mosquitto(stock).await;
            let id = container.id().to_owned();
            // Keep the container alive for the whole process; Drop needs a reactor on this thread.
            std::mem::forget(container);
            BenchBroker {
                port,
                mock_stats: None,
                label: backend.report_label(),
                guard: BrokerGuard::Mosquitto(id),
            }
        }
        BenchBrokerBackend::Mock => {
            let receive_max = RECEIVE_MAXIMUM.get();
            // Guard covers panic/timeout between spawn and BenchBroker construction.
            let mut pending = KillChildOnDrop::new(spawn_mock_broker_child(receive_max));
            let ready = mock_broker::read_mock_broker_ready(pending.child_mut());
            BenchBroker {
                port: ready.mqtt_port,
                mock_stats: Some(mock_broker::MockStatsPort::new(ready.stats_port)),
                label: BenchBrokerBackend::Mock.report_label(),
                guard: BrokerGuard::Mock(pending.into_child()),
            }
        }
    }
}

fn spawn_mock_broker_child(receive_max: u16) -> Child {
    let exe = std::env::current_exe().expect("current_exe for mock broker child");
    pinned_command(&exe, "MQTT_COMPARE_BROKER_CPUS")
        .env("MQTT_COMPARE_ROLE", "mock_broker")
        .env("MQTT_COMPARE_RECEIVE_MAX", receive_max.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn mqtt_compare mock broker child")
}

impl Drop for BenchBroker {
    fn drop(&mut self) {
        match &mut self.guard {
            BrokerGuard::Mock(child) => {
                let _ = child.kill();
                let _ = child.wait();
            }
            BrokerGuard::Mosquitto(id) => {
                let _ = std::process::Command::new("docker")
                    .args(["rm", "-f", id.as_str()])
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status();
            }
        }
    }
}
