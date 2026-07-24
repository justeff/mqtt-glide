//! Absolute multi-client MQTT compare harness (no paired deltas).
//!
//! Runs a scenario matrix for each selected client and writes JSONL under
//! `target/mqtt_compare/<run_id>/` (override with `MQTT_COMPARE_OUT`).
//!
//! See `benches/README.md`.

mod compare;
mod support;

use compare::fixtures::{
    GLIDE_VERSION, LOAD_MESSAGES, LOAD_WINDOW, PEER_MQTT5_VERSION, PEER_RUMQTTC_VERSION, bench_runs, parse_clients,
};
use compare::report::{ResultWriter, RunMeta, output_dir};
use compare::run::MatrixCtx;
use compare::{build_matrix, execute_matrix};
use support::broker::bench_broker_backend;
use tokio::runtime::Runtime;

fn host_label() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "unknown-host".to_owned())
}

fn main() {
    match std::env::var("MQTT_COMPARE_ROLE").as_deref() {
        Ok("publisher") => {
            support::publisher::run_publisher_process();
            return;
        }
        Ok("mock_broker") => {
            support::mock_broker::run_mock_broker_process();
            return;
        }
        _ => {}
    }

    let rt = Runtime::new().expect("tokio runtime");
    let broker = rt.block_on(support::broker::start_bench_broker());
    let clients = parse_clients();
    let cells = build_matrix();
    let out = output_dir();
    let run_id = out.file_name().and_then(|s| s.to_str()).unwrap_or("run").to_owned();
    let writer = ResultWriter::create(out.clone(), &clients);
    let client_labels: Vec<&str> = clients.iter().map(|c| c.as_str()).collect();
    writer.write_meta(&RunMeta {
        run_id: &run_id,
        host: &host_label(),
        broker: broker.label(),
        runs: bench_runs(),
        load_messages: LOAD_MESSAGES,
        load_window: LOAD_WINDOW,
        clients: client_labels,
        glide_version: GLIDE_VERSION,
        rumqttc_version: PEER_RUMQTTC_VERSION,
        mqtt5_version: PEER_MQTT5_VERSION,
    });

    eprintln!("mqtt_compare absolute matrix");
    eprintln!("  out: {}", writer.dir().display());
    eprintln!("  broker: {}", broker.label());
    eprintln!(
        "  clients: {:?}",
        clients.iter().map(|c| c.as_str()).collect::<Vec<_>>()
    );
    eprintln!(
        "  versions: mqtt-glide {GLIDE_VERSION}, rumqttc-next {PEER_RUMQTTC_VERSION}, mqtt5 {PEER_MQTT5_VERSION}"
    );
    eprintln!("  cells: {}", cells.len());
    eprintln!("  runs (median-of): {}", bench_runs());

    rt.block_on(execute_matrix(
        MatrixCtx {
            port: broker.port(),
            backend: bench_broker_backend(),
            mock_stats: broker.mock_stats(),
        },
        &clients,
        &cells,
        &writer,
    ));
    eprintln!("done — results in {}", writer.dir().display());
}
