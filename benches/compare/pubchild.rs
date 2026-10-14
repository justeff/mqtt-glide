//! Shared publisher subprocess spawn for ingress load.

use std::process::{Child, Stdio};

use super::fixtures::{LOAD_WINDOW, pinned_command};
use super::harness::BenchQos;

/// Spawns the `MQTT_COMPARE_ROLE=publisher` child of this bench binary.
pub fn spawn_publisher_child(
    port: u16,
    messages: usize,
    client_id: &str,
    topic: &str,
    payload_size: usize,
    qos: BenchQos,
) -> Child {
    let exe = std::env::current_exe().expect("current_exe for publisher child");
    pinned_command(&exe, "MQTT_COMPARE_PUBLISHER_CPUS")
        .env("MQTT_COMPARE_ROLE", "publisher")
        .env("MQTT_COMPARE_PORT", port.to_string())
        .env("MQTT_COMPARE_MSGS", messages.to_string())
        .env("MQTT_COMPARE_WINDOW", LOAD_WINDOW.to_string())
        .env("MQTT_COMPARE_TOPIC", topic)
        .env("MQTT_COMPARE_PAYLOAD_SIZE", payload_size.to_string())
        .env("MQTT_COMPARE_CLIENT_ID", client_id)
        .env("MQTT_COMPARE_PUB_QOS", qos.as_u8().to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn mqtt_compare publisher child")
}

/// Waits for publisher child success.
pub fn wait_child(mut child: Child) {
    let status = child.wait().expect("publisher child wait");
    assert!(status.success(), "publisher child failed: {status}");
}
