//! Minimal Mosquitto testcontainer starter for benches (mirrors `tests/brokers/helpers.rs`).

use testcontainers::ContainerAsync;
use testcontainers::ContainerRequest;
use testcontainers::GenericImage;
use testcontainers::ImageExt;
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;

/// Bench-oriented Mosquitto config: anonymous, no persistence, no artificial
/// inflight/queue caps, TCP_NODELAY, quiet logs / no `$SYS` spam.
///
/// Does **not** remove Docker/testcontainers networking overhead or Mosquitto's
/// own single-threaded processing cost — only config-level client flow limits.
const CONF_BENCH: &str = "\
listener 1883\n\
allow_anonymous true\n\
persistence false\n\
autosave_interval 0\n\
set_tcp_nodelay true\n\
connection_messages false\n\
sys_interval 0\n\
log_dest none\n\
# 0 = unlimited (default inflight is 20; default queued is 1000).\n\
max_inflight_messages 0\n\
max_queued_messages 0\n\
";

/// Same as [`CONF_BENCH`] but with Mosquitto's stock flow-control limits, i.e. what a
/// client meets on an untuned broker (Receive Maximum 20 in CONNACK).
const CONF_STOCK: &str = "\
listener 1883\n\
allow_anonymous true\n\
persistence false\n\
autosave_interval 0\n\
set_tcp_nodelay true\n\
connection_messages false\n\
sys_interval 0\n\
log_dest none\n\
";

fn mosquitto_image(conf: &'static str) -> ContainerRequest<GenericImage> {
    GenericImage::new("eclipse-mosquitto", "2.1.2-alpine")
        // `log_dest none` suppresses the usual "running" stderr lines — short delay is enough
        // for listen; clients still retry connect via their own timeouts.
        .with_wait_for(WaitFor::seconds(1))
        .with_copy_to("/mosquitto-test.conf", conf.as_bytes().to_vec())
        .with_cmd(["mosquitto", "-c", "/mosquitto-test.conf"])
}

/// Starts anonymous Mosquitto (bench-tuned, or `stock` limits) and returns `(container, host_port)`.
pub async fn start_mosquitto(stock: bool) -> (ContainerAsync<GenericImage>, u16) {
    let container = mosquitto_image(if stock { CONF_STOCK } else { CONF_BENCH })
        .start()
        .await
        .expect("failed to start Mosquitto container — is Podman/Docker running?");

    let port = container.get_host_port_ipv4(1883).await.expect("mapped mosquitto port");
    pin_container(container.id());
    (container, port)
}

/// Applies `MQTT_COMPARE_BROKER_CPUS` to the running container so the broker stays off the
/// client's cores. Rootless Podman has no cpuset cgroup controller for the user, but its
/// container processes belong to the user, so `taskset` on the host PID works; rootful
/// Docker falls back to `docker update --cpuset-cpus`.
fn pin_container(id: &str) {
    use std::process::{Command, Stdio};

    let Some(cpus) = crate::compare::fixtures::child_cpus("MQTT_COMPARE_BROKER_CPUS") else {
        return;
    };
    let pid = Command::new("docker")
        .args(["inspect", "--format", "{{.State.Pid}}", id])
        .stderr(Stdio::null())
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_owned())
        .filter(|pid| !pid.is_empty() && pid != "0");
    let pinned_by_pid = pid.is_some_and(|pid| {
        Command::new("taskset")
            .args(["-a", "-p", "-c", &cpus, &pid])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|status| status.success())
    });
    if pinned_by_pid {
        return;
    }
    let status = Command::new("docker")
        .args(["update", "--cpuset-cpus", &cpus, id])
        .stdout(Stdio::null())
        .status()
        .expect("run `docker update` to pin the Mosquitto container");
    assert!(
        status.success(),
        "pinning Mosquitto to CPUs {cpus} failed (taskset and docker update)"
    );
}
