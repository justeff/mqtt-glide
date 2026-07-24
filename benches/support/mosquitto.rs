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

fn mosquitto_image() -> ContainerRequest<GenericImage> {
    GenericImage::new("eclipse-mosquitto", "2.1.2-alpine")
        // `log_dest none` suppresses the usual "running" stderr lines — short delay is enough
        // for listen; clients still retry connect via their own timeouts.
        .with_wait_for(WaitFor::seconds(1))
        .with_copy_to("/mosquitto-test.conf", CONF_BENCH.as_bytes().to_vec())
        .with_cmd(["mosquitto", "-c", "/mosquitto-test.conf"])
}

/// Starts anonymous Mosquitto and returns `(container, host_port)`.
pub async fn start_mosquitto() -> (ContainerAsync<GenericImage>, u16) {
    let container = mosquitto_image()
        .start()
        .await
        .expect("failed to start Mosquitto container — is Podman/Docker running?");

    let port = container.get_host_port_ipv4(1883).await.expect("mapped mosquitto port");
    (container, port)
}
