//! Leave without losing messages when Kubernetes (or systemd) stops the process.
//!
//! Kubernetes sends SIGTERM and waits `terminationGracePeriodSeconds` (default 30 s) before
//! SIGKILL. A QoS 1 message is `PUBACK`ed once it is in the inbound channel, so everything
//! in that channel must be handled before the process exits:
//!
//! 1. Shared subscription with Clean Start (each replica its own client ID): unsubscribe
//!    first. The broker must stop routing new messages to this replica [MQTT-3.10.4-2] and
//!    finish the QoS 1 deliveries it has started [MQTT-3.10.4-3], so the other replicas take
//!    over without relying on the broker to re-route a closed session's messages. With a
//!    persistent session skip this step: it would delete the subscription the next process
//!    wants to resume.
//! 2. `shutdown()`: queued publishes are sent and their `PUBACK`s awaited, inbound is still
//!    delivered and acknowledged, then `DISCONNECT`; bounded by `graceful_disconnect_timeout`.
//! 3. Keep handling messages until the channel ends: it closes once the driver has finished,
//!    so nothing acknowledged is left unhandled. Then flush your own buffers.
//!
//! The worker keeps draining the channel during steps 1 and 2: the read task waits for room
//! in a full channel, and the `UNSUBACK` arrives behind the messages already sent.
//!
//! ```shell
//! MQTT_GLIDE_BROKER=mqtt://127.0.0.1:1883 cargo run --example graceful_shutdown
//! ```

use std::time::Duration;

use mqtt_glide::{ClientBuilder, InboundMessage, QoS, StandardReconnectPolicy, shared_filter};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let broker = std::env::var("MQTT_GLIDE_BROKER").unwrap_or_else(|_| "mqtt://127.0.0.1:1883".into());
    let (tx, mut rx) = mpsc::channel(1024);

    let client = ClientBuilder::new(&broker)?
        .reconnect(StandardReconnectPolicy::new(
            Duration::from_secs(1),
            Duration::from_secs(30),
        ))
        .inbound_sink(tx)
        .connect()
        .await?;

    let filter = shared_filter("glide-workers", "demo/glide/jobs/#")?;
    client.subscribe([(filter.as_str(), QoS::AtLeastOnce)]).await?;

    let mut worker = tokio::spawn(async move {
        while let Some(message) = rx.recv().await {
            handle(message).await;
        }
        // The driver has finished: every acknowledged message is handled.
        // Flush batches, commit offsets, close database writers here.
    });

    tokio::select! {
        () = terminate() => {}
        _ = &mut worker => return Err("connection ended before shutdown".into()),
    }

    // 1. Hand the group's work to the other replicas.
    if let Err(error) = client.unsubscribe([filter.as_str()]).await {
        eprintln!("unsubscribe before shutdown failed: {error}");
    }
    // 2. Drain and DISCONNECT.
    client.shutdown().await?;
    // 3. Wait until the worker has handled the rest.
    worker.await?;
    Ok(())
}

async fn handle(message: InboundMessage) {
    println!("{} => {} bytes", message.topic, message.payload.len());
}

/// Resolves on SIGTERM (what Kubernetes sends) or Ctrl+C.
async fn terminate() {
    #[cfg(unix)]
    {
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = sigterm.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}
