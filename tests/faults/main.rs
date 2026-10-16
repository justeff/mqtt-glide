//! Fault-injection tests: mqtt-glide against a real Mosquitto behind an in-process TCP
//! fault proxy ([`mqtt_glide::testing::FaultProxy`]).
//!
//! An observer client connects to the broker directly (not through the proxy) and records
//! what actually reached it, so delivery claims are checked end to end. Needs Docker /
//! Podman like the `brokers` tests; without it every test returns early.

#[path = "../brokers/helpers.rs"]
#[allow(dead_code)]
mod helpers;

#[path = "../common/mod.rs"]
mod common;

use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::static_credentials;
use mqtt_glide::{
    AppError, BrokerUrl, ClientId, ConnectOptions, Connection, ConnectionOutcome, InboundMessage, MqttClient,
    NoopLifecycle, Publish, QoS, ReconnectPolicy, SessionPhase, connect,
};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::timeout;

use mqtt_glide::testing::FaultProxy;

/// Reconnect fast on transport / protocol loss; stop otherwise.
struct FastReconnect;

impl ReconnectPolicy for FastReconnect {
    fn delay_after(&self, outcome: &ConnectionOutcome) -> Option<Duration> {
        match outcome {
            ConnectionOutcome::Transport(_) | ConnectionOutcome::Protocol(_) => Some(Duration::from_millis(50)),
            _ => None,
        }
    }
}

struct Session {
    client: MqttClient,
    phase: watch::Receiver<SessionPhase>,
    run: JoinHandle<Result<(), ConnectionOutcome>>,
    inbound: mpsc::Receiver<InboundMessage>,
}

fn options(port: u16) -> ConnectOptions {
    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("broker url");
    ConnectOptions::new(url)
}

async fn start(options: ConnectOptions) -> Session {
    let (tx, inbound) = mpsc::channel::<InboundMessage>(10_000);
    let (client, connection, mut phase): (_, Connection<_>, _) =
        connect(options, static_credentials(), FastReconnect, tx, NoopLifecycle).expect("connect wiring");
    let run = tokio::spawn(connection.run());
    await_phase(&mut phase, "initial connect", |p| {
        matches!(p, SessionPhase::Connected { .. })
    })
    .await;
    Session {
        client,
        phase,
        run,
        inbound,
    }
}

async fn await_phase(
    phase: &mut watch::Receiver<SessionPhase>,
    label: &str,
    predicate: impl Fn(&SessionPhase) -> bool,
) {
    timeout(Duration::from_secs(10), phase.wait_for(|p| predicate(p)))
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {label}"))
        .unwrap_or_else(|_| panic!("phase watch closed while waiting for {label}"));
}

async fn stop(session: Session) {
    session.client.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), session.run).await;
}

/// Observer connected straight to the broker, subscribed to `filter` at QoS 1.
async fn observer(broker_port: u16, filter: &str) -> Session {
    let session = start(options(broker_port)).await;
    session
        .client
        .subscribe([(filter, QoS::AtLeastOnce)])
        .await
        .expect("observer subscribe");
    session
}

fn index_payload(i: u32) -> Bytes {
    Bytes::copy_from_slice(&i.to_be_bytes())
}

fn payload_index(payload: &[u8]) -> u32 {
    u32::from_be_bytes(payload.try_into().expect("4-byte index payload"))
}

/// Collects observed indices until `done(seen)` holds or `deadline` passed.
async fn collect_indices(
    inbound: &mut mpsc::Receiver<InboundMessage>,
    deadline: Duration,
    done: impl Fn(&BTreeSet<u32>) -> bool,
) -> BTreeSet<u32> {
    let mut seen = BTreeSet::new();
    let end = Instant::now() + deadline;
    while !done(&seen) {
        let Some(left) = end.checked_duration_since(Instant::now()) else {
            break;
        };
        match timeout(left, inbound.recv()).await {
            Ok(Some(message)) => {
                seen.insert(payload_index(&message.payload));
            }
            _ => break,
        }
    }
    seen
}

/// Publishes `count` QoS 1 messages; calls `inject` once `inject_after` have been
/// enqueued; returns each publish's result in order.
async fn burst_with_fault(
    client: &MqttClient,
    topic: &str,
    count: u32,
    inject_after: u32,
    inject: impl FnOnce(),
) -> Vec<Result<(), AppError>> {
    let mut inject = Some(inject);
    let mut completions = Vec::with_capacity(count as usize);
    for i in 0..count {
        if i == inject_after
            && let Some(inject) = inject.take()
        {
            inject();
        }
        let publish = Publish::new(topic.to_owned(), index_payload(i)).qos(QoS::AtLeastOnce);
        completions.push(client.start_publish(publish).await.expect("enqueue"));
    }
    let mut results = Vec::with_capacity(completions.len());
    for completion in completions {
        results.push(completion.wait().await);
    }
    results
}

/// Resumed session: an RST mid-burst loses nothing. Every publish completes successfully
/// (unacknowledged ones are resent with DUP after the reconnect) and reaches the broker.
#[tokio::test]
async fn reset_mid_burst_resumed_session_delivers_everything() {
    let Some((_broker, broker_port)) = helpers::start_mosquitto().await else {
        return;
    };
    let mut watcher = observer(broker_port, "faults/resume").await;
    let proxy = FaultProxy::start(([127, 0, 0, 1], broker_port).into())
        .await
        .expect("fault proxy");
    let session = start(
        options(proxy.port())
            .client_id(ClientId::try_new("faults-resume").expect("client id"))
            .clean_start(false)
            .session_expiry(Some(Duration::from_secs(60))),
    )
    .await;

    let results = burst_with_fault(&session.client, "faults/resume", 2_000, 600, || proxy.reset_all()).await;
    for (i, result) in results.iter().enumerate() {
        assert!(
            result.is_ok(),
            "publish {i} must survive the reset in a resumed session: {result:?}"
        );
    }
    let seen = collect_indices(&mut watcher.inbound, Duration::from_secs(10), |seen| {
        seen.len() == 2_000
    })
    .await;
    assert_eq!(
        seen.len(),
        2_000,
        "broker must have received every publish at least once"
    );
    assert!(proxy.accepted() >= 2, "the reset must have forced a reconnect");

    stop(session).await;
    while watcher.inbound.try_recv().is_ok() {}
    stop(watcher).await;
}

/// Clean session: an RST mid-burst may fail in-flight publishes, but nothing hangs, every
/// publish reported as successful really reached the broker, and the client recovers.
#[tokio::test]
async fn reset_mid_burst_clean_session_reports_truthfully() {
    let Some((_broker, broker_port)) = helpers::start_mosquitto().await else {
        return;
    };
    let mut watcher = observer(broker_port, "faults/clean").await;
    let proxy = FaultProxy::start(([127, 0, 0, 1], broker_port).into())
        .await
        .expect("fault proxy");
    let session = start(options(proxy.port())).await;

    let results = timeout(
        Duration::from_secs(30),
        burst_with_fault(&session.client, "faults/clean", 2_000, 600, || proxy.reset_all()),
    )
    .await
    .expect("no publish may hang across the reset");
    let ok: BTreeSet<u32> = (0u32..)
        .zip(&results)
        .filter(|(_, r)| r.is_ok())
        .map(|(i, _)| i)
        .collect();
    for result in &results {
        assert!(
            matches!(result, Ok(()) | Err(AppError::ConnectionLost)),
            "only ConnectionLost may fail a publish here: {result:?}"
        );
    }
    // Publishes that failed with ConnectionLost may still have reached the broker (only
    // the PUBACK got lost), so wait for the acknowledged set, not for a count.
    let seen = collect_indices(&mut watcher.inbound, Duration::from_secs(10), |seen| ok.is_subset(seen)).await;
    let missing: Vec<_> = ok.difference(&seen).take(10).collect();
    assert!(
        missing.is_empty(),
        "acknowledged publishes missing at the broker: {missing:?}"
    );
    assert!(proxy.accepted() >= 2, "the reset must have forced a reconnect");

    session
        .client
        .publish("faults/clean", index_payload(999_999))
        .qos(QoS::AtLeastOnce)
        .await
        .expect("client works again after the reset");
    stop(session).await;
    stop(watcher).await;
}

/// A link that silently stops forwarding (no FIN, no RST) is detected by the keep-alive
/// watchdog, and the client reconnects once the link works again.
#[tokio::test]
async fn stalled_link_is_detected_by_keep_alive() {
    let Some((_broker, broker_port)) = helpers::start_mosquitto().await else {
        return;
    };
    let proxy = FaultProxy::start(([127, 0, 0, 1], broker_port).into())
        .await
        .expect("fault proxy");
    let mut session = start(options(proxy.port()).keep_alive(Duration::from_secs(1))).await;

    let stalled_at = Instant::now();
    proxy.set_stalled(true);
    await_phase(&mut session.phase, "stall detected", |p| {
        matches!(p, SessionPhase::Disconnected(_))
    })
    .await;
    let detected_after = stalled_at.elapsed();
    assert!(
        detected_after < Duration::from_secs(4),
        "dead link must be detected within ~2 keep-alive intervals, took {detected_after:?}"
    );
    assert!(!session.client.is_connected());

    proxy.set_stalled(false);
    await_phase(&mut session.phase, "reconnected", |p| {
        matches!(p, SessionPhase::Connected { .. })
    })
    .await;
    session
        .client
        .publish("faults/stall", "after")
        .await
        .expect("publish after recovery");
    stop(session).await;
}

/// Every byte in its own TCP write: packets split at arbitrary boundaries in both
/// directions must still encode, decode and round-trip intact.
#[tokio::test]
async fn byte_by_byte_link_round_trips() {
    let Some((_broker, broker_port)) = helpers::start_mosquitto().await else {
        return;
    };
    let proxy = FaultProxy::start(([127, 0, 0, 1], broker_port).into())
        .await
        .expect("fault proxy");
    proxy.set_slice(Some(1));
    let mut session = start(options(proxy.port())).await;
    session
        .client
        .subscribe([("faults/slice/#", QoS::AtLeastOnce)])
        .await
        .expect("subscribe through a sliced link");

    let sizes: Vec<usize> = (0..30).map(|i| i * 97).collect();
    for (i, size) in sizes.iter().enumerate() {
        let payload: Vec<u8> = (0..*size).map(|b| (b % 251) as u8).collect();
        session
            .client
            .publish(format!("faults/slice/{i}"), payload)
            .qos(QoS::AtLeastOnce)
            .user_property("index", i.to_string())
            .await
            .expect("publish through a sliced link");
    }
    for (i, size) in sizes.iter().enumerate() {
        let message = timeout(Duration::from_secs(10), session.inbound.recv())
            .await
            .expect("echo arrives")
            .expect("sink open");
        assert_eq!(&*message.topic, format!("faults/slice/{i}"));
        assert_eq!(message.payload.len(), *size);
        assert!(message.payload.iter().enumerate().all(|(b, v)| *v == (b % 251) as u8));
    }
    stop(session).await;
}

/// High latency: a pipelined QoS 1 burst overlaps round trips instead of paying one per
/// message.
#[tokio::test]
async fn pipelined_burst_overlaps_round_trips_on_slow_link() {
    let Some((_broker, broker_port)) = helpers::start_mosquitto().await else {
        return;
    };
    let proxy = FaultProxy::start(([127, 0, 0, 1], broker_port).into())
        .await
        .expect("fault proxy");
    proxy.set_latency(Duration::from_millis(25));
    let session = start(options(proxy.port())).await;

    let started = Instant::now();
    let publishes = (0..300u32).map(|i| Publish::new("faults/latency", index_payload(i)).qos(QoS::AtLeastOnce));
    session
        .client
        .publish_batch(publishes, NonZeroUsize::new(64).expect("nz"))
        .await
        .expect("burst over a slow link");
    let elapsed = started.elapsed();
    // Serial would need ≥ 300 × 50 ms = 15 s.
    assert!(
        elapsed < Duration::from_secs(5),
        "burst must pipeline, took {elapsed:?}"
    );
    stop(session).await;
}

/// Shutdown over a dead link still finishes within the graceful budget.
#[tokio::test]
async fn shutdown_on_stalled_link_respects_budget() {
    let Some((_broker, broker_port)) = helpers::start_mosquitto().await else {
        return;
    };
    let proxy = FaultProxy::start(([127, 0, 0, 1], broker_port).into())
        .await
        .expect("fault proxy");
    let session = start(options(proxy.port()).graceful_disconnect_timeout(Duration::from_millis(300))).await;

    proxy.set_stalled(true);
    let completion = session
        .client
        .try_start_publish(Publish::new("faults/shutdown", "x").qos(QoS::AtLeastOnce))
        .expect("enqueue");
    let started = Instant::now();
    session.client.shutdown().await.expect("shutdown");
    let result = timeout(Duration::from_secs(3), session.run)
        .await
        .expect("driver stops");
    assert!(
        started.elapsed() < Duration::from_millis(1_500),
        "budget exceeded: {:?}",
        started.elapsed()
    );
    assert!(matches!(result, Ok(Err(ConnectionOutcome::Shutdown(_))) | Ok(Ok(()))));
    assert!(matches!(completion.wait().await, Err(AppError::ConnectionLost)));
}

/// While the broker side refuses connections the client keeps retrying; once it accepts
/// again the session comes back.
#[tokio::test]
async fn refused_reconnects_recover() {
    let Some((_broker, broker_port)) = helpers::start_mosquitto().await else {
        return;
    };
    let proxy = FaultProxy::start(([127, 0, 0, 1], broker_port).into())
        .await
        .expect("fault proxy");
    let mut session = start(options(proxy.port())).await;

    proxy.set_refusing(true);
    proxy.reset_all();
    await_phase(&mut session.phase, "dropped", |p| {
        matches!(p, SessionPhase::Disconnected(_))
    })
    .await;
    common::wait_until(
        Duration::from_secs(5),
        || proxy.accepted() >= 4,
        "several refused attempts",
    )
    .await;

    proxy.set_refusing(false);
    await_phase(&mut session.phase, "recovered", |p| {
        matches!(p, SessionPhase::Connected { .. })
    })
    .await;
    session
        .client
        .publish("faults/refused", "back")
        .await
        .expect("publish after recovery");
    stop(session).await;
}

/// Topic Aliases are per connection: after an RST the client must bind the alias again
/// before reusing it (the broker would otherwise reject the PUBLISH), including for the
/// publishes it resends into the resumed session ([MQTT-3.3.2-7]).
#[tokio::test]
async fn reset_with_topic_aliases_rebinds_on_the_new_connection() {
    let Some((_broker, broker_port)) = helpers::start_mosquitto().await else {
        return;
    };
    let mut watcher = observer(broker_port, "faults/alias").await;
    let proxy = FaultProxy::start(([127, 0, 0, 1], broker_port).into())
        .await
        .expect("fault proxy");
    let session = start(
        options(proxy.port())
            .client_id(ClientId::try_new("faults-alias").expect("client id"))
            .clean_start(false)
            .session_expiry(Some(Duration::from_secs(60)))
            .outbound_topic_aliases(4),
    )
    .await;

    let results = burst_with_fault(&session.client, "faults/alias", 1_000, 300, || proxy.reset_all()).await;
    for (i, result) in results.iter().enumerate() {
        assert!(result.is_ok(), "publish {i} failed across the reset: {result:?}");
    }
    let seen = collect_indices(&mut watcher.inbound, Duration::from_secs(10), |seen| {
        seen.len() == 1_000
    })
    .await;
    assert_eq!(seen.len(), 1_000, "every aliased publish must reach the broker");
    assert!(session.client.is_connected(), "broker must not have rejected an alias");

    stop(session).await;
    stop(watcher).await;
}

/// Subscribes and publishes 20 messages of growing size over `url` after `slice` was
/// applied, and checks every echo arrives intact.
#[cfg(feature = "websocket")]
async fn websocket_roundtrip_over_sliced_link(url: String, proxy: &FaultProxy, slice: impl FnOnce(&FaultProxy)) {
    let mut session = start(ConnectOptions::new(BrokerUrl::parse(&url).expect("ws url"))).await;
    // Slice only after the HTTP upgrade: tungstenite rejects a handshake that arrives in
    // more than 64 reads averaging under 128 bytes as a slowloris attempt. Everything after
    // the upgrade (WebSocket frames, MQTT packets) is what these tests are about.
    slice(proxy);
    session
        .client
        .subscribe([("faults/ws/#", QoS::AtLeastOnce)])
        .await
        .expect("subscribe over sliced ws");
    for i in 0..20usize {
        let payload: Vec<u8> = (0..i * 211).map(|b| (b % 251) as u8).collect();
        session
            .client
            .publish(format!("faults/ws/{i}"), payload)
            .qos(QoS::AtLeastOnce)
            .await
            .expect("publish over sliced ws");
    }
    for i in 0..20usize {
        let message = timeout(Duration::from_secs(10), session.inbound.recv())
            .await
            .expect("echo")
            .expect("open");
        assert_eq!(&*message.topic, format!("faults/ws/{i}"));
        assert_eq!(message.payload.len(), i * 211);
        assert!(message.payload.iter().enumerate().all(|(b, v)| *v == (b % 251) as u8));
    }
    stop(session).await;
}

/// WebSocket over a link that forwards single bytes in both directions (HiveMQ): WebSocket
/// frames and the MQTT packets inside them arrive split at every boundary, on the client and
/// on the broker, and must still round-trip.
#[cfg(feature = "websocket")]
#[tokio::test]
async fn websocket_byte_by_byte_link_round_trips_hivemq() {
    let Some((_broker, _tcp_port, ws_port)) = helpers::start_hivemq_with_websocket().await else {
        return;
    };
    let proxy = FaultProxy::start(([127, 0, 0, 1], ws_port).into())
        .await
        .expect("fault proxy");
    let url = format!("ws://127.0.0.1:{}/mqtt", proxy.port());
    websocket_roundtrip_over_sliced_link(url, &proxy, |proxy| proxy.set_slice(Some(1))).await;
}

/// Same against Mosquitto's WebSocket listener (the 2.0 line, see
/// `helpers::start_mosquitto_websockets` for why not 2.1), sliced in both directions.
#[cfg(feature = "websocket")]
#[tokio::test]
async fn websocket_byte_by_byte_link_round_trips_mosquitto() {
    let Some((_broker, _tcp_port, ws_port)) = helpers::start_mosquitto_websockets().await else {
        return;
    };
    let proxy = FaultProxy::start(([127, 0, 0, 1], ws_port).into())
        .await
        .expect("fault proxy");
    let url = format!("ws://127.0.0.1:{}/mqtt", proxy.port());
    websocket_roundtrip_over_sliced_link(url, &proxy, |proxy| proxy.set_slice(Some(1))).await;
}

/// Resumed session over WebSocket: an RST mid-burst loses nothing (reconnect redoes the
/// WebSocket handshake, unacknowledged publishes are resent).
#[cfg(feature = "websocket")]
#[tokio::test]
async fn websocket_reset_mid_burst_resumed_session_delivers_everything() {
    let Some((_broker, tcp_port, ws_port)) = helpers::start_mosquitto_websockets().await else {
        return;
    };
    let mut watcher = observer(tcp_port, "faults/wsresume").await;
    let proxy = FaultProxy::start(([127, 0, 0, 1], ws_port).into())
        .await
        .expect("fault proxy");
    let url = BrokerUrl::parse(&format!("ws://127.0.0.1:{}/mqtt", proxy.port())).expect("ws url");
    let session = start(
        ConnectOptions::new(url)
            .client_id(ClientId::try_new("faults-ws-resume").expect("client id"))
            .clean_start(false)
            .session_expiry(Some(Duration::from_secs(60))),
    )
    .await;

    let results = burst_with_fault(&session.client, "faults/wsresume", 1_000, 300, || proxy.reset_all()).await;
    for (i, result) in results.iter().enumerate() {
        assert!(result.is_ok(), "publish {i} must survive the reset: {result:?}");
    }
    let seen = collect_indices(&mut watcher.inbound, Duration::from_secs(10), |seen| {
        seen.len() == 1_000
    })
    .await;
    assert_eq!(seen.len(), 1_000);
    assert!(proxy.accepted() >= 2, "the reset must have forced a reconnect");
    stop(session).await;
    stop(watcher).await;
}
