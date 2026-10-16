//! Broker integration tests (Mosquitto via testcontainers).
//!
//! Mosquitto integration: CONNECT/CONNACK, disconnect, auth failure, shared subscribe,
//! QoS1, keep-alive / reconnect / SessionPhase, User Properties, Message Expiry, shutdown.
//! parallel Connection smoke.
//!
//! When Docker is unavailable, Mosquitto helpers return `None` and each test early-returns
//! (see `skipping brokers test: docker unavailable`) instead of panicking.

mod helpers;
mod hivemq;
#[cfg(feature = "websocket")]
mod websocket;

#[path = "../common/mod.rs"]
mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::{NoReconnect, wait_for_phase};
use mqtt_glide::Password;
use mqtt_glide::{
    AuthError, ClientId, ConnectOptions, ConnectionOutcome, CredentialsProvider, InboundMessage, NoopLifecycle,
    Publish, QoS, ReconnectPolicy, SessionParams, SessionPhase, SubscribeAckReason, Subscription, connect,
    shared_filter,
};
use tokio::sync::mpsc;
use tokio::time::timeout;

use helpers::{
    connected, dynsec, publish_qos0_via_raw_session, publish_qos1_via_raw_session, start_mosquitto,
    start_mosquitto_dynsec, start_mosquitto_on_port, start_mosquitto_reject_connect, tcp_url,
};

/// Reconnect quickly on transport/protocol; stop on auth/shutdown/app.
struct TransportOnlyReconnect;

impl ReconnectPolicy for TransportOnlyReconnect {
    fn delay_after(&self, outcome: &ConnectionOutcome) -> Option<Duration> {
        match outcome {
            ConnectionOutcome::Transport(_) | ConnectionOutcome::Protocol(_) => Some(Duration::from_millis(100)),
            _ => None,
        }
    }
}

/// Like [`TransportOnlyReconnect`], but counts Transport `delay_after` consultations (AC1).
struct CountingTransportReconnect {
    transport_consults: Arc<AtomicUsize>,
}

impl ReconnectPolicy for CountingTransportReconnect {
    fn delay_after(&self, outcome: &ConnectionOutcome) -> Option<Duration> {
        match outcome {
            ConnectionOutcome::Transport(_) => {
                self.transport_consults.fetch_add(1, Ordering::SeqCst);
                Some(Duration::from_millis(100))
            }
            ConnectionOutcome::Protocol(_) => Some(Duration::from_millis(100)),
            _ => None,
        }
    }
}

/// Counts `delay_after` consultations; Auth → `None` (no endless reconnect).
struct CountingStopOnAuth {
    auth_consults: Arc<AtomicUsize>,
    transport_consults: Arc<AtomicUsize>,
}

impl ReconnectPolicy for CountingStopOnAuth {
    fn delay_after(&self, outcome: &ConnectionOutcome) -> Option<Duration> {
        match outcome {
            ConnectionOutcome::Auth(_) => {
                self.auth_consults.fetch_add(1, Ordering::SeqCst);
                None
            }
            ConnectionOutcome::Transport(_) | ConnectionOutcome::Protocol(_) => {
                self.transport_consults.fetch_add(1, Ordering::SeqCst);
                Some(Duration::from_millis(50))
            }
            _ => None,
        }
    }
}

struct CountingCredentials {
    fetches: Arc<AtomicUsize>,
    invalidations: Arc<AtomicUsize>,
}

impl CredentialsProvider for CountingCredentials {
    async fn credentials(&self) -> Result<(String, Password), AuthError> {
        self.fetches.fetch_add(1, Ordering::SeqCst);
        Ok(("user".into(), Password::from("pass")))
    }

    async fn invalidate(&self) {
        self.invalidations.fetch_add(1, Ordering::SeqCst);
    }
}

/// Options for the Mosquitto on `port` with a client id from `prefix`.
fn options(port: u16, prefix: &str) -> ConnectOptions {
    ConnectOptions::new(tcp_url(port)).client_id(ClientId::from_prefix(prefix))
}

/// Asserts that a single-filter SUBACK granted QoS 0 or 1.
fn assert_granted(reasons: &[SubscribeAckReason]) {
    assert!(
        matches!(
            reasons,
            [SubscribeAckReason::GrantedQos0 | SubscribeAckReason::GrantedQos1]
        ),
        "expected a granted QoS, got {reasons:?}"
    );
}

/// Receives the next inbound message within ten seconds.
async fn next(inbound: &mut mpsc::Receiver<InboundMessage>, what: &str) -> InboundMessage {
    timeout(Duration::from_secs(10), inbound.recv())
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {what}"))
        .expect("handoff closed")
}

/// Shared filter `$share/{group}/{topic}` against Mosquitto: SUBACK success + inbound PUBLISH to handoff.
#[tokio::test]
async fn mosquitto_shared_subscription_delivers_to_handoff() {
    let Some((_container, port)) = start_mosquitto().await else {
        return;
    };
    let mut c = connected(options(port, "share-sub"), NoReconnect, 8).await;

    let topic = "shared/demo";
    let filter = shared_filter("share-test", topic).expect("valid ShareName");
    assert!(
        filter.starts_with("$share/share-test/"),
        "shared_filter must produce $share/{{group}}/{{topic}}, got {filter}"
    );

    let reasons = c
        .client
        .subscribe(vec![Subscription::new(filter, QoS::AtLeastOnce)])
        .await
        .expect("shared subscribe must complete with SUBACK");
    assert_granted(&reasons);

    publish_qos0_via_raw_session(port, "shared-pub", topic, b"shared-payload").await;

    let enqueued = next(&mut c.inbound, "the shared delivery").await;
    assert_eq!(&*enqueued.topic, topic);
    assert_eq!(&*enqueued.payload, b"shared-payload");

    c.stop().await;
}

/// Default Mosquitto advertises Receive Maximum 20: a QoS 1 window of 64 must wait for
/// slots, not fail with `Backpressure`.
#[tokio::test]
async fn mosquitto_qos1_window_wider_than_receive_maximum() {
    use std::num::NonZeroUsize;

    let Some((_container, port)) = start_mosquitto().await else {
        return;
    };
    let c = connected(options(port, "qos1-window"), NoReconnect, 8).await;
    assert!(
        c.session.server_receive_maximum.get() < 64,
        "test needs a broker window narrower than the publish window, got {}",
        c.session.server_receive_maximum
    );

    let messages = (0..500).map(|i| Publish::new("qos1/window", format!("m{i}")).qos(QoS::AtLeastOnce));
    timeout(
        Duration::from_secs(20),
        c.client.publish_batch(messages, NonZeroUsize::new(64).expect("nz")),
    )
    .await
    .expect("publish_batch must not hang")
    .expect("publish_batch wider than the broker Receive Maximum must succeed");

    c.stop().await;
}

/// User Properties + Message Expiry Interval survive publish → subscribe echo via Mosquitto; the
/// QoS 1 `send` resolves on the broker's PUBACK. The MQTT exchange runs over plain TCP, an
/// ordered lossless byte stream ([MQTT-4.2.0-1]).
#[tokio::test]
async fn mosquitto_publish_properties_roundtrip() {
    use bytes::Bytes;
    use mqtt_glide::PublishProperties;

    let Some((_container, port)) = start_mosquitto().await else {
        return;
    };
    let mut c = connected(options(port, "props"), NoReconnect, 8).await;

    let topic = "props/echo";
    let reasons = c
        .client
        .subscribe(vec![Subscription::new(topic, QoS::AtLeastOnce)])
        .await
        .expect("subscribe");
    assert_granted(&reasons);

    let expiry = 60u32;
    timeout(
        Duration::from_secs(10),
        c.client.send(
            Publish::new(topic, Bytes::from_static(b"props-body"))
                .qos(QoS::AtLeastOnce)
                .properties(PublishProperties {
                    message_expiry_interval: Some(expiry),
                    user_properties: vec![("k1", "v1").into(), ("ts", "1").into()],
                    ..PublishProperties::default()
                }),
        ),
    )
    .await
    .expect("QoS 1 publish must not hang")
    .expect("publish with properties");

    let enqueued = next(&mut c.inbound, "the echo").await;
    assert_eq!(&*enqueued.topic, topic);
    assert_eq!(&*enqueued.payload, b"props-body");
    assert_eq!(
        enqueued.properties.message_expiry_interval,
        Some(expiry),
        "Message Expiry Interval must round-trip through the broker"
    );
    assert_eq!(
        enqueued.properties.user_properties.len(),
        2,
        "both user properties must round-trip; got {:?}",
        enqueued.properties.user_properties
    );
    assert!(
        enqueued
            .properties
            .user_properties
            .iter()
            .any(|p| p.name == "k1" && p.value == "v1"),
        "User Property k1=v1 must round-trip; got {:?}",
        enqueued.properties.user_properties
    );
    assert!(
        enqueued
            .properties
            .user_properties
            .iter()
            .any(|p| p.name == "ts" && p.value == "1"),
        "User Property ts=1 must round-trip; got {:?}",
        enqueued.properties.user_properties
    );

    c.stop().await;
}

// --- Keep-Alive, Reconnect, SessionPhase ---

/// AC1: Broker kill / TCP reset → reconnect per policy → Connected again.
#[tokio::test]
async fn mosquitto_broker_kill_reconnects_and_emits_connected() {
    let Some((container, port)) = start_mosquitto().await else {
        return;
    };
    let transport_consults = Arc::new(AtomicUsize::new(0));
    let policy = CountingTransportReconnect {
        transport_consults: Arc::clone(&transport_consults),
    };
    let mut c = connected(
        options(port, "reconnect-kill").keep_alive(Duration::from_secs(5)),
        policy,
        8,
    )
    .await;
    let gen1 = c.generation;
    assert!(gen1 >= 1, "first successful connect must bump generation");

    // Kill broker (TCP reset / connection loss) — drop releases the host port.
    drop(container);

    wait_for_phase(&mut c.phase, Duration::from_secs(15), |e| {
        matches!(e, SessionPhase::Disconnected(ConnectionOutcome::Transport(_)))
    })
    .await;

    // Restart on the same host port so the policy-driven reconnect can succeed.
    let Some(_broker) = start_mosquitto_on_port(port).await else {
        return;
    };

    let second = wait_for_phase(&mut c.phase, Duration::from_secs(20), |e| {
        matches!(
            e,
            SessionPhase::Connected {
                session: SessionParams { from_connack: true, .. },
                ..
            }
        )
    })
    .await;
    let SessionPhase::Connected { generation: gen2, .. } = second else {
        unreachable!("pred matched Connected");
    };
    assert!(
        gen2 > gen1,
        "reconnect Connected must use a newer generation (was {gen1}, got {gen2})"
    );
    assert!(
        transport_consults.load(Ordering::SeqCst) >= 1,
        "ReconnectPolicy::delay_after must be consulted for Transport after broker kill \
         (got {} consults) — Connected alone is not enough",
        transport_consults.load(Ordering::SeqCst)
    );

    c.stop().await;
}

/// AC2: Auth fail → Stopped + invalidate; policy returns None for Auth → no endless reconnect.
#[tokio::test]
async fn mosquitto_auth_fail_stops_without_endless_reconnect() {
    let Some((_container, port)) = start_mosquitto_reject_connect().await else {
        return;
    };
    let fetches = Arc::new(AtomicUsize::new(0));
    let invalidations = Arc::new(AtomicUsize::new(0));
    let auth_consults = Arc::new(AtomicUsize::new(0));
    let transport_consults = Arc::new(AtomicUsize::new(0));

    let creds = CountingCredentials {
        fetches: Arc::clone(&fetches),
        invalidations: Arc::clone(&invalidations),
    };
    let policy = CountingStopOnAuth {
        auth_consults: Arc::clone(&auth_consults),
        transport_consults: Arc::clone(&transport_consults),
    };

    let (tx, _rx) = mpsc::channel::<InboundMessage>(4);
    let (_client, connection, mut phase) =
        connect(options(port, "reconnect-auth"), creds, policy, tx, NoopLifecycle).expect("connect wiring");

    let run = tokio::spawn(connection.run());

    wait_for_phase(&mut phase, Duration::from_secs(10), |e| {
        matches!(
            e,
            SessionPhase::Disconnected(ConnectionOutcome::Auth(AuthError::NotAuthorized))
        )
    })
    .await;

    assert_eq!(
        invalidations.load(Ordering::SeqCst),
        1,
        "CredentialsProvider::invalidate must be invoked exactly once on Auth fail"
    );

    // Driver must stop — not spin reconnecting against dead credentials.
    let outcome = timeout(Duration::from_secs(3), run)
        .await
        .expect("run must finish after Auth stop (no endless reconnect)")
        .expect("run task join");
    assert!(
        matches!(outcome, Err(ConnectionOutcome::Auth(AuthError::NotAuthorized))),
        "Auth fail must end run as Stopped/Auth, got {outcome:?}"
    );

    let fetches_after = fetches.load(Ordering::SeqCst);
    assert!(
        (1..=2).contains(&fetches_after),
        "credentials must not be fetched in an endless loop; got {fetches_after} fetches"
    );

    // Policy may be consulted for Auth (returning None) or Auth may bypass delay —
    // either way transport reconnect must not run and Auth must not loop.
    assert_eq!(
        transport_consults.load(Ordering::SeqCst),
        0,
        "Auth path must not fall through to Transport reconnect"
    );
    assert!(
        auth_consults.load(Ordering::SeqCst) <= 1,
        "Auth delay_after must not be consulted repeatedly; got {}",
        auth_consults.load(Ordering::SeqCst)
    );

    // No successful session after Auth stop.
    if matches!(
        *phase.borrow(),
        SessionPhase::Connected {
            session: SessionParams { from_connack: true, .. },
            ..
        }
    ) {
        panic!("Auth fail must not leave phase Connected after Stopped");
    }
}

/// AC3: Full handoff + keep-alive — connection stays up via PINGs; no spurious reconnect from stall.
///
/// Hard keep-alive scheduling gate is the unit test
/// `keep_alive_survives_handoff_stall_without_spurious_reconnect` (mock enforcer).
/// This Mosquitto case covers broker-facing survival under a full handoff.
#[tokio::test]
async fn mosquitto_handoff_full_keep_alive_no_spurious_reconnect() {
    let Some((_container, port)) = start_mosquitto().await else {
        return;
    };
    // Short keep-alive so Mosquitto would drop us within a few seconds without PINGs.
    let keep_alive = Duration::from_secs(2);
    // Capacity 1: one fill saturates; next inbound publish stalls Read on handoff (§3.2).
    let mut c = connected(
        options(port, "keepalive").keep_alive(keep_alive),
        TransportOnlyReconnect,
        1,
    )
    .await;
    let gen_before = c.generation;

    let topic = "keepalive/stall";
    let reasons = c
        .client
        .subscribe(vec![Subscription::new(topic, QoS::AtLeastOnce)])
        .await
        .expect("subscribe");
    assert_granted(&reasons);

    // First publish fills the handoff; leave capacity full so the next inbound stalls Read.
    publish_qos0_via_raw_session(port, "keepalive-pub1", topic, b"fill").await;
    // Do not drain — leave the one slot occupied.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(c.inbound.len() == 1, "handoff must be full after first inbound publish");

    // Second publish parks Read on handoff push while Write must keep PINGing.
    publish_qos0_via_raw_session(port, "keepalive-pub2", topic, b"stall").await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Wait past 1.5 × keep_alive — Mosquitto disconnects idle clients without PINGREQ.
    // With keep-alive, Write must keep PINGing; stall must not cause a reconnect.
    let liveness_window = keep_alive.mul_f32(2.0) + Duration::from_secs(1);
    match timeout(
        liveness_window,
        c.phase.wait_for(|p| match p {
            SessionPhase::Disconnected(_) => true,
            SessionPhase::Connected { generation, .. } => *generation != gen_before,
            _ => false,
        }),
    )
    .await
    {
        Err(_) => {
            // No lifecycle transition — connection stayed up (expected).
        }
        Ok(Ok(guard)) => match &*guard {
            SessionPhase::Disconnected(outcome) => {
                panic!("handoff stall must not cause keepalive-death or disconnect; got Disconnected({outcome:?})");
            }
            SessionPhase::Connected { generation, .. } => {
                panic!(
                    "handoff stall must not cause spurious reconnect (Connected gen={generation}, was {gen_before})"
                );
            }
            other => panic!("unexpected phase change under stall: {other:?}"),
        },
        Ok(Err(_)) => panic!("phase watch closed while connection should stay alive under stall"),
    }

    // Drain so Read can finish the stalled push; connection still live.
    let fill = next(&mut c.inbound, "the first handoff message").await;
    assert_eq!(&*fill.payload, b"fill");
    let stalled = next(&mut c.inbound, "the stalled handoff message").await;
    assert_eq!(&*stalled.payload, b"stall");

    c.stop().await;
}

/// Mosquitto's Dynamic Security drops every client of a role whose ACLs change. When the
/// change revokes a filter, the client comes back, gets 0x87 for that filter on the
/// resubscribe, and stays connected with the rest instead of reconnecting over and over;
/// once the ACL is back, the next restore grants it.
#[tokio::test]
async fn mosquitto_revoked_role_acl_keeps_the_client_connected() {
    let Some((container, port)) = start_mosquitto_dynsec().await else {
        return;
    };
    dynsec(&container, &["createClient", "user", "-p", "pass"]).await;
    dynsec(&container, &["createRole", "device"]).await;
    for (acl_type, topic) in [
        ("subscribeLiteral", "acl/cmd"),
        ("subscribeLiteral", "acl/other"),
        ("publishClientSend", "acl/#"),
    ] {
        dynsec(&container, &["addRoleACL", "device", acl_type, topic, "allow"]).await;
    }
    dynsec(&container, &["addClientRole", "user", "device"]).await;

    let mut c = connected(
        ConnectOptions::new(tcp_url(port)).client_id(ClientId::try_from("acl-device").expect("client id")),
        TransportOnlyReconnect,
        16,
    )
    .await;
    let granted = c
        .client
        .subscribe([("acl/cmd", QoS::AtLeastOnce), ("acl/other", QoS::AtLeastOnce)])
        .await
        .expect("subscribe");
    assert!(granted.iter().all(|reason| reason.is_granted()), "{granted:?}");

    dynsec(&container, &["removeRoleACL", "device", "subscribeLiteral", "acl/cmd"]).await;
    let mut refused = c.client.refused_subscriptions();
    let listed = timeout(Duration::from_secs(20), refused.wait_for(|list| !list.is_empty()))
        .await
        .expect("refusal listed")
        .expect("watch open")
        .clone();
    assert_eq!(listed.len(), 1, "{listed:?}");
    assert_eq!(
        (&*listed[0].filter, listed[0].reason),
        ("acl/cmd", SubscribeAckReason::NotAuthorized)
    );
    let generation = match &*c.phase.borrow() {
        SessionPhase::Connected { generation, .. } => *generation,
        other => panic!("a refusal must not end the session: {other:?}"),
    };
    // The rest of the session works: the other subscription receives.
    c.client.publish("acl/other", "still").await.expect("publish");
    let message = next(&mut c.inbound, "acl/other").await;
    assert_eq!(&*message.payload, b"still");
    assert!(
        matches!(&*c.phase.borrow(), SessionPhase::Connected { generation: now, .. } if *now == generation),
        "no reconnect loop, got {:?}",
        *c.phase.borrow()
    );

    dynsec(
        &container,
        &["addRoleACL", "device", "subscribeLiteral", "acl/cmd", "allow"],
    )
    .await;
    timeout(Duration::from_secs(20), refused.wait_for(|list| list.is_empty()))
        .await
        .expect("granted again")
        .expect("watch open");
    c.client.publish("acl/cmd", "back").await.expect("publish");
    timeout(Duration::from_secs(10), async {
        loop {
            let message = c.inbound.recv().await.expect("handoff open");
            if &*message.payload == b"back" {
                break;
            }
        }
    })
    .await
    .expect("acl/cmd delivered again");

    c.stop().await;
}

/// AC4: After reconnect, prior subscriptions are active again (message arrives without re-subscribe).
#[tokio::test]
async fn mosquitto_resubscribes_after_reconnect() {
    let Some((container, port)) = start_mosquitto().await else {
        return;
    };
    let mut c = connected(
        options(port, "resub").keep_alive(Duration::from_secs(5)),
        TransportOnlyReconnect,
        8,
    )
    .await;

    let topic = "resub/demo";
    let reasons = c
        .client
        .subscribe(vec![Subscription::new(topic, QoS::AtLeastOnce)])
        .await
        .expect("initial subscribe");
    assert_granted(&reasons);

    publish_qos0_via_raw_session(port, "resub-pub1", topic, b"before-kill").await;
    let before = next(&mut c.inbound, "the pre-reconnect message").await;
    assert_eq!(&*before.payload, b"before-kill");

    drop(container);
    wait_for_phase(&mut c.phase, Duration::from_secs(15), |e| {
        matches!(e, SessionPhase::Disconnected(_))
    })
    .await;

    let Some(_broker) = start_mosquitto_on_port(port).await else {
        return;
    };
    wait_for_phase(&mut c.phase, Duration::from_secs(20), |e| {
        matches!(
            e,
            SessionPhase::Connected {
                session: SessionParams { from_connack: true, .. },
                ..
            }
        )
    })
    .await;

    // Do not call subscribe again — driver must re-issue prior filters after Clean Start reconnect.
    publish_qos0_via_raw_session(port, "resub-pub2", topic, b"after-reconnect").await;
    let after = next(
        &mut c.inbound,
        "the post-reconnect message (subscriptions were not restored)",
    )
    .await;
    assert_eq!(
        &*after.payload, b"after-reconnect",
        "message after reconnect must arrive on the prior subscription"
    );
    assert_eq!(&*after.topic, topic);

    c.stop().await;
}

/// handoff message remains receivable after `shutdown()` (Mosquitto path).
#[tokio::test]
async fn mosquitto_handoff_survives_shutdown() {
    let Some((_container, port)) = start_mosquitto().await else {
        return;
    };
    let topic = "handoff/survive";
    let c = connected(options(port, "handoff"), NoReconnect, 4).await;

    c.client
        .subscribe(vec![Subscription::new(topic, QoS::AtLeastOnce)])
        .await
        .expect("subscribe");
    publish_qos1_via_raw_session(port, "handoff-pub", topic, b"still-here").await;

    timeout(Duration::from_secs(10), async {
        while c.inbound.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("handoff fill");

    let mut inbound = c.stop().await;

    let got = timeout(Duration::from_secs(2), inbound.recv())
        .await
        .expect("recv after shutdown")
        .expect("handoff");
    assert_eq!(&*got.payload, b"still-here");
}

/// MQTT §3.7: two parallel `Connection`s against the same broker — isolated handoff + phase watch,
/// no global state; each receives only its own subscribed topic/message.
#[tokio::test]
async fn mosquitto_two_parallel_connections_isolated_handoff_and_phase() {
    let Some((_container, port)) = start_mosquitto().await else {
        return;
    };
    let a = connected(options(port, "parallel-a"), NoReconnect, 8).await;
    let mut b = connected(options(port, "parallel-b"), NoReconnect, 8).await;
    assert!(
        a.generation >= 1 && b.generation >= 1,
        "each Connection must own its generation"
    );

    let topic_a = "parallel/a";
    let topic_b = "parallel/b";

    let reasons_a = a
        .client
        .subscribe(vec![Subscription::new(topic_a, QoS::AtLeastOnce)])
        .await
        .expect("subscribe A");
    let reasons_b = b
        .client
        .subscribe(vec![Subscription::new(topic_b, QoS::AtLeastOnce)])
        .await
        .expect("subscribe B");
    assert_granted(&reasons_a);
    assert_granted(&reasons_b);

    // `a` is taken apart so its inbound sink outlives its shutdown below.
    let helpers::Client {
        client: client_a,
        inbound: mut rx_a,
        run: run_a,
        ..
    } = a;

    publish_qos0_via_raw_session(port, "parallel-pub-a", topic_a, b"only-for-a").await;
    publish_qos0_via_raw_session(port, "parallel-pub-b", topic_b, b"only-for-b").await;

    let msg_a = next(&mut rx_a, "A's message").await;
    let msg_b = next(&mut b.inbound, "B's message").await;

    assert_eq!(&*msg_a.topic, topic_a);
    assert_eq!(&*msg_a.payload, b"only-for-a");
    assert_eq!(&*msg_b.topic, topic_b);
    assert_eq!(&*msg_b.payload, b"only-for-b");

    // Cross-isolation without a soft absence window: queues empty, then unique probes
    // must land only on the owning connection.
    assert!(
        matches!(rx_a.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
        "A handoff must be empty after its own message"
    );
    assert!(
        matches!(b.inbound.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
        "B handoff must be empty after its own message"
    );

    publish_qos0_via_raw_session(port, "parallel-probe-a", topic_a, b"probe-a").await;
    publish_qos0_via_raw_session(port, "parallel-probe-b", topic_b, b"probe-b").await;

    let probe_a = next(&mut rx_a, "A's probe").await;
    let probe_b = next(&mut b.inbound, "B's probe").await;
    assert_eq!(&*probe_a.payload, b"probe-a");
    assert_eq!(&*probe_b.payload, b"probe-b");
    assert!(
        matches!(rx_a.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
        "A must not receive B's probe"
    );
    assert!(
        matches!(b.inbound.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
        "B must not receive A's probe"
    );

    // Shutting down A must not tear down B's session / phase watch.
    client_a.shutdown().await.expect("shutdown A");
    helpers::join_run_after_shutdown(run_a).await;

    // B still alive: publish again and receive on B only.
    publish_qos0_via_raw_session(port, "parallel-pub-b2", topic_b, b"still-b").await;
    let still_b = next(&mut b.inbound, "B's message after A's shutdown").await;
    assert_eq!(&*still_b.payload, b"still-b");
    assert!(
        matches!(
            rx_a.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected | mpsc::error::TryRecvError::Empty)
        ),
        "A must not receive after shutdown while B is probed"
    );

    // B must stay connected: phase remains Connected; no Disconnected within a short window.
    assert!(
        matches!(*b.phase.borrow(), SessionPhase::Connected { .. }),
        "B phase must remain Connected after A shutdown, got {:?}",
        *b.phase.borrow()
    );
    match timeout(Duration::from_millis(200), b.phase.changed()).await {
        Err(_) => {}
        Ok(Ok(())) => {
            let next = b.phase.borrow().clone();
            assert!(
                !matches!(next, SessionPhase::Disconnected(_)),
                "B must not disconnect when A shuts down; got {next:?}"
            );
        }
        Ok(Err(_)) => panic!("B phase watch must remain open after A shutdown"),
    }

    b.stop().await;
}

/// Outbound Topic Aliases accepted by Mosquitto (default Topic Alias Maximum 10).
#[tokio::test]
async fn mosquitto_outbound_topic_aliases() {
    let Some((_container, port)) = start_mosquitto().await else {
        return;
    };
    hivemq::outbound_aliases_roundtrip(port, "mosq/alias").await;
}

/// Mosquitto 2.1 creates Topic Aliases towards clients that accept them (it binds an alias on
/// the first message per topic, then sends an empty topic + alias): a subscriber with a
/// non-zero `topic_alias_maximum` must still see every message under its full topic. This
/// needs the 2.1 line; 2.0 never sends aliases.
#[tokio::test]
async fn mosquitto_broker_created_topic_aliases_resolve() {
    let Some((_container, port)) = start_mosquitto().await else {
        return;
    };
    let mut subscriber = connected(
        ConnectOptions::new(tcp_url(port)).topic_alias_maximum(4),
        NoReconnect,
        1_000,
    )
    .await;
    subscriber
        .client
        .subscribe([("alias-in/#", QoS::AtLeastOnce)])
        .await
        .expect("subscribe");

    let publisher = connected(ConnectOptions::new(tcp_url(port)), NoReconnect, 4).await;
    // Six topics, four aliases: the broker has to rebind aliases on the way.
    let mut expected = Vec::new();
    for round in 0..5 {
        for topic in 0..6 {
            let topic = format!("alias-in/sensor-{topic}/temperature");
            publisher
                .client
                .publish(topic.clone(), round.to_string())
                .qos(QoS::AtLeastOnce)
                .await
                .expect("publish");
            expected.push((topic, round.to_string()));
        }
    }
    for (topic, payload) in expected {
        let message = timeout(Duration::from_secs(5), subscriber.inbound.recv())
            .await
            .expect("delivery")
            .expect("open");
        assert_eq!(
            (&*message.topic, &*message.payload),
            (topic.as_str(), payload.as_bytes())
        );
    }
    assert!(
        subscriber.client.is_connected(),
        "alias resolution must not have failed the session"
    );

    publisher.stop().await;
    subscriber.stop().await;
}

/// Subscription Identifiers end to end: two overlapping subscriptions with different ids;
/// the message arrives tagged with both (one copy carrying both ids, or one copy per
/// subscription, both are allowed by the spec).
#[tokio::test]
async fn mosquitto_subscription_identifiers_tag_deliveries() {
    let Some((_container, port)) = start_mosquitto().await else {
        return;
    };
    let mut c = connected(ConnectOptions::new(tcp_url(port)), NoReconnect, 16).await;

    c.client
        .subscribe_with_id(11, [("subid/#", QoS::AtLeastOnce)])
        .await
        .expect("subscribe id 11");
    c.client
        .subscribe_with_id(22, [("subid/+/alarm", QoS::AtLeastOnce)])
        .await
        .expect("subscribe id 22");
    c.client
        .publish("subid/boiler/alarm", "hot")
        .qos(QoS::AtLeastOnce)
        .await
        .expect("publish");
    c.client
        .publish("subid/boiler/temp", "80")
        .qos(QoS::AtLeastOnce)
        .await
        .expect("publish");

    let mut alarm_ids = std::collections::BTreeSet::new();
    let mut temp_ids = std::collections::BTreeSet::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while let Some(left) = deadline.checked_duration_since(std::time::Instant::now()) {
        let Ok(Some(message)) = timeout(left, c.inbound.recv()).await else {
            break;
        };
        let target = if message.topic.ends_with("alarm") {
            &mut alarm_ids
        } else {
            &mut temp_ids
        };
        target.extend(message.subscription_ids.iter().map(|id| id.get()));
    }
    assert_eq!(
        alarm_ids.into_iter().collect::<Vec<_>>(),
        [11, 22],
        "matches both subscriptions"
    );
    assert_eq!(
        temp_ids.into_iter().collect::<Vec<_>>(),
        [11],
        "matches only the wildcard"
    );

    c.stop().await;
}

/// Request / response end to end: a responder answers with `Publish::reply_to`, concurrent
/// requests each get their own response (correlation), responses never reach the
/// requester's inbound sink, and an unanswered request times out.
#[tokio::test]
async fn mosquitto_request_response() {
    use mqtt_glide::AppError;

    let Some((_container, port)) = start_mosquitto().await else {
        return;
    };

    let helpers::Client {
        client: responder,
        inbound: mut requests,
        run: responder_run,
        ..
    } = connected(ConnectOptions::new(tcp_url(port)), NoReconnect, 64).await;
    responder
        .subscribe([("rr/echo", QoS::AtLeastOnce)])
        .await
        .expect("responder subscribe");
    let answering = tokio::spawn({
        let responder = responder.clone();
        async move {
            while let Some(request) = requests.recv().await {
                let mut answer = b"re:".to_vec();
                answer.extend_from_slice(&request.payload);
                let reply = Publish::reply_to(&request, answer).expect("request carries a response topic");
                responder.send(reply.qos(QoS::AtLeastOnce)).await.expect("reply");
            }
        }
    });

    let requester_client = connected(ConnectOptions::new(tcp_url(port)), NoReconnect, 64).await;
    let requester = requester_client
        .client
        .requester("rr/replies/client-a")
        .await
        .expect("requester");
    let mut asks = tokio::task::JoinSet::new();
    for i in 0..20 {
        let requester = requester.clone();
        asks.spawn(async move {
            let reply = requester
                .request("rr/echo", format!("q{i}"))
                .qos(QoS::AtLeastOnce)
                .await
                .expect("response");
            assert_eq!(
                &*reply.payload,
                format!("re:q{i}").as_bytes(),
                "each request gets its own response"
            );
        });
    }
    while let Some(done) = asks.join_next().await {
        done.expect("request task");
    }
    assert!(
        requester_client.inbound.is_empty(),
        "responses must not reach the inbound sink"
    );

    let err = requester
        .request("rr/nobody-listens", "hello?")
        .timeout(Duration::from_millis(300))
        .await
        .expect_err("no responder");
    assert!(matches!(err, AppError::ResponseTimeout(_)), "{err:?}");

    answering.abort();
    requester_client.stop().await;
    responder.shutdown().await.expect("shutdown");
    helpers::join_run_after_shutdown(responder_run).await;
}
