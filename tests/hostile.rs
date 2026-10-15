//! Hostile-broker regression tests: a scripted raw-TCP "evil broker" attacks the client.
//!
//! The broker side never uses the library codec: packets are assembled byte by byte, so malformed
//! input is possible. Per attack the suite checks that
//!
//! - nothing inside the library panics (a process-wide panic hook records panics raised from
//!   `src/` and its dependencies),
//! - nothing hangs (every public future resolves, the driver ends or keeps running as specified),
//! - protocol violations end the connection with the DISCONNECT reason code the spec asks for
//!   (0x81 Malformed Packet, 0x82 Protocol Error, 0x94 Topic Alias invalid, 0x95 Packet too
//!   large), and the client closes the socket itself,
//! - nothing is reconnected in a busy loop.
//!
//! Bounds that only prove "does not hang" are deliberately generous so the suite stays stable on a
//! loaded machine; assertions about timing behaviour (keep-alive detection, reconnect pacing)
//! count events against a wide margin instead of measuring short durations.
//!
//! Run with `cargo test --features testing --test hostile`. Two soak tests are `#[ignore]`d, see
//! [`soak_random_resets_qos0_qos1`] and [`soak_client_memory_is_flat_under_resets`].

use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Once};
use std::time::{Duration, Instant};

use mqtt_glide::testing::{FaultProxy, TestBroker};
use mqtt_glide::{
    AppError, BrokerUrl, ClientId, ConnectOptions, Connection, ConnectionOutcome, InboundMessage, InboundSink,
    MqttClient, NoopLifecycle, OnInboundSink, Publish, QoS, ReconnectPolicy, SessionPhase, StandardReconnectPolicy,
    StaticCredentials, connect,
};
use rstest::rstest;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::timeout;

// ---- Panic detection and bounds ----

static PANICS: Mutex<Vec<(String, String)>> = Mutex::new(Vec::new());
static HOOK: Once = Once::new();

/// Records every panic that does not originate in this file (library or dependency panics) with
/// the name of the thread it happened on (`#[tokio::test]` runs on the test's own thread).
fn install_panic_hook() {
    HOOK.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let file = info.location().map(|l| l.file().to_owned()).unwrap_or_default();
            if !file.ends_with("hostile.rs") {
                let thread = std::thread::current().name().unwrap_or("?").to_owned();
                PANICS.lock().unwrap().push((thread, format!("{file}: {info}")));
            }
            previous(info);
        }));
    });
}

/// Fails when the library panicked on this test's thread (`all_threads`: on any thread, for the
/// multi-threaded soak tests).
fn assert_no_library_panic_on(all_threads: bool) {
    let thread = std::thread::current().name().unwrap_or("?").to_owned();
    let panics: Vec<_> = PANICS
        .lock()
        .unwrap()
        .iter()
        .filter(|(t, _)| all_threads || *t == thread)
        .map(|(_, m)| m.clone())
        .collect();
    assert!(panics.is_empty(), "library panicked: {panics:#?}");
}

fn assert_no_library_panic() {
    assert_no_library_panic_on(false);
}

/// Bounds a step so a hang fails the test instead of stalling CI.
async fn within<T>(secs: u64, fut: impl std::future::IntoFuture<Output = T>) -> T {
    install_panic_hook();
    timeout(Duration::from_secs(secs), fut.into_future())
        .await
        .unwrap_or_else(|_| panic!("HANG: still running after {secs}s"))
}

// ---- Raw packet building ----

fn varint(mut n: usize) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let mut byte = (n % 128) as u8;
        n /= 128;
        if n > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if n == 0 {
            return out;
        }
    }
}

/// Fixed header byte, Remaining Length and body.
fn pkt(first: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![first];
    out.extend(varint(body.len()));
    out.extend_from_slice(body);
    out
}

fn utf8(s: &str) -> Vec<u8> {
    let mut out = (s.len() as u16).to_be_bytes().to_vec();
    out.extend_from_slice(s.as_bytes());
    out
}

/// Property encoders: identifier byte followed by the value.
mod prop {
    pub fn u8(id: u8, v: u8) -> Vec<u8> {
        vec![id, v]
    }
    pub fn u16(id: u8, v: u16) -> Vec<u8> {
        [&[id][..], &v.to_be_bytes()].concat()
    }
    pub fn u32(id: u8, v: u32) -> Vec<u8> {
        [&[id][..], &v.to_be_bytes()].concat()
    }
    pub fn varint(id: u8, v: usize) -> Vec<u8> {
        [&[id][..], &super::varint(v)].concat()
    }
}

/// Property length followed by the properties.
fn props(parts: &[Vec<u8>]) -> Vec<u8> {
    let body = parts.concat();
    [varint(body.len()), body].concat()
}

fn connack_with(session_present: bool, reason: u8, props_block: &[u8]) -> Vec<u8> {
    pkt(0x20, &[&[u8::from(session_present), reason][..], props_block].concat())
}

/// CONNACK Success with the given properties.
fn connack(parts: &[Vec<u8>]) -> Vec<u8> {
    connack_with(false, 0, &props(parts))
}

fn publish(topic: &str, qos: u8, pid: u16, prop_parts: &[Vec<u8>], payload: &[u8]) -> Vec<u8> {
    let mut body = utf8(topic);
    if qos > 0 {
        body.extend(pid.to_be_bytes());
    }
    body.extend(props(prop_parts));
    body.extend_from_slice(payload);
    pkt(0x30 | (qos << 1), &body)
}

fn puback(pid: u16) -> Vec<u8> {
    pkt(0x40, &pid.to_be_bytes())
}

fn suback(pid: u16, reasons: &[u8]) -> Vec<u8> {
    pkt(0x90, &[&pid.to_be_bytes()[..], &[0], reasons].concat())
}

fn unsuback(pid: u16, reasons: &[u8]) -> Vec<u8> {
    pkt(0xB0, &[&pid.to_be_bytes()[..], &[0], reasons].concat())
}

fn server_disconnect(reason: u8) -> Vec<u8> {
    pkt(0xE0, &[reason])
}

const PINGRESP: [u8; 2] = [0xD0, 0x00];

// ---- Evil broker ----

/// One packet the client sent: fixed header byte and body.
#[derive(Debug, Clone)]
struct Raw {
    first: u8,
    body: Vec<u8>,
}

impl Raw {
    fn kind(&self) -> u8 {
        self.first >> 4
    }
}

/// Decoded fixed fields of a PUBLISH the client sent.
#[derive(Debug, Clone)]
struct Sent {
    dup: bool,
    qos: u8,
    pid: u16,
}

fn parse_publish(raw: &Raw) -> Sent {
    let qos = (raw.first >> 1) & 3;
    let topic_len = usize::from(u16::from_be_bytes([raw.body[0], raw.body[1]]));
    let pid = if qos > 0 {
        u16::from_be_bytes([raw.body[2 + topic_len], raw.body[3 + topic_len]])
    } else {
        0
    };
    Sent {
        dup: raw.first & 0x08 != 0,
        qos,
        pid,
    }
}

/// `(packet id, topic filters)` of a SUBSCRIBE / UNSUBSCRIBE body.
fn parse_filters(body: &[u8], with_options: bool) -> (u16, Vec<String>) {
    let pid = u16::from_be_bytes([body[0], body[1]]);
    let (props_len, used) = read_varint(&body[2..]).expect("properties");
    let mut i = 2 + used + props_len;
    let mut filters = Vec::new();
    while i < body.len() {
        let len = usize::from(u16::from_be_bytes([body[i], body[i + 1]]));
        filters.push(String::from_utf8_lossy(&body[i + 2..i + 2 + len]).into_owned());
        i += 2 + len + usize::from(with_options);
    }
    (pid, filters)
}

fn read_varint(buf: &[u8]) -> Option<(usize, usize)> {
    let mut value = 0usize;
    let mut mult = 1usize;
    for (i, b) in buf.iter().enumerate().take(4) {
        value += usize::from(b & 0x7F) * mult;
        if b & 0x80 == 0 {
            return Some((value, i + 1));
        }
        mult *= 128;
    }
    None
}

struct Peer {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl Peer {
    fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            buf: Vec::new(),
        }
    }

    /// Next whole packet, `None` on EOF / reset.
    async fn next(&mut self) -> Option<Raw> {
        loop {
            if let Some((len, used)) = read_varint(self.buf.get(1..).unwrap_or(&[])) {
                let total = 1 + used + len;
                if self.buf.len() >= total {
                    let first = self.buf[0];
                    let body = self.buf[1 + used..total].to_vec();
                    self.buf.drain(..total);
                    return Some(Raw { first, body });
                }
            }
            let mut chunk = [0u8; 8192];
            match self.stream.read(&mut chunk).await {
                Ok(0) | Err(_) => return None,
                Ok(n) => self.buf.extend_from_slice(&chunk[..n]),
            }
        }
    }

    /// Next packet that is not a PINGREQ.
    async fn next_skip_ping(&mut self) -> Option<Raw> {
        loop {
            let p = self.next().await?;
            if p.kind() != 12 {
                return Some(p);
            }
        }
    }

    async fn send(&mut self, bytes: &[u8]) {
        let _ = self.stream.write_all(bytes).await;
        let _ = self.stream.flush().await;
    }

    async fn expect_connect(&mut self) {
        let raw = self.next().await.expect("CONNECT");
        assert_eq!(raw.kind(), 1, "first packet must be CONNECT, got {raw:?}");
    }

    /// CONNECT in, CONNACK (with `parts` as properties) out.
    async fn handshake(&mut self, parts: &[Vec<u8>]) {
        self.expect_connect().await;
        self.send(&connack(parts)).await;
    }

    /// Reads until DISCONNECT (its reason, 0 when the byte is absent) or EOF (`None`).
    async fn expect_disconnect(&mut self) -> Option<u8> {
        loop {
            let p = self.next().await?;
            if p.kind() == 14 {
                return Some(p.body.first().copied().unwrap_or(0));
            }
        }
    }

    /// Reads until the client closes the socket.
    async fn wait_eof(&mut self) {
        while self.next().await.is_some() {}
    }

    /// Next SUBSCRIBE (acknowledged with `granted`) or UNSUBSCRIBE; returns its filters.
    async fn expect_request(&mut self, kind: u8) -> (u16, Vec<String>) {
        let r = self.next_skip_ping().await.expect("request");
        assert_eq!(r.kind(), kind, "unexpected {r:?}");
        parse_filters(&r.body, kind == 8)
    }

    /// Acknowledges the next SUBSCRIBE with QoS 1 for every filter.
    async fn accept_subscribe(&mut self) -> Vec<String> {
        let (pid, filters) = self.expect_request(8).await;
        self.send(&suback(pid, &vec![1; filters.len()])).await;
        filters
    }
}

struct Listener(TcpListener);

impl Listener {
    async fn accept(&self) -> Peer {
        let (s, _) = self.0.accept().await.expect("accept");
        Peer::new(s)
    }
}

/// Binds an ephemeral listener and runs `script` on it.
async fn evil<T, F, Fut>(script: F) -> (u16, JoinHandle<T>)
where
    F: FnOnce(Listener) -> Fut + Send + 'static,
    Fut: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().unwrap().port();
    (port, tokio::spawn(async move { script(Listener(listener)).await }))
}

// ---- Client harness ----

/// Reconnect policy with a fixed delay (`None`: never reconnect).
#[derive(Clone, Copy)]
struct Reconnect(Option<Duration>);

impl Reconnect {
    fn never() -> Self {
        Self(None)
    }
    fn every(delay: Duration) -> Self {
        Self(Some(delay))
    }
}

impl ReconnectPolicy for Reconnect {
    fn delay_after(&self, _outcome: &ConnectionOutcome) -> Option<Duration> {
        self.0
    }
}

/// Generous defaults: nothing here is meant to time out unless a test says so.
fn base_options(port: u16) -> ConnectOptions {
    ConnectOptions::new(BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("url"))
        .client_id(ClientId::try_new("hostile").expect("id"))
        .keep_alive(Duration::from_secs(60))
        .ack_timeout(Duration::from_secs(5))
        .connect_timeout(Duration::from_secs(5))
        .graceful_disconnect_timeout(Duration::from_secs(1))
}

struct Harness {
    client: MqttClient,
    phase: watch::Receiver<SessionPhase>,
    run: JoinHandle<Result<(), ConnectionOutcome>>,
    rx: flume::Receiver<InboundMessage>,
}

impl Harness {
    fn start(options: ConnectOptions, policy: Reconnect, sink_capacity: usize) -> Self {
        install_panic_hook();
        let (tx, rx) = flume::bounded::<InboundMessage>(sink_capacity);
        let (client, connection, phase): (_, Connection<_>, _) = connect(
            options,
            StaticCredentials::new("user", "pass"),
            policy,
            tx,
            NoopLifecycle,
        )
        .expect("connect wiring");
        Self {
            client,
            phase,
            run: tokio::spawn(connection.run()),
            rx,
        }
    }

    fn plain(port: u16) -> Self {
        Self::start(base_options(port), Reconnect::never(), 1024)
    }

    async fn connected(&mut self) {
        within(10, self.phase.wait_for(|p| matches!(p, SessionPhase::Connected { .. })))
            .await
            .expect("phase watch");
    }

    /// Waits for the driver task to end and returns its result.
    async fn finish(&mut self, secs: u64) -> Result<(), ConnectionOutcome> {
        match timeout(Duration::from_secs(secs), &mut self.run).await {
            Ok(Ok(result)) => result,
            Ok(Err(e)) => panic!("driver task panicked: {e}"),
            Err(_) => panic!("HANG: driver still running {secs}s after the attack"),
        }
    }

    /// Orderly end of a test: shutdown, then the driver must be done.
    async fn close(mut self) {
        let _ = self.client.shutdown().await;
        let _ = self.finish(10).await;
        assert_no_library_panic();
    }
}

fn is_task_join(outcome: &ConnectionOutcome) -> bool {
    matches!(outcome, ConnectionOutcome::Application(AppError::TaskJoin(_)))
}

// ---- Protocol violations after a good handshake ----

/// Options for the attack tables: Topic Alias Maximum 3 and a Maximum Packet Size of 1000 bytes.
fn attack_options(port: u16) -> ConnectOptions {
    base_options(port)
        .topic_alias_maximum(3)
        .max_packet_size(std::num::NonZeroU32::new(1000).unwrap())
}

const SENTINEL_PID: u16 = 4242;

/// What the scripted broker saw after it sent the attack.
#[derive(Debug, Default)]
struct Seen {
    /// Reason code of the client's DISCONNECT.
    disconnect: Option<u8>,
    /// The client closed the socket by itself after its DISCONNECT.
    closed: bool,
    /// The client acknowledged the sentinel PUBLISH sent behind the attack.
    acked: bool,
}

/// Completes the handshake, sends `attack` and observes the client. With `sentinel` a QoS 1
/// PUBLISH follows the attack: its PUBACK proves, without any timing, that the client processed
/// everything before it and kept the session.
async fn run_attack(attack: Vec<u8>, sentinel: bool) -> (Seen, Result<(), ConnectionOutcome>, Vec<InboundMessage>) {
    let (port, broker) = evil(move |l| async move {
        let mut p = l.accept().await;
        p.handshake(&[]).await;
        p.send(&attack).await;
        if sentinel {
            p.send(&publish("sentinel", 1, SENTINEL_PID, &[], b"s")).await;
        }
        let mut seen = Seen::default();
        while let Some(r) = p.next_skip_ping().await {
            if r.kind() == 4 && r.body.starts_with(&SENTINEL_PID.to_be_bytes()) {
                seen.acked = true;
                break;
            }
            if r.kind() == 14 {
                seen.disconnect = Some(r.body.first().copied().unwrap_or(0));
                seen.closed = timeout(Duration::from_secs(10), p.wait_eof()).await.is_ok();
                break;
            }
        }
        // Held open: after a DISCONNECT the client must hang up by itself.
        (seen, p)
    })
    .await;
    let mut h = Harness::start(attack_options(port), Reconnect::never(), 1024);
    let (seen, peer) = within(30, broker).await.expect("broker task");
    if seen.acked {
        h.client.shutdown().await.expect("shutdown");
    }
    let result = h.finish(10).await;
    drop(peer);
    let delivered = h.rx.try_iter().filter(|m| m.topic != "sentinel").collect();
    assert_no_library_panic();
    (seen, result, delivered)
}

/// Every attack in the table must be answered with the DISCONNECT reason code the spec asks for,
/// the client must close the socket and end its driver with an error.
#[rstest]
#[case::remaining_length_five_bytes(vec![0x30, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F], 0x81)]
#[case::remaining_length_announces_256_mib(vec![0x30, 0xFF, 0xFF, 0xFF, 0x7F], 0x95)]
#[case::reserved_packet_type_zero(vec![0x00, 0x00], 0x81)]
#[case::garbage(vec![0xFF; 64], 0x81)]
#[case::puback_wrong_flags(pkt(0x41, &[0, 1]), 0x81)]
#[case::suback_wrong_flags(pkt(0x91, &[0, 1, 0, 0]), 0x81)]
#[case::puback_zero_length(vec![0x40, 0x00], 0x81)]
#[case::suback_without_reason_codes(pkt(0x90, &[0, 1, 0]), 0x81)]
#[case::publish_qos1_packet_id_zero(publish("a/b", 1, 0, &[], b"x"), 0x81)]
#[case::publish_qos3(pkt(0x36, &[&utf8("a/b")[..], &[0, 1, 0]].concat()), 0x81)]
#[case::publish_dup_on_qos0(pkt(0x38, &[&utf8("a/b")[..], &[0]].concat()), 0x81)]
#[case::publish_wildcard_topic(publish("a/+/b", 0, 0, &[], b"x"), 0x81)]
#[case::publish_topic_invalid_utf8(pkt(0x30, &[0, 2, 0xC3, 0x28, 0]), 0x81)]
#[case::publish_qos2_unsupported(publish("a/b", 2, 7, &[], b"x"), 0x82)]
#[case::publish_empty_topic_without_alias(publish("", 0, 0, &[], b"x"), 0x82)]
#[case::publish_duplicate_property(publish("a", 0, 0, &[prop::u32(0x02, 1), prop::u32(0x02, 2)], b"x"), 0x82)]
#[case::publish_unknown_topic_alias(publish("", 0, 0, &[prop::u16(0x23, 2)], b"x"), 0x94)]
#[case::publish_topic_alias_zero(publish("a", 0, 0, &[prop::u16(0x23, 0)], b"x"), 0x94)]
#[case::publish_topic_alias_above_maximum(publish("a", 0, 0, &[prop::u16(0x23, 4)], b"x"), 0x94)]
#[case::publish_over_maximum_packet_size(publish("big", 0, 0, &[], &[b'x'; 2000]), 0x95)]
#[case::pubrec_unsupported(pkt(0x50, &[0, 1]), 0x82)]
#[case::subscribe_from_server(pkt(0x82, &[0, 1, 0, 0, 1, b'a', 0]), 0x82)]
#[case::connect_from_server(pkt(0x10, &[0, 4, b'M', b'Q', b'T', b'T', 5, 2, 0, 10, 0, 0, 0]), 0x82)]
#[case::second_connack(connack(&[]), 0x82)]
#[tokio::test]
async fn attack_is_answered_with_disconnect_reason(#[case] attack: Vec<u8>, #[case] code: u8) {
    let (seen, result, delivered) = run_attack(attack, false).await;
    assert_eq!(
        seen.disconnect,
        Some(code),
        "expected DISCONNECT {code:#04x}; driver: {result:?}, delivered: {}",
        delivered.len()
    );
    assert!(seen.closed, "client did not close the socket after its DISCONNECT");
    let err = result.expect_err("driver should end with an error");
    assert!(!is_task_join(&err), "driver task failed (panic?): {err:?}");
}

/// Input that is odd but legal (or tolerated on purpose) must not end the session: the sentinel
/// behind it is still acknowledged, and only the expected number of messages reaches the sink.
#[rstest]
#[case::puback_for_unknown_id(puback(5), 0)]
#[case::duplicate_puback([puback(6), puback(6)].concat(), 0)]
#[case::suback_for_unknown_id(suback(7, &[0]), 0)]
#[case::unsuback_for_unknown_id(unsuback(8, &[0]), 0)]
#[case::puback_with_large_id(puback(60_000), 0)]
#[case::subscription_identifier_zero(publish("a", 0, 0, &[prop::varint(0x0B, 0)], b"x"), 1)]
#[case::unsolicited_auth(pkt(0xF0, &[0x00, 0x00]), 0)]
#[case::invalid_utf8_with_payload_format_indicator(publish("a", 0, 0, &[prop::u8(0x01, 1)], &[0xC3, 0x28, 0xFF]), 1)]
#[tokio::test]
async fn tolerated_input_keeps_the_session(#[case] attack: Vec<u8>, #[case] delivered_expected: usize) {
    let (seen, result, delivered) = run_attack(attack, true).await;
    assert!(
        seen.acked && seen.disconnect.is_none(),
        "the session was ended: {seen:?}, driver: {result:?}"
    );
    assert_eq!(delivered.len(), delivered_expected);
}

// ---- CONNACK phase ----

#[derive(Debug, Clone, Copy)]
enum Mute {
    /// Accepts the TCP connection and never sends a byte.
    AfterAccept,
    /// Reads the CONNECT and stays silent.
    AfterConnect,
    /// Reads the CONNECT and closes.
    EofAfterConnect,
    /// Sends all but the last byte of a CONNACK and stalls.
    TruncatedConnack,
}

/// A broker that never completes the handshake must not hold the connection attempt open longer
/// than `connect_timeout`; an early EOF ends it at once.
#[rstest]
#[case::silent_after_accept(Mute::AfterAccept)]
#[case::silent_after_connect(Mute::AfterConnect)]
#[case::eof_after_connect(Mute::EofAfterConnect)]
#[case::truncated_connack(Mute::TruncatedConnack)]
#[tokio::test]
async fn connect_ends_when_the_broker_never_answers(#[case] mute: Mute) {
    let (port, broker) = evil(move |l| async move {
        let mut p = l.accept().await;
        match mute {
            Mute::AfterAccept => {}
            Mute::AfterConnect => p.expect_connect().await,
            Mute::EofAfterConnect => {
                p.expect_connect().await;
                return;
            }
            Mute::TruncatedConnack => {
                p.expect_connect().await;
                let full = connack(&[]);
                p.send(&full[..full.len() - 1]).await;
            }
        }
        p.wait_eof().await;
    })
    .await;
    let mut h = Harness::start(
        base_options(port).connect_timeout(Duration::from_secs(2)),
        Reconnect::never(),
        16,
    );
    let result = h.finish(15).await;
    assert!(matches!(result, Err(ConnectionOutcome::Transport(_))), "{result:?}");
    broker.abort();
    assert_no_library_panic();
}

/// Session Present = 1 in answer to a Clean Start connect is inconsistent: the client says so
/// with DISCONNECT 0x82 and stops, although its reconnect policy would retry.
#[tokio::test]
async fn session_present_after_clean_start_stops_the_client() {
    let (port, broker) = evil(|l| async move {
        let mut p = l.accept().await;
        p.expect_connect().await;
        p.send(&connack_with(true, 0, &props(&[]))).await;
        p.expect_disconnect().await
    })
    .await;
    let mut h = Harness::start(base_options(port), Reconnect::every(Duration::from_millis(100)), 16);
    let result = h.finish(15).await;
    assert!(
        matches!(
            result,
            Err(ConnectionOutcome::Application(AppError::SessionPresentInconsistent(_)))
        ),
        "{result:?}"
    );
    assert_eq!(within(10, broker).await.unwrap(), Some(0x82));
    assert_no_library_panic();
}

/// A CONNACK that breaks the spec in its properties is refused: the client answers with
/// DISCONNECT 0x82 and ends the session.
#[rstest]
#[case::receive_maximum_zero(prop::u16(0x21, 0))]
#[case::maximum_packet_size_zero(prop::u32(0x27, 0))]
#[case::maximum_qos_two(prop::u8(0x24, 2))]
#[tokio::test]
async fn invalid_connack_property_is_refused(#[case] property: Vec<u8>) {
    let (port, broker) = evil(move |l| async move {
        let mut p = l.accept().await;
        p.expect_connect().await;
        p.send(&connack(&[property])).await;
        timeout(Duration::from_secs(10), p.expect_disconnect()).await
    })
    .await;
    let mut h = Harness::plain(port);
    let result = h.finish(15).await;
    let reaction = within(15, broker).await.unwrap();
    assert_eq!(reaction.ok().flatten(), Some(0x82), "driver: {result:?}");
    let err = result.expect_err("driver should end with an error");
    assert!(!is_task_join(&err), "driver task failed (panic?): {err:?}");
    assert_no_library_panic();
}

// ---- Requests the broker's limits rule out ----

#[derive(Debug, Clone)]
enum Request {
    Publish(String),
    Subscribe(String),
    Unsubscribe(String),
}

const LONG_FILTER: &str = "a/very/long/filter/that/exceeds/twelve/bytes";

/// A request the client cannot encode or the broker's Maximum Packet Size rules out is refused
/// locally with an error: it never reaches the wire and the session stays up ([MQTT-3.2.2-15]).
#[rstest]
#[case::publish_over_maximum_packet_size(Request::Publish("some/long/topic/name".into()), true)]
#[case::subscribe_over_maximum_packet_size(Request::Subscribe(LONG_FILTER.into()), true)]
#[case::unsubscribe_over_maximum_packet_size(Request::Unsubscribe(LONG_FILTER.into()), true)]
#[case::subscribe_filter_longer_than_65535(Request::Subscribe("a".repeat(70_000)), false)]
#[case::unsubscribe_filter_longer_than_65535(Request::Unsubscribe("a".repeat(70_000)), false)]
#[tokio::test]
async fn refused_request_keeps_the_session(#[case] request: Request, #[case] tiny_limit: bool) {
    let connack_parts = if tiny_limit { vec![prop::u32(0x27, 12)] } else { vec![] };
    let (port, broker) = evil(move |l| async move {
        let mut p = l.accept().await;
        p.handshake(&connack_parts).await;
        // everything up to the liveness probe below
        let mut kinds = Vec::new();
        while let Some(r) = p.next_skip_ping().await {
            kinds.push(r.kind());
            if r.kind() == 3 && r.body.starts_with(&[0, 1, b't']) {
                break;
            }
        }
        (kinds, p)
    })
    .await;
    let mut h = Harness::plain(port);
    h.connected().await;
    let result = match request {
        Request::Publish(topic) => within(10, h.client.publish(topic, vec![0u8; 100]).qos(QoS::AtLeastOnce))
            .await
            .map(drop),
        Request::Subscribe(filter) => within(10, h.client.subscribe([(filter, QoS::AtLeastOnce)]))
            .await
            .map(drop),
        Request::Unsubscribe(filter) => within(10, h.client.unsubscribe([filter])).await.map(drop),
    };
    assert!(result.is_err(), "the request must be refused: {result:?}");
    within(10, h.client.publish("t", "x"))
        .await
        .expect("the session must survive a refused request");
    let (kinds, _peer) = within(15, broker).await.unwrap();
    assert_eq!(
        kinds,
        [3],
        "the refused request reached the wire or the session was torn down"
    );
    h.close().await;
}

// ---- Acknowledgements with the wrong number of reason codes ----

#[derive(Debug, Clone, Copy)]
enum AckKind {
    Subscribe,
    Unsubscribe,
}

/// A SUBACK / UNSUBACK that does not carry exactly one reason code per filter fails the request
/// and ends the session: with DISCONNECT 0x82 when the count is wrong, with 0x81 when there is
/// no reason code at all (a malformed packet).
#[rstest]
#[case::suback_too_few(AckKind::Subscribe, vec![1], 0x82)]
#[case::suback_none(AckKind::Subscribe, vec![], 0x81)]
#[case::suback_too_many(AckKind::Subscribe, vec![1, 0, 0, 0], 0x82)]
#[case::unsuback_none(AckKind::Unsubscribe, vec![], 0x81)]
#[case::unsuback_too_few(AckKind::Unsubscribe, vec![0], 0x82)]
#[case::unsuback_too_many(AckKind::Unsubscribe, vec![0, 0, 0], 0x82)]
#[tokio::test]
async fn ack_with_wrong_reason_count_ends_the_session(
    #[case] kind: AckKind,
    #[case] reasons: Vec<u8>,
    #[case] code: u8,
) {
    let (port, broker) = evil(move |l| async move {
        let mut p = l.accept().await;
        p.handshake(&[]).await;
        let (pid, filters) = p
            .expect_request(if matches!(kind, AckKind::Subscribe) { 8 } else { 10 })
            .await;
        assert_eq!(filters.len(), 2);
        let ack = match kind {
            AckKind::Subscribe => suback(pid, &reasons),
            AckKind::Unsubscribe => unsuback(pid, &reasons),
        };
        p.send(&ack).await;
        let reason = p.expect_disconnect().await;
        (reason, p)
    })
    .await;
    let mut h = Harness::plain(port);
    h.connected().await;
    let response = match kind {
        AckKind::Subscribe => within(
            10,
            h.client
                .subscribe([("a/+", QoS::AtLeastOnce), ("b/#", QoS::AtMostOnce)]),
        )
        .await
        .map(drop),
        AckKind::Unsubscribe => within(10, h.client.unsubscribe(["a/+", "b/#"])).await.map(drop),
    };
    assert!(
        response.is_err(),
        "the request succeeded with a malformed ack: {response:?}"
    );
    let (reason, _peer) = within(15, broker).await.unwrap();
    assert_eq!(reason, Some(code));
    let result = h.finish(10).await;
    assert!(result.is_err(), "driver should end with an error");
    assert_no_library_panic();
}

// ---- Subscriptions, resubscribe and resumed sessions ----

/// The application unsubscribes from one of two filters and the broker answers with `reason`
/// (also 0x11 "No subscription existed", which is what the application wanted). After a
/// reconnect only the other filter may be subscribed again.
#[rstest]
#[case::success(0x00)]
#[case::no_subscription_existed(0x11)]
#[tokio::test]
async fn unsubscribed_filter_is_not_resubscribed(#[case] reason: u8) {
    let (port, broker) = evil(move |l| async move {
        let mut p = l.accept().await;
        p.handshake(&[]).await;
        p.accept_subscribe().await;
        p.accept_subscribe().await;
        let (pid, filters) = p.expect_request(10).await;
        p.send(&unsuback(pid, &vec![reason; filters.len()])).await;
        drop(p);
        let mut p = l.accept().await;
        p.handshake(&[]).await;
        // everything the client re-subscribes to: wait for the first SUBSCRIBE, then until quiet
        let mut resubscribed = Vec::new();
        let mut patience = Duration::from_secs(10);
        while let Ok(Some(r)) = timeout(patience, p.next_skip_ping()).await {
            if r.kind() == 8 {
                let (pid, filters) = parse_filters(&r.body, true);
                p.send(&suback(pid, &vec![1; filters.len()])).await;
                resubscribed.extend(filters);
                patience = Duration::from_millis(500);
            }
        }
        resubscribed
    })
    .await;
    let mut h = Harness::start(base_options(port), Reconnect::every(Duration::from_millis(50)), 16);
    h.connected().await;
    h.client.subscribe([("keep/in", QoS::AtLeastOnce)]).await.unwrap();
    h.client.subscribe([("keep/out", QoS::AtLeastOnce)]).await.unwrap();
    within(10, h.client.unsubscribe(["keep/out"]))
        .await
        .expect("unsubscribe");
    let resubscribed = within(30, broker).await.unwrap();
    assert_eq!(resubscribed, ["keep/in"], "unsubscribe answered {reason:#x}");
    h.close().await;
}

/// On a reconnect the broker sends hostile packets while the client waits for the SUBACK of its
/// resubscribe. That phase must answer them like the read task does.
#[rstest]
#[case::malformed_publish(publish("a", 1, 0, &[], b"x"), 0x81)]
#[case::garbage(vec![0xFF; 16], 0x81)]
#[case::unknown_topic_alias(publish("", 0, 0, &[prop::u16(0x23, 2)], b"x"), 0x94)]
#[case::topic_alias_above_maximum(publish("a", 0, 0, &[prop::u16(0x23, 4)], b"x"), 0x94)]
#[case::qos2_publish(publish("a", 2, 4, &[], b"x"), 0x82)]
#[case::pubrec(pkt(0x50, &[0, 1]), 0x82)]
#[case::second_connack(connack(&[]), 0x82)]
#[case::publish_over_maximum_packet_size(publish("a", 0, 0, &[], &[0u8; 2000]), 0x95)]
#[tokio::test]
async fn resubscribe_phase_answers_violations_with_disconnect_reason(#[case] attack: Vec<u8>, #[case] code: u8) {
    let (port, broker) = evil(move |l| async move {
        let mut p = l.accept().await;
        p.handshake(&[]).await;
        p.accept_subscribe().await;
        drop(p);
        let mut p = l.accept().await;
        p.handshake(&[]).await;
        p.expect_request(8).await; // the resubscribe
        p.send(&attack).await;
        let reason = timeout(Duration::from_secs(10), p.expect_disconnect()).await;
        (reason, p)
    })
    .await;
    let mut h = Harness::start(attack_options(port), Reconnect::every(Duration::from_millis(100)), 64);
    h.connected().await;
    h.client.subscribe([("a/b", QoS::AtLeastOnce)]).await.unwrap();
    let (reason, _peer) = within(30, broker).await.unwrap();
    assert_eq!(reason.ok().flatten(), Some(code));
    h.close().await;
}

fn resumable(port: u16) -> ConnectOptions {
    base_options(port)
        .clean_start(false)
        .session_expiry(Some(Duration::from_secs(300)))
        .ack_timeout(Duration::from_secs(3))
}

/// Reads client packets until `n` PUBLISHes arrived (or the client closed).
async fn read_publishes(p: &mut Peer, n: usize) -> Vec<Sent> {
    let mut out = Vec::new();
    while out.len() < n {
        match within(15, p.next_skip_ping()).await {
            Some(r) if r.kind() == 3 => out.push(parse_publish(&r)),
            Some(_) => {}
            None => break,
        }
    }
    out
}

/// Connection 1 takes `n` unacknowledged QoS 1 publishes and dies. Connection 2 answers with
/// `session_present` and `connack_parts`; every PUBLISH the client sends there is collected
/// (and acknowledged when `ack`) until the client shuts down. Returns what connection 1 saw,
/// what connection 2 saw and the outcome of each publish.
async fn carried_publishes(
    retain: bool,
    n: usize,
    session_present: bool,
    connack_parts: Vec<Vec<u8>>,
    ack: bool,
) -> (Vec<Sent>, Vec<Sent>, Vec<Result<(), AppError>>) {
    let (port, broker) = evil(move |l| async move {
        let mut p = l.accept().await;
        p.handshake(&[]).await;
        let first = read_publishes(&mut p, n).await;
        drop(p);
        let mut p = l.accept().await;
        p.expect_connect().await;
        p.send(&connack_with(session_present, 0, &props(&connack_parts))).await;
        let mut resent = Vec::new();
        while let Some(r) = p.next_skip_ping().await {
            if r.kind() == 3 {
                let sent = parse_publish(&r);
                if ack && sent.qos > 0 {
                    p.send(&puback(sent.pid)).await;
                }
                resent.push(sent);
            }
        }
        (first, resent)
    })
    .await;
    let mut h = Harness::start(resumable(port), Reconnect::every(Duration::from_millis(100)), 16);
    h.connected().await;
    let mut completions = Vec::new();
    for i in 0..n {
        let publish = Publish::new("r/topic", vec![i as u8])
            .qos(QoS::AtLeastOnce)
            .retain(retain);
        completions.push(h.client.start_publish(publish).await.expect("start_publish"));
    }
    let mut outcomes = Vec::new();
    for c in completions {
        outcomes.push(within(15, c.wait()).await);
    }
    h.close().await;
    let (first, resent) = within(15, broker).await.unwrap();
    (first, resent, outcomes)
}

/// On a resumed session unacknowledged QoS 1 PUBLISHes are resent in order, with DUP = 1 and
/// their original Packet Identifiers ([MQTT-4.4.0-1]).
#[tokio::test]
async fn resumed_session_resends_unacknowledged_publishes_with_dup() {
    let (first, resent, outcomes) = carried_publishes(false, 2, true, vec![], true).await;
    assert_eq!(first.len(), 2);
    assert_eq!(resent.len(), 2, "not resent");
    for (before, after) in first.iter().zip(&resent) {
        assert_eq!(before.pid, after.pid);
        assert!(after.dup && !before.dup, "DUP must be set on the resend only");
    }
    assert!(outcomes.iter().all(Result::is_ok), "{outcomes:?}");
}

/// Session Present = 0: the broker lost the session, so nothing may be resent into it.
#[tokio::test]
async fn lost_session_does_not_resend_carried_publishes() {
    let (_, resent, outcomes) = carried_publishes(false, 1, false, vec![], false).await;
    assert!(resent.is_empty(), "resent into a session the broker does not have");
    assert!(matches!(outcomes[0], Err(AppError::ConnectionLost)), "{outcomes:?}");
}

/// The resumed session's CONNACK lowers Maximum QoS to 0 ([MQTT-3.2.2-11]) or withdraws Retain
/// Available ([MQTT-3.2.2-14]): the carried PUBLISH, now forbidden, must not be sent any more.
#[rstest]
#[case::maximum_qos_lowered(false, prop::u8(0x24, 0))]
#[case::retain_withdrawn(true, prop::u8(0x25, 0))]
#[tokio::test]
async fn resume_does_not_resend_what_the_broker_no_longer_allows(#[case] retain: bool, #[case] property: Vec<u8>) {
    let (_, resent, outcomes) = carried_publishes(retain, 1, true, vec![property], false).await;
    assert!(resent.is_empty(), "resent although the broker forbids it: {resent:?}");
    assert!(outcomes[0].is_err(), "{outcomes:?}");
}

// ---- Liveness and reconnect pacing ----

#[derive(Debug, Clone, Copy)]
enum Stall {
    /// Nothing at all after the CONNACK, pings stay unanswered.
    Silent,
    /// The start of a SUBACK that never completes.
    MidPacket,
}

/// A broker that stops talking is noticed by the keep-alive watchdog. Keep Alive is one second,
/// the bound is ten.
#[rstest]
#[case::never_answers_pingreq(Stall::Silent)]
#[case::stalls_mid_packet(Stall::MidPacket)]
#[tokio::test]
async fn keep_alive_detects_a_stalled_broker(#[case] stall: Stall) {
    let (port, broker) = evil(move |l| async move {
        let mut p = l.accept().await;
        p.handshake(&[]).await;
        if matches!(stall, Stall::MidPacket) {
            p.send(&[0x90, 100, 0, 1, 0, 0, 0]).await;
        }
        p.wait_eof().await;
    })
    .await;
    let mut h = Harness::start(
        base_options(port).keep_alive(Duration::from_secs(1)),
        Reconnect::never(),
        16,
    );
    let result = h.finish(10).await;
    assert!(result.is_err(), "driver should end with an error");
    within(10, broker).await.unwrap();
    assert_no_library_panic();
}

/// A broker that stops reading fills the client's send buffer: publishes fail, none hangs, and
/// the connection is torn down by the keep-alive watchdog.
#[tokio::test]
async fn broker_that_stops_reading_gets_the_connection_dropped() {
    let (port, broker) = evil(|l| async move {
        let mut p = l.accept().await;
        p.handshake(&[]).await;
        std::future::pending::<()>().await; // never read again, hold the socket open
        drop(p);
    })
    .await;
    let opts = base_options(port)
        .keep_alive(Duration::from_secs(2))
        .ack_timeout(Duration::from_millis(500));
    let mut h = Harness::start(opts, Reconnect::never(), 16);
    h.connected().await;
    let payload = bytes::Bytes::from(vec![7u8; 512 * 1024]);
    within(60, async {
        while h.client.is_connected() {
            let r = h.client.publish("big", payload.clone()).qos(QoS::AtLeastOnce).await;
            assert!(r.is_err(), "publish succeeded towards a broker that never reads");
        }
    })
    .await;
    let result = h.finish(10).await;
    assert!(result.is_err(), "driver should end with an error");
    broker.abort();
    assert_no_library_panic();
}

/// A broker that answers pings but never acknowledges anything (a zombie): every call resolves
/// with its timeout error, none hangs, even with the Receive Maximum window used up.
#[tokio::test]
async fn unacknowledged_requests_resolve_with_timeouts() {
    let (port, broker) = evil(|l| async move {
        let mut p = l.accept().await;
        p.handshake(&[prop::u16(0x21, 2)]).await;
        while let Some(r) = p.next().await {
            if r.kind() == 12 {
                p.send(&PINGRESP).await;
            }
        }
    })
    .await;
    let opts = base_options(port).ack_timeout(Duration::from_millis(500));
    let mut h = Harness::start(opts, Reconnect::never(), 16);
    h.connected().await;
    let sub = within(10, h.client.subscribe([("a", QoS::AtLeastOnce)])).await;
    assert!(matches!(sub, Err(AppError::SubscribeAckTimeout(_))), "{sub:?}");
    let unsub = within(10, h.client.unsubscribe(["a"])).await;
    assert!(matches!(unsub, Err(AppError::UnsubscribeAckTimeout(_))), "{unsub:?}");
    for _ in 0..5 {
        let r = within(10, h.client.publish("t", "x").qos(QoS::AtLeastOnce)).await;
        assert!(
            matches!(r, Err(AppError::PublishTimeout(_) | AppError::Backpressure(_))),
            "{r:?}"
        );
    }
    h.close().await;
    broker.abort();
}

#[derive(Debug, Clone, Copy)]
enum Misbehave {
    CloseImmediately,
    CloseAfterConnack,
    DisconnectAfterConnack,
    RefuseConnack(u8),
    GarbageConnack,
}

/// Counts the TCP connections `misbehave` provokes within `window`, with `delay` as the
/// reconnect delay.
async fn count_connects(misbehave: Misbehave, delay: Duration, window: Duration) -> usize {
    let connects = Arc::new(AtomicUsize::new(0));
    let counter = connects.clone();
    let (port, broker) = evil(move |l| async move {
        loop {
            let mut p = l.accept().await;
            counter.fetch_add(1, Ordering::SeqCst);
            match misbehave {
                Misbehave::CloseImmediately => {}
                Misbehave::CloseAfterConnack => p.handshake(&[]).await,
                Misbehave::DisconnectAfterConnack => {
                    p.handshake(&[]).await;
                    p.send(&server_disconnect(0x8B)).await;
                }
                Misbehave::RefuseConnack(code) => {
                    p.expect_connect().await;
                    p.send(&connack_with(false, code, &props(&[]))).await;
                }
                Misbehave::GarbageConnack => {
                    p.expect_connect().await;
                    p.send(&[0xFF; 20]).await;
                }
            }
        }
    })
    .await;
    let h = Harness::start(base_options(port), Reconnect::every(delay), 16);
    tokio::time::sleep(window).await;
    let n = connects.load(Ordering::SeqCst);
    h.close().await;
    broker.abort();
    n
}

/// Whatever a broker does to a connection, reconnects are paced by the policy: 200 ms delay over
/// three seconds is about 15 attempts; a client ignoring its delay makes thousands on loopback.
#[rstest]
#[case::accept_and_close(Misbehave::CloseImmediately)]
#[case::connack_then_close(Misbehave::CloseAfterConnack)]
#[case::connack_then_disconnect(Misbehave::DisconnectAfterConnack)]
#[case::connack_not_authorized(Misbehave::RefuseConnack(0x87))]
#[case::connack_quota_exceeded(Misbehave::RefuseConnack(0x97))]
#[case::garbage_connack(Misbehave::GarbageConnack)]
#[tokio::test]
async fn reconnect_delay_is_respected(#[case] misbehave: Misbehave) {
    let n = count_connects(misbehave, Duration::from_millis(200), Duration::from_secs(3)).await;
    assert!(
        (2..=40).contains(&n),
        "{n} connection attempts in 3 s with a 200 ms delay"
    );
}

/// The documented `StandardReconnectPolicy::mark_live()` on every `Connected` must not turn a
/// broker that completes the handshake and drops the connection into a busy reconnect loop.
#[tokio::test]
async fn mark_live_after_every_connect_does_not_busy_loop() {
    let connects = Arc::new(AtomicUsize::new(0));
    let counter = connects.clone();
    let (port, broker) = evil(move |l| async move {
        loop {
            let mut p = l.accept().await;
            counter.fetch_add(1, Ordering::SeqCst);
            p.handshake(&[]).await;
        }
    })
    .await;
    let policy = StandardReconnectPolicy::new(Duration::from_millis(300), Duration::from_secs(1));
    let (tx, _rx) = flume::bounded::<InboundMessage>(16);
    let (client, connection, mut phase) = connect(
        base_options(port),
        StaticCredentials::new("u", "p"),
        policy.clone(),
        tx,
        NoopLifecycle,
    )
    .unwrap();
    let run = tokio::spawn(connection.run());
    let marker = tokio::spawn(async move {
        while phase.changed().await.is_ok() {
            if matches!(&*phase.borrow(), SessionPhase::Connected { .. }) {
                policy.mark_live();
            }
        }
    });
    tokio::time::sleep(Duration::from_secs(3)).await;
    let n = connects.load(Ordering::SeqCst);
    client.shutdown().await.unwrap();
    let _ = timeout(Duration::from_secs(10), run).await;
    marker.abort();
    broker.abort();
    assert!(
        (2..=40).contains(&n),
        "mark_live defeats the reconnect delay: {n} connects in 3 s with a 300 ms delay"
    );
    assert_no_library_panic();
}

// ---- Soak tests (ignored): random faults between clients and a broker ----

/// Duration of a soak test from `SOAK_SECS` (default 300).
fn soak_secs() -> u64 {
    std::env::var("SOAK_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300)
}

/// VmRSS of this process in kB.
fn rss_kb() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    status
        .lines()
        .find_map(|l| l.strip_prefix("VmRSS:"))
        .and_then(|v| v.split_whitespace().next()?.parse().ok())
        .unwrap_or(0)
}

/// Random resets, refused connects, byte slicing and stalls on `proxy` until `stop` is set
/// (`SOAK_NO_CHAOS=1`: none of that, the same traffic as a control). Returns the number of resets.
async fn chaos(proxy: Arc<FaultProxy>, stop: Arc<AtomicBool>, seed: u64) -> usize {
    // xorshift: no `rand` dev-dependency
    let mut state = seed;
    let mut rng = move |lo: u64, hi: u64| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        lo + state % (hi - lo + 1)
    };
    let (mut resets, faults) = (0, std::env::var_os("SOAK_NO_CHAOS").is_none());
    while !stop.load(Ordering::Relaxed) {
        tokio::time::sleep(Duration::from_millis(rng(300, 1800))).await;
        if !faults {
            continue;
        }
        match rng(0, 4) {
            0 | 1 => {
                proxy.reset_all();
                resets += 1;
            }
            2 => {
                proxy.set_refusing(true);
                proxy.reset_all();
                resets += 1;
                tokio::time::sleep(Duration::from_millis(rng(100, 700))).await;
                proxy.set_refusing(false);
            }
            3 => {
                proxy.set_slice(Some(rng(1, 7) as usize));
                tokio::time::sleep(Duration::from_millis(rng(500, 2000))).await;
                proxy.set_slice(None);
            }
            _ => {
                // long enough for the 2 s keep-alive watchdog to fire
                proxy.set_stalled(true);
                tokio::time::sleep(Duration::from_millis(rng(500, 4500))).await;
                proxy.set_stalled(false);
            }
        }
    }
    proxy.set_stalled(false);
    proxy.set_refusing(false);
    proxy.set_slice(None);
    resets
}

type Driver = JoinHandle<Result<(), ConnectionOutcome>>;

/// Two subscribers and four publishers behind a fault proxy. Publishers send QoS 0 and QoS 1
/// (single and in pipelined bursts of 20) on `soak/q{0,1}/{n}` with a big-endian sequence number
/// as payload; subscribers count deliveries and repeated `(topic, sequence)` pairs.
struct Fleet {
    clients: Vec<(MqttClient, Driver)>,
    subscribers: Vec<JoinHandle<()>>,
    publishers: Vec<JoinHandle<Vec<u32>>>,
    stop: Arc<AtomicBool>,
    received: Arc<AtomicUsize>,
    duplicates: Arc<AtomicUsize>,
}

impl Fleet {
    fn client(proxy: &FaultProxy, name: &str, sink: impl InboundSink) -> (MqttClient, Driver) {
        let options = ConnectOptions::new(BrokerUrl::parse(&proxy.url()).unwrap())
            .client_id(ClientId::try_new(name).unwrap())
            .keep_alive(Duration::from_secs(2))
            .ack_timeout(Duration::from_secs(3))
            .command_channel_capacity(std::num::NonZeroUsize::new(256).unwrap());
        let policy = StandardReconnectPolicy::new(Duration::from_millis(100), Duration::from_secs(1));
        let (client, connection, _phase) =
            connect(options, StaticCredentials::new("u", "p"), policy, sink, NoopLifecycle).expect("connect wiring");
        (client, tokio::spawn(connection.run()))
    }

    async fn start(proxy: &FaultProxy, prefix: &str) -> Self {
        let mut fleet = Self {
            clients: Vec::new(),
            subscribers: Vec::new(),
            publishers: Vec::new(),
            stop: Arc::default(),
            received: Arc::default(),
            duplicates: Arc::default(),
        };
        for i in 0..2 {
            let (tx, rx) = flume::bounded::<InboundMessage>(1024);
            let (client, driver) = Self::client(proxy, &format!("{prefix}-sub-{i}"), tx);
            let started = Instant::now();
            while client.subscribe([("soak/q1/#", QoS::AtLeastOnce)]).await.is_err() {
                assert!(
                    started.elapsed() < Duration::from_secs(30),
                    "subscriber never subscribed"
                );
            }
            let (received, duplicates) = (fleet.received.clone(), fleet.duplicates.clone());
            fleet.subscribers.push(tokio::spawn(async move {
                let mut seen = std::collections::HashSet::new();
                while let Ok(m) = rx.recv_async().await {
                    let seq = u32::from_be_bytes(m.payload[..4].try_into().unwrap());
                    if !seen.insert((m.topic.to_string(), seq)) {
                        duplicates.fetch_add(1, Ordering::Relaxed);
                    }
                    received.fetch_add(1, Ordering::Relaxed);
                }
            }));
            fleet.clients.push((client, driver));
        }
        for i in 0..4 {
            let (client, driver) = Self::client(proxy, &format!("{prefix}-pub-{i}"), OnInboundSink::new(|_m| {}));
            fleet.clients.push((client.clone(), driver));
            let publisher = Self::publish(client, i, fleet.stop.clone());
            fleet.publishers.push(tokio::spawn(publisher));
        }
        fleet
    }

    /// Publishes until `stop`; returns the QoS 1 sequence numbers that were acknowledged.
    async fn publish(client: MqttClient, id: usize, stop: Arc<AtomicBool>) -> Vec<u32> {
        let (mut acked, mut seq) = (Vec::new(), 0u32);
        let q1 = |seq: u32| Publish::new(format!("soak/q1/{id}"), seq.to_be_bytes().to_vec()).qos(QoS::AtLeastOnce);
        while !stop.load(Ordering::Relaxed) {
            if seq % 4 == 0 {
                let _ = client
                    .publish(format!("soak/q0/{id}"), seq.to_be_bytes().to_vec())
                    .await;
                seq += 1;
            } else if seq % 50 == 49 {
                let mut pending = Vec::new();
                for _ in 0..20 {
                    if let Ok(c) = client.start_publish(q1(seq)).await {
                        pending.push((seq, c));
                    }
                    seq += 1;
                }
                for (s, c) in pending {
                    if c.wait().await.is_ok() {
                        acked.push(s);
                    }
                }
            } else {
                if client.send(q1(seq)).await.is_ok() {
                    acked.push(seq);
                }
                seq += 1;
            }
            tokio::time::sleep(Duration::from_millis(3)).await;
        }
        acked
    }

    /// Stops the publishers and shuts every client down (each driver must end cleanly). Returns
    /// the acknowledged QoS 1 sequence numbers per publisher.
    async fn finish(self) -> Vec<Vec<u32>> {
        self.stop.store(true, Ordering::Relaxed);
        let mut acked = Vec::new();
        for t in self.publishers {
            acked.push(
                timeout(Duration::from_secs(60), t)
                    .await
                    .expect("publisher hung")
                    .unwrap(),
            );
        }
        for (client, run) in self.clients {
            client.shutdown().await.unwrap();
            let r = timeout(Duration::from_secs(10), run)
                .await
                .expect("driver hung at shutdown");
            assert!(
                matches!(r, Ok(Ok(())) | Ok(Err(ConnectionOutcome::Shutdown(_)))),
                "driver ended abnormally: {r:?}"
            );
        }
        self.subscribers.iter().for_each(JoinHandle::abort);
        acked
    }
}

/// Soak: 4 publishers and 2 subscribers behind a fault-injecting proxy to a `TestBroker`. A QoS 1
/// publish acknowledged to the application must have reached the broker exactly once, subscribers
/// never see a duplicate and keep receiving after the resets, nothing panics or hangs.
///
/// ```text
/// SOAK_SECS=600 cargo test --release --features testing --test hostile soak_random -- --ignored --nocapture
/// ```
///
/// `SOAK_SECS` sets the duration (default 300 s); `SOAK_NO_CHAOS=1` runs the same traffic
/// without faults as a control.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "long-running soak test; run explicitly"]
async fn soak_random_resets_qos0_qos1() {
    install_panic_hook();
    let secs = soak_secs();
    let broker = TestBroker::start().await.expect("broker");
    let proxy = FaultProxy::start(([127, 0, 0, 1], broker.port()).into())
        .await
        .expect("proxy");
    let proxy = Arc::new(proxy);
    let fleet = Fleet::start(&proxy, "soak").await;
    let faults = tokio::spawn(chaos(proxy.clone(), fleet.stop.clone(), 0x9E37_79B9_7F4A_7C15));

    let started = Instant::now();
    let mut at_80_percent = None;
    while started.elapsed() < Duration::from_secs(secs) {
        tokio::time::sleep(Duration::from_secs(5)).await;
        if at_80_percent.is_none() && started.elapsed() > Duration::from_secs(secs * 4 / 5) {
            at_80_percent = Some(fleet.received.load(Ordering::Relaxed));
        }
        eprintln!(
            "[soak {:>4}s] RSS {:>7} kB  broker saw {:>7} publishes  proxy accepted {:>5}  delivered {}",
            started.elapsed().as_secs(),
            rss_kb(),
            broker.received().len(),
            proxy.accepted(),
            fleet.received.load(Ordering::Relaxed),
        );
    }
    fleet.stop.store(true, Ordering::Relaxed);
    let resets = faults.await.unwrap();
    let (received, duplicates) = (fleet.received.clone(), fleet.duplicates.clone());
    let acked = fleet.finish().await;

    let mut arrivals = std::collections::HashMap::<(usize, u32), usize>::new();
    for r in broker.received() {
        if let Some(id) = r.topic.strip_prefix("soak/q1/") {
            let seq = u32::from_be_bytes(r.payload[..4].try_into().unwrap());
            *arrivals.entry((id.parse().unwrap(), seq)).or_default() += 1;
        }
    }
    let lost: Vec<_> = (acked.iter().enumerate())
        .flat_map(|(id, seqs)| seqs.iter().map(move |s| (id, *s)))
        .filter(|key| !arrivals.contains_key(key))
        .collect();
    let total: usize = acked.iter().map(Vec::len).sum();
    eprintln!(
        "SOAK RESULT after {secs}s: QoS 1 acknowledged {total}, lost {}, duplicates at broker {}, at subscribers {}, \
         resets {resets}, proxy accepted {}, final RSS {} kB",
        lost.len(),
        arrivals.values().filter(|n| **n > 1).count(),
        duplicates.load(Ordering::Relaxed),
        proxy.accepted(),
        rss_kb(),
    );
    assert_no_library_panic_on(true);
    assert!(
        lost.is_empty(),
        "acknowledged to the app but never seen by the broker: {lost:?}"
    );
    assert!(
        arrivals.values().all(|n| *n == 1),
        "duplicate QoS 1 deliveries at the broker"
    );
    assert_eq!(
        duplicates.load(Ordering::Relaxed),
        0,
        "duplicate deliveries to subscribers"
    );
    assert!(total > 100, "soak did not make progress: {total} QoS 1 acknowledged");
    let (before, after) = (at_80_percent.unwrap_or(0), received.load(Ordering::Relaxed));
    assert!(after > before, "subscribers stopped receiving after the resets");
}

/// Minimal broker that acknowledges QoS 1, answers pings and subscriptions, and pushes a QoS 1
/// message to subscribed clients every 2 ms. It stores nothing per message, so process RSS
/// reflects the client.
async fn sink_connection(mut p: Peer) {
    match p.next().await {
        Some(r) if r.kind() == 1 => {}
        _ => return,
    }
    p.send(&connack(&[])).await;
    let mut subscribed = false;
    let mut tick = tokio::time::interval(Duration::from_millis(2));
    let mut next_pid = 0u16;
    loop {
        tokio::select! {
            r = p.next() => match r {
                None => return,
                Some(r) => match r.kind() {
                    3 => {
                        let sent = parse_publish(&r);
                        if sent.qos == 1 {
                            p.send(&puback(sent.pid)).await;
                        }
                    }
                    8 => {
                        let (pid, filters) = parse_filters(&r.body, true);
                        p.send(&suback(pid, &vec![1; filters.len()])).await;
                        subscribed = true;
                    }
                    12 => p.send(&PINGRESP).await,
                    14 => return,
                    _ => {}
                },
            },
            _ = tick.tick(), if subscribed => {
                next_pid = next_pid % 60_000 + 1;
                p.send(&publish("soak/q1/x", 1, next_pid, &[], &[0, 0, 0, 0])).await;
            }
        }
    }
}

/// Soak: the client's memory stays flat under random resets, against a broker that keeps no
/// per-message state (so RSS reflects the client alone). Fails when RSS after warm-up grows by
/// more than 20 MB.
///
/// ```text
/// SOAK_SECS=300 cargo test --release --features testing --test hostile soak_client_memory -- --ignored --nocapture
/// ```
///
/// `SOAK_SECS` sets the duration (default 300 s); `SOAK_NO_CHAOS=1` runs the same traffic
/// without faults as a control.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "long-running soak test; run explicitly"]
async fn soak_client_memory_is_flat_under_resets() {
    install_panic_hook();
    let secs = soak_secs();
    let listener = Listener(TcpListener::bind("127.0.0.1:0").await.unwrap());
    let broker_port = listener.0.local_addr().unwrap().port();
    let broker = tokio::spawn(async move {
        loop {
            tokio::spawn(sink_connection(listener.accept().await));
        }
    });
    let proxy = FaultProxy::start(([127, 0, 0, 1], broker_port).into())
        .await
        .expect("proxy");
    let proxy = Arc::new(proxy);
    let fleet = Fleet::start(&proxy, "mem").await;
    let faults = tokio::spawn(chaos(proxy.clone(), fleet.stop.clone(), 0xD1B5_4A32_D192_ED03));

    let started = Instant::now();
    let mut samples: Vec<(u64, u64)> = Vec::new();
    while started.elapsed() < Duration::from_secs(secs) {
        tokio::time::sleep(Duration::from_secs(10)).await;
        samples.push((started.elapsed().as_secs(), rss_kb()));
        eprintln!(
            "[soak {:>4}s] RSS {:>7} kB  accepted {:>5}  received {:>8}",
            started.elapsed().as_secs(),
            samples.last().unwrap().1,
            proxy.accepted(),
            fleet.received.load(Ordering::Relaxed),
        );
    }
    fleet.stop.store(true, Ordering::Relaxed);
    let resets = faults.await.unwrap();
    fleet.finish().await;
    broker.abort();
    assert_no_library_panic_on(true);

    let (first, last) = (samples[samples.len() / 4], *samples.last().unwrap());
    eprintln!(
        "SOAK RESULT: {secs}s, {resets} resets, {} connections accepted; RSS {} kB @{}s -> {} kB @{}s",
        proxy.accepted(),
        first.1,
        first.0,
        last.1,
        last.0,
    );
    assert!(
        last.1 < first.1 + 20_000,
        "client RSS kept growing: {} kB -> {} kB over {}s",
        first.1,
        last.1,
        last.0 - first.0
    );
}
