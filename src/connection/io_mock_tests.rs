//! Duplex / MockIo unit tests for read/write I/O edge cases (TL-02 / TL-04).
//!
//! These drive [`read::run`] / [`write::run`] over `tokio::io::duplex` or tiny
//! test-only mocks ([`StallWrite`], [`FailWrite`]) — no `TcpListener`.

use std::io;
use std::num::NonZeroU16;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use bytestring::ByteString;
use futures_util::StreamExt;
use nonzero_ext::nonzero;
use rstest::rstest;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::codec::{FramedRead, FramedWrite};
use tokio_util::sync::CancellationToken;

use crate::codec::{
    DecodedFrame, Packet, PublishDelivery, PublishMessage, PublishProperties, QoS, V5Codec, encode_publish,
};
use crate::connection::packet_id::InflightPots;
use crate::connection::read::{ReadDoneSignal, ReadOutcome, ReadSession};
use crate::connection::subscriptions::SubscriptionRegistry;
use crate::connection::write::{WriteOutcome, WriteSession};
use crate::connection::{ControlMessage, Generation};
use crate::error::{ProtocolError, TransportError};

/// Encoded QoS0 PUBLISH used as a multi-byte frame for split / reset tests.
fn sample_qos0_publish_wire() -> Bytes {
    let message = PublishMessage {
        topic: ByteString::from_static("io/mock"),
        payload: Bytes::from_static(b"partial-payload"),
        properties: PublishProperties::default(),
    };
    let delivery = PublishDelivery {
        dup: false,
        retain: false,
        qos: QoS::AtMostOnce,
        packet_id: None,
    };
    let mut buf = BytesMut::new();
    encode_publish(&message, delivery, u32::MAX, &mut buf).expect("encode qos0 publish");
    buf.freeze()
}

struct ReadTaskHandle {
    cancel: CancellationToken,
    inbound_activity: Arc<AtomicBool>,
    handoff_stalled: Arc<AtomicBool>,
    handoff_rx: flume::Receiver<PublishMessage>,
    join: JoinHandle<ReadOutcome>,
    _stub_tx: mpsc::Sender<()>,
}

fn spawn_read_task<R>(framed: FramedRead<R, V5Codec>) -> ReadTaskHandle
where
    R: AsyncRead + Unpin + Send + 'static,
{
    spawn_read_task_with_handoff_cap(framed, 8)
}

fn spawn_read_task_with_handoff_cap<R>(framed: FramedRead<R, V5Codec>, handoff_cap: usize) -> ReadTaskHandle
where
    R: AsyncRead + Unpin + Send + 'static,
{
    let (control_tx, _control_rx) = mpsc::channel(8);
    let (stub_tx, stub_rx) = mpsc::channel(1);
    let (handoff_tx, handoff_rx) = flume::bounded(handoff_cap);
    let cancel = CancellationToken::new();
    let (read_done_tx, _read_done_rx) = oneshot::channel();
    let inbound_activity = Arc::new(AtomicBool::new(false));
    let handoff_stalled = Arc::new(AtomicBool::new(false));

    let join = tokio::spawn(crate::connection::read::run(
        ReadSession {
            cancel: cancel.clone(),
            shutdown_notify: CancellationToken::new(),
            generation: Generation(1),
            receive_maximum: nonzero!(8u16),
            handoff_stalled: Arc::clone(&handoff_stalled),
            inbound_activity: Arc::clone(&inbound_activity),
        },
        stub_rx,
        framed,
        handoff_tx,
        control_tx,
        ReadDoneSignal::new(read_done_tx),
    ));

    ReadTaskHandle {
        cancel,
        inbound_activity,
        handoff_stalled,
        handoff_rx,
        join,
        _stub_tx: stub_tx,
    }
}

struct WriteTaskHandle {
    control_tx: mpsc::Sender<ControlMessage>,
    cancel: CancellationToken,
    shutdown_notify: CancellationToken,
    join: JoinHandle<WriteOutcome>,
    _stub_tx: mpsc::Sender<()>,
    _command_tx: mpsc::Sender<crate::connection::ClientCommand>,
}

fn spawn_write_task<W>(framed: FramedWrite<W, V5Codec>) -> WriteTaskHandle
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    let (control_tx, control_rx) = mpsc::channel(8);
    let (command_tx, command_rx) = mpsc::channel(8);
    let (stub_tx, stub_rx) = mpsc::channel(1);
    let cancel = CancellationToken::new();
    let shutdown_notify = CancellationToken::new();
    let (read_done_tx, read_done_rx) = oneshot::channel();
    let _ = read_done_tx.send(());

    let join = tokio::spawn(crate::connection::write::run(
        control_rx,
        command_rx,
        stub_rx,
        framed,
        WriteSession {
            generation: Generation(1),
            keep_alive: Duration::from_secs(3600),
            subscription_registry: Arc::new(RwLock::new(SubscriptionRegistry::default())),
            cancel: cancel.clone(),
            shutdown_notify: shutdown_notify.clone(),
            graceful_disconnect_timeout: Duration::from_millis(200),
            max_qos: QoS::AtLeastOnce,
            retain_available: true,
            receive_maximum: nonzero!(8u16),
            handoff_stalled: Arc::new(AtomicBool::new(false)),
            inbound_activity: Arc::new(AtomicBool::new(false)),
            bench_inbound_publish_ack_counter: None,
            session_carry: None,
        },
        InflightPots::new(NonZeroU16::new(4).expect("nz")),
        read_done_rx,
    ));

    WriteTaskHandle {
        control_tx,
        cancel,
        shutdown_notify,
        join,
        _stub_tx: stub_tx,
        _command_tx: command_tx,
    }
}

/// Named handles for [`StallWrite`] so callers avoid a clippy `type_complexity` 4-tuple.
struct StallWriteHarness {
    io: Option<StallWrite>,
    stall: Arc<AtomicBool>,
    waker: Arc<Mutex<Option<Waker>>>,
    written: Arc<Mutex<Vec<u8>>>,
}

impl StallWriteHarness {
    fn new() -> Self {
        let stall = Arc::new(AtomicBool::new(true));
        let waker = Arc::new(Mutex::new(None));
        let written = Arc::new(Mutex::new(Vec::new()));
        Self {
            io: Some(StallWrite {
                stall: Arc::clone(&stall),
                waker: Arc::clone(&waker),
                written: Arc::clone(&written),
            }),
            stall,
            waker,
            written,
        }
    }

    fn take_io(&mut self) -> StallWrite {
        self.io.take().expect("StallWrite io already taken")
    }

    fn release(&self) {
        release_stall(&self.stall, &self.waker);
    }
}

/// Test-only `AsyncWrite` that returns [`Poll::Pending`] while `stall` is set.
struct StallWrite {
    stall: Arc<AtomicBool>,
    waker: Arc<Mutex<Option<Waker>>>,
    written: Arc<Mutex<Vec<u8>>>,
}

fn release_stall(stall: &AtomicBool, waker: &Mutex<Option<Waker>>) {
    stall.store(false, Ordering::Release);
    if let Some(waker) = waker.lock().expect("waker lock").take() {
        waker.wake();
    }
}

impl AsyncWrite for StallWrite {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        if self.stall.load(Ordering::Acquire) {
            *self.waker.lock().expect("waker lock") = Some(cx.waker().clone());
            return Poll::Pending;
        }
        self.written.lock().expect("written lock").extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// Which `AsyncWrite` poll fails with [`io::ErrorKind::ConnectionReset`] (1-based index).
#[derive(Debug, Clone, Copy)]
enum FailOn {
    Write(usize),
    Flush(usize),
}

/// Test-only `AsyncWrite` that injects [`ConnectionReset`] on the nth write or flush poll.
struct FailWrite {
    fail_on: FailOn,
    write_polls: usize,
    flush_polls: usize,
}

impl FailWrite {
    fn new(fail_on: FailOn) -> Self {
        Self {
            fail_on,
            write_polls: 0,
            flush_polls: 0,
        }
    }

    fn connection_reset() -> io::Error {
        io::Error::new(io::ErrorKind::ConnectionReset, "injected connection reset")
    }
}

impl AsyncWrite for FailWrite {
    fn poll_write(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        let this = self.as_mut().get_mut();
        this.write_polls += 1;
        if matches!(this.fail_on, FailOn::Write(target) if target == this.write_polls) {
            return Poll::Ready(Err(Self::connection_reset()));
        }
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.as_mut().get_mut();
        this.flush_polls += 1;
        if matches!(this.fail_on, FailOn::Flush(target) if target == this.flush_polls) {
            return Poll::Ready(Err(Self::connection_reset()));
        }
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

async fn wait_until(flag: &AtomicBool, want: bool, label: &str) {
    timeout(Duration::from_secs(2), async {
        loop {
            if flag.load(Ordering::Acquire) == want {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timeout waiting for {label} == {want}"));
}

/// Partial read: frame split across peer writes — decoder waits, then completes (no `TcpListener`).
#[rstest]
#[case::split_after_fixed_header(1)]
#[case::split_mid_frame(8)]
#[tokio::test]
async fn read_task_partial_frame_over_duplex(#[case] split_at: usize) {
    let wire = sample_qos0_publish_wire();
    assert!(
        split_at > 0 && split_at < wire.len(),
        "split must leave a non-empty remainder"
    );

    let (task_end, mut peer) = tokio::io::duplex(256);
    let framed = FramedRead::new(task_end, V5Codec::with_max_packet_size(0));
    let mut harness = spawn_read_task(framed);

    peer.write_all(&wire[..split_at]).await.expect("partial write");
    peer.flush().await.expect("flush partial");

    tokio::select! {
        biased;
        outcome = &mut harness.join => {
            panic!("read task must stay pending on an incomplete frame, got {outcome:?}");
        }
        _ = tokio::time::sleep(Duration::from_millis(80)) => {}
    }
    assert!(
        !harness.inbound_activity.load(Ordering::Acquire),
        "incomplete frame must not mark inbound activity"
    );
    assert!(
        harness.handoff_rx.is_empty(),
        "incomplete frame must not hand off a publish"
    );

    peer.write_all(&wire[split_at..]).await.expect("remainder write");
    peer.flush().await.expect("flush remainder");

    let message = timeout(Duration::from_secs(2), harness.handoff_rx.recv_async())
        .await
        .expect("handoff timeout")
        .expect("handoff recv");
    assert_eq!(&*message.topic, "io/mock");
    assert_eq!(&*message.payload, b"partial-payload");
    assert!(harness.inbound_activity.load(Ordering::Acquire));

    harness.cancel.cancel();
    let outcome = timeout(Duration::from_secs(2), harness.join)
        .await
        .expect("join timeout")
        .expect("join");
    assert_eq!(outcome, ReadOutcome::Cancelled);
}

/// Reset mid-frame: peer closes after a partial write — transport / peer-closed style outcome.
#[tokio::test]
async fn read_task_reset_mid_frame_over_duplex() {
    let wire = sample_qos0_publish_wire();
    let split_at = 1;

    let (task_end, mut peer) = tokio::io::duplex(256);
    let framed = FramedRead::new(task_end, V5Codec::with_max_packet_size(0));
    let harness = spawn_read_task(framed);

    peer.write_all(&wire[..split_at]).await.expect("partial write");
    peer.flush().await.expect("flush partial");
    peer.shutdown().await.expect("peer shutdown mid-frame");
    drop(peer);

    let outcome = timeout(Duration::from_secs(2), harness.join)
        .await
        .expect("join timeout")
        .expect("join");
    assert!(
        matches!(
            outcome,
            ReadOutcome::Transport(TransportError::Connection(_)) | ReadOutcome::PeerClosed
        ),
        "mid-frame peer reset must surface transport/peer-closed, got {outcome:?}"
    );
    assert!(
        harness.handoff_rx.is_empty(),
        "reset mid-frame must not deliver a publish"
    );
}

/// Write task over duplex (no `TcpListener`): PINGREQ reaches the peer as wire bytes.
#[tokio::test]
async fn write_task_pingreq_over_duplex() {
    let (task_end, peer) = tokio::io::duplex(256);
    let framed = FramedWrite::new(task_end, V5Codec::with_max_packet_size(0));
    let harness = spawn_write_task(framed);

    let peer_read = tokio::spawn(async move {
        let mut framed = FramedRead::new(peer, V5Codec::with_max_packet_size(0));
        match framed.next().await {
            Some(Ok((DecodedFrame::Control(Packet::PingRequest), _))) => true,
            other => panic!("expected PingRequest on duplex peer, got {other:?}"),
        }
    });

    harness.control_tx.send(ControlMessage::PingReq).await.expect("pingreq");

    let saw_ping = timeout(Duration::from_secs(2), peer_read)
        .await
        .expect("peer timeout")
        .expect("peer join");
    assert!(saw_ping);

    drop(harness.control_tx);
    harness.cancel.cancel();
    let _ = timeout(Duration::from_secs(2), harness.join).await;
}

/// Hanging write: `poll_write` stays `Pending` until the stall is released.
#[tokio::test]
async fn write_task_hanging_poll_write_stalls_until_released() {
    let mut stall = StallWriteHarness::new();
    let framed = FramedWrite::new(stall.take_io(), V5Codec::with_max_packet_size(0));
    let mut harness = spawn_write_task(framed);

    harness.control_tx.send(ControlMessage::PingReq).await.expect("pingreq");

    tokio::select! {
        biased;
        outcome = &mut harness.join => {
            panic!("write task must stay pending while poll_write stalls, got {outcome:?}");
        }
        _ = tokio::time::sleep(Duration::from_millis(100)) => {}
    }
    assert!(
        stall.written.lock().expect("written lock").is_empty(),
        "no bytes may leave the framed write while stall is armed"
    );

    stall.release();

    // Allow the write task to complete the stalled flush before we cancel.
    tokio::task::yield_now().await;
    tokio::time::sleep(Duration::from_millis(30)).await;

    let bytes = stall.written.lock().expect("written lock").clone();
    assert_eq!(bytes, b"\xc0\x00", "PINGREQ must flush after stall release");

    drop(harness.control_tx);
    harness.cancel.cancel();
    let outcome = timeout(Duration::from_secs(2), harness.join)
        .await
        .expect("join timeout")
        .expect("join");
    assert_eq!(outcome, WriteOutcome::Cancelled);
}

/// Write I/O error: FailWrite injects ConnectionReset on the chosen poll — task exits Transport (table #1).
#[rstest]
#[case::fail_first_write(FailOn::Write(1))]
#[case::fail_first_flush(FailOn::Flush(1))]
#[tokio::test]
async fn write_task_io_error_on_nth_poll(#[case] fail_on: FailOn) {
    let framed = FramedWrite::new(FailWrite::new(fail_on), V5Codec::with_max_packet_size(0));
    let harness = spawn_write_task(framed);

    harness.control_tx.send(ControlMessage::PingReq).await.expect("pingreq");

    let outcome = timeout(Duration::from_secs(2), harness.join)
        .await
        .expect("join timeout")
        .expect("join");
    assert!(
        matches!(outcome, WriteOutcome::Transport(TransportError::Connection(_))),
        "injected write/flush reset must surface Transport, got {outcome:?}"
    );
}

/// Truncated / illegal inbound bytes: read task surfaces protocol or transport without hanging (table #2).
#[rstest]
#[case::reserved_packet_type(&[0x00, 0x00], true)]
#[case::malformed_publish_qos3(&[0x36, 0x00], true)]
#[case::truncated_then_eof(&[0x30, 0x80], false)]
#[tokio::test]
async fn read_task_malformed_or_truncated_inbound(#[case] bytes: &[u8], #[case] expect_protocol: bool) {
    let (task_end, mut peer) = tokio::io::duplex(256);
    let framed = FramedRead::new(task_end, V5Codec::with_max_packet_size(0));
    let harness = spawn_read_task(framed);

    peer.write_all(bytes).await.expect("inject bytes");
    peer.flush().await.expect("flush inject");
    if !expect_protocol {
        peer.shutdown().await.expect("eof after truncated");
        drop(peer);
    }

    let outcome = timeout(Duration::from_secs(2), harness.join)
        .await
        .expect("join timeout")
        .expect("join");
    if expect_protocol {
        assert!(
            matches!(outcome, ReadOutcome::Protocol(ProtocolError::Decode(_))),
            "illegal MQTT bytes must yield Protocol(Decode), got {outcome:?}"
        );
    } else {
        assert!(
            matches!(
                outcome,
                ReadOutcome::Transport(TransportError::Connection(_)) | ReadOutcome::PeerClosed
            ),
            "truncated frame + EOF must yield transport/peer-closed, got {outcome:?}"
        );
    }
    assert!(harness.handoff_rx.is_empty(), "bad inbound must not deliver a publish");
}

/// Cancel / shutdown_notify while poll_write is Pending: task exits without releasing the stall (table #3 / TL-05).
#[rstest]
#[case::session_cancel(false)]
#[case::shutdown_notify(true)]
#[tokio::test]
async fn write_task_cancel_while_poll_write_pending(#[case] use_shutdown_notify: bool) {
    let mut stall = StallWriteHarness::new();
    let written = Arc::clone(&stall.written);
    let framed = FramedWrite::new(stall.take_io(), V5Codec::with_max_packet_size(0));
    let mut harness = spawn_write_task(framed);

    harness.control_tx.send(ControlMessage::PingReq).await.expect("pingreq");

    tokio::select! {
        biased;
        outcome = &mut harness.join => {
            panic!("write task must stay pending while poll_write stalls, got {outcome:?}");
        }
        _ = tokio::time::sleep(Duration::from_millis(100)) => {}
    }
    assert!(
        written.lock().expect("written lock").is_empty(),
        "stall must still hold bytes before cancel/shutdown"
    );

    if use_shutdown_notify {
        harness.shutdown_notify.cancel();
    } else {
        harness.cancel.cancel();
    }

    let outcome = timeout(Duration::from_secs(2), harness.join)
        .await
        .expect("join timeout")
        .expect("join");
    if use_shutdown_notify {
        assert_eq!(outcome, WriteOutcome::Shutdown);
    } else {
        assert_eq!(outcome, WriteOutcome::Cancelled);
    }
}

/// Stuck hot flush aborted by shutdown, then stall released so graceful DISCONNECT can flush.
#[tokio::test]
async fn write_task_stuck_flush_shutdown_then_disconnect() {
    let mut stall = StallWriteHarness::new();
    let written = Arc::clone(&stall.written);
    let framed = FramedWrite::new(stall.take_io(), V5Codec::with_max_packet_size(0));
    let mut harness = spawn_write_task(framed);

    harness.control_tx.send(ControlMessage::PingReq).await.expect("pingreq");

    tokio::select! {
        biased;
        outcome = &mut harness.join => {
            panic!("write task must stay pending while poll_write stalls, got {outcome:?}");
        }
        _ = tokio::time::sleep(Duration::from_millis(100)) => {}
    }
    assert!(
        written.lock().expect("written lock").is_empty(),
        "stall must still hold bytes before shutdown"
    );

    harness.shutdown_notify.cancel();
    // Release after Hot flush abort so Graceful DISCONNECT flush can proceed.
    stall.release();

    let outcome = timeout(Duration::from_secs(2), harness.join)
        .await
        .expect("join timeout")
        .expect("join");
    assert_eq!(outcome, WriteOutcome::Shutdown);

    let bytes = written.lock().expect("written lock").clone();
    assert!(
        bytes.iter().any(|b| (b & 0xF0) == 0xE0),
        "graceful path must emit DISCONNECT after stall release; got {bytes:?}"
    );
}

/// Handoff full then drain: second publish stalls until recv frees capacity (table #4).
#[tokio::test]
async fn read_task_handoff_full_then_drain() {
    let wire = sample_qos0_publish_wire();
    let (task_end, mut peer) = tokio::io::duplex(512);
    let framed = FramedRead::new(task_end, V5Codec::with_max_packet_size(0));
    let mut harness = spawn_read_task_with_handoff_cap(framed, 1);

    peer.write_all(&wire).await.expect("first publish");
    peer.flush().await.expect("flush first");
    timeout(Duration::from_secs(2), async {
        loop {
            if harness.handoff_rx.len() == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first publish should fill handoff");
    assert!(
        !harness.handoff_stalled.load(Ordering::Acquire),
        "accepted fill must not mark handoff_stalled"
    );

    peer.write_all(&wire).await.expect("second publish");
    peer.flush().await.expect("flush second");
    wait_until(&harness.handoff_stalled, true, "handoff_stalled").await;

    tokio::select! {
        biased;
        outcome = &mut harness.join => {
            panic!("read task must stay pending on full handoff, got {outcome:?}");
        }
        _ = tokio::time::sleep(Duration::from_millis(80)) => {}
    }
    assert_eq!(
        harness.handoff_rx.len(),
        1,
        "only one publish may sit in a full handoff"
    );

    let first = harness.handoff_rx.recv_async().await.expect("drain first slot");
    assert_eq!(&*first.topic, "io/mock");

    let second = timeout(Duration::from_secs(2), harness.handoff_rx.recv_async())
        .await
        .expect("second handoff timeout")
        .expect("second handoff");
    assert_eq!(&*second.topic, "io/mock");
    wait_until(&harness.handoff_stalled, false, "handoff_stalled cleared").await;

    harness.cancel.cancel();
    let outcome = timeout(Duration::from_secs(2), harness.join)
        .await
        .expect("join timeout")
        .expect("join");
    assert_eq!(outcome, ReadOutcome::Cancelled);
}

/// Multi-split partial frame: decoder waits across mid-topic / mid-payload splits (table #6).
#[rstest]
#[case::split_after_remaining_length(2)]
#[case::split_mid_topic(7)]
#[case::split_mid_payload(19)]
#[tokio::test]
async fn read_task_partial_frame_extra_split_points(#[case] split_at: usize) {
    let wire = sample_qos0_publish_wire();
    assert!(
        split_at > 0 && split_at < wire.len(),
        "split must leave a non-empty remainder"
    );

    let (task_end, mut peer) = tokio::io::duplex(256);
    let framed = FramedRead::new(task_end, V5Codec::with_max_packet_size(0));
    let mut harness = spawn_read_task(framed);

    peer.write_all(&wire[..split_at]).await.expect("partial write");
    peer.flush().await.expect("flush partial");

    tokio::select! {
        biased;
        outcome = &mut harness.join => {
            panic!("read task must stay pending on an incomplete frame, got {outcome:?}");
        }
        _ = tokio::time::sleep(Duration::from_millis(80)) => {}
    }
    assert!(
        harness.handoff_rx.is_empty(),
        "incomplete frame must not hand off a publish"
    );

    peer.write_all(&wire[split_at..]).await.expect("remainder write");
    peer.flush().await.expect("flush remainder");

    let message = timeout(Duration::from_secs(2), harness.handoff_rx.recv_async())
        .await
        .expect("handoff timeout")
        .expect("handoff recv");
    assert_eq!(&*message.topic, "io/mock");
    assert_eq!(&*message.payload, b"partial-payload");

    harness.cancel.cancel();
    let outcome = timeout(Duration::from_secs(2), harness.join)
        .await
        .expect("join timeout")
        .expect("join");
    assert_eq!(outcome, ReadOutcome::Cancelled);
}
