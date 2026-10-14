//! In-process TCP fault proxy between a client under test and a broker.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

#[derive(Default)]
struct Faults {
    latency_ms: AtomicU64,
    stalled: AtomicBool,
    /// 0 = no slicing. Towards the broker / towards the client.
    slice_up: AtomicUsize,
    slice_down: AtomicUsize,
    refusing: AtomicBool,
    /// Bumped by `reset_all`; connections from an older epoch abort.
    epoch: AtomicU64,
    reset: Notify,
    accepted: AtomicUsize,
}

/// TCP proxy between a client and a broker that injects network faults on demand.
///
/// Faults are switched at runtime, so a test can inject them at a precise point (mid-burst,
/// while a publish waits for its ack) instead of on a timer:
///
/// - [`Self::set_latency`]: delay every forwarded chunk (both directions).
/// - [`Self::set_stalled`]: stop forwarding without closing, a silent dead link.
/// - [`Self::set_slice`]: forward in writes of at most `n` bytes, so MQTT packets arrive split
///   at arbitrary byte boundaries.
/// - [`Self::reset_all`]: abort every open connection with an RST.
/// - [`Self::set_refusing`]: accept and immediately drop new connections.
///
/// Point the client at [`Self::url`] instead of the broker. Dropping the proxy stops it and
/// resets its connections.
///
/// # Examples
///
/// ```no_run
/// use mqtt_glide::testing::{FaultProxy, TestBroker};
///
/// # async fn demo() -> std::io::Result<()> {
/// let broker = TestBroker::start().await?;
/// let proxy = FaultProxy::start(([127, 0, 0, 1], broker.port()).into()).await?;
/// // ... connect a client to `proxy.url()`, then:
/// proxy.reset_all(); // the client sees a connection reset and reconnects
/// # Ok(())
/// # }
/// ```
pub struct FaultProxy {
    port: u16,
    faults: Arc<Faults>,
    accept_task: tokio::task::JoinHandle<()>,
}

impl FaultProxy {
    /// Listens on an ephemeral port on `127.0.0.1` and forwards every connection to `upstream`.
    pub async fn start(upstream: SocketAddr) -> std::io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let faults = Arc::new(Faults::default());
        let accept_faults = Arc::clone(&faults);
        let accept_task = tokio::spawn(async move {
            loop {
                let Ok((client, _)) = listener.accept().await else {
                    return;
                };
                accept_faults.accepted.fetch_add(1, Ordering::SeqCst);
                if accept_faults.refusing.load(Ordering::SeqCst) {
                    drop(client);
                    continue;
                }
                let faults = Arc::clone(&accept_faults);
                tokio::spawn(async move {
                    let Ok(upstream) = TcpStream::connect(upstream).await else {
                        return;
                    };
                    serve(client, upstream, faults).await;
                });
            }
        });
        Ok(Self {
            port,
            faults,
            accept_task,
        })
    }

    /// URL to connect the client to, e.g. `mqtt://127.0.0.1:41235`.
    pub fn url(&self) -> String {
        format!("mqtt://127.0.0.1:{}", self.port)
    }

    /// Port the client should connect to.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Connections accepted so far (including refused ones).
    pub fn accepted(&self) -> usize {
        self.faults.accepted.load(Ordering::SeqCst)
    }

    /// Delays every forwarded chunk by `latency` (zero: no delay).
    pub fn set_latency(&self, latency: Duration) {
        let ms = u64::try_from(latency.as_millis()).unwrap_or(u64::MAX);
        self.faults.latency_ms.store(ms, Ordering::SeqCst);
    }

    /// Holds back all traffic without closing anything while `stalled`.
    pub fn set_stalled(&self, stalled: bool) {
        self.faults.stalled.store(stalled, Ordering::SeqCst);
    }

    /// Forwards in writes of at most `bytes` in both directions (`None`: as read).
    pub fn set_slice(&self, bytes: Option<usize>) {
        self.set_slice_towards_broker(bytes);
        self.set_slice_towards_client(bytes);
    }

    /// Like [`Self::set_slice`], client → broker only.
    pub fn set_slice_towards_broker(&self, bytes: Option<usize>) {
        self.faults.slice_up.store(bytes.unwrap_or(0), Ordering::SeqCst);
    }

    /// Like [`Self::set_slice`], broker → client only.
    pub fn set_slice_towards_client(&self, bytes: Option<usize>) {
        self.faults.slice_down.store(bytes.unwrap_or(0), Ordering::SeqCst);
    }

    /// Accepts and immediately drops new connections while `refusing`.
    pub fn set_refusing(&self, refusing: bool) {
        self.faults.refusing.store(refusing, Ordering::SeqCst);
    }

    /// Aborts every connection that is open right now (RST towards the client).
    pub fn reset_all(&self) {
        self.faults.epoch.fetch_add(1, Ordering::SeqCst);
        self.faults.reset.notify_waiters();
    }
}

impl Drop for FaultProxy {
    fn drop(&mut self) {
        self.accept_task.abort();
        self.reset_all();
    }
}

async fn serve(client: TcpStream, upstream: TcpStream, faults: Arc<Faults>) {
    let _ = client.set_nodelay(true);
    let _ = upstream.set_nodelay(true);
    let epoch = faults.epoch.load(Ordering::SeqCst);
    // Linger 0 → dropping the socket sends RST instead of FIN, like a crashed peer.
    #[allow(deprecated)] // tokio marks set_linger deprecated (blocking close); the RST is the point
    let _ = client.set_linger(Some(Duration::ZERO));
    let (client_read, client_write) = client.into_split();
    let (upstream_read, upstream_write) = upstream.into_split();
    let reset = faults.reset.notified();
    tokio::pin!(reset);
    reset.as_mut().enable();
    if faults.epoch.load(Ordering::SeqCst) != epoch {
        return;
    }
    tokio::select! {
        () = &mut reset => {}
        () = pump(client_read, upstream_write, Arc::clone(&faults), Direction::TowardsBroker) => {}
        () = pump(upstream_read, client_write, Arc::clone(&faults), Direction::TowardsClient) => {}
    }
    // Dropping all four halves closes both sockets (RST towards the client).
}

#[derive(Clone, Copy)]
enum Direction {
    TowardsBroker,
    TowardsClient,
}

async fn pump(mut from: OwnedReadHalf, mut to: OwnedWriteHalf, faults: Arc<Faults>, direction: Direction) {
    let mut buf = vec![0u8; 16 * 1024];
    loop {
        let read = match from.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        while faults.stalled.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let latency = faults.latency_ms.load(Ordering::SeqCst);
        if latency > 0 {
            tokio::time::sleep(Duration::from_millis(latency)).await;
        }
        let slice = match direction {
            Direction::TowardsBroker => &faults.slice_up,
            Direction::TowardsClient => &faults.slice_down,
        };
        let slice = match slice.load(Ordering::SeqCst) {
            0 => read,
            n => n,
        };
        for chunk in buf[..read].chunks(slice) {
            if to.write_all(chunk).await.is_err() || to.flush().await.is_err() {
                return;
            }
            if slice < read {
                tokio::task::yield_now().await;
            }
        }
    }
}

impl std::fmt::Debug for FaultProxy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FaultProxy")
            .field("port", &self.port)
            .finish_non_exhaustive()
    }
}
