//! Heap allocations per message on the client's hot paths (no peers, mock broker only).
//!
//! Counts `alloc`/`realloc` calls and bytes of this process (client, Tokio runtime, harness
//! tasks; the broker and the ingress publisher are child processes and not counted) between the
//! start and the end of a burst, and prints them per message. With `ALLOC_SITES=<n>` every n-th
//! allocation also records a backtrace, and the most frequent call sites are printed.
//!
//! ```shell
//! cargo bench --features __bench --bench alloc_profile
//! ALLOC_SITES=7 ALLOC_SCENARIOS=ingress_q1 cargo bench --features __bench --bench alloc_profile
//! ```
//!
//! Knobs: `ALLOC_MSGS` (default 20000), `ALLOC_SCENARIOS` (comma list of `out_q0`, `out_q1`,
//! `pipe_q0`, `pipe_q1`, `ingress_q0`, `ingress_q1`; default all), `ALLOC_PAYLOAD` (default 256).

#![allow(dead_code, unsafe_code, unused_imports)]

mod compare;
mod support;

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use compare::clients::client_for;
use compare::fixtures::{LOAD_WINDOW, next_id, payload_bytes};
use compare::harness::{AckPolicy, BenchQos, ClientId, SessionCfg};
use compare::pubchild::{spawn_publisher_child, wait_child};
use support::publisher::PersistentPublisher;

static ALLOCS: AtomicU64 = AtomicU64::new(0);
static REALLOCS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);
static SAMPLE_EVERY: AtomicUsize = AtomicUsize::new(0);
static SAMPLING: AtomicBool = AtomicBool::new(false);
static SAMPLES: Mutex<Vec<std::backtrace::Backtrace>> = Mutex::new(Vec::new());

thread_local! {
    static IN_HOOK: Cell<bool> = const { Cell::new(false) };
}

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let n = ALLOCS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        sample(n);
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let n = ALLOCS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        sample(n);
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let n = REALLOCS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        sample(n.wrapping_add(1 << 40));
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

fn sample(n: u64) {
    if !SAMPLING.load(Ordering::Relaxed) {
        return;
    }
    let every = SAMPLE_EVERY.load(Ordering::Relaxed) as u64;
    if every == 0 || !n.is_multiple_of(every) {
        return;
    }
    IN_HOOK.with(|flag| {
        if flag.replace(true) {
            return;
        }
        let bt = std::backtrace::Backtrace::force_capture();
        if let Ok(mut samples) = SAMPLES.lock() {
            samples.push(bt);
        }
        flag.set(false);
    });
}

#[global_allocator]
static GLOBAL: Counting = Counting;

#[derive(Clone, Copy)]
struct Snapshot {
    allocs: u64,
    reallocs: u64,
    bytes: u64,
}

fn snapshot() -> Snapshot {
    Snapshot {
        allocs: ALLOCS.load(Ordering::Relaxed),
        reallocs: REALLOCS.load(Ordering::Relaxed),
        bytes: BYTES.load(Ordering::Relaxed),
    }
}

fn report(label: &str, messages: usize, before: Snapshot, after: Snapshot) {
    let m = messages as f64;
    println!(
        "{label:<12} {:>6.2} allocs/msg  {:>5.2} reallocs/msg  {:>8.1} B/msg",
        (after.allocs - before.allocs) as f64 / m,
        (after.reallocs - before.reallocs) as f64 / m,
        (after.bytes - before.bytes) as f64 / m,
    );
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn qos_of(tag: &str) -> BenchQos {
    if tag.ends_with("q0") {
        BenchQos::AtMostOnce
    } else {
        BenchQos::AtLeastOnce
    }
}

async fn outbound(port: u16, tag: &str, messages: usize, pipelined: bool, stats: support::mock_broker::MockStatsPort) {
    let client = client_for(ClientId::Glide);
    let cfg = SessionCfg {
        port,
        client_id: next_id("alloc-out"),
        qos: qos_of(tag),
        ack: None,
    };
    let session = client.setup_outbound(&cfg).await;
    let payload = payload_bytes(env_usize("ALLOC_PAYLOAD", 256));
    stats.reset().await;
    for _ in 0..64 {
        session.publish_serial(&payload).await;
    }
    stats.wait_exact(64).await.expect("warmup wire");
    stats.reset().await;

    let before = snapshot();
    SAMPLING.store(true, Ordering::Relaxed);
    if pipelined {
        session.publish_pipelined(&payload, LOAD_WINDOW, messages).await;
    } else {
        for _ in 0..messages {
            session.publish_serial(&payload).await;
        }
    }
    stats.wait_exact(messages as u64).await.expect("wire");
    SAMPLING.store(false, Ordering::Relaxed);
    report(tag_label(tag, pipelined), messages, before, snapshot());
    session.close().await;
}

fn tag_label(tag: &str, pipelined: bool) -> &'static str {
    match (tag, pipelined) {
        ("out_q0", _) => "out_q0",
        ("out_q1", _) => "out_q1",
        ("pipe_q0", _) => "pipe_q0",
        _ => "pipe_q1",
    }
}

async fn ingress(port: u16, tag: &str, messages: usize) {
    let client = client_for(ClientId::Glide);
    let qos = qos_of(tag);
    let ack = (qos == BenchQos::AtLeastOnce).then_some(AckPolicy::AfterEnqueue);
    let cfg = SessionCfg {
        port,
        client_id: next_id("alloc-in"),
        qos,
        ack,
    };
    let mut session = client.setup_ingress(&cfg).await;
    let payload = payload_bytes(env_usize("ALLOC_PAYLOAD", 256));
    {
        let mut warm = PersistentPublisher::connect(port, &next_id("pub-warm")).await;
        session.drain();
        session.reset_barrier();
        for _ in 0..64 {
            warm.send(qos, session.topic(), &payload).await;
            session.recv_touch().await;
        }
        session.wait_barrier(64).await;
        warm.drain_completions(qos).await;
        warm.disconnect().await;
        session.drain();
    }
    session.reset_barrier();
    let child = spawn_publisher_child(
        port,
        messages,
        &next_id("child-pub"),
        session.topic(),
        payload.len(),
        qos,
        None,
    );
    // Do not start counting before the publisher process is up and sending.
    session.recv_touch().await;
    let before = snapshot();
    SAMPLING.store(true, Ordering::Relaxed);
    for _ in 1..messages {
        session.recv_touch().await;
    }
    session.wait_barrier(messages).await;
    SAMPLING.store(false, Ordering::Relaxed);
    report(
        if qos == BenchQos::AtMostOnce {
            "ingress_q0"
        } else {
            "ingress_q1"
        },
        messages - 1,
        before,
        snapshot(),
    );
    wait_child(child);
    session.close().await;
}

fn print_sites() {
    let samples = std::mem::take(&mut *SAMPLES.lock().unwrap());
    if samples.is_empty() {
        return;
    }
    let every = SAMPLE_EVERY.load(Ordering::Relaxed);
    let mut sites: HashMap<String, usize> = HashMap::new();
    for bt in &samples {
        let text = bt.to_string();
        // Keep the first few frames outside the allocator plumbing, as `symbol` lines only.
        let frames: Vec<&str> = text
            .lines()
            .filter(|l| l.trim_start().chars().next().is_some_and(|c| c.is_ascii_digit()) && l.contains(':'))
            .map(|l| l.split_once(':').map_or(l, |(_, s)| s).trim())
            .filter(|s| {
                !s.starts_with("alloc_profile::")
                    && !s.contains("alloc::alloc")
                    && !s.contains("__rust_")
                    && !s.contains("std::backtrace")
                    && !s.contains("core::ops::function")
                    && !s.contains("alloc::raw_vec")
                    && !s.contains("alloc::vec::")
                    && !s.contains("alloc::boxed")
                    && !s.contains("alloc::sync")
                    && !s.contains("alloc::slice")
                    && !s.contains("alloc::fmt")
                    && !s.contains("alloc::string")
                    && !s.contains("<alloc::")
                    && !s.contains("hashbrown")
            })
            .take(4)
            .collect();
        *sites.entry(frames.join("  <-  ")).or_default() += 1;
    }
    let mut sites: Vec<_> = sites.into_iter().collect();
    sites.sort_by_key(|site| std::cmp::Reverse(site.1));
    println!("\nallocation sites (1 in {every} sampled; count = samples):");
    for (site, count) in sites.iter().take(25) {
        println!("{count:>6}  {site}");
    }
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
    // cargo passes `--bench` to harness = false targets.
    let messages = env_usize("ALLOC_MSGS", 20_000);
    let wanted: Vec<String> = std::env::var("ALLOC_SCENARIOS")
        .unwrap_or_else(|_| "out_q0,out_q1,pipe_q0,pipe_q1,ingress_q0,ingress_q1".into())
        .split(',')
        .map(str::to_owned)
        .collect();
    SAMPLE_EVERY.store(env_usize("ALLOC_SITES", 0), Ordering::Relaxed);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    let broker = rt.block_on(support::broker::start_bench_broker());
    let port = broker.port();
    let stats = broker.mock_stats().expect("run with MQTT_COMPARE_BROKER=mock");
    println!(
        "payload {} B, {messages} msgs, counted: client + runtime + harness only",
        env_usize("ALLOC_PAYLOAD", 256)
    );
    for tag in &wanted {
        rt.block_on(async {
            match tag.as_str() {
                "out_q0" | "out_q1" => outbound(port, tag, messages, false, stats).await,
                "pipe_q0" | "pipe_q1" => outbound(port, tag, messages, true, stats).await,
                "ingress_q0" | "ingress_q1" => ingress(port, tag, messages).await,
                other => panic!("unknown scenario {other}"),
            }
        });
    }
    print_sites();
}
