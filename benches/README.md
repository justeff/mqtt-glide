# mqtt-glide benchmarks

The two benchmark documents split like this:

- [`BENCHMARKS.md`](../BENCHMARKS.md) — the published numbers, what each scenario measures, and the fairness rules for reading them.
- **This file** — how to run the harness yourself: env knobs, broker backends, client caps, and the fairness constraints it enforces.

Rebuild the generated blocks of [`BENCHMARKS.md`](../BENCHMARKS.md) and the detail pages in [`results/`](results/) with `python3 benches/render_benchmarks.py` (reads `target/bench-results/<run>/`; `--results`, `--out`, `--detail` override the paths). The run names and the full refresh command are listed under [Reproduce](../BENCHMARKS.md#reproduce).

The host, pinning and toolchain of the published numbers are in [Setup](../BENCHMARKS.md#setup).

## Perf matrix: absolute multi-client (`mqtt_compare`)

Custom harness (no Criterion): measures each client × scenario cell **absolutely** (no paired deltas).
Uses the Cargo **`bench`** profile. Results go to JSONL files; rebuild [`BENCHMARKS.md`](../BENCHMARKS.md) from those files.

### Output

```text
target/mqtt_compare/<run_id>/   # or MQTT_COMPARE_OUT=dir
  meta.json
  glide.jsonl
  rumqttc.jsonl
  mqtt5.jsonl
```

Each JSONL line is one cell (`status: ok` or `n/a` with `reason`). The median goes into `msg_s`, `cpu_ns_per_msg`, `p50_us`, …; the per-run values into `msg_s_runs`, `cpu_ns_per_msg_runs`, `p50_us_runs` and `p99_us_runs`, so the spread can be read back. `meta.json` records the knobs of the run (client and mock Receive Maximum, ingress window, Tokio workers, CPUs, versions).

### Scenarios

| `MQTT_COMPARE_SCENARIOS` value | Models |
|--------------------------------|--------|
| `outbound_concurrent` | Service: N caller tasks share one client, each awaiting every publish (msg/s, CPU, p50/p99/p99.9) |
| `outbound_pipelined` | Bulk: one task, window 64 |
| `outbound_rate` | Open loop at fixed offered msg/s, 1 ms ticks; latency from each task's release (msg/s, CPU, p50/p99/p99.9) |
| `ingress_load` | Publisher process bursts at the client: decode, app queue, consumer task |
| `ingress_rate` | Publisher process sends at a fixed offered msg/s (1 ms ticks); latency from the publisher's write (`CLOCK_MONOTONIC` stamp in the payload) to the consumer taking the message off the app queue (msg/s, CPU, p50/p99/p99.9) |

Default: all five. Rows carry an optional `level` (caller tasks for concurrent, offered msg/s for rate) and `p999_us`.

### Clients (`MQTT_COMPARE_CLIENTS`)

| Value | Crate | Version |
|-------|--------|---------|
| `glide` | mqtt-glide (this crate) | current checkout |
| `rumqttc` | `rumqttc-v5-next` | 0.34.0 |
| `mqtt5` | LabOverWire `mqtt5` | 0.50.0 |

Default: `glide,rumqttc,mqtt5`. Versions are written to each run's `meta.json` (`glide_version`, `rumqttc_version`, `mqtt5_version`).

### Ack policy caps (ingress QoS 1)

| Policy | glide | rumqttc | mqtt5 |
|--------|:-----:|:-------:|:-----:|
| `after_enqueue` | yes | yes (`manual_acks`) | **n/a** |
| `before` | **n/a** | yes (auto-ack) | yes (inherent) |

### Broker (`MQTT_COMPARE_BROKER`)

| Value | Backend | Requires |
|-------|---------|----------|
| `mosquitto` (default) | eclipse-mosquitto `2.1.2-alpine` via testcontainers; tuned (`set_tcp_nodelay`, `max_inflight_messages 0`, `max_queued_messages 0`, no persistence/`$SYS`/logs) | Podman or Docker |
| `mosquitto-stock` | Same image with stock `max_inflight_messages 20` / `max_queued_messages 1000` (CONNACK Receive Maximum 20); shows behaviour against an untuned broker | Podman or Docker |
| `mock` | Out-of-process minimal MQTT 5 broker (multi-thread Tokio, Arc fanout, `TCP_NODELAY`, inbound PUBLISH counter on a stats port, announces `MQTT_COMPARE_MOCK_RM`, delivers at the lower of publish and subscription QoS, honours the client's Receive Maximum for QoS 1 deliveries, handles every frame already in its read buffer before writing, so PUBACKs to a publisher and deliveries freed by a subscriber's PUBACKs go out one write per batch, as a real broker does) | None |
| `external` | A broker that is already running on `127.0.0.1:$MQTT_COMPARE_BROKER_PORT`, described in reports by `MQTT_COMPARE_BROKER_LABEL`. The published Mosquitto numbers use this with Mosquitto containers in the same Podman pod (see [Reproduce](../BENCHMARKS.md#reproduce)); the bench neither starts nor pins it | A running broker |

### Matrix filters and knobs

| Env | Default | Meaning |
|-----|---------|---------|
| `MQTT_COMPARE_RUNS` | 5 | Median-of runs per cell |
| `MQTT_COMPARE_QOS` | `1` | `0`, `1`, or `0,1` |
| `MQTT_COMPARE_PAYLOADS` | `256` | Comma sizes |
| `MQTT_COMPARE_ACK` | `after_enqueue,before` | Ingress QoS1 ack policies |
| `MQTT_COMPARE_SCENARIOS` | all five | See [Scenarios](#scenarios) |
| `MQTT_COMPARE_LOAD_MSGS` | 50000 | Messages per load burst |
| `MQTT_COMPARE_CLIENT_RM` | 1024 | Receive Maximum every client announces (caps the broker's QoS 1 deliveries in flight). The libraries' defaults are glide 32, rumqttc and mqtt5 65 535 |
| `MQTT_COMPARE_MOCK_RM` | 1024 | Receive Maximum the mock broker announces in CONNACK (caps the client's outbound QoS 1 in flight) |
| `MQTT_COMPARE_MOCK_DELIVERY_INFLIGHT` | unset | QoS 1 deliveries the mock keeps unacknowledged per subscriber at most, below the subscriber's Receive Maximum; models a broker with its own cap (EMQX `max_inflight`, default 32) |
| `MQTT_COMPARE_INGRESS_WINDOW` | 1024 | QoS 1 PUBLISHes the ingress publisher keeps unacknowledged (further capped by the broker's Receive Maximum) |
| `MQTT_COMPARE_INGRESS_RATES` | `10000,50000,200000` | Offered msg/s levels for `ingress_rate` |
| `MQTT_COMPARE_CONCURRENCY` | `8,64,512` | Caller-task levels for `outbound_concurrent` |
| `MQTT_COMPARE_RATES` | `10000,50000` | Offered msg/s levels for `outbound_rate` |
| `MQTT_COMPARE_RATE_SECS` | 2 | Seconds per `outbound_rate` run |
| `MQTT_COMPARE_BROKER_CPUS` | unset | CPU list (`taskset -c` syntax) for the broker: mock child is wrapped in `taskset`; the Mosquitto container is pinned via `taskset` on its host PID, falling back to `docker update --cpuset-cpus` |
| `MQTT_COMPARE_PUBLISHER_CPUS` | unset | CPU list for the ingress publisher child |
| `MQTT_COMPARE_OUT` | `target/mqtt_compare/<ts>` | Output directory |
| `MQTT_COMPARE_BROKER_PORT`, `MQTT_COMPARE_BROKER_LABEL` | unset | Port and report label of the `external` broker |
| `TOKIO_WORKER_THREADS` | CPU count | Worker threads of the bench's multi-thread runtime (recorded in `meta.json`) |

The client's own cores are set by running the bench under `taskset` (`meta.json` records `client_cpus`, `broker_cpus`, `publisher_cpus`, `concurrency`, `rates`, `rate_secs`). The window for `outbound_pipelined` is fixed at 64.

```shell
# Smoke (mock, one run, qos1 payload 256 only)
MQTT_COMPARE_BROKER=mock MQTT_COMPARE_RUNS=1 cargo bench --features __bench --bench mqtt_compare

# Only the service-style fan-out at one level
MQTT_COMPARE_BROKER=mock MQTT_COMPARE_RUNS=1 MQTT_COMPARE_SCENARIOS=outbound_concurrent \
MQTT_COMPARE_CONCURRENCY=64 cargo bench --features __bench --bench mqtt_compare

# Only glide + rumqttc, ingress with PUBACK after the app queue
MQTT_COMPARE_CLIENTS=glide,rumqttc MQTT_COMPARE_ACK=after_enqueue MQTT_COMPARE_RUNS=10 cargo bench --features __bench --bench mqtt_compare
```

Pin the client, broker and publisher to disjoint cores when you can; on hybrid CPUs, avoid putting the broker on slow cores (it becomes the bottleneck) and avoid sharing cores between client and broker (it distorts tails).

### Fairness notes

- Shared fixtures: broker, payload pattern, window 64, the same client Receive Maximum for every client (1024 unless `MQTT_COMPARE_CLIENT_RM`), 50 000-message bursts.
- **Ingress publisher:** keeps up to `MQTT_COMPARE_INGRESS_WINDOW` QoS 1 PUBLISHes in flight (capped by the broker's Receive Maximum) and writes up to 64 per syscall, so at QoS 1 the client's Receive Maximum, not the load generator, limits ingress.
- **Harness on the runtime:** the whole matrix runs in a spawned task, so callers, the rate generator and the ingress consumer are tasks on the client's runtime, as an application's are. On the `block_on` thread, every message a waiting consumer got cost the producing client a futex wake-up, and how often depended on the client's timing.
- **Clients take turns:** every run of a cell measures all clients once, starting with a different client each run (`ingress_fanin` does the same for the sinks of a row). This cancels host drift over minutes, which is large on a VM under macOS.
- Client adapters use idiomatic APIs (not a lowest-common-denominator path).
- Unsupported caps are `n/a`: no fake queues / fake manual ack.
- **Ingress is always handoff:** every ingress cell uses decode, bounded app queue (`tokio::sync::mpsc`), consumer task. QoS 1 `after_enqueue` vs `before` only moves PUBACK relative to that handoff. See [`BENCHMARKS.md`](../BENCHMARKS.md).
- **CPU ns/msg** is `getrusage` of the whole bench process (client, runtime and harness tasks). Ingress load excludes the publisher child.
- **Outbound wire stopwatch (mock):** after the burst, the parent sends `WAIT N` on the mock stats port; the mock replies `RECEIVED <count>` when it has decoded N PUBLISH frames (or `TIMEOUT <count>` after 60 s). A count mismatch fails the cell.
- **Outbound QoS 0 on Mosquitto:** not run (no wire counter). Use the mock for QoS 0; Mosquitto outbound uses QoS 1 (PUBACK).
- **Outbound rate:** open loop. The generator releases a task per message on a fixed schedule and never waits on the client; latency is measured from release.
- **`publish().await` semantics differ at QoS 0:** rumqttc returns on event-loop channel accept, glide after write-buffer encode, mqtt5 after encode and write on the caller.

## Multi-broker fan-in (`ingress_fanin`)

mqtt-glide only. N clients, each connected to its own mock broker child with its own publisher child, hand inbound messages to one shared handoff that M consumer tasks drain. Each consumer touches the message and spins `FANIN_WORK_NS` per message to stand in for application work. This is the shape of an application that subscribes on several brokers and processes everything in one worker pool.

| `FANIN_SINKS` value | Consumers | Handoff |
|---|---|---|
| `mpsc` | 1 | `tokio::sync::mpsc` (single consumer) |
| `flume` | M | `flume::Sender` through the library's `flume` feature (MPMC, mutex inside) |
| `async_channel` | M | `async_channel::Sender` through the library's `async-channel` feature (MPMC, lock-free queue) |
| `shard` | M | one `tokio::sync::mpsc` per consumer; the sink starts round-robin and tries each shard once before reporting full. No per-topic order |
| `inline` | N | `on_inbound`: the work runs on each connection's read task |

The total handoff capacity is the same for every sink (10 000; `shard` splits it across consumers). Reported per cell: msg/s (clock starts at the first consumed message), process CPU per message, and the clients' `handoff_stalls` (`ClientStats`) per 1000 messages. Results: `<out>/fanin.jsonl` and `meta.json` (default `target/ingress_fanin/<ts>`).

```shell
cargo bench --features __bench,stats,flume,async-channel --bench ingress_fanin
FANIN_BROKERS=2 FANIN_CONSUMERS=4 FANIN_SINKS=async_channel,shard MQTT_COMPARE_RUNS=1 \
  cargo bench --features __bench,stats,flume,async-channel --bench ingress_fanin
```

| Env | Default | Meaning |
|-----|---------|---------|
| `FANIN_BROKERS` | `1,2,4` | Broker connections N |
| `FANIN_CONSUMERS` | `1,2,4` | Consumer tasks M (`flume`, `async_channel`, `shard`) |
| `FANIN_SINKS` | all | See above |
| `FANIN_WORK_NS` | `0,1000` | Busy work per message in the consumer |
| `FANIN_MSGS` | 2000000 | Messages per run, split across the brokers |
| `FANIN_PAYLOAD` | 64 | Payload bytes |

`MQTT_COMPARE_QOS`, `MQTT_COMPARE_RUNS`, `MQTT_COMPARE_CLIENT_RM`, `MQTT_COMPARE_INGRESS_WINDOW`, `MQTT_COMPARE_BROKER_CPUS`, `MQTT_COMPARE_PUBLISHER_CPUS` and `MQTT_COMPARE_OUT` apply as for `mqtt_compare`. On macOS the many unpinned processes (N brokers, N publishers, the client) compete for cores and QoS 1 with several brokers can fall into small-batch ping-pong; use pinned Linux numbers.

## codec_throughput

No-broker microbench for wire attribution on both codec directions:

- **encode** `encode_publish` (including topic validation) — short topic, 71 B topic, and property-block shapes
- **decode** `V5Codec::decode_frame` — PUBLISH (same three shapes) and minimal PUBACK; decode rows include the per-fill buffer refill memcpy (same shape as a socket read into the codec buffer)

The results are not part of [`BENCHMARKS.md`](../BENCHMARKS.md); run it on your own host. Single-digit-ns
deltas between builds can be code-layout artifacts; verify suspected regressions with
`-C llvm-args=-align-all-functions=6` on both sides before attributing them to a change.

```shell
cargo bench --bench codec_throughput
```

## alloc_profile

Heap allocations per message on the client's hot paths, against the mock broker (`MQTT_COMPARE_BROKER=mock`). A counting global allocator counts `alloc` / `realloc` calls and bytes of the bench process (the client, its Tokio runtime and the harness tasks; the broker and the ingress publisher are child processes and not counted) between the start and the end of a burst.

The harness itself allocates about one payload copy per published message (`Bytes::copy_from_slice`) and, in the `out_*` rows, one boxed future per call (`async_trait`); subtract those when reading the outbound rows. The `pipe_*` rows are the closest to the client alone. The inbound rows include one `tokio::sync::mpsc` receiver wakeup per message.

```shell
MQTT_COMPARE_BROKER=mock cargo bench --features __bench --bench alloc_profile
# which call sites allocate (every 7th allocation records a backtrace; counts are inflated by that)
MQTT_COMPARE_BROKER=mock ALLOC_SITES=7 ALLOC_SCENARIOS=pipe_q1 cargo bench --features __bench --bench alloc_profile
```

| Env | Default | Meaning |
|-----|---------|---------|
| `ALLOC_MSGS` | 20000 | Messages per scenario |
| `ALLOC_SCENARIOS` | all | `out_q0`, `out_q1` (one task, one publish at a time), `pipe_q0`, `pipe_q1` (`publish_window`), `ingress_q0`, `ingress_q1` |
| `ALLOC_PAYLOAD` | 256 | Payload bytes |
| `ALLOC_SITES` | off | Record a backtrace for every n-th allocation and print the most frequent call sites |

### Allocator

On Linux with glibc malloc, the allocator can matter more than anything in the client. A publish allocates its payload on the calling thread and the write task frees it after encoding; glibc then takes the owning arena's lock on every free, which contends with the next `malloc` on the calling thread. Measured on a 2-core pinned client (Fedora aarch64 VM, 256 B payload, mock broker, payload allocated per message): QoS 0 `outbound_pipelined` 1.3M msg/s and 1050 CPU ns/msg with glibc, 2.2M msg/s and 760 CPU ns/msg with mimalloc; `outbound_concurrent` at 64 callers 1.0M against 1.75M msg/s. With one shared `Bytes` payload instead (no per-message allocation) glibc reaches 2.7M msg/s and 560 CPU ns/msg, so the client's own allocations are not the problem; the cross-thread free of the caller's payload is. QoS 1, which is bound by syscalls and wakeups, does not gain (mimalloc is up to 4% worse there). musl (Alpine images) costs 10 to 50% more CPU per message in the concurrent and ingress cells. An application that publishes from many tasks on Linux should set a thread-caching allocator such as mimalloc or jemalloc as its `#[global_allocator]`.
