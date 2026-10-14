# mqtt-glide benchmarks

The two benchmark documents split like this:

- [`BENCHMARKS.md`](../BENCHMARKS.md) — the published numbers, what each scenario measures, and the fairness rules for reading them.
- **This file** — how to run the harness yourself: env knobs, broker backends, client caps, and the fairness constraints it enforces.

Rebuild the published tables from JSONL with `python3 benches/render_benchmarks.py` (bolds best msg/s / lowest CPU / lowest latency per row; takes `--mock`, `--mosquitto`, `--mosquitto-stock` dirs, defaulting to `target/mqtt_compare/full-*`).

## Host baseline (documented numbers)

Numbers in [`BENCHMARKS.md`](../BENCHMARKS.md) / README highlights were measured on:

| | |
|--|--|
| CPU | Intel Core Ultra 7 258V (Lunar Lake): 4 P-cores (CPUs 0-3), 4 LP E-cores (CPUs 4-7), no SMT |
| Memory | 32 GB (30 GiB usable) |
| OS | Fedora Linux 44, kernel 7.2.10, cpufreq governor `performance` |
| Toolchain | rustc 1.99.0; Podman 5.8.7 (rootless), eclipse-mosquitto `2.1.2-alpine` |
| Pinning | client `taskset -c 0-1`; broker and ingress publisher on 2-3 |

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

Each JSONL line is one cell (`status: ok` or `n/a` with `reason`).

### Scenarios

| `MQTT_COMPARE_SCENARIOS` value | Models |
|--------------------------------|--------|
| `outbound_concurrent` | Service: N caller tasks share one client, each awaiting every publish (msg/s, CPU, p50/p99/p99.9) |
| `outbound_pipelined` | Bulk: one task, window 64 |
| `outbound_spawn` | Relay: one task per message, whole burst spawned at once (msg/s, CPU) |
| `outbound_rate` | Open loop at fixed offered msg/s, 1 ms ticks; latency from each task's release (msg/s, CPU, p50/p99/p99.9) |
| `outbound_serial_latency` | Idle round-trip probe (p50/p99) |
| `ingress_load` | Publisher process bursts at the client: decode, app queue, drain |
| `ingress_serial` | Idle ingress round-trip probe (p50/p99) |

Default: all seven. The old `outbound_serial` throughput scenario was removed. Rows carry an optional `level` (caller tasks for concurrent, offered msg/s for rate) and `p999_us`.

### Clients (`MQTT_COMPARE_CLIENTS`)

| Value | Crate | Version |
|-------|--------|---------|
| `glide` | mqtt-glide (this crate) | current checkout (`main`, pre-0.3) |
| `rumqttc` | `rumqttc-next` | **0.33.3** (pinned; do not bump to 0.34+ without a full re-bench) |
| `mqtt5` | LabOverWire `mqtt5` | **0.37.2** (pinned) |

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
| `mock` | Out-of-process minimal MQTT 5 broker (multi-thread Tokio, Arc fanout, `TCP_NODELAY`, inbound PUBLISH counter on a stats port, `receive_max` 1024) | None |

### Matrix filters and knobs

| Env | Default | Meaning |
|-----|---------|---------|
| `MQTT_COMPARE_RUNS` | 5 | Median-of runs per cell |
| `MQTT_COMPARE_QOS` | `1` | `0`, `1`, or `0,1` |
| `MQTT_COMPARE_PAYLOADS` | `256` | Comma sizes |
| `MQTT_COMPARE_ACK` | `after_enqueue,before` | Ingress QoS1 ack policies |
| `MQTT_COMPARE_SCENARIOS` | all seven | See [Scenarios](#scenarios) |
| `MQTT_COMPARE_LOAD_MSGS` | 50000 | Messages per load burst |
| `MQTT_COMPARE_CONCURRENCY` | `8,64,512` | Caller-task levels for `outbound_concurrent` |
| `MQTT_COMPARE_RATES` | `10000,50000` | Offered msg/s levels for `outbound_rate` |
| `MQTT_COMPARE_RATE_SECS` | 2 | Seconds per `outbound_rate` run |
| `MQTT_COMPARE_BROKER_CPUS` | unset | CPU list (`taskset -c` syntax) for the broker: mock child is wrapped in `taskset`; the Mosquitto container is pinned via `taskset` on its host PID, falling back to `docker update --cpuset-cpus` |
| `MQTT_COMPARE_PUBLISHER_CPUS` | unset | CPU list for the ingress publisher child |
| `MQTT_COMPARE_OUT` | `target/mqtt_compare/<ts>` | Output directory |

The client's own cores are set by running the bench under `taskset` (`meta.json` records `client_cpus`, `broker_cpus`, `publisher_cpus`, `concurrency`, `rates`, `rate_secs`). The window for `outbound_pipelined` is fixed at 64.

```shell
# Published baseline (one run per broker; see BENCHMARKS.md for the host)
MQTT_COMPARE_BROKER=<mock|mosquitto|mosquitto-stock> MQTT_COMPARE_QOS=<0,1 for mock | 1> \
MQTT_COMPARE_BROKER_CPUS=2-3 MQTT_COMPARE_PUBLISHER_CPUS=2-3 MQTT_COMPARE_RUNS=5 \
  taskset -c 0-1 cargo bench --bench mqtt_compare

# Smoke (mock, one run, qos1 payload 256 only)
MQTT_COMPARE_BROKER=mock MQTT_COMPARE_RUNS=1 cargo bench --bench mqtt_compare

# Only the service-style fan-out at one level
MQTT_COMPARE_BROKER=mock MQTT_COMPARE_RUNS=1 MQTT_COMPARE_SCENARIOS=outbound_concurrent \
MQTT_COMPARE_CONCURRENCY=64 cargo bench --bench mqtt_compare

# Only glide + rumqttc, after_enqueue cutover-style cells
MQTT_COMPARE_CLIENTS=glide,rumqttc MQTT_COMPARE_ACK=after_enqueue MQTT_COMPARE_RUNS=10 cargo bench --bench mqtt_compare
```

Pin the client, broker and publisher to disjoint cores when you can; on hybrid CPUs, avoid putting the broker on slow cores (it becomes the bottleneck) and avoid sharing cores between client and broker (it distorts tails).

### Fairness notes

- Shared fixtures: broker, payload pattern, window 64, receive_max 1024 (mock), 50 000-message bursts.
- Client adapters use idiomatic APIs (not a lowest-common-denominator path).
- Unsupported caps are `n/a`: no fake queues / fake manual ack.
- **Ingress is always handoff:** every ingress cell uses decode, bounded app queue (flume), drain. QoS 1 `after_enqueue` vs `before` only moves PUBACK relative to that handoff. See [`BENCHMARKS.md`](../BENCHMARKS.md).
- **CPU ns/msg** is `getrusage` of the whole bench process (client, runtime and harness tasks). Ingress load excludes the publisher child.
- **Outbound wire stopwatch (mock):** after the burst, the parent sends `WAIT N` on the mock stats port; the mock replies `RECEIVED <count>` when it has decoded N PUBLISH frames (or `TIMEOUT <count>` after 60 s). A count mismatch fails the cell.
- **Outbound QoS 0 on Mosquitto:** not run (no wire counter). Use the mock for QoS 0; Mosquitto outbound uses QoS 1 (PUBACK).
- **Outbound rate:** open loop. The generator releases a task per message on a fixed schedule and never waits on the client; latency is measured from release.
- **`publish().await` semantics differ at QoS 0:** rumqttc returns on event-loop channel accept, glide after write-buffer encode, mqtt5 after encode and write on the caller.

## codec_throughput

No-broker microbench for wire attribution on both codec directions:

- **encode** `encode_publish` (including topic validation) — short topic, 71 B topic, and property-block shapes
- **decode** `V5Codec::decode_frame` — PUBLISH (same three shapes) and minimal PUBACK; decode rows include the per-fill buffer refill memcpy (same shape as a socket read into the codec buffer)

Indicative numbers live in [`BENCHMARKS.md`](../BENCHMARKS.md) / README highlights. Single-digit-ns
deltas between builds can be code-layout artifacts; verify suspected regressions with
`-C llvm-args=-align-all-functions=6` on both sides before attributing them to a change.

```shell
cargo bench --bench codec_throughput
```
