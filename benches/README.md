# mqtt-glide benchmarks

Published numbers live in [`BENCHMARKS.md`](../BENCHMARKS.md). This file is **how to run** the harness (fairness, env knobs, caps).
Rebuild published tables from JSONL with `python3 benches/render_benchmarks.py` (bolds best msg/s / lowest CPU / lowest latency per row).

## Host baseline (documented numbers)

Numbers in [`BENCHMARKS.md`](../BENCHMARKS.md) / README highlights were measured on:

| | |
|--|--|
| Machine | Apple MacBook Pro (`MacBookPro18,2`) |
| Chip | Apple M1 Max, 10 cores (8 performance + 2 efficiency) |
| Memory | 32 GB |
| OS | macOS 26.5.2 |

Re-run on other hosts will differ; update the host table when refreshing the published baseline.

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

### Clients (`MQTT_COMPARE_CLIENTS`)

| Value | Crate | Baseline version |
|-------|--------|------------------|
| `glide` | mqtt-glide (this crate) | **0.1.0** (`CARGO_PKG_VERSION`; not every release is re-benched) |
| `rumqttc` | `rumqttc-next` | **0.33.3** (pinned; do not bump to 0.34+ without a full re-bench) |
| `mqtt5` | LabOverWire `mqtt5` | **0.37.2** (pinned) |

Default: `glide,rumqttc,mqtt5`. Versions are written to each run’s `meta.json` (`glide_version`, `rumqttc_version`, `mqtt5_version`).

### Ack policy caps (ingress QoS 1)

| Policy | glide | rumqttc | mqtt5 |
|--------|:-----:|:-------:|:-----:|
| `after_enqueue` | yes | yes (`manual_acks`) | **n/a** |
| `before` | **n/a** | yes (auto-ack) | yes (inherent) |

### Broker (`MQTT_COMPARE_BROKER`)

| Value | Backend | Requires |
|-------|---------|----------|
| `mosquitto` (default) | eclipse-mosquitto `2.1.2-alpine` via testcontainers; bench-tuned (`set_tcp_nodelay`, `max_inflight_messages 0`, `max_queued_messages 0`, no persistence/`$SYS`/logs) | Podman or Docker |
| `mock` | Out-of-process minimal MQTT 5 broker (multi-thread Tokio, Arc fanout, `TCP_NODELAY`, inbound PUBLISH counter on a stats port; no Podman/Docker) | None |

### Matrix filters

| Env | Default | Meaning |
|-----|---------|---------|
| `MQTT_COMPARE_RUNS` | 5 | Median-of runs per cell |
| `MQTT_COMPARE_QOS` | `1` | `0`, `1`, or `0,1` |
| `MQTT_COMPARE_PAYLOADS` | `256` | Comma sizes |
| `MQTT_COMPARE_ACK` | `after_enqueue,before` | Ingress QoS1 ack policies |
| `MQTT_COMPARE_SCENARIOS` | all six | `ingress_load`, `outbound_serial`, `outbound_pipelined`, `outbound_concurrent`, `ingress_serial`, `outbound_serial_latency` |
| `MQTT_COMPARE_OUT` | `target/mqtt_compare/<ts>` | Output directory |

```shell
# Published baseline (Mosquitto, median-of-100)
MQTT_COMPARE_RUNS=100 cargo bench --bench mqtt_compare

# Smoke (mock, one run, qos1 payload 256 only)
MQTT_COMPARE_BROKER=mock MQTT_COMPARE_RUNS=1 cargo bench --bench mqtt_compare

# Only HTTP-style outbound fan-out
MQTT_COMPARE_BROKER=mock MQTT_COMPARE_RUNS=1 MQTT_COMPARE_SCENARIOS=outbound_concurrent \
  cargo bench --bench mqtt_compare

# Only glide + rumqttc, after_enqueue cutover-style cells
MQTT_COMPARE_CLIENTS=glide,rumqttc MQTT_COMPARE_ACK=after_enqueue MQTT_COMPARE_RUNS=10 cargo bench --bench mqtt_compare
```
### Fairness notes

- Shared fixtures: broker, payload pattern, `LOAD_WINDOW` / `LOAD_CONCURRENCY` 64, `LOAD_MESSAGES` 5000, receive_max 1024.
- Client adapters use idiomatic APIs (not a lowest-common-denominator path).
- Unsupported caps are `n/a` — no fake queues / fake manual ack.
- **Ingress is always handoff:** every ingress cell uses decode → bounded app queue (flume) → drain. There is no “process inline / no app queue” scenario. QoS 1 `after_enqueue` vs `before` only moves PUBACK relative to that handoff. See [`BENCHMARKS.md`](../BENCHMARKS.md).
- Ingress load CPU uses a publisher subprocess (`getrusage` excludes publisher).
- **Outbound load stopwatch (mock):** after the client burst, the parent sends `WAIT N` on the mock stats port; the mock replies `RECEIVED <count>` when it has decoded N PUBLISH frames (or `TIMEOUT <count>` after 60s). Exact count mismatch fails the cell with that message.
- **Outbound QoS 0 on Mosquitto:** load **and** serial latency are `n/a` (`qos0_no_wire_counter`). Use mock for fair QoS 0 wire throughput/latency; Mosquitto cutover outbound uses **QoS 1**.
- **Outbound serial latency (mock):** each sample is `publish` + `WAIT 1` (broker decoded). Mosquitto QoS 1 latency remains PUBACK-bounded `publish().await`.
- **Outbound serial feed (QoS 0, Mock only):** same `WAIT N` stopwatch (wall msg/s is wire-fair), but each `publish().await` still means different things — rumqttc mostly EventLoop enqueue; glide waits for write-buffer encode; mqtt5 encodes/`write_packet` on the caller. Prefer **pipelined** / **concurrent** / QoS 1 for visible per-message completion; see [`BENCHMARKS.md`](../BENCHMARKS.md) caveats.
- **Outbound pipelined:** one feeder keeps `LOAD_WINDOW` in flight (glide: `publish_window`; rumqttc QoS 1 notices; mqtt5 spawns).
- **Outbound concurrent:** `LOAD_CONCURRENCY` tasks share one session and only call `publish_serial` (HTTP-style fan-out; harness spawn is identical for every client).
- **Outbound QoS 1 on Mosquitto:** serial is RTT-bound; pipelined / concurrent (64) are the fair sustained-throughput compares (PUBACK ≈ broker saw the message).

## codec_throughput

No-broker microbench for outbound wire attribution: `encode_publish`
(including topic validation). Indicative numbers live in [`BENCHMARKS.md`](../BENCHMARKS.md) / README highlights.

```shell
cargo bench --bench codec_throughput
```
