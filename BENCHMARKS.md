# Benchmarks

Absolute numbers for mqtt-glide, rumqttc-v5-next, and LabOverWire mqtt5 against the same broker, same payload, same harness. Nothing here is a paired A/B: every number is measured on its own and only means something together with the conditions it was measured under.

Versions: mqtt-glide `main` (pre-0.3; the crate still says 0.2.0), `rumqttc-v5-next` 0.33.3, `mqtt5` 0.48.0. Payload 256 B, median of 5 runs, 50 000-message bursts. The headline scenario is a service sharing one client between many tasks (`outbound_concurrent`); the other scenarios model other shapes of use. Read [How to read this page](#how-to-read-this-page) before quoting any single number.

## How to read this page

Every number is one cell: one client, one scenario (and level), one QoS, one ack policy, one broker. Cells are only comparable to cells that differ in the client column.

### Scenarios

| Scenario | Models | What it measures |
|----------|--------|------------------|
| `outbound_concurrent` | A service: one shared client, many request tasks | 8 / 64 / 512 long-lived caller tasks, each awaiting every `publish`. msg/s, CPU ns/msg, per-publish p50 / p99 / p99.9. **Headline.** |
| `outbound_pipelined` | Bulk export | One task keeps a window of 64 in flight through each client's idiomatic pipeline. msg/s, CPU ns/msg |
| `outbound_spawn` | A relay: one task per message | The whole 50 000-message burst is spawned at once, each task awaiting its publish. msg/s, CPU ns/msg (no latency: all tasks start at once) |
| `outbound_rate` | A given load | Open loop at 10 000 and 50 000 msg/s offered (1 ms ticks, 2 s). The generator never waits on the client; latency runs from each task's release, so falling behind shows as latency and a lower achieved rate. CPU ns/msg at a fixed load |
| `outbound_serial_latency` | Idle round-trip probe | One publish at a time on an idle connection. p50 / p99 |
| `ingress_load` | A subscriber under burst | A publisher process bursts at the client under test: decode, bounded app queue, drain. msg/s, CPU ns/msg |
| `ingress_serial` | Idle round-trip probe | One message at a time, publisher send until the subscriber pulls it off the app queue. p50 / p99 |

There is no serial-throughput scenario any more: one task awaiting each publish in turn mostly measures the round trip, so it survives only as the idle latency probe.

### Units

Load cells are `msg/s · CPU ns/msg`, extended with `· p50 / p99 / p99.9 µs` where the scenario records per-publish latency. Serial latency cells are `p50 / p99 µs`. **Bold** marks, per metric and per row, the best value among clients with `status: ok` (highest msg/s, lowest CPU and latency); ties are all bold. Bold is not a significance test.

CPU ns/msg is `getrusage` of the whole bench process: the client, its runtime threads **and the harness tasks** (callers, spawner, ticker, drain). Harness work is identical across clients but not free. Ingress load excludes the publisher child process. Outbound rate rows show about 49.6k of the 50k offered for every client: the wall time also covers draining the last messages after the 2 s schedule, so this is not a client limit.

### Ack policies

Apply to ingress QoS 1 only and decide *when* PUBACK is sent relative to the app-queue handoff. Both policies still go through the queue.

| Policy | Meaning | Who can do it |
|--------|---------|---------------|
| `after_enqueue` | PUBACK after the app queue accepted the message | mqtt-glide (only mode), rumqttc via `manual_acks` |
| `before` | PUBACK on decode, before the app sees the message | rumqttc auto-ack, mqtt5 (inherent) |

Cross-policy cells (glide `after_enqueue` against a peer's `before`) are not cutover-fair.

### `n/a` reasons

| Marker | Meaning |
|--------|---------|
| `n/a (no_manual_ack)` | mqtt5 has no enqueue-before-ack API |
| `n/a (no_auto_ack)` | mqtt-glide does not expose auto-ack; use `after_enqueue` |

Mosquitto outbound QoS 0 is not run at all, by design: there is no wire counter to know when the broker saw the messages (see below).

## Setup

### Host

| | |
|--|--|
| CPU | Intel Core Ultra 7 258V (Lunar Lake): 4 P-cores (CPUs 0-3), 4 LP E-cores (CPUs 4-7), no SMT |
| Memory | 32 GB (30 GiB usable) |
| OS | Fedora Linux 44, kernel 7.2.10, cpufreq governor `performance` |
| Toolchain | rustc 1.99.0 |
| Container | Podman 5.8.7 (rootless), eclipse-mosquitto `2.1.2-alpine` |

### CPU pinning

| Process | CPUs |
|---------|------|
| Client under test (whole bench process; Tokio multi-thread runtime → 2 workers) | `taskset -c 0-1` |
| Broker (mock child or Mosquitto container) and ingress publisher | 2-3 (`MQTT_COMPARE_BROKER_CPUS=2-3`, `MQTT_COMPARE_PUBLISHER_CPUS=2-3`) |

Why: on this hybrid CPU, placing the broker on the LP E-cores made the broker the bottleneck (QoS 1 capped near 170k msg/s) and roughly doubled the client's CPU/msg through cross-cluster traffic. Sharing all P-cores lets client and broker compete and distorts p99. Splitting the P-cores 2/2 isolates both. Absolute numbers are specific to this split; on other hosts, re-pin accordingly.

### Matrix

| Knob | Value |
|------|--------|
| Runs | median of 5 |
| Load burst | 50 000 messages (`MQTT_COMPARE_LOAD_MSGS`) |
| Concurrency levels | 8, 64, 512 |
| Offered rates | 10 000 and 50 000 msg/s, 2 s each |
| Pipelined window | 64 |
| Payload | 256 B |
| QoS | mock: 0 and 1; Mosquitto (both): 1 |
| Clients | `glide`, `rumqttc`, `mqtt5` |
| Source | `target/mqtt_compare/full-{mock,mosquitto,mosquitto-stock}/` (JSONL + `meta.json`) |

Command (per broker; QoS `0,1` for mock, `1` for Mosquitto):

```shell
MQTT_COMPARE_BROKER=<mock|mosquitto|mosquitto-stock> MQTT_COMPARE_QOS=<0,1|1> \
MQTT_COMPARE_BROKER_CPUS=2-3 MQTT_COMPARE_PUBLISHER_CPUS=2-3 MQTT_COMPARE_RUNS=5 \
  taskset -c 0-1 cargo bench --features __bench --bench mqtt_compare
```

### Brokers

| Backend | Role |
|---------|------|
| `mosquitto` | Tuned (`max_inflight_messages 0`, `max_queued_messages 0`, `set_tcp_nodelay`, no persistence / `$SYS` / logs). The published baseline |
| `mosquitto-stock` | Stock `max_inflight_messages 20` / `max_queued_messages 1000`, i.e. CONNACK Receive Maximum 20. Added because the tuned broker hid problems with a small broker window: mqtt-glide previously failed in this setup for windows wider than 20 (fixed on main) |
| `mock` | Out-of-process mock MQTT 5 (multi-thread, Arc fanout, `TCP_NODELAY`, inbound PUBLISH counter, `receive_max` 1024). A client-side ceiling, not a broker claim |

## Fairness rules

### Ingress is always the handoff use case

All ingress cells measure decode → bounded app queue (flume) → drain, never "handle inline in the decode path". Peers are adapted to that shape. QoS 1 `after_enqueue` / `before` only moves PUBACK relative to the handoff.

### Outbound: wire counter (mock) vs Mosquitto

On the mock, outbound wall time ends when its stats port answers `WAIT N` with the count of decoded PUBLISH frames; a count mismatch or timeout fails the cell, so an early `publish().await` return cannot end a cell early. On Mosquitto only QoS 1 is usable (PUBACK means the broker saw the message), so outbound QoS 0 is skipped there. Do not read that as missing coverage.

### What `publish().await` returns on

The same call guarantees different things per client at QoS 0, which matters for the latency columns:

| Client | `publish().await` returns when (QoS 0) |
|--------|----------------------------------------|
| mqtt-glide | The write task has encoded the packet into the connection write buffer |
| rumqttc-v5-next | The request is accepted onto the event-loop channel; encode and write happen later |
| mqtt5 | The calling task has encoded and written the packet |

Per-publish latency in `outbound_concurrent` and `outbound_rate` is the time to this completion (QoS 0) or to PUBACK (QoS 1), not wire arrival. Throughput is always wire-complete via the barrier above (mock) or PUBACK (Mosquitto).

### Other notes

- Client adapters use idiomatic APIs, not a lowest-common-denominator path. Pipelined uses `publish_window` (glide), notices (rumqttc), and spawns (mqtt5). rumqttc is pinned to 0.33.3; see Observations for why.
- `outbound_spawn` is an extreme shape: 50 000 tasks wait on the client at once.
- Serial latency rows are broker round trips; small differences between clients there are within what a single idle probe can resolve.

## Observations

Facts read off the tables below, on this host and pinning only.

- QoS 1 on tuned Mosquitto: glide has the highest msg/s and lowest CPU/msg at 8, 64 and 512 callers (8 callers: 156k against 148k and 138k, the broker round trip dominates there). Against stock Mosquitto (Receive Maximum 20) glide and mqtt5 lose throughput at 64 and 512 callers compared with the tuned broker (glide about 293k to 244k at 64 and 317k to 237k at 512; mqtt5 about 172k to 144k and 216k to 145k), while rumqttc barely does (about 206k to 201k at 64 and 220k to 211k at 512). Glide still leads msg/s on the stock broker, but at 64 and 512 callers and pipelined rumqttc has the lower CPU/msg (about 4.5k against 4.6-4.7k at 64 and 512 callers; 3.9k against 4.2k pipelined).
- rumqttc is pinned to 0.33.3 because 0.34.0 scans its whole outgoing table on every flush. Against a broker without a Receive Maximum that table has 65 535 slots and throughput falls to a few thousand msg/s.
- `outbound_spawn`: rumqttc reaches about 23-25k msg/s at about 44-48 µs CPU/msg, versus roughly 129-512k msg/s for the others. It slows sharply when tens of thousands of tasks wait on its request channel at once; this burst shape is extreme.
- Peers win some cells: rumqttc and mqtt5 have lower idle `outbound_serial_latency` at QoS 0 on the mock (about 14-15 µs p50 versus 20 µs), mqtt5 is lowest on idle QoS 1 latency on the mock and on tuned Mosquitto (24-26 µs versus 27-29 µs; on stock Mosquitto glide and rumqttc are within 1 µs and ahead of mqtt5), and rumqttc is ahead on mock `ingress_load` at QoS 0 (422k versus 367k msg/s).
- Idle p99 values (serial latency, especially ingress) move by tens of µs between refreshes for every client; only the p50 columns are stable enough to compare. Throughput can move too: mqtt5 at 512 callers on tuned Mosquitto has been 204k, 216k and 230k across refreshes.
- `outbound_rate`: all clients keep up with 10k and 50k msg/s offered; the differences are CPU/msg and the tail (p99.9), not throughput.
- Ingress load on Mosquitto is about 153-155k msg/s for every client, which is the broker/publisher pace, not a client limit.

## Cost of the `stats` feature

The `stats` feature counts events on the connection tasks (see [Metrics](README.md#metrics) in the README). To see what that costs, the same code was run with the feature off and on, interleaved, 3 rounds each. Only glide was run: tuned Mosquitto for the outbound cells, the mock broker for ingress. Hardware, pinning and payload are as described under [Setup](#setup).

| Cell | off msg/s | on msg/s | Δ | off ns/msg | on ns/msg | Δ |
|---|---:|---:|---:|---:|---:|---:|
| `ingress_load`, QoS 0 | 373.6k | 374.4k | +0.2% | 2292 | 2366 | +3.2% |
| `ingress_load`, QoS 1 `after_enqueue` | 232.7k | 233.8k | +0.5% | 2241 | 2262 | +0.9% |
| `outbound_concurrent`, 512 callers, QoS 1 | 328.0k | 331.6k | +1.1% | 3200 | 3230 | +0.9% |
| `outbound_concurrent`, 64 callers, QoS 1 | 313.0k | 311.7k | -0.4% | 3312 | 3365 | +1.6% |
| `outbound_concurrent`, 8 callers, QoS 1 | 170.0k | 169.8k | -0.1% | 4200 | 4246 | +1.1% |
| `outbound_pipelined`, QoS 1 | 305.7k | 318.8k | +4.3% | 3639 | 3582 | -1.6% |

`ns/msg` is client CPU time per message. Throughput is unchanged: the differences go both ways, and the largest (+4.3%, pipelined, in favour of the feature being on) is not a speedup the counters can cause. CPU per message is within about 2% in every cell but one (`ingress_load` at QoS 0, +3.2%), mostly slightly higher with the feature on. The spread from round to round is of the same size, so these runs cannot resolve a cost smaller than that.

<!-- benchmarks:generated:start -->

## Headline: QoS 1, 256 B, tuned Mosquitto

**Bold** = highest msg/s, lowest CPU ns/msg, lowest p50 / p99 / p99.9 among `status: ok` clients in that row (ties all bold).

| Scenario | glide | rumqttc | mqtt5 |
|---|---:|---:|---:|
| Outbound concurrent, 64 callers | **293 160** · **3 600** · **216** / **290** / 635 µs | 206 252 · 4 782 · 306 / 513 / **554** µs | 171 652 · 7 590 · 386 / 536 / 1 363 µs |
| Outbound concurrent, 512 callers | **316 591** · **3 430** · **1 655** / **2 007** / **2 083** µs | 220 438 · 4 540 · 2 359 / 3 777 / 4 052 µs | 215 616 · 6 810 · 2 152 / 3 613 / 3 825 µs |
| Outbound spawn | **321 036** · **4 949** | 23 763 · 46 088 | 179 119 · 9 554 |
| Outbound rate, 50 000 msg/s offered | 49 611 · **5 014** · **331** / **875** / **1 213** µs | 49 614 · 5 930 · 617 / 938 / 1 646 µs | **49 621** · 8 248 · 622 / 1 030 / 2 261 µs |
| Outbound pipelined | **298 327** · **3 691** | 216 274 · 4 527 | 180 156 · 8 513 |

---

## Mosquitto (tuned: unlimited inflight / queue)

Broker: eclipse-mosquitto `2.1.2-alpine` via testcontainers (anonymous; `set_tcp_nodelay`; `max_inflight_messages 0`, `max_queued_messages 0`; no persistence / `$SYS` / logs).

**Bold** = highest msg/s, lowest CPU ns/msg, lowest p50 / p99 / p99.9 among `status: ok` clients in that row (ties all bold).

Outbound QoS 0 is omitted: no wire counter without a measuring subscriber; see the mock tables.

### Outbound concurrent (callers sharing one client)

Units: msg/s · CPU ns/msg · p50 / p99 / p99.9 µs per publish.

#### QoS 1

| Level | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 8 callers | **155 716** · **4 329** · **51.0** / **71.7** / **83.6** µs | 148 347 · 5 223 · 53.3 / 79.1 / 93.5 µs | 138 263 · 8 185 · 57.3 / 89.1 / 105 µs |
| 64 callers | **293 160** · **3 600** · **216** / **290** / 635 µs | 206 252 · 4 782 · 306 / 513 / **554** µs | 171 652 · 7 590 · 386 / 536 / 1 363 µs |
| 512 callers | **316 591** · **3 430** · **1 655** / **2 007** / **2 083** µs | 220 438 · 4 540 · 2 359 / 3 777 / 4 052 µs | 215 616 · 6 810 · 2 152 / 3 613 / 3 825 µs |

### Outbound spawn (one task per message, 50k at once)

Units: msg/s · CPU ns/msg (no latency: all tasks start at once).

#### QoS 1

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | **321 036** · **4 949** | 23 763 · 46 088 | 179 119 · 9 554 |

### Outbound rate (open loop, offered msg/s)

Units: msg/s · CPU ns/msg · p50 / p99 / p99.9 µs per publish.

#### QoS 1

| Level | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 10 000 msg/s offered | 9 981 · **7 037** · **121** / **171** / **324** µs | **9 981** · 8 760 · 127 / 224 / 770 µs | 9 981 · 11 013 · 129 / 233 / 788 µs |
| 50 000 msg/s offered | 49 611 · **5 014** · **331** / **875** / **1 213** µs | 49 614 · 5 930 · 617 / 938 / 1 646 µs | **49 621** · 8 248 · 622 / 1 030 / 2 261 µs |

### Outbound pipelined (window 64, one task)

Units: msg/s · CPU ns/msg.

#### QoS 1

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | **298 327** · **3 691** | 216 274 · 4 527 | 180 156 · 8 513 |

### Outbound serial latency (idle round trip)

Units: p50 / p99 µs.

#### QoS 1

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | 27.4 / 32.5 | 27.2 / 34.0 | **24.1** / **30.4** |

### Ingress load

Units: msg/s · CPU ns/msg.

#### QoS 1 · `after_enqueue`

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | **153 020** · **3 823** | 153 019 · 4 778 | n/a (`no_manual_ack`) |

#### QoS 1 · `before`

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | n/a (`no_auto_ack`) | 154 250 · **4 052** | **154 287** · 4 768 |

### Ingress serial latency (idle)

Units: p50 / p99 µs.

#### QoS 1 · `after_enqueue`

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | 32.9 / 44.6 | **31.8** / **41.3** | n/a (`no_manual_ack`) |

#### QoS 1 · `before`

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | n/a (`no_auto_ack`) | **25.6** / 43.5 | 27.5 / **42.0** |


---

## Mosquitto (stock limits)

Broker: same image with Mosquitto's defaults, `max_inflight_messages 20` / `max_queued_messages 1000`. The broker advertises Receive Maximum 20 in CONNACK, so a client may have at most 20 unacknowledged QoS 1 PUBLISH packets in flight. QoS 1 only.

**Bold** = highest msg/s, lowest CPU ns/msg, lowest p50 / p99 / p99.9 among `status: ok` clients in that row (ties all bold).

### Outbound concurrent (callers sharing one client)

Units: msg/s · CPU ns/msg · p50 / p99 / p99.9 µs per publish.

#### QoS 1

| Level | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 8 callers | **167 137** · **4 277** · **47.9** / **70.7** / **81.0** µs | 141 913 · 5 227 · 55.9 / 80.4 / 108 µs | 139 489 · 8 092 · 57.2 / 89.0 / 107 µs |
| 64 callers | **244 055** · 4 675 · **261** / **336** / 760 µs | 201 163 · **4 546** · 317 / 393 / **418** µs | 143 570 · 7 730 · 447 / 574 / 1 181 µs |
| 512 callers | **236 897** · 4 612 · **2 127** / **2 576** / **2 682** µs | 211 109 · **4 500** · 2 409 / 2 652 / 2 687 µs | 145 476 · 7 758 · 3 519 / 4 193 / 4 527 µs |

### Outbound spawn (one task per message, 50k at once)

Units: msg/s · CPU ns/msg (no latency: all tasks start at once).

#### QoS 1

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | **196 379** · **7 050** | 22 727 · 48 265 | 129 008 · 10 558 |

### Outbound rate (open loop, offered msg/s)

Units: msg/s · CPU ns/msg · p50 / p99 / p99.9 µs per publish.

#### QoS 1

| Level | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 10 000 msg/s offered | **9 982** · **7 233** · **121** / **187** / **581** µs | 9 980 · 8 807 · 126 / 213 / 822 µs | 9 981 · 10 825 · 128 / 226 / 1 354 µs |
| 50 000 msg/s offered | 49 608 · **5 923** · **346** / **902** / 1 448 µs | **49 630** · 6 433 · 409 / 910 / **1 110** µs | 49 619 · 9 532 · 505 / 1 045 / 1 622 µs |

### Outbound pipelined (window 64, one task)

Units: msg/s · CPU ns/msg.

#### QoS 1

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | **235 225** · 4 207 | 217 023 · **3 923** | 150 935 · 9 831 |

### Outbound serial latency (idle round trip)

Units: p50 / p99 µs.

#### QoS 1

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | 27.9 / 34.0 | **27.3** / **32.2** | 34.3 / 37.6 |

### Ingress load

Units: msg/s · CPU ns/msg.

#### QoS 1 · `after_enqueue`

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | **155 446** · **3 800** | 155 190 · 4 823 | n/a (`no_manual_ack`) |

#### QoS 1 · `before`

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | n/a (`no_auto_ack`) | 153 498 · **4 081** | **154 066** · 4 784 |

### Ingress serial latency (idle)

Units: p50 / p99 µs.

#### QoS 1 · `after_enqueue`

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | 31.7 / 41.5 | **31.6** / **39.6** | n/a (`no_manual_ack`) |

#### QoS 1 · `before`

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | n/a (`no_auto_ack`) | **25.7** / **36.7** | 27.3 / 40.0 |


---

## Mock ceiling

Broker: out-of-process mock MQTT 5 (`MQTT_COMPARE_BROKER=mock`): multi-thread Tokio, Arc fanout, `TCP_NODELAY`, inbound PUBLISH counter, `receive_max` 1024.
Use for client CPU / enqueue ceiling attribution, not as a broker product comparison.

**Bold** = highest msg/s, lowest CPU ns/msg, lowest p50 / p99 / p99.9 among `status: ok` clients in that row (ties all bold).

### Outbound concurrent (callers sharing one client)

Units: msg/s · CPU ns/msg · p50 / p99 / p99.9 µs per publish.

#### QoS 0

| Level | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 8 callers | **1 161 399** · **1 443** · **6.4** / **18.3** / 52.2 µs | 278 889 · 4 338 · 28.5 / 32.2 / 95.6 µs | 275 926 · 2 605 · 28.9 / 32.8 / **50.6** µs |
| 64 callers | **1 362 772** · **1 217** · **42.0** / **67.3** / **155** µs | 282 217 · 4 406 · 224 / 248 / 450 µs | 262 371 · 2 745 · 243 / 263 / 1 270 µs |
| 512 callers | **1 386 700** · **1 271** · **270** / **1 585** / **2 439** µs | 266 748 · 4 860 · 1 851 / 2 107 / 2 715 µs | 260 646 · 2 785 · 1 955 / 2 128 / 3 489 µs |

#### QoS 1

| Level | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 8 callers | **342 969** · **3 875** · **22.4** / **41.7** / **53.1** µs | 230 625 · 4 647 · 30.6 / 59.0 / 65.7 µs | 165 838 · 7 635 · 50.0 / 67.3 / 81.3 µs |
| 64 callers | **403 895** · **3 591** · **159** / **182** / **199** µs | 224 151 · 5 448 · 283 / 378 / 491 µs | 169 128 · 7 906 · 393 / 474 / 541 µs |
| 512 callers | **452 228** · **3 393** · **1 125** / **1 169** / **1 213** µs | 231 758 · 5 470 · 2 256 / 2 729 / 3 267 µs | 176 927 · 7 640 · 2 920 / 3 405 / 3 480 µs |

### Outbound spawn (one task per message, 50k at once)

Units: msg/s · CPU ns/msg (no latency: all tasks start at once).

#### QoS 0

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | **512 106** · **3 446** | 24 480 · 44 841 | 197 093 · 6 213 |

#### QoS 1

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | **396 043** · **4 362** | 25 160 · 43 946 | 170 145 · 9 622 |

### Outbound rate (open loop, offered msg/s)

Units: msg/s · CPU ns/msg · p50 / p99 / p99.9 µs per publish.

#### QoS 0

| Level | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 10 000 msg/s offered | 9 980 · **5 624** · 24.3 / **51.2** / **73.9** µs | **9 982** · 6 276 · **18.1** / 58.6 / 277 µs | 9 981 · 6 896 · 33.2 / 62.0 / 83.0 µs |
| 50 000 msg/s offered | 49 628 · **3 198** · **87.0** / **210** / 328 µs | **49 645** · 3 508 · 91.2 / 232 / **268** µs | 49 634 · 3 664 · 118 / 237 / 355 µs |

#### QoS 1

| Level | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 10 000 msg/s offered | 9 979 · **7 292** · **69.9** / **106** / **486** µs | 9 980 · 8 717 · 76.0 / 115 / 677 µs | **9 981** · 10 523 · 82.5 / 120 / 622 µs |
| 50 000 msg/s offered | **49 632** · **3 998** · **235** / **297** / 982 µs | 49 625 · 5 018 · 247 / 330 / **454** µs | 49 624 · 6 182 · 374 / 600 / 1 258 µs |

### Outbound pipelined (window 64, one task)

Units: msg/s · CPU ns/msg.

#### QoS 0

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | **1 922 414** · **952** | 266 640 · 5 870 | 241 208 · 5 034 |

#### QoS 1

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | **430 189** · **3 417** | 238 051 · 4 954 | 212 098 · 7 781 |

### Outbound serial latency (idle round trip)

Units: p50 / p99 µs.

#### QoS 0

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | 19.7 / 23.2 | 14.9 / 19.2 | **14.3** / **17.7** |

#### QoS 1

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | 29.4 / 32.9 | 29.0 / 32.2 | **25.9** / **28.9** |

### Ingress load

Units: msg/s · CPU ns/msg.

#### QoS 0

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | 366 516 · 2 450 | **422 390** · **2 212** | 302 788 · 3 138 |

#### QoS 1 · `after_enqueue`

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | **241 566** · **2 257** | 222 105 · 2 814 | n/a (`no_manual_ack`) |

#### QoS 1 · `before`

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | n/a (`no_auto_ack`) | **206 510** · **2 146** | 196 746 · 3 855 |

### Ingress serial latency (idle)

Units: p50 / p99 µs.

#### QoS 0

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | **12.3** / **13.9** | 14.2 / 16.7 | 15.7 / 17.7 |

#### QoS 1 · `after_enqueue`

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | **11.9** / **15.3** | 14.8 / 16.7 | n/a (`no_manual_ack`) |

#### QoS 1 · `before`

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | n/a (`no_auto_ack`) | 16.6 / **18.7** | **16.6** / 20.2 |


---

## Refresh

```shell
# one run per broker, pinned (see benches/README.md)
for b in mock mosquitto mosquitto-stock; do
  q=1; [ $b = mock ] && q=0,1
  MQTT_COMPARE_BROKER=$b MQTT_COMPARE_QOS=$q \
  MQTT_COMPARE_BROKER_CPUS=2-3 MQTT_COMPARE_PUBLISHER_CPUS=2-3 \
  MQTT_COMPARE_RUNS=5 MQTT_COMPARE_OUT=target/mqtt_compare/full-$b \
    taskset -c 0-1 cargo bench --features __bench --bench mqtt_compare
done

# rebuild the tables above
python3 benches/render_benchmarks.py
```

<!-- benchmarks:generated:end -->

## Reproducing

Everything above the generated tables describes *what* was measured; [`benches/README.md`](benches/README.md) describes *how* to run it.

It covers the env knobs (`MQTT_COMPARE_*`), the broker backends, the fairness constraints baked into the harness, and the JSONL output that `python3 benches/render_benchmarks.py` turns back into the tables on this page.
