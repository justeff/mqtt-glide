# Benchmarks

Absolute numbers for mqtt-glide, rumqttc-next, and LabOverWire mqtt5 against the same broker, same payload, same harness. Nothing here is a paired A/B: every number is measured on its own and only means something together with the conditions it was measured under.

Versions: mqtt-glide `main` (pre-0.3; the crate still says 0.2.0), `rumqttc-next` 0.33.3, `mqtt5` 0.37.2. Payload 256 B, median of 5 runs, 50 000-message bursts. The headline scenario is a service sharing one client between many tasks (`outbound_concurrent`); the other scenarios model other shapes of use. Read [How to read this page](#how-to-read-this-page) before quoting any single number.

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
  taskset -c 0-1 cargo bench --bench mqtt_compare
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
| rumqttc-next | The request is accepted onto the event-loop channel; encode and write happen later |
| mqtt5 | The calling task has encoded and written the packet |

Per-publish latency in `outbound_concurrent` and `outbound_rate` is the time to this completion (QoS 0) or to PUBACK (QoS 1), not wire arrival. Throughput is always wire-complete via the barrier above (mock) or PUBACK (Mosquitto).

### Other notes

- Client adapters use idiomatic APIs, not a lowest-common-denominator path. Pipelined uses `publish_window` (glide), notices (rumqttc), and spawns (mqtt5).
- `outbound_spawn` is an extreme shape: 50 000 tasks wait on the client at once.
- Serial latency rows are broker round trips; small differences between clients there are within what a single idle probe can resolve.

## Observations

Facts read off the tables below, on this host and pinning only.

- QoS 1 on tuned Mosquitto: at 64 and 512 callers, glide has the highest msg/s and lowest CPU/msg in this run; at 8 callers all three are within about 6% of each other (the broker round trip dominates). Against stock Mosquitto (Receive Maximum 20) every client loses throughput at higher concurrency compared with the tuned broker, and the CPU/msg ordering at 64 and 512 callers is no longer glide first (rumqttc is lower there).
- `outbound_spawn`: rumqttc reaches about 23-25k msg/s at about 44-48 µs CPU/msg, versus roughly 150-520k msg/s for the others. It slows sharply when tens of thousands of tasks wait on its request channel at once; this burst shape is extreme.
- Peers win some cells: rumqttc and mqtt5 have lower idle `outbound_serial_latency` at QoS 0 on the mock (about 14-15 µs p50 versus 20 µs), mqtt5 is lowest on idle QoS 1 latency on tuned Mosquitto, and rumqttc is ahead on mock `ingress_load` at QoS 0.
- `outbound_rate`: all clients keep up with 10k and 50k msg/s offered; the differences are CPU/msg and the tail (p99.9), not throughput.
- Ingress load on Mosquitto is about 154k msg/s for every client, which is the broker/publisher pace, not a client limit.

<!-- benchmarks:generated:start -->

## Headline: QoS 1, 256 B, tuned Mosquitto

**Bold** = highest msg/s, lowest CPU ns/msg, lowest p50 / p99 / p99.9 among `status: ok` clients in that row (ties all bold).

| Scenario | glide | rumqttc | mqtt5 |
|---|---:|---:|---:|
| Outbound concurrent, 64 callers | **272 531** · **4 128** · **243** / **311** / **505** µs | 209 668 · 4 673 · 298 / 506 / 1 828 µs | 169 067 · 6 323 · 377 / 488 / 993 µs |
| Outbound concurrent, 512 callers | **311 459** · **3 687** · **1 661** / **2 022** / **2 378** µs | 224 353 · 4 401 · 2 371 / 3 778 / 4 392 µs | 177 379 · 6 340 · 2 883 / 4 312 / 4 624 µs |
| Outbound spawn | **313 104** · **5 140** | 22 626 · 48 117 | 156 195 · 9 091 |
| Outbound rate, 50 000 msg/s offered | 49 616 · **5 311** · **425** / **912** / **1 063** µs | **49 625** · 5 926 · 669 / 1 005 / 1 498 µs | 49 616 · 7 911 · 511 / 973 / 2 558 µs |
| Outbound pipelined | **304 930** · **3 768** | 216 711 · 4 463 | 169 280 · 7 374 |

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
| 8 callers | **153 186** · **5 004** · **51.7** / **75.5** / **85.0** µs | 151 899 · 5 203 · 52.4 / 79.0 / 89.1 µs | 144 617 · 6 608 · 54.1 / 87.2 / 116 µs |
| 64 callers | **272 531** · **4 128** · **243** / **311** / **505** µs | 209 668 · 4 673 · 298 / 506 / 1 828 µs | 169 067 · 6 323 · 377 / 488 / 993 µs |
| 512 callers | **311 459** · **3 687** · **1 661** / **2 022** / **2 378** µs | 224 353 · 4 401 · 2 371 / 3 778 / 4 392 µs | 177 379 · 6 340 · 2 883 / 4 312 / 4 624 µs |

### Outbound spawn (one task per message, 50k at once)

Units: msg/s · CPU ns/msg (no latency: all tasks start at once).

#### QoS 1

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | **313 104** · **5 140** | 22 626 · 48 117 | 156 195 · 9 091 |

### Outbound rate (open loop, offered msg/s)

Units: msg/s · CPU ns/msg · p50 / p99 / p99.9 µs per publish.

#### QoS 1

| Level | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 10 000 msg/s offered | 9 979 · **7 456** · 123 / **201** / **288** µs | 9 982 · 8 752 · 126 / 219 / 923 µs | **9 982** · 9 258 · **123** / 204 / 712 µs |
| 50 000 msg/s offered | 49 616 · **5 311** · **425** / **912** / **1 063** µs | **49 625** · 5 926 · 669 / 1 005 / 1 498 µs | 49 616 · 7 911 · 511 / 973 / 2 558 µs |

### Outbound pipelined (window 64, one task)

Units: msg/s · CPU ns/msg.

#### QoS 1

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | **304 930** · **3 768** | 216 711 · 4 463 | 169 280 · 7 374 |

### Outbound serial latency (idle round trip)

Units: p50 / p99 µs.

#### QoS 1

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | 27.8 / 32.4 | 27.4 / 34.7 | **23.4** / **30.1** |

### Ingress load

Units: msg/s · CPU ns/msg.

#### QoS 1 · `after_enqueue`

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | 154 187 · **4 171** | **154 622** · 4 817 | n/a (`no_manual_ack`) |

#### QoS 1 · `before`

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | n/a (`no_auto_ack`) | **154 183** · **4 101** | 152 254 · 5 013 |

### Ingress serial latency (idle)

Units: p50 / p99 µs.

#### QoS 1 · `after_enqueue`

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | 32.7 / 42.9 | **31.1** / **39.5** | n/a (`no_manual_ack`) |

#### QoS 1 · `before`

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | n/a (`no_auto_ack`) | **27.0** / 60.8 | 27.6 / **59.4** |


---

## Mosquitto (stock limits)

Broker: same image with Mosquitto's defaults, `max_inflight_messages 20` / `max_queued_messages 1000`. The broker advertises Receive Maximum 20 in CONNACK, so a client may have at most 20 unacknowledged QoS 1 PUBLISH packets in flight. QoS 1 only.

**Bold** = highest msg/s, lowest CPU ns/msg, lowest p50 / p99 / p99.9 among `status: ok` clients in that row (ties all bold).

### Outbound concurrent (callers sharing one client)

Units: msg/s · CPU ns/msg · p50 / p99 / p99.9 µs per publish.

#### QoS 1

| Level | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 8 callers | **150 763** · **4 983** · **52.4** / **75.5** / **87.7** µs | 149 622 · 5 247 · 52.8 / 78.7 / 89.9 µs | 144 593 · 6 638 · 54.2 / 86.8 / 110 µs |
| 64 callers | **215 191** · 4 973 · **298** / **379** / **497** µs | 210 991 · **4 517** · 302 / 379 / 614 µs | 170 234 · 6 404 · 373 / 514 / 1 030 µs |
| 512 callers | **228 761** · 5 074 · **2 229** / 2 659 / **2 699** µs | 211 660 · **4 609** · 2 412 / **2 650** / 2 699 µs | 176 810 · 6 267 · 2 859 / 4 333 / 4 677 µs |

### Outbound spawn (one task per message, 50k at once)

Units: msg/s · CPU ns/msg (no latency: all tasks start at once).

#### QoS 1

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | **183 165** · **7 627** | 22 590 · 48 719 | 156 934 · 9 011 |

### Outbound rate (open loop, offered msg/s)

Units: msg/s · CPU ns/msg · p50 / p99 / p99.9 µs per publish.

#### QoS 1

| Level | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 10 000 msg/s offered | 9 980 · **7 575** · 123 / 208 / 870 µs | 9 980 · 8 792 · 125 / 221 / 689 µs | **9 980** · 9 280 · **123** / **207** / **651** µs |
| 50 000 msg/s offered | 49 617 · 6 480 · 405 / **910** / **1 322** µs | **49 621** · **6 366** · **395** / 950 / 1 721 µs | 49 613 · 7 794 · 508 / 987 / 2 370 µs |

### Outbound pipelined (window 64, one task)

Units: msg/s · CPU ns/msg.

#### QoS 1

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | **226 648** · 4 474 | 213 620 · **3 969** | 170 700 · 7 510 |

### Outbound serial latency (idle round trip)

Units: p50 / p99 µs.

#### QoS 1

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | 27.9 / 32.8 | **27.0** / **32.3** | 33.8 / 37.5 |

### Ingress load

Units: msg/s · CPU ns/msg.

#### QoS 1 · `after_enqueue`

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | 154 368 · **4 143** | **154 997** · 4 879 | n/a (`no_manual_ack`) |

#### QoS 1 · `before`

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | n/a (`no_auto_ack`) | **153 771** · **4 128** | 153 059 · 4 992 |

### Ingress serial latency (idle)

Units: p50 / p99 µs.

#### QoS 1 · `after_enqueue`

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | 33.0 / 68.0 | **32.0** / **40.7** | n/a (`no_manual_ack`) |

#### QoS 1 · `before`

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | n/a (`no_auto_ack`) | **25.7** / **37.6** | 28.1 / 37.9 |


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
| 8 callers | **1 162 328** · **1 447** · **6.3** / **17.5** / 55.9 µs | 275 261 · 4 310 · 28.9 / 32.1 / 162 µs | 309 121 · 2 239 · 25.8 / 29.5 / **35.5** µs |
| 64 callers | **1 342 928** · **1 247** · **42.7** / **67.7** / **117** µs | 284 203 · 4 626 · 218 / 282 / 450 µs | 303 998 · 2 350 · 209 / 236 / 800 µs |
| 512 callers | **1 411 507** · **1 232** · **261** / **1 771** / 3 227 µs | 268 771 · 4 823 · 1 837 / 2 082 / **2 143** µs | 302 374 · 2 307 · 1 666 / 1 943 / 2 981 µs |

#### QoS 1

| Level | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 8 callers | **332 024** · **4 069** · **23.0** / **38.4** / **54.4** µs | 232 811 · 4 688 · 30.2 / 58.2 / 65.8 µs | 222 294 · 5 900 · 35.3 / 61.3 / 139 µs |
| 64 callers | **415 881** · **3 667** · **154** / **177** / **196** µs | 224 850 · 5 362 · 282 / 374 / 479 µs | 216 907 · 5 852 · 286 / 497 / 1 313 µs |
| 512 callers | **418 046** · **3 523** · **1 221** / **1 895** / **2 376** µs | 235 208 · 5 334 · 2 197 / 2 830 / 3 562 µs | 209 771 · 6 134 · 2 369 / 3 718 / 3 994 µs |

### Outbound spawn (one task per message, 50k at once)

Units: msg/s · CPU ns/msg (no latency: all tasks start at once).

#### QoS 0

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | **524 973** · **3 335** | 24 855 · 44 274 | 213 726 · 5 227 |

#### QoS 1

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | **399 561** · **4 429** | 25 053 · 43 929 | 169 307 · 8 821 |

### Outbound rate (open loop, offered msg/s)

Units: msg/s · CPU ns/msg · p50 / p99 / p99.9 µs per publish.

#### QoS 0

| Level | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 10 000 msg/s offered | **9 982** · **5 881** · 24.8 / **53.9** / **63.1** µs | 9 981 · 6 372 · **18.6** / 59.3 / 87.3 µs | 9 980 · 6 578 · 31.4 / 62.0 / 788 µs |
| 50 000 msg/s offered | 49 622 · 3 463 · **91.5** / 232 / 582 µs | 49 632 · 3 633 · 92.7 / 236 / **327** µs | **49 642** · **3 388** · 98.7 / **218** / 367 µs |

#### QoS 1

| Level | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 10 000 msg/s offered | **9 982** · **7 450** · **70.8** / **102** / **220** µs | 9 980 · 8 790 · 76.9 / 124 / 863 µs | 9 978 · 9 627 · 76.1 / 137 / 477 µs |
| 50 000 msg/s offered | 49 628 · **4 321** · 261 / **334** / **799** µs | 49 627 · 5 174 · **250** / 364 / 1 334 µs | **49 629** · 5 429 · 302 / 495 / 2 397 µs |

### Outbound pipelined (window 64, one task)

Units: msg/s · CPU ns/msg.

#### QoS 0

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | **1 897 562** · **937** | 262 258 · 5 850 | 253 378 · 4 717 |

#### QoS 1

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | **436 053** · **3 509** | 236 235 · 4 965 | 192 680 · 7 095 |

### Outbound serial latency (idle round trip)

Units: p50 / p99 µs.

#### QoS 0

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | 20.0 / 22.1 | 14.8 / 18.9 | **14.3** / **17.5** |

#### QoS 1

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | 29.3 / 32.5 | 29.1 / 33.5 | **24.7** / **29.9** |

### Ingress load

Units: msg/s · CPU ns/msg.

#### QoS 0

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | 356 246 · 2 698 | **385 952** · **2 289** | 249 962 · 4 241 |

#### QoS 1 · `after_enqueue`

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | **242 350** · **2 209** | 225 490 · 2 964 | n/a (`no_manual_ack`) |

#### QoS 1 · `before`

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | n/a (`no_auto_ack`) | **213 044** · **2 162** | 196 341 · 5 160 |

### Ingress serial latency (idle)

Units: p50 / p99 µs.

#### QoS 0

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | **12.6** / **14.3** | 14.5 / 16.9 | 15.9 / 18.5 |

#### QoS 1 · `after_enqueue`

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | **12.2** / **15.3** | 14.7 / 17.6 | n/a (`no_manual_ack`) |

#### QoS 1 · `before`

| Payload | glide | rumqttc | mqtt5 |
|---:|---:|---:|---:|
| 256 B | n/a (`no_auto_ack`) | **15.6** / **18.7** | 16.5 / 20.5 |


---

## Refresh

```shell
# one run per broker, pinned (see benches/README.md)
for b in mock mosquitto mosquitto-stock; do
  q=1; [ $b = mock ] && q=0,1
  MQTT_COMPARE_BROKER=$b MQTT_COMPARE_QOS=$q \
  MQTT_COMPARE_BROKER_CPUS=2-3 MQTT_COMPARE_PUBLISHER_CPUS=2-3 \
  MQTT_COMPARE_RUNS=5 MQTT_COMPARE_OUT=target/mqtt_compare/full-$b \
    taskset -c 0-1 cargo bench --bench mqtt_compare
done

# rebuild the tables above
python3 benches/render_benchmarks.py
```

<!-- benchmarks:generated:end -->

## Reproducing

Everything above the generated tables describes *what* was measured; [`benches/README.md`](benches/README.md) describes *how* to run it.

It covers the env knobs (`MQTT_COMPARE_*`), the broker backends, the fairness constraints baked into the harness, and the JSONL output that `python3 benches/render_benchmarks.py` turns back into the tables on this page.
