# Benchmarks

Absolute multi-client results for **mqtt-glide**, **rumqttc-next**, and LabOverWire **mqtt5**.
Compare clients only within the same cell (scenario × QoS × payload × ack policy × broker).

How to run / fairness / caps: [`benches/README.md`](benches/README.md).

## Host

| | |
|--|--|
| Machine | Apple MacBook Pro (`MacBookPro18,2`) |
| Chip | Apple M1 Max, 10 cores (8 performance + 2 efficiency) |
| Memory | 32 GB |
| OS | macOS 26.5.2 |

## Matrix (this baseline)

| Knob | Value |
|------|--------|
| Runs | median-of **100** |
| Load burst | 5 000 messages, window / concurrency **64** |
| Payloads | 64 / 256 / 1 024 / 4 096 bytes |
| QoS | 0 and 1 |
| Clients | `glide`, `rumqttc`, `mqtt5` |
| mqtt-glide (this baseline) | **0.1.0** (not every release is re-benched) |
| Peer crates (locked) | `rumqttc-next` **0.33.3**, LabOverWire `mqtt5` **0.37.2** |
| Ack (ingress QoS 1 only) | `after_enqueue`, `before` |
| Source dirs | `target/mqtt_compare/full-mosquitto-100/`, `full-mock-100/`; concurrent: `outbound-concurrent-mosquitto-100/`, `outbound-concurrent-mock-100/` |
| Outbound QoS 0 | **Mock only** (wire `WAIT`); Mosquitto → `n/a` (`qos0_no_wire_counter`) including latency |
| Outbound QoS 1 | Mosquitto + mock (PUBACK / wire) |

**Mosquitto** is the published baseline (eclipse-mosquitto `2.1.2-alpine`, bench-tuned).
**Mock** is a ceiling / attribution broker (out-of-process, multi-thread, Arc fanout, inbound PUBLISH counter) — not a production broker claim.

### Reading the tables

- **Bold** in a row: best msg/s and lowest CPU (load), or lowest p50 and p99 (latency), among clients with `status: ok` (ties all bold).
- Load cells: **msg/s · CPU ns/msg** (`getrusage` of the bench process; ingress load excludes the publisher child).
- Latency cells: **p50 / p99 µs**.
- `n/a (no_manual_ack)` — mqtt5 has no enqueue-before-ack API.
- `n/a (no_auto_ack)` — mqtt-glide does not expose auto-ack; use `after_enqueue` for glide ingress QoS 1.
- `n/a (qos0_no_wire_counter)` — harness skip for Mosquitto outbound QoS 0 (not rendered in Mosquitto tables; see Mock).

Cross-policy cells (glide `after_enqueue` vs mqtt5/rumqttc `before`) are **not** cutover-fair.

### Caveats

#### Ingress is always the handoff use case

All **ingress** cells (load + serial latency, QoS 0 and 1) measure **decode → app queue (flume) → drain**, not “handle the message inline in a decode/callback path.”

- The harness always enqueues a handoff record and later `recv`s it (`recv_touch` / barrier on handoffs or PUBACKs).
- That matches mqtt-glide’s product model (`InboundSink` accept). Peers are adapted to the same shape so cutover compares stay fair.
- There is **no** bench cell for “I got a PUBLISH and processed it directly without an app queue.”
- QoS 1 `after_enqueue` / `before` only changes **when PUBACK is sent** relative to that handoff — both still use the queue.

#### Outbound load: wire counter (mock) vs Mosquitto

On **`MQTT_COMPARE_BROKER=mock`**, outbound serial/pipelined/concurrent wall time ends when the mock stats port answers `WAIT N` with `RECEIVED <count>` (inbound PUBLISH frames decoded). `TIMEOUT <count>` or a count ≠ N fails the cell. Early `publish().await` completion cannot finish the cell early.

On **Mosquitto**, all **outbound QoS 0** cells (serial / pipelined / concurrent load + serial latency) are **`n/a` (`qos0_no_wire_counter`)**. Prefer **QoS 1** for Mosquitto cutover outbound (PUBACK ≈ broker saw the message). Fair QoS 0 wire attribution lives on the mock.

#### Outbound feed shapes (serial vs pipelined vs concurrent)

| Scenario | Feeder model | Per-message API |
|----------|--------------|-----------------|
| **outbound_serial** | One task | `publish().await` then next |
| **outbound_pipelined** | One feeder, window 64 | Client-idiomatic pipeline (`publish_window` / rumqttc notices / mqtt5 spawns) |
| **outbound_concurrent** | 64 harness tasks, shared session | Only `publish().await` / `publish_serial` (HTTP-style fan-out) |

Mock outbound **load** ends on the same wire barrier (`WAIT N`), but clients still **feed** the burst differently when the harness calls idiomatic serial `publish().await` (Mock QoS 0 only — Mosquitto outbound QoS 0 is skipped):

| Client | What each serial `publish().await` does (QoS 0) | Encode / invalid-topic errors on that call? |
|--------|--------------------------------------------------|-----------------------------------------------|
| **mqtt-glide** | Waits until the write task has encoded into the connection write buffer (oneshot). | **Yes** (returned on the completion / `publish` result). |
| **rumqttc-next** | Accepts the request onto the EventLoop channel only; encode/write happen later in the event loop (often batched). | **No** on that `await` — mainly channel full / early topic checks; later failures surface via the event loop. |
| **mqtt5** | Encodes and `write_packet` / TCP write on the calling task, then returns. | **Yes** (on that call). |

With `WAIT N`, rumqttc **cannot** finish the cell early by enqueue-only awaits — wall msg/s is wire-complete for all three (and on Mock serial QoS 0, glide typically leads msg/s). What still differs is **what each `publish().await` guarantees** (write/completion vs channel accept) and thus CPU/shape of the feeder. Prefer **outbound pipelined**, **outbound concurrent**, and **QoS 1** for “sustained publish throughput with visible per-message completion.” Serial remains useful as “cost of idiomatic one-at-a-time `publish().await`.” Concurrent keeps that same await API but overlaps many callers (fair across clients: harness spawn is identical).

#### Latency stopwatches

| Cell | Barrier |
|------|---------|
| **Ingress serial** (QoS 0/1) | Publisher send → subscriber flume handoff `recv` (same app-queue path as ingress load) |
| **Outbound serial latency QoS 1** (Mosquitto) | `publish().await` until PUBACK |
| **Outbound serial latency** (mock, QoS 0/1) | Start → `publish` → mock `WAIT 1` / `RECEIVED` (broker decoded the frame) |

Do not treat Mosquitto outbound QoS 0 latency as missing coverage — it is intentionally skipped. Mock outbound QoS 0 latency is wire-complete, not API-queue latency.

#### Other

- **Outbound serial QoS 1** is largely Mosquitto RTT-bound (~4 k msg/s ≈ 250 µs round-trip). msg/s parity is expected; CPU gaps while blocked on PUBACK are parking/overhead, not wire encode.
- **Outbound serial / pipelined / concurrent QoS 0** on mock is wire-complete (`WAIT N`); absolute rates still depend on client enqueue/write efficiency and (for serial) per-call await semantics above.
- Keep Mosquitto **outbound QoS 1** load (serial + pipelined + concurrent): that remains the cutover throughput / RTT story. Only QoS 0 outbound is dropped there.

<!-- benchmarks:generated:start -->

## Highlights (Mosquitto, 256 B)

Cutover-relevant where both support the cell. **Bold** = best msg/s or lowest CPU (or lowest p50/p99) among ok clients in that row.

| Scenario | glide | rumqttc | mqtt5 |
|----------|------:|--------:|------:|
| Ingress load QoS 1 `after_enqueue` | **56 914** · **4 695** | 52 654 · 7 864 | n/a (`no_manual_ack`) |
| Ingress load QoS 1 `before` | n/a (`no_auto_ack`) | **55 088** · **4 436** | 50 783 · 12 556 |
| Outbound pipelined QoS 1 | **101 179** · **5 541** | 86 440 · 9 407 | 61 848 · 30 199 |
| Outbound concurrent QoS 1 | **96 157** · **8 547** | 84 998 · 12 852 | 73 815 · 25 782 |
| Outbound serial QoS 1 | **3 932** · 47 417 | 3 736 · 51 299 | 3 826 · **37 221** |
| Ingress serial QoS 1 `after_enqueue` (p50/p99 µs) | **312** / **488** | 314 / 846 | n/a (`no_manual_ack`) |
| Outbound serial latency QoS 1 (p50/p99 µs) | 250 / **400** | 254 / 482 | **250** / 437 |

---

## Mosquitto baseline

Broker: eclipse-mosquitto `2.1.2-alpine` via testcontainers (anonymous; `set_tcp_nodelay`; unlimited inflight/queue; no persistence / `$SYS` / logs).

**Bold** = best msg/s / lowest CPU ns/msg (load), or lowest p50 / p99 (latency), among ok clients in the row.

Outbound QoS 0 (serial / pipelined / concurrent load + serial latency) is omitted here — no wire counter without a measuring subscriber; see **Mock ceiling**.

### Ingress load

Units: msg/s · CPU ns/msg.

#### QoS 0

| Payload | glide | rumqttc | mqtt5 |
|--------:|------:|--------:|------:|
| 64 B | **137 368** · **2 259** | 132 649 · 2 361 | 135 295 · 4 623 |
| 256 B | **134 689** · 1 816 | 132 232 · **1 549** | 129 394 · 4 943 |
| 1 024 B | **55 283** · 3 047 | 53 054 · **2 560** | 49 824 · 6 255 |
| 4 096 B | **15 682** · 10 201 | 14 901 · **9 162** | 14 125 · 12 530 |

#### QoS 1 · `after_enqueue`

| Payload | glide | rumqttc | mqtt5 |
|--------:|------:|--------:|------:|
| 64 B | **61 085** · **4 575** | 58 440 · 7 699 | n/a (`no_manual_ack`) |
| 256 B | **56 914** · **4 695** | 52 654 · 7 864 | n/a (`no_manual_ack`) |
| 1 024 B | **40 354** · **5 002** | 37 471 · 7 757 | n/a (`no_manual_ack`) |
| 4 096 B | **14 657** · **11 894** | 13 373 · 13 546 | n/a (`no_manual_ack`) |

#### QoS 1 · `before`

| Payload | glide | rumqttc | mqtt5 |
|--------:|------:|--------:|------:|
| 64 B | n/a (`no_auto_ack`) | **59 339** · **4 470** | 55 782 · 11 748 |
| 256 B | n/a (`no_auto_ack`) | **55 088** · **4 436** | 50 783 · 12 556 |
| 1 024 B | n/a (`no_auto_ack`) | **37 813** · **4 368** | 35 313 · 12 890 |
| 4 096 B | n/a (`no_auto_ack`) | **13 502** · **10 286** | 12 737 · 17 736 |

### Outbound serial

#### QoS 1

| Payload | glide | rumqttc | mqtt5 |
|--------:|------:|--------:|------:|
| 64 B | **3 943** · 47 199 | 3 797 · 50 665 | 3 880 · **36 889** |
| 256 B | **3 932** · 47 417 | 3 736 · 51 299 | 3 826 · **37 221** |
| 1 024 B | **3 911** · 47 681 | 3 749 · 51 198 | 3 571 · **40 372** |
| 4 096 B | **3 352** · 46 611 | 3 246 · 50 035 | 3 056 · **42 775** |

### Outbound pipelined (window 64)

#### QoS 1

| Payload | glide | rumqttc | mqtt5 |
|--------:|------:|--------:|------:|
| 64 B | **108 009** · **6 326** | 91 867 · 9 750 | 66 089 · 29 124 |
| 256 B | **101 179** · **5 541** | 86 440 · 9 407 | 61 848 · 30 199 |
| 1 024 B | **65 323** · **4 484** | 58 101 · 8 032 | 45 918 · 30 326 |
| 4 096 B | **21 724** · **7 768** | 20 021 · 10 074 | 18 622 · 31 466 |

### Outbound concurrent (64 callers)

#### QoS 1

| Payload | glide | rumqttc | mqtt5 |
|--------:|------:|--------:|------:|
| 64 B | **104 279** · **8 993** | 90 297 · 13 147 | 76 124 · 25 253 |
| 256 B | **96 157** · **8 547** | 84 998 · 12 852 | 73 815 · 25 782 |
| 1 024 B | 59 534 · **8 299** | **60 470** · 11 972 | 56 651 · 25 730 |
| 4 096 B | **22 446** · **12 625** | 22 061 · 15 469 | 22 109 · 27 567 |

### Ingress serial latency

Units: p50 / p99 µs.

#### QoS 0

| Payload | glide | rumqttc | mqtt5 |
|--------:|------:|--------:|------:|
| 64 B | **245** / **377** | 264 / 458 | 257 / 496 |
| 256 B | 259 / **391** | **255** / 509 | 284 / 549 |
| 1 024 B | **252** / **401** | 255 / 433 | 264 / 569 |
| 4 096 B | **300** / **482** | 318 / 748 | 303 / 533 |

#### QoS 1 · `after_enqueue`

| Payload | glide | rumqttc | mqtt5 |
|--------:|------:|--------:|------:|
| 64 B | **307** / **505** | 317 / 688 | n/a (`no_manual_ack`) |
| 256 B | **312** / **488** | 314 / 846 | n/a (`no_manual_ack`) |
| 1 024 B | 317 / **520** | **315** / 897 | n/a (`no_manual_ack`) |
| 4 096 B | **342** / **811** | 346 / 1 252 | n/a (`no_manual_ack`) |

#### QoS 1 · `before`

| Payload | glide | rumqttc | mqtt5 |
|--------:|------:|--------:|------:|
| 64 B | n/a (`no_auto_ack`) | **307** / **736** | 310 / 974 |
| 256 B | n/a (`no_auto_ack`) | **309** / **913** | 318 / 1 098 |
| 1 024 B | n/a (`no_auto_ack`) | **313** / **679** | 324 / 723 |
| 4 096 B | n/a (`no_auto_ack`) | 344 / 1 408 | **340** / **1 051** |

### Outbound serial latency

#### QoS 1

| Payload | glide | rumqttc | mqtt5 |
|--------:|------:|--------:|------:|
| 64 B | **248** / **392** | 250 / 433 | 261 / 485 |
| 256 B | 250 / **400** | 254 / 482 | **250** / 437 |
| 1 024 B | 251 / **422** | 258 / 502 | **249** / 436 |
| 4 096 B | 294 / 544 | 295 / 948 | **289** / **499** |


---

## Mock ceiling

Broker: out-of-process mock MQTT 5 (`MQTT_COMPARE_BROKER=mock`) — multi-thread Tokio, Arc fanout, `TCP_NODELAY`, inbound PUBLISH counter, `receive_max` 1024.
Use for client CPU / enqueue ceiling attribution, not as a broker product comparison.

### Ingress load

#### QoS 0

| Payload | glide | rumqttc | mqtt5 |
|--------:|------:|--------:|------:|
| 64 B | 149 330 · 5 591 | **151 747** · **5 433** | 23 810 · 12 406 |
| 256 B | **150 770** · 5 607 | 112 692 · **5 570** | 27 870 · 10 949 |
| 1 024 B | **149 772** · **5 731** | 126 062 · 5 782 | 26 810 · 12 944 |
| 4 096 B | **124 807** · 7 870 | 73 367 · **6 308** | 23 978 · 14 751 |

#### QoS 1 · `after_enqueue`

| Payload | glide | rumqttc | mqtt5 |
|--------:|------:|--------:|------:|
| 64 B | **103 739** · **6 336** | 57 200 · 6 843 | n/a (`no_manual_ack`) |
| 256 B | **100 148** · **6 542** | 47 325 · 6 912 | n/a (`no_manual_ack`) |
| 1 024 B | **98 268** · **6 692** | 38 704 · 7 134 | n/a (`no_manual_ack`) |
| 4 096 B | **65 845** · **7 617** | 30 233 · 8 847 | n/a (`no_manual_ack`) |

#### QoS 1 · `before`

| Payload | glide | rumqttc | mqtt5 |
|--------:|------:|--------:|------:|
| 64 B | n/a (`no_auto_ack`) | **49 687** · **5 191** | 23 265 · 10 053 |
| 256 B | n/a (`no_auto_ack`) | **42 722** · **5 499** | 23 094 · 11 094 |
| 1 024 B | n/a (`no_auto_ack`) | **35 619** · **5 529** | 21 373 · 11 877 |
| 4 096 B | n/a (`no_auto_ack`) | **29 747** · **6 582** | 18 817 · 13 983 |

### Outbound serial

#### QoS 0

| Payload | glide | rumqttc | mqtt5 |
|--------:|------:|--------:|------:|
| 64 B | **65 085** · 20 468 | 61 236 · 5 957 | 32 179 · **4 968** |
| 256 B | **65 115** · 20 370 | 60 005 · 4 528 | 32 338 · **3 798** |
| 1 024 B | **63 988** · 20 672 | 56 600 · 4 814 | 30 666 · **3 817** |
| 4 096 B | **62 581** · 21 018 | 50 518 · 5 602 | 28 326 · **4 520** |

#### QoS 1

| Payload | glide | rumqttc | mqtt5 |
|--------:|------:|--------:|------:|
| 64 B | **21 959** · 25 678 | 18 507 · 26 885 | 18 579 · **19 465** |
| 256 B | **21 322** · 26 262 | 18 456 · 27 012 | 19 680 · **19 127** |
| 1 024 B | **22 268** · 24 898 | 18 425 · 27 238 | 19 331 · **19 825** |
| 4 096 B | **21 855** · 24 977 | 18 046 · 27 916 | 18 383 · **21 003** |

### Outbound pipelined (window 64)

#### QoS 0

| Payload | glide | rumqttc | mqtt5 |
|--------:|------:|--------:|------:|
| 64 B | **1 571 071** · **644** | 56 354 · 16 113 | 31 070 · 15 429 |
| 256 B | **1 418 389** · **886** | 57 143 · 14 527 | 31 578 · 11 914 |
| 1 024 B | **863 632** · **1 773** | 55 310 · 14 864 | 30 108 · 12 452 |
| 4 096 B | **252 417** · **6 173** | 49 806 · 15 739 | 27 418 · 15 993 |

#### QoS 1

| Payload | glide | rumqttc | mqtt5 |
|--------:|------:|--------:|------:|
| 64 B | **195 574** · **8 665** | 41 545 · 22 567 | 23 611 · 29 559 |
| 256 B | **182 628** · **10 010** | 40 911 · 22 910 | 24 281 · 27 885 |
| 1 024 B | **172 530** · **9 755** | 40 603 · 23 146 | 24 398 · 27 734 |
| 4 096 B | **130 554** · **12 499** | 36 530 · 24 224 | 23 485 · 29 918 |

### Outbound concurrent (64 callers)

#### QoS 0

| Payload | glide | rumqttc | mqtt5 |
|--------:|------:|--------:|------:|
| 64 B | **1 083 160** · **4 336** | 138 265 · 14 894 | 146 732 · 8 951 |
| 256 B | **988 492** · **5 054** | 132 060 · 15 517 | 141 628 · 9 222 |
| 1 024 B | **647 994** · **5 278** | 124 614 · 16 285 | 128 560 · 9 904 |
| 4 096 B | **237 365** · **9 135** | 115 981 · 17 432 | 101 065 · 12 042 |

#### QoS 1

| Payload | glide | rumqttc | mqtt5 |
|--------:|------:|--------:|------:|
| 64 B | **215 961** · **8 259** | 151 338 · 10 047 | 113 411 · 27 053 |
| 256 B | **199 011** · **9 577** | 146 277 · 10 616 | 111 929 · 27 522 |
| 1 024 B | **189 403** · **10 003** | 134 384 · 11 709 | 108 282 · 27 660 |
| 4 096 B | **142 644** · **12 645** | 123 645 · 13 739 | 103 060 · 28 593 |

### Ingress serial latency

Units: p50 / p99 µs.

#### QoS 0

| Payload | glide | rumqttc | mqtt5 |
|--------:|------:|--------:|------:|
| 64 B | **45** / **86** | 57 / 213 | 72 / 230 |
| 256 B | **45** / **83** | 59 / 224 | 73 / 275 |
| 1 024 B | **45** / **84** | 59 / 232 | 73 / 292 |
| 4 096 B | **46** / **88** | 63 / 241 | 75 / 278 |

#### QoS 1 · `after_enqueue`

| Payload | glide | rumqttc | mqtt5 |
|--------:|------:|--------:|------:|
| 64 B | **43** / **84** | 66 / 257 | n/a (`no_manual_ack`) |
| 256 B | **43** / **84** | 69 / 273 | n/a (`no_manual_ack`) |
| 1 024 B | **44** / **85** | 75 / 301 | n/a (`no_manual_ack`) |
| 4 096 B | **44** / **89** | 79 / 318 | n/a (`no_manual_ack`) |

#### QoS 1 · `before`

| Payload | glide | rumqttc | mqtt5 |
|--------:|------:|--------:|------:|
| 64 B | n/a (`no_auto_ack`) | **65** / **264** | 75 / 286 |
| 256 B | n/a (`no_auto_ack`) | **72** / 296 | 76 / **162** |
| 1 024 B | n/a (`no_auto_ack`) | **74** / 303 | 76 / **293** |
| 4 096 B | n/a (`no_auto_ack`) | 79 / 330 | **78** / **294** |

### Outbound serial latency

#### QoS 0

| Payload | glide | rumqttc | mqtt5 |
|--------:|------:|--------:|------:|
| 64 B | **63** / **104** | 77 / 330 | 71 / 126 |
| 256 B | **63** / **104** | 78 / 328 | 72 / 109 |
| 1 024 B | **64** / **106** | 78 / 328 | 72 / 113 |
| 4 096 B | **65** / **104** | 78 / 326 | 74 / 143 |

#### QoS 1

| Payload | glide | rumqttc | mqtt5 |
|--------:|------:|--------:|------:|
| 64 B | **72** / **139** | 89 / 336 | 80 / 154 |
| 256 B | **72** / **140** | 89 / 337 | 80 / 154 |
| 1 024 B | **72** / **141** | 89 / 336 | 81 / 147 |
| 4 096 B | **74** / **146** | 89 / 320 | 83 / 153 |


---

## Refresh

```shell
# Mosquitto baseline (Podman or Docker) — default SCENARIOS includes outbound_concurrent
MQTT_COMPARE_QOS=0,1 \
MQTT_COMPARE_PAYLOADS=64,256,1024,4096 \
MQTT_COMPARE_RUNS=100 \
MQTT_COMPARE_OUT=target/mqtt_compare/full-mosquitto-100 \
  cargo bench --bench mqtt_compare

# Mock ceiling
MQTT_COMPARE_BROKER=mock \
MQTT_COMPARE_QOS=0,1 \
MQTT_COMPARE_PAYLOADS=64,256,1024,4096 \
MQTT_COMPARE_RUNS=100 \
MQTT_COMPARE_OUT=target/mqtt_compare/full-mock-100 \
  cargo bench --bench mqtt_compare

# Rebuild tables (bold winners) from JSONL
python3 benches/render_benchmarks.py
```

This baseline’s concurrent rows were measured into `outbound-concurrent-mosquitto-100/` / `outbound-concurrent-mock-100/` and merged by hand; a full refresh into `full-*` (with default scenarios) is enough next time. Then sync the README highlight table if the Mosquitto 256 B story changes.

<!-- benchmarks:generated:end -->
