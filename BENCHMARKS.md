# Benchmarks

mqtt-glide against rumqttc-v5-next and LabOverWire mqtt5: same broker, payload, harness and pinning, every client with its default settings apart from the [shared knobs](#fairness-rules), clients taking turns run by run. Numbers are absolute and only hold under the conditions in [Setup](#setup).

Detail pages with every scenario and level: [stock Mosquitto](benches/results/mosquitto-stock.md), [Mosquitto without flow control](benches/results/mosquitto.md), [mock broker](benches/results/mock.md), [which inbound sink](benches/results/inbound-sinks.md) (mqtt-glide only). Running the harness and its env knobs: [`benches/README.md`](benches/README.md).

## Verdict

- **Publishing (Mosquitto, QoS 1):** glide leads at every caller count with the lowest CPU and latency: 149k / 246k / 246k msg/s at 8 / 64 / 512 callers, rumqttc 138k to 163k, mqtt5 83k to 114k.
- **Publishing at 50 000 msg/s offered:** all three keep up; glide 18.6 µs CPU per publish, p50 / p99 0.64 / 1.6 ms; rumqttc 22.0 µs, 0.83 / 1.9 ms; mqtt5 27.2 µs, 0.83 / 1.9 ms.
- **Receiving from Mosquitto:** the broker is the limit, all three get about 102k msg/s at QoS 1 and 220k at QoS 0. glide at the lowest CPU: 3.6 µs per QoS 1 message (PUBACK after the app queue), rumqttc 4.7 µs with the same policy and 4.1 µs with PUBACK on decode, mqtt5 4.3 µs.
- **Mosquitto without flow control** (Receive Maximum 65 535, a separate run): glide about the same at 232k / 249k msg/s with 64 / 512 callers; mqtt5 80k, rumqttc 7.9k.
- **Client ceiling (mock):** publishing QoS 0 pipelined 3.23M msg/s against 195k and 146k; QoS 1 882k against 174k and 135k. Receiving QoS 0 3.81M against 2.60M and 1.40M; QoS 1 2.51M, 11× rumqttc with the same ack policy.
- **QoS 0 publish latency:** rumqttc's `publish().await` returns sooner (p50 51 vs 110 µs at 50 000 msg/s on the mock); the return points differ, see [Fairness rules](#fairness-rules).
- **Broker capping deliveries at 32:** glide receives 836k msg/s, rumqttc 245k with the same policy and 781k with PUBACK on decode, mqtt5 244k.
- **Payload size:** glide leads at every size, closest receiving 16 KiB at QoS 0 (270k vs 252k); publishing 16 KiB at QoS 1 it reaches 399k msg/s against 133k and 121k.

## Summary

<!-- bench:summary:start -->

**Real broker**: stock Mosquitto (its default flow control), QoS 1 unless noted, 256 B.

| Scenario | glide | rumqttc | mqtt5 |
|---|---:|---:|---:|
| Outbound concurrent, 8 callers | **148 635** ±1% · **7 429** · **53.0** / **94.5** / **110** µs | 137 952 ±1% · 7 727 · 58.3 / 102 / 118 µs | 83 025 ±1% · 13 476 · 97.3 / 143 / 162 µs |
| Outbound concurrent, 64 callers | **245 808** ±0% · **5 970** · **259** / **325** / **360** µs | 162 071 ±1% · 9 578 · 393 / 478 / 514 µs | 114 131 ±1% · 14 295 · 558 / 784 / 851 µs |
| Outbound concurrent, 512 callers | **245 557** ±1% · **6 006** · **2 085** / **2 222** / **2 262** µs | 162 979 ±1% · 9 483 · 3 140 / 3 365 / 3 432 µs | 114 442 ±1% · 14 324 · 4 473 / 5 066 / 5 211 µs |
| Outbound pipelined | **254 104** ±1% · **3 056** | 218 380 ±11% · 3 883 | 130 316 ±1% · 16 060 |
| Outbound rate, 50 000 msg/s offered | 49 041 ±0% · **18 619** · **642** / **1 624** / **2 151** µs | 49 259 ±0% · 21 993 · 832 / 1 870 / 2 444 µs | **49 357** ±0% · 27 229 · 826 / 1 879 / 2 243 µs |
| Ingress load, QoS 0 | 216 919 ±1% · **1 602** | **222 875** ±0% · 1 977 | 221 468 ±2% · 1 849 |
| Ingress load, QoS 1, PUBACK after the app queue | **102 461** ±0% · **3 550** | 102 166 ±1% · 4 698 | n/a (`no_manual_ack`) |
| Ingress load, QoS 1, PUBACK on decode | n/a (`no_auto_ack`) | 103 706 ±1% · **4 080** | **104 431** ±0% · 4 254 |
| Ingress rate 50 000 msg/s, QoS 1, PUBACK after the app queue | **49 848** ±0% · **4 495** · **237** / 317 / **353** µs | 49 847 ±0% · 5 808 · 240 / **316** / 369 µs | n/a (`no_manual_ack`) |
| Ingress rate 50 000 msg/s, QoS 1, PUBACK on decode | n/a (`no_auto_ack`) | 49 849 ±0% · **5 209** · **237** / **313** / **358** µs | **49 860** ±0% · 5 471 · 243 / 318 / 462 µs |

**Client ceiling**: mock broker, 256 B.

| Scenario | glide | rumqttc | mqtt5 |
|---|---:|---:|---:|
| Outbound concurrent 512, QoS 0 | **1 600 212** ±1% · **1 764** · **316** / **412** / **447** µs | 188 584 ±2% · 7 857 · 2 708 / 2 968 / 3 486 µs | 195 514 ±2% · 3 671 · 2 619 / 2 733 / 2 783 µs |
| Outbound concurrent 512, QoS 1 | **1 438 610** ±1% · **1 902** · **353** / **469** / **509** µs | 123 272 ±1% · 9 543 · 4 152 / 4 384 / 4 504 µs | 81 079 ±2% · 13 218 · 6 285 / 7 017 / 7 205 µs |
| Outbound pipelined, QoS 0 | **3 225 995** ±4% · **274** | 195 009 ±7% · 7 685 | 146 221 ±13% · 7 860 |
| Outbound pipelined, QoS 1 | **881 863** ±4% · **474** | 173 914 ±1% · 4 996 | 134 903 ±2% · 14 454 |
| Ingress load, QoS 0 | **3 808 696** ±4% · **225** | 2 604 834 ±2% · 392 | 1 404 834 ±3% · 1 505 |
| Ingress load, QoS 1, PUBACK after the app queue | **2 512 178** ±5% · **322** | 223 911 ±1% · 3 669 | n/a (`no_manual_ack`) |
| Ingress load, QoS 1, PUBACK on decode | n/a (`no_auto_ack`) | **1 706 951** ±4% · **572** | 243 527 ±14% · 3 547 |

Units: msg/s ±spread · CPU ns/msg · p50 / p99 / p99.9 µs where the scenario has latency.
**Bold** = best value in the row (highest msg/s, lowest CPU ns/msg and latency) among clients that ran; `±` = half the min–max range of the runs relative to the median.
Ingress QoS 1 has a row per ack policy: `after_enqueue` sends PUBACK once the message is in the app queue, `before` on decode. Each client runs the policies it has (glide only `after_enqueue`, mqtt5 only `before`); `before` saves the client work and can lose a message the app never saw.

<!-- bench:summary:end -->

## Mosquitto without flow control

A cross-check with Mosquitto's flow control switched off (`max_inflight_messages 0`, `max_queued_messages 0`): CONNACK Receive Maximum 65 535, no queue limit. QoS 1, 256 B.

<!-- bench:tuned:start -->

| Scenario | glide | rumqttc | mqtt5 |
|---|---:|---:|---:|
| Outbound concurrent, 64 callers | **232 187** ±0% · **6 374** · **274** / **367** / **458** µs | 7 887 ±2% · 133 802 · 8 103 / 8 484 / 8 842 µs | 79 870 ±1% · 14 230 · 804 / 966 / 1 019 µs |
| Outbound concurrent, 512 callers | **249 235** ±5% · **6 067** · **2 053** / **2 221** / **2 310** µs | 7 842 ±1% · 134 664 · 65 075 / 67 234 / 73 554 µs | 78 660 ±1% · 14 441 · 6 514 / 7 132 / 7 339 µs |
| Outbound pipelined | **256 599** ±1% · **3 103** | 8 963 ±2% · 108 819 | 124 254 ±1% · 15 607 |
| Ingress load, QoS 1, PUBACK after the app queue | **106 543** ±1% · **3 538** | 16 215 ±1% · 57 639 | n/a (`no_manual_ack`) |
| Ingress load, QoS 1, PUBACK on decode | n/a (`no_auto_ack`) | **136 952** ±4% · 7 100 | 105 742 ±0% · **4 224** |

Units: msg/s ±spread · CPU ns/msg · p50 / p99 / p99.9 µs per publish.

<!-- bench:tuned:end -->

## Broker with its own in-flight cap

Some brokers deliver fewer unacknowledged QoS 1 messages than the client's Receive Maximum allows (EMQX `max_inflight` defaults to 32). The mock with deliveries capped at 32 (`MQTT_COMPARE_MOCK_DELIVERY_INFLIGHT=32`), QoS 1, 256 B:

<!-- bench:capped:start -->

| Scenario | glide | rumqttc | mqtt5 |
|---|---:|---:|---:|
| Ingress load, QoS 1, PUBACK after the app queue | **836 134** ±1% · **341** | 245 317 ±1% · 2 765 | n/a (`no_manual_ack`) |
| Ingress load, QoS 1, PUBACK on decode | n/a (`no_auto_ack`) | **780 909** ±3% · **627** | 244 038 ±0% · 3 525 |
| Ingress rate 10 000 msg/s, QoS 1, PUBACK after the app queue | 9 933 ±0% · **2 647** · **174** / **212** / **268** µs | **9 942** ±0% · 12 046 · 376 / 458 / 766 µs | n/a (`no_manual_ack`) |
| Ingress rate 10 000 msg/s, QoS 1, PUBACK on decode | n/a (`no_auto_ack`) | **9 946** ±0% · **4 153** · **195** / **241** / **334** µs | 9 933 ±0% · 10 472 · 296 / 372 / 826 µs |
| Ingress rate 50 000 msg/s, QoS 1, PUBACK after the app queue | 49 687 ±0% · **1 348** · **308** / **423** / **625** µs | **49 772** ±0% · 8 708 · 837 / 1 369 / 1 458 µs | n/a (`no_manual_ack`) |
| Ingress rate 50 000 msg/s, QoS 1, PUBACK on decode | n/a (`no_auto_ack`) | 49 680 ±0% · **2 427** · **359** / **461** / **642** µs | **49 682** ±0% · 9 283 · 663 / 1 172 / 1 457 µs |
| Ingress rate 200 000 msg/s, QoS 1, PUBACK after the app queue | 199 378 ±0% · **908** · **728** / 1 458 / 1 607 µs | **199 414** ±0% · 2 814 · 779 / **1 443** / **1 564** µs | n/a (`no_manual_ack`) |
| Ingress rate 200 000 msg/s, QoS 1, PUBACK on decode | n/a (`no_auto_ack`) | **199 358** ±0% · **1 613** · 759 / 1 549 / 1 873 µs | 199 342 ±0% · 3 292 · **705** / **1 398** / **1 794** µs |

Units: msg/s ±spread · CPU ns/msg · p50 / p99 / p99.9 µs. Ingress QoS 1 has a row per ack policy: `after_enqueue` sends PUBACK once the message is in the app queue, `before` on decode. Each client runs the policies it has (glide only `after_enqueue`, mqtt5 only `before`); `before` saves the client work and can lose a message the app never saw.

<!-- bench:capped:end -->

## Payload size

Mock broker, the [matrix](#matrix) otherwise unchanged; 256 B is the peer run.

<!-- bench:payload:start -->

**Ingress load, QoS 0**:

| Payload | glide | rumqttc | mqtt5 |
|---|---:|---:|---:|
| 16 B | **4 774 312** ±11% · **186** · 76 MB/s | 3 084 499 ±5% · 340 · 49 MB/s | 2 179 762 ±2% · 930 · 35 MB/s |
| 256 B | **3 808 696** ±4% · **225** · 975 MB/s | 2 604 834 ±2% · 392 · 667 MB/s | 1 404 834 ±3% · 1 505 · 360 MB/s |
| 1 024 B | **2 084 207** ±11% · **282** · 2 134 MB/s | 1 825 508 ±4% · 554 · 1 869 MB/s | 1 064 366 ±7% · 1 400 · 1 090 MB/s |
| 16 384 B | **270 426** ±5% · **2 257** · 4 431 MB/s | 251 741 ±11% · 2 382 · 4 125 MB/s | 236 155 ±6% · 3 095 · 3 869 MB/s |

**Ingress load, QoS 1, PUBACK after the app queue (`after_enqueue`)**:

| Payload | glide | rumqttc | mqtt5 |
|---|---:|---:|---:|
| 16 B | **2 542 495** ±14% · **267** · 41 MB/s | 229 560 ±1% · 3 545 · 4 MB/s | n/a (`no_manual_ack`) |
| 256 B | **2 512 178** ±5% · **322** · 643 MB/s | 223 911 ±1% · 3 669 · 57 MB/s | n/a (`no_manual_ack`) |
| 1 024 B | **1 468 459** ±2% · **417** · 1 504 MB/s | 257 758 ±1% · 2 653 · 264 MB/s | n/a (`no_manual_ack`) |
| 16 384 B | **261 833** ±2% · **2 473** · 4 290 MB/s | 168 273 ±1% · 4 843 · 2 757 MB/s | n/a (`no_manual_ack`) |

**Outbound pipelined, QoS 0**:

| Payload | glide | rumqttc | mqtt5 |
|---|---:|---:|---:|
| 16 B | **4 281 149** ±4% · **217** · 68 MB/s | 200 469 ±1% · 7 301 · 3 MB/s | 154 178 ±1% · 7 372 · 2 MB/s |
| 256 B | **3 225 995** ±4% · **274** · 826 MB/s | 195 009 ±7% · 7 685 · 50 MB/s | 146 221 ±13% · 7 860 · 37 MB/s |
| 1 024 B | **2 106 209** ±0% · **406** · 2 157 MB/s | 188 949 ±1% · 7 890 · 193 MB/s | 140 156 ±0% · 8 129 · 144 MB/s |
| 16 384 B | **445 457** ±1% · **2 163** · 7 298 MB/s | 138 850 ±1% · 10 648 · 2 275 MB/s | 99 000 ±1% · 12 313 · 1 622 MB/s |

**Outbound pipelined, QoS 1**:

| Payload | glide | rumqttc | mqtt5 |
|---|---:|---:|---:|
| 16 B | **993 472** ±5% · **426** · 16 MB/s | 179 748 ±0% · 4 746 · 3 MB/s | 149 967 ±2% · 13 868 · 2 MB/s |
| 256 B | **881 863** ±4% · **474** · 226 MB/s | 173 914 ±1% · 4 996 · 45 MB/s | 134 903 ±2% · 14 454 · 35 MB/s |
| 1 024 B | **925 248** ±0% · **575** · 947 MB/s | 171 911 ±0% · 5 071 · 176 MB/s | 146 434 ±1% · 15 371 · 150 MB/s |
| 16 384 B | **399 202** ±1% · **2 361** · 6 541 MB/s | 132 882 ±1% · 6 991 · 2 177 MB/s | 121 000 ±1% · 19 115 · 1 982 MB/s |

Units: msg/s ±spread · CPU ns/msg · payload MB/s. 256 B comes from the peer run.

<!-- bench:payload:end -->

## How to read this page

A cell is one client in one scenario (and level), at one QoS, one ack policy, one payload, against one broker. Compare cells along the client columns of one table; tables were measured at different times.

### Scenarios

| Scenario | Models | What it measures |
|----------|--------|------------------|
| `outbound_concurrent` | A service: one shared client, many request tasks | 8 / 64 / 512 long-lived caller tasks, each awaiting every `publish`. msg/s, CPU ns/msg, per-publish p50 / p99 / p99.9 |
| `outbound_pipelined` | Bulk export | One task keeps a window of 64 in flight through each client's idiomatic pipeline. msg/s, CPU ns/msg |
| `outbound_rate` | A service at a given load | Open loop at 10 000 and 50 000 msg/s offered (1 ms ticks, 2 s), each publish in its own task. The generator never waits on the client, so falling behind shows as latency and a lower achieved rate. CPU ns/msg and latency at a fixed load |
| `ingress_load` | A subscriber under burst | A publisher process bursts at the client: decode, bounded app queue, consumer task. msg/s, CPU ns/msg |
| `ingress_rate` | A subscriber at a given load | The publisher sends at 10 000 / 50 000 / 200 000 msg/s offered; latency from the publisher's write to the consumer taking the message off the app queue. msg/s, CPU ns/msg, p50 / p99 / p99.9 |

### Units and markers

- Cells: `msg/s ±spread · CPU ns/msg`, plus `· p50 / p99 / p99.9 µs` where the scenario records latency. Every cell is the **median of 5 runs**; `±` is half the min–max range of those runs relative to the median.
- **CPU ns/msg** is `getrusage` of the whole bench process: the client, its runtime threads and the harness tasks (callers, ticker, consumer). Harness work is the same for every client but not free. Broker and publisher are separate processes and not counted.
- **Bold** marks, per metric and row, the best client (highest msg/s, lowest CPU and latency); ties are all bold. Bold is not a significance test: a lead inside the spread is not a lead.
- `n/a (no_manual_ack)`: mqtt5 cannot acknowledge after the app queue. `n/a (no_auto_ack)`: glide does not acknowledge before it. `n/a (qos0_no_wire_counter)`: outbound QoS 0 on Mosquitto, see [Fairness rules](#fairness-rules).

### Ack policies (ingress QoS 1)

Both policies go through the same app queue; they only decide when PUBACK is sent. Ingress QoS 1 has one row per policy, and each client runs the policies it has.

| Policy | PUBACK is sent | Clients |
|--------|----------------|---------|
| `after_enqueue` | after the app queue accepted the message (no loss on a crash between decode and queue) | mqtt-glide (only mode), rumqttc via `manual_acks` |
| `before` | on decode, before the app sees the message | rumqttc auto-ack, mqtt5 (only mode) |

## Setup

### Host

| | |
|--|--|
| Machine | Apple M5 Max, 36 GB, macOS 27.0.1 |
| Linux | Podman 6.1.4 machine (applehv): Fedora CoreOS 44, kernel 7.1.4 aarch64, 12 vCPU, 16 GB |
| Bench container | `rust:1-trixie` (Debian 13), `--cap-add SYS_NICE` for `taskset` |
| Broker | eclipse-mosquitto `2.1.2-alpine` containers in the same Podman pod (loopback) |
| Build | Cargo's `bench` profile with its defaults (no LTO, 16 codegen units), one binary for all three clients |

Linux, because that is where MQTT clients run in production and the only place all three processes can be pinned. A VM makes wake-ups and syscalls dearer than bare metal, for every client alike; the core cells were [cross-checked on x86](#x86-cross-check).

### CPU pinning

| Process | CPUs |
|---------|------|
| Client under test (whole bench process, Tokio multi-thread runtime with 4 workers) | 0-3 |
| Broker (mock child or Mosquitto) | 4-7 |
| Ingress publisher child | 8-11 |

### Matrix

| Knob | Value |
|------|-------|
| Runs | median of 5 per cell, clients taking turns |
| Payload | 256 B (the [payload table](#payload-size) varies it) |
| Burst | mock 1 000 000 messages, Mosquitto 200 000 |
| Client Receive Maximum | 1024 for every client |
| Mock broker Receive Maximum | 1024 |
| Ingress publisher window | 1024 QoS 1 PUBLISHes in flight, up to 64 per write |
| App queue (ingress handoff) | `tokio::sync::mpsc`, capacity 10 000, drained by a consumer task |
| QoS | 0 and 1 (outbound QoS 0 only on the mock) |

Client versions: each detail page (`meta.json` of the run).

### Brokers

| Broker | Config |
|--------|--------|
| Stock Mosquitto | Mosquitto's default flow control (CONNACK Receive Maximum 20, 1000 queued), `set_tcp_nodelay true`, no persistence, `$SYS` or logs |
| Mosquitto without flow control | The same plus `max_inflight_messages 0` (CONNACK Receive Maximum 65 535) and `max_queued_messages 0` |
| Mock | Out-of-process MQTT 5 broker built for the bench: multi-thread, `TCP_NODELAY`, honours the client's Receive Maximum, one write per batch of PUBACKs or deliveries, counts decoded PUBLISH frames. Faster than every client, so it shows each client's ceiling. Not a broker claim |

## Fairness rules

- **Same knobs, otherwise defaults.** Every client announces Receive Maximum 1024 (the defaults differ: glide 256, the others 65 535) and uses `TCP_NODELAY` (the default of glide and mqtt5, set for rumqttc). rumqttc also gets a 1 MiB packet limit, because its 10 KiB default rejects the 16 KiB payloads. Nothing else is tuned for any client.
- **The harness runs where application code runs:** callers, rate generator and consumer are tasks on the client's Tokio runtime, the same code for every client.
- **Load generator and mock are not the limit.** Both are separate pinned processes; on Mosquitto the broker limits ingress.
- **Ingress is always a handoff:** decode → bounded app queue → consumer task, for every client.
- **Both ack policies are shown;** a client is compared only with clients running the same policy.
- **Outbound ends on the wire.** On the mock a burst ends when the mock has decoded all N PUBLISH frames; on Mosquitto only QoS 1 runs outbound (PUBACK proves delivery).
- **Idiomatic APIs.** Pipelined uses `publish_window` (glide), the notice future (rumqttc), spawned publishes (mqtt5). Unsupported capabilities are `n/a`, never emulated.
- **QoS 0 `publish().await` returns at different points:** glide once its write task has encoded the message, rumqttc when the request is on its event-loop channel, mqtt5 after encode and write on the caller. QoS 0 latency measures that point; throughput is always wire-complete.
- **Clients take turns.** Each run of a cell measures every client once, the first client rotating, so host drift hits all clients alike.

## x86 cross-check

The core cells on native Linux x86: Intel Core Ultra 7 258V, Fedora 44 (kernel 7.2), client on P-cores 0-3, mock on 4-5, publisher on 6-7, 256 B, median of 3, clients taking turns. Same versions as above. glide is ahead in every cell it runs except receiving QoS 0, a tie at about half rumqttc's CPU per message (279 vs 490 ns).

| Cell (msg/s) | glide | rumqttc | mqtt5 |
|---|---:|---:|---:|
| Ingress load, QoS 0 | 2.02M | **2.03M** | 1.24M |
| Ingress load, QoS 1, PUBACK after the app queue | **2.18M** | 214k | n/a |
| Ingress load, QoS 1, PUBACK on decode | n/a | **1.40M** | 243k |
| Outbound pipelined, QoS 0 | **2.08M** | 207k | 159k |
| Outbound pipelined, QoS 1 | **937k** | 167k | 222k |
| Outbound concurrent 64, QoS 0 | **1.74M** | 213k | 236k |
| Outbound concurrent 64, QoS 1 | **688k** | 155k | 149k |

## Reproduce

All published numbers come from one script run inside a Podman pod on the [host above](#host). The pod shares network and PID namespaces between the Mosquitto containers and the bench container, so the bench can pin the broker with `taskset` and connect over loopback.

```shell
# pod with tuned (1883) and stock (1884) Mosquitto
podman pod create --name glidebench --share net,ipc,uts,pid
podman run -d --pod glidebench --name mosq \
  -v ./benches/mosquitto/tuned.conf:/mosquitto/config/mosquitto.conf:ro eclipse-mosquitto:2.1.2-alpine
podman run -d --pod glidebench --name mosqstock \
  -v ./benches/mosquitto/stock.conf:/mosquitto/config/mosquitto.conf:ro eclipse-mosquitto:2.1.2-alpine

# inside a Rust container in the same pod (--cap-add SYS_NICE), repository at /src
taskset -a -p -c 4-7 $(pgrep -x mosquitto)
cargo bench --no-run --features __bench,stats,flume,async-channel --bench mqtt_compare --bench ingress_fanin

# one run, e.g. the peer comparison on the mock broker
TOKIO_WORKER_THREADS=4 MQTT_COMPARE_BROKER=mock MQTT_COMPARE_QOS=0,1 MQTT_COMPARE_LOAD_MSGS=1000000 \
MQTT_COMPARE_BROKER_CPUS=4-7 MQTT_COMPARE_PUBLISHER_CPUS=8-11 \
MQTT_COMPARE_OUT=target/bench-results/peers-mock \
  taskset -c 0-3 cargo bench --features __bench,stats --bench mqtt_compare

python3 benches/render_benchmarks.py   # rewrites the blocks above and benches/results/*.md
```

| Run (`target/bench-results/…`) | Settings on top of the line above |
|---|---|
| `peers-mock` | as shown |
| `peers-mosquitto` | `MQTT_COMPARE_BROKER=external MQTT_COMPARE_BROKER_PORT=1883`, 200 000 messages |
| `peers-mosquitto-stock` | `MQTT_COMPARE_BROKER=external MQTT_COMPARE_BROKER_PORT=1884`, 200 000 messages |
| `capped-mock` | QoS 1, `MQTT_COMPARE_MOCK_DELIVERY_INFLIGHT=32`, `MQTT_COMPARE_SCENARIOS=ingress_load,ingress_rate` |
| `payload` | 500 000 messages, `MQTT_COMPARE_SCENARIOS=ingress_load,outbound_pipelined`, `MQTT_COMPARE_PAYLOADS=16,1024,16384` |
| `fanin` | `cargo bench --features __bench,stats,flume,async-channel --bench ingress_fanin`, QoS 0 and 1, default matrix |

The Mosquitto configs are [`benches/mosquitto/tuned.conf`](benches/mosquitto/tuned.conf) and [`benches/mosquitto/stock.conf`](benches/mosquitto/stock.conf); the stock one only drops the two flow-control lines, so it falls back to Mosquitto's defaults (inflight 20, 1000 queued).
