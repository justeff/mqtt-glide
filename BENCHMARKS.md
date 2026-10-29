# Benchmarks

mqtt-glide against rumqttc-v5-next and LabOverWire mqtt5: same broker, same payload, same harness, same pinning, clients taking turns run by run. Every number is an absolute measurement and only means something together with the conditions under which it was measured ([Setup](#setup)).

1. [Verdict](#verdict): which client is ahead where, and why.
2. [Summary](#summary): the headline cells on a real broker and on a mock broker that is faster than every client.
3. [Stock Mosquitto](#stock-mosquitto), a [broker with its own in-flight cap](#broker-with-its-own-in-flight-cap), [payload size](#payload-size) and [which inbound sink](#which-inbound-sink) to use.
4. Detail pages with every scenario and level: [tuned Mosquitto](benches/results/mosquitto.md), [stock Mosquitto](benches/results/mosquitto-stock.md), [mock broker](benches/results/mock.md).

How to run the harness yourself, and every env knob, is in [`benches/README.md`](benches/README.md).

## Verdict

- **Publishing with many callers: glide.** On tuned Mosquitto glide is ahead from 64 callers up: 235k vs 201k msg/s for rumqttc at 64, 253k vs 194k at 512, with lower CPU per message and about 20% lower p99. Against the mock, which shows how much each client can push, the gap from 64 callers up is about 3 to 15×: QoS 0 pipelined 3.43M vs 244k msg/s, 512 callers at QoS 1 1.47M vs 230k.
- **At a fixed load: glide uses the least CPU.** At 50 000 msg/s offered on Mosquitto, glide costs 12.3 µs CPU per publish against 14.3 µs (rumqttc) and 22.6 µs (mqtt5), with about half their p50 and a third to half lower p99.
- **Receiving: glide uses the least CPU.** With PUBACK after the app queue, glide spends 3.5 µs per QoS 1 message from Mosquitto against rumqttc's 4.4 µs, less than rumqttc's PUBACK on decode (3.9 µs), and 4.6 against 5.7 µs at 50 000 msg/s. Against the mock it receives QoS 1 at 2.5M msg/s, 8× rumqttc with the same ack policy. All clients reach what Mosquitto delivers (about 100k msg/s at QoS 1); at that limit glide is now even with rumqttc rather than ahead, see [Why](#why).
- **Few callers: a tie.** With 8 callers on Mosquitto glide and rumqttc are within 2% in throughput and latency; rumqttc spends 6% less CPU.
- **QoS 0 publish latency: rumqttc.** A QoS 0 `publish().await` returns sooner on rumqttc (p50 41 vs 57 µs at 10 000 msg/s on the mock), because it returns when the request is on its channel; glide waits until its write task has encoded the message ([Fairness rules](#fairness-rules)).
- **Large payloads: glide.** It leads at every size in every direction except receiving 16 KiB at QoS 0, a tie within the spread. Publishing 16 KiB at QoS 1 it reaches 361k msg/s against rumqttc's 181k on the mock; against Mosquitto, which limits both, it spends less CPU per publish. With 8 callers and 16 KiB, rumqttc is still 12% ahead on Mosquitto, for the reason [Why](#why) gives for few callers.
- **mqtt5** is behind in nearly every cell. Against tuned Mosquitto, which announces no Receive Maximum, its concurrent publishing stays near 84k msg/s however many callers there are; against stock Mosquitto it reaches 124k.

## Summary

<!-- bench:summary:start -->

**Real broker**: tuned Mosquitto, QoS 1 unless noted, 256 B.

| Scenario | glide | rumqttc | mqtt5 |
|---|---:|---:|---:|
| Outbound concurrent, 8 callers | 145 013 ±0% · 7 587 · **54.5** / 95.9 / 111 µs | **147 261** ±1% · **7 115** · 54.8 / **94.8** / **109** µs | 83 870 ±1% · 13 816 · 96.8 / 143 / 164 µs |
| Outbound concurrent, 64 callers | **234 701** ±1% · **6 201** · **271** / **358** / **424** µs | 201 105 ±1% · 6 508 · 311 / 556 / 606 µs | 85 024 ±1% · 13 738 · 769 / 925 / 970 µs |
| Outbound concurrent, 512 callers | **252 546** ±5% · **5 918** · **2 038** / **2 217** / **2 284** µs | 193 985 ±0% · 6 609 · 2 643 / 2 794 / 3 389 µs | 83 387 ±1% · 13 847 · 6 154 / 6 874 / 7 037 µs |
| Outbound pipelined | **261 117** ±2% · **3 031** | 234 962 ±12% · 3 838 | 152 120 ±3% · 14 867 |
| Outbound rate, 50 000 msg/s offered | 49 294 ±0% · **12 311** · **494** / **1 274** / **1 460** µs | 49 293 ±0% · 14 315 · 968 / 1 989 / 2 672 µs | **49 470** ±0% · 22 596 · 894 / 2 544 / 2 778 µs |
| Ingress load, QoS 0 | **206 476** ±3% · **1 561** | 205 343 ±1% · 1 864 | 205 692 ±2% · 1 906 |
| Ingress load, QoS 1, PUBACK after the app queue | 102 577 ±1% · **3 545** | **103 092** ±1% · 4 438 | n/a (`no_manual_ack`) |
| Ingress load, QoS 1, PUBACK on decode | n/a (`no_auto_ack`) | 105 437 ±1% · **3 916** | **105 500** ±0% · 4 154 |
| Ingress rate 50 000 msg/s, QoS 1, PUBACK after the app queue | 49 927 ±0% · **4 606** · 624 / 1 366 / 1 503 µs | **49 933** ±0% · 5 674 · **615** / **1 344** / **1 491** µs | n/a (`no_manual_ack`) |
| Ingress rate 50 000 msg/s, QoS 1, PUBACK on decode | n/a (`no_auto_ack`) | **49 935** ±0% · **4 999** · 639 / 1 400 / 1 560 µs | 49 926 ±0% · 5 420 · **636** / **1 386** / **1 485** µs |

**Client ceiling**: mock broker, 256 B.

| Scenario | glide | rumqttc | mqtt5 |
|---|---:|---:|---:|
| Outbound concurrent 512, QoS 0 | **1 580 001** ±1% · **1 796** · **318** / **426** / **462** µs | 224 890 ±1% · 6 960 · 2 272 / 2 492 / 3 389 µs | 196 918 ±2% · 3 701 · 2 598 / 2 712 / 2 757 µs |
| Outbound concurrent 512, QoS 1 | **1 467 276** ±0% · **1 774** · **348** / **478** / **518** µs | 229 630 ±0% · 5 387 · 2 230 / 2 365 / 2 610 µs | 84 124 ±1% · 12 860 · 6 074 / 6 850 / 7 012 µs |
| Outbound pipelined, QoS 0 | **3 430 455** ±2% · **276** | 244 215 ±2% · 6 660 | 148 062 ±1% · 7 732 |
| Outbound pipelined, QoS 1 | **904 876** ±2% · **602** | 267 156 ±0% · 3 166 | 162 232 ±3% · 14 265 |
| Ingress load, QoS 0 | **3 871 577** ±3% · **210** | 3 135 584 ±3% · 332 | 1 560 483 ±7% · 1 426 |
| Ingress load, QoS 1, PUBACK after the app queue | **2 494 761** ±6% · **305** | 294 797 ±7% · 2 780 | n/a (`no_manual_ack`) |
| Ingress load, QoS 1, PUBACK on decode | n/a (`no_auto_ack`) | **2 150 128** ±2% · **448** | 248 302 ±13% · 3 477 |

Units: msg/s ±spread · CPU ns/msg · p50 / p99 / p99.9 µs where the scenario has latency.
**Bold** = best value in the row (highest msg/s, lowest CPU ns/msg and latency) among clients that ran; `±` = half the min–max range of the runs relative to the median.
Ingress QoS 1 has a row per ack policy: `after_enqueue` sends PUBACK once the message is in the app queue, `before` on decode. Each client runs the policies it has (glide only `after_enqueue`, mqtt5 only `before`); `before` saves the client work and can lose a message the app never saw.

<!-- bench:summary:end -->

## Why

Every statement here comes from a `perf` profile or a counter of the bench process on the [host](#host), not from reading the code alone.

- **glide's split read and write tasks** are why it scales: callers hand publishes to the write task, which writes up to 64 at once and completes their PUBACKs in batches, while the read task decodes in parallel. Under load that is what puts glide's outbound at several times the peers' on the mock.
- **Inbound PUBACKs are written by the read task.** For each inbound QoS 1 message the read task used to wake two tasks: the consumer (handoff) and the write task (PUBACK). With two runnable tasks on one worker, Tokio wakes a sleeping second worker thread to take one; in a profile at 50 000 msg/s from Mosquitto, sending the PUBACK request was 977 of 1557 samples spent waking threads. The read task now writes the PUBACKs of a read burst itself, through the shared writer when the write task is not writing, which halved the CPU per message. rumqttc's event loop reads and acknowledges in one task, too.
- **PUBACKs are never held back.** Holding them to batch more per write, until more messages arrive or for up to 1 ms, was tried and dropped: a [broker with its own in-flight cap](#broker-with-its-own-in-flight-cap) stops and waits for them, which cut it to 11k msg/s. The price of writing at the end of each burst: against a broker that delivers one message at a time, fewer PUBACKs share a TCP segment than when a late-woken write task picked several up (3.36 instead of 3.25 segments per message in the pod). At Mosquitto's ceiling that costs glide 4–6% of the broker's throughput (104k instead of 108k msg/s tuned, 101k instead of 107k stock, measured interleaved), although Mosquitto spends slightly less CPU per message. Yielding once before the write recovered it but brought back two thirds of the CPU cost, because the yield is the same cross-thread wake-up.
- **Few callers cost glide about 8% more CPU than rumqttc.** The write task completes PUBACKs in batches; each completion wakes the caller that waits for it, and with several runnable callers on one worker Tokio wakes other worker threads to take them. In the 8-caller profile against Mosquitto, completing publishes is the largest single source of thread wake-ups (610 of 1413 samples), and futex calls are 16% of glide's worker time against none in rumqttc's, whose event loop completes one PUBACK at a time. With many callers the same batching spreads the callers over all cores, which is where glide's lead comes from; slowing it down for the 8-caller case would cost that.
- **Large publishes share a write.** The write task flushes mid-burst only once 16 KiB **and** 16 publishes are buffered (or 1 MiB). With the byte threshold alone, every publish of 16 KiB or more went out in a write of its own, and 16 KiB QoS 1 pipelined ran at 149k msg/s, behind rumqttc. A larger byte threshold alone also helped, but made QoS 1 at 1 KiB 5–10% slower, because the broker sees the first publishes of a burst later and acknowledges them later.
- **QoS 0 publish latency** is the different return point of `publish().await`, not work: glide's p50 includes one hop to the write task and back.

## Stock Mosquitto

Mosquitto with its default flow control: it announces Receive Maximum 20 and queues at most 1000 messages per client. This is what a client meets against an untuned broker. QoS 1, 256 B.

<!-- bench:stock:start -->

| Scenario | glide | rumqttc | mqtt5 |
|---|---:|---:|---:|
| Outbound concurrent, 64 callers | **243 997** ±2% · **5 883** · **261** / **328** / **364** µs | 201 041 ±1% · 6 959 · 316 / 389 / 421 µs | 124 940 ±1% · 14 732 · 502 / 737 / 806 µs |
| Outbound concurrent, 512 callers | **247 596** ±1% · **5 843** · **2 068** / **2 225** / **2 296** µs | 200 435 ±1% · 6 870 · 2 545 / 2 703 / 2 774 µs | 123 728 ±1% · 14 641 · 4 124 / 4 732 / 4 923 µs |
| Outbound pipelined | 256 563 ±3% · **2 891** | **269 659** ±6% · 2 906 | 142 709 ±1% · 15 516 |
| Ingress load, QoS 1, PUBACK after the app queue | 99 567 ±4% · **3 599** | **102 374** ±1% · 4 477 | n/a (`no_manual_ack`) |
| Ingress load, QoS 1, PUBACK on decode | n/a (`no_auto_ack`) | 98 251 ±2% · **4 036** | **100 871** ±1% · 4 243 |

Units: msg/s ±spread · CPU ns/msg · p50 / p99 / p99.9 µs per publish.

<!-- bench:stock:end -->

Mosquitto's default window of 20 does not hold glide back: concurrent publishing is as fast as against tuned Mosquitto (248k msg/s, 24% ahead of rumqttc). Pipelined, rumqttc is 5% ahead, inside the ±6% spread. Receiving is broker-bound for all clients; glide is 3% behind rumqttc at 20% less CPU per message. mqtt5 does better here than against tuned Mosquitto, because the window keeps its in-flight table small.

## Broker with its own in-flight cap

Some brokers keep fewer QoS 1 deliveries unacknowledged per client than the client's Receive Maximum allows; EMQX's `max_inflight` defaults to 32. Such a broker stops after 32 deliveries until PUBACKs come back, so a client that held PUBACKs back to batch them would stall it. The mock with deliveries capped at 32 (`MQTT_COMPARE_MOCK_DELIVERY_INFLIGHT=32`), QoS 1, 256 B:

<!-- bench:capped:start -->

| Scenario | glide | rumqttc | mqtt5 |
|---|---:|---:|---:|
| Ingress load, QoS 1, PUBACK after the app queue | **829 721** ±2% · **334** | 316 249 ±1% · 2 198 | n/a (`no_manual_ack`) |
| Ingress load, QoS 1, PUBACK on decode | n/a (`no_auto_ack`) | **820 312** ±1% · **526** | 248 484 ±0% · 3 465 |
| Ingress rate 10 000 msg/s, QoS 1, PUBACK after the app queue | 9 978 ±0% · **2 410** · **166** / **205** / **377** µs | **9 979** ±0% · 8 103 · 290 / 342 / 384 µs | n/a (`no_manual_ack`) |
| Ingress rate 10 000 msg/s, QoS 1, PUBACK on decode | n/a (`no_auto_ack`) | 9 978 ±0% · **3 193** · **179** / **206** / **334** µs | **9 978** ±0% · 9 865 · 286 / 359 / 392 µs |
| Ingress rate 50 000 msg/s, QoS 1, PUBACK after the app queue | **49 882** ±0% · **1 288** · **288** / **399** / **439** µs | 49 878 ±0% · 6 135 · 535 / 958 / 1 023 µs | n/a (`no_manual_ack`) |
| Ingress rate 50 000 msg/s, QoS 1, PUBACK on decode | n/a (`no_auto_ack`) | 49 878 ±0% · **1 950** · **344** / **433** / **472** µs | **49 881** ±0% · 7 552 · 522 / 1 074 / 1 191 µs |
| Ingress rate 200 000 msg/s, QoS 1, PUBACK after the app queue | **199 663** ±0% · **774** · **565** / **1 272** / **1 409** µs | 199 617 ±0% · 2 341 · 659 / 1 290 / 1 782 µs | n/a (`no_manual_ack`) |
| Ingress rate 200 000 msg/s, QoS 1, PUBACK on decode | n/a (`no_auto_ack`) | **199 699** ±0% · **1 157** · **579** / **1 225** / **1 496** µs | 199 637 ±0% · 3 274 · 710 / 5 202 / 6 331 µs |

Units: msg/s ±spread · CPU ns/msg · p50 / p99 / p99.9 µs. Ingress QoS 1 has a row per ack policy: `after_enqueue` sends PUBACK once the message is in the app queue, `before` on decode. Each client runs the policies it has (glide only `after_enqueue`, mqtt5 only `before`); `before` saves the client work and can lose a message the app never saw.

<!-- bench:capped:end -->

The cap costs every client throughput, but no client stalls: each acknowledges what it has as soon as nothing more is buffered. glide receives 830k msg/s, rumqttc 316k with the same ack policy and 820k with PUBACK on decode. At a fixed rate glide's latency is the lowest of the clients that acknowledge after the app queue.

## Payload size

Mock broker, the [matrix](#matrix) otherwise unchanged; 256 B is the peer run.

<!-- bench:payload:start -->

**Ingress load, QoS 0**:

| Payload | glide | rumqttc | mqtt5 |
|---|---:|---:|---:|
| 16 B | **5 422 424** ±1% · **175** · 87 MB/s | 3 800 986 ±2% · 288 · 61 MB/s | 2 279 919 ±2% · 911 · 36 MB/s |
| 256 B | **3 871 577** ±3% · **210** · 991 MB/s | 3 135 584 ±3% · 332 · 803 MB/s | 1 560 483 ±7% · 1 426 · 399 MB/s |
| 1 024 B | **2 093 954** ±1% · **285** · 2 144 MB/s | 2 082 432 ±4% · 506 · 2 132 MB/s | 1 016 674 ±9% · 1 481 · 1 041 MB/s |
| 16 384 B | **236 783** ±13% · **2 210** · 3 879 MB/s | 209 135 ±14% · 2 320 · 3 426 MB/s | 234 184 ±11% · 2 846 · 3 837 MB/s |

**Ingress load, QoS 1, PUBACK after the app queue (`after_enqueue`)**:

| Payload | glide | rumqttc | mqtt5 |
|---|---:|---:|---:|
| 16 B | **2 565 571** ±13% · **262** · 41 MB/s | 288 618 ±2% · 2 840 · 5 MB/s | n/a (`no_manual_ack`) |
| 256 B | **2 494 761** ±6% · **305** · 639 MB/s | 294 797 ±7% · 2 780 · 75 MB/s | n/a (`no_manual_ack`) |
| 1 024 B | **1 531 939** ±1% · **410** · 1 569 MB/s | 336 353 ±1% · 2 128 · 344 MB/s | n/a (`no_manual_ack`) |
| 16 384 B | **259 576** ±3% · **2 411** · 4 253 MB/s | 196 104 ±2% · 4 100 · 3 213 MB/s | n/a (`no_manual_ack`) |

**Outbound pipelined, QoS 0**:

| Payload | glide | rumqttc | mqtt5 |
|---|---:|---:|---:|
| 16 B | **4 035 702** ±1% · **220** · 65 MB/s | 258 552 ±1% · 6 212 · 4 MB/s | 154 532 ±1% · 7 300 · 2 MB/s |
| 256 B | **3 430 455** ±2% · **276** · 878 MB/s | 244 215 ±2% · 6 660 · 63 MB/s | 148 062 ±1% · 7 732 · 38 MB/s |
| 1 024 B | **2 097 280** ±1% · **408** · 2 148 MB/s | 234 151 ±1% · 6 924 · 240 MB/s | 141 286 ±1% · 8 065 · 145 MB/s |
| 16 384 B | **444 616** ±0% · **2 160** · 7 285 MB/s | 172 617 ±1% · 9 255 · 2 828 MB/s | 99 533 ±1% · 12 245 · 1 631 MB/s |

**Outbound pipelined, QoS 1**:

| Payload | glide | rumqttc | mqtt5 |
|---|---:|---:|---:|
| 16 B | **1 068 464** ±0% · **527** · 17 MB/s | 268 378 ±0% · 3 146 · 4 MB/s | 177 527 ±1% · 13 463 · 3 MB/s |
| 256 B | **904 876** ±2% · **602** · 232 MB/s | 267 156 ±0% · 3 166 · 68 MB/s | 162 232 ±3% · 14 265 · 42 MB/s |
| 1 024 B | **904 245** ±1% · **746** · 926 MB/s | 255 493 ±0% · 3 273 · 262 MB/s | 159 992 ±1% · 14 885 · 164 MB/s |
| 16 384 B | **360 819** ±0% · **3 059** · 5 912 MB/s | 181 080 ±0% · 4 704 · 2 967 MB/s | 125 884 ±1% · 18 421 · 2 062 MB/s |

Units: msg/s ±spread · CPU ns/msg · payload MB/s. 256 B comes from the peer run.

<!-- bench:payload:end -->

- glide leads at 16 B, 256 B and 1 KiB in every row. QoS 1 ingress with PUBACK after the app queue is 4 to 9× rumqttc's.
- At 16 KiB, receiving QoS 0 is a tie within the spread (±11% to ±14%). Receiving QoS 1, glide is 32% faster than rumqttc at 41% less CPU; publishing, it reaches 2 to 2.6× rumqttc's msg/s (QoS 1 361k vs 181k, QoS 0 445k vs 173k) at a quarter to two thirds of its CPU per message.

## Which inbound sink

mqtt-glide only (`ingress_fanin`). N clients, each subscribed on its own mock broker with its own publisher, hand every message to one shared queue that M consumer tasks drain. Each consumer spins a fixed amount of work per message (0 or 1000 ns) to stand in for the application. This is the shape of an application that ingests from several brokers into one worker pool.

| Sink | Handoff |
|------|---------|
| `mpsc` | `tokio::sync::mpsc`, one consumer |
| `flume` | `flume::Sender` (feature `flume`): bounded MPMC channel, one mutex inside |
| `async_channel` | `async_channel::Sender` (feature `async-channel`): bounded MPMC channel on a lock-free queue |
| `shard` | one `tokio::sync::mpsc` per consumer; each message goes to the next shard with room. No per-topic ordering |
| `inline` | `on_inbound`: the work runs on each connection's read task, no queue |

Total queue capacity is 10 000 for every sink. Brokers on CPUs 4-7, publishers on 8-11, client process on 0-3.

<!-- bench:fanin:start -->

**QoS 0, 0 ns work per message:**

| Brokers × consumers | `mpsc` | `flume` | `async_channel` | `shard` | `inline` |
|---|---:|---:|---:|---:|---:|
| 1 × 1 | **5.30M** ±1% · 161 | 5.30M ±1% · 141 | 5.29M ±1% · 143 | 5.29M ±0% · 166 | 5.25M ±1% · 113 |
| 1 × 2 |  | 3.16M ±19% · 599 | **4.65M** ±18% · 357 | 4.52M ±6% · 362 |  |
| 1 × 4 |  | 3.16M ±6% · 683 | **4.37M** ±16% · 588 | 4.07M ±5% · 455 |  |
| 2 × 1 | **5.02M** ±2% · 447 · 4.8 | 4.61M ±15% · 532 · 1.2 | 4.89M ±5% · 416 | 4.79M ±2% · 458 · 6.0 |  |
| 2 × 2 |  | 4.24M ±11% · 644 | 5.14M ±5% · 487 | 5.07M ±2% · 463 · 0.9 | **10.38M** ±1% · 108 |
| 2 × 4 |  | 2.49M ±19% · 998 | 3.15M ±12% · 805 | **4.68M** ±1% · 619 |  |
| 4 × 1 | **4.58M** ±1% · 556 · 29.4 | 3.93M ±14% · 762 · 1.7 | 4.28M ±2% · 814 · 0.3 | 4.48M ±1% · 570 · 29.3 |  |
| 4 × 2 |  | 2.80M ±21% · 1 000 | 4.63M ±20% · 759 · 0.1 | **5.99M** ±1% · 562 · 12.8 |  |
| 4 × 4 |  | 2.65M ±19% · 1 092 | 3.93M ±32% · 826 | 6.39M ±1% · 545 · 4.1 | **19.20M** ±9% · 123 |

**QoS 0, 1000 ns work per message:**

| Brokers × consumers | `mpsc` | `flume` | `async_channel` | `shard` | `inline` |
|---|---:|---:|---:|---:|---:|
| 1 × 1 | 851k ±0% · 1 212 · 7.9 | 866k ±2% · 1 157 | 893k ±2% · 1 172 · 0.1 | 847k ±0% · 1 220 · 7.9 | **896k** ±0% · 1 115 |
| 1 × 2 |  | 1.56M ±3% · 1 354 · 0.1 | 1.55M ±2% · 1 368 · 0.1 | **1.65M** ±0% · 1 239 · 3.5 |  |
| 1 × 4 |  | 2.92M ±1% · 1 303 | 2.80M ±2% · 1 360 | **3.18M** ±0% · 1 250 · 1.7 |  |
| 2 × 1 | **882k** ±0% · 1 941 · 130.4 | 867k ±1% · 1 975 · 120.4 | 861k ±1% · 2 063 · 137.2 | 881k ±0% · 1 960 · 130.2 |  |
| 2 × 2 |  | 1.41M ±7% · 1 803 · 36.2 | 1.31M ±12% · 1 958 · 76.4 | 1.50M ±0% · 1 430 · 15.1 | **1.77M** ±1% · 1 117 |
| 2 × 4 |  | 2.31M ±3% · 1 615 | 2.73M ±8% · 1 441 · 0.1 | **2.93M** ±0% · 1 357 · 4.3 |  |
| 4 × 1 | **792k** ±0% · 2 464 · 552.2 | 717k ±2% · 2 869 · 597.1 | 760k ±3% · 2 675 · 348.3 | 790k ±0% · 2 469 · 548.6 |  |
| 4 × 2 |  | 635k ±1% · 3 257 · 238.7 | 700k ±4% · 3 075 · 231.9 | **1.59M** ±0% · 1 838 · 170.8 |  |
| 4 × 4 |  | 2.09M ±2% · 1 725 · 0.1 | 2.49M ±5% · 1 574 · 0.2 | 2.67M ±0% · 1 489 · 9.3 | **3.52M** ±1% · 1 115 |

**QoS 1, 0 ns work per message:**

| Brokers × consumers | `mpsc` | `flume` | `async_channel` | `shard` | `inline` |
|---|---:|---:|---:|---:|---:|
| 1 × 1 | 2.45M ±3% · 243 | **2.61M** ±1% · 204 | 2.45M ±1% · 200 | 2.52M ±3% · 257 | 2.52M ±1% · 176 |
| 1 × 2 |  | 2.33M ±6% · 584 | 2.34M ±5% · 439 | **2.35M** ±5% · 441 |  |
| 1 × 4 |  | 2.31M ±5% · 744 | **2.49M** ±5% · 596 | 2.40M ±4% · 510 |  |
| 2 × 1 | 4.46M ±6% · 453 · 0.7 | 3.66M ±14% · 482 | **4.50M** ±5% · 345 | 4.13M ±3% · 453 · 0.7 |  |
| 2 × 2 |  | 3.80M ±13% · 524 | 3.98M ±9% · 477 | 4.09M ±6% · 471 | **4.78M** ±5% · 176 |
| 2 × 4 |  | 2.23M ±12% · 985 | 2.49M ±12% · 894 | **3.84M** ±2% · 646 |  |
| 4 × 1 | 3.14M ±2% · 704 · 25.1 | 4.04M ±11% · 643 · 0.1 | **4.30M** ±21% · 711 · 0.2 | 3.04M ±1% · 724 · 26.0 |  |
| 4 × 2 |  | 3.16M ±11% · 795 · 0.1 | 4.25M ±4% · 741 · 0.1 | **4.41M** ±0% · 590 · 5.1 |  |
| 4 × 4 |  | 2.93M ±11% · 862 | 3.55M ±8% · 888 · 0.1 | 4.60M ±1% · 606 · 1.7 | **9.17M** ±3% · 160 |

**QoS 1, 1000 ns work per message:**

| Brokers × consumers | `mpsc` | `flume` | `async_channel` | `shard` | `inline` |
|---|---:|---:|---:|---:|---:|
| 1 × 1 | 807k ±0% · 1 324 · 28.9 | 848k ±4% · 1 346 · 40.4 | **854k** ±1% · 1 438 · 80.0 | 801k ±0% · 1 332 · 28.0 | 831k ±0% · 1 184 |
| 1 × 2 |  | 1.53M ±1% · 1 651 · 61.4 | 1.49M ±3% · 1 793 · 107.4 | **1.55M** ±0% · 1 322 · 7.7 |  |
| 1 × 4 |  | 2.16M ±1% · 1 636 · 0.3 | 2.43M ±2% · 1 459 | **2.78M** ±7% · 1 302 |  |
| 2 × 1 | 837k ±0% · 2 069 · 277.6 | **888k** ±1% · 2 041 · 293.9 | 883k ±0% · 2 057 · 292.6 | 836k ±0% · 2 074 · 273.9 |  |
| 2 × 2 |  | 1.54M ±4% · 1 684 · 71.0 | 1.50M ±3% · 1 776 · 94.5 | 1.58M ±1% · 1 522 · 52.3 | **1.61M** ±2% · 1 187 |
| 2 × 4 |  | 2.11M ±11% · 1 692 · 1.4 | 2.35M ±3% · 1 614 | **2.73M** ±1% · 1 405 · 7.6 |  |
| 4 × 1 | 695k ±0% · 3 331 · 468.5 | 594k ±1% · 3 719 · 446.1 | 661k ±2% · 3 481 · 457.3 | **696k** ±1% · 3 337 · 461.9 |  |
| 4 × 2 |  | 1.21M ±15% · 2 370 · 221.4 | 1.37M ±8% · 2 295 · 265.0 | **1.55M** ±0% · 1 917 · 149.2 |  |
| 4 × 4 |  | 2.01M ±2% · 1 747 · 8.4 | 2.16M ±1% · 1 744 · 6.4 | 2.50M ±0% · 1 500 · 19.0 | **3.17M** ±1% · 1 153 |

Cells: msg/s ±spread · CPU ns/msg · handoff stalls per 1000 messages (omitted when ~0). **Bold** = fastest sink in the row. `mpsc` only runs with one consumer; `inline` runs the work on each connection's read task, so its consumer count is the broker count.

<!-- bench:fanin:end -->

- **Cheap work per message:** a shared queue does not scale with consumers; more consumers add contention. `inline` (the work in `.on_inbound`, no queue) is fastest by far: 10.4M msg/s from 2 brokers, 19.2M from 4 at QoS 0, 9.2M from 4 at QoS 1.
- **Real work (1000 ns):** consumers scale. One queue per consumer (`shard`) is the fastest queue in most rows, up to 2× a shared one (4 brokers × 2 consumers, QoS 0: 1.59M vs 0.70M).
- **async-channel vs flume:** async-channel is ahead in 24 of 36 rows, by up to 65% (QoS 0, 4 × 2, no work); flume is 4 to 8% ahead in three rows.
- **Recommendation:** non-blocking work per message → `.on_inbound`. A worker pool fed by several brokers → async-channel, or one `tokio::sync::mpsc` per worker chosen by a hash of the topic, which also keeps per-topic order; no shared queue with several consumers keeps it.

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
| Bench container | Debian 13 (glibc 2.41), rustc 1.99.1, `--cap-add SYS_NICE` for `taskset` |
| Broker | eclipse-mosquitto `2.1.2-alpine` containers in the same Podman pod (shared network namespace, loopback) |

Linux, because that is where MQTT clients run in production and the only place the three processes can be pinned and profiled with `perf`. A VM makes wake-ups and syscalls somewhat dearer than bare metal, for every client alike. The core cells were [cross-checked natively on macOS](#macos-cross-check).

### CPU pinning

| Process | CPUs |
|---------|------|
| Client under test (whole bench process, Tokio multi-thread runtime with 4 workers) | 0-3 |
| Broker (mock child or Mosquitto, pinned with `taskset` on its PID) | 4-7 |
| Ingress publisher child | 8-11 |

Client, broker and load generator never share a core, so one of them cannot slow down another by competing for CPU.

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
| Allocator | system (glibc) |
| QoS | mock 0 and 1; tuned Mosquitto 0 and 1 (outbound QoS 0 not run, see below); stock Mosquitto 1 |

Versions: see each detail page (`meta.json` of the run).

### Brokers

| Broker | Role |
|--------|------|
| Tuned Mosquitto | `max_inflight_messages 0`, `max_queued_messages 0`, `set_tcp_nodelay true`, no persistence, `$SYS` or logs. The real-broker comparison |
| Stock Mosquitto | Same config without the two flow-control lines, so Mosquitto's defaults apply: `max_inflight_messages 20` (CONNACK Receive Maximum 20), `max_queued_messages 1000` |
| Mock | Out-of-process MQTT 5 broker built for the bench: multi-thread, `TCP_NODELAY`, honours the client's Receive Maximum, handles every frame already in its read buffer before it writes (one write per batch of PUBACKs or deliveries, as a real broker does), counts decoded PUBLISH frames. It is faster than every client, so it shows each client's ceiling. Not a broker claim |

## Fairness rules

- **Same knobs for every client.** Every client announces the same Receive Maximum (1024). The libraries' defaults differ (glide 256, rumqttc and mqtt5 65 535); with defaults, the comparison would measure the default, not the client.
- **The harness runs where an application's code runs.** Callers, the rate generator and the ingress consumer are tasks on the same Tokio runtime as the client, the same harness code for every client. Run on the thread that called `block_on` instead, every message a waiting consumer gets costs the producing client a futex wake-up that no real consumer task causes; how many depended on each client's timing.
- **The load generator and broker are not the limit.** Publisher and mock broker are separate pinned processes; the mock is faster than every client (on the mock, glide's QoS 1 ingress reaches about 2M msg/s). On Mosquitto, the broker is the limit for ingress.
- **Ingress is always a handoff.** Every ingress cell is decode → bounded `tokio::sync::mpsc` app queue → consumer task; no client handles messages inline in its read loop. Peers are adapted to that shape.
- **Both ack policies are shown.** Ingress QoS 1 has a row for PUBACK after the app queue and one for PUBACK on decode; a client is compared only with clients running the same policy.
- **Outbound ends on the wire.** On the mock, a burst ends when the mock has decoded all N PUBLISH frames; a count mismatch fails the cell, so an early `publish().await` return cannot shorten it. On Mosquitto only QoS 1 is run outbound (PUBACK proves the broker has it); outbound QoS 0 needs a wire counter and runs on the mock only.
- **Idiomatic APIs.** Pipelined uses `publish_window` (glide), the notice future (rumqttc), spawned publishes (mqtt5). Unsupported capabilities are `n/a`, never emulated.
- **What `publish().await` returns on at QoS 0 differs:** glide once the write task has encoded the message into its write buffer, rumqttc when the request is accepted onto its event-loop channel, mqtt5 after encode and write on the caller. QoS 0 latency columns measure that point; throughput is always wire-complete.
- **Clients take turns.** Each of the 5 runs of a cell measures every client once, with the first client rotating per run; fan-in sinks of one table row do the same. The host drifts by up to 2× over minutes (macOS places the VM's vCPUs on performance or efficiency cores), so measuring one client's 5 runs and then the next one's would compare different minutes.
- **Versions:** rumqttc is pinned to 0.33.3 because 0.34.0 scans its whole outgoing table on every flush; against a broker without Receive Maximum that table has 65 535 slots and throughput falls to a few thousand msg/s.

## macOS cross-check

The core cells, run natively on macOS on the same machine (no pinning possible, mock broker, 256 B, median of 3, clients taking turns). glide leads every cell on both platforms. The only change of order is between the peers: concurrent QoS 0 at 64 callers, where mqtt5 is ahead of rumqttc on macOS (292k vs 253k) and behind on Linux.

| Cell | glide | rumqttc | mqtt5 |
|---|---:|---:|---:|
| Ingress load, QoS 0 | **3.86M** | 3.70M | 3.08M |
| Ingress load, QoS 1, PUBACK after the app queue | **3.74M** | 312k | n/a |
| Ingress load, QoS 1, PUBACK on decode | n/a | **2.81M** | 303k |
| Outbound pipelined, QoS 0 | **4.14M** | 253k | 216k |
| Outbound pipelined, QoS 1 | **1.18M** | 295k | 239k |
| Outbound concurrent 64, QoS 0 | **2.28M** | 253k | 292k |
| Outbound concurrent 64, QoS 1 | **1.01M** | 280k | 156k |

msg/s. On macOS Tokio uses kqueue instead of epoll, and the three processes share all cores.

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
| `peers-mosquitto-stock` | port 1884, QoS 1, 200 000 messages, `MQTT_COMPARE_SCENARIOS=ingress_load,outbound_pipelined,outbound_concurrent` |
| `capped-mock` | QoS 1, `MQTT_COMPARE_MOCK_DELIVERY_INFLIGHT=32`, `MQTT_COMPARE_SCENARIOS=ingress_load,ingress_rate` |
| `payload` | 500 000 messages, `MQTT_COMPARE_SCENARIOS=ingress_load,outbound_pipelined`, `MQTT_COMPARE_PAYLOADS=16,1024,16384` |
| `fanin` | `cargo bench --features __bench,stats,flume,async-channel --bench ingress_fanin`, QoS 0 and 1, default matrix |

The Mosquitto configs are [`benches/mosquitto/tuned.conf`](benches/mosquitto/tuned.conf) and [`benches/mosquitto/stock.conf`](benches/mosquitto/stock.conf); the stock one only drops the two flow-control lines, so it falls back to Mosquitto's defaults (inflight 20, 1000 queued).
