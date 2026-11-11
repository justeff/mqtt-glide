# Benchmarks

mqtt-glide against rumqttc-v5-next and LabOverWire mqtt5: same broker, payload, harness and pinning, every client with its default settings apart from the [shared knobs](#fairness-rules), clients taking turns run by run. Numbers are absolute and only hold under the conditions in [Setup](#setup).

Detail pages with every scenario and level: [tuned Mosquitto](benches/results/mosquitto.md), [stock Mosquitto](benches/results/mosquitto-stock.md), [mock broker](benches/results/mock.md). Running the harness and its env knobs: [`benches/README.md`](benches/README.md).

## Verdict

- **Publishing (tuned Mosquitto, QoS 1):** glide leads at every caller count with the lowest CPU and latency: 144k / 232k / 249k msg/s at 8 / 64 / 512 callers, mqtt5 79k to 81k, rumqttc 7.9k.
- **Publishing at 50 000 msg/s offered:** glide 16.0 µs CPU per publish, p50 0.67 ms; mqtt5 26.7 µs, 0.84 ms; rumqttc reaches 4.7k msg/s.
- **Receiving QoS 1 from Mosquitto:** the broker is the limit. glide 107k msg/s at 3.5 µs CPU per message (PUBACK after the app queue); rumqttc 16k with the same policy, 137k with PUBACK on decode at 7.1 µs; mqtt5 106k at 4.2 µs.
- **Stock Mosquitto:** glide publishes 242k msg/s with 64 callers against 159k (rumqttc) and 114k (mqtt5); receiving, all three get about 100k, glide at the lowest CPU.
- **Client ceiling (mock):** publishing QoS 0 pipelined 3.23M msg/s against 195k and 146k; QoS 1 882k against 174k and 135k. Receiving QoS 0 3.81M against 2.60M and 1.40M; QoS 1 2.51M, 11× rumqttc with the same ack policy.
- **QoS 0 publish latency:** rumqttc's `publish().await` returns sooner (p50 51 vs 110 µs at 50 000 msg/s on the mock); the return points differ, see [Fairness rules](#fairness-rules).
- **Broker capping deliveries at 32:** glide receives 836k msg/s, rumqttc 245k with the same policy and 781k with PUBACK on decode, mqtt5 244k.
- **Payload size:** glide leads at every size, closest receiving 16 KiB at QoS 0 (270k vs 252k); publishing 16 KiB at QoS 1 it reaches 399k msg/s against 133k and 121k.

## Summary

<!-- bench:summary:start -->

**Real broker**: tuned Mosquitto, QoS 1 unless noted, 256 B.

| Scenario | glide | rumqttc | mqtt5 |
|---|---:|---:|---:|
| Outbound concurrent, 8 callers | **143 701** ±1% · **7 762** · **55.0** / **97.3** / **113** µs | 7 867 ±1% · 133 967 · 1 012 / 1 099 / 1 147 µs | 81 068 ±2% · 13 938 · 100.0 / 146 / 166 µs |
| Outbound concurrent, 64 callers | **232 187** ±0% · **6 374** · **274** / **367** / **458** µs | 7 887 ±2% · 133 802 · 8 103 / 8 484 / 8 842 µs | 79 870 ±1% · 14 230 · 804 / 966 / 1 019 µs |
| Outbound concurrent, 512 callers | **249 235** ±5% · **6 067** · **2 053** / **2 221** / **2 310** µs | 7 842 ±1% · 134 664 · 65 075 / 67 234 / 73 554 µs | 78 660 ±1% · 14 441 · 6 514 / 7 132 / 7 339 µs |
| Outbound pipelined | **256 599** ±1% · **3 103** | 8 963 ±2% · 108 819 | 124 254 ±1% · 15 607 |
| Outbound rate, 50 000 msg/s offered | 49 033 ±0% · **15 961** · **666** / **1 454** / **1 705** µs | 4 698 ±1% · 272 952 · 16 203 557 / 19 244 763 / 19 280 966 µs | **49 124** ±0% · 26 675 · 842 / 3 017 / 4 853 µs |
| Ingress load, QoS 0 | 213 187 ±2% · **1 641** | **231 683** ±15% · 4 763 | 220 986 ±1% · 1 930 |
| Ingress load, QoS 1, PUBACK after the app queue | **106 543** ±1% · **3 538** | 16 215 ±1% · 57 639 | n/a (`no_manual_ack`) |
| Ingress load, QoS 1, PUBACK on decode | n/a (`no_auto_ack`) | **136 952** ±4% · 7 100 | 105 742 ±0% · **4 224** |
| Ingress rate 50 000 msg/s, QoS 1, PUBACK after the app queue | **49 819** ±0% · **5 622** · **778** / **1 633** / **1 892** µs | 15 479 ±2% · 60 447 · 2 243 824 / 4 406 696 / 4 446 292 µs | n/a (`no_manual_ack`) |
| Ingress rate 50 000 msg/s, QoS 1, PUBACK on decode | n/a (`no_auto_ack`) | **49 805** ±0% · 13 908 · 927 / 1 918 / 3 093 µs | 49 783 ±0% · **6 802** · **801** / **1 638** / **1 855** µs |

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

## Stock Mosquitto

Mosquitto's defaults: Receive Maximum 20, at most 1000 queued messages per client. QoS 1, 256 B.

<!-- bench:stock:start -->

| Scenario | glide | rumqttc | mqtt5 |
|---|---:|---:|---:|
| Outbound concurrent, 64 callers | **241 712** ±1% · **6 124** · **263** / **332** / **371** µs | 158 861 ±1% · 9 775 · 401 / 492 / 527 µs | 114 419 ±1% · 14 389 · 558 / 783 / 851 µs |
| Outbound concurrent, 512 callers | **244 712** ±0% · **6 067** · **2 089** / **2 241** / **2 311** µs | 159 903 ±1% · 9 693 · 3 198 / 3 423 / 3 520 µs | 112 265 ±1% · 14 562 · 4 566 / 5 162 / 5 327 µs |
| Outbound pipelined | **259 664** ±1% · **3 019** | 249 778 ±11% · 3 036 | 129 535 ±0% · 15 924 |
| Ingress load, QoS 1, PUBACK after the app queue | 102 211 ±4% · **3 566** | **102 804** ±1% · 4 669 | n/a (`no_manual_ack`) |
| Ingress load, QoS 1, PUBACK on decode | n/a (`no_auto_ack`) | 99 460 ±2% · **4 226** | **101 361** ±1% · 4 380 |

Units: msg/s ±spread · CPU ns/msg · p50 / p99 / p99.9 µs per publish.

<!-- bench:stock:end -->

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

## Which inbound sink

mqtt-glide only (`ingress_fanin`). N clients, each on its own mock broker with its own publisher, hand every message to one shared sink that M consumer tasks drain; each consumer spins 0 or 1000 ns per message to stand in for the application. Total queue capacity 10 000 for every sink.

| Sink | Handoff |
|------|---------|
| `mpsc` | `tokio::sync::mpsc`, one consumer |
| `flume` | `flume::Sender` (feature `flume`): bounded MPMC channel, one mutex inside |
| `async_channel` | `async_channel::Sender` (feature `async-channel`): bounded MPMC channel on a lock-free queue |
| `shard` | one `tokio::sync::mpsc` per consumer; each message goes to the next shard with room. No per-topic ordering |
| `inline` | `on_inbound`: the work runs on each connection's read task, no queue |

<!-- bench:fanin:start -->

**QoS 0, 0 ns work per message:**

| Brokers × consumers | `mpsc` | `flume` | `async_channel` | `shard` | `inline` |
|---|---:|---:|---:|---:|---:|
| 1 × 1 | 4.92M ±3% · 176 | 4.88M ±5% · 151 | **5.05M** ±3% · 153 | 4.84M ±3% · 179 | 4.95M ±1% · 123 |
| 1 × 2 |  | 4.01M ±18% · 430 | **4.50M** ±12% · 382 | 3.40M ±20% · 376 |  |
| 1 × 4 |  | 2.91M ±6% · 730 | 3.03M ±10% · 709 | **3.95M** ±18% · 462 |  |
| 2 × 1 | 4.88M ±2% · 477 · 3.2 | 3.55M ±14% · 594 | **5.82M** ±9% · 353 | 4.82M ±1% · 469 · 4.3 |  |
| 2 × 2 |  | 3.45M ±2% · 636 | 5.21M ±7% · 499 | 5.10M ±2% · 465 · 0.5 | **9.09M** ±6% · 124 |
| 2 × 4 |  | 2.43M ±15% · 1 008 | 2.83M ±22% · 844 | **4.60M** ±0% · 631 |  |
| 4 × 1 | 4.40M ±1% · 578 · 29.6 | 2.87M ±20% · 974 · 1.4 | **4.51M** ±30% · 816 · 0.2 | 4.41M ±1% · 584 · 29.5 |  |
| 4 × 2 |  | 3.79M ±13% · 814 | 4.34M ±21% · 815 | **5.84M** ±1% · 578 · 12.6 |  |
| 4 × 4 |  | 3.49M ±14% · 859 | 3.80M ±28% · 839 | 6.35M ±2% · 557 · 4.1 | **18.42M** ±6% · 137 |

**QoS 0, 1000 ns work per message:**

| Brokers × consumers | `mpsc` | `flume` | `async_channel` | `shard` | `inline` |
|---|---:|---:|---:|---:|---:|
| 1 × 1 | 841k ±0% · 1 224 · 7.8 | 858k ±0% · 1 166 | 851k ±0% · 1 175 | 841k ±0% · 1 228 · 7.9 | **890k** ±0% · 1 124 |
| 1 × 2 |  | 1.57M ±0% · 1 363 · 0.1 | 1.56M ±3% · 1 384 · 0.1 | **1.65M** ±0% · 1 240 · 3.5 |  |
| 1 × 4 |  | 2.52M ±1% · 1 513 | 2.72M ±3% · 1 381 | **3.17M** ±0% · 1 254 · 1.6 |  |
| 2 × 1 | 878k ±0% · 1 953 · 126.3 | 866k ±1% · 1 984 · 117.9 | 859k ±1% · 2 067 · 135.2 | **882k** ±0% · 1 952 · 126.4 |  |
| 2 × 2 |  | 1.23M ±29% · 2 067 · 72.6 | 1.30M ±3% · 1 954 · 72.6 | 1.51M ±1% · 1 427 · 15.0 | **1.77M** ±0% · 1 124 |
| 2 × 4 |  | 2.32M ±8% · 1 623 · 0.1 | 2.69M ±3% · 1 466 · 0.1 | **2.93M** ±0% · 1 360 · 4.3 |  |
| 4 × 1 | 773k ±0% · 2 498 · 543.9 | 714k ±3% · 2 833 · 580.6 | 754k ±3% · 2 679 · 340.1 | **789k** ±0% · 2 469 · 545.6 |  |
| 4 × 2 |  | 636k ±6% · 3 232 · 230.4 | 705k ±5% · 3 035 · 222.0 | **1.59M** ±0% · 1 833 · 166.3 |  |
| 4 × 4 |  | 2.02M ±3% · 1 771 · 0.4 | 2.47M ±2% · 1 607 · 0.2 | 2.66M ±0% · 1 496 · 9.1 | **3.50M** ±2% · 1 131 |

**QoS 1, 0 ns work per message:**

| Brokers × consumers | `mpsc` | `flume` | `async_channel` | `shard` | `inline` |
|---|---:|---:|---:|---:|---:|
| 1 × 1 | 2.30M ±5% · 260 | **2.35M** ±4% · 214 | 2.35M ±1% · 213 | 2.27M ±2% · 265 | 2.30M ±4% · 185 |
| 1 × 2 |  | 2.17M ±6% · 586 | **2.21M** ±5% · 469 | 2.20M ±2% · 460 |  |
| 1 × 4 |  | 2.15M ±8% · 646 | 2.16M ±1% · 639 | **2.18M** ±3% · 531 |  |
| 2 × 1 | 3.92M ±1% · 450 · 0.7 | 3.56M ±16% · 514 | **4.06M** ±3% · 386 | 3.92M ±2% · 460 · 0.6 |  |
| 2 × 2 |  | 3.86M ±4% · 508 | 3.58M ±4% · 521 | 3.75M ±2% · 510 | **4.46M** ±1% · 197 |
| 2 × 4 |  | 2.13M ±1% · 1 013 | 2.30M ±4% · 954 | **3.58M** ±3% · 678 |  |
| 4 × 1 | 2.98M ±4% · 734 · 25.6 | 3.97M ±11% · 654 · 0.1 | **4.23M** ±6% · 726 · 0.2 | 3.05M ±1% · 733 · 24.8 |  |
| 4 × 2 |  | 3.11M ±12% · 823 · 0.1 | 4.21M ±16% · 741 · 0.1 | **4.39M** ±1% · 592 · 4.7 |  |
| 4 × 4 |  | 2.89M ±12% · 873 | 3.46M ±5% · 884 · 0.1 | 4.71M ±2% · 610 · 1.8 | **9.20M** ±2% · 162 |

**QoS 1, 1000 ns work per message:**

| Brokers × consumers | `mpsc` | `flume` | `async_channel` | `shard` | `inline` |
|---|---:|---:|---:|---:|---:|
| 1 × 1 | 798k ±1% · 1 336 · 27.8 | **854k** ±2% · 1 448 · 90.4 | 849k ±2% · 1 340 · 37.0 | 792k ±1% · 1 339 · 28.2 | 817k ±1% · 1 197 |
| 1 × 2 |  | 1.54M ±0% · 1 652 · 68.4 | 1.48M ±1% · 1 817 · 116.4 | **1.54M** ±0% · 1 335 · 8.0 |  |
| 1 × 4 |  | 2.30M ±7% · 1 557 | **2.44M** ±1% · 1 484 | 2.41M ±1% · 1 322 |  |
| 2 × 1 | 834k ±0% · 2 084 · 275.6 | **888k** ±0% · 2 036 · 289.3 | 883k ±0% · 2 049 · 284.3 | 834k ±0% · 2 077 · 271.9 |  |
| 2 × 2 |  | 1.52M ±1% · 1 710 · 68.9 | 1.51M ±2% · 1 827 · 109.6 | 1.57M ±0% · 1 534 · 52.5 | **1.58M** ±1% · 1 199 |
| 2 × 4 |  | 1.99M ±5% · 1 752 | 2.35M ±4% · 1 637 · 0.6 | **2.73M** ±0% · 1 408 · 7.6 |  |
| 4 × 1 | 690k ±0% · 3 362 · 460.5 | 593k ±2% · 3 726 · 439.4 | 664k ±2% · 3 503 · 456.5 | **699k** ±0% · 3 357 · 465.9 |  |
| 4 × 2 |  | 1.19M ±2% · 2 409 · 222.7 | 1.37M ±3% · 2 318 · 265.9 | **1.55M** ±0% · 1 927 · 149.7 |  |
| 4 × 4 |  | 1.99M ±2% · 1 752 · 7.6 | 2.23M ±2% · 1 707 · 9.0 | 2.50M ±1% · 1 504 · 18.8 | **3.14M** ±3% · 1 165 |

Cells: msg/s ±spread · CPU ns/msg · handoff stalls per 1000 messages (omitted when ~0). **Bold** = fastest sink in the row. `mpsc` only runs with one consumer; `inline` runs the work on each connection's read task, so its consumer count is the broker count.

<!-- bench:fanin:end -->

- **Cheap work per message:** `inline` (`.on_inbound`, no queue) is fastest by far: 18.4M msg/s from 4 brokers at QoS 0, 9.2M at QoS 1. A shared queue does not scale with consumers.
- **1000 ns work:** consumers scale; one queue per consumer (`shard`) is the fastest queue in most rows, up to 2.5× a shared one (4 × 2, QoS 0: 1.59M vs 636k).
- **Recommendation:** non-blocking work → `.on_inbound`. A worker pool fed by several brokers → async-channel, or one `tokio::sync::mpsc` per worker chosen by a hash of the topic, which also keeps per-topic order.

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
| QoS | mock 0 and 1; tuned Mosquitto 0 and 1 (outbound QoS 0 only on the mock); stock Mosquitto 1 |

Client versions: each detail page (`meta.json` of the run).

### Brokers

| Broker | Config |
|--------|--------|
| Tuned Mosquitto | `max_inflight_messages 0` (CONNACK Receive Maximum 65 535), `max_queued_messages 0`, `set_tcp_nodelay true`, no persistence, `$SYS` or logs |
| Stock Mosquitto | The same without the two flow-control lines: Receive Maximum 20, 1000 queued |
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
| `peers-mosquitto-stock` | port 1884, QoS 1, 200 000 messages, `MQTT_COMPARE_SCENARIOS=ingress_load,outbound_pipelined,outbound_concurrent` |
| `capped-mock` | QoS 1, `MQTT_COMPARE_MOCK_DELIVERY_INFLIGHT=32`, `MQTT_COMPARE_SCENARIOS=ingress_load,ingress_rate` |
| `payload` | 500 000 messages, `MQTT_COMPARE_SCENARIOS=ingress_load,outbound_pipelined`, `MQTT_COMPARE_PAYLOADS=16,1024,16384` |
| `fanin` | `cargo bench --features __bench,stats,flume,async-channel --bench ingress_fanin`, QoS 0 and 1, default matrix |

The Mosquitto configs are [`benches/mosquitto/tuned.conf`](benches/mosquitto/tuned.conf) and [`benches/mosquitto/stock.conf`](benches/mosquitto/stock.conf); the stock one only drops the two flow-control lines, so it falls back to Mosquitto's defaults (inflight 20, 1000 queued).
