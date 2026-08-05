# mqtt-glide

[![crates.io](https://img.shields.io/crates/v/mqtt-glide.svg)](https://crates.io/crates/mqtt-glide)
[![docs.rs](https://docs.rs/mqtt-glide/badge.svg)](https://docs.rs/mqtt-glide)
[![license: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

Tokio MQTT **5** client: duplex API, injectable inbound handoff, and flow control without an app-side event loop.

MUST tracker: [`tests/conformance/checklist.md`](tests/conformance/checklist.md) · Protocol: [OASIS MQTT 5.0](https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html)

## Quickstart

```toml
[dependencies]
mqtt-glide = "0.1"
tokio = { version = "1", features = ["rt-multi-thread", "macros", "net", "time"] }
flume = "0.12"   # optional; any InboundSink works
```

Connect, publish once, shut down — no event loop to drive:

```rust
use mqtt_glide::{ClientBuilder, QoS};

let client = ClientBuilder::new("mqtt://127.0.0.1:1883")?
    .discard_inbound()
    .connect()
    .await?;

client
    .publish_with()
    .topic("sensors/room1/temp")
    .payload("22.5")
    .qos(QoS::AtLeastOnce)
    .send()
    .await?;

client.shutdown().await?;
```

`connect()` spawns the connection driver; the cloneable `MqttClient` handle is all your code keeps. Receiving works the same way — pick an inbound handoff (channel or inline handler) and consume messages in your own tasks. Runnable flows live under [`examples/`](examples/); the tables below are the reference for knobs and semantics.

## Design goals

1. **No app event loop:** subscribe and publish in parallel without `eventloop.poll()`.
2. **Clean drain:** graceful shutdown without peeking crate internals; no "acked but not forwarded" loss window.
3. **One handoff:** socket → decode → injectable [`InboundSink`] → your workers (not client queue + app queue).
4. **Duplex from day one:** ingress and egress are first-class; QoS 0/1 with MQTT 5 publish properties.
5. **Spec-backed:** client **MUST**s tracked in the conformance checklist; product non-goals are explicit (see feature matrix).
6. **Auth stays outside:** `CredentialsProvider` hook only; no OAuth/token manager in this crate.

**Non-goals:** MQTT 3.1.1, QoS 2, Topic Alias, WebSockets, `no_std`, embedding business/domain logic.

## Why another client?

Typical `rumqttc`-style stacks push apps into `eventloop.poll()`, an internal ack buffer, and a second hop into the app queue. That makes **graceful drain** awkward and couples keep-alive to handoff stalls: a full app queue can stop `poll()`, starve PINGs, and disconnect (often by accident). Auto-ack on decode also opens a silent loss window (broker considers the message delivered before your workers accept it).

**mqtt-glide** separates concerns so backpressure is policy, not an event-loop accident:

| Concern | Approach |
|---------|----------|
| App API | Cloneable `MqttClient` (no polling) |
| I/O | Separate **read** and **write** tasks on a split socket |
| Inbound | Injectable `InboundSink` (e.g. `flume` / `tokio::mpsc`) |
| QoS 1 ingress | **Enqueue-before-ack:** `PUBACK` only after the sink accepts |
| Keep-alive under load | Write keeps sending `PINGREQ` while read waits on a full sink; liveness knows about self-induced read stall |
| Shutdown | Graceful `DISCONNECT` + join |

## Architecture

Three roles, one connection driver:

1. **`MqttClient`:** cheap cloneable handle; commands only (no socket I/O).
2. **Write task:** `FramedWrite` + codec; encodes `PUBLISH` / `SUBSCRIBE` / acks / `PINGREQ`; prioritizes inbound `PUBACK`s over app publishes.
3. **Read task + coordinator:** `FramedRead` + codec; control frames handled inline; application `PUBLISH` goes to the sink. Reconnect and credential refresh live here, not in app code.

```text
                    +---------------------------+
                    |     MqttClient (handle)   |
                    +-------------+-------------+
                                  | commands
                                  v
+------------------+     +--------+--------+     +------------------+
|   Write task     |<----|  Connection /   |---->|   Read task      |
| encode + flush   |     |  coordinator    |     | decode+dispatch  |
+--------+---------+     | keepalive,      |     +--------+---------+
         |               | inflight, reconnect|            |
         v               +--------+--------+               v
   TCP/TLS socket <---------------+---------------> TCP/TLS socket
                                  |
                                  v
                    +---------------------------+
                    | InboundSink (injectable)  |
                    +---------------------------+
```

**Why split read/write?** A full handoff must not freeze PINGs. The read task may block on sink push; the write task still keep-alives and can send app publishes (within flow control). A full sink still delays *outbound* `PUBACK` / `PINGRESP` that sit behind unread frames. Inbound Receive Maximum bounds how far that can grow; control frames already decoded are handled before the next handoff wait.

| Term | Knob | Default | Role |
|------|------|--------:|------|
| **Cap** | `ConnectOptions::command_channel_capacity` | `32` | App → write-task admission; full → `AppError::Backpressure` |
| **Window** | app concurrency only | n/a | Concurrent QoS 1 `publish` / `start_publish` waits; not a library option |
| **Receive Maximum (client)** | `ConnectOptions::receive_maximum` | `32` | Inbound QoS 1 limit on `CONNECT` (`NonZeroU16`) |
| **Inflight (outbound)** | `CONNACK` server Receive Maximum + broker | broker | Outbound QoS 1 Packet-ID / `PUBACK` waits |

Do not confuse `DEFAULT_RECEIVE_MAXIMUM` (`32`, `ConnectOptions`) with codec `RECEIVE_MAX_DEFAULT` (`65535`, absent wire property).

## Installation

Base dependencies are shown in the [Quickstart](#quickstart). TLS: enable **exactly one** of `tls-rustls-aws-lc` (default) or `tls-rustls-ring`. Prefer pinning in dependents (`default-features = false, features = […]`). The client uses `ClientConfig::builder_with_provider` and does not rely on process-global rustls defaults. Do not enable both features and do not use `--all-features`.

```toml
mqtt-glide = { version = "0.1", default-features = false, features = ["tls-rustls-ring"] }
```

```shell
cargo test --lib
cargo test --no-default-features --features tls-rustls-ring --lib
```

## API

**Prefer convenience** (zero extra cost vs. the primitives underneath):

| Goal | API |
|------|-----|
| Connect | [`ClientBuilder`](src/builder.rs) — `.inbound_sink` / `.on_inbound` / `.discard_inbound` before `connect` |
| One publish | [`publish_with`](src/client.rs) → `.send().await` |
| Many publishes in flight | [`publish_window`](src/publish_window.rs) → `.push_retry().await` … `.drain().await` |
| Same-QoS burst | [`publish_batch`](src/client.rs) → one call (`push_retry` + `drain` under the hood) |
| Receive + workers | `.inbound_sink(flume/mpsc)` + your `recv` loop |
| Tiny sync inbound work | `.on_inbound(\|msg\| …)` (runs on the **read task** — keep it short) |

**Power** (same semantics, more control): [`connect`](src/client.rs), custom [`InboundSink`](src/inbound_sink.rs), [`publish`](src/client.rs) / [`start_publish`](src/client.rs) + [`PublishCompletion`](src/client.rs).

### Publish paths

| Method | Role | Notes |
|--------|------|-------|
| **`publish_with()`** | Convenience one-shot | `.send().await` (= `publish`). Type-state topic/payload; default QoS 0. Helpers: `.message_expiry(secs)`, `.user_property(name, value)`. `.properties(...)` replaces the whole struct — call it before those helpers. |
| **`publish_window(n)`** | Convenience pipeline | Sliding window over `start_publish` (no task per message). `.push()` = one attempt; `.push_retry()` yields on `Backpressure`. Always `.drain()` after the burst. |
| **`publish_batch(msgs, qos, retain, n)`** | Convenience same-QoS burst | Internal window of capacity `n`; all messages share `qos` / `retain`. Prefer `publish_window` for mixed QoS. |
| `publish(msg, qos, retain)` | Power one-shot | Ready [`PublishMessage`](src/codec/publish.rs). |
| `start_publish` + `PublishCompletion` | Power pipeline | Custom windowing / wait order. Full command channel → `AppError::Backpressure`. |

QoS 2 is rejected (`AppError::InvalidRequest`). Packet ID / `dup` are assigned by the driver.

### Inbound handoff

| API | When |
|-----|------|
| **`.inbound_sink(tx)`** | Normal apps — queue to workers (enqueue-before-ack) |
| **`.on_inbound(handler)`** | Inline on the read task: counters, filters, metrics, tiny sync work. **Not** for heavy processing (stalls decode/keepalive) |
| **`.discard_inbound()`** | Outbound-only / ignore inbound |
| Custom `InboundSink` | Special buffers, existing systems |

### Subscribe, unsubscribe, shutdown

| Method | Notes |
|--------|-------|
| `subscribe(filters)` | Per-filter `SUBACK` reason codes. Accepts `[("topic", QoS)]`, `[Subscription::new(...).no_local(true)]`, or a `Vec`. |
| `unsubscribe(filters)` | Per-filter `UNSUBACK` reason codes. Accepts `["topic"]` / `Vec` / any `IntoIterator`. |
| `shutdown()` | Graceful `DISCONNECT`, flush inbound `PUBACK`s, join driver tasks. |

Shared filters: build with `shared_filter(group, topic)` (validates ShareName).

### Hooks

| Trait / helper | Role |
|----------------|------|
| `CredentialsProvider` | Async `credentials()` before each `CONNECT`; `invalidate()` on CONNECT-level `NotAuthorized` only |
| `ReconnectPolicy` | `delay_after(outcome) -> Option<Duration>` (`None` = stop). Ready-made: `StandardReconnectPolicy` + `mark_live()` |
| `LifecycleHook` | Sync Connected / Disconnected side effects on the driver task — keep short (no await). Prefer the `SessionPhase` watch for readiness |
| `InboundSink` | Injectable inbound handoff; built-ins: `flume`, `tokio::mpsc`, `OnInboundSink` (`.on_inbound`). Custom sinks **MUST** be cancel-safe |

### Examples

Broader flows live under [`examples/`](examples/) (broker via `MQTT_GLIDE_BROKER`, default `mqtt://127.0.0.1:1883`). Prefer those over copying README snippets. Fluent APIs are the default path; power APIs appear only where they are clearly simpler or more flexible.

| Example | Shows |
|---------|--------|
| [`subscribe.rs`](examples/subscribe.rs) | `inbound_sink`, `Subscription` options, receive, `unsubscribe`, `shutdown` |
| [`on_inbound.rs`](examples/on_inbound.rs) | Inline `.on_inbound` handler (read-task; keep short) |
| [`publish.rs`](examples/publish.rs) | `publish_with`, `PublishMessage::new` + `publish`, `publish_batch`, Will Message, shared filter |
| [`pipeline.rs`](examples/pipeline.rs) | `publish_window` + fluent `.push_retry()` / `.drain()` (mixed topics) |
| [`request_response.rs`](examples/request_response.rs) | Properties R/R by hand (no RPC helper yet) |
| [`tls.rs`](examples/tls.rs) | `mqtts://`, rustls system roots, `connect_timeout` / `ack_timeout` |
| [`hooks.rs`](examples/hooks.rs) | `CredentialsProvider`, `LifecycleHook`, `mark_live()` |

```shell
cargo run --example subscribe
cargo run --example on_inbound
cargo run --example publish
cargo run --example pipeline
cargo run --example request_response
cargo run --example tls
cargo run --example hooks
```

### Defaults and semantics

| Knob | Default | Notes |
|------|--------:|-------|
| `ack_timeout` | 5 s | Shared `PUBACK` / `SUBACK` / `UNSUBACK` await |
| `connect_timeout` | 5 s | Shared TCP + TLS + `CONNACK` budget |
| `graceful_disconnect_timeout` | 2 s | Caps read-join + inbound `PUBACK` flush + `DISCONNECT` on `shutdown` |
| Packet IDs | n/a | Reserved until matching ACK or session teardown (no reap-on-cancel) |
| Inbound QoS 1 `PUBACK` | n/a | Only after successful `InboundSink` push; custom `send` **MUST** be cancel-safe |
| `SessionPhase` / lifecycle | n/a | Prefer hook **or** watch for side effects, not both |
| `CredentialsProvider::invalidate` | no-op | `CONNECT`-level `NotAuthorized` only (never `SUBACK` ACL) |

**`StandardReconnectPolicy::mark_live()`:** call once the session is fully ready (typically after subscriptions succeed). The next transport failure then retries immediately (`Duration::ZERO`) once, before falling back to the configured delay. Auth failures use the normal delay, then `max_auth_backoff` after a streak (`DEFAULT_AUTH_FAILURE_THRESHOLD`). Credential cache clearing is `CredentialsProvider::invalidate`, not this policy.

**Custom `InboundSink` cancel safety:** built-ins (`flume`, `tokio::mpsc`, `OnInboundSink`) are fine. If you implement `InboundSink`, dropping `send()` before `Ok(())` **MUST** mean the message was **not** accepted by your consumer. On shutdown the driver may drop a pending send and will withhold `PUBACK` in that case. `.on_inbound` runs on the connection read task — keep the handler short or use a channel sink.

Spec **SHOULD**s this client adopts: `CONNACK` within `connect_timeout`; `PINGRESP` watchdog (~1× Keep Alive) with handoff-stall exemption.

## Feature matrix

Legend: ✅ supported · ⚠️ partial / deferred · ❌ not implemented (open or out of scope)

| Area | Status | Notes |
|------|:------:|-------|
| MQTT 5 wire (`CONNECT`…`DISCONNECT`, QoS 0/1) | ✅ | MQTT 3.1.1 out of scope |
| Shared subscriptions (`$share/…`) | ✅ | Caller builds the filter; `shared_filter` validates ShareName |
| Topic filter wildcards | ✅ | Client validates `+` / `#` placement before encode |
| Publish properties (expiry, user props, `response_topic`, `correlation_data`) | ✅ | On `PublishProperties` |
| TLS (rustls + system roots) | ✅ | Feature-selected crypto provider |
| Reconnect / credentials hooks | ✅ | Traits + optional helpers |
| Enqueue-before-ack + `InboundSink` | ✅ | Product backpressure policy |
| Cap / Receive Maximum flow control | ✅ | See Cap vs Window above |
| Inbound Maximum Packet Size | ⚠️ | Unbounded: the client sends no CONNECT Maximum Packet Size, so a rogue broker could force large frame allocations. A client-side cap knob is a planned follow-up |
| Graceful `shutdown` | ✅ | Budget via `graceful_disconnect_timeout` |
| Subscription identifiers | ⚠️ | Deferred |
| Request/response *helper* / CONNECT negotiation | ⚠️ | Properties ✅ (`response_topic` / `correlation_data`); no inbox RPC API; `request_response_info` deferred |
| Will message (`CONNECT`) | ✅ | `.will(topic, payload)` (+ `.will_qos` / `.will_retain`); QoS 0\|1; power: `WillMessage` |
| Optional `SO_SNDBUF` / `SO_RCVBUF` | ⚠️ | OS defaults today |
| Enhanced AUTH | ❌ | |
| WebSockets | ❌ | TCP (+ TLS) only |
| Topic Alias | ❌ | Maximum = 0; inbound alias → protocol error (no alias state across reconnects) |
| QoS 2 | ❌ | Rejected at the API (no 4-way handshake / store-and-forward) |
| Persistent sessions / DUP redelivery | ❌ | No local store-and-forward; outbound `dup` always false. `clean_start = false` resumes broker-held sessions and re-subscribes when the broker reports `session_present = false` |

Broker-dependent: Message Expiry *drop after interval* (property round-trip is tested), shared-sub rebalancing under handoff stall, redelivery after unclean disconnect (client guarantees enqueue-before-ack only).

## Performance

Results: [`BENCHMARKS.md`](BENCHMARKS.md). How to run: [`benches/README.md`](benches/README.md).

**Host:** Apple MacBook Pro, M1 Max (8P+2E), 32 GB, macOS 26.5.2. **Baseline broker:** Mosquitto `2.1.2-alpine` (median-of-100).

Highlights at **256 B** (load: msg/s · CPU ns/msg; latency: p50/p99 µs). Compare only within the same ack cell. Prefer **QoS 1** / pipelined rows for Mosquitto cutover — outbound QoS 0 is mock-only (wire `WAIT`); see [`BENCHMARKS.md`](BENCHMARKS.md).

| Scenario | glide | rumqttc | mqtt5 |
|----------|------:|--------:|------:|
| Ingress load QoS 1 `after_enqueue` | **56 914** · **4 695** | 52 654 · 7 864 | n/a |
| Outbound pipelined QoS 1 | **101 179** · **5 541** | 86 440 · 9 407 | 61 848 · 30 199 |
| Outbound serial QoS 1 | **3 932** · 47 417 | 3 736 · 51 299 | 3 826 · **37 221** |
| Ingress serial QoS 1 `after_enqueue` | **312** / **488** | 314 / 846 | n/a |
| Outbound serial latency QoS 1 | 250 / **400** | 254 / 482 | **250** / 437 |

Mock broker numbers are a ceiling (see [`BENCHMARKS.md`](BENCHMARKS.md)); wire-only `encode_publish` is ~27 ns/msg (`codec_throughput`).

### Tuning for throughput

Defaults (`command_channel_capacity` / `receive_maximum` = **32**) favour many light connections. For sustained pipelines, raise the **public** knobs — there is no env-var tuning in the library (`MQTT_COMPARE_*` is bench-only).

| Goal | What to set |
|------|-------------|
| Outbound pipeline / many concurrent `publish` | `command_channel_capacity` ≥ your in-flight count; prefer [`publish_window`](src/publish_window.rs) (or your own window over `start_publish`) |
| Inbound QoS 1 under load | Larger `receive_maximum`; keep the `InboundSink` draining (full sink stalls decode and delays `PUBACK`) |
| Cutover-style compare | Same receive window / request capacity as peers; measure **pipelined** or **concurrent** QoS 1, not serial RTT |

Do not confuse Cap (app → write admission) with MQTT Receive Maximum (inbound QoS 1 window) or CONNACK server Receive Maximum (outbound QoS 1 inflight). Internal write coalesce / soft-flush sizes are fixed implementation details, not `ConnectOptions`.

```shell
# Mosquitto via Podman or Docker (median-of-100 baseline)
MQTT_COMPARE_RUNS=100 cargo bench --bench mqtt_compare

# Quick smoke (no container)
MQTT_COMPARE_BROKER=mock MQTT_COMPARE_RUNS=1 cargo bench --bench mqtt_compare
```

## License

Licensed under either of

- Apache License, Version 2.0 ([`LICENSE-APACHE`](LICENSE-APACHE) or https://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([`LICENSE-MIT`](LICENSE-MIT) or https://opensource.org/licenses/MIT)

at your option.
