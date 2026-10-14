# mqtt-glide

[![crates.io](https://img.shields.io/crates/v/mqtt-glide.svg)](https://crates.io/crates/mqtt-glide)
[![docs.rs](https://docs.rs/mqtt-glide/badge.svg)](https://docs.rs/mqtt-glide)
[![license: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

An MQTT 5 client for Tokio. You connect once, get a cloneable handle, and publish and receive from your own tasks. There is no event loop your application has to poll.

Inbound messages go to a sink you provide: a channel, or a closure that runs inline. For QoS 1, mqtt-glide sends `PUBACK` only after that sink has accepted the message, so a message the broker considers delivered is already in your queue, not in flight somewhere inside the client.

MQTT 5 only, QoS 0 and 1, over TCP, TLS or (opt-in) WebSocket. No MQTT 3.1.1, no QoS 2, `no_std`, or business logic; see the [feature matrix](#feature-matrix) before you adopt it.

## Install

```toml
[dependencies]
mqtt-glide = "0.2"
tokio = { version = "1", features = ["rt-multi-thread", "macros", "net", "time"] }
flume = "0.12"   # optional; any InboundSink works
```

TLS goes through rustls. Enable **exactly one** crypto provider: `tls-rustls-aws-lc` (default) or `tls-rustls-ring`. Prefer pinning the choice in dependents. The client builds its `ClientConfig` with `builder_with_provider` and does not rely on a process-global rustls default, so enabling both features (or `--all-features`) is rejected.

```toml
mqtt-glide = { version = "0.2", default-features = false, features = ["tls-rustls-ring"] }
```

MQTT over WebSocket (for networks that only let HTTP(S) out, or brokers behind an HTTP ingress) is behind the optional `websocket` feature; it adds `tokio-tungstenite` for the handshake and framing only:

```toml
mqtt-glide = { version = "0.2", features = ["websocket"] }
```

Then connect with `ws://broker:8080/mqtt` or `wss://broker/mqtt`.

## Quickstart

Connect, publish once, shut down. No event loop to drive:

```rust,no_run
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use mqtt_glide::{ClientBuilder, QoS};

    let client = ClientBuilder::new("mqtt://127.0.0.1:1883")?
        .discard_inbound()
        .connect()
        .await?;

    client
        .publish("sensors/room1/temp", "22.5")
        .qos(QoS::AtLeastOnce)
        .send()
        .await?;

    client.shutdown().await?;
    Ok(())
}
```

Receiving is the mirror image: hand the client a channel sender, then read the receiver wherever you like.

```rust,no_run
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use mqtt_glide::{ClientBuilder, InboundMessage, QoS};

    let (tx, mut rx) = tokio::sync::mpsc::channel::<InboundMessage>(1024);

    let client = ClientBuilder::new("mqtt://127.0.0.1:1883")?
        .inbound_sink(tx)
        .connect()
        .await?;

    client.subscribe([("sensors/+/temp", QoS::AtLeastOnce)]).await?;

    // For QoS 1, PUBACK goes out after this channel accepted the message,
    // not when it was decoded off the socket.
    while let Some(msg) = rx.recv().await {
        println!("{}: {} bytes", msg.topic, msg.payload.len());
    }

    client.shutdown().await?;
    Ok(())
}
```

If the channel is full, the read task waits and the broker stops sending. That is backpressure working, not a stall. Nothing in your code has to be polled for the connection to stay alive.

`connect()` spawns the connection driver; the cloneable `MqttClient` handle is all your code keeps. Runnable versions of both flows live under [`examples/`](examples/); the tables further down are the reference for knobs and semantics.

## What makes it different

### What happens to an inbound QoS 1 message

1. The read task decodes the frame off the socket.
2. It pushes the message into your `InboundSink`, waiting if the sink is full.
3. Only once the sink has accepted it does the write task send `PUBACK`.
4. Your workers take it off the other end and do the work.

Step 3 after step 2 is the whole point, and the rest of this document calls it **enqueue-before-ack**. A client that acks at step 1 tells the broker "delivered" while the message still only exists inside the client. If the process dies there, the broker has already moved on. Here the broker keeps ownership until your queue has the message. The guarantee ends there: once the handoff succeeded, the message belongs to your application, and what happens to it afterwards is your responsibility.

### One handoff instead of two queues

Decoded messages go directly into your `InboundSink`: a `flume` or `tokio::sync::mpsc` sender, an inline handler, or your own implementation. There is no library-owned inbound queue in front of it to size, drain, or reason about.

### A slow consumer costs throughput, not the session

Read and write run as separate tasks on a split socket. While the read task waits on a full sink, the write task keeps sending `PINGREQ`, and the keep-alive watchdog knows the difference between "the broker is gone" and "we are busy". A backed-up consumer does not get you disconnected. It is not free: outbound `PUBACK`s wait until the sink accepts, and inbound frames (including `PINGRESP`) sit unread in the socket buffer while the read task is blocked; `receive_maximum` bounds how far behind it can fall.

| Concern | Approach |
|---------|----------|
| App API | Cloneable `MqttClient` (no polling) |
| I/O | Separate read and write tasks on a split socket |
| Inbound | Injectable `InboundSink` (e.g. `flume` / `tokio::mpsc`) |
| QoS 1 ingress | enqueue-before-ack: `PUBACK` only after the sink accepts |
| Keep-alive under load | Write keeps sending `PINGREQ` while read waits on a full sink; liveness treats that stall as self-induced |
| Shutdown | Graceful drain, then `DISCONNECT` |

## Design goals

- Clean drain: graceful shutdown without reaching into crate internals, and no "acked but not forwarded" window.
- Spec-backed: every normative statement of [OASIS MQTT 5.0](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html) is listed in [`CONFORMANCE.md`](CONFORMANCE.md) with its scope for a client and the tests that check it. The page is generated from the `[MQTT-x.y.z-n]` references in the tests, and CI fails when it goes stale.
- Fuzzed: the codec is fuzzed with arbitrary broker input in arbitrary read sizes ([`fuzz/`](fuzz/README.md)).
- Auth stays outside: a `CredentialsProvider` hook is the whole story. No OAuth or token manager lives in this crate.

Non-goals are deliberate rather than unfinished; the [feature matrix](#feature-matrix) lists them.

## Coming from rumqttc?

Three things change in your code.

### No `eventloop.poll()`

In a polled client the event loop is yours to drive: connection progress, keep-alive, and incoming publishes all happen as a side effect of your poll call. Here `connect()` spawns the driver and it owns the socket; your code only keeps `MqttClient` handles, which are cheap to clone.

### One queue instead of two

A polled loop hands you notifications that you forward into your own channel: two queues, two capacities, two places to get backpressure wrong. You pass your channel in at connect time instead, and the driver writes into it.

### Backpressure is a policy, not an accident

When a full application queue blocks the same loop that sends `PINGREQ`, a slow consumer can end the session. With read and write split, a slow consumer slows down reading only.

| In rumqttc | Here |
|------------|------|
| `EventLoop::poll()` in a task you own | Driver task spawned by `connect()` |
| `Event::Incoming(Packet::Publish(..))` from the poll loop | Messages arrive in the `InboundSink` you injected |
| `manual_acks` plus an explicit `ack(..)` call | `PUBACK` follows sink acceptance; nothing to call |
| Connection state inferred from polled events | `LifecycleHook`, or the `SessionPhase` watch (`client.session_phase()`) |
| `publish(..)` resolves when the request is queued for the event loop | `publish(..)` resolves when the frame is encoded into the write buffer (QoS 0; not necessarily flushed) or when `PUBACK` arrives (QoS 1) |
| A refused publish shows up as a polled `PubAck` you must match yourself | `AppError::PublishRejected` with the reason code, Reason String and User Properties |
| Request/response: match your own correlation data in the poll loop | `client.requester(topic)` then `request(..).await` |

### What it costs

No MQTT 3.1.1 and no QoS 2: both are out of scope, not pending. If you need either, rumqttc remains the better fit. Unacknowledged QoS 1 publishes are kept in memory only: they are resent after a reconnect into a resumed session (see [No session](#no-session)), but not after a process restart. Cross-process session resume needs a stable client ID (`.client_id("…")?` or `ClientId::try_new`) plus `clean_start(false)` and a non-zero `session_expiry`. rumqttc is also the far more widely deployed crate; this one is 0.2.x, and the API can still move.

## Architecture

`connect()` gives you a handle and starts one driver over a single socket:

1. `MqttClient`: a cheap cloneable handle. It sends commands and does no socket I/O.
2. Write task: encodes `PUBLISH` / `SUBSCRIBE` / acks / `PINGREQ`, and puts inbound `PUBACK`s ahead of your publishes.
3. Read task: decodes frames, answers control packets itself, and passes application `PUBLISH` to your sink. Reconnect and credential refresh happen here, not in your code.

```text
                 +---------------------------+
                 |   MqttClient (handle)     |  clone it, share it
                 +-------------+-------------+
                               | commands
                               v
        +----------------------+----------------------+
        |              connection driver              |
        |     keep-alive · reconnect · credentials    |
        +----------+----------------------+-----------+
                   |                      |
              write task              read task
            encode + flush         decode + route
                   |                      |
                   |                      +---> InboundSink (your queue)
                   |                      |
                   +-----------+----------+
                               |
                  one TCP/TLS socket (split)
```

Why split read and write? A stalled handoff must not freeze keep-alive. The read task can sit on a sink push while the write task keeps sending `PINGREQ` and your publishes. A full sink still delays outbound `PUBACK`s (they wait on sink acceptance), and inbound frames sit unread in the socket buffer; the inbound receive window bounds how far that can grow, and control frames that were already decoded are handled before the next handoff wait.

## Defaults worth changing before production

The builder defaults are chosen for a predictable first run, not for a long-lived service. Four of them will surprise you:

### No reconnect

The default policy is `NoReconnect`: one connect attempt, and a dropped connection stays dropped. Pass `.reconnect(StandardReconnectPolicy::new(delay, max_auth_backoff))` or your own `ReconnectPolicy` if the process should outlive a broker restart.

### A new client ID every process (by default)

Without an explicit `client_id`, the builder mints a fresh UUID (`ClientId::from_prefix("glide")`). That ID is reused for reconnects inside the same process. For broker-side session resume across restarts, set a stable ID with `.client_id("my-service")?` (or `ConnectOptions::client_id(ClientId::try_new(...))`) together with `clean_start(false)` and a non-zero `session_expiry`.

### No session

`clean_start` is `true` and `session_expiry` is unset (property omitted; MQTT default 0 s), so the broker drops your subscriptions and anything queued for you as soon as the connection ends. Resuming within the process needs `clean_start(false)` plus a non-zero `session_expiry`; re-subscribe whenever CONNACK reports `session_present = false`.

What happens to your publishes when the connection drops and the client reconnects:

| Publish state at the drop | Clean Start (`true`) | Resumed session (`clean_start(false)`, `session_expiry` > 0) |
|---|---|---|
| Never sent (parked behind Receive Maximum, or still queued) | sent on the next connection | sent on the next connection |
| QoS 1 sent, `PUBACK` missing | fails with `AppError::ConnectionLost` | resent in order with its original packet id and `DUP` set ([MQTT-4.4.0-1]); fails with `ConnectionLost` if CONNACK reports no session |

A caller that stops waiting earlier (its `ack_timeout` ran out during a long reconnect backoff) gets `PublishAckTimeout`, but the publish is not withdrawn: a never-sent one is dropped, an unacknowledged one in a resumed session is still resent. Retrying on that timeout can therefore deliver twice, as usual for QoS 1. Keep `ack_timeout` above your reconnect delay, or wait on a `PublishCompletion` yourself, if that matters.

### Capacities sized for a few connections, not a pipeline

`command_channel_capacity` and `receive_maximum` are both `32`. See [Flow control](#flow-control).

| Setting | Default | Change it when |
|---------|--------:|----------------|
| reconnect policy | `NoReconnect` | the process must outlive a broker restart |
| `client_id` | random UUID | you need session resume across restarts (`.client_id("…")?`) |
| `clean_start` / `session_expiry` | `true` / unset | same (needs the stable `client_id` too) |
| `topic_alias_maximum` / `outbound_topic_aliases` | `0` / `0` | long topics published or received often: aliases replace the topic bytes after the first message per connection |
| `fail_fast_when_disconnected` | `false` (queue) | a publish that cannot go out now is worthless, or you buffer yourself: commands then fail with `AppError::NotConnected` during reconnects; check `client.is_connected()` for a snapshot |
| `command_channel_capacity` | `32` | you keep more than ~32 publishes in flight |
| `receive_maximum` | `32` | inbound QoS 1 rate is high and workers are fast |
| `keep_alive` | 60 s | your broker or NAT expects otherwise |
| `connect_timeout` / `ack_timeout` | 5 s / 5 s | links are slow or the broker is far |
| `graceful_disconnect_timeout` | 2 s | shutdown must flush a large backlog |
| `max_packet_size` (inbound) | 1 MiB | you accept larger payloads |
| `wait_connected` | `true` | you want `connect()` to return before CONNACK |

See [`ConnectOptions`](https://docs.rs/mqtt-glide/latest/mqtt_glide/options/struct.ConnectOptions.html) for the full set.

## Flow control

Three limits, easy to confuse, only two of them yours to set.

### Command channel capacity

`ConnectOptions::command_channel_capacity`, default `32`: how many commands may queue up for the write task. When it is full, `publish` / `send` / `subscribe` / `unsubscribe` and `PublishWindow::push` wait up to `ack_timeout` for room (then `AppError::Backpressure`), while `start_publish` fails fast with `AppError::Backpressure`. Set it to at least the number of publishes you keep in flight.

### Receive Maximum

`ConnectOptions::receive_maximum`, default `32`: how many inbound QoS 1 messages the broker may leave unacknowledged with this client. This is the knob that bounds memory when your sink is slow.

### Outbound inflight

Not yours: the broker's Receive Maximum from `CONNACK` decides how many of your QoS 1 publishes can await `PUBACK` at once. Publishes beyond that window wait in order inside the write task (up to 32, then the command channel fills) and go out as `PUBACK`s free slots. They do not fail, and subscribe / unsubscribe are not held up behind them.

How many publishes you have in flight is an application decision, not a library option. That is what [`publish_window`](https://docs.rs/mqtt-glide/latest/mqtt_glide/publish_window/struct.PublishWindow.html) gives you.

Do not confuse `DEFAULT_RECEIVE_MAXIMUM` (`32`, on `ConnectOptions`) with the codec's `RECEIVE_MAX_DEFAULT` (`65535`, the value assumed when the wire property is absent). Internal write coalesce / soft-flush sizes are fixed; there is no env-var tuning in the library (`MQTT_COMPARE_*` is bench-only).

| Goal | What to set |
|------|-------------|
| Outbound pipeline / many concurrent `publish` | `command_channel_capacity` ≥ your in-flight count; prefer [`publish_window`](https://docs.rs/mqtt-glide/latest/mqtt_glide/publish_window/struct.PublishWindow.html) (or your own window over `start_publish`) |
| Inbound QoS 1 under load | Larger `receive_maximum`; keep the `InboundSink` draining (a full sink stalls decode and delays `PUBACK`) |
| Comparing against another client | Same receive window and request capacity as the peer; measure pipelined or concurrent QoS 1, not serial round-trips |

## API reference

The tables below are the lookup surface: which call to reach for, what it guarantees, and which defaults apply. Full signatures live on [docs.rs](https://docs.rs/mqtt-glide).

Prefer convenience (zero extra cost vs. the primitives underneath):

| Goal | API |
|------|-----|
| Connect | [`ClientBuilder`](https://docs.rs/mqtt-glide/latest/mqtt_glide/builder/struct.ClientBuilder.html): `.inbound_sink` / `.on_inbound` / `.discard_inbound` before `connect` |
| One publish | [`publish`](https://docs.rs/mqtt-glide/latest/mqtt_glide/client/struct.MqttClient.html#method.publish) → `.await` (or `.send().await`) |
| Many publishes in flight | [`publish_window`](https://docs.rs/mqtt-glide/latest/mqtt_glide/publish_window/struct.PublishWindow.html) → `.push(Publish::new(..)).await` … `.drain().await` |
| Burst | [`publish_batch`](https://docs.rs/mqtt-glide/latest/mqtt_glide/client/struct.MqttClient.html#method.publish_batch) → one call (`push` + `drain` under the hood) |
| Subscribe | [`subscribe`](https://docs.rs/mqtt-glide/latest/mqtt_glide/client/struct.MqttClient.html#method.subscribe) / `subscribe_with_id` → per-filter reason codes plus the broker's Reason String and User Properties |
| Request / response | `client.requester(response_topic).await?` → `.request(topic, payload).await` resolves with the response |
| Receive + workers | `.inbound_sink(flume/mpsc)` + your `recv` loop |
| Tiny sync inbound work | `.on_inbound(\|msg\| …)` (runs on the read task; keep it short) |

Power (same semantics, more control): [`connect`](https://docs.rs/mqtt-glide/latest/mqtt_glide/client/fn.connect.html), custom [`InboundSink`](https://docs.rs/mqtt-glide/latest/mqtt_glide/inbound_sink/trait.InboundSink.html), [`send`](https://docs.rs/mqtt-glide/latest/mqtt_glide/client/struct.MqttClient.html#method.send) / [`start_publish`](https://docs.rs/mqtt-glide/latest/mqtt_glide/client/struct.MqttClient.html#method.start_publish) + [`PublishCompletion`](https://docs.rs/mqtt-glide/latest/mqtt_glide/client/struct.PublishCompletion.html).

### Publish paths

| Method | Role | Notes |
|--------|------|-------|
| `publish(topic, payload)` | Convenience one-shot | `.await` or `.send().await`; `.start()` enqueues without waiting. Default QoS 0. Setters: `.qos()`, `.retain()`, `.message_expiry(secs)`, `.user_property(name, value)` / `.user_properties(iter)`, `.content_type()`, `.response_topic()`, `.correlation_data()`. `.properties(...)` replaces the whole struct; call it before the other property setters. |
| `publish_window(n)` | Convenience pipeline | Sliding window over `enqueue_publish` (no task per message). `.push(impl Into<Publish>)` waits for command-channel room. Always `.drain()` after the burst. |
| `publish_batch(msgs, n)` | Convenience burst | Internal window of capacity `n`; each message is an `impl Into<Publish>` carrying its own QoS / retain. |
| `send(publish)` | Power one-shot | Ready [`Publish`](https://docs.rs/mqtt-glide/latest/mqtt_glide/publish/struct.Publish.html) value (also converts from a `PublishMessage`). |
| `start_publish` + `PublishCompletion` | Power pipeline | Custom windowing / wait order. Full command channel → immediate `AppError::Backpressure`. `enqueue_publish` waits for room instead. |
| `client.requester(topic)` → `request(topic, payload)` | Request / response | Subscribes once to `topic` (QoS 1, restored on reconnect); each request carries it as Response Topic with fresh Correlation Data and resolves with the matching `InboundMessage`. Setters: `.qos()`, `.timeout()` (default 10 s, then `AppError::ResponseTimeout`), `.user_property()`, `.content_type()`, `.message_expiry()`. Responses never reach the inbound sink. |
| `Publish::reply_to(&request, payload)` | Answering side | `Some(Publish)` addressed to the request's Response Topic with its Correlation Data copied; `None` if the request carried no Response Topic. |

QoS 2 is rejected (`AppError::InvalidRequest`). Packet ID / `dup` are assigned by the driver. A QoS 1 publish the broker refuses (PUBACK reason 0x80+) fails with `AppError::PublishRejected(PublishRejection)`: `reason`, `reason_string` and `user_properties` as the broker sent them.

### Inbound handoff

| API | When |
|-----|------|
| `.inbound_sink(tx)` | Normal apps: queue to workers (enqueue-before-ack) |
| `.on_inbound(handler)` | Inline on the read task: counters, filters, metrics, tiny sync work. Not for heavy processing (stalls decode/keepalive) |
| `.discard_inbound()` | Outbound-only / ignore inbound |
| Custom `InboundSink` | Special buffers, existing systems |

Each delivery is an `InboundMessage`: `topic`, `payload`, `properties`, plus `retain` (a retained message the broker sent because you subscribed, as opposed to a live one), `dup` (possible QoS 1 redelivery, e.g. after a reconnect into a resumed session) the delivered `qos`, and `subscription_ids` (the identifiers of the `subscribe_with_id` subscriptions it matched; empty otherwise). `Publish::from(msg)` republishes one.

### Subscribe, unsubscribe, shutdown

| Method | Notes |
|--------|-------|
| `subscribe(filters)` | `SubscribeResponse`: per-filter `SUBACK` reason codes (`reasons`, also via deref / `as_slice`) plus `reason_string` and `user_properties`. Accepts `[("topic", QoS)]`, `[Subscription::new(...).no_local(true)]`, or a `Vec`. QoS 2 filters are rejected (`InvalidRequest`). |
| `subscribe_with_id(id, filters)` | Same, tagging the filters with a Subscription Identifier (`1..=268_435_455`); matching inbound messages report it in `subscription_ids`. Kept across reconnects; `InvalidRequest` if the broker announced no support. |
| `unsubscribe(filters)` | `UnsubscribeResponse`: per-filter `UNSUBACK` reason codes plus `reason_string` and `user_properties`. Accepts `["topic"]` / `Vec` / any `IntoIterator`. |
| `shutdown()` | Graceful drain, then `DISCONNECT`: queued commands are sent, outbound QoS 1 waits for its `PUBACK`s, inbound keeps being delivered and acknowledged meanwhile. New commands fail with `AppError::ShuttingDown`. Bounded by `graceful_disconnect_timeout`; `shutdown()` itself returns once shutdown is requested. |

Shared filters: build with `shared_filter(group, topic)` (validates ShareName).

Granted subscriptions are remembered and restored on reconnect whenever the broker holds no session for the client (Clean Start, or CONNACK without Session Present), **before** `SessionPhase::Connected` is published. After a reconnect, `Connected` therefore means "connected and subscribed again"; on the first connect it only means "connected", and readiness is your own `subscribe` returning granted reason codes. If the broker refuses a restored filter (for example after an ACL change), the connection counts as failed and the reconnect policy decides; there is never a `Connected` with only part of the subscriptions back.

### Hooks

| Trait / helper | Role |
|----------------|------|
| `CredentialsProvider` | Async `credentials()` before each `CONNECT`; `invalidate()` on CONNECT-level `NotAuthorized` only |
| `ReconnectPolicy` | `delay_after(outcome) -> Option<Duration>` (`None` = stop). The outcome of a broker `DISCONNECT` or refused `CONNACK` includes the broker's Reason String (and Server Reference). Ready-made: `StandardReconnectPolicy` + `mark_live()` |
| `LifecycleHook` | Sync `Connected` / `Disconnected` side effects on the driver task; keep short (no await). `ClientBuilder` already waits for the first `Connected`. For async waits use the [`SessionPhase`](https://docs.rs/mqtt-glide/latest/mqtt_glide/connection/enum.SessionPhase.html) watch (`client.session_phase()`, any client) and `client.is_connected()` for a snapshot |
| `InboundSink` | Injectable inbound handoff; built-ins: `flume`, `tokio::mpsc`, `OnInboundSink` (`.on_inbound`). Custom sinks **MUST** be cancel-safe |

### Examples

Broader flows live under [`examples/`](examples/) and are the better starting point than copying README snippets. Run any of them with `cargo run --example <name>`; they connect to `MQTT_GLIDE_BROKER` (default `mqtt://127.0.0.1:1883`).

| Example | Shows |
|---------|--------|
| [`subscribe.rs`](examples/subscribe.rs) | `inbound_sink`, `Subscription` options, receive, `unsubscribe`, `shutdown` |
| [`on_inbound.rs`](examples/on_inbound.rs) | Inline `.on_inbound` handler (read-task; keep short) |
| [`publish.rs`](examples/publish.rs) | `publish`, `Publish::new` + `send`, `publish_batch`, Will Message, shared filter |
| [`pipeline.rs`](examples/pipeline.rs) | `publish_window` + `.push(Publish::new(..))` / `.drain()` (mixed topics) |
| [`request_response.rs`](examples/request_response.rs) | `client.requester(topic)` + `request(..).await`, answering with `Publish::reply_to` |
| [`tls.rs`](examples/tls.rs) | `mqtts://`, rustls system roots, `connect_timeout` / `ack_timeout` |
| [`hooks.rs`](examples/hooks.rs) | `CredentialsProvider`, `LifecycleHook`, `mark_live()` |

### Defaults and semantics

| Knob | Default | Notes |
|------|--------:|-------|
| `ack_timeout` | 5 s | Shared `PUBACK` / `SUBACK` / `UNSUBACK` await |
| Request timeout | 10 s | `DEFAULT_REQUEST_TIMEOUT`; per request via `.timeout(..)` |
| `connect_timeout` | 5 s | Shared TCP + TLS + `CONNACK` budget |
| `graceful_disconnect_timeout` | 2 s | Caps the whole `shutdown` drain: queued sends, outbound `PUBACK` wait, inbound delivery + `PUBACK` flush, `DISCONNECT` |
| Packet IDs | n/a | Reserved until matching ACK or session teardown (no reap-on-cancel) |
| Inbound QoS 1 `PUBACK` | n/a | Only after successful `InboundSink` push; custom `send` **MUST** be cancel-safe |
| Readiness | n/a | `ClientBuilder::connect` waits for the first `Connected` (`wait_connected`, default `true`); the low-level `connect` returns a `SessionPhase` watch instead. Use the hook or the watch for side effects, not both |
| `CredentialsProvider::invalidate` | no-op | `CONNECT`-level `NotAuthorized` only (never `SUBACK` ACL) |

`StandardReconnectPolicy::mark_live()`: call once the session is fully ready (typically after subscriptions succeed). The next transport failure then retries immediately (`Duration::ZERO`) once, before falling back to the configured delay. Auth failures use the normal delay, then `max_auth_backoff` after a streak (`DEFAULT_AUTH_FAILURE_THRESHOLD`). Credential cache clearing is `CredentialsProvider::invalidate`, not this policy.

Custom `InboundSink` cancel safety: built-ins (`flume`, `tokio::mpsc`, `OnInboundSink`) are fine. If you implement `InboundSink`, dropping `send()` before `Ok(())` **MUST** mean the message was not accepted by your consumer. On shutdown the driver may drop a pending send and will withhold `PUBACK` in that case. `.on_inbound` runs on the connection read task; keep the handler short or use a channel sink.

Spec **SHOULD**s this client adopts: `CONNACK` within `connect_timeout`; `PINGRESP` watchdog (~1× Keep Alive) with handoff-stall exemption.

## Feature matrix

The authoritative answer to "does it do X?", including the things it deliberately does not do.

Legend: ✅ supported · ⚠️ partial / deferred · ❌ not implemented (open or out of scope)

| Area | Status | Notes |
|------|:------:|-------|
| MQTT 5 wire (`CONNECT`…`DISCONNECT`, QoS 0/1) | ✅ | MQTT 3.1.1 out of scope |
| Shared subscriptions (`$share/…`) | ✅ | Caller builds the filter; `shared_filter` validates ShareName |
| Topic filter wildcards | ✅ | Client validates `+` / `#` placement before encode |
| Publish properties (expiry, user props, content type, `response_topic`, `correlation_data`) | ✅ | On `PublishProperties`; setters on `Publish` |
| TLS (rustls + system roots) | ✅ | Feature-selected crypto provider |
| Reconnect / credentials hooks | ✅ | Traits + optional helpers |
| Enqueue-before-ack + `InboundSink` | ✅ | Product backpressure policy |
| Cap / Receive Maximum flow control | ✅ | See [Flow control](#flow-control) |
| Inbound Maximum Packet Size | ✅ | CONNECT advertises [`DEFAULT_MAX_PACKET_SIZE`](https://docs.rs/mqtt-glide/latest/mqtt_glide/codec/constant.DEFAULT_MAX_PACKET_SIZE.html) (1 MiB) by default; override via `ConnectOptions::max_packet_size`. Broker CONNACK limit applies outbound only |
| Graceful `shutdown` | ✅ | Budget via `graceful_disconnect_timeout` |
| Subscription identifiers | ✅ | `subscribe_with_id(id, filters)`; inbound `InboundMessage::subscription_ids`; kept across resubscribe; refused when CONNACK says unsupported |
| Request/response | ✅ | `client.requester(response_topic)` → `request(topic, payload).await` resolves with the response (correlation handled, responses bypass the inbound sink, `ResponseTimeout`); `Publish::reply_to(&request, payload)` on the answering side. CONNECT Request Response Information not requested |
| Will message (`CONNECT`) | ✅ | `.will(topic, payload)` (+ `.will_qos` / `.will_retain`); QoS 0\|1; power: `WillMessage` |
| Optional `SO_SNDBUF` / `SO_RCVBUF` | ⚠️ | OS defaults today |
| Broker diagnostics (Reason String, User Properties) | ✅ | Passed through on `PUBACK` rejections (`PublishRejection`), `SUBACK` / `UNSUBACK` (`SubscribeResponse` / `UnsubscribeResponse`), and in the outcome of a broker `DISCONNECT` / refused `CONNACK`. The client's own outbound `DISCONNECT` carries no reason string |
| Enhanced AUTH (`AUTH` packets, re-authentication) | ❌ | Not implemented; `CredentialsProvider` covers username/password only |
| WebSockets | ✅ opt-in | `websocket` feature: `ws://host[:port]/path` / `wss://…` (subprotocol `mqtt`, binary frames, §6). `wss` uses the same rustls setup as `mqtts`. Without the feature such URLs fail to parse |
| Topic Alias | ✅ opt-in | Inbound: `topic_alias_maximum(n)`, resolved before delivery. Outbound: `outbound_topic_aliases(n)`, capped by the broker's maximum, LRU rebinding. Per connection, reset on reconnect |
| QoS 2 | ❌ | Rejected at the API (no 4-way handshake / store-and-forward) |
| Persistent sessions / DUP redelivery | ✅ in process | Resumed sessions resend unacknowledged QoS 1 with `DUP` and original ids ([MQTT-4.4.0-1]); state is in memory, not persisted across restarts. Needs a stable client ID (`.client_id("…")?` / `ClientId::try_new`; `ClientId::from_prefix` always mints a fresh UUID), `clean_start(false)` and a non-zero `session_expiry` |

Broker-dependent: Message Expiry *drop after interval* (property round-trip is tested), shared-sub rebalancing under handoff stall, redelivery after unclean disconnect (client guarantees enqueue-before-ack only).

## Performance

All results and the reading guide: [`BENCHMARKS.md`](BENCHMARKS.md). How to run them yourself: [`benches/README.md`](benches/README.md).

Headline at 256 B, QoS 1, tuned Mosquitto `2.1.2-alpine`, median of 5 runs, 50 000-message bursts, on an Intel Core Ultra 7 258V with the client pinned to 2 P-cores (msg/s · CPU ns/msg; **bold** = best in row):

| Scenario | glide (main) | rumqttc-v5-next 0.33.3 | mqtt5 0.48.0 |
|----------|------:|--------:|------:|
| 64 callers sharing one client | **293 160** · **3 600** | 206 252 · 4 782 | 171 652 · 7 590 |
| 512 callers sharing one client | **316 591** · **3 430** | 220 438 · 4 540 | 215 616 · 6 810 |
| Pipelined, window 64 | **298 327** · **3 691** | 216 274 · 4 527 | 180 156 · 8 513 |

These are absolute numbers on one host and broker setup, not paired comparisons. Against a stock-limit Mosquitto (Receive Maximum 20) glide and mqtt5 lose throughput at high concurrency, and peers lead some cells (for example idle QoS 0 outbound latency and QoS 0 ingress load on the mock broker, and mqtt5's idle QoS 1 latency on tuned Mosquitto). rumqttc is pinned to 0.33.3 because 0.34.0 scans its whole outgoing table on every flush; against a broker without a Receive Maximum that table has 65 535 slots and throughput falls to a few thousand msg/s. Per-publish p50/p99/p99.9 latency, a task-per-message burst, fixed-rate load, ingress, and the pinning and fairness details are in [`BENCHMARKS.md`](BENCHMARKS.md). Wire-only `encode_publish` costs about 27 ns/msg (`codec_throughput`).

Throughput knobs live under [Flow control](#flow-control); the `MQTT_COMPARE_*` variables configure the bench harness and have no effect on the library.

## License

Licensed under either of

- Apache License, Version 2.0 ([`LICENSE-APACHE`](LICENSE-APACHE) or https://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([`LICENSE-MIT`](LICENSE-MIT) or https://opensource.org/licenses/MIT)

at your option.
