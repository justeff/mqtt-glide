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
mqtt-glide = "0.3"
tokio = { version = "1", features = ["rt-multi-thread", "macros", "net", "time"] }
```

TLS goes through rustls. Enable **exactly one** crypto provider: `tls-rustls-aws-lc` (default) or `tls-rustls-ring`. Prefer pinning the choice in dependents. The client builds its `ClientConfig` with `builder_with_provider` and does not rely on a process-global rustls default, so enabling both features (or `--all-features`) is rejected.

```toml
mqtt-glide = { version = "0.3", default-features = false, features = ["tls-rustls-ring"] }
```

MQTT over WebSocket (for networks that only let HTTP(S) out, or brokers behind an HTTP ingress) is behind the optional `websocket` feature; it adds `tokio-tungstenite` for the handshake and framing only:

```toml
mqtt-glide = { version = "0.3", features = ["websocket"] }
```

Then connect with `ws://broker:8080/mqtt` or `wss://broker/mqtt`.

Minimum supported Rust version: **1.88**. It is raised only in a minor release (before 1.0: a `0.x` bump), never in a patch release, and every raise is listed in the changelog.

Inbound messages go into any `InboundSink`; `tokio::sync::mpsc::Sender` works out of the box. To hand them to a `flume::Sender` instead, enable the `flume` feature. It ties your build to flume 0.12, so a flume upgrade in mqtt-glide is a breaking change for you:

```toml
mqtt-glide = { version = "0.3", features = ["flume"] }
flume = "0.12"
```

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

Decoded messages go directly into your `InboundSink`: a `tokio::sync::mpsc` sender (or `flume` with the feature), an inline handler, or your own implementation. There is no library-owned inbound queue in front of it to size, drain, or reason about.

### A slow consumer costs throughput, not the session

Read and write run as separate tasks on a split socket. While the read task waits on a full sink, the write task keeps sending `PINGREQ`, and the keep-alive watchdog knows the difference between "the broker is gone" and "we are busy". A backed-up consumer does not get you disconnected. It is not free: outbound `PUBACK`s wait until the sink accepts, and inbound frames (including `PINGRESP`) sit unread in the socket buffer while the read task is blocked; `receive_maximum` bounds how far behind it can fall.

| Concern | Approach |
|---------|----------|
| App API | Cloneable `MqttClient` (no polling) |
| I/O | Separate read and write tasks on a split socket |
| Inbound | Injectable `InboundSink` (e.g. `tokio::mpsc`, `flume` with the feature) |
| QoS 1 ingress | enqueue-before-ack: `PUBACK` only after the sink accepts |
| Keep-alive under load | Write keeps sending `PINGREQ` while read waits on a full sink; liveness treats that stall as self-induced |
| Shutdown | Graceful drain, then `DISCONNECT` |
| Testing your service | In-process test broker and fault proxy (`testing` feature) |

### Your MQTT code gets tests, not mocks

Mocking the client only proves your code called a function. With the `testing` feature, a test starts a real MQTT 5 broker inside the test process in milliseconds, one per test, with no container. Your code connects to it as it would in production. The test then checks what the code published, plays the devices or services on the other side, and cuts the network, refuses connections or rejects publishes to see how the code copes. See [Testing your application](#testing-your-application).

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
| The `PubAck` (reason code, Reason String) shows up in the poll loop, to be matched to the publish yourself | `publish(..).await` returns it: `PublishResponse` on success, `AppError::PublishRejected` inside the `PublishError` on refusal |
| Request/response: match your own correlation data in the poll loop | `client.requester(topic)` then `request(..).await` |

### What it costs

No MQTT 3.1.1 and no QoS 2: both are out of scope, not pending. If you need either, rumqttc remains the better fit. Unacknowledged QoS 1 publishes are kept in memory only: they are resent after a reconnect into a resumed session (see [No session](#no-session)), but not after a process restart. Cross-process session resume needs a stable client ID (`.client_id("…")?` or `ClientId::try_new`) plus `clean_start(false)` and a non-zero `session_expiry`. rumqttc is also the far more widely deployed crate; this one is 0.3.x, and the API can still move.

## Upgrading from 0.3

Awaiting a publish now returns `Result<PublishResponse, PublishError>` instead of `Result<(), AppError>`: the response carries the PUBACK (reason code, Reason String, User Properties, whether it answers a resend), the error carries the `AppError` plus `delivery`, whether the broker can have the message. `PublishError` converts into `AppError`, so `publish(..).await?` in a function returning `AppError` needs no change; code that matches `Err(AppError::…)` on a publish matches `err.error` instead. A message's `properties` field is a `Properties` that derefs to `PublishProperties`: reading and changing it is unchanged, a struct literal needs `props.into()`. A broker DISCONNECT is now `TransportError::BrokerDisconnect { reason, .. }` and a refused CONNACK `TransportError::ConnectRefused { reason, .. }`, with the reason code as a field, no longer text inside `TransportError::Connection` or `ProtocolError::Violation`. The [CHANGELOG](CHANGELOG.md#unreleased) has the details.

## Upgrading from 0.2

The biggest changes: a new publish API (`client.publish(topic, payload).qos(..)`, and an owned `Publish` for `send`), inbound messages delivered as `InboundMessage` (with `retain`, `dup` and `qos`), the non-blocking enqueue renamed to `try_start_publish` while `start_publish` now waits for room, and `#[non_exhaustive]` on most public enums and response types, so a `match` on them needs a wildcard arm. A filter the broker refuses to restore after a reconnect no longer makes the client reconnect; watch `refused_subscriptions()` instead. The [CHANGELOG](CHANGELOG.md#030---2026-10-17) has a migration table from the old calls to the new ones.

## Architecture

`connect()` gives you a handle and starts one driver over a single socket:

1. `MqttClient`: a cheap cloneable handle. It sends commands and does no socket I/O.
2. Write task: encodes `PUBLISH` / `SUBSCRIBE` / acks / `PINGREQ`, and puts inbound `PUBACK`s ahead of your publishes.
3. Read task: decodes frames, answers control packets itself, and passes application `PUBLISH` to your sink.

The driver around these tasks (`Connection::run`) owns reconnects and credential refresh; neither is your code's job.

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

Without an explicit `client_id`, the builder mints a fresh UUID (`ClientId::from_prefix("glide")`). That ID is reused for reconnects inside the same process. For broker-side session resume across restarts, set a stable ID with `.client_id("my-service")?` (or `.client_id(ClientId::try_new("my-service")?)` on `ConnectOptions`) together with `clean_start(false)` and a non-zero `session_expiry`. Two processes must not share one ID: the broker hands the session to whichever connected last and ends the other connection with 0x8E Session taken over. `StandardReconnectPolicy` then waits its long delay rather than taking the session straight back, but during a rolling update one ID per pod (with a short `session_expiry`) avoids the takeover altogether.

### No session

`clean_start` is `true` and `session_expiry` is unset (property omitted; MQTT default 0 s), so the broker drops your subscriptions and anything queued for you as soon as the connection ends. Resuming a session needs `clean_start(false)` plus a non-zero `session_expiry`; when CONNACK reports `session_present = false`, the client subscribes again to everything the application subscribed to.

A service restarted with `clean_start(false)`, the same client ID and an unexpired session resumes the session the broker kept for it, and receives what was queued meanwhile. The broker keeps that session's subscriptions as well, including ones the new version no longer wants. A service that changes its subscriptions between versions should unsubscribe the old filters, or connect once with Clean Start to discard the session.

What happens to your publishes when the connection drops and the client reconnects:

| Publish state at the drop | Clean Start (`true`) | Resumed session (`clean_start(false)`, `session_expiry` > 0) |
|---|---|---|
| Never sent (parked behind Receive Maximum, or still queued) | sent on the next connection | sent on the next connection |
| QoS 1 sent, `PUBACK` missing | fails with `AppError::ConnectionLost` | resent in order with its original packet id and `DUP` set ([MQTT-4.4.0-1]); fails with `ConnectionLost` if CONNACK reports no session |

A caller that stops waiting earlier (its `ack_timeout` ran out during a long reconnect backoff) gets `PublishTimeout`, and `PublishError::delivery` says which case it hit. For QoS 1, `Delivery::NotSent`: the publish had not been written, and it never will be, so publishing it again cannot duplicate it. `Delivery::Unknown`: it had been written; an unacknowledged one in a resumed session is still resent, so publishing it again yourself can deliver it twice. QoS 0 keeps no write state (it would cost QoS 0 throughput), so its timeout says `Unknown`; one not yet written is still never written afterwards. To wait for the PUBACK however long a reconnect takes, use `.ack_timeout(None)` on the publish, or `PublishCompletion::wait_for(None)`.

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

On `ClientBuilder` itself only `reconnect`, `client_id` (a string), `credentials`, `lifecycle`, the Will setters and `wait_connected` exist. Every other setting in the table, and the other opt-ins such as `follow_server_redirects` and `http_proxy`, is a field of [`ConnectOptions`](https://docs.rs/mqtt-glide/latest/mqtt_glide/struct.ConnectOptions.html) (which lists the full set; `wait_connected` is not among them). Set them through [`ClientBuilder::options`](https://docs.rs/mqtt-glide/latest/mqtt_glide/builder/struct.ClientBuilder.html#method.options), or start from `ClientBuilder::from_options`:

```rust,no_run
# async fn demo() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
use std::time::Duration;

use mqtt_glide::ClientBuilder;

let client = ClientBuilder::new("mqtt://127.0.0.1:1883")?
    .client_id("my-service")?
    .options(|options| {
        options
            .clean_start(false)
            .session_expiry(Some(Duration::from_secs(3600)))
            .fail_fast_when_disconnected(true)
            .topic_alias_maximum(16)
            .follow_server_redirects(true)
    })
    .discard_inbound()
    .connect()
    .await?;
# client.shutdown().await?;
# Ok(())
# }
```

## Flow control

Three limits, easy to confuse, only two of them yours to set.

### Command channel capacity

`ConnectOptions::command_channel_capacity`, default `32`: how many commands may queue up for the write task. When it is full, `publish` / `send` / `subscribe` / `unsubscribe` and `PublishWindow::push` and `start_publish` wait up to `ack_timeout` for room (then `AppError::Backpressure`), while `try_start_publish` fails fast with `AppError::Backpressure`. Set it to at least the number of publishes you keep in flight.

### Receive Maximum

`ConnectOptions::receive_maximum`, default `32`: how many inbound QoS 1 messages the broker may leave unacknowledged with this client. This is the knob that bounds memory when your sink is slow.

What a slow sink can hold is bounded by its own capacity, the read task's one message in hand, and `max_packet_size` per message. A decoded message usually costs about its wire size, payload and strings share the read buffer. User Properties are the exception: each costs about 70 bytes of heap however short it is, so a broker sending a 1 MiB PUBLISH packed with empty User Properties (5 bytes each on the wire) makes it about 14 MiB once decoded. If you do not trust the broker's publishers, size `max_packet_size` and the sink with that factor in mind.

### Outbound inflight

Not yours: the broker's Receive Maximum from `CONNACK` decides how many of your QoS 1 publishes can await `PUBACK` at once. Publishes beyond that window wait in order inside the write task (up to 32, then the command channel fills) and go out as `PUBACK`s free slots. They do not fail, and subscribe / unsubscribe are not held up behind them.

How many publishes you have in flight is an application decision, not a library option. That is what [`publish_window`](https://docs.rs/mqtt-glide/latest/mqtt_glide/struct.PublishWindow.html) gives you.

Do not confuse `DEFAULT_RECEIVE_MAXIMUM` (`32`, on `ConnectOptions`) with the protocol default of `65535`, which applies when the wire property is absent. Internal write coalesce / soft-flush sizes are fixed; there is no env-var tuning in the library (`MQTT_COMPARE_*` is bench-only).

| Goal | What to set |
|------|-------------|
| Outbound pipeline / many concurrent `publish` | `command_channel_capacity` ≥ your in-flight count; prefer [`publish_window`](https://docs.rs/mqtt-glide/latest/mqtt_glide/struct.PublishWindow.html) (or your own window over `start_publish`) |
| Inbound QoS 1 under load | Larger `receive_maximum`; keep the `InboundSink` draining (a full sink stalls decode and delays `PUBACK`) |
| Comparing against another client | Same receive window and request capacity as the peer; measure pipelined or concurrent QoS 1, not serial round-trips |

## API reference

The tables below are the lookup surface: which call to reach for, what it guarantees, and which defaults apply. Full signatures live on [docs.rs](https://docs.rs/mqtt-glide).

Prefer convenience (zero extra cost vs. the primitives underneath):

| Goal | API |
|------|-----|
| Connect | [`ClientBuilder`](https://docs.rs/mqtt-glide/latest/mqtt_glide/builder/struct.ClientBuilder.html): `.inbound_sink` / `.on_inbound` / `.discard_inbound` before `connect` |
| One publish | [`publish`](https://docs.rs/mqtt-glide/latest/mqtt_glide/struct.MqttClient.html#method.publish) → `.await` (or `.send().await`) |
| Many publishes in flight | [`publish_window`](https://docs.rs/mqtt-glide/latest/mqtt_glide/struct.PublishWindow.html) → `.push(Publish::new(..)).await` … `.drain().await` |
| Burst | [`publish_batch`](https://docs.rs/mqtt-glide/latest/mqtt_glide/struct.MqttClient.html#method.publish_batch) → one call (`push` + `drain` under the hood) |
| Subscribe | [`subscribe`](https://docs.rs/mqtt-glide/latest/mqtt_glide/struct.MqttClient.html#method.subscribe) / `subscribe_with_id` → per-filter reason codes plus the broker's Reason String and User Properties |
| Request / response | `client.requester(response_topic).await?` → `.request(topic, payload).await` resolves with the response |
| Receive + workers | `.inbound_sink(mpsc)` + your `recv` loop |
| Tiny sync inbound work | `.on_inbound(\|msg\| …)` (runs on the read task; keep it short) |

Power (same semantics, more control): [`connect`](https://docs.rs/mqtt-glide/latest/mqtt_glide/fn.connect.html), custom [`InboundSink`](https://docs.rs/mqtt-glide/latest/mqtt_glide/trait.InboundSink.html), [`send`](https://docs.rs/mqtt-glide/latest/mqtt_glide/struct.MqttClient.html#method.send) / [`start_publish`](https://docs.rs/mqtt-glide/latest/mqtt_glide/struct.MqttClient.html#method.start_publish) / [`try_start_publish`](https://docs.rs/mqtt-glide/latest/mqtt_glide/struct.MqttClient.html#method.try_start_publish) + [`PublishCompletion`](https://docs.rs/mqtt-glide/latest/mqtt_glide/struct.PublishCompletion.html).

### Publish paths

| Method | Role | Notes |
|--------|------|-------|
| `publish(topic, payload)` | Convenience one-shot | `.await` or `.send().await`; `.start().await` returns a `PublishCompletion` once queued, `.try_start()` without waiting at all. Default QoS 0. Setters: `.qos()`, `.retain()`, `.message_expiry(secs)`, `.user_property(name, value)` / `.user_properties(iter)`, `.content_type()`, `.response_topic()`, `.correlation_data()`. `.properties(...)` replaces the whole struct; call it before the other property setters. |
| `publish_window(n)` | Convenience pipeline | Sliding window over `start_publish` (no task per message). `.push(impl Into<Publish>)` waits for command-channel room. Always `.drain()` after the burst. |
| `publish_batch(msgs, n)` | Convenience burst | Internal window of capacity `n`; each message is an `impl Into<Publish>` carrying its own QoS / retain. |
| `send(publish)` | Power one-shot | Ready [`Publish`](https://docs.rs/mqtt-glide/latest/mqtt_glide/struct.Publish.html) value (also converts from a `PublishMessage`). |
| `start_publish` / `try_start_publish` + `PublishCompletion` | Power pipeline | Custom windowing / wait order. `start_publish` waits for command-channel room (up to `ack_timeout`); `try_start_publish` fails at once with `AppError::Backpressure` on a full channel, like `try_send` on a channel. |
| `client.requester(topic)` → `request(topic, payload)` | Request / response | Subscribes once to `topic` (QoS 1, restored on reconnect); each request carries it as Response Topic with fresh Correlation Data and resolves with the matching `InboundMessage`. Setters: `.qos()`, `.timeout()` (default 10 s, then `AppError::ResponseTimeout`), `.user_property()`, `.content_type()`, `.message_expiry()`. Responses never reach the inbound sink. |
| `Publish::reply_to(&request, payload)` | Answering side | `Some(Publish)` addressed to the request's Response Topic with its Correlation Data copied; `None` if the request carried no Response Topic. |

QoS 2 is rejected (`AppError::InvalidRequest`). Packet ID / `dup` are assigned by the driver.

A completed publish returns a `PublishResponse`: for QoS 1 the PUBACK's `reason()` (`Success` or `NoMatchingSubscribers`), `reason_string()` and `user_properties()` as the broker sent them, and `resent()` when the PUBACK answers a DUP resend after a reconnect (the message may then have reached subscribers twice). QoS 0 has no acknowledgement: `reason()` is `None`. A failed publish returns a `PublishError`: the `error` (`AppError`), and `delivery`: `NotSent` (never written; publishing again cannot duplicate it), `Unknown` (written, no PUBACK, so it may be at the broker; also any QoS 0 timeout) or `Rejected` (the broker refused it with reason 0x80+, `AppError::PublishRejected(PublishRejection)` with the reason code, Reason String and User Properties). `start` / `try_start` / `start_publish` / `try_start_publish` fail only before anything was sent (`Delivery::NotSent`). Set a per-publish acknowledgement timeout with `.ack_timeout(Some(d))`, or `None` to wait until the PUBACK or a definite failure.

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

Shared filters: build with `shared_filter(group, topic)` (validates ShareName). To route inbound messages by filter, `topic_matches(filter, topic)` applies the MQTT matching rules (`+`, `#`, `$`-topics, `$share/…`); Subscription Identifiers (`subscribe_with_id`) let the broker do it instead.

A service that Kubernetes (or systemd) stops with SIGTERM loses nothing it acknowledged if it unsubscribes a shared group first, calls `shutdown()`, and keeps handling messages until the inbound channel ends: the channel closes once the driver has finished. [`graceful_shutdown.rs`](examples/graceful_shutdown.rs) shows the sequence, including when to skip the unsubscribe (persistent sessions).

Granted subscriptions are remembered and restored on reconnect whenever the broker holds no session for the client (Clean Start, or CONNACK without Session Present), **before** `SessionPhase::Connected` is published. Each `subscribe` call is repeated as its own SUBSCRIBE, so a broker that caps the filters per SUBSCRIBE (AWS IoT Core: 8) accepts the restore as it accepted the original; up to ten wait for their SUBACK at once. After a reconnect, `Connected` therefore means "connected, and every filter the broker allows is subscribed again"; on the first connect it only means "connected", and readiness is your own `subscribe` returning granted reason codes.

If the broker refuses a restored filter (for example after an ACL change), the connection stays up, and the other subscriptions and publishing keep working. The filter appears in `client.refused_subscriptions()`, a watch with the SUBACK reason code and Reason String, and is logged at `warn`. The client asks for it again on the same connection, first after about 5 s, then doubling up to every 5 minutes, until the broker grants it or you `unsubscribe` it, so a fixed ACL needs no restart. A non-empty list makes a good "degraded" health check. Only a broker that does not answer the SUBSCRIBE at all within `ack_timeout` makes the client reconnect.

### Hooks

| Trait / helper | Role |
|----------------|------|
| `CredentialsProvider` | Async `credentials()` before each `CONNECT`; `invalidate()` on CONNECT-level `NotAuthorized` only |
| `ReconnectPolicy` | `delay_after(outcome) -> Option<Duration>` (`None` = stop). The outcome of a broker `DISCONNECT` or refused `CONNACK` includes the broker's Reason String (and Server Reference). Ready-made: `StandardReconnectPolicy` + `mark_live()` |
| `LifecycleHook` | Sync `Connected` / `Disconnected` side effects on the driver task; keep short (no await). `ClientBuilder` already waits for the first `Connected`. For async waits use the [`SessionPhase`](https://docs.rs/mqtt-glide/latest/mqtt_glide/enum.SessionPhase.html) watch (`client.session_phase()`, any client) and `client.is_connected()` for a snapshot |
| `InboundSink` | Injectable inbound handoff; built-ins: `tokio::mpsc`, `OnInboundSink` (`.on_inbound`), `flume` (feature). Custom sinks **MUST** be cancel-safe |

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
| [`graceful_shutdown.rs`](examples/graceful_shutdown.rs) | SIGTERM handling: unsubscribe a shared group, `shutdown()`, drain the channel until it ends |

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

`StandardReconnectPolicy::mark_live()`: call once the session is fully ready (typically after subscriptions succeed). The next transport failure then retries immediately (`Duration::ZERO`) once, before falling back to the configured delay; only if the session lasted at least that delay, so a broker that drops every connection right away is not dialled in a loop. Auth failures use the normal delay, then `max_auth_backoff` after a streak (`DEFAULT_AUTH_FAILURE_THRESHOLD`). Credential cache clearing is `CredentialsProvider::invalidate`, not this policy.

Custom `InboundSink` cancel safety: built-ins (`tokio::mpsc`, `OnInboundSink`, `flume`) are fine. If you implement `InboundSink`, dropping `send()` before `Ok(())` **MUST** mean the message was not accepted by your consumer. On shutdown the driver may drop a pending send and will withhold `PUBACK` in that case. `.on_inbound` runs on the connection read task; keep the handler short or use a channel sink.

Spec **SHOULD**s this client adopts: `CONNACK` within `connect_timeout`; `PINGRESP` watchdog (~1× Keep Alive) with handoff-stall exemption.

## Testing your application

The `testing` feature (meant for `[dev-dependencies]`, no extra dependencies) lets the tests of a service built on mqtt-glide run against real MQTT, without a broker container and without mocking the client. Such a test can answer three questions:

- **Does the service publish the right thing?** Topic, payload, QoS, retain flag and properties of everything it sent.
- **Does it react to what arrives?** The test publishes as a device or another service would, so the service's subscriptions and wildcards are exercised too.
- **Does it survive trouble?** A dropped network, a broker that refuses connections or rejects publishes (as a wrong ACL would), latency, a silent stall, or packets cut at arbitrary bytes.

```toml
[dev-dependencies]
mqtt-glide = { version = "0.3", features = ["testing"] }
```

A test for a service that switches lights on command and reports their state:

```rust,no_run
# #[cfg(feature = "testing")]
# async fn demo() -> Result<(), Box<dyn std::error::Error>> {
# async fn start_light_service(_broker_url: String) {}
use std::time::Duration;
use mqtt_glide::{Publish, QoS, testing::TestBroker};

const WAIT: Duration = Duration::from_secs(2);

let broker = TestBroker::start().await?;
start_light_service(broker.url()).await; // your code, connecting as client ID "lights"
assert!(broker.wait_for_subscription("lights", "cmd/+", WAIT).await);

// A wall switch sends a command; the service must report the new state, retained.
broker.publish(Publish::new("cmd/kitchen", "on").qos(QoS::AtLeastOnce));
let sent = broker.wait_for_publishes(1, WAIT).await;
assert_eq!((&*sent[0].topic, &sent[0].payload[..]), ("state/kitchen", &b"on"[..]));
assert!(sent[0].retain);

// The network drops: the service must reconnect and subscribe again on its own.
broker.drop_client("lights");
assert!(broker.wait_for_connects(2, WAIT).await);
assert!(broker.wait_for_subscription("lights", "cmd/+", WAIT).await);
# Ok(())
# }
```

The two helpers:

- `testing::TestBroker`: an in-process MQTT 5 broker on a free local port. It routes between its clients (wildcards, retained messages, Wills, `$share` groups, No Local), records what clients publish (`received`, `wait_for_publishes`), shows who is connected and subscribed (`connected_clients`, `subscriptions`, `wait_for_connects`, `wait_for_subscription`), injects messages (`publish`), and simulates trouble: `drop_client` / `drop_connections` (network cut), `disconnect_all(reason)`, `refuse_connections(reason)`, `reject_publishes(reason)`, `ack_publishes(|publish| PubAck::new(..).reason_string(..))` (a chosen PUBACK per message: 0x10 No Matching Subscribers, a Reason String, User Properties), `refuse_subscription(filter, reason)`. It is a test double, not a broker to run: no persistence, authentication, QoS 2 or session resumption.
- `testing::FaultProxy`: a TCP proxy in front of any broker (the `TestBroker` or a real one) that adds latency, stalls silently, splits packets at arbitrary bytes, resets connections or refuses new ones, switched at runtime.

The `TestBroker` checks your service, not mqtt-glide: it shares the crate's codec, so it cannot catch the crate's own misreadings of the spec. That is what the tests against real brokers and [`CONFORMANCE.md`](CONFORMANCE.md) are for.

## Metrics

With the `stats` feature, `client.stats()` returns what the connection has done since `connect`, across reconnects:

- **Connection:** sessions established, failed attempts, lost sessions, keep-alive pings.
- **Outbound:** publishes per QoS, resends after a reconnect, PUBACKs accepted and rejected, PUBACKs that came after the caller gave up (`late_acks`), and right now: publishes waiting for their PUBACK, publishes waiting for a slot in the broker's Receive Maximum, commands queued for the connection.
- **Inbound:** publishes per QoS, duplicates, how often and how long the inbound sink was full.
- **Bytes** in each direction.

The crate does not choose a metrics system for you. Read the snapshot on each scrape (a few dozen atomic loads) and hand the numbers to Prometheus, OpenTelemetry or a log line. Without the feature the counting is compiled out. With it on, each counted event is a plain atomic store on the task that owns the counter; in an interleaved on/off comparison, throughput did not change and CPU per message stayed within about 2% in all but one of six cells, at the size of the run-to-run spread ([Cost of the `stats` feature](BENCHMARKS.md#cost-of-the-stats-feature)).

```toml
mqtt-glide = { version = "0.3", features = ["stats"] }
```

```rust,no_run
# #[cfg(feature = "stats")]
# fn demo(client: &mqtt_glide::MqttClient) {
let stats = client.stats();
let reconnects = stats.connection.connects.saturating_sub(1);
let broker_is_slow = stats.outbound.awaiting_quota > 0;
let consumer_held_up_the_broker_for = stats.inbound.handoff_stall_time;
# let _ = (reconnects, broker_is_slow, consumer_held_up_the_broker_for);
# }
```

## Feature matrix

The authoritative answer to "does it do X?", including the things it deliberately does not do.

Legend: ✅ supported · ⚠️ partial / deferred · ❌ not implemented (open or out of scope)

| Area | Status | Notes |
|------|:------:|-------|
| MQTT 5 wire (`CONNECT`…`DISCONNECT`, QoS 0/1) | ✅ | MQTT 3.1.1 out of scope |
| Shared subscriptions (`$share/…`) | ✅ | Caller builds the filter; `shared_filter` validates ShareName |
| Topic filter wildcards | ✅ | Client validates `+` / `#` placement before encode; `topic_matches` matches a topic against a filter |
| Publish properties (expiry, user props, content type, `response_topic`, `correlation_data`) | ✅ | On `PublishProperties` (a message holds them as `Properties`, allocated only when set); setters on `Publish` |
| TLS (rustls + system roots) | ✅ | Feature-selected crypto provider |
| Reconnect / credentials hooks | ✅ | Traits + optional helpers |
| Enqueue-before-ack + `InboundSink` | ✅ | Product backpressure policy |
| Cap / Receive Maximum flow control | ✅ | See [Flow control](#flow-control) |
| Inbound Maximum Packet Size | ✅ | CONNECT advertises [`DEFAULT_MAX_PACKET_SIZE`](https://docs.rs/mqtt-glide/latest/mqtt_glide/constant.DEFAULT_MAX_PACKET_SIZE.html) (1 MiB) by default; override via `ConnectOptions::max_packet_size`. Broker CONNACK limit applies outbound only |
| Graceful `shutdown` | ✅ | Budget via `graceful_disconnect_timeout` |
| Subscription identifiers | ✅ | `subscribe_with_id(id, filters)`; inbound `InboundMessage::subscription_ids`; kept across resubscribe; refused when CONNACK says unsupported |
| Request/response | ✅ | `client.requester(response_topic)` → `request(topic, payload).await` resolves with the response (correlation handled, responses bypass the inbound sink, `ResponseTimeout`); `Publish::reply_to(&request, payload)` on the answering side. CONNECT Request Response Information not requested |
| Will message (`CONNECT`) | ✅ | `.will(topic, payload)` (+ `.will_qos` / `.will_retain`); QoS 0\|1; power: `WillMessage` |
| Optional `SO_SNDBUF` / `SO_RCVBUF` | ⚠️ | OS defaults today |
| Broker diagnostics (Reason String, User Properties) | ✅ | Passed through on `PUBACK` rejections (`PublishRejection`), `SUBACK` / `UNSUBACK` (`SubscribeResponse` / `UnsubscribeResponse`), and in the outcome of a broker `DISCONNECT` / refused `CONNACK`. The client's own outbound `DISCONNECT` carries no reason string |
| Client counters (`MqttClient::stats`) | ✅ opt-in | `stats` feature; see [Metrics](#metrics). No dependencies, no metrics system chosen for you |
| Test helpers (`TestBroker`, `FaultProxy`) | ✅ opt-in | `testing` feature; see [Testing your application](#testing-your-application) |
| HTTP proxy (`CONNECT` tunnel) | ✅ opt-in | `ConnectOptions::http_proxy(HttpProxy::parse("http://user:pass@proxy:3128")?)`; TLS / WebSocket run end to end through the tunnel; Basic auth optional. Not read from `HTTP(S)_PROXY`. SOCKS not supported |
| Server redirection (§4.11) | ✅ opt-in | `follow_server_redirects(true)`: CONNACK / DISCONNECT 0x9C (Use another server, next connection only) or 0x9D (Server moved, for good) with a Server Reference reconnects there at once; scheme, TLS and WebSocket path stay, at most five redirects in a row. Off: reported in the outcome |
| Enhanced AUTH (`AUTH` packets, re-authentication) | ❌ | Not implemented; `CredentialsProvider` covers username/password only |
| WebSockets | ✅ opt-in | `websocket` feature: `ws://host[:port]/path` / `wss://…` (subprotocol `mqtt`, binary frames, §6). `wss` uses the same rustls setup as `mqtts`. Without the feature such URLs fail to parse |
| Topic Alias | ✅ opt-in | Inbound: `topic_alias_maximum(n)`, resolved before delivery. Outbound: `outbound_topic_aliases(n)`, capped by the broker's maximum, LRU rebinding. Per connection, reset on reconnect |
| QoS 2 | ❌ | Rejected at the API (no 4-way handshake / store-and-forward) |
| Persistent sessions / DUP redelivery | ✅ in process | Resumed sessions resend unacknowledged QoS 1 with `DUP` and original ids ([MQTT-4.4.0-1]); state is in memory, not persisted across restarts. Needs a stable client ID (`.client_id("…")?` / `ClientId::try_new`; `ClientId::from_prefix` always mints a fresh UUID), `clean_start(false)` and a non-zero `session_expiry`. A restarted process resumes the broker's session (queued messages arrive, old subscriptions stay); the old process's unacknowledged publishes are gone |

Broker-dependent: Message Expiry *drop after interval* (property round-trip is tested), shared-sub rebalancing under handoff stall, redelivery after unclean disconnect (client guarantees enqueue-before-ack only).

## Performance

All results and the reading guide: [`BENCHMARKS.md`](BENCHMARKS.md). How to run them yourself: [`benches/README.md`](benches/README.md).

Headline at 256 B, QoS 1, tuned Mosquitto `2.1.2-alpine`, median of 5 runs, 50 000-message bursts, on an Intel Core Ultra 7 258V with the client pinned to 2 P-cores (msg/s · CPU ns/msg; **bold** = best in row):

| Scenario | glide (main) | rumqttc-v5-next 0.33.3 | mqtt5 0.48.0 |
|----------|------:|--------:|------:|
| 64 callers sharing one client | **293 160** · **3 600** | 206 252 · 4 782 | 171 652 · 7 590 |
| 512 callers sharing one client | **316 591** · **3 430** | 220 438 · 4 540 | 215 616 · 6 810 |
| Pipelined, window 64 | **298 327** · **3 691** | 216 274 · 4 527 | 180 156 · 8 513 |

These are absolute numbers on one host and broker setup, not paired comparisons. Against a stock-limit Mosquitto (Receive Maximum 20) glide and mqtt5 lose throughput at high concurrency, and peers lead some cells (for example idle QoS 0 outbound latency and QoS 0 ingress load on the mock broker, and mqtt5's idle QoS 1 latency on tuned Mosquitto). rumqttc is pinned to 0.33.3 because 0.34.0 scans its whole outgoing table on every flush; against a broker without a Receive Maximum that table has 65 535 slots and throughput falls to a few thousand msg/s. Per-publish p50/p99/p99.9 latency, a task-per-message burst, fixed-rate load, ingress, and the pinning and fairness details are in [`BENCHMARKS.md`](BENCHMARKS.md). The wire codec alone, without broker or client, is measured by `cargo bench --bench codec_throughput` ([`benches/README.md`](benches/README.md)).

Throughput knobs live under [Flow control](#flow-control); the `MQTT_COMPARE_*` variables configure the bench harness and have no effect on the library.

## License

Licensed under either of

- Apache License, Version 2.0 ([`LICENSE-APACHE`](LICENSE-APACHE) or https://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([`LICENSE-MIT`](LICENSE-MIT) or https://opensource.org/licenses/MIT)

at your option.
