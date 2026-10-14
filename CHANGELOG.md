# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed (breaking)

- New publish API. `Publish` is an owned value carrying topic, payload, QoS, retain and properties (`Publish::new(topic, payload).qos(..).retain(..)`; `From<PublishMessage>` as QoS 0). Migration:

  | 0.2 | 0.3 |
  |---|---|
  | `client.publish_with().topic(t).payload(p).qos(q).send().await` | `client.publish(t, p).qos(q).await` |
  | `client.publish(msg, qos, retain).await` | `client.send(Publish::from(msg).qos(qos).retain(retain)).await` |
  | `client.start_publish(msg, qos, retain)` (fails on a full channel) | `client.try_start_publish(Publish::from(msg).qos(qos).retain(retain))`, or `client.start_publish(publish).await` to wait for room |
  | `pipe.publish_with().topic(t).payload(p).push_retry().await` | `pipe.push(Publish::new(t, p)).await` |
  | `pipe.push(msg, qos, retain)` / `pipe.push_retry(..)` | `pipe.push(Publish::from(msg).qos(qos).retain(retain))` |
  | `client.publish_batch(msgs, qos, retain, n)` | `client.publish_batch(publishes, n)` (QoS / retain per message) |
  | loop of `.user_property(k, v)` | `.user_properties(iter)` |

  Removed: `MqttClient::publish_with`, `PublishWith`, `WindowPublishWith`, `PublishWindow::push_retry`, the type-state markers and the `publish_builder` module (now `publish`)
- Awaited publishes (`publish(..).await`, `send`, `PublishWindow::push`, `publish_batch`) and `subscribe` / `unsubscribe` now wait for room in the command channel (bounded by `ack_timeout`, then `AppError::Backpressure`) instead of failing at once on a full channel. The non-blocking enqueue is renamed `try_start_publish` (`PublishBuilder::try_start`); `start_publish` (`PublishBuilder::start`) now waits for room, following `try_send` / `send` on a channel. A sender swapped by a reconnect while waiting is retried on the new session channel

- Publishes that were never sent (parked behind the broker's Receive Maximum, or still in the command channel) no longer fail when the connection drops; they go out on the next connection, as publishes issued during the reconnect backoff already did. The command channel now lives as long as the client, so the same holds for subscribe / unsubscribe still queued, and for everything issued while disconnected, however many reconnect attempts fail before a session is up (each failed attempt used to drop the queue, failing it with `AppError::CommandChannelClosed`)
- `shutdown()` drains before it disconnects, bounded by `graceful_disconnect_timeout`: commands already queued are sent, outbound QoS 1 publishes get their `PUBACK`s instead of failing at once, and the read task keeps delivering and acknowledging inbound messages meanwhile, so they are no longer dropped with the socket buffer. Only what is still unacknowledged when the budget runs out fails with `AppError::ConnectionLost`. Every command issued after `shutdown()` fails with the new `AppError::ShuttingDown`
- `AppError` is `#[non_exhaustive]`, and so are `SessionPhase`, `ConnectionOutcome`, `ConnectError`, `AuthError`, `TransportError`, `ProtocolError`, `ShutdownError`, `ShareNameError`, `TlsConfig`, `DecodeError`, `EncodeError`, `PublishRejection`, `SubscribeResponse` and `UnsubscribeResponse`: a `match` on them needs a wildcard arm, and the structs are only built by the client. New variants and fields are then no longer breaking changes
- `ConnectOptions` can no longer be written as a struct literal (it gained a private field when the bench-only `bench_inbound_publish_ack_counter` hook left the public API); build it with `ConnectOptions::new(url)` and its setters
- `InboundSinkClosed` and `InboundTrySendError` implement `Display` and `std::error::Error`; `ClientBuilder` and `OnInboundSink` implement `Debug`

- Removed from the public API: `Connection::state` / `Connection::generation` (`run` consumes the `Connection`, so they could only ever report the initial state) together with the `ConnectionState` / `Generation` re-exports, and the test-only `TlsMode` / `resolve_tls_mode`. Observe the session through `SessionPhase`, a `LifecycleHook` or `MqttClient::is_connected`

- Wire-level codec items are no longer re-exported at the crate root: `Connect`, `ConnectAck`, `Disconnect`, `Packet`, `DecodedFrame`, `V5Codec`, `PublishPacket`, `PublishPacketMeta`, `PublishDelivery`, `PublishAck`, `Subscribe`, `SubscribeAck`, `Unsubscribe`, `UnsubscribeAck`, `DecodeError`, `EncodeError`, `RECEIVE_MAX_DEFAULT`, `encode_publish`, `ensure_publish_wire_fits`. Import them from `mqtt_glide::codec`. Message, option and reason-code types stay at the root

- Inbound messages are delivered as the new `InboundMessage` (topic, payload, properties plus `retain`, `dup`, `qos`) instead of `PublishMessage`, so applications can tell retained messages from live ones and see redeliveries. `InboundSink`, `on_inbound` and the inbound channel types change accordingly (`flume::Sender<InboundMessage>`, `tokio::sync::mpsc::Sender<InboundMessage>`); field access (`msg.topic`, `msg.payload`) is unchanged. `Publish::from(InboundMessage)` republishes one

- Requests that were waiting when the connection broke (QoS 1 publishes in flight or parked, subscribe / unsubscribe awaiting their ack) now always fail with `AppError::ConnectionLost`. Before, a write-side failure reported `SendFailed` and a read-side one `ConnectionLost` for the same situation, depending on which side noticed first. `SendFailed` remains for the one command whose own write failed

- Broker diagnostics are passed through instead of dropped: `AppError::PublishRejected` now carries a `PublishRejection` (PUBACK reason code, Reason String, User Properties) instead of a formatted string; `subscribe` / `unsubscribe` return `SubscribeResponse` / `UnsubscribeResponse` (per-filter reason codes plus Reason String and User Properties; they deref to the reason list, compare equal to a `Vec` of reasons and offer `as_slice`, so most callers compile unchanged); the Reason String and Server Reference of a broker DISCONNECT and the Reason String of a refused CONNACK appear in the connection outcome

### Added

- `stats` feature: `MqttClient::stats()` returns a `ClientStats` snapshot of what the connection did since `connect`, across reconnects (sessions, failed attempts and losses, publishes per QoS in and out, PUBACKs accepted and rejected, publishes waiting for an ack or a Receive Maximum slot, queued commands, duplicates received, inbound sink stalls and their duration, bytes each way), for the application to export to whatever metrics system it uses. No dependencies; without the feature the counting compiles away
- `testing` feature with test helpers for applications: `testing::TestBroker`, an in-process MQTT 5 broker (routing with wildcards, retained messages, Wills, shared subscriptions, No Local; records publishes; waits for connects and subscriptions; injects messages; drops, disconnects, refuses connections or rejects publishes on demand), and `testing::FaultProxy`, a TCP proxy that injects latency, stalls, split packets, resets and refused connects
- HTTP proxy support: `ConnectOptions::http_proxy(HttpProxy)` tunnels the broker connection through an HTTP proxy with `CONNECT` (optional Basic auth via `HttpProxy::basic_auth` or `http://user:pass@host:port`); TLS and WebSocket run end to end through the tunnel. `HttpProxy::parse` rejects other schemes with the new `BrokerUrlError::UnsupportedProxyScheme`
- Server redirection, opt-in: with `ConnectOptions::follow_server_redirects(true)` a CONNACK or DISCONNECT with Reason Code 0x9C (Use another server) or 0x9D (Server moved) and a Server Reference makes the client connect there at once, regardless of the reconnect policy (0x9C for the next connection only, 0x9D for good; host and port from the reference, scheme / TLS / WebSocket path unchanged; at most five redirects in a row, where a session that ran for a minute ends the streak; an IPv6 reference may come without brackets). A refused CONNACK's Server Reference is now also part of the outcome text
- Request / response: `MqttClient::requester(response_topic)` returns a `Requester`; `requester.request(topic, payload)` publishes with Response Topic and fresh Correlation Data and resolves with the matching response (`AppError::ResponseTimeout` after `DEFAULT_REQUEST_TIMEOUT` or `.timeout(..)`); responses are routed to the waiting request and never reach the inbound sink. `Publish::reply_to(&request, payload)` builds the answer; `Requester::response_topic`, `Requester::close` (the topic stays routed and subscribed while another requester of the client uses it); `RequestBuilder` sets QoS, User Properties, Content Type, Message Expiry and the timeout; `SubscribeAckReason::is_granted`
- Subscription Identifiers: `MqttClient::subscribe_with_id(id, filters)` tags subscriptions, inbound messages report every matching id in `InboundMessage::subscription_ids` (also `PublishPacket::subscription_ids`), and restored subscriptions keep their id across reconnects. `SessionParams::subscription_identifiers_available`; a broker without support gets `AppError::InvalidRequest` instead of a protocol error
- MQTT over WebSocket behind the optional `websocket` feature: `ws://` / `wss://` broker URLs (`BrokerUrl::websocket_path`), `mqtt` subprotocol, binary frames per §6, `wss` on the client's own rustls configuration. Without the feature such URLs fail with `BrokerUrlError::WebSocketUnsupported`
- Topic Aliases, opt-in: `ConnectOptions::topic_alias_maximum` accepts aliases from the broker (resolved before delivery), `ConnectOptions::outbound_topic_aliases` uses them when publishing (capped by the broker's CONNACK maximum, least recently used topic rebound when full, reset on every new connection). `SessionParams::server_topic_alias_maximum`; codec: `encode_publish_with_alias` / `OutboundTopicAlias`
- `MqttClient::session_phase`: the session-phase watch, now also reachable for clients made with `ClientBuilder` (which only returns the `MqttClient`)
- `MqttClient::is_connected` and `ConnectOptions::fail_fast_when_disconnected`: with it, publish / subscribe / unsubscribe fail at once with the new `AppError::NotConnected` while no connection is up, instead of queueing for the next connection (the default, still bounded by `ack_timeout`)
- `Publish` setters for Content Type, Response Topic and Correlation Data; read accessors (`topic`, `payload`, `qos_level`, `is_retained`, `publish_properties`, `into_parts`)
- `PublishBuilder::start` (enqueue without waiting) and `PublishBuilder::into_publish`

### Fixed

- A Will the encoder cannot send (U+0000 in its Content Type or User Properties, a topic, payload or Correlation Data over 65 535 bytes) is refused up front with `AppError::InvalidRequest`, like an invalid Will topic. It used to fail every CONNECT, so a retrying reconnect policy kept trying for good
- The client pings on the Keep Alive it sent in CONNECT. A `keep_alive` with a fraction of a second (1.9 s went out as 1 s) or above 65 535 s made an idle client ping later than the broker expected, and the broker dropped it
- `BrokerUrl::parse` (and so `ClientBuilder::new`) rejects a host with whitespace, control characters, `@`, `/`, `\`, `?`, `#` or stray brackets, and an IPv6 address without brackets (`mqtt://2001:db8::1` was read as host `2001:db8:` with port 1). Such a host reached DNS, the TLS server name and an HTTP proxy's `CONNECT` line, where CR/LF could add header lines
- An inbound PUBLISH with Message Expiry Interval 0 (a broker forwarding a message whose lifetime is used up) is accepted. It was decoded as a Malformed Packet, so the client disconnected, and with a resumed session the broker redelivered the same message into a disconnect loop. `PublishProperties::message_expiry_interval` and `WillMessage::message_expiry_interval` are now `Option<u32>` (breaking); the `message_expiry(0)` setters still mean "no expiry"
- A broker that stops reading while keeping the connection open now ends the session: once the socket is full, a write that stays blocked for 1.5 x Keep Alive is a transport error and the client reconnects. Keep-alive pings come from the blocked write task, so before this the session stayed "connected" for good and publishes timed out one by one
- A SUBSCRIBE or UNSUBSCRIBE waiting for its ack no longer takes one of the broker's Receive Maximum slots. Its packet id came from the same low range as publish ids, so with the window nearly full a QoS 1 publish failed with `AppError::Backpressure("no free packet identifiers in publish window")` instead of waiting, and a subscribe that timed out kept the window one slot short for the rest of the connection
- A service restarted with `clean_start(false)` resumes the session the broker kept for it: Session Present = 1 on the first connection of a process is accepted, and the messages the broker queued meanwhile are delivered. Before, the client answered with DISCONNECT 0x82 and stopped for good (`SessionPresentInconsistent`), so a persistent session only survived reconnects within one process. The broker keeps the old session's subscriptions; a service that changes its subscriptions between versions unsubscribes the old ones or connects once with Clean Start
- A packet the client cannot decode is answered with a DISCONNECT carrying the matching reason (0x81 Malformed Packet, 0x95 Packet too large, ...) before the connection closes, instead of closing silently ([MQTT-3.14.1-1]); an invalid inbound Topic Alias gets 0x94, and a packet a client must never receive (QoS 2 acknowledgements, server-only packets) 0x82 Protocol Error. Well-formed packets the spec calls a Protocol Error (a property included twice, a CONNACK Receive Maximum of 0 or Maximum QoS of 2, a PUBLISH with an empty topic and no Topic Alias) now get 0x82 instead of 0x81 through the new `DecodeError::ProtocolViolation`, and a PUBLISH with Topic Alias 0 gets 0x94 (`DecodeError::TopicAliasInvalid`). `DecodeError::to_reason_code` maps set reserved bits and the forbidden packet type 0 to Malformed Packet
- QoS 1 publishes beyond the broker's CONNACK Receive Maximum now wait for a free slot (FIFO, sent as `PUBACK`s arrive) instead of failing with `AppError::Backpressure("publish inflight at capacity")`. A `publish_window` / `publish_batch` wider than the broker window (e.g. 64 against a default mosquitto, which allows 20) used to fail in `drain()`. SUBSCRIBE / UNSUBSCRIBE still pass parked publishes; a parked publish whose caller gave up (ack timeout / drop) is never sent
- Resumed sessions resend unacknowledged QoS 1 publishes ([MQTT-4.4.0-1]). With `clean_start(false)` and a non-zero `session_expiry`, a connection loss no longer fails in-flight QoS 1 publishes: after reconnect with Session Present they are resent in the original order with their original packet ids and DUP set, and the caller's completion resolves on the new PUBACK. If the broker reports no session, they fail with `AppError::ConnectionLost` ([MQTT-3.2.2-5]), as do all of them on shutdown or when the driver stops reconnecting. More than the new connection's Receive Maximum allows wait and are resent as PUBACKs free slots ([MQTT-4.9.0-2]), still before anything new. Clean Start sessions are unchanged
- `subscribe` rejects QoS 2 filters with `AppError::InvalidRequest` before any wire bytes. Inbound QoS 2 is not supported, so a granted QoS 2 subscription disconnected the session on the first matching message, and the resubscribe registry restored it after every reconnect (disconnect loop)

## [0.2.0] - 2026-08-06

### Added

- Stable Client Identifiers: `ClientId::try_new` / `FromStr` / `TryFrom` (plus `ClientIdError`); `ClientBuilder::client_id` takes `impl AsRef<str>` and returns `Result`
- Default builder client ID prefix is `glide-` (was `mqtt5-`) so random IDs are not confused with LabOverWire `mqtt5` in broker logs
- Will Message (LWT) on CONNECT: `ClientBuilder` / `ConnectOptions` `.will(topic, payload)` (+ `.will_qos` / `.will_retain`)
- Fluent codec helpers: `PublishMessage::new` / `.properties`, `PublishProperties` setters, `Connect::username` / `password`
- `BrokerUrlError::InvalidHost` for malformed bracketed IPv6 hosts (breaking: enum is now `#[non_exhaustive]`)
- `AppError::SessionPresentInconsistent` when CONNACK Session Present disagrees with Clean Start / process-local session affinity
- `ConnectOptions::max_packet_size` (default [`DEFAULT_MAX_PACKET_SIZE`] = 1 MiB) advertised on CONNECT and enforced on the inbound decoder

### Fixed

- Reject outbound packets whose body exceeds the Remaining Length ceiling (268,435,455 bytes, §2.1.4) with `EncodeError::InvalidLength` before any wire bytes: a multi-GiB PUBLISH payload could previously truncate the internal `u32` size cast when no Maximum Packet Size was negotiated (corrupt frame), and an oversize control body could leave a partial fixed header in the write buffer
- Reject CONNACK `Maximum Packet Size` of `0` on decode (§3.2.2.3.6, Protocol Error → `DecodeError::InvalidProtocol`); it was accepted and, because the codec treats limit `0` as unbounded, silently disabled outbound size enforcement
- Validate outbound Response Topics before wire bytes: PUBLISH `response_topic` ([MQTT-3.3.2-13/14], empty via [MQTT-4.7.3-1]) and Will Topic / Will Response Topic at codec encode ([MQTT-3.1.3-11]) reject empty / wildcard / U+0000
- Enforce [MQTT-1.5.4-2] on the client send paths: CONNECT (including Will properties and `auth_method`) and PUBLISH properties reject U+0000 at plan time, keeping the hot encode loop single-pass; SUBSCRIBE/UNSUBSCRIBE filters were already rejected by write-task validation. Server-shaped packets (CONNACK/SUBACK) with reason strings are never emitted by the client; outbound DISCONNECT uses the minimal form without UTF-8 diagnostic fields
- `Packet::encode_wire` rolls back the fixed header when body encoding fails, so a rejected packet leaves no stray header bytes in a batched buffer (same contract as `encode_publish`)
- Harden ACK property budgeting: no u32 wrap below the header budget, and the Reason String trim check now mirrors the size path
- `BrokerUrl` parses bracketed IPv6 literals (`mqtt://[::1]:1883`, `mqtts://[2001:db8::1]`)
- Inbound decoder is no longer capped by the broker's CONNACK `Maximum Packet Size`; that limit constrains client→server packets only ([MQTT-3.2.2-15]). The client advertises its own limit in CONNECT ([MQTT-3.1.2-24]) via `ConnectOptions::max_packet_size` (default [`DEFAULT_MAX_PACKET_SIZE`] = 1 MiB) and enforces it on the inbound decoder
- Session Present consistency: stop (no reconnect loop) when Clean Start = 1 but Session Present = 1 ([MQTT-3.2.2-2]), or when a resume CONNECT gets Session Present = 1 while this process has no local session affinity ([MQTT-3.2.2-4]); affinity is process-local and independent of an empty subscription registry after unsubscribe-all. `clean_start = false` with `session_present = false` re-SUBSCRIBEs; `SessionParams` exposes `session_present`
- Auto-resubscribe validates the SUBACK reason-code count against the sent filters ([MQTT-3.8.4-6]); empty or short status lists are protocol violations
- Command channel stays open while credentials are invalidated on an auth-failure reconnect (invalidation now runs after the channels are replenished, and also when the backoff is cancelled by shutdown)
- `connect` / `connect_with_shared` reject `clean_start = false` with an empty / missing client ID ([MQTT-3.1.3-7])

### Changed

- Breaking: remove unused `AppError::NotImplemented` (test stub exits map to `AppError::Internal`)
- Breaking: codec fields renamed to MQTT 5 terms: `session_expiry_interval_secs` → `session_expiry_interval`, will `message` → `payload`, `is_utf8_payload` → `payload_format_indicator`, `will_delay_interval_sec` → `will_delay_interval`, `server_keepalive_sec` → `server_keep_alive`
- Breaking: `UserProperty` is a struct (`name` / `value`) with `From<(K, V)>` instead of a `(ByteString, ByteString)` alias
- perf(codec): Topic Name validation (`NUL`/`+`/`#`) scans with SIMD `memchr3` for topics ≥ 32 bytes on encode and decode (~20–25 % faster PUBLISH encode/decode with long topics; short topics keep the branch-free loop), and the Remaining Length minimality check ([MQTT-1.5.5-1]) is a single last-byte test instead of a recomputed width; wire behavior is unchanged
- `codec_throughput` bench now also measures the decode path (PUBLISH shapes + minimal PUBACK) and property-block encode
- Trim crates.io package: exclude `benches/`, `tests/`, `BENCHMARKS.md`, and `CONTRIBUTING.md` (still in git)

## [0.1.1] - 2026-07-27

### Fixed

- Inbound PUBLISH `Subscription Identifier` (including illegal `0`) is parsed and discarded instead of disconnecting; `0` is logged at `debug`
- `DecodeError::MalformedPacket` now carries an ops-facing reason (`decode error: Malformed packet: …`)

## [0.1.0] - 2026-07-24

### Added

- Initial public release of the MQTT 5 Tokio client (MQTT 3.1.1 out of scope)
- Dual-path read/write connection driver with keepalive, reconnect hooks, and graceful `DISCONNECT`
- Enqueue-before-ack ingress via injectable `InboundSink` (`flume` / `mpsc` / `.on_inbound` / `.discard_inbound`)
- `ClientBuilder` connect path and power `connect` API
- Outbound publish: `publish` / `publish_with`, `start_publish` + `PublishCompletion`, `publish_window`, and `publish_batch` for same-QoS bursts
- QoS 0 and 1 only (QoS 2 rejected at the API); shared subscription helpers; MQTT 5 publish property builders
- Injectable `CredentialsProvider` / `ReconnectPolicy` (no OAuth/token manager in-crate)
- rustls TLS with exactly one crypto feature: `tls-rustls-aws-lc` (default) or `tls-rustls-ring`
- Absolute multi-client bench matrix (`mqtt_compare`) vs `rumqttc-next` / LabOverWire `mqtt5`; Mosquitto cutover baseline and mock ceiling in `BENCHMARKS.md`
