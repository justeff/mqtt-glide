# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Will Message (LWT) on CONNECT: `ClientBuilder` / `ConnectOptions` `.will(topic, payload)` (+ `.will_qos` / `.will_retain`)
- Fluent codec helpers: `PublishMessage::new` / `.properties`, `PublishProperties` setters, `Connect::username` / `password`
- `BrokerUrlError::InvalidHost` for malformed bracketed IPv6 hosts

### Fixed

- Reject CONNACK `Maximum Packet Size` of `0` on decode (§3.2.2.3.6, Protocol Error); it was accepted and, because the codec treats limit `0` as unbounded, silently disabled outbound size enforcement
- Validate outbound Response Topics before wire bytes: PUBLISH `response_topic` ([MQTT-3.3.2-13/14]) and Will Topic / Will Response Topic at codec encode ([MQTT-3.1.3-11]) reject empty / wildcard / U+0000
- Enforce [MQTT-1.5.4-2] on encode: outbound UTF-8 strings containing U+0000 are rejected (decode already rejected them). Plan-time checks per packet keep the hot encode path single-pass
- Harden ACK property budgeting: no u32 wrap below the header budget, `ack_props::encode` errors on a zero budget instead of a debug-only assert, and the Reason String trim check now mirrors the size path
- `BrokerUrl` parses bracketed IPv6 literals (`mqtt://[::1]:1883`, `mqtts://[2001:db8::1]`)
- Inbound decoder is no longer capped by the broker's CONNACK `Maximum Packet Size` — that limit constrains client→server packets only ([MQTT-3.2.2-15]); inbound stays unbounded because the client never advertises its own limit in CONNECT ([MQTT-3.1.2-24])
- `clean_start = false`: re-SUBSCRIBE after reconnect when the broker reports `session_present = false` instead of silently losing the app's subscriptions; `SessionParams` now exposes `session_present`
- Auto-resubscribe validates the SUBACK reason-code count against the sent filters ([MQTT-3.9.3-1]); empty or short status lists are protocol violations
- Command channel stays open while credentials are invalidated on an auth-failure reconnect (invalidation now runs after the channels are replenished)

### Changed

- Breaking: codec fields renamed to MQTT 5 terms — `session_expiry_interval_secs` → `session_expiry_interval`, will `message` → `payload`, `is_utf8_payload` → `payload_format_indicator`, `will_delay_interval_sec` → `will_delay_interval`, `server_keepalive_sec` → `server_keep_alive`
- Breaking: `UserProperty` is a struct (`name` / `value`) with `From<(K, V)>` instead of a `(ByteString, ByteString)` alias
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
