# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

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
