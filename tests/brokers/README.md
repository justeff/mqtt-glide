# Broker Matrix Tests

Integration tests against real brokers via [testcontainers](https://github.com/testcontainers/testcontainers-rs).

## Mosquitto 2.x coverage

Image: `eclipse-mosquitto:2.1.2-alpine` (anonymous + reject-connect configs).

| Test | Expectation |
|------|-------------|
| `mosquitto_connect_connack_and_disconnect` | CONNECT/CONNACK success (`SessionParams.from_connack`), then clean shutdown |
| `mosquitto_not_authorized_invalidates_credentials` | CONNACK `AuthError::NotAuthorized` + `CredentialsProvider::invalidate` (single attempt) |
| `mosquitto_shared_subscription_delivers_to_handoff` | `$share/{group}/{topic}` SUBACK + broker PUBLISH reaches flume handoff |
| `mosquitto_qos1_publish_awaits_puback` | QoS 1 `publish().await` completes after Mosquitto PUBACK |
| `mosquitto_publish_properties_roundtrip` | User Properties + Message Expiry Interval **property** round-trip publish → subscribe (immediate delivery; broker drop-on-expiry not asserted here) |
| `mosquitto_broker_kill_reconnects_and_emits_connected` | Broker kill → Transport disconnect → policy reconnect → Connected (newer generation) |
| `mosquitto_auth_fail_stops_without_endless_reconnect` | Auth fail → Stopped + invalidate; no endless reconnect |
| `mosquitto_handoff_full_keep_alive_no_spurious_reconnect` | Full handoff stall; connection stays up past keep-alive window (no spurious reconnect). Wire PINGREQ scheduling is covered by the unit keep-alive enforcer test |
| `mosquitto_resubscribes_after_reconnect` | After reconnect, prior subscriptions deliver without re-subscribe |
| `mosquitto_session_phase_updates_app_state_without_eventloop_poll` | App health driven solely by `SessionPhase` watch |
| `mosquitto_shutdown_pubacks_before_disconnect` | Incoming QoS1 → flume → shutdown → clean disconnect (no redelivery of already-acked payload). Wire order PUBACK-before-DISCONNECT is covered by unit tests |
| `mosquitto_handoff_survives_shutdown` | Handoff message remains receivable after `shutdown()` |
| `mosquitto_two_parallel_connections_isolated_handoff_and_phase` | Two parallel `Connection`s: isolated handoffs via unique probe publishes; B’s phase stays Connected after A shutdown |

Requires a Docker-compatible API (Podman works when `DOCKER_HOST` points at the machine).

```shell
cargo test --test brokers
```

## Connection and protocol integration coverage

These tests do **not** use Mosquitto. They cover transport failure and DISCONNECT-reason classification with a minimal in-process MQTT 5 peer (same production codec on both sides — classification regression, not independent wire-conformance).

| Test | Expectation |
|------|-------------|
| `unreachable_broker_returns_transport_error` | Transport error when the endpoint is unavailable |
| `broker_disconnect_reason_classifies_auth_transport_protocol` | DISCONNECT reason maps to Auth, Transport, or Protocol |

```shell
cargo test --test connection
```

## EMQX

**Follow-up (not in this suite):** EMQX coverage for Shared Subs under load / Last is optional and is not implemented here. Add when needed; do not treat absence as silent pass.

## Helpers

| Helper | Role |
|--------|------|
| `start_mosquitto` / `start_mosquitto_on_port` / `start_mosquitto_reject_connect` | Mosquitto containers |
| `publish_qos0_via_raw_session` / `publish_qos1_via_raw_session` | Second MQTT 5 session for inbound publishes |
