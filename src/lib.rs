//! MQTT 5 Tokio client.
//!
//! Application-facing types live at the crate root; the wire codec (packet types, framing,
//! encode helpers) is internal. Connection options, error
//! types, credential/reconnect hooks, and the [`connect`] entry point define the
//! public application contract. For a higher-level connect path with defaults, see
//! [`ClientBuilder`]. Session I/O is driven by [`Connection::run`], backed by the
//! internal connection read/write loops.
//!
//! # TLS crypto provider
//!
//! TLS uses rustls. Enable **exactly one** Cargo feature:
//!
//! - `tls-rustls-aws-lc` (default) — rustls + aws-lc-rs
//! - `tls-rustls-ring` — rustls + ring
//!
//! The client builds its `ClientConfig` with the selected provider explicitly
//! (`builder_with_provider`), so MQTT TLS does not depend on a process-global
//! rustls default. Do not enable both features and do not use `--all-features`.
//! Downstream crates should pin explicitly, e.g.
//! `mqtt-glide = { ..., default-features = false, features = ["tls-rustls-aws-lc"] }`.
//!
//! # Other features
//!
//! - `websocket`: `ws://` / `wss://` broker URLs (MQTT over WebSocket).
//! - `testing`: the in-process `testing::TestBroker` and `testing::FaultProxy` for tests of
//!   applications; meant for `[dev-dependencies]`.
//! - `stats`: `MqttClient::stats` returns counters of what the connection did.
//!
//! Crate overview: see the [repository README](https://github.com/justeff/mqtt-glide).
//! Spec coverage: `CONFORMANCE.md` in the repository.

#[cfg(all(feature = "tls-rustls-aws-lc", feature = "tls-rustls-ring"))]
compile_error!("features `tls-rustls-aws-lc` and `tls-rustls-ring` are mutually exclusive");

#[cfg(not(any(feature = "tls-rustls-aws-lc", feature = "tls-rustls-ring")))]
compile_error!("enable exactly one of `tls-rustls-aws-lc` or `tls-rustls-ring`");

mod ack;
pub mod builder;
mod client;
// The wire layer is internal; `__internals` opens it for the crate's own tests, benches
// and fuzz target only.
#[cfg(feature = "__internals")]
#[doc(hidden)]
pub mod codec;
#[cfg(not(feature = "__internals"))]
mod codec;
#[cfg(feature = "__internals")]
#[doc(hidden)]
pub mod connection;
#[cfg(not(feature = "__internals"))]
mod connection;
mod error;
mod inbound_sink;
mod options;
mod policies;
mod publish;
mod publish_window;
mod request;
mod stats;
#[cfg(test)]
mod test_util;
#[cfg(feature = "testing")]
pub mod testing;
mod util;

pub use client::{
    AnonymousCredentials, ClientBuilder, CredentialsProvider, MqttClient, NoReconnect, Password, Publish,
    PublishBuilder, PublishCompletion, PublishWindow, ReconnectPolicy, StaticCredentials, Subscription, connect,
};
// Application-facing message and option types. Wire-level packet types, the codec and the
// encode helpers stay internal.
pub use ack::{
    Delivery, PublishError, PublishRejection, PublishResponse, RefusedSubscription, SubscribeResponse,
    UnsubscribeResponse,
};
pub use codec::{
    ConnectAckReason, DEFAULT_MAX_PACKET_SIZE, DisconnectReasonCode, InboundMessage, PublishAckReason, PublishMessage,
    PublishProperties, QoS, RetainHandling, SubscribeAckReason, SubscriptionOptions, UnknownCode, UnsubscribeAckReason,
    UserProperties, UserProperty, WillMessage,
};
pub use connection::{Connection, LifecycleHook, NoopLifecycle, SessionParams, SessionPhase};
pub use error::{AppError, AuthError, ConnectError, ConnectionOutcome, ProtocolError, ShutdownError, TransportError};
pub use inbound_sink::{InboundSink, InboundSinkClosed, InboundTrySendError, OnInboundSink};
pub use options::{
    COMMAND_CHANNEL_CAPACITY, ConnectOptions, DEFAULT_ACK_TIMEOUT, DEFAULT_CONNECT_TIMEOUT,
    DEFAULT_GRACEFUL_DISCONNECT_TIMEOUT, DEFAULT_RECEIVE_MAXIMUM, TlsConfig,
};
pub use policies::{DEFAULT_AUTH_FAILURE_THRESHOLD, StandardReconnectPolicy};
pub use request::{DEFAULT_REQUEST_TIMEOUT, RequestBuilder, Requester};
#[cfg(feature = "stats")]
pub use stats::{ClientStats, ConnectionStats, InboundStats, OutboundStats};
pub use util::{
    BrokerUrl, BrokerUrlError, ClientId, ClientIdError, HttpProxy, ShareNameError, is_shared_subscription_filter,
    shared_filter, validate_share_name,
};

// Compile README code samples as doctests (`cargo test --doc`).
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
mod readme_doctests {}
