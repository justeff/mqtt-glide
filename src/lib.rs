//! MQTT 5 Tokio client.
//!
//! Application-facing types live at the crate root; the wire codec (packet types, framing,
//! encode helpers) is under [`codec`]. Connection options, error
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
//! Crate overview: see the [repository README](https://github.com/justeff/mqtt-glide).
//! MUST tracker:
//! `tests/conformance/checklist.md`.

#[cfg(all(feature = "tls-rustls-aws-lc", feature = "tls-rustls-ring"))]
compile_error!("features `tls-rustls-aws-lc` and `tls-rustls-ring` are mutually exclusive");

#[cfg(not(any(feature = "tls-rustls-aws-lc", feature = "tls-rustls-ring")))]
compile_error!("enable exactly one of `tls-rustls-aws-lc` or `tls-rustls-ring`");

pub mod builder;
pub mod client;
pub mod codec;
pub mod connection;
pub mod error;
pub mod inbound_sink;
pub mod options;
pub mod policies;
pub mod publish;
pub mod publish_window;
pub mod util;

pub use client::{
    AnonymousCredentials, ClientBuilder, CredentialsProvider, MqttClient, NoReconnect, Publish, PublishBuilder,
    PublishCompletion, PublishWindow, ReconnectPolicy, StaticCredentials, Subscription, connect,
};
// Application-facing message and option types. Wire-level packet types, the codec and the
// encode helpers stay under [`codec`].
pub use codec::{
    ConnectAckReason, DEFAULT_MAX_PACKET_SIZE, DisconnectReasonCode, PublishAckReason, PublishMessage,
    PublishProperties, QoS, RetainHandling, SubscribeAckReason, SubscriptionOptions, UnsubscribeAckReason,
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
pub use util::{
    BrokerUrl, BrokerUrlError, ClientId, ClientIdError, ShareNameError, is_shared_subscription_filter, shared_filter,
    validate_share_name,
};

// Compile README code samples as doctests (`cargo test --doc`).
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
mod readme_doctests {}
