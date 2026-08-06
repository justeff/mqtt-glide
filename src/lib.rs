//! MQTT 5 Tokio client.
//!
//! The wire codec API is re-exported at the crate root. Connection options, error
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
pub mod publish_builder;
pub mod publish_window;
pub mod util;

pub use client::{
    AnonymousCredentials, ClientBuilder, CredentialsProvider, MqttClient, NoReconnect, PublishCompletion,
    PublishWindow, PublishWith, ReconnectPolicy, StaticCredentials, Subscription, WindowPublishWith, connect,
};
pub use codec::{
    Connect, ConnectAck, ConnectAckReason, DEFAULT_MAX_PACKET_SIZE, DecodeError, DecodedFrame, Disconnect,
    DisconnectReasonCode, EncodeError, Packet, PublishAck, PublishAckReason, PublishDelivery, PublishMessage,
    PublishPacket, PublishPacketMeta, PublishProperties, QoS, RECEIVE_MAX_DEFAULT, RetainHandling, Subscribe,
    SubscribeAck, SubscribeAckReason, SubscriptionOptions, Unsubscribe, UnsubscribeAck, UnsubscribeAckReason,
    UserProperties, UserProperty, V5Codec, WillMessage, encode_publish, ensure_publish_wire_fits,
};
pub use connection::{
    Connection, ConnectionState, Generation, LifecycleHook, NoopLifecycle, SessionParams, SessionPhase, TlsMode,
};
pub use error::{AppError, AuthError, ConnectError, ConnectionOutcome, ProtocolError, ShutdownError, TransportError};
pub use inbound_sink::{InboundSink, InboundSinkClosed, InboundTrySendError, OnInboundSink};
pub use options::{
    COMMAND_CHANNEL_CAPACITY, ConnectOptions, DEFAULT_ACK_TIMEOUT, DEFAULT_CONNECT_TIMEOUT,
    DEFAULT_GRACEFUL_DISCONNECT_TIMEOUT, DEFAULT_RECEIVE_MAXIMUM, TlsConfig,
};
pub use policies::{DEFAULT_AUTH_FAILURE_THRESHOLD, StandardReconnectPolicy};
pub use util::{
    BrokerUrl, BrokerUrlError, ClientId, ClientIdError, ShareNameError, is_shared_subscription_filter,
    shared_filter, validate_share_name,
};

// Compile README code samples as doctests (`cargo test --doc`).
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
mod readme_doctests {}

// Re-export TLS decision hook for callers / structure tests.
pub use connection::resolve_tls_mode;
