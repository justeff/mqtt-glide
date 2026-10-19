//! Test helpers for applications built on this client (`testing` feature).
//!
//! - [`TestBroker`]: a small in-process MQTT 5 broker on a local port, so code that uses an
//!   [`MqttClient`](crate::MqttClient) can be tested without a container. It routes publishes
//!   between its clients, keeps retained messages, publishes Wills, records everything clients
//!   publish, and lets a test refuse connections, answer publishes with a chosen PUBACK (or
//!   reject them, or lose the PUBACKs) or drop every connection. A client that connects with
//!   a Session Expiry Interval keeps its session across connections: subscriptions,
//!   unacknowledged deliveries (resent with DUP) and QoS 1 messages that arrived meanwhile.
//! - [`FaultProxy`]: a TCP proxy in front of any broker that injects network faults on demand
//!   (latency, a silent stall, packets split at arbitrary bytes, connection resets, refused
//!   connects).
//!
//! The broker is for tests, not production: sessions live in memory only (gone with the
//! broker), no authentication, no QoS 2, and no Subscription Identifiers on delivered
//! messages. Shared subscriptions deliver to their connected members only.
//!
//! # Examples
//!
//! ```no_run
//! use std::time::Duration;
//!
//! use mqtt_glide::testing::TestBroker;
//! use mqtt_glide::{ClientBuilder, QoS};
//!
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! let broker = TestBroker::start().await?;
//! let client = ClientBuilder::new(broker.url())?.discard_inbound().connect().await?;
//!
//! client.publish("plant/line-1/state", "running").qos(QoS::AtLeastOnce).await?;
//!
//! let received = broker.wait_for_publishes(1, Duration::from_secs(1)).await;
//! assert_eq!(&*received[0].topic, "plant/line-1/state");
//! # Ok(())
//! # }
//! ```

mod broker;
mod fault_proxy;

pub use broker::{PubAck, ReceivedPublish, TestBroker};
pub use fault_proxy::FaultProxy;
