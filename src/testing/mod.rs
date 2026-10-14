//! Test helpers for applications built on this client (`testing` feature).
//!
//! - [`TestBroker`]: a small in-process MQTT 5 broker on a local port, so code that uses an
//!   [`MqttClient`](crate::MqttClient) can be tested without a container. It routes publishes
//!   between its clients, keeps retained messages, publishes Wills, records everything clients
//!   publish, and lets a test refuse connections, reject publishes or drop every connection.
//! - [`FaultProxy`]: a TCP proxy in front of any broker that injects network faults on demand
//!   (latency, a silent stall, packets split at arbitrary bytes, connection resets, refused
//!   connects).
//!
//! The broker is for tests, not production: no persistence, no authentication, no QoS 2, no
//! session resumption (every CONNACK says Session Present 0), and no Subscription Identifiers
//! on delivered messages.
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

pub use broker::{ReceivedPublish, TestBroker};
pub use fault_proxy::FaultProxy;
