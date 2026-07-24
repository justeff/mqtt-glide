//! Client adapters.

mod glide;
mod mqtt5;
mod rumqttc;

use super::harness::{BenchClient, ClientId};

pub use glide::GlideClient;
pub use mqtt5::Mqtt5Client;
pub use rumqttc::RumqttcClient;

/// Instantiates the adapter for `id`.
pub fn client_for(id: ClientId) -> Box<dyn BenchClient> {
    match id {
        ClientId::Glide => Box::new(GlideClient),
        ClientId::Rumqttc => Box::new(RumqttcClient),
        ClientId::Mqtt5 => Box::new(Mqtt5Client),
    }
}
