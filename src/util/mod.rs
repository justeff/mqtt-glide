//! Shared helpers (client ID generation, broker URL and proxy parsing, shared filters).

pub mod broker_url;
pub mod client_id;
pub mod proxy;
pub mod shared_filter;

pub use broker_url::{BrokerUrl, BrokerUrlError};
pub use client_id::{ClientId, ClientIdError};
pub use proxy::HttpProxy;
pub use shared_filter::{ShareNameError, is_shared_subscription_filter, shared_filter, validate_share_name};
