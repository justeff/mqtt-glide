//! Extension hooks: `CredentialsProvider` + `StandardReconnectPolicy::mark_live`.
//!
//! - `credentials()` runs before each `CONNECT` (async; return username + password).
//! - `invalidate()` runs on CONNECT-level `NotAuthorized` only (never SUBACK ACL).
//! - `mark_live()` arms one immediate transport retry after the session is fully ready.
//!
//! ```shell
//! MQTT_GLIDE_BROKER=mqtt://127.0.0.1:1883 cargo run --example hooks
//! ```

use std::sync::Arc;
use std::time::Duration;

use mqtt_glide::{AuthError, ClientBuilder, CredentialsProvider, QoS, StandardReconnectPolicy};
use secrecy::SecretString;
use tokio::sync::Mutex;

/// Example token cache. Real apps fetch/refresh OAuth (or similar) here.
struct TokenProvider {
    token: Arc<Mutex<String>>,
}

impl CredentialsProvider for TokenProvider {
    async fn credentials(&self) -> Result<(String, SecretString), AuthError> {
        let token = self.token.lock().await.clone();
        Ok(("oauth2_client".into(), SecretString::from(token)))
    }

    async fn invalidate(&self) {
        // Evict so the next `credentials()` call refreshes.
        *self.token.lock().await = "refreshed-token".into();
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let broker = std::env::var("MQTT_GLIDE_BROKER").unwrap_or_else(|_| "mqtt://127.0.0.1:1883".into());
    let (tx, _rx) = flume::bounded(256);

    let policy = StandardReconnectPolicy::new(
        Duration::from_secs(3),   // transport / protocol delay
        Duration::from_secs(300), // auth-streak backoff cap
    );

    let client = ClientBuilder::new(&broker)?
        .credentials(TokenProvider {
            token: Arc::new(Mutex::new("initial-token".into())),
        })
        .reconnect(policy.clone())
        .inbound_sink(tx)
        .connect()
        .await?;

    client.subscribe([("sensors/+/temp", QoS::AtLeastOnce)]).await?;

    // Session fully ready — next transport blip retries immediately once.
    policy.mark_live();

    client
        .publish_with()
        .topic("sensors/room1/temp")
        .payload("22.5")
        .qos(QoS::AtLeastOnce)
        .send()
        .await?;

    client.shutdown().await?;
    Ok(())
}
