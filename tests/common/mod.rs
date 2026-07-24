//! Shared test doubles and helpers for `mqtt-glide` integration tests.
//!
//! Included via `#[path = "../common/mod.rs"] mod common;` in each integration
//! binary (`tests/connection.rs`, `tests/brokers/main.rs`) since Cargo builds
//! every file under `tests/` as an independent binary and does not share
//! modules across them automatically.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use mqtt_glide::{AuthError, ConnectionOutcome, CredentialsProvider, ReconnectPolicy, SessionPhase};
use secrecy::SecretString;
use tokio::sync::watch;
use tokio::time::timeout;

/// Fixed username/password credentials ("user"/"pass") that never invalidate.
pub fn static_credentials() -> mqtt_glide::StaticCredentials {
    mqtt_glide::StaticCredentials::new("user", "pass")
}

/// Fixed credentials that count [`CredentialsProvider::invalidate`] calls.
pub struct CountingInvalidate {
    pub invalidations: Arc<AtomicUsize>,
}

impl CredentialsProvider for CountingInvalidate {
    async fn credentials(&self) -> Result<(String, SecretString), AuthError> {
        Ok(("user".into(), SecretString::from("pass".to_owned())))
    }

    async fn invalidate(&self) {
        self.invalidations.fetch_add(1, Ordering::SeqCst);
    }
}

/// Reconnect policy that never reconnects — a single connect attempt.
pub struct NoReconnect;

impl ReconnectPolicy for NoReconnect {
    fn delay_after(&self, _outcome: &ConnectionOutcome) -> Option<Duration> {
        None
    }
}

/// Wait until the phase watch matches `pred` or the deadline elapses.
pub async fn wait_for_phase<F>(
    phase: &mut watch::Receiver<SessionPhase>,
    deadline: Duration,
    mut pred: F,
) -> SessionPhase
where
    F: FnMut(&SessionPhase) -> bool,
{
    match timeout(deadline, phase.wait_for(|p| pred(p))).await {
        Ok(Ok(guard)) => guard.clone(),
        Ok(Err(_)) => panic!("phase watch closed before matching SessionPhase"),
        Err(_) => panic!("timed out waiting for matching SessionPhase"),
    }
}
