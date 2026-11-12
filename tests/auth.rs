//! Enhanced authentication (§4.12) against the `TestBroker`: the CONNECT exchange and
//! re-authentication on an open connection.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use mqtt_glide::testing::{AuthRequest, AuthStep, TestBroker};
use mqtt_glide::{
    AppError, AuthError, AuthExchange, Authenticator, ClientBuilder, ConnectAckReason, ConnectError, InboundMessage,
    MqttClient, QoS, StandardReconnectPolicy, TransportError,
};
use tokio::sync::mpsc;

const METHOD: &str = "nonce-token";
const WAIT: Duration = Duration::from_secs(2);

/// Sends `hello`, answers a challenge `c` with `signed:c`, and trusts a broker whose final data
/// is `server-ok`.
#[derive(Default)]
struct NonceToken {
    starts: AtomicUsize,
    invalidations: AtomicUsize,
    fail_start: AtomicBool,
    finished_with: Arc<Mutex<Vec<Option<Bytes>>>>,
}

struct NonceExchange {
    finished_with: Arc<Mutex<Vec<Option<Bytes>>>>,
}

impl Authenticator for NonceToken {
    type Exchange = NonceExchange;

    fn method(&self) -> &str {
        METHOD
    }

    async fn start(&self) -> Result<(NonceExchange, Option<Bytes>), AuthError> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        if self.fail_start.load(Ordering::SeqCst) {
            return Err(AuthError::Provider("token endpoint down".into()));
        }
        let exchange = NonceExchange {
            finished_with: Arc::clone(&self.finished_with),
        };
        Ok((exchange, Some(Bytes::from_static(b"hello"))))
    }

    async fn invalidate(&self) {
        self.invalidations.fetch_add(1, Ordering::SeqCst);
    }
}

impl AuthExchange for NonceExchange {
    async fn respond(&mut self, challenge: Option<Bytes>) -> Result<Option<Bytes>, AuthError> {
        let challenge = challenge.ok_or_else(|| AuthError::Provider("challenge without nonce".into()))?;
        Ok(Some([&b"signed:"[..], &challenge].concat().into()))
    }

    async fn finish(&mut self, data: Option<Bytes>) -> Result<(), AuthError> {
        self.finished_with.lock().expect("lock").push(data.clone());
        match data.as_deref() {
            Some(b"server-ok") => Ok(()),
            _ => Err(AuthError::NotAuthorized),
        }
    }
}

/// The broker side of [`NonceToken`]; `reauth` answers re-authentications. Returns the log
/// of every step it was asked.
fn script(broker: &TestBroker, final_data: &'static [u8], reauth: AuthStep) -> Arc<Mutex<Vec<AuthRequest>>> {
    let log = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&log);
    broker.enhanced_auth(METHOD, move |request| {
        seen.lock().expect("lock").push(request.clone());
        match request.data.as_deref() {
            Some(b"hello") => AuthStep::Continue(Bytes::from_static(b"nonce")),
            Some(b"signed:nonce") if request.reauthentication => reauth.clone(),
            Some(b"signed:nonce") => AuthStep::Accept(Some(Bytes::from_static(final_data))),
            _ => AuthStep::Refuse,
        }
    });
    log
}

async fn client(
    broker: &TestBroker,
    authenticator: &Arc<NonceToken>,
) -> Result<(MqttClient, mpsc::Receiver<InboundMessage>), ConnectError> {
    let (tx, rx) = mpsc::channel(16);
    let client = ClientBuilder::new(broker.url())
        .expect("url")
        .client_id("authenticated")
        .expect("client id")
        .authenticator(Arc::clone(authenticator))
        .reconnect(StandardReconnectPolicy::new(
            Duration::from_millis(20),
            Duration::from_millis(20),
        ))
        .inbound_sink(tx)
        .connect()
        .await?;
    Ok((client, rx))
}

/// Publishes to itself, so the connection is shown to carry messages.
async fn round_trip(client: &MqttClient, rx: &mut mpsc::Receiver<InboundMessage>, payload: &'static str) {
    client
        .publish("auth/echo", payload)
        .qos(QoS::AtLeastOnce)
        .await
        .expect("publish");
    let message = tokio::time::timeout(WAIT, rx.recv())
        .await
        .expect("in time")
        .expect("open");
    assert_eq!(&message.payload[..], payload.as_bytes());
}

fn requests(log: &Mutex<Vec<AuthRequest>>) -> Vec<(Option<Bytes>, bool)> {
    log.lock()
        .expect("lock")
        .iter()
        .map(|request| (request.data.clone(), request.reauthentication))
        .collect()
}

fn data(bytes: &'static [u8]) -> Option<Bytes> {
    Some(Bytes::from_static(bytes))
}

/// CONNECT carries the method and first data, the challenge is answered with AUTH before the
/// CONNACK, and the CONNACK's data reaches `finish`.
#[tokio::test]
async fn connect_runs_the_challenge_exchange() {
    let broker = TestBroker::start().await.expect("broker");
    let log = script(&broker, b"server-ok", AuthStep::Refuse);
    let authenticator = Arc::new(NonceToken::default());
    let (client, mut rx) = client(&broker, &authenticator).await.expect("connect");
    client
        .subscribe([("auth/echo", QoS::AtLeastOnce)])
        .await
        .expect("subscribe");
    round_trip(&client, &mut rx, "in").await;

    assert_eq!(
        requests(&log),
        [(data(b"hello"), false), (data(b"signed:nonce"), false)]
    );
    assert_eq!(*authenticator.finished_with.lock().expect("lock"), [data(b"server-ok")]);
    assert_eq!(authenticator.invalidations.load(Ordering::SeqCst), 0);
}

/// A refusal fails the first connect as an auth error and drops the authenticator's material;
/// another method is refused with 0x8C; a broker `finish` distrusts ends the connect too.
#[tokio::test]
async fn refused_or_distrusted_authentication_fails_the_first_connect() {
    let broker = TestBroker::start().await.expect("broker");
    broker.enhanced_auth(METHOD, |_| AuthStep::Refuse);
    let authenticator = Arc::new(NonceToken::default());
    let error = client(&broker, &authenticator).await.expect_err("refused");
    assert!(
        matches!(error, ConnectError::Auth(AuthError::NotAuthorized)),
        "{error:?}"
    );
    assert_eq!(authenticator.invalidations.load(Ordering::SeqCst), 1);

    broker.enhanced_auth("another-method", |_| AuthStep::Accept(None));
    let error = client(&broker, &authenticator).await.expect_err("refused");
    assert!(
        matches!(
            error,
            ConnectError::Transport(TransportError::ConnectRefused {
                reason: ConnectAckReason::BadAuthenticationMethod,
                ..
            })
        ),
        "{error:?}"
    );

    script(&broker, b"impostor", AuthStep::Refuse);
    let error = client(&broker, &authenticator).await.expect_err("distrusted");
    assert!(
        matches!(error, ConnectError::Auth(AuthError::NotAuthorized)),
        "{error:?}"
    );
    // The client returns once it has sent its DISCONNECT; the broker unregisters it when it
    // reads that, a moment later.
    tokio::time::timeout(Duration::from_secs(5), async {
        while !broker.connected_clients().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the distrusted client is gone");

    authenticator.fail_start.store(true, Ordering::SeqCst);
    let error = client(&broker, &authenticator).await.expect_err("no token");
    assert!(matches!(error, ConnectError::Auth(AuthError::Provider(_))), "{error:?}");
}

/// Re-authentication runs a fresh exchange on the open connection while messages keep
/// flowing; the connection stays.
#[tokio::test]
async fn reauthentication_keeps_the_connection() {
    let broker = TestBroker::start().await.expect("broker");
    let log = script(
        &broker,
        b"server-ok",
        AuthStep::Accept(Some(Bytes::from_static(b"server-ok"))),
    );
    let authenticator = Arc::new(NonceToken::default());
    let (client, mut rx) = client(&broker, &authenticator).await.expect("connect");
    client
        .subscribe([("auth/echo", QoS::AtLeastOnce)])
        .await
        .expect("subscribe");

    let (reauthenticated, ()) = tokio::join!(client.reauthenticate(), round_trip(&client, &mut rx, "during"));
    reauthenticated.expect("re-authenticated");
    client.reauthenticate().await.expect("again");
    round_trip(&client, &mut rx, "after").await;

    let reauthentications: Vec<_> = requests(&log).into_iter().filter(|(_, reauth)| *reauth).collect();
    assert_eq!(
        reauthentications,
        [
            (data(b"hello"), true),
            (data(b"signed:nonce"), true),
            (data(b"hello"), true),
            (data(b"signed:nonce"), true)
        ]
    );
    assert_eq!(authenticator.finished_with.lock().expect("lock").len(), 3);
    assert_eq!(broker.connect_count(), 1);
    assert_eq!(authenticator.starts.load(Ordering::SeqCst), 3);
}

/// A broker refusing the re-authentication closes the connection; the client reconnects and
/// authenticates anew with fresh material.
#[tokio::test]
async fn refused_reauthentication_reconnects() {
    let broker = TestBroker::start().await.expect("broker");
    script(&broker, b"server-ok", AuthStep::Refuse);
    let authenticator = Arc::new(NonceToken::default());
    let (client, mut rx) = client(&broker, &authenticator).await.expect("connect");
    client
        .subscribe([("auth/echo", QoS::AtLeastOnce)])
        .await
        .expect("subscribe");

    assert_eq!(
        client.reauthenticate().await,
        Err(AppError::Auth(AuthError::NotAuthorized))
    );
    assert!(broker.wait_for_connects(2, WAIT).await, "reconnected");
    assert_eq!(authenticator.invalidations.load(Ordering::SeqCst), 1);
    round_trip(&client, &mut rx, "back").await;
}

/// A re-authentication that cannot start sends nothing and leaves the connection alone.
#[tokio::test]
async fn reauthentication_that_cannot_start_keeps_the_connection() {
    let broker = TestBroker::start().await.expect("broker");
    script(&broker, b"server-ok", AuthStep::Refuse);
    let authenticator = Arc::new(NonceToken::default());
    let (client, mut rx) = client(&broker, &authenticator).await.expect("connect");
    client
        .subscribe([("auth/echo", QoS::AtLeastOnce)])
        .await
        .expect("subscribe");

    authenticator.fail_start.store(true, Ordering::SeqCst);
    let error = client.reauthenticate().await.expect_err("no token");
    assert!(matches!(error, AppError::Auth(AuthError::Provider(_))), "{error:?}");
    round_trip(&client, &mut rx, "still").await;
    assert_eq!(broker.connect_count(), 1);
}

/// `reauthenticate` needs an authenticator, a connection, and a client that is not shutting
/// down.
#[tokio::test]
async fn reauthenticate_needs_an_authenticator_and_a_connection() {
    let broker = TestBroker::start().await.expect("broker");
    script(&broker, b"server-ok", AuthStep::Accept(None));
    let plain = ClientBuilder::new(broker.url())
        .expect("url")
        .discard_inbound()
        .connect()
        .await
        .expect("connect");
    assert!(matches!(plain.reauthenticate().await, Err(AppError::InvalidRequest(_))));

    let authenticator = Arc::new(NonceToken::default());
    let (tx, _rx) = mpsc::channel(4);
    let client = ClientBuilder::new(broker.url())
        .expect("url")
        .authenticator(Arc::clone(&authenticator))
        .reconnect(StandardReconnectPolicy::new(
            Duration::from_secs(60),
            Duration::from_secs(60),
        ))
        .inbound_sink(tx)
        .connect()
        .await
        .expect("connect");
    broker.drop_connections();
    let mut phase = client.session_phase();
    tokio::time::timeout(WAIT, phase.wait_for(|_| !client.is_connected()))
        .await
        .expect("noticed")
        .expect("watch");
    assert_eq!(client.reauthenticate().await, Err(AppError::NotConnected));

    client.shutdown().await.expect("shutdown");
    assert_eq!(client.reauthenticate().await, Err(AppError::ShuttingDown));
}

/// A connection lost under a pending or just-made request answers it at once, not after the
/// next connection came up (the reconnect waits a minute here).
#[tokio::test]
async fn reauthenticate_does_not_wait_for_the_next_connection() {
    let broker = TestBroker::start().await.expect("broker");
    script(
        &broker,
        b"server-ok",
        AuthStep::Accept(Some(Bytes::from_static(b"server-ok"))),
    );
    for _ in 0..20 {
        let authenticator = Arc::new(NonceToken::default());
        let client = ClientBuilder::new(broker.url())
            .expect("url")
            .authenticator(Arc::clone(&authenticator))
            .reconnect(StandardReconnectPolicy::new(
                Duration::from_secs(60),
                Duration::from_secs(60),
            ))
            .discard_inbound()
            .connect()
            .await
            .expect("connect");
        let reauthenticate = tokio::spawn({
            let client = client.clone();
            async move { client.reauthenticate().await }
        });
        broker.drop_connections();
        let result = tokio::time::timeout(WAIT, reauthenticate)
            .await
            .expect("answered without a new connection")
            .expect("join");
        assert!(
            matches!(result, Ok(()) | Err(AppError::ConnectionLost | AppError::NotConnected)),
            "{result:?}"
        );
        let late = tokio::time::timeout(WAIT, client.reauthenticate())
            .await
            .expect("answered");
        assert!(
            matches!(late, Err(AppError::NotConnected | AppError::ConnectionLost)),
            "{late:?}"
        );
        client.shutdown().await.expect("shutdown");
    }
}

/// `Duration::MAX` as "no timeout" works for re-authentication and the connect budget too: the
/// step deadline used to overflow `Instant` and panic the read task, which stopped the client
/// for good.
#[tokio::test]
async fn reauthentication_with_unbounded_timeouts() {
    let broker = TestBroker::start().await.expect("broker");
    script(
        &broker,
        b"server-ok",
        AuthStep::Accept(Some(Bytes::from_static(b"server-ok"))),
    );
    let authenticator = Arc::new(NonceToken::default());
    let (tx, mut rx) = mpsc::channel(16);
    let client = ClientBuilder::new(broker.url())
        .expect("url")
        .authenticator(Arc::clone(&authenticator))
        .options(|options| options.ack_timeout(Duration::MAX).connect_timeout(Duration::MAX))
        .inbound_sink(tx)
        .connect()
        .await
        .expect("connect");
    client
        .subscribe([("auth/echo", QoS::AtLeastOnce)])
        .await
        .expect("subscribe");

    tokio::time::timeout(WAIT, client.reauthenticate())
        .await
        .expect("in time")
        .expect("re-authenticated");
    round_trip(&client, &mut rx, "after").await;
    assert_eq!(broker.connect_count(), 1);
}
