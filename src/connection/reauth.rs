//! Re-authentication on an open connection (§4.12.1), run by the read task.
//!
//! The read task sees the broker's AUTH packets, so it drives the exchange: it takes requests
//! from [`crate::MqttClient::reauthenticate`], runs the [`Authenticator`](crate::Authenticator)
//! steps next to reading (never blocking the socket on them), and queues the client's AUTH
//! packets on the control path to the write task. Publishes and acknowledgements keep flowing
//! meanwhile.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{OwnedMutexGuard, mpsc, oneshot};
use tokio::time::{Instant, Sleep, sleep_until};

use super::ControlMessage;
use super::transport::check_broker_auth;
use crate::auth::{ExchangeErased, SharedAuthenticator, Started, check_auth_data};
use crate::codec::{Auth, AuthReasonCode, DisconnectReasonCode};
use crate::error::{AppError, AuthError, ConnectionOutcome, ProtocolError, TransportError};

use super::read::ReadOutcome;

/// Answer to one [`crate::MqttClient::reauthenticate`] call.
pub(crate) type ReauthReply = oneshot::Sender<Result<(), AppError>>;

/// Requests waiting for the read task; a few, so a call made during an exchange runs after it.
const REQUEST_CAPACITY: usize = 4;

/// What every connection's read task needs to run re-authentications.
#[derive(Debug, Clone)]
pub(crate) struct ReauthLink {
    authenticator: SharedAuthenticator,
    /// Taken by the read task of the current connection.
    requests: Arc<tokio::sync::Mutex<mpsc::Receiver<ReauthReply>>>,
    ack_timeout: Duration,
}

/// The client's end of the request queue and the driver's link to it.
pub(crate) fn reauth_channel(
    authenticator: SharedAuthenticator,
    ack_timeout: Duration,
) -> (mpsc::Sender<ReauthReply>, ReauthLink) {
    let (tx, rx) = mpsc::channel(REQUEST_CAPACITY);
    let link = ReauthLink {
        authenticator,
        requests: Arc::new(tokio::sync::Mutex::new(rx)),
        ack_timeout,
    };
    (tx, link)
}

type Step<T> = std::pin::Pin<Box<dyn Future<Output = T> + Send>>;

/// A finished [`AuthExchange::respond`](crate::AuthExchange::respond): the exchange back, and
/// the answer.
type Responded = (Box<dyn ExchangeErased>, Result<Option<Bytes>, AuthError>);

enum State {
    Idle,
    /// [`Authenticator::start`](crate::Authenticator::start) runs; nothing sent yet.
    Starting {
        reply: ReauthReply,
        step: Step<Result<Started, AuthError>>,
    },
    /// An AUTH went out; the broker's answer is due before the timer fires.
    Waiting {
        reply: ReauthReply,
        exchange: Box<dyn ExchangeErased>,
    },
    /// [`AuthExchange::respond`](crate::AuthExchange::respond) runs.
    Responding {
        reply: ReauthReply,
        step: Step<Responded>,
    },
    /// [`AuthExchange::finish`](crate::AuthExchange::finish) runs.
    Finishing {
        reply: ReauthReply,
        step: Step<Result<(), AuthError>>,
    },
}

/// Something the exchange waited for.
pub(crate) enum Event {
    Request(ReauthReply),
    Started(Result<Started, AuthError>),
    Responded(Box<dyn ExchangeErased>, Result<Option<Bytes>, AuthError>),
    Finished(Result<(), AuthError>),
    TimedOut,
}

/// Ends the connection: DISCONNECT with `reason` (none when the write side is gone), then the
/// read task returns `outcome`.
#[derive(Debug)]
pub(crate) struct Failure {
    pub(crate) reason: Option<DisconnectReasonCode>,
    pub(crate) outcome: ReadOutcome,
}

/// Re-authentication state of one connection's read task.
pub(crate) struct Reauth {
    authenticator: SharedAuthenticator,
    requests: OwnedMutexGuard<mpsc::Receiver<ReauthReply>>,
    ack_timeout: Duration,
    state: State,
    /// Deadline of the running step. One timer per connection, re-armed per step: the read
    /// loop polls the exchange on every pass and must not register a fresh timer each time.
    timer: Pin<Box<Sleep>>,
    /// Time the read task spent away from the exchange since the timer was armed, added to the
    /// deadline only when the timer fires, so a busy inbound path does not reset it per message.
    held_up: Duration,
}

impl Reauth {
    /// Takes the request queue for this connection. `None` if another read task still holds
    /// it, which the supervisor rules out (it joins a session's tasks before the next starts).
    pub(crate) fn new(link: &ReauthLink) -> Option<Self> {
        let Ok(requests) = Arc::clone(&link.requests).try_lock_owned() else {
            tracing::warn!("re-authentication requests still held by the previous connection");
            return None;
        };
        Some(Self {
            authenticator: link.authenticator.clone(),
            requests,
            ack_timeout: link.ack_timeout,
            state: State::Idle,
            timer: Box::pin(sleep_until(Instant::now())),
            held_up: Duration::ZERO,
        })
    }

    /// Whether an exchange runs (and with it a deadline).
    pub(crate) fn busy(&self) -> bool {
        !matches!(self.state, State::Idle)
    }

    /// Whether the broker's answer is awaited.
    #[cfg(test)]
    fn waiting(&self) -> bool {
        matches!(self.state, State::Waiting { .. })
    }

    /// Moves the deadline by time the read task spent away from the exchange (a full inbound
    /// sink): neither the broker's answer that arrived meanwhile nor an authenticator step that
    /// was not polled is late.
    pub(crate) fn extend_deadline(&mut self, by: Duration) {
        if self.busy() {
            self.held_up += by;
        }
    }

    /// The next event. Cancel-safe: the steps are polled in place, so a `select!` that drops
    /// this future loses nothing.
    pub(crate) async fn next_event(&mut self) -> Event {
        let Self {
            state,
            requests,
            timer,
            held_up,
            ..
        } = self;
        match state {
            State::Idle => match requests.recv().await {
                Some(reply) => Event::Request(reply),
                // Every client handle is gone; nobody can ask any more.
                None => std::future::pending().await,
            },
            // Every step is bounded like the broker's answer: a hung token endpoint must not
            // hold the caller (or, mid-exchange, the broker) forever.
            State::Starting { step, .. } => match bounded(step, timer, held_up).await {
                Some(started) => Event::Started(started),
                None => Event::TimedOut,
            },
            State::Waiting { .. } => {
                expired(timer, held_up).await;
                Event::TimedOut
            }
            State::Responding { step, .. } => match bounded(step, timer, held_up).await {
                Some((exchange, result)) => Event::Responded(exchange, result),
                None => Event::TimedOut,
            },
            State::Finishing { step, .. } => match bounded(step, timer, held_up).await {
                Some(finished) => Event::Finished(finished),
                None => Event::TimedOut,
            },
        }
    }

    /// Advances the exchange after `event`.
    pub(crate) async fn handle(
        &mut self,
        event: Event,
        control_tx: &mpsc::Sender<ControlMessage>,
    ) -> Result<(), Failure> {
        match (std::mem::replace(&mut self.state, State::Idle), event) {
            (State::Idle, Event::Request(reply)) => {
                if !reply.is_closed() {
                    let authenticator = self.authenticator.clone();
                    let step = Box::pin(async move { authenticator.start().await });
                    self.arm();
                    self.state = State::Starting { reply, step };
                }
                Ok(())
            }
            (State::Starting { reply, .. }, Event::Started(started)) => match started {
                // [MQTT-4.12.1-1] Re-authenticate with the CONNECT's method.
                Ok((exchange, data)) if check_auth_data(&data).is_ok() => {
                    self.send(AuthReasonCode::ReAuth, data, reply, exchange, control_tx)
                        .await
                }
                // Nothing went out: the connection carries on.
                Ok((_, data)) => {
                    let error = check_auth_data(&data).expect_err("checked above");
                    let _ = reply.send(Err(AppError::Auth(error)));
                    Ok(())
                }
                Err(error) => {
                    let _ = reply.send(Err(AppError::Auth(error)));
                    Ok(())
                }
            },
            (State::Starting { reply, .. }, Event::TimedOut) => {
                let error = AuthError::Provider(format!("authenticator did not start within {:?}", self.ack_timeout));
                let _ = reply.send(Err(AppError::Auth(error)));
                Ok(())
            }
            (State::Responding { reply, .. }, Event::Responded(exchange, result)) => match result {
                // [MQTT-4.12.0-3] Continue authentication.
                Ok(data) => match check_auth_data(&data) {
                    Ok(()) => {
                        self.send(AuthReasonCode::ContinueAuth, data, reply, exchange, control_tx)
                            .await
                    }
                    Err(error) => Err(failed(reply, error)),
                },
                Err(error) => Err(failed(reply, error)),
            },
            (State::Responding { reply, .. } | State::Finishing { reply, .. }, Event::TimedOut) => {
                let error = AuthError::Provider(format!("authenticator did not answer within {:?}", self.ack_timeout));
                Err(failed(reply, error))
            }
            (State::Finishing { reply, .. }, Event::Finished(result)) => match result {
                Ok(()) => {
                    let _ = reply.send(Ok(()));
                    Ok(())
                }
                Err(error) => Err(failed(reply, error)),
            },
            (State::Waiting { reply, .. }, Event::TimedOut) => {
                let _ = reply.send(Err(AppError::ReauthenticationTimeout(self.ack_timeout)));
                // [MQTT-4.12.1-2] A failed re-authentication closes the connection.
                Err(Failure {
                    reason: Some(DisconnectReasonCode::UnspecifiedError),
                    outcome: ReadOutcome::Transport(TransportError::Connection(format!(
                        "no answer to re-authentication within {:?}",
                        self.ack_timeout
                    ))),
                })
            }
            // `next_event` only yields the event of the current state.
            (state, _) => {
                self.state = state;
                Ok(())
            }
        }
    }

    /// An AUTH from the broker.
    pub(crate) fn on_broker_auth(&mut self, auth: Auth) -> Result<(), Failure> {
        match std::mem::replace(&mut self.state, State::Idle) {
            State::Waiting {
                reply, mut exchange, ..
            } => {
                let expected = match auth.reason_code {
                    AuthReasonCode::Success => AuthReasonCode::Success,
                    _ => AuthReasonCode::ContinueAuth,
                };
                if let Err(error) = check_broker_auth(&auth, self.authenticator.method(), expected) {
                    let _ = reply.send(Err(AppError::ConnectionLost));
                    return Err(protocol_failure(error));
                }
                let data = auth.auth_data;
                self.arm();
                self.state = if expected == AuthReasonCode::Success {
                    let step = Box::pin(async move { exchange.finish(data).await });
                    State::Finishing { reply, step }
                } else {
                    let step = Box::pin(async move {
                        let result = exchange.respond(data).await;
                        (exchange, result)
                    });
                    State::Responding { reply, step }
                };
                Ok(())
            }
            // The broker answers again before the client did.
            state @ (State::Responding { .. } | State::Finishing { .. }) => {
                // `conclude` answers the caller when the read task ends.
                self.state = state;
                Err(protocol_failure(ProtocolError::Violation(
                    "broker AUTH while the client's answer was still due".into(),
                )))
            }
            // No AUTH of ours is open: tolerated like any unsolicited AUTH.
            state => {
                tracing::debug!(reason = ?auth.reason_code, "ignoring AUTH outside a re-authentication");
                self.state = state;
                Ok(())
            }
        }
    }

    /// The read task ends with `outcome`: an open request learns how. A broker DISCONNECT
    /// refusing the client is reported as such; anything else lost the connection.
    pub(crate) fn conclude(&mut self, outcome: &ReadOutcome) {
        let error = match outcome {
            ReadOutcome::BrokerDisconnect(disconnect) => match super::map_broker_disconnect_outcome(disconnect) {
                ConnectionOutcome::Auth(error) => AppError::Auth(error),
                _ => AppError::ConnectionLost,
            },
            _ => AppError::ConnectionLost,
        };
        self.fail_open(error);
    }

    fn fail_open(&mut self, error: AppError) {
        let reply = match std::mem::replace(&mut self.state, State::Idle) {
            State::Idle => None,
            State::Starting { reply, .. }
            | State::Waiting { reply, .. }
            | State::Responding { reply, .. }
            | State::Finishing { reply, .. } => Some(reply),
        };
        if let Some(reply) = reply {
            let _ = reply.send(Err(error));
        }
        // Queued requests asked about this connection; the next one authenticates afresh.
        while let Ok(reply) = self.requests.try_recv() {
            let _ = reply.send(Err(AppError::ConnectionLost));
        }
    }

    async fn send(
        &mut self,
        reason_code: AuthReasonCode,
        data: Option<Bytes>,
        reply: ReauthReply,
        exchange: Box<dyn ExchangeErased>,
        control_tx: &mpsc::Sender<ControlMessage>,
    ) -> Result<(), Failure> {
        let auth = Auth {
            reason_code,
            auth_method: Some(self.authenticator.method().clone()),
            auth_data: data,
            ..Auth::default()
        };
        if control_tx.send(ControlMessage::Auth(Box::new(auth))).await.is_err() {
            let _ = reply.send(Err(AppError::ConnectionLost));
            return Err(Failure {
                reason: None,
                outcome: ReadOutcome::PeerClosed,
            });
        }
        self.arm();
        self.state = State::Waiting { reply, exchange };
        Ok(())
    }

    /// Starts the deadline of a step that begins now.
    fn arm(&mut self) {
        self.held_up = Duration::ZERO;
        self.timer.as_mut().reset(Instant::now() + self.ack_timeout);
    }
}

/// The step's output, or `None` once its deadline passed.
async fn bounded<T>(step: &mut Step<T>, timer: &mut Pin<Box<Sleep>>, held_up: &mut Duration) -> Option<T> {
    tokio::select! {
        // Like `timeout_at`: a step that is ready wins over a deadline passing at the same time.
        biased;
        output = step => Some(output),
        () = expired(timer, held_up) => None,
    }
}

/// Completes once the deadline, moved by the time the read task was held up, has passed.
/// Cancel-safe: a move is applied to the timer before waiting on it again.
async fn expired(timer: &mut Pin<Box<Sleep>>, held_up: &mut Duration) {
    loop {
        timer.as_mut().await;
        if held_up.is_zero() {
            return;
        }
        let deadline = timer.deadline() + std::mem::take(held_up);
        timer.as_mut().reset(deadline);
    }
}

impl Drop for Reauth {
    fn drop(&mut self) {
        self.fail_open(AppError::ConnectionLost);
    }
}

/// The authenticator gave up mid-exchange: DISCONNECT 0x80 ([MQTT-4.12.1-2]).
fn failed(reply: ReauthReply, error: AuthError) -> Failure {
    let _ = reply.send(Err(AppError::Auth(error.clone())));
    Failure {
        reason: Some(DisconnectReasonCode::UnspecifiedError),
        outcome: ReadOutcome::Auth(error),
    }
}

fn protocol_failure(error: ProtocolError) -> Failure {
    Failure {
        reason: Some(DisconnectReasonCode::ProtocolError),
        outcome: ReadOutcome::Protocol(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{AuthExchange, Authenticator};
    use crate::codec::Disconnect;

    /// Sends "first", answers a challenge `c` with `answer:c`, and trusts the broker only when
    /// its final data is `server-ok`.
    #[derive(Default)]
    struct Scripted {
        fail_start: bool,
        fail_respond: bool,
        hang_start: bool,
        hang_respond: bool,
        oversized_start: bool,
    }

    struct ScriptedExchange {
        fail_respond: bool,
        hang_respond: bool,
    }

    impl Authenticator for Scripted {
        type Exchange = ScriptedExchange;

        fn method(&self) -> &str {
            "scripted"
        }

        async fn start(&self) -> Result<(ScriptedExchange, Option<Bytes>), AuthError> {
            if self.fail_start {
                return Err(AuthError::Provider("token endpoint down".into()));
            }
            if self.hang_start {
                std::future::pending::<()>().await;
            }
            let exchange = ScriptedExchange {
                fail_respond: self.fail_respond,
                hang_respond: self.hang_respond,
            };
            let first = if self.oversized_start {
                Bytes::from(vec![0; 65_536])
            } else {
                Bytes::from_static(b"first")
            };
            Ok((exchange, Some(first)))
        }
    }

    impl AuthExchange for ScriptedExchange {
        async fn respond(&mut self, challenge: Option<Bytes>) -> Result<Option<Bytes>, AuthError> {
            if self.fail_respond {
                return Err(AuthError::Provider("cannot sign".into()));
            }
            if self.hang_respond {
                std::future::pending::<()>().await;
            }
            let challenge = challenge.unwrap_or_default();
            Ok(Some([&b"answer:"[..], &challenge].concat().into()))
        }

        async fn finish(&mut self, data: Option<Bytes>) -> Result<(), AuthError> {
            match data.as_deref() {
                Some(b"server-ok") => Ok(()),
                _ => Err(AuthError::NotAuthorized),
            }
        }
    }

    struct Rig {
        requests: mpsc::Sender<ReauthReply>,
        reauth: Reauth,
        control_tx: mpsc::Sender<ControlMessage>,
        control_rx: mpsc::Receiver<ControlMessage>,
    }

    fn rig(authenticator: Scripted, ack_timeout: Duration) -> Rig {
        let (requests, link) = reauth_channel(SharedAuthenticator::new(authenticator), ack_timeout);
        let reauth = Reauth::new(&link).expect("request queue free");
        assert!(Reauth::new(&link).is_none(), "one read task at a time holds the queue");
        let (control_tx, control_rx) = mpsc::channel(8);
        Rig {
            requests,
            reauth,
            control_tx,
            control_rx,
        }
    }

    impl Rig {
        fn request(&self) -> oneshot::Receiver<Result<(), AppError>> {
            let (reply, answer) = oneshot::channel();
            self.requests.try_send(reply).expect("queued");
            answer
        }

        async fn step(&mut self) -> Result<(), Failure> {
            let event = self.reauth.next_event().await;
            self.reauth.handle(event, &self.control_tx).await
        }

        fn sent(&mut self) -> Auth {
            match self.control_rx.try_recv() {
                Ok(ControlMessage::Auth(auth)) => *auth,
                other => panic!("expected an AUTH, got {other:?}"),
            }
        }

        /// Request → `start` → AUTH 0x19 out, waiting for the broker.
        async fn started(&mut self) -> oneshot::Receiver<Result<(), AppError>> {
            let answer = self.request();
            self.step().await.expect("request");
            self.step().await.expect("started");
            assert!(self.reauth.waiting());
            answer
        }
    }

    fn broker_auth(reason_code: AuthReasonCode, method: &str, data: &'static [u8]) -> Auth {
        Auth {
            reason_code,
            auth_method: Some(method.into()),
            auth_data: Some(Bytes::from_static(data)),
            ..Auth::default()
        }
    }

    /// The whole exchange: AUTH 0x19 with the CONNECT's method ([MQTT-4.12.1-1]), a challenge
    /// answered with AUTH 0x18 ([MQTT-4.12.0-3]), every AUTH with the method
    /// ([MQTT-4.12.0-5]) and a valid Reason Code ([MQTT-3.15.2-1]) but no Reason String or
    /// User Property, which could push it over the broker's Maximum Packet Size
    /// ([MQTT-3.15.2-2], [MQTT-3.15.2-3]); the broker's Success ends it.
    #[tokio::test]
    async fn reauthentication_runs_the_exchange() {
        let mut rig = rig(Scripted::default(), Duration::from_secs(5));
        let answer = rig.started().await;
        let reauthenticate = rig.sent();
        assert_eq!(reauthenticate.reason_code, AuthReasonCode::ReAuth);
        assert_eq!(reauthenticate.auth_method.as_deref(), Some("scripted"));
        assert_eq!(reauthenticate.auth_data.as_deref(), Some(&b"first"[..]));
        assert!(reauthenticate.reason_string.is_none() && reauthenticate.user_properties.is_empty());

        rig.reauth
            .on_broker_auth(broker_auth(AuthReasonCode::ContinueAuth, "scripted", b"nonce"))
            .expect("challenge");
        assert!(!rig.reauth.waiting());
        rig.step().await.expect("responded");
        let response = rig.sent();
        assert_eq!(response.reason_code, AuthReasonCode::ContinueAuth);
        assert_eq!(response.auth_method.as_deref(), Some("scripted"));
        assert_eq!(response.auth_data.as_deref(), Some(&b"answer:nonce"[..]));
        assert!(response.reason_string.is_none() && response.user_properties.is_empty());

        // With Authentication Data the Success carries the method too (§3.15.2.1).
        let success = broker_auth(AuthReasonCode::Success, "scripted", b"server-ok");
        rig.reauth.on_broker_auth(success).expect("success");
        rig.step().await.expect("finished");
        assert_eq!(answer.await.expect("answered"), Ok(()));
        assert!(rig.control_rx.try_recv().is_err(), "nothing more sent");
    }

    /// A failing `start` sends nothing and leaves the connection alone.
    #[tokio::test]
    async fn failing_start_keeps_the_connection() {
        let mut rig = rig(
            Scripted {
                fail_start: true,
                ..Scripted::default()
            },
            Duration::from_secs(5),
        );
        let answer = rig.request();
        rig.step().await.expect("request");
        rig.step().await.expect("a failed start ends nothing");
        assert!(matches!(answer.await, Ok(Err(AppError::Auth(AuthError::Provider(_))))));
        assert!(rig.control_rx.try_recv().is_err());
        assert!(!rig.reauth.waiting());
    }

    /// The authenticator giving up or distrusting the broker mid-exchange closes the connection
    /// with DISCONNECT 0x80 ([MQTT-4.12.1-2]).
    #[tokio::test]
    async fn failing_exchange_closes_the_connection() {
        let mut rig = rig(
            Scripted {
                fail_respond: true,
                ..Scripted::default()
            },
            Duration::from_secs(5),
        );
        let answer = rig.started().await;
        rig.reauth
            .on_broker_auth(broker_auth(AuthReasonCode::ContinueAuth, "scripted", b"nonce"))
            .expect("challenge");
        let failure = rig.step().await.expect_err("respond failed");
        assert_eq!(failure.reason, Some(DisconnectReasonCode::UnspecifiedError));
        assert!(matches!(failure.outcome, ReadOutcome::Auth(AuthError::Provider(_))));
        assert!(matches!(answer.await, Ok(Err(AppError::Auth(AuthError::Provider(_))))));

        let mut rig = rig_default();
        let answer = rig.started().await;
        rig.reauth
            .on_broker_auth(broker_auth(AuthReasonCode::Success, "scripted", b"impostor"))
            .expect("success");
        let failure = rig.step().await.expect_err("finish refused the broker");
        assert_eq!(failure.reason, Some(DisconnectReasonCode::UnspecifiedError));
        assert_eq!(answer.await, Ok(Err(AppError::Auth(AuthError::NotAuthorized))));
    }

    fn rig_default() -> Rig {
        rig(Scripted::default(), Duration::from_secs(5))
    }

    /// A broker AUTH that does not fit the exchange is a Protocol Error: another method
    /// ([MQTT-4.12.0-5]), a Re-authenticate from the server, a Success with data but without
    /// the method, or a second AUTH before the client answered the first.
    #[tokio::test]
    async fn broker_auth_out_of_place_is_a_protocol_error() {
        let success_without_method = Auth {
            auth_data: Some(Bytes::from_static(b"server-ok")),
            ..Auth::default()
        };
        for auth in [
            broker_auth(AuthReasonCode::ContinueAuth, "other", b"nonce"),
            broker_auth(AuthReasonCode::ReAuth, "scripted", b"nonce"),
            success_without_method,
        ] {
            let mut rig = rig_default();
            let answer = rig.started().await;
            let failure = rig.reauth.on_broker_auth(auth).expect_err("refused");
            assert_eq!(failure.reason, Some(DisconnectReasonCode::ProtocolError));
            assert!(matches!(failure.outcome, ReadOutcome::Protocol(_)));
            assert_eq!(answer.await, Ok(Err(AppError::ConnectionLost)));
        }

        let mut rig = rig_default();
        let answer = rig.started().await;
        rig.reauth
            .on_broker_auth(broker_auth(AuthReasonCode::ContinueAuth, "scripted", b"nonce"))
            .expect("challenge");
        let failure = rig
            .reauth
            .on_broker_auth(broker_auth(AuthReasonCode::ContinueAuth, "scripted", b"again"))
            .expect_err("second challenge before the answer");
        assert_eq!(failure.reason, Some(DisconnectReasonCode::ProtocolError));
        rig.reauth.conclude(&failure.outcome);
        assert_eq!(answer.await, Ok(Err(AppError::ConnectionLost)));
    }

    /// An AUTH while no exchange is open is ignored, like without an authenticator.
    #[tokio::test]
    async fn unsolicited_auth_is_ignored() {
        let mut rig = rig_default();
        rig.reauth
            .on_broker_auth(broker_auth(AuthReasonCode::ContinueAuth, "scripted", b"x"))
            .expect("ignored");
        assert!(!rig.reauth.waiting());
    }

    /// No answer within the ack timeout closes the connection; time the read task spent on a
    /// full inbound sink does not count.
    #[tokio::test]
    async fn missing_answer_times_out_unless_the_read_task_was_held_up() {
        let mut rig = rig(Scripted::default(), Duration::from_millis(50));
        let answer = rig.started().await;
        rig.reauth.extend_deadline(Duration::from_millis(400));
        let early = tokio::time::timeout(Duration::from_millis(200), rig.reauth.next_event()).await;
        assert!(early.is_err(), "the extended deadline has not passed yet");

        let failure = rig.step().await.expect_err("timed out");
        assert_eq!(failure.reason, Some(DisconnectReasonCode::UnspecifiedError));
        assert!(matches!(failure.outcome, ReadOutcome::Transport(_)));
        assert_eq!(
            answer.await,
            Ok(Err(AppError::ReauthenticationTimeout(Duration::from_millis(50))))
        );
    }

    /// When the connection ends, the open request learns why (a broker refusal is an auth
    /// error) and queued ones that the connection lost.
    #[tokio::test]
    async fn ending_connection_answers_open_and_queued_requests() {
        let mut rig = rig_default();
        let open = rig.started().await;
        let queued = rig.request();
        let refusal = Disconnect::new(DisconnectReasonCode::NotAuthorized);
        rig.reauth.conclude(&ReadOutcome::BrokerDisconnect(Box::new(refusal)));
        assert_eq!(open.await, Ok(Err(AppError::Auth(AuthError::NotAuthorized))));
        assert_eq!(queued.await, Ok(Err(AppError::ConnectionLost)));

        let mut rig = rig_default();
        let open = rig.started().await;
        drop(rig);
        assert_eq!(open.await, Ok(Err(AppError::ConnectionLost)));
    }

    /// The authenticator's steps are bounded by the ack timeout too: a hung `start` fails the
    /// request and leaves the connection (nothing was sent), a hung answer closes it with
    /// DISCONNECT 0x80 ([MQTT-4.12.1-2]). Data longer than a two-byte length is refused the same
    /// way before it reaches the encoder.
    #[tokio::test]
    async fn hung_or_oversized_authenticator_steps_fail() {
        let mut hung_start = rig(
            Scripted {
                hang_start: true,
                ..Scripted::default()
            },
            Duration::from_millis(50),
        );
        let answer = hung_start.request();
        hung_start.step().await.expect("request");
        hung_start.step().await.expect("a hung start ends nothing");
        assert!(matches!(answer.await, Ok(Err(AppError::Auth(AuthError::Provider(_))))));
        assert!(hung_start.control_rx.try_recv().is_err());

        let mut oversized = rig(
            Scripted {
                oversized_start: true,
                ..Scripted::default()
            },
            Duration::from_secs(5),
        );
        let answer = oversized.request();
        oversized.step().await.expect("request");
        oversized.step().await.expect("oversized data ends nothing");
        assert!(matches!(answer.await, Ok(Err(AppError::Auth(AuthError::Provider(_))))));
        assert!(oversized.control_rx.try_recv().is_err());

        let mut hung_respond = rig(
            Scripted {
                hang_respond: true,
                ..Scripted::default()
            },
            Duration::from_millis(50),
        );
        let answer = hung_respond.started().await;
        hung_respond
            .reauth
            .on_broker_auth(broker_auth(AuthReasonCode::ContinueAuth, "scripted", b"nonce"))
            .expect("challenge");
        let failure = hung_respond.step().await.expect_err("the answer never came");
        assert_eq!(failure.reason, Some(DisconnectReasonCode::UnspecifiedError));
        assert!(matches!(failure.outcome, ReadOutcome::Auth(AuthError::Provider(_))));
        assert!(matches!(answer.await, Ok(Err(AppError::Auth(AuthError::Provider(_))))));
    }
}
