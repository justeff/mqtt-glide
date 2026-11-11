//! Re-authentication on an open connection (§4.12.1), run by the read task.
//!
//! The read task sees the broker's AUTH packets, so it drives the exchange: it takes requests
//! from [`crate::MqttClient::reauthenticate`], runs the [`Authenticator`](crate::Authenticator)
//! steps next to reading (never blocking the socket on them), and queues the client's AUTH
//! packets on the control path to the write task. Publishes and acknowledgements keep flowing
//! meanwhile.

use std::collections::VecDeque;
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

/// The `reauthenticate` calls waiting for the read task.
///
/// Not a channel: a caller that gave up must not keep its place while the read task is held up
/// (a stalled consumer), or a few given-up calls refused every later one as backpressure.
#[derive(Debug, Default)]
pub(crate) struct ReauthRequests {
    waiting: std::sync::Mutex<VecDeque<ReauthReply>>,
    /// `waiting.len()`: the idle read task looks at it on every pass, without the lock.
    len: std::sync::atomic::AtomicUsize,
    added: futures_util::task::AtomicWaker,
}

// `AtomicWaker` holds its waker in an `UnsafeCell`, which would take `RefUnwindSafe` from
// `MqttClient` and everything holding one (as in 0.5.0, they keep it). A panic cannot leave the
// queue half-changed: its mutex poisons, and the waker slot only ever holds a whole waker.
impl std::panic::RefUnwindSafe for ReauthRequests {}

impl ReauthRequests {
    /// Queues `reply`, or hands it back when [`REQUEST_CAPACITY`] callers still wait.
    pub(crate) fn push(&self, reply: ReauthReply) -> Result<(), ReauthReply> {
        let mut waiting = self.lock();
        waiting.retain(|reply| !reply.is_closed());
        if waiting.len() >= REQUEST_CAPACITY {
            self.len.store(waiting.len(), std::sync::atomic::Ordering::Release);
            return Err(reply);
        }
        waiting.push_back(reply);
        self.len.store(waiting.len(), std::sync::atomic::Ordering::Release);
        drop(waiting);
        self.added.wake();
        Ok(())
    }

    fn pop(&self) -> Option<ReauthReply> {
        if self.len.load(std::sync::atomic::Ordering::Acquire) == 0 {
            return None;
        }
        let mut waiting = self.lock();
        let reply = waiting.pop_front();
        self.len.store(waiting.len(), std::sync::atomic::Ordering::Release);
        reply
    }

    /// The next request. Cancel-safe: a request is taken only when this returns it.
    async fn next(&self) -> ReauthReply {
        std::future::poll_fn(|cx| {
            if let Some(reply) = self.pop() {
                return std::task::Poll::Ready(reply);
            }
            // Registered before looking again: a push in between wakes this task.
            self.added.register(cx.waker());
            self.pop().map_or(std::task::Poll::Pending, std::task::Poll::Ready)
        })
        .await
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, VecDeque<ReauthReply>> {
        self.waiting.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Broker challenges one re-authentication may take. SCRAM needs one; a broker that keeps
/// answering with Continue Authentication would otherwise keep the exchange, and every
/// `reauthenticate` call queued behind it, open for good, each round within `ack_timeout`.
const MAX_CHALLENGES: u8 = 16;

/// What every connection's read task needs to run re-authentications.
#[derive(Debug, Clone)]
pub(crate) struct ReauthLink {
    authenticator: SharedAuthenticator,
    requests: Arc<ReauthRequests>,
    /// Held by the read task of the current connection.
    taken: Arc<tokio::sync::Mutex<()>>,
    ack_timeout: Duration,
    /// The broker's Maximum Packet Size on the current connection (CONNACK).
    broker_max_packet_size: Option<u32>,
}

impl ReauthLink {
    /// The link for a connection whose CONNACK announced `broker_max_packet_size`.
    pub(crate) fn for_connection(mut self, broker_max_packet_size: Option<u32>) -> Self {
        self.broker_max_packet_size = broker_max_packet_size;
        self
    }
}

/// The client's end of the request queue and the driver's link to it.
pub(crate) fn reauth_channel(
    authenticator: SharedAuthenticator,
    ack_timeout: Duration,
) -> (Arc<ReauthRequests>, ReauthLink) {
    let requests = Arc::new(ReauthRequests::default());
    let link = ReauthLink {
        authenticator,
        requests: Arc::clone(&requests),
        taken: Arc::new(tokio::sync::Mutex::new(())),
        ack_timeout,
        broker_max_packet_size: None,
    };
    (requests, link)
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
    requests: Arc<ReauthRequests>,
    _taken: OwnedMutexGuard<()>,
    ack_timeout: Duration,
    state: State,
    /// Deadline of the running step. One timer per connection, re-armed per step: the read
    /// loop polls the exchange on every pass and must not register a fresh timer each time.
    timer: Pin<Box<Sleep>>,
    /// Time the read task spent away from the exchange since the timer was armed, added to the
    /// deadline only when the timer fires, so a busy inbound path does not reset it per message.
    held_up: Duration,
    /// Broker challenges (AUTH Continue Authentication) of the running exchange.
    challenges: u8,
    /// The deadline of the broker's answer passed once already and the frames received by then
    /// were read first (see [`Self::defer_timeout`]).
    timeout_deferred: bool,
    broker_max_packet_size: Option<u32>,
}

impl Reauth {
    /// Takes the request queue for this connection. `None` if another read task still holds
    /// it, which the supervisor rules out (it joins a session's tasks before the next starts).
    pub(crate) fn new(link: &ReauthLink) -> Option<Self> {
        let Ok(taken) = Arc::clone(&link.taken).try_lock_owned() else {
            tracing::warn!("re-authentication requests still held by the previous connection");
            return None;
        };
        Some(Self {
            authenticator: link.authenticator.clone(),
            requests: Arc::clone(&link.requests),
            _taken: taken,
            ack_timeout: link.ack_timeout,
            state: State::Idle,
            timer: Box::pin(sleep_until(Instant::now())),
            held_up: Duration::ZERO,
            challenges: 0,
            timeout_deferred: false,
            broker_max_packet_size: link.broker_max_packet_size,
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

    /// Whether a timeout of the broker's answer waits for frames already received to be read.
    ///
    /// Once per answer: a read task that was not polled for a while (a busy runtime, a full
    /// control channel) finds the timer and the broker's answer ready at the same time, and the
    /// answer must win. A broker that keeps sending other packets cannot postpone it twice.
    pub(crate) fn defer_timeout(&mut self) -> bool {
        let defer = matches!(self.state, State::Waiting { .. }) && !self.timeout_deferred;
        self.timeout_deferred |= defer;
        defer
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
            State::Idle => Event::Request(requests.next().await),
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
                    self.challenges = 0;
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
                if expected == AuthReasonCode::ContinueAuth {
                    self.challenges = self.challenges.saturating_add(1);
                    if self.challenges > MAX_CHALLENGES {
                        let error =
                            AuthError::Provider(format!("broker kept challenging: more than {MAX_CHALLENGES} rounds"));
                        return Err(failed(reply, error));
                    }
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
            // No AUTH of ours is open: only the client starts a re-authentication (§4.12.1), so
            // an AUTH here belongs to no exchange.
            state => {
                self.state = state;
                Err(protocol_failure(ProtocolError::Violation(format!(
                    "broker AUTH {:?} outside a re-authentication",
                    auth.reason_code
                ))))
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
        while let Some(reply) = self.requests.pop() {
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
        // An AUTH over the broker's Maximum Packet Size must not be sent ([MQTT-3.2.2-15]); the
        // write task would meet that as an encode error and drop the connection without a
        // word. The first one fails the call alone, nothing having been sent yet; later in
        // the exchange the client gives up on it with DISCONNECT 0x80.
        if let Some(max) = self.broker_max_packet_size
            && encoded_len(&auth).is_none_or(|len| len > max)
        {
            let error = AuthError::Provider(format!(
                "Authentication Data does not fit the broker's Maximum Packet Size of {max} bytes"
            ));
            if reason_code == AuthReasonCode::ReAuth {
                let _ = reply.send(Err(AppError::Auth(error)));
                return Ok(());
            }
            return Err(failed(reply, error));
        }
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
        self.timeout_deferred = false;
        self.timer.as_mut().reset(crate::util::deadline_after(self.ack_timeout));
    }
}

/// Bytes `auth` takes on the wire; `None` when it cannot be encoded at all.
fn encoded_len(auth: &Auth) -> Option<u32> {
    let mut buffer = bytes::BytesMut::new();
    crate::codec::V5Codec::default()
        .encode_packet(crate::codec::Packet::Auth(auth.clone()), &mut buffer)
        .ok()?;
    u32::try_from(buffer.len()).ok()
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
        let deadline = crate::util::extend_deadline(timer.deadline(), std::mem::take(held_up));
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
        requests: Arc<ReauthRequests>,
        reauth: Reauth,
        control_tx: mpsc::Sender<ControlMessage>,
        control_rx: mpsc::Receiver<ControlMessage>,
    }

    fn rig(authenticator: Scripted, ack_timeout: Duration) -> Rig {
        rig_with_max_packet_size(authenticator, ack_timeout, None)
    }

    fn rig_with_max_packet_size(authenticator: Scripted, ack_timeout: Duration, max: Option<u32>) -> Rig {
        let (requests, link) = reauth_channel(SharedAuthenticator::new(authenticator), ack_timeout);
        let link = link.for_connection(max);
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
            self.requests.push(reply).expect("queued");
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

    /// A few callers still waiting refuse the next one, but callers that gave up (their future
    /// dropped while the read task was held up) do not hold their place.
    #[test]
    fn given_up_requests_leave_room_in_the_queue() {
        let requests = ReauthRequests::default();
        let mut waiting: Vec<_> = (0..REQUEST_CAPACITY)
            .map(|_| {
                let (reply, answer) = oneshot::channel();
                requests.push(reply).expect("room");
                answer
            })
            .collect();
        let (reply, _answer) = oneshot::channel();
        assert!(requests.push(reply).is_err(), "full while every caller waits");

        waiting.truncate(1);
        let (reply, _answer) = oneshot::channel();
        requests.push(reply).expect("room after three callers gave up");
        assert_eq!(requests.lock().len(), 2);
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

    /// A broker that keeps challenging gets at most `MAX_CHALLENGES` answers; the exchange then
    /// fails with DISCONNECT 0x80 instead of staying open, with every later call queued behind
    /// it ([MQTT-4.12.1-2]).
    #[tokio::test]
    async fn endless_challenges_end_the_exchange() {
        let mut rig = rig(Scripted::default(), Duration::from_secs(5));
        let answer = rig.started().await;
        rig.sent();
        for _ in 0..MAX_CHALLENGES {
            rig.reauth
                .on_broker_auth(broker_auth(AuthReasonCode::ContinueAuth, "scripted", b"again"))
                .expect("challenge within the limit");
            rig.step().await.expect("responded");
            rig.sent();
        }
        let failure = rig
            .reauth
            .on_broker_auth(broker_auth(AuthReasonCode::ContinueAuth, "scripted", b"again"))
            .expect_err("one challenge too many");
        assert_eq!(failure.reason, Some(DisconnectReasonCode::UnspecifiedError));
        assert!(matches!(answer.await, Ok(Err(AppError::Auth(AuthError::Provider(_))))));
    }

    /// An AUTH over the broker's Maximum Packet Size is never sent ([MQTT-3.2.2-15]): the first
    /// one fails the call and leaves the connection alone, a later one ends the exchange with
    /// DISCONNECT 0x80.
    #[tokio::test]
    async fn auth_over_the_broker_maximum_packet_size_is_not_sent() {
        let first = Auth {
            reason_code: AuthReasonCode::ReAuth,
            auth_method: Some("scripted".into()),
            auth_data: Some(Bytes::from_static(b"first")),
            ..Auth::default()
        };
        let first_len = encoded_len(&first).expect("encodes");

        let mut rig = rig_with_max_packet_size(Scripted::default(), Duration::from_secs(5), Some(first_len - 1));
        let answer = rig.request();
        rig.step().await.expect("request");
        rig.step().await.expect("a refused first AUTH ends nothing");
        assert!(matches!(answer.await, Ok(Err(AppError::Auth(AuthError::Provider(_))))));
        assert!(rig.control_rx.try_recv().is_err(), "nothing sent");
        assert!(!rig.reauth.busy());

        // The first fits; the answer to a long challenge does not.
        let mut rig = rig_with_max_packet_size(Scripted::default(), Duration::from_secs(5), Some(first_len));
        let answer = rig.started().await;
        rig.sent();
        rig.reauth
            .on_broker_auth(broker_auth(
                AuthReasonCode::ContinueAuth,
                "scripted",
                b"a long challenge",
            ))
            .expect("challenge");
        let failure = rig.step().await.expect_err("the answer does not fit");
        assert_eq!(failure.reason, Some(DisconnectReasonCode::UnspecifiedError));
        assert!(matches!(answer.await, Ok(Err(AppError::Auth(AuthError::Provider(_))))));
        assert!(rig.control_rx.try_recv().is_err(), "the answer was not sent");
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

    /// An AUTH while no AUTH of the client is open is a Protocol Error: only the client starts a
    /// re-authentication (§4.12.1). A request whose `start` still runs learns that the
    /// connection was lost.
    #[tokio::test]
    async fn unsolicited_auth_is_a_protocol_error() {
        let mut rig = rig_default();
        let failure = rig
            .reauth
            .on_broker_auth(broker_auth(AuthReasonCode::ContinueAuth, "scripted", b"x"))
            .expect_err("no exchange open");
        assert_eq!(failure.reason, Some(DisconnectReasonCode::ProtocolError));

        let mut rig = rig_default();
        let answer = rig.request();
        rig.step().await.expect("request");
        let failure = rig
            .reauth
            .on_broker_auth(broker_auth(AuthReasonCode::ReAuth, "scripted", b"x"))
            .expect_err("nothing sent yet");
        assert_eq!(failure.reason, Some(DisconnectReasonCode::ProtocolError));
        rig.reauth.conclude(&failure.outcome);
        assert_eq!(answer.await, Ok(Err(AppError::ConnectionLost)));
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
