//! Enhanced authentication (§4.12) against scripted brokers: what goes on the wire, and how
//! a broker that breaks the exchange is answered.

use std::time::Duration;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::oneshot;
use tokio::time::timeout;

use super::test_support::{MockFramedRead, MockFramedWrite, NoReconnect, spawn_broker_once, static_test_credentials};
use crate::auth::{AuthExchange, Authenticator};
use crate::client::connect;
use crate::codec::{Auth, AuthReasonCode, ConnectAck, DecodedFrame, DisconnectReasonCode, InboundMessage, Packet, QoS};
use crate::connection::NoopLifecycle;
use crate::error::{AppError, AuthError, ConnectionOutcome, ProtocolError, TransportError};
use crate::options::ConnectOptions;
use crate::util::BrokerUrl;

const METHOD: &str = "scripted";
const WAIT: Duration = Duration::from_secs(5);

/// Sends `first`, answers a challenge `c` with `answer:c` (after `delay`), or fails to.
#[derive(Default, Clone)]
struct Scripted {
    fail_respond: bool,
    delay: Duration,
}

struct ScriptedExchange(Scripted);

impl Authenticator for Scripted {
    type Exchange = ScriptedExchange;

    fn method(&self) -> &str {
        METHOD
    }

    async fn start(&self) -> Result<(ScriptedExchange, Option<Bytes>), AuthError> {
        Ok((ScriptedExchange(self.clone()), Some(Bytes::from_static(b"first"))))
    }
}

impl AuthExchange for ScriptedExchange {
    async fn respond(&mut self, challenge: Option<Bytes>) -> Result<Option<Bytes>, AuthError> {
        tokio::time::sleep(self.0.delay).await;
        if self.0.fail_respond {
            return Err(AuthError::Provider("cannot sign".into()));
        }
        Ok(Some([&b"answer:"[..], &challenge.unwrap_or_default()].concat().into()))
    }
}

fn auth(reason_code: AuthReasonCode, method: &str, data: &'static [u8]) -> Packet {
    Packet::Auth(Auth {
        reason_code,
        auth_method: Some(method.into()),
        auth_data: Some(Bytes::from_static(data)),
        ..Auth::default()
    })
}

fn connack_with_method(method: &str) -> Packet {
    Packet::ConnectAck(Box::new(ConnectAck {
        auth_method: Some(method.into()),
        ..ConnectAck::default()
    }))
}

async fn next(read: &mut MockFramedRead) -> Option<Packet> {
    match timeout(WAIT, read.next()).await {
        Ok(Some(Ok((DecodedFrame::Control(packet), _)))) => Some(packet),
        Ok(Some(Ok((DecodedFrame::Publish(_), _)))) => Some(Packet::PingRequest),
        _ => None,
    }
}

/// Reads until the client's DISCONNECT; its reason, or `None` if it just closed.
async fn disconnect_reason(read: &mut MockFramedRead) -> Option<DisconnectReasonCode> {
    loop {
        match next(read).await? {
            Packet::Disconnect(disconnect) => return Some(disconnect.reason_code),
            _ => continue,
        }
    }
}

/// Starts a client with `authenticator` against a one-connection broker running `script`;
/// returns the driver's result and what the script reported.
async fn run<T: Send + 'static, F, Fut>(
    authenticator: Option<Scripted>,
    configure: impl FnOnce(ConnectOptions) -> ConnectOptions,
    script: F,
) -> (
    crate::MqttClient,
    tokio::task::JoinHandle<Result<(), ConnectionOutcome>>,
    oneshot::Receiver<T>,
)
where
    F: FnOnce(MockFramedRead, MockFramedWrite) -> Fut + Send + 'static,
    Fut: Future<Output = T> + Send + 'static,
{
    let (report_tx, report_rx) = oneshot::channel();
    let port = spawn_broker_once(move |read, write| async move {
        let _ = report_tx.send(script(read, write).await);
    })
    .await;
    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("url");
    let mut options = configure(ConnectOptions::new(url));
    if let Some(authenticator) = authenticator {
        options = options.authenticator(authenticator);
    }
    let (tx, _rx) = flume::bounded::<InboundMessage>(4);
    let (client, connection, _phase) =
        connect(options, static_test_credentials(), NoReconnect, tx, NoopLifecycle).expect("wiring");
    (client, tokio::spawn(connection.run()), report_rx)
}

async fn driver_result(
    driver: tokio::task::JoinHandle<Result<(), ConnectionOutcome>>,
) -> Result<(), ConnectionOutcome> {
    timeout(WAIT, driver).await.expect("driver ends").expect("join")
}

/// CONNECT carries the method and first data; the challenge is answered with AUTH 0x18 and the
/// same method ([MQTT-4.12.0-3], [MQTT-4.12.0-5]); until the CONNACK the client sends nothing but
/// AUTH, even with a publish queued ([MQTT-3.1.2-30]); the queued publish follows the CONNACK.
#[tokio::test]
async fn only_auth_goes_out_before_the_connack() {
    let (client, driver, report) = run(
        Some(Scripted::default()),
        |options| options,
        |mut read, mut write| async move {
            let Some(Packet::Connect(connect)) = next(&mut read).await else {
                return Err("no CONNECT");
            };
            if connect.auth_method.as_deref() != Some(METHOD) || connect.auth_data.as_deref() != Some(&b"first"[..]) {
                return Err("CONNECT without method or data");
            }
            write
                .send(auth(AuthReasonCode::ContinueAuth, METHOD, b"nonce"))
                .await
                .map_err(|_| "send")?;
            // Room for a queued PUBLISH to sneak out before the answer.
            tokio::time::sleep(Duration::from_millis(100)).await;
            match next(&mut read).await {
                Some(Packet::Auth(answer))
                    if answer.reason_code == AuthReasonCode::ContinueAuth
                        && answer.auth_method.as_deref() == Some(METHOD)
                        && answer.auth_data.as_deref() == Some(&b"answer:nonce"[..]) => {}
                _ => return Err("expected the AUTH answer first"),
            }
            write.send(connack_with_method(METHOD)).await.map_err(|_| "send")?;
            match timeout(WAIT, read.next()).await {
                Ok(Some(Ok((DecodedFrame::Publish(publish), _)))) if &*publish.message.topic == "after/connack" => {
                    Ok(())
                }
                _ => Err("expected the queued PUBLISH after the CONNACK"),
            }
        },
    )
    .await;
    let publish = client.publish("after/connack", "x").qos(QoS::AtMostOnce).try_start();
    assert!(publish.is_ok(), "queued");
    assert_eq!(timeout(WAIT, report).await.expect("script").expect("report"), Ok(()));
    client.shutdown().await.expect("shutdown");
    driver_result(driver).await.expect("clean stop");
}

/// A broker AUTH that does not fit is answered with DISCONNECT 0x82: another method than the
/// CONNECT's, or a CONNACK naming another method or none ([MQTT-4.12.0-5]); without a method in
/// the CONNECT, an AUTH before or after the CONNACK or a CONNACK naming a method
/// ([MQTT-4.12.0-6]); with one, an AUTH after the CONNACK that answers no AUTH of the client.
#[tokio::test]
async fn broker_breaking_the_connect_exchange_gets_a_protocol_error() {
    enum Break {
        AuthMethod,
        ConnackMethod,
        ConnackWithoutMethod,
        AuthWithoutMethodInConnect,
        ConnackMethodWithoutMethodInConnect,
        AuthAfterConnackWithoutMethodInConnect,
        UnsolicitedAuthAfterConnack,
    }
    for broken in [
        Break::AuthMethod,
        Break::ConnackMethod,
        Break::ConnackWithoutMethod,
        Break::AuthWithoutMethodInConnect,
        Break::ConnackMethodWithoutMethodInConnect,
        Break::AuthAfterConnackWithoutMethodInConnect,
        Break::UnsolicitedAuthAfterConnack,
    ] {
        let authenticator = (!matches!(
            broken,
            Break::AuthWithoutMethodInConnect
                | Break::ConnackMethodWithoutMethodInConnect
                | Break::AuthAfterConnackWithoutMethodInConnect
        ))
        .then(Scripted::default);
        let (_client, driver, report) = run(
            authenticator,
            |options| options,
            move |mut read, mut write| async move {
                let _ = next(&mut read).await;
                let packets = match broken {
                    Break::AuthMethod | Break::AuthWithoutMethodInConnect => {
                        vec![auth(AuthReasonCode::ContinueAuth, "other", b"nonce")]
                    }
                    Break::ConnackMethod => vec![connack_with_method("other")],
                    Break::ConnackWithoutMethod => vec![Packet::ConnectAck(Box::default())],
                    Break::ConnackMethodWithoutMethodInConnect => vec![connack_with_method(METHOD)],
                    Break::AuthAfterConnackWithoutMethodInConnect => vec![
                        Packet::ConnectAck(Box::default()),
                        auth(AuthReasonCode::ContinueAuth, METHOD, b"nonce"),
                    ],
                    Break::UnsolicitedAuthAfterConnack => vec![
                        connack_with_method(METHOD),
                        auth(AuthReasonCode::ContinueAuth, METHOD, b"nonce"),
                    ],
                };
                for packet in packets {
                    let _ = write.send(packet).await;
                }
                disconnect_reason(&mut read).await
            },
        )
        .await;
        assert_eq!(
            timeout(WAIT, report).await.expect("script").expect("report"),
            Some(DisconnectReasonCode::ProtocolError)
        );
        let result = driver_result(driver).await;
        assert!(matches!(result, Err(ConnectionOutcome::Protocol(_))), "{result:?}");
    }
}

/// The authenticator failing during the CONNECT exchange ends the attempt with DISCONNECT 0x80
/// and an auth error; one slower than the connect budget ends it as a timeout.
#[tokio::test]
async fn authenticator_failing_or_stalling_during_connect_ends_the_attempt() {
    let failing = Scripted {
        fail_respond: true,
        ..Scripted::default()
    };
    let (_client, driver, report) = run(
        Some(failing),
        |options| options,
        |mut read, mut write| async move {
            let _ = next(&mut read).await;
            let _ = write.send(auth(AuthReasonCode::ContinueAuth, METHOD, b"nonce")).await;
            disconnect_reason(&mut read).await
        },
    )
    .await;
    assert_eq!(
        timeout(WAIT, report).await.expect("script").expect("report"),
        Some(DisconnectReasonCode::UnspecifiedError)
    );
    let result = driver_result(driver).await;
    assert!(
        matches!(result, Err(ConnectionOutcome::Auth(AuthError::Provider(_)))),
        "{result:?}"
    );

    let stalling = Scripted {
        delay: Duration::from_secs(30),
        ..Scripted::default()
    };
    let (_client, driver, _report) = run(
        Some(stalling),
        |options| options.connect_timeout(Duration::from_millis(100)),
        |mut read, mut write| async move {
            let _ = next(&mut read).await;
            let _ = write.send(auth(AuthReasonCode::ContinueAuth, METHOD, b"nonce")).await;
            disconnect_reason(&mut read).await
        },
    )
    .await;
    match driver_result(driver).await {
        Err(ConnectionOutcome::Transport(TransportError::Connection(message))) => {
            assert!(message.contains("enhanced authentication timed out"), "{message}");
        }
        other => panic!("expected a connect timeout, got {other:?}"),
    }
}

/// Re-authentication starts with AUTH 0x19 and the CONNECT's method ([MQTT-4.12.1-1]). A broker
/// that does not answer it within the ack timeout gets DISCONNECT 0x80 and the connection closes
/// ([MQTT-4.12.1-2]); one that answers with another method gets
/// 0x82.
#[tokio::test]
async fn broker_failing_a_reauthentication_loses_the_connection() {
    for answer_with_other_method in [false, true] {
        let (client, driver, report) = run(
            Some(Scripted::default()),
            |options| options.ack_timeout(Duration::from_millis(200)),
            move |mut read, mut write| async move {
                let _ = next(&mut read).await;
                let _ = write.send(connack_with_method(METHOD)).await;
                let reauthenticate = match next(&mut read).await {
                    Some(Packet::Auth(auth)) => auth,
                    _ => return Err("expected AUTH 0x19"),
                };
                if reauthenticate.reason_code != AuthReasonCode::ReAuth
                    || reauthenticate.auth_method.as_deref() != Some(METHOD)
                {
                    return Err("AUTH 0x19 must carry the CONNECT's method");
                }
                if answer_with_other_method {
                    let _ = write.send(auth(AuthReasonCode::ContinueAuth, "other", b"nonce")).await;
                }
                disconnect_reason(&mut read).await.ok_or("no DISCONNECT")
            },
        )
        .await;
        let mut phase = client.session_phase();
        timeout(WAIT, phase.wait_for(|_| client.is_connected()))
            .await
            .expect("connected")
            .expect("watch");

        let reauthenticated = client.reauthenticate().await;
        let reported = timeout(WAIT, report).await.expect("script").expect("report");
        let result = driver_result(driver).await;
        if answer_with_other_method {
            assert_eq!(reauthenticated, Err(AppError::ConnectionLost));
            assert_eq!(reported, Ok(DisconnectReasonCode::ProtocolError));
            assert!(
                matches!(result, Err(ConnectionOutcome::Protocol(ProtocolError::Violation(_)))),
                "{result:?}"
            );
        } else {
            assert_eq!(
                reauthenticated,
                Err(AppError::ReauthenticationTimeout(Duration::from_millis(200)))
            );
            assert_eq!(reported, Ok(DisconnectReasonCode::UnspecifiedError));
            assert!(matches!(result, Err(ConnectionOutcome::Transport(_))), "{result:?}");
        }
    }
}

/// A re-authentication running into a graceful shutdown still gets its answers out while the
/// drain waits for PUBACKs; the shutdown then ends with a normal DISCONNECT.
#[tokio::test]
async fn reauthentication_answers_go_out_during_the_shutdown_drain() {
    use crate::codec::{PublishAck, PublishAckReason};

    let (reauth_seen_tx, reauth_seen_rx) = oneshot::channel();
    let (client, driver, report) = run(
        Some(Scripted::default()),
        |options| options,
        |mut read, mut write| async move {
            let _ = next(&mut read).await;
            write.send(connack_with_method(METHOD)).await.map_err(|_| "send")?;
            let mut packet_id = None;
            let mut reauth_seen = Some(reauth_seen_tx);
            while packet_id.is_none() || reauth_seen.is_some() {
                match timeout(WAIT, read.next()).await {
                    Ok(Some(Ok((DecodedFrame::Publish(publish), _)))) => packet_id = publish.meta.packet_id,
                    Ok(Some(Ok((DecodedFrame::Control(Packet::Auth(auth)), _))))
                        if auth.reason_code == AuthReasonCode::ReAuth =>
                    {
                        let _ = reauth_seen.take().map(|seen| seen.send(()));
                    }
                    _ => return Err("expected the PUBLISH and AUTH 0x19"),
                }
            }
            // The client is draining now: it waits for the PUBACK.
            tokio::time::sleep(Duration::from_millis(100)).await;
            write
                .send(auth(AuthReasonCode::ContinueAuth, METHOD, b"nonce"))
                .await
                .map_err(|_| "send")?;
            match next(&mut read).await {
                Some(Packet::Auth(answer)) if answer.auth_data.as_deref() == Some(&b"answer:nonce"[..]) => {}
                _ => return Err("expected the AUTH answer during the drain"),
            }
            let ack = PublishAck {
                packet_id: packet_id.expect("QoS 1"),
                reason_code: PublishAckReason::Success,
                reason_string: None,
                properties: Vec::new(),
            };
            write.send(Packet::PublishAck(ack)).await.map_err(|_| "send")?;
            disconnect_reason(&mut read).await.ok_or("no DISCONNECT")
        },
    )
    .await;
    let mut phase = client.session_phase();
    timeout(WAIT, phase.wait_for(|_| client.is_connected()))
        .await
        .expect("connected")
        .expect("watch");
    let publish = tokio::spawn({
        let client = client.clone();
        async move { client.publish("drain/me", "x").qos(QoS::AtLeastOnce).await }
    });
    let reauthenticate = tokio::spawn({
        let client = client.clone();
        async move { client.reauthenticate().await }
    });
    timeout(WAIT, reauth_seen_rx).await.expect("AUTH 0x19").expect("seen");
    client.shutdown().await.expect("shutdown");

    assert_eq!(
        timeout(WAIT, report).await.expect("script").expect("report"),
        Ok(DisconnectReasonCode::NormalDisconnection)
    );
    assert!(publish.await.expect("join").is_ok(), "the drain got its PUBACK");
    let reauthenticated = reauthenticate.await.expect("join");
    assert_eq!(
        reauthenticated,
        Err(AppError::ConnectionLost),
        "the broker never finished it"
    );
    driver_result(driver).await.expect("clean stop");
}

/// Calls given up while the read task is held up by a full inbound sink do not keep their
/// places: a later call waits for its turn instead of failing with `Backpressure`.
#[tokio::test]
async fn reauthenticate_calls_that_gave_up_leave_room() {
    let port = spawn_broker_once(|mut read, mut write| async move {
        let _ = next(&mut read).await;
        let _ = write.send(connack_with_method(METHOD)).await;
        // More messages than the one-slot sink takes: the read task waits for room.
        for payload in ["a", "b", "c"] {
            let message = crate::codec::PublishMessage::new("held/up", payload);
            let delivery = crate::codec::PublishDelivery {
                dup: false,
                retain: false,
                qos: QoS::AtMostOnce,
                packet_id: None,
            };
            crate::codec::encode_publish(&message, delivery, 0, write.write_buffer_mut()).expect("encode");
        }
        let _ = write.flush().await;
        tokio::time::sleep(Duration::from_secs(10)).await;
    })
    .await;
    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("url");
    let options = ConnectOptions::new(url).authenticator(Scripted::default());
    let (tx, _rx) = flume::bounded::<InboundMessage>(1);
    let (client, connection, _phase) =
        connect(options, static_test_credentials(), NoReconnect, tx, NoopLifecycle).expect("wiring");
    let driver = tokio::spawn(connection.run());
    let mut phase = client.session_phase();
    timeout(WAIT, phase.wait_for(|_| client.is_connected()))
        .await
        .expect("connected")
        .expect("watch");
    tokio::time::sleep(Duration::from_millis(100)).await;

    for _ in 0..8 {
        let gave_up = timeout(Duration::from_millis(20), client.reauthenticate()).await;
        assert!(gave_up.is_err(), "the read task is held up: {gave_up:?}");
    }
    let waiting = timeout(Duration::from_millis(200), client.reauthenticate()).await;
    assert!(waiting.is_err(), "waits for its turn instead of failing: {waiting:?}");

    client.shutdown().await.expect("shutdown");
    let _ = timeout(WAIT, driver).await;
}
