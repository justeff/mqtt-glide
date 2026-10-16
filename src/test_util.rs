//! Setup shared by the unit tests of the client API: wiring a client against a mock broker and
//! a broker that records the PUBLISHes it receives and acknowledges QoS 1.

use std::num::NonZeroU16;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::task::JoinHandle;
use tokio::time::timeout;

use crate::client::{MqttClient, NoReconnect, ReconnectPolicy, connect};
use crate::codec::{
    ConnectAck, DecodedFrame, InboundMessage, Packet, PublishAck, PublishAckReason, PublishPacket, QoS,
};
use crate::connection::NoopLifecycle;
use crate::connection::test_support::{
    accept_connect_send_connack_with, await_connected, spawn_broker_once, static_test_credentials,
};
use crate::error::ConnectionOutcome;
use crate::options::ConnectOptions;
use crate::util::BrokerUrl;

/// The driver task of a started client.
pub(crate) type RunHandle = JoinHandle<Result<(), ConnectionOutcome>>;

/// What a recording broker saw, shared with the test.
pub(crate) type Recorded<T> = Arc<tokio::sync::Mutex<Vec<T>>>;

/// Room in the inbound sink of a started client: more than any test delivers.
const INBOUND_CAPACITY: usize = 256;

/// Default options for a plain-TCP broker on localhost.
pub(crate) fn options_for(port: u16) -> ConnectOptions {
    ConnectOptions::new(BrokerUrl::parse(&format!("mqtt://127.0.0.1:{port}")).expect("valid url"))
}

/// Wires a client with test credentials, spawns its driver and waits for the CONNACK.
pub(crate) async fn start_with(
    options: ConnectOptions,
    reconnect: impl ReconnectPolicy + 'static,
) -> (MqttClient, flume::Receiver<InboundMessage>, RunHandle) {
    let (tx, rx) = flume::bounded::<InboundMessage>(INBOUND_CAPACITY);
    let (client, connection, mut phase) =
        connect(options, static_test_credentials(), reconnect, tx, NoopLifecycle).expect("connect wiring");
    let run = tokio::spawn(connection.run());
    await_connected(&mut phase).await;
    (client, rx, run)
}

/// [`start_with`] without reconnects: one mock session.
pub(crate) async fn start(options: ConnectOptions) -> (MqttClient, flume::Receiver<InboundMessage>, RunHandle) {
    start_with(options, NoReconnect).await
}

/// A PUBACK with reason Success.
pub(crate) fn puback(packet_id: NonZeroU16) -> Packet {
    puback_with(packet_id, PublishAckReason::Success)
}

/// A PUBACK with the given reason code and no properties.
pub(crate) fn puback_with(packet_id: NonZeroU16, reason_code: PublishAckReason) -> Packet {
    Packet::PublishAck(PublishAck {
        packet_id,
        reason_code,
        properties: Vec::new(),
        reason_string: None,
    })
}

/// The QoS, Packet Identifier and payload of a PUBLISH: what most recording tests compare.
pub(crate) fn qos_id_payload(packet: &PublishPacket) -> (QoS, Option<NonZeroU16>, Bytes) {
    (packet.meta.qos, packet.meta.packet_id, packet.message.payload.clone())
}

/// CONNACK with `receive_max`, then records `record(packet)` for every PUBLISH and PUBACKs the
/// QoS 1 ones at once, until DISCONNECT or EOF.
pub(crate) async fn spawn_puback_broker<T: Send + 'static>(
    receive_max: NonZeroU16,
    record: impl Fn(&PublishPacket) -> T + Send + 'static,
) -> (u16, Recorded<T>) {
    let recorded: Recorded<T> = Arc::default();
    let seen = Arc::clone(&recorded);
    let port = spawn_broker_once(move |mut framed_read, mut framed_write| async move {
        let ack = ConnectAck {
            receive_max,
            ..ConnectAck::default()
        };
        if !accept_connect_send_connack_with(&mut framed_read, &mut framed_write, ack).await {
            return;
        }
        loop {
            match framed_read.next().await {
                Some(Ok((DecodedFrame::Publish(packet), _))) => {
                    seen.lock().await.push(record(&packet));
                    if let (QoS::AtLeastOnce, Some(id)) = (packet.meta.qos, packet.meta.packet_id) {
                        let _ = framed_write.send(puback(id)).await;
                    }
                }
                Some(Ok((DecodedFrame::Control(Packet::Disconnect(_)), _))) | Some(Err(_)) | None => break,
                Some(Ok(_)) => {}
            }
        }
    })
    .await;
    (port, recorded)
}

/// Waits until `recorded` holds at least `n` entries (the broker runs on its own task) and
/// returns a copy.
pub(crate) async fn recorded_at_least<T: Clone>(recorded: &tokio::sync::Mutex<Vec<T>>, n: usize) -> Vec<T> {
    timeout(Duration::from_secs(2), async {
        loop {
            let seen = recorded.lock().await;
            if seen.len() >= n {
                return seen.clone();
            }
            drop(seen);
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("the broker did not record {n} entries in time"))
}
