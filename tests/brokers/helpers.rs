//! Shared Mosquitto container helpers for mqtt-glide broker tests.
//!
//! eclipse-mosquitto `2.1.2-alpine`, anonymous + reject-connect configs.

use std::time::Duration;

use bytes::{Bytes, BytesMut};
use bytestring::ByteString;
use futures_util::{SinkExt, StreamExt};
use mqtt_glide::{
    Connect, ConnectAckReason, DEFAULT_MAX_PACKET_SIZE, DecodedFrame, Disconnect, Packet, PublishDelivery,
    PublishMessage, PublishProperties, QoS, V5Codec, encode_publish,
};
use testcontainers::ContainerAsync;
use testcontainers::ContainerRequest;
use testcontainers::GenericImage;
use testcontainers::ImageExt;
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::codec::{FramedRead, FramedWrite};

/// Mosquitto config used by most tests: anonymous access, no auth.
const CONF_NO_AUTH: &str = "listener 1883\nallow_anonymous true\n";

/// Mosquitto config that rejects all connections: anonymous access disabled,
/// no password file configured → every CONNECT results in ConnAck(NotAuthorized).
const CONF_REJECT_CONNECT: &str = "listener 1883\nallow_anonymous false\n";

fn mosquitto_image(conf: &'static str) -> ContainerRequest<GenericImage> {
    GenericImage::new("eclipse-mosquitto", "2.1.2-alpine")
        .with_wait_for(WaitFor::message_on_stderr("mosquitto version"))
        .with_wait_for(WaitFor::message_on_stderr(" running"))
        .with_copy_to("/mosquitto-test.conf", conf.as_bytes().to_vec())
        .with_cmd(["mosquitto", "-c", "/mosquitto-test.conf"])
}

/// Starts a Mosquitto container with anonymous access and returns the container
/// and the mapped host port.
pub async fn start_mosquitto() -> (ContainerAsync<GenericImage>, u16) {
    let container = mosquitto_image(CONF_NO_AUTH)
        .start()
        .await
        .expect("failed to start Mosquitto container");

    let port = container.get_host_port_ipv4(1883).await.expect("mapped mosquitto port");
    (container, port)
}

/// Starts a Mosquitto container that rejects all CONNECT attempts with
/// `ConnAck(NotAuthorized)`. Used to test auth-failure handling.
pub async fn start_mosquitto_reject_connect() -> (ContainerAsync<GenericImage>, u16) {
    let container = mosquitto_image(CONF_REJECT_CONNECT)
        .start()
        .await
        .expect("failed to start Mosquitto container (reject-connect)");

    let port = container.get_host_port_ipv4(1883).await.expect("mapped mosquitto port");
    (container, port)
}

/// Starts Mosquitto mapped to a fixed host port (reconnect tests after stop/drop).
///
/// After the previous container releases the port, the reconnect loop can find the
/// broker again on the same host port.
pub async fn start_mosquitto_on_port(port: u16) -> ContainerAsync<GenericImage> {
    use testcontainers::core::ContainerPort;

    mosquitto_image(CONF_NO_AUTH)
        .with_mapped_port(port, ContainerPort::Tcp(1883))
        .start()
        .await
        .expect("failed to start Mosquitto container on fixed port")
}

/// Second MQTT 5 session: CONNECT + QoS 0 PUBLISH + DISCONNECT (no client crate publish API yet).
///
/// Used so broker integration can deliver application messages without.
pub async fn publish_qos0_via_raw_session(port: u16, client_id: &str, topic: &str, payload: &[u8]) {
    let stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("raw publisher connect");
    let (read_half, write_half) = stream.into_split();
    let mut framed_read = FramedRead::new(read_half, V5Codec::with_max_packet_size(0));
    let mut framed_write = FramedWrite::new(write_half, V5Codec::with_max_packet_size(0));

    let connect = Connect {
        clean_start: true,
        keep_alive: 30,
        client_id: ByteString::from(client_id.to_owned()),
        topic_alias_max: 0,
        ..Connect::default()
    };
    framed_write
        .send(Packet::Connect(Box::new(connect)))
        .await
        .expect("send CONNECT");

    let connack = timeout(Duration::from_secs(5), framed_read.next())
        .await
        .expect("CONNACK wait")
        .expect("stream closed")
        .expect("decode CONNACK");
    match connack {
        (DecodedFrame::Control(Packet::ConnectAck(ack)), _) if ack.reason_code == ConnectAckReason::Success => {}
        other => panic!("raw publisher expected Success CONNACK, got {other:?}"),
    }

    let message = PublishMessage {
        topic: ByteString::from(topic.to_owned()),
        payload: Bytes::copy_from_slice(payload),
        properties: PublishProperties::default(),
    };
    let delivery = PublishDelivery {
        dup: false,
        retain: false,
        qos: QoS::AtMostOnce,
        packet_id: None,
    };
    let mut buf = BytesMut::new();
    encode_publish(&message, delivery, DEFAULT_MAX_PACKET_SIZE, &mut buf).expect("encode QoS0 publish");
    framed_write
        .get_mut()
        .write_all(&buf)
        .await
        .expect("write PUBLISH bytes");
    framed_write.flush().await.expect("flush PUBLISH");

    framed_write
        .send(Packet::Disconnect(Disconnect::default()))
        .await
        .expect("send DISCONNECT");
}

/// Second MQTT 5 session: CONNECT + QoS 1 PUBLISH + wait for PUBACK + DISCONNECT.
///
/// Delivers an application message that the broker will forward as QoS 1 to subscribers.
pub async fn publish_qos1_via_raw_session(port: u16, client_id: &str, topic: &str, payload: &[u8]) {
    use std::num::NonZeroU16;

    let stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("raw publisher connect");
    let (read_half, write_half) = stream.into_split();
    let mut framed_read = FramedRead::new(read_half, V5Codec::with_max_packet_size(0));
    let mut framed_write = FramedWrite::new(write_half, V5Codec::with_max_packet_size(0));

    let connect = Connect {
        clean_start: true,
        keep_alive: 30,
        client_id: ByteString::from(client_id.to_owned()),
        topic_alias_max: 0,
        ..Connect::default()
    };
    framed_write
        .send(Packet::Connect(Box::new(connect)))
        .await
        .expect("send CONNECT");

    let connack = timeout(Duration::from_secs(5), framed_read.next())
        .await
        .expect("CONNACK wait")
        .expect("stream closed")
        .expect("decode CONNACK");
    match connack {
        (DecodedFrame::Control(Packet::ConnectAck(ack)), _) if ack.reason_code == ConnectAckReason::Success => {}
        other => panic!("raw publisher expected Success CONNACK, got {other:?}"),
    }

    let message = PublishMessage {
        topic: ByteString::from(topic.to_owned()),
        payload: Bytes::copy_from_slice(payload),
        properties: PublishProperties::default(),
    };
    let delivery = PublishDelivery {
        dup: false,
        retain: false,
        qos: QoS::AtLeastOnce,
        packet_id: Some(NonZeroU16::new(1).expect("nz")),
    };
    let mut buf = BytesMut::new();
    encode_publish(&message, delivery, DEFAULT_MAX_PACKET_SIZE, &mut buf).expect("encode QoS1 publish");
    framed_write
        .get_mut()
        .write_all(&buf)
        .await
        .expect("write PUBLISH bytes");
    framed_write.flush().await.expect("flush PUBLISH");

    let puback = timeout(Duration::from_secs(5), framed_read.next())
        .await
        .expect("PUBACK wait")
        .expect("stream closed")
        .expect("decode PUBACK");
    match puback {
        (DecodedFrame::Control(Packet::PublishAck(_)), _) => {}
        other => panic!("raw publisher expected PUBACK, got {other:?}"),
    }

    framed_write
        .send(Packet::Disconnect(Disconnect::default()))
        .await
        .expect("send DISCONNECT");
}
