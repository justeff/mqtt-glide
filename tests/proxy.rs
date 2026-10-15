//! MQTT through an HTTP `CONNECT` proxy, against an in-process proxy and scripted broker.

#[path = "common/mod.rs"]
mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{NoReconnect, spawn_peer_broker, static_credentials, wait_for_phase};
use mqtt_glide::{
    BrokerUrl, ClientId, ConnectOptions, ConnectionOutcome, HttpProxy, InboundMessage, NoopLifecycle, SessionPhase,
    TransportError, connect,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::timeout;

/// A one-connection proxy: records the request head, answers with `response`, and on a 2xx
/// splices the client to the requested target.
async fn spawn_proxy(response: &'static str) -> (u16, Arc<Mutex<String>>, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
    let port = listener.local_addr().expect("addr").port();
    let seen = Arc::new(Mutex::new(String::new()));
    let handle = tokio::spawn({
        let seen = Arc::clone(&seen);
        async move {
            let (mut client, _) = listener.accept().await.expect("accept");
            let mut head = Vec::new();
            while !head.ends_with(b"\r\n\r\n") {
                head.push(client.read_u8().await.expect("request byte"));
            }
            let head = String::from_utf8(head).expect("utf8 head");
            *seen.lock().expect("lock") = head.clone();
            client.write_all(response.as_bytes()).await.expect("respond");
            if !response.starts_with("HTTP/1.1 2") {
                return;
            }
            let target = head
                .lines()
                .next()
                .and_then(|line| line.split_whitespace().nth(1))
                .expect("CONNECT target")
                .to_owned();
            let mut upstream = TcpStream::connect(target).await.expect("upstream");
            let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
        }
    });
    (port, seen, handle)
}

fn options(broker_port: u16, proxy: HttpProxy) -> ConnectOptions {
    let url = BrokerUrl::parse(&format!("mqtt://127.0.0.1:{broker_port}")).expect("valid url");
    ConnectOptions::new(url)
        .client_id(ClientId::from_prefix("proxied"))
        .connect_timeout(Duration::from_secs(2))
        .http_proxy(proxy)
}

/// The session runs through the tunnel; the proxy sees the broker's address and the Basic
/// credentials, and nothing else of the MQTT stream before the tunnel is up.
#[tokio::test]
async fn session_runs_through_the_tunnel() {
    let (broker_port, broker) = spawn_peer_broker(|broker| async move {
        let mut session = broker.accept_session().await;
        session.expect_connect().await;
        session.send_connack_success().await;
        session.hold().await;
    })
    .await;
    let (proxy_port, seen, proxy) = spawn_proxy("HTTP/1.1 200 Connection established\r\n\r\n").await;

    let proxy_config = HttpProxy::parse(&format!("http://svc:s%3Acret@127.0.0.1:{proxy_port}")).expect("proxy url");
    let (tx, _rx) = mpsc::channel::<InboundMessage>(4);
    let (client, connection, mut phase) = connect(
        options(broker_port, proxy_config),
        static_credentials(),
        NoReconnect,
        tx,
        NoopLifecycle,
    )
    .expect("wiring");
    let run = tokio::spawn(connection.run());

    wait_for_phase(&mut phase, Duration::from_secs(5), |event| {
        matches!(event, SessionPhase::Connected { .. })
    })
    .await;
    let head = seen.lock().expect("lock").clone();
    assert!(
        head.starts_with(&format!("CONNECT 127.0.0.1:{broker_port} HTTP/1.1\r\n")),
        "{head}"
    );
    // base64("svc:s:cret")
    assert!(
        head.contains("Proxy-Authorization: Basic c3ZjOnM6Y3JldA==\r\n"),
        "{head}"
    );

    client.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(5), run).await.expect("run join");
    broker.abort();
    proxy.abort();
}

/// A proxy that refuses the tunnel ends the attempt with a transport error naming its status.
#[tokio::test]
async fn refused_tunnel_is_a_transport_error() {
    let (proxy_port, _seen, proxy) =
        spawn_proxy("HTTP/1.1 407 Proxy Authentication Required\r\nProxy-Authenticate: Basic\r\n\r\n").await;

    let (tx, _rx) = mpsc::channel::<InboundMessage>(4);
    let (_client, connection, _phase) = connect(
        options(1, HttpProxy::new("127.0.0.1", proxy_port)),
        static_credentials(),
        NoReconnect,
        tx,
        NoopLifecycle,
    )
    .expect("wiring");
    let outcome = timeout(Duration::from_secs(5), connection.run()).await.expect("run");
    match outcome {
        Err(ConnectionOutcome::Transport(TransportError::Connection(message))) => {
            assert!(message.contains("407"), "{message}");
        }
        other => panic!("expected a transport error, got {other:?}"),
    }
    proxy.await.expect("proxy");
}
