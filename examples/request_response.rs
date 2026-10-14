//! MQTT 5 request/response via `response_topic` + `correlation_data`.
//!
//! This works today at the **property** layer, but there is **no** client RPC helper
//! (no inbox demux / `await reply`). The example wires two clients by hand — correct,
//! but more boilerplate than a future `request_response()` API.
//!
//! ```shell
//! MQTT_GLIDE_BROKER=mqtt://127.0.0.1:1883 cargo run --example request_response
//! ```

use std::time::Duration;

use bytes::Bytes;
use mqtt_glide::{ClientBuilder, PublishProperties, QoS};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let broker = std::env::var("MQTT_GLIDE_BROKER").unwrap_or_else(|_| "mqtt://127.0.0.1:1883".into());

    let request_topic = "demo/glide/rr/request";
    let response_topic = "demo/glide/rr/reply";
    let correlation = Bytes::from_static(b"corr-001");

    let (req_tx, req_rx) = flume::bounded(8);
    let (resp_tx, resp_rx) = flume::bounded(8);

    let responder = ClientBuilder::new(&broker)?.inbound_sink(req_tx).connect().await?;
    let requester = ClientBuilder::new(&broker)?.inbound_sink(resp_tx).connect().await?;

    responder.subscribe([(request_topic, QoS::AtLeastOnce)]).await?;
    requester.subscribe([(response_topic, QoS::AtLeastOnce)]).await?;

    // Responder: one request → reply on Response Topic with Correlation Data echoed.
    let responder_client = responder.clone();
    let correlation_expected = correlation.clone();
    let responder_task = tokio::spawn(async move {
        let msg = req_rx.recv_async().await?;
        let reply_to = msg
            .properties
            .response_topic
            .clone()
            .unwrap_or_else(|| response_topic.into());
        let corr = msg
            .properties
            .correlation_data
            .clone()
            .unwrap_or_else(|| correlation_expected.clone());
        responder_client
            .publish(reply_to, "pong")
            .qos(QoS::AtLeastOnce)
            .properties(
                PublishProperties::default()
                    .correlation_data(corr)
                    .content_type("text/plain"),
            )
            .send()
            .await?;
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    });

    requester
        .publish(request_topic, "ping")
        .qos(QoS::AtLeastOnce)
        .properties(
            PublishProperties::default()
                .response_topic(response_topic)
                .correlation_data(correlation.clone()),
        )
        .send()
        .await?;

    // App-level bound only: ack/connect timeouts do not cover waiting on the inbound sink.
    let reply = tokio::time::timeout(Duration::from_secs(5), resp_rx.recv_async())
        .await
        .map_err(|_| "timed out waiting for response")??;
    assert_eq!(reply.properties.correlation_data.as_ref(), Some(&correlation));
    println!(
        "reply: {} => {} (correlation ok)",
        reply.topic,
        String::from_utf8_lossy(&reply.payload)
    );

    requester.shutdown().await?;
    responder.shutdown().await?;
    let _ = responder_task.await;
    Ok(())
}
