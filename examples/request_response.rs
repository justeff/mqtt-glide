//! MQTT 5 request/response with [`Requester`](mqtt_glide::Requester) and
//! [`Publish::reply_to`](mqtt_glide::Publish::reply_to).
//!
//! The requester subscribes once to its own response topic; each request gets fresh
//! Correlation Data and resolves with its response. The responder answers every request it
//! takes from its inbound sink.
//!
//! ```shell
//! MQTT_GLIDE_BROKER=mqtt://127.0.0.1:1883 cargo run --example request_response
//! ```

use std::time::Duration;

use mqtt_glide::{ClientBuilder, Publish, QoS};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let broker = std::env::var("MQTT_GLIDE_BROKER").unwrap_or_else(|_| "mqtt://127.0.0.1:1883".into());
    let request_topic = "demo/glide/rr/time";

    // Responder: answer every request on `request_topic`.
    let (requests_tx, requests) = flume::bounded(64);
    let responder = ClientBuilder::new(&broker)?.inbound_sink(requests_tx).connect().await?;
    responder.subscribe([(request_topic, QoS::AtLeastOnce)]).await?;
    let responder_task = tokio::spawn({
        let responder = responder.clone();
        async move {
            while let Ok(request) = requests.recv_async().await {
                let answer = format!("pong for {}", String::from_utf8_lossy(&request.payload));
                if let Some(reply) = Publish::reply_to(&request, answer)
                    && let Err(error) = responder.send(reply.qos(QoS::AtLeastOnce)).await
                {
                    eprintln!("reply failed: {error}");
                }
            }
        }
    });

    // Requester: one response topic, any number of concurrent requests.
    let client = ClientBuilder::new(&broker)?.discard_inbound().connect().await?;
    let requester = client.requester("demo/glide/rr/replies/example").await?;
    for question in ["ping 1", "ping 2"] {
        let reply = requester
            .request(request_topic, question)
            .qos(QoS::AtLeastOnce)
            .timeout(Duration::from_secs(5))
            .await?;
        println!("{question} => {}", String::from_utf8_lossy(&reply.payload));
    }

    requester.close().await?;
    client.shutdown().await?;
    responder.shutdown().await?;
    responder_task.abort();
    Ok(())
}
