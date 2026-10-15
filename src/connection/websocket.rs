//! MQTT over WebSocket (§6): handshake plus an `AsyncRead` / `AsyncWrite` byte-stream
//! adapter, so the regular MQTT codec runs unchanged on top of WebSocket frames.
//!
//! - Reads turn binary frames into a byte stream; MQTT packets may span frames or share
//!   one ([MQTT-6.0.0-2]). Any other data frame is a protocol error ([MQTT-6.0.0-1]);
//!   a Close frame reads as end of stream.
//! - Writes are collected and go out as **one binary frame per flush**, so the write task's
//!   batching carries over to the WebSocket layer.
//! - The client offers the `mqtt` subprotocol and requires the server to select it
//!   ([MQTT-6.0.0-3], [MQTT-6.0.0-4]).

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Instant;

use bytes::{Buf, Bytes, BytesMut};
use futures_util::{Sink, Stream};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::timeout_at;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::{self, Message};

use crate::error::{ConnectionOutcome, TransportError};
use crate::options::ConnectOptions;

/// WebSocket subprotocol for MQTT ([MQTT-6.0.0-3]).
const SUBPROTOCOL: &str = "mqtt";

/// Collected writes are flushed early once this many bytes are pending, so a long burst
/// does not build one huge frame.
const MAX_PENDING_WRITE: usize = 64 * 1024;

/// Runs the WebSocket client handshake over an established TCP or TLS `stream`.
pub(crate) async fn handshake<S>(
    options: &ConnectOptions,
    stream: S,
    deadline: Instant,
) -> Result<WsByteStream<S>, ConnectionOutcome>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let url = &options.broker_url;
    let scheme = if url.use_tls() { "wss" } else { "ws" };
    let host = if url.host().contains(':') {
        format!("[{}]", url.host())
    } else {
        url.host().to_owned()
    };
    let uri = format!(
        "{scheme}://{host}:{}{}",
        url.port(),
        url.websocket_path().unwrap_or("/")
    );
    let mut request = uri.into_client_request().map_err(ws_error)?;
    request
        .headers_mut()
        .insert("Sec-WebSocket-Protocol", HeaderValue::from_static(SUBPROTOCOL));

    let (ws, response) = match timeout_at(
        tokio::time::Instant::from_std(deadline),
        tokio_tungstenite::client_async(request, stream),
    )
    .await
    {
        Ok(result) => result.map_err(ws_error)?,
        Err(_) => {
            return Err(ConnectionOutcome::Transport(TransportError::Connection(format!(
                "WebSocket handshake timed out after {:?}",
                options.connect_timeout
            ))));
        }
    };
    let selected = response
        .headers()
        .get("Sec-WebSocket-Protocol")
        .and_then(|value| value.to_str().ok());
    if selected != Some(SUBPROTOCOL) {
        return Err(ConnectionOutcome::Transport(TransportError::Connection(format!(
            "WebSocket server selected subprotocol {selected:?}, MQTT requires \"mqtt\" ([MQTT-6.0.0-4])"
        ))));
    }
    Ok(WsByteStream {
        ws,
        read: Bytes::new(),
        write: BytesMut::new(),
    })
}

fn ws_error(error: tungstenite::Error) -> ConnectionOutcome {
    ConnectionOutcome::Transport(TransportError::Connection(format!("WebSocket: {error}")))
}

/// MQTT byte stream over a WebSocket connection (see module docs).
pub(crate) struct WsByteStream<S> {
    ws: WebSocketStream<S>,
    /// Unread rest of the last binary frame.
    read: Bytes,
    /// Bytes written since the last flush (sent as one binary frame).
    write: BytesMut,
}

impl<S> WsByteStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// Sends the pending write bytes as one binary frame and flushes the WebSocket.
    fn poll_send_pending(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if !self.write.is_empty() {
            ready!(Pin::new(&mut self.ws).poll_ready(cx)).map_err(io::Error::other)?;
            let frame = self.write.split().freeze();
            Pin::new(&mut self.ws)
                .start_send(Message::Binary(frame))
                .map_err(io::Error::other)?;
        }
        Pin::new(&mut self.ws).poll_flush(cx).map_err(io::Error::other)
    }
}

impl<S> AsyncRead for WsByteStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        loop {
            if !this.read.is_empty() {
                let n = this.read.len().min(buf.remaining());
                buf.put_slice(&this.read[..n]);
                this.read.advance(n);
                return Poll::Ready(Ok(()));
            }
            match ready!(Pin::new(&mut this.ws).poll_next(cx)) {
                Some(Ok(Message::Binary(frame))) => this.read = frame,
                // Pings are answered by tungstenite; control frames carry no MQTT bytes.
                Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_))) => {}
                Some(Ok(Message::Close(_))) | None => return Poll::Ready(Ok(())),
                Some(Ok(Message::Text(_))) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "WebSocket text frame on an MQTT connection ([MQTT-6.0.0-1])",
                    )));
                }
                Some(Err(error)) => return Poll::Ready(Err(io::Error::other(error))),
            }
        }
    }
}

impl<S> AsyncWrite for WsByteStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if this.write.len() >= MAX_PENDING_WRITE {
            ready!(this.poll_send_pending(cx))?;
        }
        this.write.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().poll_send_pending(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        ready!(this.poll_send_pending(cx))?;
        Pin::new(&mut this.ws).poll_close(cx).map_err(io::Error::other)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures_util::{SinkExt, StreamExt};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};

    use super::*;
    use crate::util::BrokerUrl;

    /// In-process WebSocket server; `subprotocol` is echoed in the handshake response.
    #[allow(clippy::result_large_err)] // tungstenite's handshake callback signature
    async fn server(
        subprotocol: Option<&'static str>,
        script: impl FnOnce(WebSocketStream<TcpStream>) -> futures_util::future::BoxFuture<'static, ()> + Send + 'static,
    ) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let ws = tokio_tungstenite::accept_hdr_async(stream, move |request: &Request, mut response: Response| {
                assert_eq!(
                    request
                        .headers()
                        .get("Sec-WebSocket-Protocol")
                        .and_then(|v| v.to_str().ok()),
                    Some("mqtt"),
                    "client must offer the mqtt subprotocol"
                );
                if let Some(protocol) = subprotocol {
                    response
                        .headers_mut()
                        .insert("Sec-WebSocket-Protocol", HeaderValue::from_static(protocol));
                }
                Ok(response)
            })
            .await;
            if let Ok(ws) = ws {
                script(ws).await;
            }
        });
        port
    }

    async fn connect(port: u16) -> Result<WsByteStream<TcpStream>, ConnectionOutcome> {
        let options = ConnectOptions::new(BrokerUrl::parse(&format!("ws://127.0.0.1:{port}/mqtt")).expect("url"));
        let tcp = TcpStream::connect(("127.0.0.1", port)).await.expect("tcp");
        handshake(&options, tcp, Instant::now() + Duration::from_secs(5)).await
    }

    /// Packets are not assumed to align with WebSocket frames ([MQTT-6.0.0-2]). The in-process
    /// server also asserts the client offered the `mqtt` subprotocol ([MQTT-6.0.0-3]).
    #[tokio::test]
    async fn frames_read_as_one_byte_stream() {
        let port = server(Some("mqtt"), |mut ws| {
            Box::pin(async move {
                for frame in [&[1u8, 2][..], &[3], &[4, 5, 6]] {
                    ws.send(Message::Binary(Bytes::copy_from_slice(frame)))
                        .await
                        .expect("send");
                }
                ws.close(None).await.expect("close");
            })
        })
        .await;
        let mut stream = connect(port).await.expect("handshake");
        let mut all = Vec::new();
        stream.read_to_end(&mut all).await.expect("read to Close");
        assert_eq!(all, [1, 2, 3, 4, 5, 6], "frame boundaries are invisible; Close is EOF");
    }

    /// A non-binary data frame fails the connection ([MQTT-6.0.0-1]).
    #[tokio::test]
    async fn text_frame_is_a_protocol_error() {
        let port = server(Some("mqtt"), |mut ws| {
            Box::pin(async move {
                ws.send(Message::Text("nope".into())).await.expect("send");
                tokio::time::sleep(Duration::from_millis(200)).await;
            })
        })
        .await;
        let mut stream = connect(port).await.expect("handshake");
        let mut buf = [0u8; 8];
        let err = stream.read(&mut buf).await.expect_err("text frame must fail the read");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    /// Control Packets are sent in binary data frames ([MQTT-6.0.0-1]).
    #[tokio::test]
    async fn writes_go_out_as_one_binary_frame_per_flush() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let port = server(Some("mqtt"), move |mut ws| {
            Box::pin(async move {
                let first = ws.next().await.expect("frame").expect("ok");
                let _ = tx.send(first);
            })
        })
        .await;
        let mut stream = connect(port).await.expect("handshake");
        stream.write_all(b"ab").await.expect("write");
        stream.write_all(b"cd").await.expect("write");
        stream.flush().await.expect("flush");
        let frame = rx.await.expect("server saw a frame");
        assert_eq!(frame, Message::Binary(Bytes::from_static(b"abcd")));
    }

    /// A long burst without a flush goes out once 64 KiB are pending, so one frame does not
    /// grow without bound.
    #[tokio::test]
    async fn long_burst_is_sent_before_the_flush() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let port = server(Some("mqtt"), move |mut ws| {
            Box::pin(async move {
                let first = ws.next().await.expect("frame").expect("ok");
                let _ = tx.send(first);
            })
        })
        .await;
        let mut stream = connect(port).await.expect("handshake");
        let chunk = vec![7u8; 40 * 1024];
        for _ in 0..3 {
            stream.write_all(&chunk).await.expect("write");
        }
        let frame = tokio::time::timeout(Duration::from_secs(5), rx)
            .await
            .expect("a frame before any flush")
            .expect("server saw a frame");
        let Message::Binary(frame) = frame else {
            panic!("expected a binary frame, got {frame:?}");
        };
        assert_eq!(frame.len(), 80 * 1024, "the bytes pending when the limit was reached");
    }

    /// Shutdown sends what is still pending, then closes the WebSocket with a Close frame.
    #[tokio::test]
    async fn shutdown_sends_pending_bytes_then_close() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let port = server(Some("mqtt"), move |mut ws| {
            Box::pin(async move {
                let mut seen = Vec::new();
                while let Some(Ok(message)) = ws.next().await {
                    let close = message.is_close();
                    seen.push(message);
                    if close {
                        break;
                    }
                }
                let _ = tx.send(seen);
            })
        })
        .await;
        let mut stream = connect(port).await.expect("handshake");
        stream.write_all(b"xy").await.expect("write");
        stream.shutdown().await.expect("shutdown");
        let seen = tokio::time::timeout(Duration::from_secs(5), rx)
            .await
            .expect("server saw the connection end")
            .expect("server result");
        assert_eq!(seen.len(), 2, "pending bytes, then Close: {seen:?}");
        assert_eq!(seen[0], Message::Binary(Bytes::from_static(b"xy")));
        assert!(seen[1].is_close(), "got {:?}", seen[1]);
    }

    /// The client offers `mqtt` ([MQTT-6.0.0-3]; asserted by the in-process server) and refuses
    /// a server that does not select it.
    #[tokio::test]
    async fn server_without_mqtt_subprotocol_is_refused() {
        let port = server(None, |_ws| Box::pin(async {})).await;
        let err = connect(port).await.err().expect("handshake must fail");
        assert!(format!("{err:?}").contains("subprotocol"), "got {err:?}");
    }
}
