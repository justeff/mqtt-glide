//! `CONNECT` tunnel through an HTTP proxy (RFC 9110 §9.3.6).

use secrecy::ExposeSecret;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::TransportError;
use crate::util::HttpProxy;

/// Longest proxy response header block accepted before the tunnel is up.
const MAX_RESPONSE_HEAD: usize = 8 * 1024;

/// Asks `proxy` (already connected on `stream`) for a tunnel to `host:port`. On success the
/// stream carries the broker connection from the next byte on.
pub(crate) async fn tunnel<S>(stream: &mut S, proxy: &HttpProxy, host: &str, port: u16) -> Result<(), TransportError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let authority = if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };
    let mut request = format!("CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n");
    if let Some((user, password)) = proxy.credentials() {
        let token = base64(format!("{user}:{}", password.expose_secret()).as_bytes());
        request.push_str(&format!("Proxy-Authorization: Basic {token}\r\n"));
    }
    request.push_str("\r\n");
    stream.write_all(request.as_bytes()).await.map_err(proxy_io)?;
    stream.flush().await.map_err(proxy_io)?;

    let head = read_response_head(stream).await?;
    let status_line = head.lines().next().unwrap_or_default();
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse::<u16>().ok())
        .filter(|_| status_line.starts_with("HTTP/1."))
        .ok_or_else(|| TransportError::Connection(format!("HTTP proxy sent no HTTP status: {status_line:?}")))?;
    if (200..300).contains(&status) {
        Ok(())
    } else {
        Err(TransportError::Connection(format!(
            "HTTP proxy refused the tunnel: {status_line}"
        )))
    }
}

/// Reads up to the blank line ending the response head, one byte at a time so nothing past
/// it (the start of the tunnelled stream) is consumed.
async fn read_response_head<S: AsyncRead + Unpin>(stream: &mut S) -> Result<String, TransportError> {
    let mut head = Vec::with_capacity(128);
    while !head.ends_with(b"\r\n\r\n") {
        if head.len() >= MAX_RESPONSE_HEAD {
            return Err(TransportError::Connection("HTTP proxy response head too large".into()));
        }
        let byte = stream.read_u8().await.map_err(proxy_io)?;
        head.push(byte);
    }
    Ok(String::from_utf8_lossy(&head).into_owned())
}

fn proxy_io(error: std::io::Error) -> TransportError {
    TransportError::Connection(format!("HTTP proxy: {error}"))
}

/// Standard base64 with padding (RFC 4648 §4), for the Basic credentials.
fn base64(input: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let bytes = [chunk[0], *chunk.get(1).unwrap_or(&0), *chunk.get(2).unwrap_or(&0)];
        let group = u32::from(bytes[0]) << 16 | u32::from(bytes[1]) << 8 | u32::from(bytes[2]);
        for (index, shift) in [18, 12, 6, 0].into_iter().enumerate() {
            if index <= chunk.len() {
                out.push(char::from(ALPHABET[(group >> shift & 0x3F) as usize]));
            } else {
                out.push('=');
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};

    use super::*;

    #[test]
    fn base64_matches_rfc_4648_vectors() {
        for (input, expected) in [
            ("", ""),
            ("f", "Zg=="),
            ("fo", "Zm8="),
            ("foo", "Zm9v"),
            ("foobar", "Zm9vYmFy"),
            ("Aladdin:open sesame", "QWxhZGRpbjpvcGVuIHNlc2FtZQ=="),
        ] {
            assert_eq!(base64(input.as_bytes()), expected, "{input:?}");
        }
    }

    async fn run_proxy(response: &'static [u8], proxy: HttpProxy) -> (Result<(), TransportError>, String, Vec<u8>) {
        let (mut client, mut server) = duplex(4096);
        let server_side = tokio::spawn(async move {
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                request.push(server.read_u8().await.expect("request byte"));
            }
            server.write_all(response).await.expect("response");
            let mut rest = vec![0; 3];
            server.read_exact(&mut rest).await.expect("tunnelled bytes");
            (String::from_utf8(request).expect("utf8"), rest)
        });
        let result = tunnel(&mut client, &proxy, "fd00::7", 8883).await;
        if result.is_ok() {
            client.write_all(b"MQT").await.expect("tunnel write");
        } else {
            client.write_all(b"---").await.expect("write");
        }
        let (request, rest) = server_side.await.expect("server");
        (result, request, rest)
    }

    #[tokio::test]
    async fn tunnel_sends_connect_with_credentials_and_leaves_the_stream_untouched() {
        let proxy = HttpProxy::new("proxy", 3128).basic_auth("Aladdin", "open sesame");
        let (result, request, rest) = run_proxy(b"HTTP/1.1 200 Connection established\r\n\r\n", proxy).await;
        result.expect("tunnel up");
        assert_eq!(
            request,
            "CONNECT [fd00::7]:8883 HTTP/1.1\r\nHost: [fd00::7]:8883\r\n\
             Proxy-Authorization: Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ==\r\n\r\n"
        );
        assert_eq!(rest, b"MQT", "bytes after the handshake reach the broker side");
    }

    #[tokio::test]
    async fn refused_tunnel_reports_the_proxy_status() {
        let (result, request, _) = run_proxy(
            b"HTTP/1.1 407 Proxy Authentication Required\r\nProxy-Authenticate: Basic\r\n\r\n",
            HttpProxy::new("proxy", 3128),
        )
        .await;
        let error = result.expect_err("refused").to_string();
        assert!(error.contains("407"), "{error}");
        assert!(!request.contains("Proxy-Authorization"), "no credentials configured");
    }
}
