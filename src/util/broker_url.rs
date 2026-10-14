//! Broker URL parsing (`mqtt://`, `mqtts://`, `ws://`, `wss://`, host, port, path).

use thiserror::Error;

/// Default MQTT port for unencrypted connections (`mqtt://` and bare hostnames).
const DEFAULT_MQTT_PORT: u16 = 1883;

/// Default MQTT port for TLS-encrypted connections (`ssl://`, `mqtts://`).
const DEFAULT_MQTTS_PORT: u16 = 8883;

/// Default ports for MQTT over WebSocket: the HTTP(S) ports.
const DEFAULT_WS_PORT: u16 = 80;
const DEFAULT_WSS_PORT: u16 = 443;

/// Parsed MQTT broker endpoint (host, port, TLS requirement, WebSocket path).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerUrl {
    host: String,
    port: u16,
    use_tls: bool,
    /// `Some(path)` for MQTT over WebSocket.
    websocket_path: Option<String>,
}

/// Errors returned when a broker URL cannot be parsed.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum BrokerUrlError {
    /// The URL contains a port segment that is not a valid `u16`.
    #[error("mqtt broker URL contains a non-numeric port")]
    InvalidPort,
    /// The URL resolves to an empty host (for example `mqtt://:1883`).
    #[error("mqtt broker URL host must not be empty")]
    EmptyHost,
    /// The bracketed IPv6 host is malformed (for example `mqtt://[::1` or `mqtt://[::1]x`).
    #[error("mqtt broker URL host has malformed IPv6 brackets")]
    InvalidHost,
    /// `ws://` / `wss://` URL, but the crate was built without the `websocket` feature.
    #[error("MQTT over WebSocket needs the `websocket` feature of mqtt-glide")]
    WebSocketUnsupported,
    /// A proxy URL with a scheme other than `http://` (see [`crate::HttpProxy::parse`]).
    #[error("proxy URL must start with http://")]
    UnsupportedProxyScheme,
}

impl BrokerUrl {
    /// Parses host, port, TLS requirement and (for WebSocket) the path from a broker URL.
    ///
    /// Supported schemes:
    /// - `ssl://host` / `mqtts://host` — TLS (default port 8883)
    /// - `mqtt://host` — plain TCP (default port 1883)
    /// - `ws://host/path` / `wss://host/path` — MQTT over WebSocket, plain or TLS (default
    ///   ports 80 / 443, default path `/`); needs the `websocket` feature
    /// - bare hostname — plain TCP (default port 1883)
    ///
    /// An explicit port overrides the scheme default: `ssl://host:9883`.
    /// Trailing slashes on the host segment of TCP URLs are stripped.
    pub fn parse(url: &str) -> Result<Self, BrokerUrlError> {
        if let Some(rest) = url.strip_prefix("ws://") {
            return Self::parse_websocket(rest, false);
        }
        if let Some(rest) = url.strip_prefix("wss://") {
            return Self::parse_websocket(rest, true);
        }
        let (host_port, use_tls) =
            if let Some(rest) = url.strip_prefix("ssl://").or_else(|| url.strip_prefix("mqtts://")) {
                (rest, true)
            } else if let Some(rest) = url.strip_prefix("mqtt://") {
                (rest, false)
            } else {
                (url, false)
            };
        let default_port = if use_tls { DEFAULT_MQTTS_PORT } else { DEFAULT_MQTT_PORT };
        let (host, port) = split_host_port(host_port.trim_end_matches('/'), default_port)?;
        Ok(Self {
            host,
            port,
            use_tls,
            websocket_path: None,
        })
    }

    fn parse_websocket(rest: &str, use_tls: bool) -> Result<Self, BrokerUrlError> {
        if !cfg!(feature = "websocket") {
            return Err(BrokerUrlError::WebSocketUnsupported);
        }
        let (host_port, path) = match rest.find('/') {
            Some(index) => rest.split_at(index),
            None => (rest, "/"),
        };
        let default_port = if use_tls { DEFAULT_WSS_PORT } else { DEFAULT_WS_PORT };
        let (host, port) = split_host_port(host_port, default_port)?;
        Ok(Self {
            host,
            port,
            use_tls,
            websocket_path: Some(path.to_owned()),
        })
    }

    /// Returns the broker hostname.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Returns the broker port.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Returns whether TLS should be used for the connection.
    pub fn use_tls(&self) -> bool {
        self.use_tls
    }

    /// HTTP path of the WebSocket endpoint for `ws://` / `wss://` URLs, `None` for TCP.
    pub fn websocket_path(&self) -> Option<&str> {
        self.websocket_path.as_deref()
    }

    /// This endpoint moved to the server named by a broker's Server Reference (§4.11).
    ///
    /// The reference's format is not specified; brokers send `host`, `host:port`, `[v6]:port`
    /// or a URL, possibly as a space-separated list. The first entry's host and port are used
    /// (port defaulting to this endpoint's); scheme, TLS and WebSocket path stay as configured,
    /// so a redirect can never downgrade the transport. `None` if the reference names no host.
    pub(crate) fn redirected(&self, reference: &str) -> Option<Self> {
        let first = reference.split_whitespace().next()?;
        let without_scheme = first.split_once("://").map_or(first, |(_, rest)| rest);
        let host_port = match without_scheme.find('/') {
            Some(index) => &without_scheme[..index],
            None => without_scheme,
        };
        let (host, port) = split_host_port(host_port, self.port).ok()?;
        Some(Self {
            host,
            port,
            use_tls: self.use_tls,
            websocket_path: self.websocket_path.clone(),
        })
    }
}

/// Splits `host[:port]` or `[v6][:port]`, falling back to `default_port`.
pub(crate) fn split_host_port(host_port: &str, default_port: u16) -> Result<(String, u16), BrokerUrlError> {
    let (host, port) = if let Some(rest) = host_port.strip_prefix('[') {
        // Bracketed IPv6 literal: `[::1]` or `[::1]:1883`. The stored host keeps the
        // bare address (no brackets) so TCP connect and TLS server-name parsing work.
        let (v6, tail) = rest.split_once(']').ok_or(BrokerUrlError::InvalidHost)?;
        let port = match tail.strip_prefix(':') {
            Some(p) => p.parse::<u16>().map_err(|_| BrokerUrlError::InvalidPort)?,
            None if tail.is_empty() => default_port,
            None => return Err(BrokerUrlError::InvalidHost),
        };
        (v6, port)
    } else if let Some((h, p)) = host_port.rsplit_once(':') {
        let port = p.parse::<u16>().map_err(|_| BrokerUrlError::InvalidPort)?;
        (h, port)
    } else {
        (host_port, default_port)
    };
    if host.is_empty() {
        return Err(BrokerUrlError::EmptyHost);
    }
    Ok((host.to_owned(), port))
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    /// Broker URL scheme determines TLS; port defaults to 8883 (TLS) or 1883 (plain).
    #[rstest]
    #[case::ssl_default_port("ssl://broker.example.com", "broker.example.com", 8883, true)]
    #[case::mqtts_default_port("mqtts://broker.example.com", "broker.example.com", 8883, true)]
    #[case::ssl_explicit_port("ssl://broker.example.com:9883", "broker.example.com", 9883, true)]
    #[case::mqtt_default_port("mqtt://localhost", "localhost", 1883, false)]
    #[case::mqtt_explicit_port("mqtt://localhost:11883", "localhost", 11883, false)]
    #[case::bare_hostname("localhost", "localhost", 1883, false)]
    #[case::trailing_slash("ssl://broker.example.com/", "broker.example.com", 8883, true)]
    fn parse_extracts_components(
        #[case] input: &str,
        #[case] expected_host: &str,
        #[case] expected_port: u16,
        #[case] expected_tls: bool,
    ) {
        let addr = BrokerUrl::parse(input).expect("valid broker URL");
        assert_eq!(addr.host(), expected_host, "host mismatch for {input}");
        assert_eq!(addr.port(), expected_port, "port mismatch for {input}");
        assert_eq!(addr.use_tls(), expected_tls, "tls mismatch for {input}");
    }

    /// Server Reference forms seen in practice; the configured transport always stays.
    #[rstest]
    #[case::host_only("other.example.com", "other.example.com", 8883)]
    #[case::host_port("other.example.com:9883", "other.example.com", 9883)]
    #[case::ipv6("[fe80::1]:8884", "fe80::1", 8884)]
    #[case::list_takes_first("10.0.0.2:8885 10.0.0.3:8885", "10.0.0.2", 8885)]
    #[case::url_scheme_ignored("mqtt://other.example.com:1883/x", "other.example.com", 1883)]
    fn redirected_keeps_transport(#[case] reference: &str, #[case] host: &str, #[case] port: u16) {
        let base = BrokerUrl::parse("mqtts://broker.example.com").expect("valid");
        let moved = base.redirected(reference).expect("usable reference");
        assert_eq!((moved.host(), moved.port()), (host, port));
        assert!(moved.use_tls(), "a redirect never downgrades TLS");
    }

    #[test]
    fn redirected_rejects_references_without_a_host() {
        let base = BrokerUrl::parse("mqtt://broker.example.com").expect("valid");
        for reference in ["", "   ", ":1883", "[fe80::1"] {
            assert!(base.redirected(reference).is_none(), "{reference:?}");
        }
    }

    #[test]
    fn parse_rejects_non_numeric_port() {
        assert_eq!(
            BrokerUrl::parse("ssl://broker.example.com:notaport"),
            Err(BrokerUrlError::InvalidPort)
        );
    }

    #[test]
    fn parse_rejects_empty_host() {
        assert_eq!(BrokerUrl::parse("mqtt://:1883"), Err(BrokerUrlError::EmptyHost));
    }

    /// Bracketed IPv6 literals parse with default or explicit port; the stored host drops
    /// the brackets so TCP connect / TLS server-name parsing see the bare address.
    #[rstest]
    #[case::mqtt_v6_default_port("mqtt://[::1]", "::1", 1883, false)]
    #[case::mqtt_v6_explicit_port("mqtt://[::1]:11883", "::1", 11883, false)]
    #[case::mqtts_v6_default_port("mqtts://[2001:db8::1]", "2001:db8::1", 8883, true)]
    #[case::mqtts_v6_explicit_port("mqtts://[2001:db8::1]:8884", "2001:db8::1", 8884, true)]
    fn parse_bracketed_ipv6(
        #[case] input: &str,
        #[case] expected_host: &str,
        #[case] expected_port: u16,
        #[case] expected_tls: bool,
    ) {
        let addr = BrokerUrl::parse(input).expect("valid broker URL");
        assert_eq!(addr.host(), expected_host, "host mismatch for {input}");
        assert_eq!(addr.port(), expected_port, "port mismatch for {input}");
        assert_eq!(addr.use_tls(), expected_tls, "tls mismatch for {input}");
    }

    #[rstest]
    #[case::missing_close("mqtt://[::1")]
    #[case::trailing_garbage("mqtt://[::1]x")]
    fn parse_rejects_malformed_ipv6_brackets(#[case] input: &str) {
        assert_eq!(BrokerUrl::parse(input), Err(BrokerUrlError::InvalidHost));
    }

    #[test]
    fn parse_rejects_empty_ipv6_host() {
        assert_eq!(BrokerUrl::parse("mqtt://[]:1883"), Err(BrokerUrlError::EmptyHost));
    }

    #[cfg(feature = "websocket")]
    #[rstest]
    #[case::ws_default("ws://broker.example.com", "broker.example.com", 80, false, "/")]
    #[case::ws_port_path("ws://127.0.0.1:8080/mqtt", "127.0.0.1", 8080, false, "/mqtt")]
    #[case::wss_default_path("wss://broker.example.com/", "broker.example.com", 443, true, "/")]
    #[case::wss_port_deep_path("wss://[::1]:8443/a/b?x=1", "::1", 8443, true, "/a/b?x=1")]
    fn parse_websocket_urls(
        #[case] input: &str,
        #[case] host: &str,
        #[case] port: u16,
        #[case] tls: bool,
        #[case] path: &str,
    ) {
        let url = BrokerUrl::parse(input).expect("valid ws url");
        assert_eq!(
            (url.host(), url.port(), url.use_tls(), url.websocket_path()),
            (host, port, tls, Some(path))
        );
    }

    #[cfg(not(feature = "websocket"))]
    #[test]
    fn websocket_urls_need_the_feature() {
        assert_eq!(
            BrokerUrl::parse("ws://localhost:8080"),
            Err(BrokerUrlError::WebSocketUnsupported)
        );
    }

    #[test]
    fn tcp_urls_have_no_websocket_path() {
        assert_eq!(
            BrokerUrl::parse("mqtt://localhost").expect("url").websocket_path(),
            None
        );
    }
}
