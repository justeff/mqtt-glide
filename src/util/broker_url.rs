//! Broker URL parsing (`mqtt://`, `mqtts://`, host, port).

use thiserror::Error;

/// Default MQTT port for unencrypted connections (`mqtt://` and bare hostnames).
const DEFAULT_MQTT_PORT: u16 = 1883;

/// Default MQTT port for TLS-encrypted connections (`ssl://`, `mqtts://`).
const DEFAULT_MQTTS_PORT: u16 = 8883;

/// Parsed MQTT broker endpoint (host, port, TLS requirement).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerUrl {
    host: String,
    port: u16,
    use_tls: bool,
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
}

impl BrokerUrl {
    /// Parses host, port, and TLS requirement from a broker URL.
    ///
    /// Supported schemes:
    /// - `ssl://host` / `mqtts://host` — TLS (default port 8883)
    /// - `mqtt://host` — plain TCP (default port 1883)
    /// - bare hostname — plain TCP (default port 1883)
    ///
    /// An explicit port overrides the scheme default: `ssl://host:9883`.
    /// Trailing slashes on the host segment are stripped.
    pub fn parse(url: &str) -> Result<Self, BrokerUrlError> {
        let (host_port, use_tls) =
            if let Some(rest) = url.strip_prefix("ssl://").or_else(|| url.strip_prefix("mqtts://")) {
                (rest, true)
            } else if let Some(rest) = url.strip_prefix("mqtt://") {
                (rest, false)
            } else {
                (url, false)
            };

        let host_port = host_port.trim_end_matches('/');
        let default_port = if use_tls { DEFAULT_MQTTS_PORT } else { DEFAULT_MQTT_PORT };

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

        Ok(Self {
            host: host.to_owned(),
            port,
            use_tls,
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
}
