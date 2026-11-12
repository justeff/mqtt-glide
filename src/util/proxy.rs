//! HTTP proxy configuration for tunnelling the broker connection with `CONNECT`.

use std::fmt;

use crate::client::Password;

use super::broker_url::{BrokerUrlError, split_host_port};

/// Default port of an `http://` proxy URL without one.
const DEFAULT_HTTP_PORT: u16 = 80;

/// An HTTP proxy the broker connection goes through, via `CONNECT host:port`
/// (RFC 9110 §9.3.6), for networks that only let traffic out through one.
///
/// The tunnel carries the connection as it would otherwise go out: TLS (`mqtts://`, `wss://`)
/// runs end to end with the broker, so the proxy sees only the broker's host and port. Set it
/// with [`ConnectOptions::http_proxy`](crate::ConnectOptions::http_proxy).
///
/// # Examples
///
/// ```
/// use mqtt_glide::HttpProxy;
///
/// let proxy = HttpProxy::parse("http://proxy.internal:3128").expect("valid proxy URL");
/// assert_eq!((proxy.host(), proxy.port()), ("proxy.internal", 3128));
/// let with_auth = HttpProxy::new("proxy.internal", 3128).basic_auth("svc", "secret");
/// assert!(!format!("{with_auth:?}").contains("secret"));
/// ```
#[derive(Clone)]
pub struct HttpProxy {
    host: String,
    port: u16,
    credentials: Option<(String, Password)>,
}

impl HttpProxy {
    /// A proxy at `host:port` without authentication.
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
            credentials: None,
        }
    }

    /// Parses `http://[user:password@]host[:port]` (port defaults to 80; percent-escapes in
    /// user and password are decoded). Only `http://` proxies are supported.
    pub fn parse(url: &str) -> Result<Self, BrokerUrlError> {
        // Schemes are case-insensitive (RFC 3986 §3.1), as in `BrokerUrl::parse`.
        let rest = url
            .split_once("://")
            .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("http"))
            .map(|(_, rest)| rest)
            .ok_or(BrokerUrlError::UnsupportedProxyScheme)?;
        let rest = rest.trim_end_matches('/');
        let (userinfo, host_port) = match rest.rsplit_once('@') {
            Some((userinfo, host_port)) => (Some(userinfo), host_port),
            None => (None, rest),
        };
        let (host, port) = split_host_port(host_port, DEFAULT_HTTP_PORT)?;
        let mut proxy = Self::new(host, port);
        if let Some(userinfo) = userinfo {
            let (user, password) = userinfo.split_once(':').unwrap_or((userinfo, ""));
            let user = String::from_utf8(percent_decode(user)).map_err(|_| BrokerUrlError::InvalidProxyCredentials)?;
            if user.contains(':') {
                return Err(BrokerUrlError::InvalidProxyCredentials);
            }
            // The password stays bytes: a `%FC` is the byte 0xFC, not a replacement character.
            proxy = proxy.basic_auth(user, Password::from(percent_decode(password)));
        }
        Ok(proxy)
    }

    /// Authenticates to the proxy with HTTP Basic (`Proxy-Authorization`). Basic sends the
    /// credentials in clear text to the proxy; use it only on a network you trust. A user name
    /// with `:` cannot be sent (Basic splits at the first one): `connect` refuses it.
    pub fn basic_auth(mut self, user: impl Into<String>, password: impl Into<Password>) -> Self {
        self.credentials = Some((user.into(), password.into()));
        self
    }

    /// The proxy's host.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// The proxy's port.
    pub fn port(&self) -> u16 {
        self.port
    }

    pub(crate) fn credentials(&self) -> Option<&(String, Password)> {
        self.credentials.as_ref()
    }
}

impl fmt::Debug for HttpProxy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpProxy")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.credentials.as_ref().map(|(user, _)| user))
            .finish_non_exhaustive()
    }
}

/// Decodes `%XX` escapes (two hex digits; anything else is kept as written) into bytes.
fn percent_decode(input: &str) -> Vec<u8> {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && let Some(hex) = bytes.get(i + 1..i + 3)
            && hex.iter().all(u8::is_ascii_hexdigit)
        {
            let hex = std::str::from_utf8(hex).expect("ASCII hex digits");
            out.push(u8::from_str_radix(hex, 16).expect("two hex digits"));
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_reads_host_port_and_credentials() {
        let proxy = HttpProxy::parse("http://svc%40corp:p%3Ass@proxy.internal:3128/").expect("valid");
        assert_eq!((proxy.host(), proxy.port()), ("proxy.internal", 3128));
        let (user, password) = proxy.credentials().expect("credentials");
        assert_eq!((user.as_str(), password.expose()), ("svc@corp", b"p:ss".as_slice()));

        let bare = HttpProxy::parse("http://[fd00::1]").expect("valid");
        assert_eq!((bare.host(), bare.port()), ("fd00::1", 80));
        assert!(bare.credentials().is_none());

        let upper = HttpProxy::parse("HTTP://proxy:3128").expect("scheme in any case");
        assert_eq!((upper.host(), upper.port()), ("proxy", 3128));
    }

    #[test]
    fn parse_rejects_other_schemes_and_bad_hosts() {
        assert_eq!(
            HttpProxy::parse("https://proxy:443").unwrap_err(),
            BrokerUrlError::UnsupportedProxyScheme
        );
        assert_eq!(
            HttpProxy::parse("socks5://proxy:1080").unwrap_err(),
            BrokerUrlError::UnsupportedProxyScheme
        );
        assert_eq!(
            HttpProxy::parse("proxy:3128").unwrap_err(),
            BrokerUrlError::UnsupportedProxyScheme
        );
        assert_eq!(HttpProxy::parse("http://:3128").unwrap_err(), BrokerUrlError::EmptyHost);
        assert_eq!(
            HttpProxy::parse("http://proxy:x").unwrap_err(),
            BrokerUrlError::InvalidPort
        );
    }

    #[test]
    fn debug_never_shows_the_password() {
        let proxy = HttpProxy::new("proxy", 3128).basic_auth("svc", "hunter2");
        let shown = format!("{proxy:?}");
        assert!(shown.contains("svc") && !shown.contains("hunter2"), "{shown}");
    }

    /// The password is decoded to bytes, as given: a `%FC` is 0xFC, a sign is no hex digit,
    /// and a user name with `:` (which Basic cannot carry) is refused.
    #[test]
    fn credentials_are_decoded_exactly() {
        let proxy = HttpProxy::parse("http://u:p%FCss@h:1").expect("valid");
        assert_eq!(proxy.credentials().expect("credentials").1.expose(), b"p\xFCss");
        let proxy = HttpProxy::parse("http://u:%+1@h:1").expect("valid");
        assert_eq!(proxy.credentials().expect("credentials").1.expose(), b"%+1");
        assert!(matches!(
            HttpProxy::parse("http://us%3Aer:p@h:1"),
            Err(BrokerUrlError::InvalidProxyCredentials)
        ));
    }
}
