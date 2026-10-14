//! HTTP proxy configuration for tunnelling the broker connection with `CONNECT`.

use std::fmt;

use secrecy::SecretString;

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
    credentials: Option<(String, SecretString)>,
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
        let rest = url
            .strip_prefix("http://")
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
            proxy = proxy.basic_auth(percent_decode(user), percent_decode(password));
        }
        Ok(proxy)
    }

    /// Authenticates to the proxy with HTTP Basic (`Proxy-Authorization`). Basic sends the
    /// credentials in clear text to the proxy; use it only on a network you trust.
    pub fn basic_auth(mut self, user: impl Into<String>, password: impl Into<SecretString>) -> Self {
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

    pub(crate) fn credentials(&self) -> Option<&(String, SecretString)> {
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

/// Decodes `%XX` escapes; anything malformed is kept as written.
fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && let Some(byte) = input.get(i + 1..i + 3).and_then(|hex| u8::from_str_radix(hex, 16).ok())
        {
            out.push(byte);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use secrecy::ExposeSecret;

    use super::*;

    #[test]
    fn parse_reads_host_port_and_credentials() {
        let proxy = HttpProxy::parse("http://svc%40corp:p%3Ass@proxy.internal:3128/").expect("valid");
        assert_eq!((proxy.host(), proxy.port()), ("proxy.internal", 3128));
        let (user, password) = proxy.credentials().expect("credentials");
        assert_eq!((user.as_str(), password.expose_secret()), ("svc@corp", "p:ss"));

        let bare = HttpProxy::parse("http://[fd00::1]").expect("valid");
        assert_eq!((bare.host(), bare.port()), ("fd00::1", 80));
        assert!(bare.credentials().is_none());
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
}
