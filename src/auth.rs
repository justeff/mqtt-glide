//! Enhanced authentication (§4.12): a challenge / response exchange with the broker.

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use bytestring::ByteString;

use crate::error::{AppError, AuthError};

/// Enhanced authentication (§4.12): SCRAM, Kerberos or a broker's own scheme, run as a
/// challenge / response exchange in `AUTH` packets.
///
/// Set it with [`ConnectOptions::authenticator`](crate::ConnectOptions::authenticator) or
/// [`ClientBuilder::authenticator`](crate::ClientBuilder::authenticator). Every connection then
/// starts an exchange: CONNECT carries [`Self::method`] and the data from [`Self::start`], each
/// challenge the broker sends goes to [`AuthExchange::respond`], and the CONNACK that accepts the
/// client goes to [`AuthExchange::finish`]. [`MqttClient::reauthenticate`](crate::MqttClient::reauthenticate)
/// runs a fresh exchange on the open connection (§4.12.1), for example before a token expires.
///
/// Username and password from the [`CredentialsProvider`](crate::CredentialsProvider) are sent
/// as well when it supplies them; most methods leave them empty
/// ([`AnonymousCredentials`](crate::AnonymousCredentials)).
///
/// Errors are [`AuthError`]s: a failure before the CONNECT ends that connection attempt, one
/// during an exchange also sends DISCONNECT 0x80 (Unspecified error) and closes the connection.
/// Return [`AuthError::NotAuthorized`] when the broker cannot be trusted (a wrong server
/// signature), [`AuthError::Provider`] when the material could not be obtained.
///
/// # Example
///
/// A method where the broker sends a nonce and the client answers with a token bound to it:
///
/// ```no_run
/// use bytes::Bytes;
/// use mqtt_glide::{AuthError, AuthExchange, Authenticator};
///
/// struct NonceToken;
///
/// struct NonceExchange;
///
/// impl Authenticator for NonceToken {
///     type Exchange = NonceExchange;
///
///     fn method(&self) -> &str {
///         "example-nonce-token"
///     }
///
///     async fn start(&self) -> Result<(NonceExchange, Option<Bytes>), AuthError> {
///         Ok((NonceExchange, None))
///     }
/// }
///
/// impl AuthExchange for NonceExchange {
///     async fn respond(&mut self, challenge: Option<Bytes>) -> Result<Option<Bytes>, AuthError> {
///         let nonce = challenge.ok_or_else(|| AuthError::Provider("challenge without nonce".into()))?;
///         Ok(Some(sign(&nonce)))
///     }
/// }
/// # fn sign(nonce: &[u8]) -> Bytes { Bytes::copy_from_slice(nonce) }
/// ```
pub trait Authenticator: Send + Sync {
    /// The state of one exchange.
    type Exchange: AuthExchange + 'static;

    /// The Authentication Method (§3.1.2.11.9), for example `SCRAM-SHA-256`. Read once, when
    /// the client is created; the same value goes into every CONNECT and AUTH.
    fn method(&self) -> &str;

    /// Starts an exchange: for each CONNECT and each re-authentication. Returns its state and the
    /// Authentication Data to send first (the CONNECT's, or the AUTH 0x19 Re-authenticate's).
    fn start(&self) -> impl Future<Output = Result<(Self::Exchange, Option<Bytes>), AuthError>> + Send;

    /// The authentication failed (CONNACK 0x86 / 0x87, DISCONNECT 0x87 / 0x8C, or
    /// [`AuthExchange::finish`] rejecting the broker): drop cached material so the next
    /// [`Self::start`] fetches it anew. Default: nothing.
    ///
    /// A broker that disconnects with 0x87 for an ACL violation, with credentials that are
    /// fine, calls this too, and so does a broker that fails `finish`'s check: material cheap to
    /// fetch again is the safe kind to cache.
    ///
    /// Each step of an exchange (`start`, [`AuthExchange::respond`], [`AuthExchange::finish`])
    /// has the connect budget ([`ConnectOptions::connect_timeout`](crate::ConnectOptions::connect_timeout))
    /// during CONNECT and [`ConnectOptions::ack_timeout`](crate::ConnectOptions::ack_timeout)
    /// during a re-authentication; a step that takes longer fails the authentication.
    fn invalidate(&self) -> impl Future<Output = ()> + Send {
        async {}
    }
}

/// One exchange started by [`Authenticator::start`]; dropped when it ends.
pub trait AuthExchange: Send {
    /// Answers the broker's AUTH 0x18 (Continue authentication) carrying `challenge`, its
    /// Authentication Data; the answer goes back in an AUTH 0x18.
    fn respond(&mut self, challenge: Option<Bytes>) -> impl Future<Output = Result<Option<Bytes>, AuthError>> + Send;

    /// The broker accepted the client: called with the Authentication Data of the CONNACK, or of
    /// the AUTH 0x00 (Success) ending a re-authentication, for a method that checks the broker
    /// too (SCRAM's server signature). An error closes the connection. Default: accepts.
    fn finish(&mut self, data: Option<Bytes>) -> impl Future<Output = Result<(), AuthError>> + Send {
        let _ = data;
        async { Ok(()) }
    }
}

/// One authenticator shared by several clients.
impl<T: Authenticator> Authenticator for Arc<T> {
    type Exchange = T::Exchange;

    fn method(&self) -> &str {
        (**self).method()
    }

    fn start(&self) -> impl Future<Output = Result<(Self::Exchange, Option<Bytes>), AuthError>> + Send {
        (**self).start()
    }

    fn invalidate(&self) -> impl Future<Output = ()> + Send {
        (**self).invalidate()
    }
}

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// What [`SharedAuthenticator::start`] returns: the erased exchange and the first data.
pub(crate) type Started = (Box<dyn ExchangeErased>, Option<Bytes>);

trait AuthenticatorErased: Send + Sync {
    fn start(&self) -> BoxFuture<'_, Result<Started, AuthError>>;

    fn invalidate(&self) -> BoxFuture<'_, ()>;
}

impl<T: Authenticator> AuthenticatorErased for T {
    fn start(&self) -> BoxFuture<'_, Result<Started, AuthError>> {
        Box::pin(async move {
            let (exchange, data) = Authenticator::start(self).await?;
            Ok((Box::new(exchange) as Box<dyn ExchangeErased>, data))
        })
    }

    fn invalidate(&self) -> BoxFuture<'_, ()> {
        Box::pin(Authenticator::invalidate(self))
    }
}

/// An [`AuthExchange`] behind a box, so the driver can hold any.
pub(crate) trait ExchangeErased: Send {
    fn respond(&mut self, challenge: Option<Bytes>) -> BoxFuture<'_, Result<Option<Bytes>, AuthError>>;

    fn finish(&mut self, data: Option<Bytes>) -> BoxFuture<'_, Result<(), AuthError>>;
}

impl<T: AuthExchange> ExchangeErased for T {
    fn respond(&mut self, challenge: Option<Bytes>) -> BoxFuture<'_, Result<Option<Bytes>, AuthError>> {
        Box::pin(AuthExchange::respond(self, challenge))
    }

    fn finish(&mut self, data: Option<Bytes>) -> BoxFuture<'_, Result<(), AuthError>> {
        Box::pin(AuthExchange::finish(self, data))
    }
}

/// The configured [`Authenticator`], erased, with its method read once.
#[derive(Clone)]
pub(crate) struct SharedAuthenticator {
    method: ByteString,
    inner: Arc<dyn AuthenticatorErased>,
}

impl fmt::Debug for SharedAuthenticator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Authenticator")
            .field("method", &self.method)
            .finish_non_exhaustive()
    }
}

impl SharedAuthenticator {
    pub(crate) fn new(authenticator: impl Authenticator + 'static) -> Self {
        Self {
            method: ByteString::from(authenticator.method()),
            inner: Arc::new(authenticator),
        }
    }

    /// The Authentication Method sent in CONNECT and every AUTH.
    pub(crate) fn method(&self) -> &ByteString {
        &self.method
    }

    pub(crate) async fn start(&self) -> Result<Started, AuthError> {
        self.inner.start().await
    }

    pub(crate) async fn invalidate(&self) {
        self.inner.invalidate().await;
    }

    /// The method must be a non-empty UTF-8 string the encoder can carry (§1.5.4).
    pub(crate) fn validate(&self) -> Result<(), AppError> {
        let method = self.method.as_bytes();
        if method.is_empty() {
            Err(AppError::InvalidRequest(
                "authentication method must not be empty".into(),
            ))
        } else if method.contains(&0) {
            Err(AppError::InvalidRequest(
                "authentication method must not contain U+0000".into(),
            ))
        } else if method.len() > usize::from(u16::MAX) {
            Err(AppError::InvalidRequest(
                "authentication method is longer than 65535 bytes".into(),
            ))
        } else {
            Ok(())
        }
    }
}

/// Authentication Data is Binary Data with a two-byte length (§1.5.6); longer data would fail
/// in the encoder and end the connection as a protocol error, so it is the authenticator's.
pub(crate) fn check_auth_data(data: &Option<Bytes>) -> Result<(), AuthError> {
    match data {
        Some(data) if data.len() > usize::from(u16::MAX) => Err(AuthError::Provider(format!(
            "authentication data of {} bytes is longer than 65535",
            data.len()
        ))),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Echo(&'static str);

    struct EchoExchange {
        rounds: u8,
    }

    impl Authenticator for Echo {
        type Exchange = EchoExchange;

        fn method(&self) -> &str {
            self.0
        }

        async fn start(&self) -> Result<(EchoExchange, Option<Bytes>), AuthError> {
            Ok((EchoExchange { rounds: 0 }, Some(Bytes::from_static(b"first"))))
        }
    }

    impl AuthExchange for EchoExchange {
        async fn respond(&mut self, challenge: Option<Bytes>) -> Result<Option<Bytes>, AuthError> {
            self.rounds += 1;
            Ok(challenge)
        }
    }

    /// The erased wrapper keeps the method, runs the exchange and accepts by default.
    #[tokio::test]
    async fn shared_authenticator_runs_the_exchange() {
        let shared = SharedAuthenticator::new(Arc::new(Echo("echo")));
        assert_eq!(&**shared.method(), "echo");
        assert_eq!(format!("{shared:?}"), r#"Authenticator { method: "echo", .. }"#);

        let (mut exchange, first) = shared.start().await.expect("start");
        assert_eq!(first.as_deref(), Some(&b"first"[..]));
        let answer = exchange.respond(Some(Bytes::from_static(b"c"))).await.expect("respond");
        assert_eq!(answer.as_deref(), Some(&b"c"[..]));
        exchange.finish(None).await.expect("default finish accepts");
        shared.invalidate().await;
    }

    #[test]
    fn method_is_validated() {
        SharedAuthenticator::new(Echo("SCRAM-SHA-256"))
            .validate()
            .expect("valid");
        for method in ["", "a\0b"] {
            let shared = SharedAuthenticator::new(Echo(Box::leak(method.to_owned().into_boxed_str())));
            assert!(
                matches!(shared.validate(), Err(AppError::InvalidRequest(_))),
                "{method:?}"
            );
        }
        let long: &'static str = Box::leak("m".repeat(65_536).into_boxed_str());
        assert!(SharedAuthenticator::new(Echo(long)).validate().is_err());
    }
}
