//! TLS trust, client certificate and ALPN for `mqtts://` / `wss://` connections.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustls::RootCertStore;
use rustls::pki_types::pem::{self, PemObject};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use zeroize::Zeroizing;

/// How the client authenticates the broker over TLS, and how it authenticates itself.
///
/// The default trusts the operating system's root store, like a browser. A broker on a
/// private PKI needs its CA ([`Self::ca_pem`], [`Self::ca_pem_file`]); a broker that asks
/// for client certificates (mutual TLS, e.g. AWS IoT Core) needs [`Self::client_auth_pem`] or
/// [`Self::client_auth_pem_files`]. The broker certificate is always verified: there is no
/// switch to turn that off.
///
/// Certificates and keys are PEM. The `*_file` variants read the files again on every connect,
/// so certificates rotated on disk (a Kubernetes Secret renewed by cert-manager, say) are used
/// from the next reconnect on without restarting the client.
///
/// ```no_run
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use mqtt_glide::{BrokerUrl, ConnectOptions, TlsConfig};
///
/// let tls = TlsConfig::ca_pem_file("/etc/mqtt/ca.pem")
///     .client_auth_pem_files("/etc/mqtt/tls.crt", "/etc/mqtt/tls.key")
///     .alpn(["x-amzn-mqtt-ca"])?;
/// let options = ConnectOptions::new(BrokerUrl::parse("mqtts://broker.internal:8883")?).tls(tls);
/// # let _ = options;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Default)]
pub struct TlsConfig {
    roots: Roots,
    client_auth: Option<ClientAuth>,
    alpn: Vec<Vec<u8>>,
    server_name: Option<ServerName<'static>>,
}

#[derive(Clone, Default)]
enum Roots {
    #[default]
    System,
    Custom {
        anchors: Anchors,
        system: bool,
    },
}

#[derive(Clone)]
enum Anchors {
    Parsed(Arc<[CertificateDer<'static>]>),
    File(PathBuf),
}

#[derive(Clone)]
enum ClientAuth {
    Parsed {
        chain: Arc<[CertificateDer<'static>]>,
        key: Arc<Zeroizing<PrivateKeyDer<'static>>>,
    },
    Files {
        chain: PathBuf,
        key: PathBuf,
    },
}

/// Why certificate, key or TLS settings handed to [`TlsConfig`] are unusable.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum TlsConfigError {
    /// The PEM holds no `CERTIFICATE` section.
    #[error("no certificate in the PEM data")]
    NoCertificate,
    /// The PEM holds no `PRIVATE KEY`, `RSA PRIVATE KEY` or `EC PRIVATE KEY` section.
    #[error("no private key in the PEM data")]
    NoPrivateKey,
    /// The PEM data is malformed.
    #[error("invalid PEM: {0}")]
    InvalidPem(String),
    /// rustls refused a certificate or key: not a valid CA certificate, a key type the crypto
    /// provider does not support, or a key whose public half differs from the certificate's.
    #[error("rejected by rustls: {0}")]
    Rejected(String),
    /// Not a DNS name or IP address.
    #[error("invalid TLS server name: {0}")]
    InvalidServerName(String),
    /// An ALPN protocol name is empty or longer than 255 bytes.
    #[error("ALPN protocol names must be 1 to 255 bytes")]
    InvalidAlpnProtocol,
}

impl TlsConfig {
    /// Trusts the operating system's root certificates (the default).
    pub fn system_roots() -> Self {
        Self::default()
    }

    /// Trusts the CA certificates in `pem` instead of the system roots.
    ///
    /// Every `CERTIFICATE` section becomes a trust anchor; other sections are ignored. Add
    /// [`Self::with_system_roots`] to trust both.
    pub fn ca_pem(pem: impl AsRef<[u8]>) -> Result<Self, TlsConfigError> {
        let anchors = parse_certificates(pem.as_ref())?;
        root_store(&anchors)?;
        Ok(Self {
            roots: Roots::Custom {
                anchors: Anchors::Parsed(anchors.into()),
                system: false,
            },
            ..Self::default()
        })
    }

    /// Trusts the CA certificates in the PEM file at `path` instead of the system roots.
    ///
    /// The file is read on every connect, not here: a missing or broken file fails that
    /// connection attempt with [`crate::TransportError::Tls`], and the reconnect policy decides
    /// whether to try again.
    pub fn ca_pem_file(path: impl Into<PathBuf>) -> Self {
        Self {
            roots: Roots::Custom {
                anchors: Anchors::File(path.into()),
                system: false,
            },
            ..Self::default()
        }
    }

    /// Also trusts the system roots next to the CA set with [`Self::ca_pem`] or
    /// [`Self::ca_pem_file`].
    pub fn with_system_roots(mut self) -> Self {
        if let Roots::Custom { system, .. } = &mut self.roots {
            *system = true;
        }
        self
    }

    /// Presents a client certificate (mutual TLS).
    ///
    /// `chain_pem` holds the client certificate first, then any intermediates. `key_pem`
    /// holds its private key as PKCS#8 (`PRIVATE KEY`), PKCS#1 (`RSA PRIVATE KEY`) or SEC1
    /// (`EC PRIVATE KEY`). The configuration's copy of the key is wiped from memory when its
    /// last clone is dropped.
    pub fn client_auth_pem(
        mut self,
        chain_pem: impl AsRef<[u8]>,
        key_pem: impl AsRef<[u8]>,
    ) -> Result<Self, TlsConfigError> {
        let chain = parse_certificates(chain_pem.as_ref())?;
        let key = Zeroizing::new(parse_private_key(key_pem.as_ref())?);
        check_client_auth(&chain, &key)?;
        self.client_auth = Some(ClientAuth::Parsed {
            chain: chain.into(),
            key: Arc::new(key),
        });
        Ok(self)
    }

    /// Presents the client certificate chain and key from PEM files (mutual TLS).
    ///
    /// The formats are those of [`Self::client_auth_pem`]. Both files are read on every
    /// connect, so a renewed certificate is presented from the next reconnect on. A missing or
    /// broken file fails that connection attempt with [`crate::TransportError::Tls`].
    pub fn client_auth_pem_files(mut self, chain_path: impl Into<PathBuf>, key_path: impl Into<PathBuf>) -> Self {
        self.client_auth = Some(ClientAuth::Files {
            chain: chain_path.into(),
            key: key_path.into(),
        });
        self
    }

    /// Offers these ALPN protocols in the TLS handshake, most preferred first.
    ///
    /// AWS IoT Core expects `x-amzn-mqtt-ca` for MQTT with client certificates on port 443.
    /// The broker may pick none of them; MQTT does not depend on the outcome.
    pub fn alpn<I, P>(mut self, protocols: I) -> Result<Self, TlsConfigError>
    where
        I: IntoIterator<Item = P>,
        P: Into<Vec<u8>>,
    {
        let protocols: Vec<Vec<u8>> = protocols.into_iter().map(Into::into).collect();
        if protocols
            .iter()
            .any(|protocol| protocol.is_empty() || protocol.len() > 255)
        {
            return Err(TlsConfigError::InvalidAlpnProtocol);
        }
        self.alpn = protocols;
        Ok(self)
    }

    /// Verifies the broker certificate against `name` (a DNS name or IP address) instead of
    /// the broker URL's host, and sends it as SNI.
    ///
    /// For brokers reached through an address their certificate does not name: a bare IP,
    /// a port-forward, or an internal alias. The URL still decides where the client connects;
    /// after a server redirect (§4.11) the certificate is still checked against `name`.
    pub fn server_name(mut self, name: &str) -> Result<Self, TlsConfigError> {
        self.server_name = Some(parse_server_name(name).ok_or_else(|| TlsConfigError::InvalidServerName(name.into()))?);
        Ok(self)
    }

    /// Name to verify the broker certificate against: the override, or the broker host.
    pub(crate) fn server_name_for(&self, host: &str) -> Result<ServerName<'static>, String> {
        match &self.server_name {
            Some(name) => Ok(name.clone()),
            None => parse_server_name(host).ok_or_else(|| format!("invalid TLS server name: {host}")),
        }
    }

    /// A fresh rustls configuration for one connection attempt, reading the files it names.
    pub(crate) fn client_config(&self) -> Result<rustls::ClientConfig, String> {
        let roots = match &self.roots {
            Roots::System => system_root_store(RootCertStore::empty(), true)?,
            Roots::Custom { anchors, system } => {
                let store = match anchors {
                    Anchors::Parsed(anchors) => root_store(anchors),
                    Anchors::File(path) => root_store(&parse_certificates(&read_file(path)?).map_err(at(path))?),
                }
                .map_err(|error| error.to_string())?;
                if *system {
                    system_root_store(store, false)?
                } else {
                    store
                }
            }
        };

        // Explicit provider: MQTT TLS works even if the host process also enables
        // another rustls crypto feature (no reliance on process-global install_default).
        let builder = rustls::ClientConfig::builder_with_provider(Arc::new(selected_crypto_provider()))
            .with_safe_default_protocol_versions()
            .map_err(|error| format!("rustls protocol versions unavailable for selected crypto provider: {error}"))?
            .with_root_certificates(roots);
        let mut config = match &self.client_auth {
            None => builder.with_no_client_auth(),
            Some(ClientAuth::Parsed { chain, key }) => builder
                .with_client_auth_cert(chain.to_vec(), key.clone_key())
                .map_err(|error| format!("client certificate: {error}"))?,
            Some(ClientAuth::Files { chain, key }) => {
                let certificates = parse_certificates(&read_file(chain)?).map_err(at(chain))?;
                let key_pem = Zeroizing::new(read_file(key)?);
                let key_der = parse_private_key(&key_pem).map_err(at(key))?;
                builder
                    .with_client_auth_cert(certificates, key_der)
                    .map_err(|error| format!("client certificate {}: {error}", chain.display()))?
            }
        };
        config.alpn_protocols = self.alpn.clone();
        Ok(config)
    }
}

impl fmt::Debug for TlsConfig {
    /// Names files and counts certificates; never shows key or certificate bytes.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("TlsConfig");
        match &self.roots {
            Roots::System => debug.field("roots", &"system"),
            Roots::Custom { anchors, system } => {
                match anchors {
                    Anchors::Parsed(anchors) => debug.field("ca_certificates", &anchors.len()),
                    Anchors::File(path) => debug.field("ca_file", path),
                };
                debug.field("system_roots", system)
            }
        };
        match &self.client_auth {
            None => debug.field("client_auth", &None::<()>),
            Some(ClientAuth::Parsed { chain, .. }) => debug.field("client_certificates", &chain.len()),
            Some(ClientAuth::Files { chain, key }) => {
                debug.field("client_cert_file", chain).field("client_key_file", key)
            }
        };
        let alpn: Vec<_> = self
            .alpn
            .iter()
            .map(|protocol| String::from_utf8_lossy(protocol))
            .collect();
        debug
            .field("alpn", &alpn)
            .field("server_name", &self.server_name.as_ref().map(ServerName::to_str))
            .finish()
    }
}

/// Crypto provider selected by the active `tls-rustls-*` Cargo feature.
pub(crate) fn selected_crypto_provider() -> rustls::crypto::CryptoProvider {
    #[cfg(feature = "tls-rustls-aws-lc")]
    {
        rustls::crypto::aws_lc_rs::default_provider()
    }
    #[cfg(feature = "tls-rustls-ring")]
    {
        rustls::crypto::ring::default_provider()
    }
}

fn parse_server_name(name: &str) -> Option<ServerName<'static>> {
    if let Ok(ip) = name.parse::<std::net::IpAddr>() {
        return Some(ServerName::IpAddress(ip.into()));
    }
    ServerName::try_from(name.to_owned()).ok()
}

fn parse_certificates(pem: &[u8]) -> Result<Vec<CertificateDer<'static>>, TlsConfigError> {
    let certificates = CertificateDer::pem_slice_iter(pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(pem_error)?;
    if certificates.is_empty() {
        return Err(TlsConfigError::NoCertificate);
    }
    Ok(certificates)
}

fn parse_private_key(pem: &[u8]) -> Result<PrivateKeyDer<'static>, TlsConfigError> {
    PrivateKeyDer::from_pem_slice(pem).map_err(|error| match error {
        pem::Error::NoItemsFound => TlsConfigError::NoPrivateKey,
        other => pem_error(other),
    })
}

fn pem_error(error: pem::Error) -> TlsConfigError {
    TlsConfigError::InvalidPem(error.to_string())
}

fn root_store(anchors: &[CertificateDer<'static>]) -> Result<RootCertStore, TlsConfigError> {
    let mut store = RootCertStore::empty();
    for anchor in anchors {
        store
            .add(anchor.clone())
            .map_err(|error| TlsConfigError::Rejected(format!("CA certificate: {error}")))?;
    }
    Ok(store)
}

/// Adds the OS trust store; with `required`, finding no usable anchor is an error.
fn system_root_store(mut store: RootCertStore, required: bool) -> Result<RootCertStore, String> {
    let native = rustls_native_certs::load_native_certs();
    for cert in native.certs {
        store
            .add(cert)
            .map_err(|error| format!("invalid system trust anchor: {error}"))?;
    }
    if required && store.is_empty() {
        return Err(native
            .errors
            .first()
            .map(|error| error.to_string())
            .unwrap_or_else(|| "no system trust anchors found".into()));
    }
    Ok(store)
}

/// Checks the pair with rustls now, so a key that does not fit fails at configuration time.
fn check_client_auth(chain: &[CertificateDer<'static>], key: &PrivateKeyDer<'static>) -> Result<(), TlsConfigError> {
    rustls::ClientConfig::builder_with_provider(Arc::new(selected_crypto_provider()))
        .with_safe_default_protocol_versions()
        .map_err(|error| TlsConfigError::Rejected(error.to_string()))?
        .with_root_certificates(RootCertStore::empty())
        .with_client_auth_cert(chain.to_vec(), key.clone_key())
        .map(drop)
        .map_err(|error| TlsConfigError::Rejected(error.to_string()))
}

/// Reads a certificate or key file; the error names the path, never the content.
fn read_file(path: &Path) -> Result<Vec<u8>, String> {
    std::fs::read(path).map_err(|error| format!("reading {}: {error}", path.display()))
}

fn at(path: &Path) -> impl Fn(TlsConfigError) -> String + '_ {
    move |error| format!("{}: {error}", path.display())
}

#[cfg(test)]
#[path = "tls_tests.rs"]
mod tests;
