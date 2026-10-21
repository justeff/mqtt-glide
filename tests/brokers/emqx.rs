//! Enhanced authentication (§4.12) against EMQX: SCRAM-SHA-256 (RFC 5802, RFC 7677) in the
//! CONNECT and in a re-authentication on the open connection, with the broker's signature
//! checked in `finish`.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use hmac::{Hmac, KeyInit, Mac};
use mqtt_glide::{
    AuthError, AuthExchange, Authenticator, ClientBuilder, ConnectError, InboundMessage, MqttClient, NoReconnect, QoS,
};
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;
use tokio::time::timeout;

use crate::helpers::start_emqx_scram;

const USER: &str = "glide";
const PASSWORD: &str = "scram-secret";
const WAIT: Duration = Duration::from_secs(10);

/// A SCRAM-SHA-256 client without channel binding.
struct ScramSha256 {
    user: &'static str,
    password: &'static str,
    starts: AtomicUsize,
    invalidations: AtomicUsize,
    /// Exchanges whose server signature checked out.
    verified: Arc<AtomicUsize>,
}

impl ScramSha256 {
    fn new(password: &'static str) -> Arc<Self> {
        Arc::new(Self {
            user: USER,
            password,
            starts: AtomicUsize::new(0),
            invalidations: AtomicUsize::new(0),
            verified: Arc::new(AtomicUsize::new(0)),
        })
    }
}

struct ScramExchange {
    password: &'static str,
    nonce: String,
    client_first_bare: String,
    /// Set by `respond`: the signature the broker must prove in its final message.
    server_signature: Option<[u8; 32]>,
    verified: Arc<AtomicUsize>,
}

impl Authenticator for ScramSha256 {
    type Exchange = ScramExchange;

    fn method(&self) -> &str {
        "SCRAM-SHA-256"
    }

    async fn start(&self) -> Result<(ScramExchange, Option<Bytes>), AuthError> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        let nonce = uuid::Uuid::new_v4().simple().to_string();
        let client_first_bare = format!("n={},r={nonce}", self.user);
        let first = Bytes::from(format!("n,,{client_first_bare}"));
        let exchange = ScramExchange {
            password: self.password,
            nonce,
            client_first_bare,
            server_signature: None,
            verified: Arc::clone(&self.verified),
        };
        Ok((exchange, Some(first)))
    }

    async fn invalidate(&self) {
        self.invalidations.fetch_add(1, Ordering::SeqCst);
    }
}

impl AuthExchange for ScramExchange {
    async fn respond(&mut self, challenge: Option<Bytes>) -> Result<Option<Bytes>, AuthError> {
        if self.server_signature.is_some() {
            return Err(protocol("a second challenge"));
        }
        let server_first = utf8(challenge)?;
        let nonce = attribute(&server_first, 'r')?;
        let salt = base64_decode(attribute(&server_first, 's')?)?;
        let iterations: u32 = attribute(&server_first, 'i')?
            .parse()
            .map_err(|_| protocol("an iteration count that is no number"))?;
        if !nonce.starts_with(&self.nonce) || nonce.len() == self.nonce.len() {
            return Err(protocol("a nonce not extending the client's"));
        }
        if iterations < 4096 {
            return Err(protocol("fewer than 4096 iterations"));
        }

        let salted = salted_password(self.password.as_bytes(), &salt, iterations);
        let client_key = hmac(&salted, b"Client Key");
        let stored_key: [u8; 32] = Sha256::digest(client_key).into();
        // `biws` is base64 of the GS2 header `n,,`: no channel binding.
        let without_proof = format!("c=biws,r={nonce}");
        let auth_message = format!("{},{server_first},{without_proof}", self.client_first_bare);
        let signature = hmac(&stored_key, auth_message.as_bytes());
        let proof: Vec<u8> = client_key.iter().zip(signature).map(|(key, sig)| key ^ sig).collect();
        let server_key = hmac(&salted, b"Server Key");
        self.server_signature = Some(hmac(&server_key, auth_message.as_bytes()));
        Ok(Some(Bytes::from(format!(
            "{without_proof},p={}",
            base64_encode(&proof)
        ))))
    }

    async fn finish(&mut self, data: Option<Bytes>) -> Result<(), AuthError> {
        let expected = self
            .server_signature
            .ok_or_else(|| protocol("success before the challenge"))?;
        let server_final = utf8(data)?;
        if attribute(&server_final, 'e').is_ok() {
            return Err(AuthError::NotAuthorized);
        }
        if base64_decode(attribute(&server_final, 'v')?)? != expected {
            // The broker does not know the password: do not trust it.
            return Err(AuthError::NotAuthorized);
        }
        self.verified.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn protocol(what: &str) -> AuthError {
    AuthError::Provider(format!("SCRAM: broker sent {what}"))
}

fn utf8(data: Option<Bytes>) -> Result<String, AuthError> {
    let data = data.ok_or_else(|| protocol("no data"))?;
    String::from_utf8(data.to_vec()).map_err(|_| protocol("data that is not UTF-8"))
}

/// The value of `name=` in a SCRAM message.
fn attribute(message: &str, name: char) -> Result<&str, AuthError> {
    message
        .split(',')
        .find_map(|field| field.strip_prefix(name)?.strip_prefix('='))
        .ok_or_else(|| protocol(&format!("no `{name}` attribute")))
}

fn hmac(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC takes any key length");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

/// `Hi()` of RFC 5802: PBKDF2 with HMAC-SHA-256, one block.
fn salted_password(password: &[u8], salt: &[u8], iterations: u32) -> [u8; 32] {
    let mut block = hmac(password, &[salt, &1u32.to_be_bytes()].concat());
    let mut result = block;
    for _ in 1..iterations {
        block = hmac(password, &block);
        result.iter_mut().zip(block).for_each(|(out, byte)| *out ^= byte);
    }
    result
}

const BASE64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64_encode(data: &[u8]) -> String {
    let mut out = String::new();
    for chunk in data.chunks(3) {
        let bits = chunk
            .iter()
            .enumerate()
            .fold(0u32, |acc, (i, &b)| acc | u32::from(b) << (16 - 8 * i));
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(char::from(BASE64[(bits >> (18 - 6 * i) & 63) as usize]));
            } else {
                out.push('=');
            }
        }
    }
    out
}

fn base64_decode(text: &str) -> Result<Vec<u8>, AuthError> {
    let mut out = Vec::new();
    let (mut bits, mut count) = (0u32, 0);
    for byte in text.trim_end_matches('=').bytes() {
        let value = BASE64
            .iter()
            .position(|&c| c == byte)
            .ok_or_else(|| protocol("invalid base64"))?;
        bits = bits << 6 | value as u32;
        count += 6;
        if count >= 8 {
            count -= 8;
            out.push((bits >> count) as u8);
        }
    }
    Ok(out)
}

async fn connect_scram(
    port: u16,
    authenticator: &Arc<ScramSha256>,
) -> Result<(MqttClient, mpsc::Receiver<InboundMessage>), ConnectError> {
    let (tx, rx) = mpsc::channel(16);
    let client = ClientBuilder::new(format!("mqtt://127.0.0.1:{port}"))
        .expect("url")
        .authenticator(Arc::clone(authenticator))
        .reconnect(NoReconnect)
        .inbound_sink(tx)
        .connect()
        .await?;
    Ok((client, rx))
}

async fn round_trip(client: &MqttClient, rx: &mut mpsc::Receiver<InboundMessage>, payload: &'static str) {
    client
        .publish("scram/echo", payload)
        .qos(QoS::AtLeastOnce)
        .await
        .expect("publish");
    let message = timeout(WAIT, rx.recv()).await.expect("in time").expect("open");
    assert_eq!(&message.payload[..], payload.as_bytes());
}

#[test]
fn scram_primitives_match_the_rfc_7677_example() {
    // RFC 7677 §3: user "user", password "pencil".
    let salt = base64_decode("W22ZaJ0SNY7soEsUEjb6gQ==").expect("salt");
    let salted = salted_password(b"pencil", &salt, 4096);
    let client_key = hmac(&salted, b"Client Key");
    let stored_key: [u8; 32] = Sha256::digest(client_key).into();
    let auth_message = "n=user,r=rOprNGfwEbeRWgbNEkqO,\
                        r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096,\
                        c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0";
    let signature = hmac(&stored_key, auth_message.as_bytes());
    let proof: Vec<u8> = client_key.iter().zip(signature).map(|(key, sig)| key ^ sig).collect();
    assert_eq!(base64_encode(&proof), "dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=");
    let server_signature = hmac(&hmac(&salted, b"Server Key"), auth_message.as_bytes());
    assert_eq!(
        base64_encode(&server_signature),
        "6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="
    );
    assert_eq!(base64_decode(&base64_encode(b"ab")).expect("decode"), b"ab");
}

/// SCRAM in the CONNECT: the broker's signature checks out; a re-authentication on the open
/// connection runs a second exchange while messages keep flowing. A wrong password is refused
/// in the CONNACK, as an auth error that drops cached material. One container for both: EMQX
/// maps many ports, and parallel starts race for host ports under rootless Podman.
#[tokio::test]
async fn emqx_scram_connect_reauthentication_and_refusal() {
    let Some((_broker, port)) = start_emqx_scram(USER, PASSWORD).await else {
        return;
    };
    let authenticator = ScramSha256::new(PASSWORD);
    let (client, mut rx) = connect_scram(port, &authenticator).await.expect("SCRAM connect");
    assert_eq!(authenticator.verified.load(Ordering::SeqCst), 1);
    client
        .subscribe([("scram/echo", QoS::AtLeastOnce)])
        .await
        .expect("subscribe");
    round_trip(&client, &mut rx, "before").await;

    timeout(WAIT, client.reauthenticate())
        .await
        .expect("re-authentication in time")
        .expect("re-authentication");
    assert_eq!(authenticator.starts.load(Ordering::SeqCst), 2);
    assert_eq!(authenticator.verified.load(Ordering::SeqCst), 2);
    round_trip(&client, &mut rx, "after").await;
    assert_eq!(authenticator.invalidations.load(Ordering::SeqCst), 0);
    client.shutdown().await.expect("shutdown");

    let impostor = ScramSha256::new("not-the-password");
    let error = connect_scram(port, &impostor).await.expect_err("wrong password");
    assert!(
        matches!(error, ConnectError::Auth(AuthError::NotAuthorized)),
        "{error:?}"
    );
    assert_eq!(impostor.verified.load(Ordering::SeqCst), 0);
    assert_eq!(impostor.invalidations.load(Ordering::SeqCst), 1);
}
