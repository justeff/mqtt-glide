use super::*;

const CA: &[u8] = include_bytes!("../tests/certs/ca.pem");
const OTHER_CA: &[u8] = include_bytes!("../tests/certs/other-ca.pem");
const CLIENT: &[u8] = include_bytes!("../tests/certs/client.pem");
const CLIENT_KEY: &[u8] = include_bytes!("../tests/certs/client.key");
const CLIENT_SEC1_KEY: &[u8] = include_bytes!("../tests/certs/client-sec1.key");
const CLIENT2_KEY: &[u8] = include_bytes!("../tests/certs/client2.key");

fn certs_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/certs")
}

#[test]
fn server_name_defaults_to_the_broker_host() {
    let tls = TlsConfig::default();
    assert!(matches!(tls.server_name_for("127.0.0.1"), Ok(ServerName::IpAddress(_))));
    assert!(matches!(
        tls.server_name_for("broker.example.com"),
        Ok(ServerName::DnsName(_))
    ));
}

#[test]
fn server_name_override_wins_over_the_broker_host() {
    let tls = TlsConfig::default().server_name("broker.internal").expect("dns name");
    let name = tls.server_name_for("127.0.0.1").expect("override");
    assert_eq!(name.to_str(), "broker.internal");
}

#[test]
fn server_name_rejects_what_is_neither_dns_name_nor_ip() {
    assert_eq!(
        TlsConfig::default().server_name("not a host").err(),
        Some(TlsConfigError::InvalidServerName("not a host".into()))
    );
}

#[test]
fn alpn_rejects_empty_and_overlong_protocol_names() {
    assert_eq!(
        TlsConfig::default().alpn(["mqtt", ""]).err(),
        Some(TlsConfigError::InvalidAlpnProtocol)
    );
    assert_eq!(
        TlsConfig::default().alpn([vec![b'x'; 256]]).err(),
        Some(TlsConfigError::InvalidAlpnProtocol)
    );
    let tls = TlsConfig::default().alpn(["x-amzn-mqtt-ca", "mqtt"]).expect("valid");
    let config = tls.client_config().expect("config");
    assert_eq!(config.alpn_protocols, [b"x-amzn-mqtt-ca".to_vec(), b"mqtt".to_vec()]);
}

#[test]
fn ca_pem_takes_every_certificate_as_trust_anchor() {
    let bundle = [CA, OTHER_CA].concat();
    let tls = TlsConfig::ca_pem(&bundle).expect("bundle");
    assert!(format!("{tls:?}").contains("ca_certificates: 2"), "{tls:?}");
    tls.client_config().expect("config");
}

#[test]
fn ca_pem_without_certificate_is_refused() {
    assert_eq!(TlsConfig::ca_pem(CLIENT_KEY).err(), Some(TlsConfigError::NoCertificate));
    assert_eq!(TlsConfig::ca_pem("").err(), Some(TlsConfigError::NoCertificate));
}

#[test]
fn ca_pem_that_is_malformed_is_refused() {
    let truncated = &CA[..CA.len() / 2];
    assert!(matches!(
        TlsConfig::ca_pem(truncated),
        Err(TlsConfigError::InvalidPem(_))
    ));
}

#[test]
fn ca_pem_with_a_certificate_rustls_cannot_parse_is_refused() {
    let garbage = "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n";
    assert!(matches!(TlsConfig::ca_pem(garbage), Err(TlsConfigError::Rejected(_))));
}

#[test]
fn client_auth_pem_accepts_pkcs8_and_sec1_keys() {
    let tls = TlsConfig::ca_pem(CA).expect("ca");
    tls.clone().client_auth_pem(CLIENT, CLIENT_KEY).expect("pkcs8");
    tls.client_auth_pem(CLIENT, CLIENT_SEC1_KEY).expect("sec1");
}

#[test]
fn client_auth_pem_without_key_or_certificate_is_refused() {
    assert_eq!(
        TlsConfig::default().client_auth_pem(CLIENT, CLIENT).err(),
        Some(TlsConfigError::NoPrivateKey)
    );
    assert_eq!(
        TlsConfig::default().client_auth_pem(CLIENT_KEY, CLIENT_KEY).err(),
        Some(TlsConfigError::NoCertificate)
    );
}

#[test]
fn client_auth_pem_with_a_key_of_another_certificate_is_refused() {
    let error = TlsConfig::default().client_auth_pem(CLIENT, CLIENT2_KEY).err();
    assert!(matches!(error, Some(TlsConfigError::Rejected(_))), "{error:?}");
}

#[test]
fn debug_shows_no_key_or_certificate_bytes() {
    let tls = TlsConfig::ca_pem(CA)
        .and_then(|tls| tls.client_auth_pem(CLIENT, CLIENT_KEY))
        .expect("config");
    let debug = format!("{tls:?}");
    for pem in [CA, CLIENT, CLIENT_KEY] {
        let body = std::str::from_utf8(pem)
            .expect("utf8")
            .lines()
            .nth(1)
            .expect("base64 line");
        assert!(!debug.contains(&body[..16]), "{debug}");
    }
    assert!(debug.contains("client_certificates: 1"), "{debug}");
}

#[test]
fn files_are_read_when_the_connection_is_built() {
    let dir = certs_dir();
    let tls = TlsConfig::ca_pem_file(dir.join("ca.pem"))
        .client_auth_pem_files(dir.join("client.pem"), dir.join("client.key"));
    tls.client_config().expect("config from files");
}

#[test]
fn a_missing_file_names_its_path() {
    let missing = certs_dir().join("missing-ca.pem");
    let error = TlsConfig::ca_pem_file(&missing)
        .client_config()
        .expect_err("missing file");
    assert!(error.contains("missing-ca.pem"), "{error}");

    let dir = certs_dir();
    let tls =
        TlsConfig::ca_pem_file(dir.join("ca.pem")).client_auth_pem_files(dir.join("client.pem"), dir.join("nope.key"));
    let error = tls.client_config().expect_err("missing key");
    assert!(error.contains("nope.key"), "{error}");
}

#[test]
fn a_key_file_without_key_names_its_path_but_not_its_content() {
    let dir = certs_dir();
    let tls = TlsConfig::ca_pem_file(dir.join("ca.pem"))
        .client_auth_pem_files(dir.join("client.pem"), dir.join("client.pem"));
    let error = tls.client_config().expect_err("no key");
    assert!(
        error.contains("client.pem") && error.contains("no private key"),
        "{error}"
    );
}

/// Set in the child run of `system_roots_fail_without_trust_anchors`.
#[cfg(unix)]
const ROOTS_CHILD: &str = "MQTT_GLIDE_ROOTS_CHILD";

/// No trust anchor is a TLS error, never a connector that trusts nothing (or everything); a
/// custom CA next to the empty system store still works. The system store comes from
/// `SSL_CERT_FILE` when it is set, so the test runs itself again in a child process with it
/// pointing at an empty file: the environment of this process stays untouched for the tests
/// running next to it.
#[cfg(unix)]
#[test]
fn system_roots_fail_without_trust_anchors() {
    if std::env::var_os(ROOTS_CHILD).is_some() {
        match TlsConfig::system_roots().client_config() {
            Ok(_) => println!("ROOTS=ok"),
            Err(message) => println!("ROOTS=tls:{message}"),
        }
        let custom = TlsConfig::ca_pem(CA).expect("ca").with_system_roots().client_config();
        println!("CUSTOM={}", if custom.is_ok() { "ok" } else { "err" });
        return;
    }
    let name = module_path!()
        .split_once("::")
        .map(|(_, path)| format!("{path}::system_roots_fail_without_trust_anchors"))
        .expect("module path");
    let output = std::process::Command::new(std::env::current_exe().expect("test binary"))
        .args([name.as_str(), "--exact", "--nocapture", "--test-threads=1"])
        .env(ROOTS_CHILD, "1")
        .env("SSL_CERT_FILE", "/dev/null")
        .env_remove("SSL_CERT_DIR")
        .output()
        .expect("run child");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success(), "child failed: {stdout}");
    assert!(stdout.contains("ROOTS=tls:no system trust anchors found\n"), "{stdout}");
    assert!(stdout.contains("CUSTOM=ok\n"), "{stdout}");
}
