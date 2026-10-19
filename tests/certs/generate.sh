#!/bin/sh
# Test PKI for tests/tls.rs, the TLS unit tests and the Mosquitto TLS container test.
# Regenerate with `sh tests/certs/generate.sh` from the repository root; the output is
# committed so the tests need no openssl. Valid for 100 years, EC P-256 throughout.
set -eu
cd "$(dirname "$0")"

days=36500
key() { openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$1"; }

ca() {
    key "$1.key"
    openssl req -x509 -new -key "$1.key" -sha256 -days "$days" -subj "/CN=$2" \
        -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" \
        -out "$1.pem"
}

# leaf <name> <CN> <extendedKeyUsage> [subjectAltName]
leaf() {
    key "$1.key"
    openssl req -new -key "$1.key" -subj "/CN=$2" -out "$1.csr"
    {
        echo "basicConstraints=critical,CA:FALSE"
        echo "keyUsage=critical,digitalSignature"
        echo "extendedKeyUsage=$3"
        if [ -n "${4:-}" ]; then echo "subjectAltName=$4"; fi
    } > "$1.ext"
    openssl x509 -req -in "$1.csr" -CA ca.pem -CAkey ca.key -CAcreateserial -sha256 -days "$days" \
        -extfile "$1.ext" -out "$1.pem"
    rm "$1.csr" "$1.ext"
}

ca ca "mqtt-glide test CA"
ca other-ca "mqtt-glide other test CA"
leaf server "localhost" serverAuth "DNS:localhost,IP:127.0.0.1,DNS:broker.internal"
leaf client "client" clientAuth
leaf client2 "client2" clientAuth
# SEC1 ("EC PRIVATE KEY") copy of the client key, for the key format tests.
openssl pkey -in client.key -traditional -out client-sec1.key
rm -f ca.srl ca.key other-ca.key
# Mosquitto in the container test runs as an unprivileged user.
chmod 644 ./*.key
