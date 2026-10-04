#!/usr/bin/env bash
# Starts (or stops) a throwaway Artemis broker for the TLS tests, with a freshly generated test CA:
#   61616 plain OpenWire   61617 TLS   61618 TLS + client certificate required
# Needs docker, openssl and keytool (any JDK). Prints the environment variables the tests read.
#
#   tests/tls_broker.sh start [pki-dir]     (default ./pki)
#   tests/tls_broker.sh stop
set -euo pipefail
export MSYS2_ARG_CONV_EXCL='*'          # Git Bash: do not rewrite the container paths below
NAME=openwire-tls-test
IMAGE=apache/activemq-artemis:latest-alpine
PASS=changeit

case "${1:-start}" in
stop)
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  exit 0 ;;
start) ;;
*) echo "usage: $0 start [pki-dir] | stop" >&2; exit 2 ;;
esac

PKI="${2:-pki}"
mkdir -p "$PKI"
cd "$PKI"
HOST_PKI="$(pwd -W 2>/dev/null || pwd)"       # a path docker understands, also on Windows

# --- certificates: a CA, a server certificate for localhost / 127.0.0.1, a client certificate
openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.pem -days 30 -subj "/CN=Test CA" 2>/dev/null
openssl req -newkey rsa:2048 -nodes -keyout server.key -out server.csr -subj "/CN=localhost" 2>/dev/null
printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\n' > san.cnf
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial -out server.pem -days 30 -extfile san.cnf 2>/dev/null
openssl req -newkey rsa:2048 -nodes -keyout client.key -out client.csr -subj "/CN=test-client" 2>/dev/null
openssl x509 -req -in client.csr -CA ca.pem -CAkey ca.key -CAcreateserial -out client.pem -days 30 2>/dev/null
openssl pkcs12 -export -in server.pem -inkey server.key -certfile ca.pem -out server.p12 -passout "pass:$PASS" -name broker 2>/dev/null
rm -f trust.p12
keytool -importcert -noprompt -alias ca -file ca.pem -keystore trust.p12 -storetype PKCS12 -storepass "$PASS" >/dev/null 2>&1

# --- broker: take the image's default broker.xml and add the two TLS acceptors
docker rm -f "$NAME" >/dev/null 2>&1 || true
docker run -d --name "$NAME" -e ARTEMIS_USER=artemis -e ARTEMIS_PASSWORD=artemis "$IMAGE" >/dev/null
for _ in $(seq 1 60); do
  docker cp "$NAME:/var/lib/artemis-instance/etc/broker.xml" broker.orig.xml 2>/dev/null && break
  sleep 1
done
docker rm -f "$NAME" >/dev/null
COMMON="protocols=OPENWIRE;sslEnabled=true;keyStorePath=/certs/server.p12;keyStorePassword=$PASS;keyStoreType=PKCS12;trustStorePath=/certs/trust.p12;trustStorePassword=$PASS;trustStoreType=PKCS12"
sed "s#</acceptors>#<acceptor name=\"openwire-tls\">tcp://0.0.0.0:61617?$COMMON;needClientAuth=false</acceptor><acceptor name=\"openwire-mtls\">tcp://0.0.0.0:61618?$COMMON;needClientAuth=true</acceptor></acceptors>#" broker.orig.xml > broker.xml
docker run -d --name "$NAME" -p 61616:61616 -p 61617:61617 -p 61618:61618 \
  -e ARTEMIS_USER=artemis -e ARTEMIS_PASSWORD=artemis \
  -v "$HOST_PKI:/certs:ro" -v "$HOST_PKI/broker.xml:/var/lib/artemis-instance/etc-override/broker.xml:ro" "$IMAGE" >/dev/null

for _ in $(seq 1 90); do
  docker logs "$NAME" 2>&1 | grep -q "61618" && break
  sleep 1
done

cat <<EOF
Broker "$NAME" is up. Run the tests with:
export OPENWIRE_TEST_BROKER=localhost:61616 OPENWIRE_TEST_USER=artemis OPENWIRE_TEST_PASSWORD=artemis
export OPENWIRE_TEST_TLS_BROKER=localhost:61617 OPENWIRE_TEST_TLS_MTLS_BROKER=localhost:61618
export OPENWIRE_TEST_TLS_CA=$HOST_PKI/ca.pem OPENWIRE_TEST_TLS_CERT=$HOST_PKI/client.pem OPENWIRE_TEST_TLS_KEY=$HOST_PKI/client.key
python -m unittest discover -s tests
EOF
