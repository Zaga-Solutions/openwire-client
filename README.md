# openwire-client

A pure-Python [OpenWire](https://activemq.apache.org/components/classic/documentation/openwire) client for
ActiveMQ and Artemis. It speaks the broker's native JMS protocol (`tcp://host:61616`) with `asyncio`, **using
only the standard library**: no JVM, no JPype, no compiled dependencies, TLS included. It runs wherever Python
does, for example on Windows ARM64.

Supported: queues and topics, durable topic subscriptions, text messages with JMS headers and typed properties
(confirmed by the broker), receiving text, bytes and other message types, individual acknowledgements,
keep-alives, `failover:(...)` URLs and TLS with optional client certificates. Tested against ActiveMQ Classic
5.19 and Artemis.

Not supported: transactions, selectors, `ssl://` keystores (use PEM files), sending bytes/map/object messages.

## Install

```bash
pip install "openwire-client @ git+https://github.com/Zaga-Solutions/openwire-client@v0.1.1"
# no git available (e.g. a slim Docker build)? the same tag as an archive:
pip install https://github.com/Zaga-Solutions/openwire-client/archive/refs/tags/v0.1.1.tar.gz
```

Python 3.10 or later.

## Use

```python
import asyncio
from openwire_client import Envelope, connect

async def main():
    client = await connect('tcp://localhost:61616', 'admin', 'admin')

    async def on_message(env: Envelope):
        print(env.destination, env.jms_type, env.properties, env.text)

    await client.subscribe('queue://orders', None, on_message)          # consume a queue
    await client.subscribe('topic://prices', 'my-sub', on_message)      # durable: name + a client_id on connect
    await client.send(Envelope('queue://orders', '<order/>', jms_type='Order', properties={'tenant': 'a'}))
    await asyncio.sleep(1)
    await client.unsubscribe('topic://prices', 'my-sub')                 # also deletes the durable subscription
    await client.close()

asyncio.run(main())
```

* **Destinations** are strings: `queue://name`, `topic://name`, `temp-queue://…`, `temp-topic://…`. A bare name is a
  topic.
* **Handlers** are coroutines. A message is acknowledged after its handler returns; an exception in a handler
  is logged and the message is acknowledged anyway.
* **Durable subscriptions** need a `client_id` on `connect(...)` (the broker files the subscription under it)
  and a subscription name in `subscribe`. Messages sent while disconnected are delivered on the next
  `subscribe` with the same client id and name.
* **Received messages** are `Envelope`s: `text`, `message_type` (`'text'`, `'bytes'`, `'message'`), the exact
  `body` bytes of non-text messages, `message_id`, `jms_type`, `correlation_id`, `reply_to`, `priority`,
  `expiration`, `persistent`, `redelivered`, `timestamp` and `properties`. Artemis' own `__AMQ_*` properties are
  hidden unless you pass `hide_internal_properties=False` to `connect`.
* **Connection loss**: `client.closed` (an `asyncio.Event`) is set and `client.error` says why. Reconnecting is
  up to the application.
* **Timeouts**: `connect(..., timeout=10, keepalive=10)`. Every request waits for the broker's answer for
  `timeout` seconds.

## Broker URLs

`tcp://host:61616`, `nio://…`, `ssl://host:61617`, `nio+ssl://…` and
`failover:(tcp://a:61616,ssl://b:61617)`, where the first endpoint that connects is used. Of the options after
`?` only `socket.verifyHostName` (ActiveMQ's name, default `true`) is honoured; the rest is ignored.
`parse_broker_url(url)` returns the endpoints.

## TLS

Done by Python's `ssl` module: **no `cryptography` package, nothing to compile.** The broker's certificate and
host name are verified against the system's trusted CAs. Everything else is in `TlsOptions` (PEM files):

```python
from openwire_client import TlsOptions, connect

client = await connect('ssl://broker:61617', 'user', 'pw', tls=TlsOptions(
    ca_file='ca.pem',                 # extra CA to trust, e.g. a private one
    cert_file='client.pem',           # only for brokers that demand a client certificate
    key_file='client.key'))           # omit if the key is in cert_file
```

`TlsOptions.from_env(prefix='OPENWIRE_TLS_')` reads `…CA_FILE`, `…CERT_FILE`, `…KEY_FILE`, `…KEY_PASSWORD` and
`…INSECURE` from the environment, and connection errors then name those variables in their hints.
`insecure=True` switches certificate verification off (testing only). A host name mismatch can be allowed for
one broker with `ssl://host:61617?socket.verifyHostName=false`; the certificate chain is still verified.

Python 3.13+ rejects CA certificates without a "key usage" extension by default, which many private and older
`keytool`/`openssl` CAs lack and Java accepts. This library switches off just that strictness
(`VERIFY_X509_STRICT`); chain, validity and host name are still checked.

Java keystores need converting to PEM once:

```bash
# the CA (or the broker certificate) from a truststore
keytool -exportcert -rfc -alias <alias> -keystore truststore.jks -file ca.pem
# a client certificate and key from a keystore (one PEM file with both; use it as cert_file)
keytool -importkeystore -srckeystore client.jks -destkeystore client.p12 -deststoretype PKCS12
openssl pkcs12 -in client.p12 -nodes -out client.pem
```

Failures come with hints: an untrusted CA, a missing client certificate, `tcp://` pointed at a TLS port or
`ssl://` at a plain one.

## Tests

`python -m unittest discover -s tests`. `tests/test_wire.py` needs nothing. The broker tests skip themselves
unless a broker is reachable:

```bash
docker run -d --rm -p 61616:61616 -e ARTEMIS_USER=admin -e ARTEMIS_PASSWORD=admin apache/activemq-artemis:latest-alpine
OPENWIRE_TEST_BROKER=localhost:61616 OPENWIRE_TEST_USER=admin OPENWIRE_TEST_PASSWORD=admin \
  python -m unittest tests.test_broker
```

`tests/tls_broker.sh start` creates a throwaway CA and starts an Artemis broker with a TLS and a
client-certificate port (needs docker, openssl and keytool), then prints the variables for `tests/test_tls.py`.
CI runs the unit tests on several Python versions and the broker and TLS tests against Artemis and ActiveMQ
Classic containers.

## License

[MIT](LICENSE).
