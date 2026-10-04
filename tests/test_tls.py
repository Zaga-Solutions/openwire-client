"""TLS connections to a real TLS-enabled broker; skipped without one. tests/tls_broker.sh starts a suitable
Artemis broker (with a throwaway CA) and prints the variables to set:

    OPENWIRE_TEST_TLS_BROKER=localhost:61617 OPENWIRE_TEST_TLS_CA=pki/ca.pem OPENWIRE_TEST_USER=artemis \\
    OPENWIRE_TEST_PASSWORD=artemis python -m unittest tests.test_tls

Optionally OPENWIRE_TEST_TLS_MTLS_BROKER=localhost:61618 with OPENWIRE_TEST_TLS_CERT / _KEY for the port that
demands a client certificate.
"""
import asyncio
import os
import socket
import unittest

from openwire_client import BrokerError, Envelope, TlsOptions, connect

USER = os.environ.get('OPENWIRE_TEST_USER', 'admin')
PASSWORD = os.environ.get('OPENWIRE_TEST_PASSWORD', 'admin')
TLS_BROKER = os.environ.get('OPENWIRE_TEST_TLS_BROKER')
MTLS_BROKER = os.environ.get('OPENWIRE_TEST_TLS_MTLS_BROKER')
CA = os.environ.get('OPENWIRE_TEST_TLS_CA')
CERT, KEY = os.environ.get('OPENWIRE_TEST_TLS_CERT'), os.environ.get('OPENWIRE_TEST_TLS_KEY')


def reachable(spec):
    host, _, port = (spec or '').partition(':')
    try:
        socket.create_connection((host, int(port)), 1).close()
        return True
    except (OSError, ValueError):
        return False


@unittest.skipUnless(reachable(TLS_BROKER) and CA, 'no TLS broker (see the module docstring)')
class TlsBrokerTests(unittest.IsolatedAsyncioTestCase):
    url = f'ssl://{TLS_BROKER}'

    async def asyncSetUp(self):
        self.clients = []

    async def asyncTearDown(self):
        for client in self.clients:
            await client.close()

    async def open(self, url=None, tls=None):
        client = await connect(url or self.url, USER, PASSWORD, tls=tls)
        self.clients.append(client)
        return client

    async def test_round_trip_when_the_ca_is_trusted(self):
        client = await self.open(tls=TlsOptions(ca_file=CA))
        got = asyncio.Queue()
        await client.subscribe('queue://owtest.tls', None, got.put)
        await client.send(Envelope('queue://owtest.tls', 'over tls'))
        self.assertEqual((await asyncio.wait_for(got.get(), 5)).text, 'over tls')

    async def test_untrusted_certificate_is_rejected_with_a_hint(self):
        with self.assertRaisesRegex(BrokerError, 'CERTIFICATE_VERIFY_FAILED.*ca_file'):
            await self.open(tls=TlsOptions())

    async def test_hint_names_environment_variables_when_configured_from_them(self):
        with self.assertRaisesRegex(BrokerError, 'JMS_TLS_CA_FILE'):
            await self.open(tls=TlsOptions.from_env({}, prefix='JMS_TLS_'))

    async def test_insecure_mode_skips_verification(self):
        client = await self.open(tls=TlsOptions(insecure=True))
        self.assertFalse(client.closed.is_set())

    async def test_host_name_mismatch_needs_the_option(self):
        host, _, port = TLS_BROKER.partition(':')
        if host not in ('localhost', '127.0.0.1'):
            self.skipTest('needs a broker on this machine')
        # ::1 is not in the test certificate; skip when the broker does not listen on IPv6
        try:
            socket.create_connection(('::1', int(port)), 1).close()
        except OSError:
            self.skipTest('broker not reachable over IPv6')
        with self.assertRaisesRegex(BrokerError, 'mismatch'):
            await self.open(f'ssl://[::1]:{port}', TlsOptions(ca_file=CA))
        client = await self.open(f'ssl://[::1]:{port}?socket.verifyHostName=false', TlsOptions(ca_file=CA))
        self.assertFalse(client.closed.is_set())

    async def test_plain_tcp_to_a_tls_port_explains_itself(self):
        with self.assertRaisesRegex(BrokerError, 'try ssl://'):
            await self.open(f'tcp://{TLS_BROKER}')


@unittest.skipUnless(reachable(MTLS_BROKER) and CA and CERT, 'no mutual-TLS broker (see the module docstring)')
class MutualTlsTests(unittest.IsolatedAsyncioTestCase):
    url = f'ssl://{MTLS_BROKER}'

    async def test_client_certificate_is_required_and_used(self):
        with self.assertRaisesRegex(BrokerError, 'cert_file'):
            await connect(self.url, USER, PASSWORD, tls=TlsOptions(ca_file=CA))
        client = await connect(self.url, USER, PASSWORD, tls=TlsOptions(ca_file=CA, cert_file=CERT, key_file=KEY))
        self.assertFalse(client.closed.is_set())
        await client.close()


if __name__ == '__main__':
    unittest.main()
