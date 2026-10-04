"""Encoding, message conversion, URL and TLS-option tests. No broker needed."""
import os
import shutil
import ssl
import struct
import subprocess
import tempfile
import unittest
from pathlib import Path

from openwire_client import (Endpoint, TlsOptions, build_ssl_context, canonical_destination, explain_connect_error,
                             make_destination, parse_broker_url, split_destination)
from openwire_client.openwire import (ACTIVEMQ_BYTES_MESSAGE, ACTIVEMQ_QUEUE, ACTIVEMQ_TEXT_MESSAGE, ACTIVEMQ_TOPIC,
                                      envelope_from_message, marshal_map, message_text, mutf8, unmarshal_map,
                                      unmutf8)


def wire_message(kind, content, **overrides):
    """A message as unmarshalled from a MessageDispatch (only the fields the client reads)."""
    m = {'type': kind, 'command_id': 0, 'response_required': False, 'producer_id': None,
         'destination': (ACTIVEMQ_QUEUE, 'orders'), 'transaction_id': None, 'original_destination': None,
         'message_id': (110, None, (123, 'ID:host-1-2-3:1', 1, 1), 7, 0), 'original_transaction_id': None,
         'group_id': None, 'group_seq': 0, 'correlation_id': 'corr-1', 'persistent': True, 'expiration': 0,
         'priority': 4, 'reply_to': None, 'timestamp': 1791136603015, 'jms_type': 'Order', 'content': content,
         'properties': marshal_map({'tenant': 'a', 'n': 3}), 'data_structure': None, 'target_consumer_id': None,
         'compressed': False, 'redelivery_counter': 0, 'user_id': None}
    m.update(overrides)
    return m


def text_content(text):
    body = mutf8(text)
    return struct.pack('>i', len(body)) + body


class EncodingTests(unittest.TestCase):
    def test_modified_utf8_round_trip(self):
        for s in ['order-1', 'åäö €', 'a\x00b', '𝄞']:
            self.assertEqual(unmutf8(mutf8(s)), s)
        self.assertEqual(mutf8('\x00'), b'\xc0\x80')

    def test_property_map_round_trip(self):
        props = {'s': 'text', 'i': 5, 'big': 2 ** 40, 'f': 1.5, 'b': True, 'raw': b'\x00\x01', 'none': None}
        self.assertEqual(unmarshal_map(marshal_map(props)), props)
        self.assertEqual(unmarshal_map(None), {})


class EnvelopeFromMessageTests(unittest.TestCase):
    def test_text_message_with_headers(self):
        m = wire_message(ACTIVEMQ_TEXT_MESSAGE, text_content('<order/>'), reply_to=(ACTIVEMQ_TOPIC, 'replies'),
                         group_id='g1', group_seq=2, redelivery_counter=1, priority=7, expiration=99)
        env = envelope_from_message(m, 'queue://orders')
        self.assertEqual(env.text, '<order/>')
        self.assertEqual(env.message_type, 'text')
        self.assertIsNone(env.body)
        self.assertEqual(env.destination, 'queue://orders')
        self.assertEqual(env.reply_to, 'replies')                    # a bare name is a topic
        self.assertEqual(env.message_id, 'ID:host-1-2-3:1:1:1:7')
        self.assertEqual((env.jms_type, env.correlation_id, env.group_id, env.group_seq), ('Order', 'corr-1', 'g1', 2))
        self.assertEqual((env.priority, env.expiration, env.redelivered, env.persistent), (7, 99, True, True))
        self.assertEqual(env.properties, {'tenant': 'a', 'n': 3})

    def test_bytes_message_keeps_exact_bytes(self):
        payload = bytes([0x00, 0xFF, 0x80, 0x7F, 0xFE])
        env = envelope_from_message(wire_message(ACTIVEMQ_BYTES_MESSAGE, payload), 'queue://orders')
        self.assertEqual(env.message_type, 'bytes')
        self.assertEqual(env.body, payload)

    def test_other_message_types_are_reported_as_such(self):
        env = envelope_from_message(wire_message(26, b'map'), 'queue://orders')
        self.assertEqual((env.message_type, env.body), ('message', b'map'))

    def test_compressed_content_is_inflated(self):
        import zlib
        body = text_content('compressed ' * 50)
        env = envelope_from_message(wire_message(ACTIVEMQ_TEXT_MESSAGE, zlib.compress(body), compressed=True),
                                    'queue://orders')
        self.assertEqual(env.text, 'compressed ' * 50)
        self.assertEqual(message_text(wire_message(ACTIVEMQ_TEXT_MESSAGE, b'')), '')

    def test_broker_internal_properties_can_be_hidden(self):
        props = marshal_map({'__AMQ_CID': 'sender', '_AMQ_x': 1, 'real': 'yes'})
        m = wire_message(ACTIVEMQ_TEXT_MESSAGE, text_content('x'), properties=props)
        self.assertEqual(set(envelope_from_message(m, 'q', hide_internal=False).properties),
                         {'__AMQ_CID', '_AMQ_x', 'real'})
        self.assertEqual(set(envelope_from_message(m, 'q', hide_internal=True).properties), {'real'})


class DestinationTests(unittest.TestCase):
    def test_split_and_make(self):
        self.assertEqual(split_destination('queue://orders'), ('queue', 'orders'))
        self.assertEqual(split_destination('/queue/orders'), ('queue', 'orders'))
        self.assertEqual(split_destination('prices'), ('topic', 'prices'))
        self.assertEqual(split_destination('temp-queue://t'), ('temp-queue', 't'))
        self.assertEqual(make_destination('topic', 'prices'), 'prices')
        self.assertEqual(canonical_destination('topic://prices'), 'prices')
        self.assertEqual(canonical_destination('/queue/orders'), 'queue://orders')


class ParseBrokerUrlTests(unittest.TestCase):
    def hosts(self, url):
        return [tuple(e) for e in parse_broker_url(url)]

    def test_tcp(self):
        self.assertEqual(self.hosts('tcp://localhost:61616'), [('localhost', 61616, False, True)])
        self.assertEqual(self.hosts('tcp://broker'), [('broker', 61616, False, True)])
        self.assertEqual(self.hosts(' nio://broker:5000?x=y '), [('broker', 5000, False, True)])

    def test_ssl(self):
        self.assertEqual(self.hosts('ssl://secure:61617'), [('secure', 61617, True, True)])
        self.assertEqual(self.hosts('SSL://secure'), [('secure', 61617, True, True)])
        self.assertEqual(self.hosts('nio+ssl://secure:1'), [('secure', 1, True, True)])

    def test_verify_host_name_option(self):
        self.assertEqual(self.hosts('ssl://h:1?socket.verifyHostName=false'), [('h', 1, True, False)])
        self.assertEqual(self.hosts('ssl://h:1?socket.verifyHostName=true&x=y'), [('h', 1, True, True)])
        self.assertEqual(self.hosts('failover:(ssl://a:1,ssl://b:2)?socket.verifyHostName=false'),
                         [('a', 1, True, False), ('b', 2, True, False)])

    def test_failover(self):
        self.assertEqual(self.hosts('failover:(tcp://a:1,ssl://b:2)?randomize=false'),
                         [('a', 1, False, True), ('b', 2, True, True)])
        self.assertEqual(self.hosts('failover:tcp://a:1'), [('a', 1, False, True)])

    def test_unsupported(self):
        for bad in ('', 'stomp://h:61613', 'localhost:61616', 'tcp://', 'https://h'):
            with self.assertRaises(ValueError, msg=bad):
                parse_broker_url(bad)


class TlsOptionsTests(unittest.TestCase):
    def test_defaults_verify_certificate_and_host_name(self):
        context = build_ssl_context()
        self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)
        self.assertTrue(context.check_hostname)

    def test_python_313_strictness_is_relaxed_for_java_style_private_cas(self):
        self.assertFalse(build_ssl_context().verify_flags & ssl.VERIFY_X509_STRICT)

    def test_host_name_check_can_be_switched_off_without_giving_up_verification(self):
        context = build_ssl_context(verify_hostname=False)
        self.assertFalse(context.check_hostname)
        self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)

    def test_insecure_disables_verification(self):
        context = build_ssl_context(TlsOptions(insecure=True))
        self.assertEqual(context.verify_mode, ssl.CERT_NONE)
        self.assertFalse(context.check_hostname)

    def test_missing_files_are_reported(self):
        missing = str(Path(tempfile.gettempdir()) / 'no-such-file.pem')
        with self.assertRaisesRegex(OSError, 'cannot read ca_file .*no-such-file.pem'):
            build_ssl_context(TlsOptions(ca_file=missing))
        with self.assertRaisesRegex(OSError, 'cannot read cert_file .*no-such-file.pem'):
            build_ssl_context(TlsOptions(cert_file=missing))
        with self.assertRaisesRegex(OSError, 'cannot read JMS_TLS_CA_FILE'):
            build_ssl_context(TlsOptions.from_env({'JMS_TLS_CA_FILE': missing}, prefix='JMS_TLS_'))

    def test_from_env(self):
        env = {'X_CA_FILE': 'ca.pem', 'X_CERT_FILE': 'c.pem', 'X_KEY_FILE': 'k.pem', 'X_KEY_PASSWORD': 'pw',
               'X_INSECURE': 'Yes'}
        options = TlsOptions.from_env(env, prefix='X_')
        self.assertEqual((options.ca_file, options.cert_file, options.key_file, options.key_password, options.insecure),
                         ('ca.pem', 'c.pem', 'k.pem', 'pw', True))
        empty = TlsOptions.from_env({}, prefix='X_')
        self.assertEqual((empty.ca_file, empty.insecure), (None, False))
        self.assertEqual(TlsOptions.from_env({'OPENWIRE_TLS_INSECURE': '1'}).insecure, True)

    @unittest.skipUnless(shutil.which('openssl'), 'needs the openssl command line tool to make a certificate')
    def test_ca_file_and_client_certificate_are_loaded(self):
        with tempfile.TemporaryDirectory() as tmp:
            pem, key = Path(tmp, 'cert.pem'), Path(tmp, 'key.pem')
            subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
                            '-subj', '/CN=test', '-keyout', str(key), '-out', str(pem)],
                           check=True, capture_output=True)
            before = len(build_ssl_context().get_ca_certs())
            context = build_ssl_context(TlsOptions(ca_file=str(pem), cert_file=str(pem), key_file=str(key)))
            self.assertEqual(len(context.get_ca_certs()), before + 1)
            combined = Path(tmp, 'both.pem')                         # the key can also live in the certificate file
            combined.write_text(pem.read_text() + key.read_text())
            build_ssl_context(TlsOptions(cert_file=str(combined)))


class ExplainConnectErrorTests(unittest.TestCase):
    tls, plain = Endpoint('h', 1, True), Endpoint('h', 1, False)

    def test_hints(self):
        self.assertIn('ca_file', explain_connect_error(self.tls, 'x: [SSL: CERTIFICATE_VERIFY_FAILED] ...'))
        self.assertIn('cert_file', explain_connect_error(self.tls, 'x: [SSL: SSLV3_ALERT_BAD_CERTIFICATE]'))
        self.assertIn('cert_file', explain_connect_error(self.tls, 'h:1: ConnectionAbortedError [WinError 10053]'))
        self.assertIn('TLS port', explain_connect_error(self.tls, 'h:1: TimeoutError'))
        self.assertIn('try ssl://', explain_connect_error(self.plain, 'h:1: IncompleteReadError 0 bytes'))

    def test_hints_name_the_environment_variables_when_options_came_from_there(self):
        options = TlsOptions.from_env({}, prefix='JMS_TLS_')
        self.assertIn('JMS_TLS_CA_FILE', explain_connect_error(self.tls, 'CERTIFICATE_VERIFY_FAILED', options))
        self.assertIn('JMS_TLS_CERT_FILE', explain_connect_error(self.tls, 'BAD_CERTIFICATE', options))

    def test_other_errors_are_left_alone(self):
        self.assertEqual(explain_connect_error(self.plain, 'h:1: ConnectionRefusedError'), 'h:1: ConnectionRefusedError')
        self.assertEqual(explain_connect_error(self.tls, 'h:1: ConnectionRefusedError'), 'h:1: ConnectionRefusedError')


if __name__ == '__main__':
    unittest.main()
