"""Client behaviour against a real ActiveMQ or Artemis broker; skipped when none is reachable.

    OPENWIRE_TEST_BROKER=localhost:61616 OPENWIRE_TEST_USER=admin OPENWIRE_TEST_PASSWORD=admin \\
        python -m unittest tests.test_broker
"""
import asyncio
import os
import socket
import unittest
import uuid

from openwire_client import BrokerError, Envelope, connect

HOST, _, PORT = os.environ.get('OPENWIRE_TEST_BROKER', 'localhost:61616').partition(':')
PORT = int(PORT or 61616)
USER = os.environ.get('OPENWIRE_TEST_USER', 'admin')
PASSWORD = os.environ.get('OPENWIRE_TEST_PASSWORD', 'admin')
URL = f'tcp://{HOST}:{PORT}'


def broker_up() -> bool:
    try:
        socket.create_connection((HOST, PORT), 1).close()
        return True
    except OSError:
        return False


@unittest.skipUnless(broker_up(), f'no broker at {HOST}:{PORT}')
class BrokerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.clients = []
        self.received = asyncio.Queue()

    async def asyncTearDown(self):
        for client in self.clients:
            await client.close()

    async def open(self, client_id=None, **kwargs):
        client = await connect(URL, USER, PASSWORD, client_id, **kwargs)
        self.clients.append(client)
        return client

    async def handler(self, env):
        await self.received.put(env)

    async def next_message(self, timeout=5):
        return await asyncio.wait_for(self.received.get(), timeout)

    async def test_queue_and_topic_round_trip_with_headers(self):
        client = await self.open()
        name = uuid.uuid4().hex[:8]
        for destination in (f'queue://owtest.{name}', f'topic://owtest.{name}'):
            await client.subscribe(destination, None, self.handler)
            sent = Envelope(destination, '<order id="1"/>', jms_type='Order', correlation_id='c-1',
                            reply_to=f'queue://owtest.reply.{name}', group_id='g-1', group_seq=2,
                            properties={'tenant': 'a', 'count': 3, 'urgent': True})
            message_id = await client.send(sent)
            env = await self.next_message()
            self.assertEqual(env.text, '<order id="1"/>')
            self.assertEqual(env.message_id, message_id)
            self.assertEqual((env.jms_type, env.correlation_id, env.group_id), ('Order', 'c-1', 'g-1'))
            self.assertEqual(env.reply_to, f'queue://owtest.reply.{name}')
            self.assertEqual({k: env.properties[k] for k in ('tenant', 'count', 'urgent')},
                             {'tenant': 'a', 'count': 3, 'urgent': True})
            self.assertEqual((env.message_type, env.priority, env.persistent), ('text', 4, True))
            await client.unsubscribe(destination)

    async def test_unicode_and_large_bodies(self):
        client = await self.open()
        destination = f'queue://owtest.{uuid.uuid4().hex[:8]}'
        await client.subscribe(destination, None, self.handler)
        for text in ('Kaffe & Smörgås € 𝄞', 'x' * 200_000, ''):
            await client.send(Envelope(destination, text))
            self.assertEqual((await self.next_message()).text, text)

    async def test_unsubscribe_stops_delivery(self):
        client = await self.open()
        destination = f'queue://owtest.{uuid.uuid4().hex[:8]}'
        await client.subscribe(destination, None, self.handler)
        await client.unsubscribe(destination)
        await client.send(Envelope(destination, 'nobody listens'))
        with self.assertRaises(asyncio.TimeoutError):
            await self.next_message(timeout=1)

    async def test_durable_subscription_keeps_messages_while_disconnected(self):
        name = uuid.uuid4().hex[:8]
        destination, client_id, sub = f'topic://owtest.durable.{name}', f'owtest-{name}', f'{name}-sub'
        first = await self.open(client_id)
        await first.subscribe(destination, sub, self.handler)
        await first.close()

        sender = await self.open()
        await sender.send(Envelope(destination, 'sent while away'))

        again = await self.open(client_id)
        await again.subscribe(destination, sub, self.handler)
        self.assertEqual((await self.next_message()).text, 'sent while away')

        await again.unsubscribe(destination, sub)                  # also deletes it on the broker
        await again.subscribe(destination, sub, self.handler)      # ... so this starts from scratch
        await sender.send(Envelope(destination, 'after resubscribe'))
        self.assertEqual((await self.next_message()).text, 'after resubscribe')
        await again.unsubscribe(destination, sub)

    async def test_durable_subscription_names_with_dots(self):
        name = uuid.uuid4().hex[:8]
        client = await self.open(f'owtest-{name}')
        for sub in ('plain', 'with.dot', 'a.b.c-sub'):
            await client.subscribe(f'topic://owtest.{name}', f'{name}.{sub}', self.handler)
            await client.unsubscribe(f'topic://owtest.{name}', f'{name}.{sub}')

    async def test_lost_connection_sets_closed(self):
        client = await self.open()
        client._writer.transport.abort()                           # the network drops
        await asyncio.wait_for(client.closed.wait(), 5)
        self.assertTrue(client.error)
        with self.assertRaises(BrokerError):
            await client.send(Envelope('queue://owtest.x', 'too late'))

    async def test_close_is_clean(self):
        client = await self.open()
        await client.close()
        self.assertTrue(client.closed.is_set())

    async def test_failover_skips_unreachable_endpoints(self):
        client = await connect(f'failover:(tcp://127.0.0.1:1,{URL})', USER, PASSWORD)
        self.clients.append(client)
        self.assertFalse(client.closed.is_set())

    async def test_all_endpoints_down_reports_every_reason(self):
        with self.assertRaises(BrokerError) as raised:
            await connect('failover:(tcp://127.0.0.1:1,tcp://127.0.0.1:2)', timeout=2)
        self.assertIn('127.0.0.1:1', str(raised.exception))
        self.assertIn('127.0.0.1:2', str(raised.exception))

    async def test_wrong_credentials_are_rejected(self):
        try:
            anonymous = await connect(URL)
        except BrokerError:
            pass
        else:
            await anonymous.close()
            self.skipTest('this broker does not enforce authentication')
        with self.assertRaises(BrokerError):
            await connect(URL, USER, 'definitely-wrong')

    async def test_invalid_url(self):
        with self.assertRaises(ValueError):
            await connect('stomp://localhost:61613')


if __name__ == '__main__':
    unittest.main()
