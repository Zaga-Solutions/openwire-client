"""Pure-Python OpenWire client for ActiveMQ and Artemis: queues, topics, durable subscriptions, TLS.

    import asyncio
    from openwire_client import Envelope, connect

    async def main():
        client = await connect('tcp://localhost:61616', 'admin', 'admin')

        async def on_message(env):
            print(env.destination, env.text)

        await client.subscribe('queue://orders', None, on_message)
        await client.send(Envelope('queue://orders', 'hello'))
        await asyncio.sleep(1)
        await client.close()

    asyncio.run(main())
"""
from .connection import connect
from .envelope import (QUEUE, TEMP_QUEUE, TEMP_TOPIC, TOPIC, BrokerClient, BrokerError, Envelope, Handler,
                       canonical_destination, make_destination, split_destination)
from .openwire import OpenWireClient
from .tls import TlsOptions, build_ssl_context, explain_connect_error
from .url import Endpoint, parse_broker_url

__version__ = '0.1.1'

__all__ = [
    'BrokerClient', 'BrokerError', 'Endpoint', 'Envelope', 'Handler', 'OpenWireClient', 'QUEUE', 'TEMP_QUEUE',
    'TEMP_TOPIC', 'TOPIC', 'TlsOptions', 'build_ssl_context', 'canonical_destination', 'connect',
    'explain_connect_error', 'make_destination', 'parse_broker_url', 'split_destination',
]
