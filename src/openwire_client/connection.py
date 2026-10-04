"""One call to get a connected client from a broker URL."""
from __future__ import annotations

from .envelope import BrokerError
from .openwire import OpenWireClient
from .tls import TlsOptions, build_ssl_context, explain_connect_error
from .url import parse_broker_url


async def connect(url: str, username: str | None = None, password: str | None = None,
                  client_id: str | None = None, *, tls: TlsOptions | None = None, timeout: float = 10.0,
                  keepalive: float = 10.0, hide_internal_properties: bool = True) -> OpenWireClient:
    """Connects to the first reachable endpoint of `url` (tcp://, ssl://, failover:(...)) and returns the client.

    Raises ValueError for an unusable URL and BrokerError (with every endpoint's reason) when none connects.
    `tls` only matters for ssl:// endpoints; the default verifies against the system's trusted CAs."""
    errors = []
    for endpoint in parse_broker_url(url):
        try:
            context = build_ssl_context(tls, endpoint.verify_hostname) if endpoint.tls else None
        except OSError as e:                               # e.g. a CA or certificate file that cannot be read
            errors.append(f'{endpoint.host}:{endpoint.port}: {e}')
            continue
        client = OpenWireClient(endpoint.host, endpoint.port, username or None, password or None,
                                client_id or None, timeout=timeout, keepalive=keepalive, ssl_context=context,
                                hide_internal_properties=hide_internal_properties)
        try:
            await client.connect()
        except BrokerError as e:
            errors.append(explain_connect_error(endpoint, str(e), tls))
            continue
        return client
    raise BrokerError('; '.join(errors) or 'could not connect')
