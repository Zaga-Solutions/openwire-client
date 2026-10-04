"""Broker URL parsing."""
from __future__ import annotations

import re
from typing import NamedTuple
from urllib.parse import parse_qs, urlsplit

DEFAULT_PORT = 61616
DEFAULT_TLS_PORT = 61617
TLS_SCHEMES = ('ssl', 'nio+ssl')
PLAIN_SCHEMES = ('tcp', 'nio')


class Endpoint(NamedTuple):
    host: str
    port: int
    tls: bool = False
    verify_hostname: bool = True


def parse_broker_url(url: str) -> list[Endpoint]:
    """'tcp://host:61616', 'ssl://host:61617' or 'failover:(tcp://a:61616,ssl://b:61617)?x=y' -> endpoints.

    tcp://, nio:// (plain) and ssl://, nio+ssl:// (TLS) are understood. Of the transport options after '?'
    only ActiveMQ's socket.verifyHostName (default true) is used; the rest is ignored. Raises ValueError for
    anything else."""
    url = (url or '').strip()
    match = re.fullmatch(r'failover:\(?([^)?]*)\)?(\?.*)?', url)
    parts = [part.strip() for part in match.group(1).split(',')] if match else [url]
    shared = parse_qs((match.group(2) or '')[1:]) if match else {}
    endpoints = []
    for part in parts:
        parsed = urlsplit(part)
        scheme = parsed.scheme.lower()
        if scheme not in PLAIN_SCHEMES + TLS_SCHEMES:
            raise ValueError(f'Unsupported broker URL {part!r}: use tcp://host:port, ssl://host:port '
                             f'or failover:(...) of those')
        if not parsed.hostname:
            raise ValueError(f'No host in broker URL {part!r}')
        tls = scheme in TLS_SCHEMES
        options = {**shared, **parse_qs(parsed.query)}
        verify = options.get('socket.verifyHostName', ['true'])[0].strip().lower() != 'false'
        endpoints.append(Endpoint(parsed.hostname, parsed.port or (DEFAULT_TLS_PORT if tls else DEFAULT_PORT),
                                  tls, verify))
    return endpoints
