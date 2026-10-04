"""The JMS message as seen by the application, independent of the wire protocol."""
from __future__ import annotations

import asyncio
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

# Destination kinds. A destination is a string: a bare name is a topic, other kinds carry ActiveMQ's own
# prefix, e.g. 'queue://orders'.
TOPIC, QUEUE, TEMP_TOPIC, TEMP_QUEUE = 'topic', 'queue', 'temp-topic', 'temp-queue'
KINDS = (TOPIC, QUEUE, TEMP_TOPIC, TEMP_QUEUE)


def split_destination(dest: str) -> tuple[str, str]:
    """'queue://orders' -> ('queue', 'orders'), 'prices' -> ('topic', 'prices').

    Also accepts the STOMP spellings '/queue/X', '/topic/X', '/temp-queue/X', '/temp-topic/X'."""
    for kind in KINDS:
        for prefix in (f'{kind}://', f'/{kind}/'):
            if dest.startswith(prefix):
                return kind, dest[len(prefix):]
    return TOPIC, dest


def make_destination(kind: str, name: str) -> str:
    """The canonical spelling: topics bare, everything else '<kind>://<name>'."""
    return name if kind == TOPIC else f'{kind}://{name}'


def canonical_destination(dest: str) -> str:
    return make_destination(*split_destination(dest))


@dataclass
class Envelope:
    destination: str                     # topic name ('prices') or prefixed destination ('queue://orders')
    text: str
    jms_type: str | None = None          # JMSType
    correlation_id: str | None = None    # JMSCorrelationID
    reply_to: str | None = None          # JMSReplyTo, spelled like destination
    message_id: str | None = None        # JMSMessageID (set by the provider when sending)
    group_id: str | None = None          # JMSXGroupID
    group_seq: int = 0                   # JMSXGroupSeq
    persistent: bool = True              # JMSDeliveryMode
    timestamp: float = field(default_factory=time.time)
    properties: dict[str, Any] = field(default_factory=dict)
    redelivered: bool = False
    message_type: str = 'text'           # 'text' (TextMessage), 'bytes' (BytesMessage) or 'message' (anything else)
    body: bytes | None = None            # raw payload of a received bytes/other message (text is also in .text)
    priority: int = 4                    # JMSPriority
    expiration: int = 0                  # JMSExpiration (epoch ms, 0 = never)

    def prop(self, name: str, default: Any = None) -> Any:
        return self.properties.get(name, default)


Handler = Callable[[Envelope], Awaitable[None]]


class BrokerError(Exception):
    """Connection or protocol failure."""


class BrokerClient(ABC):
    """One connection to a JMS broker. OpenWireClient is the implementation in this package."""

    def __init__(self) -> None:
        self.closed = asyncio.Event()
        self.error = ''
        self.server_info = ''

    @abstractmethod
    async def connect(self) -> None:
        ...

    @abstractmethod
    async def subscribe(self, destination: str, durable_name: str | None, handler: Handler) -> None:
        """Consume a topic or queue; the message is acknowledged after the handler returns (no auto-ack).

        durable_name only applies to topics; a queue keeps its messages for the next consumer anyway."""

    @abstractmethod
    async def send(self, env: Envelope) -> str:
        """Send and wait for the broker's confirmation; returns the JMSMessageID if known."""

    @abstractmethod
    async def close(self) -> None:
        ...

    def _lost(self, reason: str) -> None:
        if not self.closed.is_set():
            self.error = reason
            self.closed.set()
