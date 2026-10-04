"""Minimal OpenWire (ActiveMQ's native JMS protocol, tcp://host:61616) client in pure Python.

Implements the subset a JMS client needs: wire format negotiation, connection/session/producer/consumer
registration, queues and (durable) topic subscriptions, sending text messages with JMS headers and properties
(synchronously confirmed by the broker), receiving text, bytes and other messages, individual
acknowledgements, keep-alives and TLS (pass an ssl.SSLContext).

The encoding follows the generated v10-v12 marshallers of ActiveMQ 5.19 (org.apache.activemq.openwire
.v12.*Marshaller.looseMarshal). We announce loose encoding and no marshalling cache, which the broker
accepts (the negotiated format is the minimum of both sides), so every frame is:
    int32 size | byte type | fields in declaration order
"""
from __future__ import annotations

import asyncio
import itertools
import logging
import os
import socket
import ssl
import struct
import time
import zlib
from typing import Any

from .envelope import (QUEUE, TEMP_QUEUE, TEMP_TOPIC, TOPIC, BrokerClient, BrokerError, Envelope, Handler,
                       canonical_destination, make_destination, split_destination)

log = logging.getLogger(__name__)

WIRE_VERSION = 12

# CommandTypes
WIREFORMAT_INFO, BROKER_INFO, CONNECTION_INFO, SESSION_INFO, CONSUMER_INFO, PRODUCER_INFO = 1, 2, 3, 4, 5, 6
REMOVE_SUBSCRIPTION_INFO, KEEP_ALIVE_INFO, SHUTDOWN_INFO, REMOVE_INFO = 9, 10, 11, 12
CONNECTION_ERROR, CONSUMER_CONTROL, CONNECTION_CONTROL, PRODUCER_ACK = 16, 17, 18, 19
MESSAGE_DISPATCH, MESSAGE_ACK = 21, 22
ACTIVEMQ_MESSAGE, ACTIVEMQ_BYTES_MESSAGE, ACTIVEMQ_TEXT_MESSAGE = 23, 24, 28
RESPONSE, EXCEPTION_RESPONSE = 30, 31
ACTIVEMQ_QUEUE, ACTIVEMQ_TOPIC, ACTIVEMQ_TEMP_QUEUE, ACTIVEMQ_TEMP_TOPIC = 100, 101, 102, 103
MESSAGE_ID, LOCAL_TRANSACTION_ID, XA_TRANSACTION_ID = 110, 111, 112
CONNECTION_ID, SESSION_ID, CONSUMER_ID, PRODUCER_ID, BROKER_ID = 120, 121, 122, 123, 124

INDIVIDUAL_ACK_TYPE = 4


# ------------------------------------------------------------------------------------ java data streams

def mutf8(s: str) -> bytes:
    """Java 'modified UTF-8' (DataOutput.writeUTF / ActiveMQ writeUTF8)."""
    if s.isascii() and '\x00' not in s:
        return s.encode('ascii')
    out = bytearray()
    units = s.encode('utf-16-le', 'surrogatepass')
    for i in range(0, len(units), 2):
        c = units[i] | units[i + 1] << 8
        if 0 < c < 0x80:
            out.append(c)
        elif c < 0x800:
            out += bytes((0xC0 | c >> 6, 0x80 | c & 0x3F))
        else:
            out += bytes((0xE0 | c >> 12, 0x80 | c >> 6 & 0x3F, 0x80 | c & 0x3F))
    return bytes(out)


def unmutf8(b: bytes) -> str:
    try:
        return b.decode('ascii')
    except UnicodeDecodeError:
        pass
    units, i = [], 0
    while i < len(b):
        c = b[i]
        if c < 0x80:
            units.append(c)
            i += 1
        elif c >> 5 == 0b110:
            units.append((c & 0x1F) << 6 | b[i + 1] & 0x3F)
            i += 2
        else:
            units.append((c & 0x0F) << 12 | (b[i + 1] & 0x3F) << 6 | b[i + 2] & 0x3F)
            i += 3
    return struct.pack(f'<{len(units)}H', *units).decode('utf-16-le', 'surrogatepass')


class Out:
    def __init__(self) -> None:
        self.b = bytearray()

    def byte(self, v: int) -> 'Out':
        self.b += struct.pack('>b', v)
        return self

    def bool(self, v: bool) -> 'Out':
        self.b.append(1 if v else 0)
        return self

    def short(self, v: int) -> 'Out':
        self.b += struct.pack('>h', v)
        return self

    def int(self, v: int) -> 'Out':
        self.b += struct.pack('>i', v)
        return self

    def long(self, v: int) -> 'Out':
        self.b += struct.pack('>q', v)
        return self

    def utf(self, s: str) -> 'Out':
        data = mutf8(s)
        self.b += struct.pack('>H', len(data)) + data
        return self

    def string(self, s: str | None) -> 'Out':
        self.bool(s is not None)
        if s is not None:
            self.utf(s)
        return self

    def byteseq(self, data: bytes | None) -> 'Out':
        self.bool(data is not None)
        if data is not None:
            self.int(len(data))
            self.b += data
        return self

    def raw(self, data: bytes) -> 'Out':
        self.b += data
        return self


class In:
    def __init__(self, data: bytes) -> None:
        self.d = memoryview(data)
        self.p = 0

    def take(self, n: int) -> bytes:
        if self.p + n > len(self.d):
            raise BrokerError('truncated OpenWire frame')
        v = bytes(self.d[self.p:self.p + n])
        self.p += n
        return v

    def byte(self) -> int:
        return struct.unpack('>b', self.take(1))[0]

    def ubyte(self) -> int:
        return self.take(1)[0]

    def bool(self) -> bool:
        return self.take(1)[0] != 0

    def short(self) -> int:
        return struct.unpack('>h', self.take(2))[0]

    def int(self) -> int:
        return struct.unpack('>i', self.take(4))[0]

    def long(self) -> int:
        return struct.unpack('>q', self.take(8))[0]

    def double(self) -> float:
        return struct.unpack('>d', self.take(8))[0]

    def float(self) -> float:
        return struct.unpack('>f', self.take(4))[0]

    def utf(self) -> str:
        n = struct.unpack('>H', self.take(2))[0]
        return unmutf8(self.take(n))

    def string(self) -> str | None:
        return self.utf() if self.bool() else None

    def byteseq(self) -> bytes | None:
        return self.take(self.int()) if self.bool() else None


# ------------------------------------------------------------------------------------ primitive maps

def marshal_map(props: dict[str, Any]) -> bytes:
    o = Out().int(len(props))
    for k, v in props.items():
        o.utf(k)
        if v is None:
            o.byte(0)
        elif isinstance(v, bool):
            o.byte(1).bool(v)
        elif isinstance(v, int):
            if -2 ** 31 <= v < 2 ** 31:
                o.byte(5).int(v)
            else:
                o.byte(6).long(v)
        elif isinstance(v, float):
            o.byte(7).raw(struct.pack('>d', v))
        elif isinstance(v, bytes):
            o.byte(10).int(len(v)).raw(v)
        else:
            s = str(v)
            if len(s) < 32767 // 4:
                o.byte(9).utf(s)
            else:
                data = mutf8(s)
                o.byte(13).int(len(data)).raw(data)
    return bytes(o.b)


def _primitive(i: In) -> Any:
    t = i.ubyte()
    if t == 0:
        return None
    if t == 1:
        return i.bool()
    if t == 2:
        return i.byte()
    if t == 3:
        return chr(struct.unpack('>H', i.take(2))[0])
    if t == 4:
        return i.short()
    if t == 5:
        return i.int()
    if t == 6:
        return i.long()
    if t == 7:
        return i.double()
    if t == 8:
        return i.float()
    if t == 9:
        return i.utf()
    if t == 10:
        return i.take(i.int())
    if t == 11:
        return _map(i)
    if t == 12:
        return [_primitive(i) for _ in range(i.int())]
    if t == 13:
        n = i.int()
        return unmutf8(i.take(n)) if n >= 0 else None
    raise BrokerError(f'unknown primitive type {t}')


def _map(i: In) -> dict[str, Any]:
    n = i.int()
    return {} if n < 0 else {i.utf(): _primitive(i) for _ in range(n)}


def unmarshal_map(data: bytes | None) -> dict[str, Any]:
    return _map(In(data)) if data else {}


# ------------------------------------------------------------------------------------ data structures

class Id(tuple):
    """ConnectionId/SessionId/ConsumerId/ProducerId as (type, fields...)."""


def w_nested(o: Out, obj: tuple | None, version: int) -> None:
    """looseMarshalNestedObject / looseMarshalCachedObject without cache."""
    o.bool(obj is not None)
    if obj is None:
        return
    t = obj[0]
    o.byte(t)
    if t == CONNECTION_ID:
        o.string(obj[1])
    elif t == SESSION_ID:
        o.string(obj[1]).long(obj[2])
    elif t == CONSUMER_ID:
        o.string(obj[1]).long(obj[2]).long(obj[3])
    elif t == PRODUCER_ID:  # connectionId, value, sessionId
        o.string(obj[1]).long(obj[3]).long(obj[2])
    elif t in (ACTIVEMQ_TOPIC, ACTIVEMQ_QUEUE, ACTIVEMQ_TEMP_TOPIC, ACTIVEMQ_TEMP_QUEUE):
        o.string(obj[1])
    elif t == MESSAGE_ID:  # textView, producerId, producerSequenceId, brokerSequenceId
        o.string(obj[1])
        w_nested(o, obj[2], version)
        o.long(obj[3]).long(obj[4])
    elif t == BROKER_ID:
        o.string(obj[1])
    else:
        raise BrokerError(f'cannot marshal type {t}')


def r_throwable(i: In, stack_traces: bool) -> str | None:
    if not i.bool():
        return None
    clazz, message = i.string(), i.string()
    if stack_traces:
        for _ in range(i.short()):
            i.string(), i.string(), i.string(), i.int()
        cause = r_throwable(i, stack_traces)
        if cause:
            message = f'{message} (caused by {cause})'
    return f'{clazz}: {message}'


def r_array(i: In, version: int) -> list | None:
    if not i.bool():
        return None
    return [r_nested(i, version) for _ in range(i.short())]


def r_nested(i: In, version: int) -> Any:
    if not i.bool():
        return None
    t = i.ubyte()
    if t == CONNECTION_ID:
        return (t, i.string())
    if t == SESSION_ID:
        return (t, i.string(), i.long())
    if t == CONSUMER_ID:
        return (t, i.string(), i.long(), i.long())
    if t == PRODUCER_ID:
        conn, value, session = i.string(), i.long(), i.long()
        return (t, conn, session, value)
    if t in (ACTIVEMQ_TOPIC, ACTIVEMQ_QUEUE, ACTIVEMQ_TEMP_TOPIC, ACTIVEMQ_TEMP_QUEUE):
        return (t, i.string())
    if t == MESSAGE_ID:
        return (t, i.string(), r_nested(i, version), i.long(), i.long())
    if t == BROKER_ID:
        return (t, i.string())
    if t == LOCAL_TRANSACTION_ID:
        return (t, i.long(), r_nested(i, version))
    if t == XA_TRANSACTION_ID:
        return (t, i.int(), i.byteseq(), i.byteseq())
    if t in (ACTIVEMQ_MESSAGE, ACTIVEMQ_BYTES_MESSAGE, ACTIVEMQ_TEXT_MESSAGE) or 23 <= t <= 29:
        return r_message(t, i, version)
    raise BrokerError(f'cannot unmarshal nested type {t}')


def message_id_str(mid: Any) -> str | None:
    if not mid:
        return None
    _, text_view, pid, seq, _ = mid
    if text_view:
        return text_view if text_view.startswith('ID:') else 'ID:' + text_view
    _, conn, session, value = pid
    return f'{conn}:{session}:{value}:{seq}'


_WIRE_KIND = {TOPIC: ACTIVEMQ_TOPIC, QUEUE: ACTIVEMQ_QUEUE, TEMP_TOPIC: ACTIVEMQ_TEMP_TOPIC,
              TEMP_QUEUE: ACTIVEMQ_TEMP_QUEUE}
_KIND_OF_WIRE = {v: k for k, v in _WIRE_KIND.items()}
# property names Artemis reserves for itself and adds to messages (e.g. __AMQ_CID, the sender's connection)
_BROKER_INTERNAL = ('_AMQ_', '__AMQ_')


def wire_dest(dest: str) -> tuple[int, str]:
    """'queue://orders' -> (ACTIVEMQ_QUEUE, 'orders'); a bare name is a topic."""
    kind, name = split_destination(dest)
    return _WIRE_KIND[kind], name


def dest_name(d: Any) -> str | None:
    if not d:
        return None
    kind, name = _KIND_OF_WIRE[d[0]], d[1]
    inner_kind, inner_name = split_destination(name)
    if inner_name != name:
        # Artemis passes a STOMP client's 'reply-to: /queue/X' on as a queue literally named '/queue/X'
        kind, name = inner_kind, inner_name
    return make_destination(kind, name)


def r_message(t: int, i: In, version: int) -> dict[str, Any]:
    m: dict[str, Any] = {'type': t, 'command_id': i.int(), 'response_required': i.bool()}
    m['producer_id'] = r_nested(i, version)
    m['destination'] = r_nested(i, version)
    m['transaction_id'] = r_nested(i, version)
    m['original_destination'] = r_nested(i, version)
    m['message_id'] = r_nested(i, version)
    m['original_transaction_id'] = r_nested(i, version)
    m['group_id'] = i.string()
    m['group_seq'] = i.int()
    m['correlation_id'] = i.string()
    m['persistent'] = i.bool()
    m['expiration'] = i.long()
    m['priority'] = i.byte()
    m['reply_to'] = r_nested(i, version)
    m['timestamp'] = i.long()
    m['jms_type'] = i.string()
    m['content'] = i.byteseq()
    m['properties'] = i.byteseq()
    m['data_structure'] = r_nested(i, version)
    m['target_consumer_id'] = r_nested(i, version)
    m['compressed'] = i.bool()
    m['redelivery_counter'] = i.int()
    r_array(i, version)          # brokerPath
    i.long()                     # arrival
    m['user_id'] = i.string()
    i.bool()                     # recievedByDFBridge
    i.bool()                     # droppable
    r_array(i, version)          # cluster
    i.long()                     # brokerInTime
    i.long()                     # brokerOutTime
    if version >= 10:
        i.bool()                 # JMSXGroupFirstForConsumer
    return m


def message_text(m: dict[str, Any]) -> str:
    content = m['content'] or b''
    if m['compressed']:
        content = zlib.decompress(content)
    if m['type'] == ACTIVEMQ_TEXT_MESSAGE:
        if not content:
            return ''
        n = struct.unpack('>i', content[:4])[0]
        return '' if n < 0 else unmutf8(content[4:4 + n])
    # BytesMessage / plain Message: a latin-1 reading that never fails; Envelope.body has the exact bytes
    return content.decode('latin-1')


def message_body(m: dict[str, Any]) -> bytes:
    """The raw payload of a message (decompressed). For a TextMessage this is the length-prefixed text."""
    content = m['content'] or b''
    return zlib.decompress(content) if m['compressed'] else content


_KIND_OF_MESSAGE = {ACTIVEMQ_TEXT_MESSAGE: 'text', ACTIVEMQ_BYTES_MESSAGE: 'bytes'}


def envelope_from_message(m: dict[str, Any], subscribed: str, hide_internal: bool = True) -> Envelope:
    """Turn an unmarshalled MessageDispatch message into an Envelope."""
    kind = _KIND_OF_MESSAGE.get(m['type'], 'message')
    return Envelope(
        destination=dest_name(m['destination']) or subscribed,
        text=message_text(m),
        jms_type=m['jms_type'], correlation_id=m['correlation_id'], reply_to=dest_name(m['reply_to']),
        message_id=message_id_str(m['message_id']), group_id=m['group_id'], group_seq=m['group_seq'],
        persistent=m['persistent'], timestamp=m['timestamp'] / 1000 if m['timestamp'] else time.time(),
        properties={k: v for k, v in unmarshal_map(m['properties']).items()
                    if not (hide_internal and k.startswith(_BROKER_INTERNAL))},
        redelivered=m['redelivery_counter'] > 0,
        message_type=kind, body=None if kind == 'text' else message_body(m),
        priority=m['priority'], expiration=m['expiration'])


# ------------------------------------------------------------------------------------ client

class OpenWireClient(BrokerClient):
    def __init__(self, host: str, port: int, username: str | None = None, password: str | None = None,
                 client_id: str | None = None, timeout: float = 10.0, keepalive: float = 10.0,
                 ssl_context: ssl.SSLContext | None = None, hide_internal_properties: bool = True) -> None:
        super().__init__()
        self.host, self.port = host, port
        self.ssl_context = ssl_context                 # TLS (ssl://) when given; the stdlib ssl module is all it takes
        self.username, self.password = username or None, password or None
        self.client_id = client_id or None
        self.timeout = timeout
        self.keepalive = keepalive
        self.version = WIRE_VERSION
        self.stack_traces = False
        self.hide_internal_properties = hide_internal_properties   # drop Artemis' __AMQ_* properties from received messages
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._cmd = itertools.count(1)
        self._pending: dict[int, asyncio.Future] = {}
        self._consumers: dict[tuple, tuple[str, Handler]] = {}
        self._consumer_seq = itertools.count(1)
        self._producer_seq = itertools.count(1)
        self._tasks: list[asyncio.Task] = []
        self._write_lock = asyncio.Lock()
        self._last_read = time.monotonic()
        self._dispatch_queue: asyncio.Queue = asyncio.Queue()
        stamp = f'{socket.gethostname()}-{os.getpid()}-{int(time.time() * 1000)}-{next(_conn_counter)}'
        self.connection_id = f'ID:{stamp}:1'
        if not self.client_id:  # the broker demands one; the Java client generates it the same way
            self.client_id = f'ID:{stamp}:0'
        self.session = (SESSION_ID, self.connection_id, 1)
        self.producer = (PRODUCER_ID, self.connection_id, 1, 1)

    # ---------------------------------------------------------------- framing

    async def _write(self, payload: bytes) -> None:
        if self._writer is None or self.closed.is_set():
            raise BrokerError(self.error or 'not connected')
        async with self._write_lock:
            self._writer.write(struct.pack('>i', len(payload)) + payload)
            await self._writer.drain()

    def _command(self, t: int, response_required: bool) -> tuple[Out, int]:
        cid = next(self._cmd)
        return Out().byte(t).int(cid).bool(response_required), cid

    async def _request(self, o: Out, cid: int) -> None:
        fut = asyncio.get_running_loop().create_future()
        self._pending[cid] = fut
        try:
            await self._write(bytes(o.b))
            await asyncio.wait_for(fut, self.timeout)
        except asyncio.TimeoutError:
            raise BrokerError('no response from broker') from None
        finally:
            self._pending.pop(cid, None)

    async def _read_frame(self) -> bytes:
        assert self._reader is not None
        size = struct.unpack('>i', await self._reader.readexactly(4))[0]
        data = await self._reader.readexactly(size)
        self._last_read = time.monotonic()
        return data

    # ---------------------------------------------------------------- connection

    async def connect(self) -> None:
        try:
            # a new lookup on every attempt, nothing is cached between attempts
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port, ssl=self.ssl_context,
                                        server_hostname=self.host if self.ssl_context else None), self.timeout)
            sock = self._writer.get_extra_info('socket')
            if sock is not None:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            props = {'TightEncodingEnabled': False, 'CacheEnabled': False, 'SizePrefixDisabled': False,
                     'TcpNoDelayEnabled': True, 'StackTraceEnabled': False, 'CacheSize': 0,
                     'MaxInactivityDuration': _Long(30000), 'MaxInactivityDurationInitalDelay': _Long(10000),
                     'MaxFrameSize': _Long(104857600), 'ProviderName': 'openwire-client', 'PlatformDetails': 'Python'}
            wf =Out().byte(WIREFORMAT_INFO).raw(b'ActiveMQ').int(WIRE_VERSION).byteseq(_marshal_wf(props))
            self._writer.write(struct.pack('>i', len(wf.b)) + bytes(wf.b))
            await self._writer.drain()
            frame = await asyncio.wait_for(self._read_frame(), self.timeout)
            if frame[0] != WIREFORMAT_INFO:
                raise BrokerError(f'expected WireFormatInfo, got type {frame[0]}')
            i = In(frame[1:])
            if i.take(8) != b'ActiveMQ':
                raise BrokerError('not an ActiveMQ OpenWire endpoint')
            broker_version = i.int()
            broker_props = unmarshal_map(i.byteseq())
            self.version = min(WIRE_VERSION, broker_version)
            if self.version < 10:
                raise BrokerError(f'broker speaks OpenWire v{broker_version}, need 10 or later')
            self.server_info = f"{broker_props.get('ProviderName', 'ActiveMQ')} {broker_props.get('ProviderVersion', '')}" \
                               f' OpenWire v{self.version}'.strip()
            self._tasks = [asyncio.create_task(self._read_loop()), asyncio.create_task(self._dispatch_loop()),
                           asyncio.create_task(self._keepalive_loop())]
            # ConnectionInfo
            o, cid = self._command(CONNECTION_INFO, True)
            w_nested(o, (CONNECTION_ID, self.connection_id), self.version)
            o.string(self.client_id).string(self.password).string(self.username)
            o.bool(False)                                   # brokerPath
            o.bool(False).bool(False).bool(True).bool(False).bool(False)  # master connector, manageable, client master, FT, failover
            o.string(None)                                  # clientIp
            await self._request(o, cid)
            o, cid = self._command(SESSION_INFO, True)
            w_nested(o, self.session, self.version)
            await self._request(o, cid)
            o, cid = self._command(PRODUCER_INFO, True)
            w_nested(o, self.producer, self.version)
            w_nested(o, None, self.version)                 # anonymous producer
            o.bool(False).bool(False).int(0)                # brokerPath, dispatchAsync, windowSize
            await self._request(o, cid)
        except BrokerError:
            await self.close()
            raise
        except (OSError, asyncio.TimeoutError, asyncio.IncompleteReadError) as e:
            await self.close()
            raise BrokerError(f'{self.host}:{self.port}: {e.__class__.__name__} {e}'.strip()) from None

    async def close(self) -> None:
        if self._writer is not None and not self.closed.is_set():
            try:
                o, _ = self._command(SHUTDOWN_INFO, False)
                await asyncio.wait_for(self._write(bytes(o.b)), 2)
            except Exception:
                pass
        self._lost(self.error or 'closed')
        for t in self._tasks:
            if t is not asyncio.current_task():
                t.cancel()
        if self._writer is not None:
            self._writer.close()
            try:
                await asyncio.wait_for(self._writer.wait_closed(), 2)
            except Exception:
                pass
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(BrokerError(self.error or 'closed'))

    def _lost(self, reason: str) -> None:
        super()._lost(reason)
        for fut in list(self._pending.values()):
            if not fut.done():
                fut.set_exception(BrokerError(reason))

    # ---------------------------------------------------------------- consuming

    async def subscribe(self, destination: str, durable_name: str | None, handler: Handler) -> None:
        dest = wire_dest(destination)
        if dest[0] != ACTIVEMQ_TOPIC:
            durable_name = None                             # durable subscriptions exist for topics only
        cons = (CONSUMER_ID, self.connection_id, 1, next(self._consumer_seq))
        self._consumers[cons] = (canonical_destination(destination), handler)
        o, cid = self._command(CONSUMER_INFO, True)
        w_nested(o, cons, self.version)
        o.bool(False)                                       # browser
        w_nested(o, dest, self.version)
        o.int(1000 if dest[0] == ACTIVEMQ_TOPIC and not durable_name else 100)   # prefetch
        o.int(0)                                            # maximumPendingMessageLimit
        o.bool(True)                                        # dispatchAsync
        # clientId: Artemis names the durable queue '<clientId>.<subscriptionName>' from this field (and looks it
        # up the same way on unsubscribe), so it must carry the connection's client id for durable consumers.
        o.string(None).string(self.client_id if durable_name else None).string(durable_name)  # selector, clientId, subscriptionName
        o.bool(False).bool(False).bool(False).byte(0)       # noLocal, exclusive, retroactive, priority
        o.bool(False)                                       # brokerPath
        w_nested(o, None, self.version)                     # additionalPredicate
        o.bool(False).bool(False).bool(False)               # networkSubscription, optimizedAck, noRangeAcks
        o.bool(False)                                       # networkConsumerPath
        try:
            await self._request(o, cid)
        except BrokerError:
            self._consumers.pop(cons, None)
            raise

    async def unsubscribe(self, destination: str, durable_name: str | None = None) -> None:
        """Close the consumers of a destination; with a durable name also delete the durable subscription."""
        destination = canonical_destination(destination)
        for cons, (d, _) in list(self._consumers.items()):
            if d == destination:
                del self._consumers[cons]
                o, cid = self._command(REMOVE_INFO, True)
                w_nested(o, cons, self.version)
                o.long(-2)                                  # lastDeliveredSequenceId: unknown
                await self._request(o, cid)
        if durable_name and split_destination(destination)[0] == TOPIC:
            o, cid = self._command(REMOVE_SUBSCRIPTION_INFO, True)
            w_nested(o, (CONNECTION_ID, self.connection_id), self.version)
            o.string(durable_name).string(self.client_id)
            await self._request(o, cid)

    async def _ack(self, consumer: tuple, destination: Any, message_id: Any) -> None:
        o, _ = self._command(MESSAGE_ACK, False)
        w_nested(o, destination, self.version)
        w_nested(o, None, self.version)                     # transactionId
        w_nested(o, consumer, self.version)
        o.byte(INDIVIDUAL_ACK_TYPE)
        w_nested(o, message_id, self.version)
        w_nested(o, message_id, self.version)
        o.int(1)                                            # messageCount
        o.bool(False)                                       # poisonCause
        await self._write(bytes(o.b))

    # ---------------------------------------------------------------- producing

    async def send(self, env: Envelope) -> str:
        seq = next(self._producer_seq)
        mid = (MESSAGE_ID, None, self.producer, seq, 0)
        body = mutf8(env.text)
        content = struct.pack('>i', len(body)) + body
        props = dict(env.properties)
        o, cid = self._command(ACTIVEMQ_TEXT_MESSAGE, True)
        w_nested(o, self.producer, self.version)
        w_nested(o, wire_dest(env.destination), self.version)
        w_nested(o, None, self.version)                     # transactionId
        w_nested(o, None, self.version)                     # originalDestination
        w_nested(o, mid, self.version)
        w_nested(o, None, self.version)                     # originalTransactionId
        o.string(env.group_id).int(env.group_seq if env.group_id else 0)
        o.string(env.correlation_id).bool(env.persistent)
        o.long(env.expiration).byte(env.priority)           # expiration, priority
        w_nested(o, wire_dest(env.reply_to) if env.reply_to else None, self.version)
        env.timestamp = time.time()
        o.long(int(env.timestamp * 1000))
        o.string(env.jms_type)
        o.byteseq(content)
        o.byteseq(marshal_map(props) if props else None)
        w_nested(o, None, self.version)                     # dataStructure
        w_nested(o, None, self.version)                     # targetConsumerId
        o.bool(False).int(0)                                # compressed, redeliveryCounter
        o.bool(False).long(0).string(None)                  # brokerPath, arrival, userID
        o.bool(False).bool(False).bool(False)               # recievedByDFBridge, droppable, cluster
        o.long(0).long(0)                                   # brokerInTime, brokerOutTime
        if self.version >= 10:
            o.bool(False)                                   # JMSXGroupFirstForConsumer
        await self._request(o, cid)
        env.message_id = message_id_str(mid)
        return env.message_id

    # ---------------------------------------------------------------- background loops

    async def _read_loop(self) -> None:
        try:
            while True:
                frame = await self._read_frame()
                self._handle(frame)
        except asyncio.CancelledError:
            raise
        except asyncio.IncompleteReadError:
            self._lost('connection closed by broker')
        except Exception as e:  # noqa: BLE001 - any failure ends this connection
            self._lost(f'{e.__class__.__name__}: {e}')

    def _handle(self, frame: bytes) -> None:
        t = frame[0]
        i = In(frame[1:])
        if t in (RESPONSE, EXCEPTION_RESPONSE):
            i.int(), i.bool()
            corr = i.int()
            fut = self._pending.get(corr)
            err = r_throwable(i, self.stack_traces) if t == EXCEPTION_RESPONSE else None
            if fut and not fut.done():
                if err:
                    fut.set_exception(BrokerError(err))
                else:
                    fut.set_result(None)
        elif t == MESSAGE_DISPATCH:
            i.int(), i.bool()
            consumer = r_nested(i, self.version)
            destination = r_nested(i, self.version)
            message = r_nested(i, self.version)
            if message is not None and consumer in self._consumers:
                self._dispatch_queue.put_nowait((consumer, destination, message))
        elif t == KEEP_ALIVE_INFO:
            i.int()
            if i.bool():
                o, _ = self._command(KEEP_ALIVE_INFO, False)
                asyncio.create_task(self._write(bytes(o.b)))
        elif t == CONNECTION_ERROR:
            i.int(), i.bool()
            self._lost(f'broker: {r_throwable(i, self.stack_traces)}')
        elif t == SHUTDOWN_INFO:
            self._lost('broker shut down the connection')
        elif t == BROKER_INFO:
            i.int(), i.bool()
            r_nested(i, self.version)
            url = i.string()
            if url and url not in self.server_info:
                self.server_info += f' @ {url}'
        # ConnectionControl, ConsumerControl, ProducerAck, ... are not needed here

    async def _dispatch_loop(self) -> None:
        while True:
            consumer, destination, m = await self._dispatch_queue.get()
            subscribed, handler = self._consumers[consumer]
            env = envelope_from_message(m, subscribed, self.hide_internal_properties)
            try:
                await handler(env)
            except Exception:  # noqa: BLE001 - a failing handler must not stop consumption
                log.exception('message handler failed for %s', subscribed)
            try:
                await self._ack(consumer, m['destination'] or destination, m['message_id'])
            except BrokerError as e:
                self._lost(str(e))
                return

    async def _keepalive_loop(self) -> None:
        while True:
            await asyncio.sleep(self.keepalive)
            if time.monotonic() - self._last_read > 3 * 30:
                self._lost('no data from broker for 90 s')
                return
            try:
                o, _ = self._command(KEEP_ALIVE_INFO, False)
                await self._write(bytes(o.b))
            except BrokerError as e:
                self._lost(str(e))
                return


_conn_counter = itertools.count(1)


class _Long(int):
    """Marker so WireFormatInfo durations are written as Java longs."""


def _marshal_wf(props: dict[str, Any]) -> bytes:
    o = Out().int(len(props))
    for k, v in props.items():
        o.utf(k)
        if isinstance(v, bool):
            o.byte(1).bool(v)
        elif isinstance(v, _Long):
            o.byte(6).long(v)
        elif isinstance(v, int):
            o.byte(5).int(v)
        else:
            o.byte(9).utf(str(v))
    return bytes(o.b)
