"""The only module that talks to redis-py."""

from collections.abc import Iterable, Sequence
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from litestar_rs.core.envelope import Envelope, Pending, Record, to_fields
from litestar_rs.core.errors import ConfigurationError, PayloadTooLarge
from litestar_rs.core.keys import (
    alive_key,
    dlq_key,
    stream_for,
    stream_keys,
    validate_namespace,
    validate_queue,
)
from litestar_rs.core.scripts import (
    TransportScripts,
    parse_xclaim_reply,
    register_transport,
)

DEFAULT_NAMESPACE = "lrs"
DEFAULT_QUEUE = "default"
DEFAULT_GROUP = "workers"
DEFAULT_MAX_PAYLOAD_BYTES = 128 * 1024

# A traceback is for a human reading the DLQ, not a document store.
MAX_DETAIL_BYTES = 8 * 1024


def _connection_kwarg(client: Redis, name: str) -> Any:
    return client.connection_pool.connection_kwargs.get(name)


class RedisStreamsTransport:
    """Stream operations for one queue.

    Takes two clients on purpose: a blocking ``XREADGROUP`` occupies its connection
    for the whole block window, and an ack or a liveness refresh queued behind it
    makes a healthy worker look dead to its peers.
    """

    def __init__(
        self,
        *,
        reader: Redis,
        control: Redis,
        consumer: str,
        namespace: str = DEFAULT_NAMESPACE,
        queue: str = DEFAULT_QUEUE,
        shards: int = 1,
        group: str = DEFAULT_GROUP,
        block_ms: int = 5_000,
        max_payload_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES,
    ) -> None:
        if reader is control:
            raise ConfigurationError(
                "reader and control must be separate clients: a blocking "
                "XREADGROUP would otherwise stall acks and liveness refreshes"
            )
        for role, client in (("reader", reader), ("control", control)):
            if _connection_kwarg(client, "decode_responses"):
                raise ConfigurationError(
                    f"{role} client must be built with decode_responses=False; "
                    "stream payloads are opaque bytes"
                )
        if not consumer:
            raise ConfigurationError("consumer must not be empty")
        if max_payload_bytes < 1:
            raise ConfigurationError(
                f"max_payload_bytes must be at least 1, got {max_payload_bytes}"
            )
        if block_ms < 0:
            raise ConfigurationError(f"block_ms must not be negative, got {block_ms}")

        socket_timeout = _connection_kwarg(reader, "socket_timeout")
        if socket_timeout is None:
            raise ConfigurationError(
                "reader client needs an explicit socket_timeout, otherwise a read "
                "hung by a failover never returns and the worker goes quiet"
            )
        if socket_timeout * 1000 <= block_ms:
            raise ConfigurationError(
                f"reader socket_timeout ({socket_timeout}s) must outlast block_ms "
                f"({block_ms}ms), or every healthy blocking read times out"
            )

        self.reader = reader
        self.control = control
        self.consumer = consumer
        self.namespace = validate_namespace(namespace)
        self.queue = validate_queue(queue)
        self.group = group
        self.block_ms = block_ms
        self.max_payload_bytes = max_payload_bytes
        self.streams = stream_keys(self.namespace, self.queue, shards)
        self.dlq = dlq_key(self.namespace)
        self._scripts: TransportScripts = register_transport(control)

    def alive_key(self, entry_id: bytes) -> str:
        return alive_key(self.namespace, entry_id)

    async def ensure_group(self) -> None:
        for stream in self.streams:
            try:
                await self.control.xgroup_create(
                    stream, self.group, id="0", mkstream=True
                )
            except ResponseError as exc:
                if "BUSYGROUP" not in str(exc):
                    raise

    async def enqueue(self, envelope: Envelope, *, queue: str) -> bytes:
        if len(envelope.payload) > self.max_payload_bytes:
            raise PayloadTooLarge(
                f"payload for task {envelope.task!r} is {len(envelope.payload)} bytes, "
                f"over the {self.max_payload_bytes} byte limit; "
                "store it out of band and enqueue a payload_ref instead"
            )
        stream = stream_for(
            self.namespace, validate_queue(queue), len(self.streams), envelope.id
        )
        fields: dict[Any, Any] = dict(to_fields(envelope))
        entry_id = await self.control.xadd(stream, fields)
        return _as_bytes(entry_id)

    async def read(self, count: int) -> list[Record]:
        if count <= 0:
            # XREADGROUP COUNT 0 is not the same thing as not reading: a fixed
            # COUNT is what skews work between workers (taskiq-redis#91).
            return []
        reply = await self.reader.xreadgroup(
            groupname=self.group,
            consumername=self.consumer,
            streams=dict.fromkeys(self.streams, ">"),
            count=count,
            block=self.block_ms,
        )
        return _records_from_read(reply)

    async def mark_alive(self, entry_ids: Sequence[bytes], *, ttl_ms: int) -> None:
        if not entry_ids:
            return
        async with self.control.pipeline(transaction=False) as pipe:
            for entry_id in entry_ids:
                pipe.set(self.alive_key(entry_id), self.consumer, px=ttl_ms)
            await pipe.execute()

    async def refresh_alive(self, entry_ids: Iterable[bytes], *, ttl_ms: int) -> None:
        """PEXPIRE rather than SET: it must not resurrect an already acked entry."""
        keys = [self.alive_key(entry_id) for entry_id in entry_ids]
        if not keys:
            return
        async with self.control.pipeline(transaction=False) as pipe:
            for key in keys:
                pipe.pexpire(key, ttl_ms)
            await pipe.execute()

    async def clear_alive(self, entry_ids: Iterable[bytes]) -> None:
        """Hand an entry over immediately instead of waiting out its TTL."""
        keys = [self.alive_key(entry_id) for entry_id in entry_ids]
        if keys:
            await self.control.delete(*keys)

    async def ack(self, stream: str, entry_ids: Sequence[bytes]) -> int:
        if not entry_ids:
            return 0
        keys = [stream, *(self.alive_key(entry_id) for entry_id in entry_ids)]
        acked = await self._scripts.ack(keys=keys, args=[self.group, *entry_ids])
        return int(acked)

    async def pending(self, *, count: int, min_idle_ms: int) -> list[Pending]:
        if count <= 0:
            return []
        found: list[Pending] = []
        for stream in self.streams:
            entries = await self.control.xpending_range(
                stream,
                self.group,
                min="-",
                max="+",
                count=count - len(found),
                idle=min_idle_ms,
            )
            found.extend(
                Pending(
                    stream=stream,
                    entry_id=_as_bytes(entry["message_id"]),
                    times_delivered=int(entry["times_delivered"]),
                )
                for entry in entries
            )
            if len(found) >= count:
                break
        return found

    async def dead_letter(
        self, record: Record, *, reason: str, detail: str, times_delivered: int
    ) -> bytes:
        """Park an entry that will never succeed, keeping everything about it.

        The original payload rides along untouched, so a fixed deployment can
        replay it without reconstructing anything.
        """
        seconds, microseconds = await self.control.time()
        fields: dict[Any, Any] = dict(record.fields)
        fields[b"dlq_reason"] = reason.encode()
        fields[b"dlq_detail"] = detail.encode()[:MAX_DETAIL_BYTES]
        fields[b"dlq_source"] = record.stream.encode()
        fields[b"dlq_deliveries"] = str(times_delivered).encode()
        fields[b"dlq_at"] = str(seconds * 1000 + microseconds // 1000).encode()
        return _as_bytes(await self.control.xadd(self.dlq, fields))

    async def reclaim(
        self, stream: str, entry_id: bytes, *, min_idle_ms: int, ttl_ms: int
    ) -> list[Record]:
        reply = await self._scripts.reclaim(
            keys=[stream, self.alive_key(entry_id)],
            args=[self.group, self.consumer, min_idle_ms, entry_id, ttl_ms],
        )
        return parse_xclaim_reply(stream, list(reply))

    async def trim(self, *, retention_ms: int) -> None:
        """Trim by time, floored at the oldest unacked entry.

        MINID has the same hazard MAXLEN does once a pending entry is older than
        the retention window -- trimming it away would drop unacknowledged work.
        """
        seconds, microseconds = await self.control.time()
        now_ms = seconds * 1000 + microseconds // 1000
        floor_ms = now_ms - retention_ms
        for stream in self.streams:
            summary = await self.control.xpending(stream, self.group)
            oldest = summary.get("min") if summary else None
            limit = floor_ms if oldest is None else min(floor_ms, _entry_ms(oldest))
            await self.control.xtrim(stream, minid=limit, approximate=True)

    async def lag(self) -> int | None:
        """Depth from XINFO GROUPS. Redis reports NULL when it cannot reconcile."""
        total = 0
        for stream in self.streams:
            groups = await self.control.xinfo_groups(stream)
            for group in groups:
                if _as_bytes(_get(group, "name")) != self.group.encode():
                    continue
                value = _get(group, "lag")
                if value is None:
                    return None
                total += int(value)
        return total


def _as_bytes(value: Any) -> bytes:
    return value if isinstance(value, bytes) else str(value).encode()


def _get(mapping: Any, name: str) -> Any:
    """XINFO replies come back keyed by str or bytes depending on the parser."""
    if name in mapping:
        return mapping[name]
    return mapping.get(name.encode())


def _entry_ms(entry_id: Any) -> int:
    return int(_as_bytes(entry_id).split(b"-")[0])


def _records_from_read(reply: Any) -> list[Record]:
    if not reply:
        return []
    pairs = reply.items() if isinstance(reply, dict) else reply
    records = []
    for stream, entries in pairs:
        name = stream.decode() if isinstance(stream, bytes) else str(stream)
        for entry_id, fields in entries:
            records.append(
                Record(stream=name, entry_id=_as_bytes(entry_id), fields=dict(fields))
            )
    return records
