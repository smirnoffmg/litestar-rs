"""One adapter per queue under measurement.

Each opens its own connections and runs its own worker, so a slow adapter cannot
borrow another's warm pool. Every adapter uses a fresh namespace per run: a
consumer group carried over from a previous run has a pending list, and that
changes the first read.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from uuid import uuid4

import anyio
from redis.asyncio import Redis

from benchmarks.harness import Adapter
from smallage.core.envelope import Envelope
from smallage.core.scheduler import RedisScheduler
from smallage.core.testing import worker_running
from smallage.core.transport import RedisStreamsTransport
from smallage.core.worker import WorkerConfig

PAYLOAD = b'{"n":0}'


class Smallage(Adapter):
    """The queue this repository is.

    `block_ms` is left at the library default rather than tuned down for the
    latency run: a benchmark that tunes only the subject is measuring the tuning.
    """

    def __init__(
        self,
        redis_url: str,
        *,
        concurrency: int = 10,
        queues: tuple[str, ...] = ("default",),
        label: str | None = None,
    ) -> None:
        self.redis_url = redis_url
        self.concurrency = concurrency
        self.queues = queues
        self.name = label or f"smallage q={len(queues)}"
        self._transport: RedisStreamsTransport | None = None
        self._scheduler: RedisScheduler | None = None

    @asynccontextmanager
    async def prepared(self) -> AsyncIterator[None]:
        namespace = f"bench{uuid4().hex[:8]}"
        reader: Redis = Redis.from_url(self.redis_url, socket_timeout=35.0)
        control: Redis = Redis.from_url(self.redis_url)
        try:
            self._transport = RedisStreamsTransport(
                reader=reader,
                control=control,
                consumer=f"bench-{uuid4().hex[:8]}",
                namespace=namespace,
                queues=self.queues,
            )
            self._scheduler = RedisScheduler(control=control, namespace=namespace)
            await self._transport.ensure_group()
            yield
        finally:
            await control.delete(*self._transport.streams)  # type: ignore[union-attr]
            self._transport = None
            self._scheduler = None
            await reader.aclose()
            await control.aclose()

    async def enqueue(self, index: int) -> None:
        if self._transport is None:
            raise RuntimeError("prepared() must be entered first")
        await self._transport.enqueue(
            Envelope(
                id=f"job-{index}",
                task="noop",
                payload=PAYLOAD,
                enqueued_at=0,
            ),
            # Highest-priority queue: the one a caller would reach for.
            queue=self.queues[0],
        )

    @asynccontextmanager
    async def consuming(self, on_handled: Callable[[], None]) -> AsyncIterator[None]:
        if self._transport is None or self._scheduler is None:
            raise RuntimeError("prepared() must be entered first")

        async def noop(envelope: Envelope) -> None:
            on_handled()

        async with worker_running(
            self._transport,
            {"noop": noop},
            WorkerConfig(
                concurrency=self.concurrency,
                # The supervisors are part of what is being measured, but their
                # default cadence would barely tick inside a short run; leaving
                # them at the default keeps the loop honest.
                trim_interval_s=60.0,
                scheduler_interval_s=1.0,
            ),
            scheduler=self._scheduler,
        ):
            yield


class RawRedisStream(Adapter):
    """The floor: `XADD` and `XREADGROUP` with no queue on top.

    Not a competitor -- a control. Every difference between this and a real queue
    is what durability, retries, liveness and dispatch actually cost, which is
    the only honest way to read a throughput number for any of them.
    """

    name = "raw XADD/XREADGROUP"

    def __init__(self, redis_url: str, *, batch: int = 10) -> None:
        self.redis_url = redis_url
        self.batch = batch
        self._client: Redis | None = None
        self._stream = ""

    @asynccontextmanager
    async def prepared(self) -> AsyncIterator[None]:
        self._stream = f"bench:{uuid4().hex[:8]}"
        client: Redis = Redis.from_url(self.redis_url, socket_timeout=35.0)
        self._client = client
        try:
            await client.xgroup_create(self._stream, "bench", id="0", mkstream=True)
            yield
        finally:
            await client.delete(self._stream)
            self._client = None
            await client.aclose()

    async def enqueue(self, index: int) -> None:
        if self._client is None:
            raise RuntimeError("prepared() must be entered first")
        await self._client.xadd(self._stream, {b"n": str(index).encode()})

    @asynccontextmanager
    async def consuming(self, on_handled: Callable[[], None]) -> AsyncIterator[None]:
        if self._client is None:
            raise RuntimeError("prepared() must be entered first")
        client = self._client

        async def loop() -> None:
            while True:
                reply = await client.xreadgroup(
                    groupname="bench",
                    consumername="bench-1",
                    streams={self._stream: ">"},
                    count=self.batch,
                    block=1000,
                )
                for _, entries in reply or []:
                    ids = [entry_id for entry_id, _ in entries]
                    for _ in ids:
                        on_handled()
                    await client.xack(self._stream, "bench", *ids)

        async with anyio.create_task_group() as tg:
            tg.start_soon(loop)
            try:
                yield
            finally:
                tg.cancel_scope.cancel()


class Saq(Adapter):
    """SAQ, the closest neighbour: async-native, Redis, heartbeat rather than a
    visibility timeout.

    Its own defaults are left alone. Tuning the comparator to match the subject
    is how a benchmark becomes an advertisement.
    """

    def __init__(self, redis_url: str, *, concurrency: int = 10) -> None:
        self.redis_url = redis_url
        self.concurrency = concurrency
        self.name = "saq"
        self._queue: object | None = None

    @asynccontextmanager
    async def prepared(self) -> AsyncIterator[None]:
        from saq import Queue

        name = f"bench{uuid4().hex[:8]}"
        queue = Queue.from_url(self.redis_url, name=name)
        self._queue = queue
        try:
            yield
        finally:
            keys = [k async for k in queue.redis.scan_iter(match=f"saq:*{name}*")]
            if keys:
                await queue.redis.delete(*keys)
            await queue.disconnect()
            self._queue = None

    async def enqueue(self, index: int) -> None:
        if self._queue is None:
            raise RuntimeError("prepared() must be entered first")
        await self._queue.enqueue("noop", n=index)

    @asynccontextmanager
    async def consuming(self, on_handled: Callable[[], None]) -> AsyncIterator[None]:
        from saq import Worker

        if self._queue is None:
            raise RuntimeError("prepared() must be entered first")

        async def noop(ctx: dict[str, object], *, n: int) -> None:
            on_handled()

        worker = Worker(
            self._queue,
            functions=[("noop", noop)],
            concurrency=self.concurrency,
        )
        async with anyio.create_task_group() as tg:
            tg.start_soon(worker.start)
            try:
                yield
            finally:
                await worker.stop()
                tg.cancel_scope.cancel()
