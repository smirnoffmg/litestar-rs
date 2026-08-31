"""A deployment that owns no queue at all, against a real Redis.

Not in ``test_topologies.py``: that file is about the Redis deployments the key
schema has to survive, and spawns containers to get them. These two need one
plain Redis, and carrying its ``slow`` marker would hide them from a run that
asked for the fast integration tests.
"""

from uuid import uuid4

import anyio
import pytest
from redis.asyncio import Redis

from smallage.core.envelope import Record
from smallage.core.scheduler import RedisScheduler
from smallage.core.testing import worker_running
from smallage.core.transport import RedisStreamsTransport
from smallage.core.worker import WorkerConfig

pytestmark = pytest.mark.integration


async def queueless(
    redis_url: str, namespace: str, foreign: str, *, block_ms: int
) -> RedisStreamsTransport:
    """A transport for a deployment that owns no queue at all."""
    reader: Redis = Redis.from_url(redis_url, socket_timeout=30.0)
    control: Redis = Redis.from_url(redis_url)
    transport = RedisStreamsTransport(
        reader=reader,
        control=control,
        consumer="worker-1",
        namespace=namespace,
        queues=(),
        external=(foreign,),
        block_ms=block_ms,
    )
    await transport.ensure_group()
    return transport


def broker_config() -> WorkerConfig:
    return WorkerConfig(
        concurrency=1,
        min_idle_ms=0,
        reclaim_interval_s=0.05,
        trim_interval_s=0.05,
        scheduler_interval_s=0.05,
    )


async def test_a_worker_with_no_queues_of_its_own_consumes_a_foreign_stream(
    redis_url: str,
) -> None:
    """The topology a pure broker deployment actually has.

    The keyspace assertion is the point: a queue stream and a group that exist
    only because validation demanded a queue name are indistinguishable, to
    anyone reading the keyspace later, from ones something is meant to write to.
    The trim loop runs throughout, over nothing.
    """
    namespace = f"t{uuid4().hex[:8]}"
    foreign = f"{{{namespace}}}:orders"
    seen: list[bytes] = []
    arrived = anyio.Event()

    async def on_order(record: Record) -> None:
        seen.append(record.fields[b"sku"])
        arrived.set()

    transport = await queueless(redis_url, namespace, foreign, block_ms=100)
    try:
        scheduler = RedisScheduler(control=transport.control, namespace=namespace)
        await transport.control.xadd(foreign, {b"sku": b"A1"})
        with anyio.fail_after(30):
            async with worker_running(
                transport,
                {},
                broker_config(),
                scheduler=scheduler,
                brokers={foreign: on_order},
            ):
                await arrived.wait()

        assert seen == [b"A1"]
        assert await transport.control.keys(f"{{{namespace}}}:q:*") == []
    finally:
        await transport.reader.aclose()
        await transport.control.aclose()


async def test_a_foreign_entry_reaches_an_idle_queueless_worker_at_once(
    redis_url: str,
) -> None:
    """`docs/broker.md` documents a `block_ms` latency floor for foreign streams.

    It is the price of keeping priority between a worker's own queues, and a
    worker that has none pays it for nothing. The margin below is what says the
    entry was waited for rather than polled up at the end of a block window.
    """
    namespace = f"t{uuid4().hex[:8]}"
    foreign = f"{{{namespace}}}:orders"
    handled = [anyio.Event(), anyio.Event()]

    async def on_order(record: Record) -> None:
        handled[int(record.fields[b"n"])].set()

    transport = await queueless(redis_url, namespace, foreign, block_ms=5_000)
    try:
        scheduler = RedisScheduler(control=transport.control, namespace=namespace)
        await transport.control.xadd(foreign, {b"n": b"0"})
        with anyio.fail_after(60):
            async with worker_running(
                transport,
                {},
                broker_config(),
                scheduler=scheduler,
                brokers={foreign: on_order},
            ):
                await handled[0].wait()
                # Long enough that the next read is a blocking one already under
                # way, so what follows measures the wait and not a lucky pass.
                await anyio.sleep(0.5)

                await transport.control.xadd(foreign, {b"n": b"1"})
                with anyio.fail_after(2):
                    await handled[1].wait()
    finally:
        await transport.reader.aclose()
        await transport.control.aclose()
