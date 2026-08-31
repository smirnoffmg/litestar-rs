"""Consumer names must not accumulate in the group forever.

A worker registers its consumer name on its first read and, with a name derived
per process start, a rolling deploy leaves one behind every time. Redis expires
none of them.
"""

import pytest
from redis.asyncio import Redis

from smallage.core.envelope import Envelope
from smallage.core.transport import RedisStreamsTransport

pytestmark = pytest.mark.integration


async def transport_named(
    redis_url: str, namespace: str, consumer: str
) -> tuple[RedisStreamsTransport, Redis, Redis]:
    reader: Redis = Redis.from_url(redis_url, socket_timeout=35.0)
    control: Redis = Redis.from_url(redis_url)
    t = RedisStreamsTransport(
        reader=reader, control=control, consumer=consumer, namespace=namespace
    )
    await t.ensure_group()
    return t, reader, control


async def consumer_names(control: Redis, transport: RedisStreamsTransport) -> set[str]:
    info = await control.xinfo_consumers(transport.streams[0], transport.group)
    return {row["name"].decode() for row in info}


async def test_a_consumer_that_left_nothing_behind_is_swept(
    redis_url: str, namespace: str
) -> None:
    """Three processes' worth of names, none of them holding anything."""
    opened = []
    try:
        for n in range(3):
            t, r, c = await transport_named(redis_url, namespace, f"worker-{n}")
            await t.read(1)  # registers the consumer even with nothing to read
            opened.append((t, r, c))

        transport, _, control = opened[0]
        assert await consumer_names(control, transport) == {
            "worker-0",
            "worker-1",
            "worker-2",
        }

        swept = await transport.sweep_consumers(min_idle_ms=0)

        assert swept == 3
        assert await consumer_names(control, transport) == set()
    finally:
        for _, r, c in opened:
            await r.aclose()
            await c.aclose()


async def test_a_consumer_still_holding_an_entry_is_left_alone(
    redis_url: str, namespace: str
) -> None:
    """Deleting it would make its pending entries unclaimable -- Redis says so.

    That is the whole risk of this sweep, so it is asserted rather than assumed.
    """
    holder, r1, c1 = await transport_named(redis_url, namespace, "holder")
    idler, r2, c2 = await transport_named(redis_url, namespace, "idler")
    try:
        await idler.read(1)
        await holder.enqueue(
            Envelope(id="j1", task="noop", payload=b"{}", enqueued_at=0),
            queue="default",
        )
        records = await holder.read(1)
        assert len(records) == 1, "the holder must own a pending entry"

        swept = await holder.sweep_consumers(min_idle_ms=0)

        assert swept == 1, "only the idler goes"
        assert await consumer_names(c1, holder) == {"holder"}

        # And the entry it holds is still reclaimable, which is the point.
        pending = await holder.pending(count=10, min_idle_ms=0)
        assert [p.consumer for p in pending] == ["holder"]
    finally:
        for c in (r1, c1, r2, c2):
            await c.aclose()


async def test_a_recently_active_consumer_is_kept(
    redis_url: str, namespace: str
) -> None:
    """Idleness is the evidence that a process is gone; without it the sweep
    would delete live workers between their reads, and Redis would recreate them
    on the next one -- churn for nothing."""
    transport, r, c = await transport_named(redis_url, namespace, "busy")
    try:
        await transport.read(1)

        swept = await transport.sweep_consumers(min_idle_ms=60_000)

        assert swept == 0
        assert await consumer_names(c, transport) == {"busy"}
    finally:
        await r.aclose()
        await c.aclose()
