"""Transport against a real Redis.

Every test runs with ``min_idle_ms=0``: by idle time any pending entry is already
eligible, so the alive key is the only thing that decides a reclaim. That removes
every wait from this suite -- no test sleeps or crosses a timeout.
"""

from collections.abc import Awaitable, Callable

import anyio
import pytest

from litestar_rs.core.envelope import Envelope, from_fields
from litestar_rs.core.transport import RedisStreamsTransport

pytestmark = pytest.mark.integration

Make = Callable[[str], Awaitable[RedisStreamsTransport]]

TTL_MS = 30_000


def envelope(task: str = "reindex", **overrides: object) -> Envelope:
    base: dict[str, object] = {
        "id": f"job-{task}",
        "task": task,
        "payload": b'{"doc_id":1}',
        "enqueued_at": 1712345678901,
    }
    return Envelope(**(base | overrides))  # type: ignore[arg-type]  # test factory


async def test_ensure_group_is_idempotent(transport: RedisStreamsTransport) -> None:
    await transport.ensure_group()
    await transport.ensure_group()
    groups = await transport.control.xinfo_groups(transport.streams[0])
    assert len(groups) == 1


async def test_entry_survives_the_round_trip(transport: RedisStreamsTransport) -> None:
    original = envelope(traceparent="00-abcdef-012345-01")
    await transport.enqueue(original, queue=transport.queues[0])

    [record] = await transport.read(10)

    assert from_fields(record.fields) == original
    assert record.fields[b"traceparent"] == b"00-abcdef-012345-01"


async def test_ack_clears_the_pel_and_the_stream(
    transport: RedisStreamsTransport,
) -> None:
    await transport.enqueue(envelope(), queue=transport.queues[0])
    [record] = await transport.read(10)
    await transport.mark_alive([record.entry_id], ttl_ms=TTL_MS)

    assert await transport.ack(record.stream, [record.entry_id]) == 1

    assert await transport.control.xlen(record.stream) == 0
    summary = await transport.control.xpending(record.stream, transport.group)
    assert summary["pending"] == 0
    assert await transport.control.exists(transport.alive_key(record.entry_id)) == 0


async def test_second_ack_is_a_no_op(transport: RedisStreamsTransport) -> None:
    await transport.enqueue(envelope(), queue=transport.queues[0])
    [record] = await transport.read(10)
    await transport.ack(record.stream, [record.entry_id])

    assert await transport.ack(record.stream, [record.entry_id]) == 0


async def test_live_owner_is_not_reclaimed(
    transport: RedisStreamsTransport, transports: Make
) -> None:
    """A long task is not reclaimed while its worker keeps saying it is alive."""
    await transport.enqueue(envelope(), queue=transport.queues[0])
    [record] = await transport.read(10)
    await transport.mark_alive([record.entry_id], ttl_ms=TTL_MS)

    peer = await transports("worker-2")
    claimed = await peer.reclaim(
        record.stream, record.entry_id, min_idle_ms=0, ttl_ms=TTL_MS
    )

    assert claimed == []
    assert (
        await transport.control.get(transport.alive_key(record.entry_id)) == b"worker-1"
    )


async def test_dead_owner_is_reclaimed(
    transport: RedisStreamsTransport, transports: Make
) -> None:
    await transport.enqueue(envelope(), queue=transport.queues[0])
    [record] = await transport.read(10)
    await transport.mark_alive([record.entry_id], ttl_ms=TTL_MS)
    await transport.clear_alive([record.entry_id])

    peer = await transports("worker-2")
    [claimed] = await peer.reclaim(
        record.stream, record.entry_id, min_idle_ms=0, ttl_ms=TTL_MS
    )

    assert claimed.entry_id == record.entry_id
    assert from_fields(claimed.fields).task == "reindex"
    assert await peer.control.get(peer.alive_key(record.entry_id)) == b"worker-2"

    [pending] = await peer.control.xpending_range(
        record.stream, peer.group, min="-", max="+", count=10
    )
    assert pending["consumer"] == b"worker-2"
    assert pending["times_delivered"] == 2


async def test_only_one_of_many_reclaimers_wins(
    transport: RedisStreamsTransport, transports: Make
) -> None:
    """The atomicity requirement on the Lua script, under real concurrency."""
    await transport.enqueue(envelope(), queue=transport.queues[0])
    [record] = await transport.read(10)

    peers = [await transports(f"peer-{n}") for n in range(8)]
    results: list[int] = []

    async def claim(peer: RedisStreamsTransport) -> None:
        claimed = await peer.reclaim(
            record.stream, record.entry_id, min_idle_ms=0, ttl_ms=TTL_MS
        )
        results.append(len(claimed))

    async with anyio.create_task_group() as tg:
        for peer in peers:
            tg.start_soon(claim, peer)

    assert sum(1 for count in results if count) == 1


async def test_reclaiming_a_vanished_entry_cleans_the_pel(
    transport: RedisStreamsTransport, transports: Make
) -> None:
    await transport.enqueue(envelope(), queue=transport.queues[0])
    [record] = await transport.read(10)
    await transport.control.xdel(record.stream, record.entry_id)

    peer = await transports("worker-2")
    assert (
        await peer.reclaim(record.stream, record.entry_id, min_idle_ms=0, ttl_ms=TTL_MS)
        == []
    )
    summary = await peer.control.xpending(record.stream, peer.group)
    assert summary["pending"] == 0


async def test_stream_does_not_grow_under_steady_ack(
    transport: RedisStreamsTransport,
) -> None:
    for round_number in range(50):
        await transport.enqueue(
            envelope(id=f"job-{round_number}"), queue=transport.queues[0]
        )
        [record] = await transport.read(10)
        await transport.mark_alive([record.entry_id], ttl_ms=TTL_MS)
        await transport.ack(record.stream, [record.entry_id])
        assert await transport.control.xlen(record.stream) == 0


async def test_trim_keeps_unacked_entries(transport: RedisStreamsTransport) -> None:
    """MINID would drop pending work older than the window; the floor prevents it."""
    await transport.enqueue(envelope(id="pending-one"), queue=transport.queues[0])
    [record] = await transport.read(10)

    await transport.trim(retention_ms=0)

    assert await transport.control.xlen(record.stream) == 1


async def test_trim_drops_acked_entries(transport: RedisStreamsTransport) -> None:
    await transport.enqueue(envelope(), queue=transport.queues[0])
    [record] = await transport.read(10)
    await transport.ack(record.stream, [record.entry_id])
    await transport.enqueue(envelope(id="kept"), queue=transport.queues[0])

    await transport.trim(retention_ms=24 * 3600 * 1000)

    assert await transport.control.xlen(record.stream) == 1


async def test_lag_reports_depth(transport: RedisStreamsTransport) -> None:
    assert await transport.lag() == 0
    await transport.enqueue(envelope(), queue=transport.queues[0])
    assert await transport.lag() == 1
    await transport.read(10)
    assert await transport.lag() == 0


async def test_high_priority_is_served_first(
    prioritised: RedisStreamsTransport,
) -> None:
    """The low entry was enqueued first and still waits."""
    await prioritised.enqueue(envelope(id="slow"), queue="low")
    await prioritised.enqueue(envelope(id="urgent"), queue="high")

    [first] = await prioritised.read(1)

    assert from_fields(first.fields).id == "urgent"
    assert prioritised.queue_of(first.stream) == "high"


async def test_the_low_queue_is_still_reached_when_high_is_empty(
    prioritised: RedisStreamsTransport,
) -> None:
    await prioritised.enqueue(envelope(id="slow"), queue="low")

    [record] = await prioritised.read(10)

    assert prioritised.queue_of(record.stream) == "low"
