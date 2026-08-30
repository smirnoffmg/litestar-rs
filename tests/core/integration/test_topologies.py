"""One set of checks, run against every Redis deployment we claim to support.

Cluster is what the hash-tagged key schema exists for: it rejects any multi-key
command whose keys land in different slots, and both Lua scripts, the scheduler's
promotion and the read across queues and shards are all multi-key.
"""

from collections.abc import Iterator
from typing import Any
from uuid import uuid4

import anyio
import pytest
from redis.asyncio import Redis

from litestar_rs.core.envelope import Envelope, from_fields
from litestar_rs.core.scheduler import RedisScheduler
from litestar_rs.core.transport import RedisStreamsTransport
from tests.core.integration.topologies import (
    MASTER_NAME,
    SENTINEL_PORT,
    Topology,
    cluster_container,
    new_sentinel,
    sentinel_container,
    standalone,
)

pytestmark = [pytest.mark.integration, pytest.mark.slow]


@pytest.fixture(scope="session")
def cluster() -> Iterator[Topology]:
    yield from cluster_container()


@pytest.fixture(scope="session")
def sentinel() -> Iterator[Topology]:
    yield from sentinel_container()


@pytest.fixture(params=["standalone", "cluster", "sentinel"])
def topology(request: pytest.FixtureRequest, redis_url: str) -> Topology:
    if request.param == "standalone":
        return standalone(redis_url)
    return request.getfixturevalue(request.param)  # type: ignore[no-any-return]


def envelope(job_id: str) -> Envelope:
    return Envelope(
        id=job_id, task="reindex", payload=b'{"doc":1}', enqueued_at=1712345678901
    )


async def test_the_whole_multi_key_surface_works(topology: Topology) -> None:
    namespace = f"t{uuid4().hex[:8]}"
    reader, control = topology.clients()
    try:
        transport = RedisStreamsTransport(
            reader=reader,
            control=control,
            consumer="worker-1",
            namespace=namespace,
            queues=("high", "low"),
            shards=2,
            block_ms=100,
        )
        scheduler = RedisScheduler(control=control, namespace=namespace, shards=2)
        await transport.ensure_group()

        # A read across two queues and two shards: four streams, one command.
        await transport.enqueue(envelope("job-1"), queue="high")
        [record] = await transport.read(10)
        assert from_fields(record.fields).id == "job-1"

        # ack.lua touches the stream and the alive key together.
        await transport.mark_alive([record.entry_id], ttl_ms=30_000)
        assert await transport.ack(record.stream, [record.entry_id]) == 1
        assert await transport.lag() == 0

        # reclaim.lua touches the stream and the alive key together as well.
        await transport.enqueue(envelope("job-2"), queue="low")
        [second] = await transport.read(10)
        peer_reader, peer_control = topology.clients()
        try:
            peer = RedisStreamsTransport(
                reader=peer_reader,
                control=peer_control,
                consumer="worker-2",
                namespace=namespace,
                queues=("high", "low"),
                shards=2,
                block_ms=100,
            )
            [claimed] = await peer.reclaim(
                second.stream, second.entry_id, min_idle_ms=0, ttl_ms=30_000
            )
            assert claimed.entry_id == second.entry_id
        finally:
            await peer_reader.aclose()
            await peer_control.aclose()

        # promote.lua reads a ZSET, a hash and writes a stream, all in one script.
        due = await scheduler.now_ms() - 1
        await scheduler.schedule_at(envelope("job-3"), queue="high", when_ms=due)
        assert len(await scheduler.promote()) == 1
        assert await scheduler.pending() == 0

        # And the leader lease, which is a plain key beside all of them.
        assert await scheduler.hold_leadership("token-a", ttl_ms=30_000) is True
        assert await scheduler.release_leadership("token-a") is True
    finally:
        await reader.aclose()
        await control.aclose()


async def test_a_namespace_occupies_one_slot_in_a_cluster(cluster: Topology) -> None:
    """Proved by the server, not by reading the braces."""
    from redis.crc import key_slot

    namespace = f"t{uuid4().hex[:8]}"
    reader, control = cluster.clients()
    try:
        # Guard the guard: with one owner the cross-slot rule still applies but
        # nothing is ever redirected, and this test would quietly weaken.
        info: dict[str, Any] = await control.cluster_info()
        assert int(info["cluster_known_nodes"]) >= 3

        transport = RedisStreamsTransport(
            reader=reader,
            control=control,
            consumer="worker-1",
            namespace=namespace,
            queues=("high", "low"),
            shards=3,
            block_ms=100,
        )
        slots = {key_slot(stream.encode()) for stream in transport.streams}
        slots.add(key_slot(transport.alive_key(b"1-0").encode()))
        slots.add(key_slot(transport.dlq.encode()))
        assert len(slots) == 1

        # What a cluster does to a script whose keys forgot their hash tag.
        # redis-py splits a plain multi-key DEL across nodes itself; a script
        # cannot be split, which is why both Lua files depend on the tag.
        with pytest.raises(Exception, match=r"(?i)slot"):
            await control.eval("return 1", 2, "no-tag-a", "no-tag-b")
    finally:
        await reader.aclose()
        await control.aclose()


async def test_a_worker_survives_a_sentinel_failover_with_work_in_flight(
    sentinel: Topology,
) -> None:
    """The failure the whole connection design is shaped around.

    A promotion moves the master out from under a worker that is parked in a
    blocking read. It must notice, reconnect and carry on -- the alternative is
    a process that looks healthy to its probes and does nothing.
    """
    from litestar_rs.core.testing import worker_running
    from litestar_rs.core.worker import WorkerConfig

    topology = sentinel
    watcher = new_sentinel()
    namespace = f"t{uuid4().hex[:8]}"
    reader, control = topology.clients()
    ran: list[str] = []
    started = anyio.Event()
    after_failover = anyio.Event()

    async def handler(env: Envelope) -> None:
        ran.append(env.id)
        started.set()
        if env.id == "after":
            after_failover.set()

    try:
        transport = RedisStreamsTransport(
            reader=reader,
            control=control,
            consumer="worker-1",
            namespace=namespace,
            block_ms=100,
        )
        scheduler = RedisScheduler(control=control, namespace=namespace)
        await transport.ensure_group()
        await transport.enqueue(envelope("before"), queue=transport.queues[0])

        config = WorkerConfig(
            concurrency=2,
            min_idle_ms=0,
            reclaim_interval_s=0.05,
            trim_interval_s=60.0,
            scheduler_interval_s=0.05,
            recovery_interval_s=0.2,
        )
        with anyio.fail_after(120):
            async with worker_running(
                transport, {"reindex": handler}, config, scheduler=scheduler
            ):
                await started.wait()

                before = await watcher.discover_master(MASTER_NAME)
                admin: Redis = Redis(host="127.0.0.1", port=SENTINEL_PORT)
                try:
                    await admin.execute_command(  # type: ignore[no-untyped-call]
                        "SENTINEL", "FAILOVER", MASTER_NAME
                    )
                    # Sentinel promotion has nothing to wait on: discovery is a
                    # query, so polling it is the only way to know it happened.
                    while (  # noqa: ASYNC110
                        await watcher.discover_master(MASTER_NAME) == before
                    ):
                        await anyio.sleep(0.2)
                finally:
                    await admin.aclose()

                await transport.enqueue(envelope("after"), queue=transport.queues[0])
                await after_failover.wait()

        assert "after" in ran, "the worker went quiet after the failover"
    finally:
        await reader.aclose()
        await control.aclose()
