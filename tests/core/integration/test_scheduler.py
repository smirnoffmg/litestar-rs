"""Scheduler against a real Redis.

Due times are set in the past rather than waited for, so nothing here sleeps.
"""

from collections.abc import Awaitable, Callable

import anyio
import pytest

from smallage.core.cron import CronJob, occurrence_envelope, occurrence_id
from smallage.core.envelope import Envelope, from_fields
from smallage.core.keys import sched_job_key, stream_key
from smallage.core.scheduler import RedisScheduler
from smallage.core.transport import RedisStreamsTransport

pytestmark = pytest.mark.integration

MakeScheduler = Callable[[], Awaitable[RedisScheduler]]


def envelope(job_id: str = "job-1", payload: bytes = b'{"doc_id":1}') -> Envelope:
    return Envelope(
        id=job_id, task="reindex", payload=payload, enqueued_at=1712345678901
    )


async def test_due_job_is_promoted(
    scheduler: RedisScheduler, transport: RedisStreamsTransport
) -> None:
    now = await scheduler.now_ms()
    job_id = await scheduler.schedule_at(envelope(), queue="default", when_ms=now - 1)

    assert len(await scheduler.promote()) == 1

    assert await scheduler.pending() == 0
    assert (
        await scheduler.control.exists(sched_job_key(scheduler.namespace, job_id)) == 0
    )
    [record] = await transport.read(10)
    assert from_fields(record.fields) == envelope()


async def test_future_job_stays_put(scheduler: RedisScheduler) -> None:
    now = await scheduler.now_ms()
    await scheduler.schedule_at(envelope(), queue="default", when_ms=now + 3_600_000)

    assert await scheduler.promote() == []
    assert await scheduler.pending() == 1


async def test_binary_payload_survives_the_hop(
    scheduler: RedisScheduler, transport: RedisStreamsTransport
) -> None:
    """The entry rides in a hash precisely so payloads are not encoded twice."""
    payload = b"\x00\xff\x1b not text at all"
    now = await scheduler.now_ms()
    await scheduler.schedule_at(
        envelope(payload=payload), queue="default", when_ms=now - 1
    )
    await scheduler.promote()

    [record] = await transport.read(10)
    assert record.fields[b"payload"] == payload
    assert b"_stream" not in record.fields


async def test_two_leaders_promote_a_job_once(
    schedulers: MakeScheduler, transport: RedisStreamsTransport
) -> None:
    """Split brain: both believe they lead, and the job is still enqueued once."""
    first = await schedulers()
    now = await first.now_ms()
    await first.schedule_at(envelope(), queue="default", when_ms=now - 1)

    peers = [await schedulers() for _ in range(4)]
    moved: list[int] = []

    async def promote(peer: RedisScheduler) -> None:
        moved.append(len(await peer.promote()))

    async with anyio.create_task_group() as tg:
        for peer in peers:
            tg.start_soon(promote, peer)

    assert sum(moved) == 1
    assert await first.pending() == 0
    assert await transport.control.xlen(stream_key(first.namespace, "default", 0)) == 1


async def test_leadership_is_exclusive(schedulers: MakeScheduler) -> None:
    first = await schedulers()
    second = await schedulers()

    assert await first.hold_leadership("token-a", ttl_ms=30_000) is True
    assert await second.hold_leadership("token-b", ttl_ms=30_000) is False

    # Renewing is compare-and-set: a worker that lost the lease must not be able
    # to keep extending the holder's key.
    assert await first.hold_leadership("token-a", ttl_ms=30_000) is True
    assert await second.release_leadership("token-b") is False

    assert await first.release_leadership("token-a") is True
    assert await second.hold_leadership("token-b", ttl_ms=30_000) is True


async def test_scheduled_job_is_unaffected_by_a_stream_trim(
    scheduler: RedisScheduler, transport: RedisStreamsTransport
) -> None:
    """Trimming the stream must never be able to drop pending scheduled work."""
    now = await scheduler.now_ms()
    await scheduler.schedule_at(envelope(), queue="default", when_ms=now - 1)

    await transport.trim(retention_ms=0)
    assert await scheduler.pending() == 1

    await scheduler.promote()
    [record] = await transport.read(10)
    assert from_fields(record.fields).id == "job-1"


async def test_cron_scheduling_is_idempotent_across_leaders(
    schedulers: MakeScheduler,
) -> None:
    """The occurrence id encodes the instant, so a second leader writes no dupe."""
    job = CronJob(name="nightly", expression="*/5 * * * *", task="reindex")
    first = await schedulers()
    second = await schedulers()

    [job_id] = await first.schedule_cron([job])
    assert await second.schedule_cron([job]) == [job_id]

    assert await first.pending() == 1
    score = await first.control.zscore(first.zset, job_id)
    assert score is not None
    assert job_id == occurrence_id(job, int(score))


async def test_schedule_in_uses_the_redis_clock(scheduler: RedisScheduler) -> None:
    """Delays are measured by Redis, so pods with drifting clocks agree."""
    await scheduler.schedule_in(envelope(), queue="default", delay_ms=3_600_000)
    assert await scheduler.promote() == []

    score = await scheduler.control.zscore(scheduler.zset, "job-1")
    assert score is not None
    assert score - await scheduler.now_ms() > 3_000_000


def every_five_minutes() -> CronJob:
    return CronJob(name="every-5", expression="*/5 * * * *", task="reindex")


async def missed_an_hour_ago(scheduler: RedisScheduler, job: CronJob) -> Envelope:
    """Write the occurrence a leader would have written before the fleet died."""
    due = await scheduler.now_ms() - 3_600_000
    envelope = occurrence_envelope(job, due)
    await scheduler.schedule_at(
        envelope, queue=job.queue, when_ms=due, scheduled_id=envelope.id
    )
    return envelope


async def test_a_missed_occurrence_runs_late_rather_than_being_lost(
    scheduler: RedisScheduler, transport: RedisStreamsTransport
) -> None:
    """The due time rides along, so a late run is recognisable as late."""
    job = every_five_minutes()
    envelope = await missed_an_hour_ago(scheduler, job)

    assert len(await scheduler.promote()) == 1

    [record] = await transport.read(10)
    assert from_fields(record.fields).enqueued_at == envelope.enqueued_at


async def test_an_outage_worth_of_occurrences_collapses_into_one(
    scheduler: RedisScheduler,
) -> None:
    """Twelve occurrences came and went; the fleet returns and runs one."""
    job = every_five_minutes()
    envelope = await missed_an_hour_ago(scheduler, job)
    await scheduler.promote()

    [resumed] = await scheduler.schedule_cron([job])

    assert resumed != envelope.id
    assert await scheduler.pending() == 1
    score = await scheduler.control.zscore(scheduler.zset, resumed)
    assert score is not None
    assert score > await scheduler.now_ms()
