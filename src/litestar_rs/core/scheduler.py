"""Delayed and periodic jobs, run by whichever worker currently holds the lease.

There is no scheduler process. Every worker tries to take a short lease; the one
that has it moves due jobs into their streams. Losing the lease costs nothing --
the next pass by the new holder picks up exactly where this one stopped.
"""

from collections.abc import Sequence
from typing import Any

from redis.asyncio import Redis

from litestar_rs.core.cron import (
    CronJob,
    next_fire_ms,
    occurrence_envelope,
)
from litestar_rs.core.envelope import Envelope, to_fields
from litestar_rs.core.errors import ConfigurationError
from litestar_rs.core.keys import (
    leader_key,
    sched_job_key,
    sched_key,
    stream_for,
    validate_namespace,
    validate_queue,
)
from litestar_rs.core.scripts import SchedulerScripts, register_scheduler

STREAM_FIELD = b"_stream"


class RedisScheduler:
    """Scheduled jobs held in a ZSET, with the payload beside it in a hash.

    The entry lives in a hash rather than inside the ZSET member so payloads stay
    opaque bytes -- packing them into a member would mean encoding them twice.
    """

    def __init__(
        self,
        *,
        control: Redis,
        namespace: str = "lrs",
        shards: int = 1,
    ) -> None:
        if control.connection_pool.connection_kwargs.get("decode_responses"):
            raise ConfigurationError(
                "control client must be built with decode_responses=False; "
                "scheduled payloads are opaque bytes"
            )
        if shards < 1:
            raise ConfigurationError(f"shards must be at least 1, got {shards}")
        self.control = control
        self.namespace = validate_namespace(namespace)
        self.shards = shards
        self.zset = sched_key(self.namespace)
        self.leader = leader_key(self.namespace)
        self._scripts: SchedulerScripts = register_scheduler(control)

    async def now_ms(self) -> int:
        """Redis is the clock. Worker clocks drift apart; this one does not."""
        seconds, microseconds = await self.control.time()
        return seconds * 1000 + microseconds // 1000

    async def schedule_at(
        self,
        envelope: Envelope,
        *,
        queue: str,
        when_ms: int,
        scheduled_id: str | None = None,
    ) -> str:
        job_id = scheduled_id or envelope.id
        stream = stream_for(
            self.namespace, validate_queue(queue), self.shards, envelope.id
        )
        fields: dict[Any, Any] = dict(to_fields(envelope))
        fields[STREAM_FIELD] = stream.encode()
        async with self.control.pipeline(transaction=True) as pipe:
            pipe.hset(sched_job_key(self.namespace, job_id), mapping=fields)
            pipe.zadd(self.zset, {job_id: when_ms})
            await pipe.execute()
        return job_id

    async def schedule_in(
        self, envelope: Envelope, *, queue: str, delay_ms: int
    ) -> str:
        return await self.schedule_at(
            envelope, queue=queue, when_ms=await self.now_ms() + delay_ms
        )

    async def schedule_cron(self, jobs: Sequence[CronJob]) -> list[str]:
        """Put the next occurrence of every job in the ZSET.

        Idempotent by construction: the id encodes the instant, so a second
        leader doing the same work writes the same member.
        """
        now = await self.now_ms()
        scheduled = []
        for job in jobs:
            fire_ms = next_fire_ms(job, now)
            if fire_ms is None:
                continue
            envelope = occurrence_envelope(job, fire_ms)
            await self.schedule_at(
                envelope, queue=job.queue, when_ms=fire_ms, scheduled_id=envelope.id
            )
            scheduled.append(envelope.id)
        return scheduled

    async def promote(self, *, limit: int = 100) -> list[bytes]:
        moved = await self._scripts.promote(
            keys=[self.zset], args=[limit, sched_job_key(self.namespace, "")]
        )
        return [bytes(entry_id) for entry_id in moved]

    async def pending(self) -> int:
        return int(await self.control.zcard(self.zset))

    async def hold_leadership(self, token: str, *, ttl_ms: int) -> bool:
        """Take the lease, or extend it if it is already ours.

        Extending is a compare-and-set: a worker that has already lost the lease
        must not be able to prolong the new holder's key.
        """
        taken = await self.control.set(self.leader, token, nx=True, px=ttl_ms)
        if taken:
            return True
        renewed = await self._scripts.renew_leader(
            keys=[self.leader], args=[token, ttl_ms]
        )
        return bool(renewed)

    async def release_leadership(self, token: str) -> bool:
        released = await self._scripts.release_leader(keys=[self.leader], args=[token])
        return bool(released)
