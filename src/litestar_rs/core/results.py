"""Storing the outcome of a job for whoever asked for it.

Results are opt-in per job. Most work is enqueued and forgotten; keeping an
outcome for every one of those would be a key and a TTL spent on nobody.
"""

from __future__ import annotations

from typing import Any

import msgspec
from redis.asyncio import Redis

from litestar_rs.core.envelope import TaskResult
from litestar_rs.core.errors import ConfigurationError
from litestar_rs.core.keys import result_key, result_wait_key, validate_namespace


class RedisResultStore:
    """Results in plain keys, with a list alongside so a waiter can block.

    The list exists because polling for a result is the thing every queue gets
    asked to stop doing. A waiter blocks on it and puts the token back, so
    several waiters on one job all wake.
    """

    def __init__(
        self, *, control: Redis, namespace: str = "lrs", default_ttl_ms: int = 3_600_000
    ) -> None:
        if control.connection_pool.connection_kwargs.get("decode_responses"):
            raise ConfigurationError(
                "control client must be built with decode_responses=False; "
                "results are opaque bytes"
            )
        if default_ttl_ms < 1:
            raise ConfigurationError(
                f"default_ttl_ms must be positive, got {default_ttl_ms}"
            )
        self.control = control
        self.namespace = validate_namespace(namespace)
        self.default_ttl_ms = default_ttl_ms

    async def store(
        self, job_id: str, result: TaskResult, *, ttl_ms: int | None = None
    ) -> None:
        ttl = ttl_ms or self.default_ttl_ms
        async with self.control.pipeline(transaction=True) as pipe:
            pipe.set(
                result_key(self.namespace, job_id),
                msgspec.msgpack.encode(result),
                px=ttl,
            )
            pipe.lpush(result_wait_key(self.namespace, job_id), b"1")
            pipe.pexpire(result_wait_key(self.namespace, job_id), ttl)
            await pipe.execute()

    async def get(self, job_id: str) -> TaskResult | None:
        raw: Any = await self.control.get(result_key(self.namespace, job_id))
        if raw is None:
            return None
        return msgspec.msgpack.decode(raw, type=TaskResult)

    async def wait(self, job_id: str, *, timeout_s: float) -> TaskResult | None:
        """Block until the job finishes, or give up. Returns None on timeout."""
        stored = await self.get(job_id)
        if stored is not None:
            return stored
        woken = await self.control.blpop(
            [result_wait_key(self.namespace, job_id)], timeout=timeout_s
        )
        if woken is None:
            return None
        # Put the token back so a second waiter on the same job also wakes.
        await self.control.rpush(result_wait_key(self.namespace, job_id), b"1")
        return await self.get(job_id)
