"""Losing the connection under in-flight work.

This is the failure mode a Sentinel failover produces: the blocking XREADGROUP
does not come back with an error, and a worker that does not notice goes quiet
while its readiness probe keeps saying it is fine. Killing the client
reproduces it without a Sentinel topology.
"""

from collections.abc import Awaitable, Callable
from typing import Any

import anyio
import pytest

from smallage.core.envelope import Envelope
from smallage.core.scheduler import RedisScheduler
from smallage.core.testing import worker_running
from smallage.core.transport import RedisStreamsTransport
from smallage.core.worker import WorkerConfig

pytestmark = [pytest.mark.integration, pytest.mark.slow]

Make = Callable[[str], Awaitable[RedisStreamsTransport]]


def envelope(job_id: str) -> Envelope:
    return Envelope(id=job_id, task="reindex", payload=b"{}", enqueued_at=1712345678901)


def config() -> WorkerConfig:
    return WorkerConfig(
        concurrency=2,
        min_idle_ms=0,
        reclaim_interval_s=0.05,
        trim_interval_s=60.0,
        scheduler_interval_s=0.05,
    )


async def test_a_worker_keeps_consuming_after_its_connection_is_killed(
    transport: RedisStreamsTransport, scheduler: RedisScheduler
) -> None:
    done: list[str] = []
    first = anyio.Event()
    second = anyio.Event()

    async def handler(env: Envelope) -> None:
        done.append(env.id)
        (first if len(done) == 1 else second).set()

    await transport.enqueue(envelope("before"), queue=transport.queues[0])

    with anyio.fail_after(60):
        async with worker_running(
            transport, {"reindex": handler}, config(), scheduler=scheduler
        ):
            await first.wait()

            # What a failover looks like from here: the socket goes away while
            # the reader is parked in a blocking read.
            killed: Any = await transport.control.execute_command(  # type: ignore[no-untyped-call]  # redis-py leaves this untyped
                "CLIENT", "KILL", "TYPE", "normal", "SKIPME", "yes"
            )
            assert int(killed) > 0, "nothing was killed, the test proves nothing"

            await transport.enqueue(envelope("after"), queue=transport.queues[0])
            await second.wait()

    assert done[0] == "before"
    assert "after" in done, "the worker went quiet after losing its connection"
    # A job may well run twice here: the connection can die between the handler
    # returning and its XACK landing. That is at-least-once behaving as promised,
    # not a defect, which is why the guarantee is the README's first paragraph.
