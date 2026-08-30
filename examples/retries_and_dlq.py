"""What happens to work that keeps failing, and how to get it back.

Two counters, deliberately apart: application failures raise `attempt`, while
reclaims -- an entry taken from a worker that stopped refreshing its liveness
key -- have a ceiling of their own. Mixing them buries healthy work after a few
deploys.

    litestar --app examples.retries_and_dlq:app workers run
    python -m examples.retries_and_dlq            # read and replay the DLQ
"""

from __future__ import annotations

import os
from typing import Any
from uuid import UUID

import anyio
import msgspec.structs
from litestar import Litestar, post
from litestar.params import FromPath
from redis.asyncio import Redis

from litestar_rs import RetryPolicy, WorkerConfig, dlq_key, from_fields
from litestar_rs.plugin import QueueConfig, QueuePlugin, TaskRegistry

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
NAMESPACE = "example-retries"

tasks = TaskRegistry()
ATTEMPTS: list[int] = []


@tasks.task
async def flaky(doc_id: UUID) -> None:
    """Fails twice, then succeeds -- the ordinary case retries exist for."""
    ATTEMPTS.append(len(ATTEMPTS))
    if len(ATTEMPTS) < 3:
        raise RuntimeError(f"attempt {len(ATTEMPTS)} failed")


@tasks.task
async def doomed(doc_id: UUID) -> None:
    """Never succeeds, so it ends up in the dead letter queue."""
    raise ValueError("this will never work")


plugin = QueuePlugin(
    QueueConfig(
        registry=tasks,
        redis_url=REDIS_URL,
        namespace=NAMESPACE,
        worker=WorkerConfig(
            retry=RetryPolicy(
                # Application failures. Three attempts, then the DLQ.
                max_attempts=3,
                # Reclaims. An entry taken from this many dead owners is killing
                # whatever picks it up, so it goes straight to the DLQ.
                max_deliveries=5,
                initial_backoff_ms=500,
                max_backoff_ms=30_000,
                # On by default: without it a batch that failed together retries
                # together, forever.
                jitter=0.2,
            )
        ),
    )
)


@post("/documents/{doc_id:uuid}/flaky")
async def queue_flaky(doc_id: FromPath[UUID]) -> str:
    await flaky.enqueue(doc_id=doc_id)
    return "queued"


@post("/documents/{doc_id:uuid}/doomed")
async def queue_doomed(doc_id: FromPath[UUID]) -> str:
    await doomed.enqueue(doc_id=doc_id)
    return "queued"


app = Litestar(route_handlers=[queue_flaky, queue_doomed], plugins=[plugin])


async def inspect_and_replay() -> None:
    """Read the dead letter queue, then put one job back.

    The original payload is untouched, so replaying is re-enqueueing it. Reset
    `attempt` unless the replay should inherit the exhausted budget.
    """
    control: Redis = Redis.from_url(REDIS_URL)
    try:
        entries: Any = await control.xrange(dlq_key(NAMESPACE), count=100)
        if not entries:
            print("dead letter queue is empty")
            return

        for entry_id, fields in entries:
            print(
                f"{entry_id.decode()} "
                f"task={fields[b'task'].decode()} "
                f"reason={fields[b'dlq_reason'].decode()} "
                f"deliveries={fields[b'dlq_deliveries'].decode()}"
            )
            print(f"    history: {fields.get(b'history', b'-').decode()}")
            print(f"    last error: {fields[b'dlq_detail'].decode().splitlines()[-1]}")

        _, first = entries[0]
        envelope = from_fields(first)
        async with plugin.connected(consumer="replay"):
            await plugin.transport.enqueue(
                msgspec.structs.replace(envelope, attempt=0, history=()),
                queue="default",
            )
        print(f"replayed {envelope.id}")
    finally:
        await control.aclose()


if __name__ == "__main__":
    anyio.run(inspect_and_replay)
