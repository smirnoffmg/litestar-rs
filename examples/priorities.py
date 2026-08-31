"""Priority queues, and shards for fairness between sources.

Priorities are about *kinds* of work; shards are about *sources*. One tenant
flooding a queue starves everyone else regardless of priority, which is a
different problem and needs a different answer.

    litestar --app examples.priorities:app workers run --queue high --queue low
"""

from __future__ import annotations

import os
from uuid import UUID

from litestar.params import FromPath

from litestar import Litestar, post
from smallage.litestar import QueueConfig, QueuePlugin, TaskRegistry

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

tasks = TaskRegistry()


@tasks.task(queue="high")
async def send_password_reset(user_id: UUID) -> None:
    print(f"password reset for {user_id}")


@tasks.task(queue="low")
async def rebuild_search_index(user_id: UUID) -> None:
    print(f"reindexing everything for {user_id}")


plugin = QueuePlugin(
    QueueConfig(
        registry=tasks,
        redis_url=REDIS_URL,
        namespace="example-priorities",
        # Highest first. A worker sweeps them without blocking, in this order,
        # and only blocks on all of them when every one comes back empty.
        queues=("high", "low"),
        # Each queue spread over four streams. A job lands on one by a hash of
        # its id, so one noisy tenant occupies one shard rather than the queue.
        shards=4,
        # Every tenth pass gives the low queue first refusal, which bounds how
        # long anything can sit behind a busy high-priority queue. Set 0 for
        # strict priority and accept the starvation that comes with it.
        fairness_every=10,
    )
)


@post("/users/{user_id:uuid}/password-reset")
async def reset(user_id: FromPath[UUID]) -> str:
    await send_password_reset.enqueue(user_id=user_id)
    return "queued on high"


@post("/users/{user_id:uuid}/reindex")
async def reindex(user_id: FromPath[UUID]) -> str:
    await rebuild_search_index.enqueue(user_id=user_id)
    return "queued on low"


app = Litestar(route_handlers=[reset, reindex], plugins=[plugin])
