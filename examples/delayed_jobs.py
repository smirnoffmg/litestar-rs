"""Work that should happen later, but only once.

Cron is for a calendar; this is for "twenty-four hours after this particular
thing happened". Both ride the same ZSET and the same leader, so there is no
scheduler process either way.

    litestar --app examples.delayed_jobs:app workers run
"""

from __future__ import annotations

import os
from uuid import UUID

from litestar.params import FromPath

from litestar import Litestar, post
from smallage import Envelope, JsonCodec
from smallage.litestar import QueueConfig, QueuePlugin, TaskRegistry

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

tasks = TaskRegistry()
codec = JsonCodec()


@tasks.task
async def send_reminder(user_id: UUID) -> None:
    print(f"reminding {user_id}")


plugin = QueuePlugin(
    QueueConfig(registry=tasks, redis_url=REDIS_URL, namespace="example-delayed")
)


@post("/users/{user_id:uuid}/signed-up")
async def signed_up(user_id: FromPath[UUID]) -> str:
    """Remind them in a day.

    The delay is measured by Redis, not by this process: clock skew between pods
    would otherwise send reminders early or late.
    """
    await plugin.scheduler.schedule_in(
        Envelope(
            id=f"reminder:{user_id}",
            task="send_reminder",
            payload=codec.encode({"user_id": str(user_id)}),
            enqueued_at=await plugin.scheduler.now_ms(),
        ),
        queue="default",
        delay_ms=24 * 60 * 60 * 1000,
    )
    return "reminder scheduled"


@post("/users/{user_id:uuid}/signed-up-at-noon")
async def signed_up_at_noon(user_id: FromPath[UUID]) -> str:
    """Or at an instant you compute yourself."""
    tomorrow_noon = await plugin.scheduler.now_ms() + 12 * 60 * 60 * 1000
    await plugin.scheduler.schedule_at(
        Envelope(
            id=f"noon:{user_id}",
            task="send_reminder",
            payload=codec.encode({"user_id": str(user_id)}),
            enqueued_at=await plugin.scheduler.now_ms(),
        ),
        queue="default",
        when_ms=tomorrow_noon,
        # A stable id means scheduling it twice replaces rather than duplicates.
        scheduled_id=f"noon:{user_id}",
    )
    return "reminder scheduled"


app = Litestar(route_handlers=[signed_up, signed_up_at_noon], plugins=[plugin])
