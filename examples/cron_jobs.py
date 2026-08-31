"""Scheduled work, and how to tell a late run from a punctual one.

There is no scheduler process: whichever worker holds the lease promotes due
jobs. Run two workers and one of them will do it.
"""

from __future__ import annotations

import os
import time

from litestar import Litestar
from smallage import CronJob
from smallage.litestar import QueueConfig, QueuePlugin, TaskRegistry

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

tasks = TaskRegistry()


@tasks.task
async def nightly_rollup() -> None:
    print("rolling up yesterday")


@tasks.task
async def expire_sessions() -> None:
    print("expiring sessions")


NIGHTLY = CronJob(
    name="nightly-rollup",
    expression="30 2 * * *",
    task="nightly_rollup",
    # Resolved in this zone, so the spring-forward day is not skipped and the
    # autumn repeat does not fire twice.
    timezone="Europe/Berlin",
)

HOUSEKEEPING = CronJob(
    name="expire-sessions",
    expression="*/15 * * * *",
    task="expire_sessions",
)

app = Litestar(
    route_handlers=[],
    plugins=[
        QueuePlugin(
            QueueConfig(
                registry=tasks,
                redis_url=REDIS_URL,
                namespace="example-cron",
                cron=[NIGHTLY, HOUSEKEEPING],
            )
        )
    ],
)


def lateness_ms(enqueued_at: int) -> int:
    """How far behind its schedule a run is.

    `enqueued_at` is the instant the occurrence was due, not the instant it
    reached the stream, which is what makes this meaningful after an outage.
    """
    return time.time_ns() // 1_000_000 - enqueued_at
