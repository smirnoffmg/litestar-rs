"""Work that blocks, and work that must not run forever.

Blocking the event loop does not merely cost throughput: it starves the liveness
refresh, and a worker that is alive but looks dead has its work reclaimed and
done twice. Synchronous tasks therefore run in a thread pool this project sizes
itself, because the asyncio default is a silent bottleneck.

    litestar --app examples.sync_tasks_and_timeouts:app workers run
"""

from __future__ import annotations

import os
import time
from uuid import UUID

import anyio
from litestar.params import FromPath

from litestar import Litestar, post
from smallage.litestar import QueueConfig, QueuePlugin, TaskRegistry

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

tasks = TaskRegistry()


@tasks.task
def render_report(report_id: UUID) -> None:
    """A plain `def`: it runs off the event loop, in the thread pool.

    Anything with a synchronous driver or a CPU-heavy body belongs here.
    """
    time.sleep(0.2)
    print(f"rendered {report_id}")


@tasks.task(timeout_s=30)
async def call_a_slow_api(report_id: UUID) -> None:
    """A timeout cancels at the next await, which async code has and threads do
    not -- so this is where one can be promised."""
    await anyio.sleep(0.1)
    print(f"called out for {report_id}")


# Declaring a timeout on a synchronous task is refused at startup rather than
# quietly ignored: threads cannot be killed, and promising a timeout you cannot
# deliver is worse than not offering one.
#
#     @tasks.task(timeout_s=30)
#     def render_report(report_id: UUID) -> None: ...
#
#     ConfigurationError: task 'render_report' is synchronous and cannot be
#     given a timeout; timeouts are guaranteed for async tasks only

plugin = QueuePlugin(
    QueueConfig(
        registry=tasks,
        redis_url=REDIS_URL,
        namespace="example-sync",
        # Sized here rather than inherited: asyncio's min(32, cpu + 4) is a
        # bottleneck nobody notices until throughput stops scaling.
        thread_limit=8,
    )
)


@post("/reports/{report_id:uuid}")
async def render(report_id: FromPath[UUID]) -> str:
    await render_report.enqueue(report_id=report_id)
    await call_a_slow_api.enqueue(report_id=report_id)
    return "queued"


app = Litestar(route_handlers=[render], plugins=[plugin])
