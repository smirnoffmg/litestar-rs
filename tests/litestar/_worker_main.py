"""An application started as a real worker, so its lifecycle runs for real.

``litestar workers run`` enters ``Litestar.lifespan()`` outside any ASGI server,
which is not how Litestar's own tests reach it, and a dependency closing over
something the lifespan opened is exactly what a fake cannot falsify. This module
is loaded by ``--app``, which gives it no arguments, so it takes its settings
from the environment.

Every step announces itself on ``LRS_TEST_SIGNAL``, in order.
"""

from __future__ import annotations

import os

from redis.asyncio import Redis

from litestar import Litestar
from smallage.core.worker import WorkerConfig
from smallage.litestar import QueueConfig, QueuePlugin, TaskRegistry

REDIS_URL = os.environ["LRS_TEST_REDIS_URL"]
NAMESPACE = os.environ["LRS_TEST_NAMESPACE"]
SIGNAL_KEY = os.environ["LRS_TEST_SIGNAL"]

tasks = TaskRegistry()

_opened: Redis | None = None
"""What the startup hook opens. None until then, so a worker that skipped the
application's lifecycle fails the task rather than quietly running it."""


async def open_ledger() -> None:
    global _opened
    _opened = Redis.from_url(REDIS_URL)
    await _opened.rpush(SIGNAL_KEY, b"startup")


async def close_ledger() -> None:
    global _opened
    assert _opened is not None
    await _opened.rpush(SIGNAL_KEY, b"shutdown")
    await _opened.aclose()
    _opened = None


async def provide_ledger() -> Redis:
    if _opened is None:
        raise RuntimeError("nothing is open: the application's lifecycle never ran")
    return _opened


@tasks.task
async def record(note: str, ledger: Redis) -> None:
    await ledger.rpush(SIGNAL_KEY, f"handled:{note}".encode())


app = Litestar(
    route_handlers=[],
    dependencies={"ledger": provide_ledger},
    on_startup=[open_ledger],
    on_shutdown=[close_ledger],
    plugins=[
        QueuePlugin(
            QueueConfig(
                registry=tasks,
                redis_url=REDIS_URL,
                namespace=NAMESPACE,
                block_ms=100,
                worker=WorkerConfig(
                    concurrency=1,
                    min_idle_ms=0,
                    reclaim_interval_s=0.05,
                    trim_interval_s=60.0,
                    scheduler_interval_s=0.05,
                ),
            )
        )
    ],
)
