"""Helpers for testing applications that use this queue.

A queue that is hard to test is a queue people work around. These are part of
the library rather than an appendix in the docs for exactly that reason.
"""

from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from functools import partial

import anyio

from litestar_rs.core.cron import CronJob
from litestar_rs.core.envelope import Envelope
from litestar_rs.core.errors import LitestarRsError
from litestar_rs.core.protocols import Scheduler, StreamTransport, TaskHandler
from litestar_rs.core.worker import WorkerConfig, run


class UnknownTask(LitestarRsError):
    """Eager mode was asked to run a task that is not registered."""


class CollectingEnqueuer:
    """Records what would have been enqueued, and runs nothing.

    For asserting that a request scheduled the work it was supposed to, without
    dragging a worker or a Redis into the test.
    """

    def __init__(self) -> None:
        self.enqueued: list[tuple[Envelope, str]] = []

    async def enqueue(self, envelope: Envelope, *, queue: str) -> bytes:
        self.enqueued.append((envelope, queue))
        return f"collected-{len(self.enqueued)}".encode()

    def tasks(self) -> list[str]:
        return [envelope.task for envelope, _ in self.enqueued]

    def assert_enqueued(self, task: str, *, times: int = 1) -> None:
        actual = self.tasks().count(task)
        if actual != times:
            raise AssertionError(
                f"expected {task!r} to be enqueued {times} time(s), "
                f"got {actual}; enqueued: {self.tasks()}"
            )

    def assert_not_enqueued(self, task: str) -> None:
        self.assert_enqueued(task, times=0)


class EagerEnqueuer:
    """Runs each task inline, at the moment it is enqueued.

    Nothing about retries, ordering or concurrency is reproduced -- that is the
    point. It keeps unit tests of application code free of infrastructure, and
    anything that depends on real queue behaviour belongs in an integration test.
    """

    def __init__(self, registry: Mapping[str, TaskHandler]) -> None:
        self.registry = registry
        self.enqueued: list[tuple[Envelope, str]] = []

    async def enqueue(self, envelope: Envelope, *, queue: str) -> bytes:
        handler = self.registry.get(envelope.task)
        if handler is None:
            raise UnknownTask(f"no handler registered for task {envelope.task!r}")
        self.enqueued.append((envelope, queue))
        await handler(envelope)
        return f"eager-{len(self.enqueued)}".encode()


@asynccontextmanager
async def worker_running(
    transport: StreamTransport,
    registry: Mapping[str, TaskHandler],
    config: WorkerConfig | None = None,
    *,
    scheduler: Scheduler,
    cron: Sequence[CronJob] = (),
) -> AsyncIterator[None]:
    """Run a real worker for the duration of the block, then stop it cleanly.

    Wrap it in a fixture for integration tests: leaving the block asks the worker
    to drain, so in-flight work finishes instead of vanishing mid-assertion.
    """
    stop = anyio.Event()
    async with anyio.create_task_group() as tg:
        tg.start_soon(
            partial(
                run,
                transport,
                registry,
                config,
                scheduler=scheduler,
                cron=cron,
                shutdown=stop,
            )
        )
        try:
            yield
        finally:
            stop.set()
