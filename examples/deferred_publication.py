"""Queueing a job inside a database transaction.

Enqueueing before COMMIT is a race the handler cannot win: the worker is fast
enough to read a row that does not exist yet, and a rollback leaves a job that
has already run. Buffer the jobs and publish them once the transaction is
through.

This is not a transactional outbox -- a crash between the commit and the flush
loses the job. What it removes is the ordering hazard. Work that must survive
that crash needs an outbox in the same database.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import UUID, uuid4

from litestar import Litestar, post

from litestar_rs import DeferredEnqueuer
from litestar_rs.plugin import QueueConfig, QueuePlugin, TaskRegistry

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

tasks = TaskRegistry()


@tasks.task
async def reindex(doc_id: UUID) -> None:
    print(f"reindexing {doc_id}")


@asynccontextmanager
async def unit_of_work(plugin: QueuePlugin) -> AsyncIterator[DeferredEnqueuer]:
    """Hold the jobs until the transaction commits, and drop them if it does not.

    `active()` binds the buffer for the block, so ordinary `task.enqueue(...)`
    calls inside route through it and no call site has to be told about the
    transaction. With SQLAlchemy this is the `after_commit` hook rather than a
    context manager; the shape is the same.
    """
    deferred = DeferredEnqueuer(plugin)
    async with deferred.active():
        try:
            yield deferred
        except Exception:
            deferred.discard()  # rolled back: the jobs were never justified
            raise
    await deferred.flush()  # committed: publish what the transaction earned


plugin = QueuePlugin(
    QueueConfig(registry=tasks, redis_url=REDIS_URL, namespace="example-deferred")
)


@post("/documents")
async def create_document() -> dict[str, str]:
    doc_id = uuid4()
    async with unit_of_work(plugin):
        # ... write the row in the same transaction ...
        await reindex.enqueue(doc_id=doc_id)  # buffered, not published
    return {"queued": str(doc_id)}


app = Litestar(route_handlers=[create_document], plugins=[plugin])
