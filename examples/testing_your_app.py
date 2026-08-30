"""Testing an application that uses the queue.

Three tools, for three different questions:

- did my handler queue the work?          CollectingEnqueuer
- does my task do the right thing?        EagerEnqueuer
- does the whole thing work end to end?   worker_running

Run these with: pytest examples/testing_your_app.py
"""

from __future__ import annotations

import os
from uuid import UUID, uuid4

import anyio
import pytest
from litestar.di import Provide

from litestar_rs import CollectingEnqueuer, EagerEnqueuer
from litestar_rs.plugin import TaskRegistry

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

tasks = TaskRegistry()
INDEXED: list[UUID] = []
DONE = anyio.Event()


class SearchClient:
    def index(self, doc_id: UUID) -> None:
        INDEXED.append(doc_id)


def search_client() -> SearchClient:
    return SearchClient()


@tasks.task
async def reindex(doc_id: UUID, search_client: SearchClient) -> None:
    search_client.index(doc_id)
    DONE.set()


DEPENDENCIES = {"search_client": Provide(search_client, sync_to_thread=False)}


async def handle_upload(doc_id: UUID) -> None:
    """Stands in for the request handler under test."""
    await reindex.enqueue(doc_id=doc_id)


async def test_the_request_queues_the_work() -> None:
    """No Redis, no worker: only the question of whether it was asked for."""
    enqueuer = CollectingEnqueuer()
    tasks.bind(DEPENDENCIES, enqueuer=enqueuer)

    await handle_upload(uuid4())

    enqueuer.assert_enqueued("reindex")
    enqueuer.assert_not_enqueued("purge")


async def test_the_task_does_its_job() -> None:
    """Eager mode runs it inline, with the real dependencies injected.

    Nothing about retries, ordering or concurrency is reproduced, which is the
    point: that belongs in an integration test, and this stays cheap.
    """
    INDEXED.clear()
    doc_id = uuid4()
    tasks.bind(DEPENDENCIES, enqueuer=EagerEnqueuer(tasks.handlers()))

    await handle_upload(doc_id)

    assert [doc_id] == INDEXED


@pytest.mark.skipif(
    os.environ.get("REDIS_URL") is None, reason="needs a Redis to talk to"
)
async def test_a_real_worker_runs_it() -> None:
    """The end-to-end version, against a real Redis and a real worker.

    Leaving the block asks the worker to drain, so in-flight work finishes
    instead of vanishing mid-assertion.
    """
    from litestar_rs import worker_running
    from litestar_rs.plugin import QueueConfig, QueuePlugin

    global DONE
    INDEXED.clear()
    DONE = anyio.Event()
    doc_id = uuid4()
    plugin = QueuePlugin(
        QueueConfig(
            registry=tasks,
            redis_url=REDIS_URL,
            namespace=f"example-testing-{uuid4().hex[:8]}",
            block_ms=100,
        )
    )

    async with plugin.connected(consumer="test-worker"):
        tasks.bind(DEPENDENCIES, enqueuer=plugin)
        await handle_upload(doc_id)

        with anyio.fail_after(30):
            async with worker_running(
                plugin.transport,
                tasks.handlers(),
                plugin.config.worker,
                scheduler=plugin.scheduler,
            ):
                await DONE.wait()

    assert [doc_id] == INDEXED
