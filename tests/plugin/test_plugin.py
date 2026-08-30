"""The plugin end to end: declare a task, post a request, run it in a worker."""

from collections.abc import AsyncIterator
from uuid import UUID, uuid4

import anyio
import pytest
from litestar import Litestar, post
from litestar.di import Provide
from litestar.testing import AsyncTestClient
from redis.asyncio import Redis

from litestar_rs.core.keys import stream_key
from litestar_rs.core.testing import worker_running
from litestar_rs.core.worker import WorkerConfig
from litestar_rs.plugin.config import QueueConfig
from litestar_rs.plugin.plugin import QueuePlugin
from litestar_rs.plugin.registry import TaskRegistry

pytestmark = pytest.mark.integration

RAN: list[tuple[UUID, str]] = []
DONE = anyio.Event()


def make_app(redis_url: str, namespace: str) -> tuple[Litestar, QueuePlugin]:
    registry = TaskRegistry()

    @registry.task
    async def reindex(doc_id: UUID, session: str) -> None:
        RAN.append((doc_id, session))
        DONE.set()

    async def session() -> AsyncIterator[str]:
        yield "session"

    plugin = QueuePlugin(
        QueueConfig(
            registry=registry,
            redis_url=redis_url,
            namespace=namespace,
            block_ms=100,
            worker=WorkerConfig(
                concurrency=2,
                min_idle_ms=0,
                reclaim_interval_s=0.05,
                trim_interval_s=60.0,
                scheduler_interval_s=0.05,
            ),
        )
    )

    @post("/documents")
    async def create() -> str:
        await reindex.enqueue(doc_id=DOC_ID)
        return "queued"

    app = Litestar(
        route_handlers=[create],
        dependencies={"session": Provide(session)},
        plugins=[plugin],
    )
    return app, plugin


DOC_ID = uuid4()


async def test_a_request_puts_a_job_on_the_stream(
    redis_url: str, namespace: str
) -> None:
    """Through the app: a handler enqueues, and the entry is really in Redis."""
    app, _ = make_app(redis_url, namespace)

    async with AsyncTestClient(app=app) as client:
        assert (await client.post("/documents")).status_code == 201

    client_redis: Redis = Redis.from_url(redis_url)
    try:
        assert await client_redis.xlen(stream_key(namespace, "default", 0)) == 1
    finally:
        await client_redis.aclose()


async def test_a_worker_runs_the_task_with_its_dependencies(
    redis_url: str, namespace: str
) -> None:
    """The worker side, in this loop: decode, inject, call, tear down."""
    global DONE
    RAN.clear()
    DONE = anyio.Event()
    _, plugin = make_app(redis_url, namespace)
    registry = plugin.config.registry

    async with plugin.connected(consumer="test-worker"):
        await registry.enqueue("reindex", {"doc_id": DOC_ID})
        with anyio.fail_after(30):
            async with worker_running(
                plugin.transport,
                registry.handlers(),
                plugin.config.worker,
                scheduler=plugin.scheduler,
            ):
                await DONE.wait()

    assert RAN == [(DOC_ID, "session")]


async def test_health_reports_the_group(redis_url: str, namespace: str) -> None:
    app, _ = make_app(redis_url, namespace)

    async with AsyncTestClient(app=app) as client:
        response = await client.get("/health/queue")

    assert response.status_code == 200
    body = response.json()
    assert body["namespace"] == namespace
    assert body["group"] == "workers"
    assert body["queues"] == ["default"]
    assert body["lag"] == 0
