"""The plugin end to end: declare a task, post a request, run it in a worker."""

import os
import signal
import sys
from collections.abc import AsyncIterator
from pathlib import Path
from uuid import UUID, uuid4

import anyio
import pytest
from litestar import Litestar, get, post
from litestar.di import Provide
from litestar.testing import AsyncTestClient
from redis.asyncio import Redis

from litestar_rs import Envelope, Record, RedisStreamsTransport
from litestar_rs.core.errors import ConfigurationError
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

ROOT = Path(__file__).resolve().parents[2]
WORKER_APP = "tests.plugin._worker_main:app"


@pytest.mark.slow
async def test_a_worker_process_runs_the_applications_own_lifecycle(
    redis_url: str, namespace: str
) -> None:
    """The worker command against a real application, in a process of its own.

    `app.lifespan()` is entered outside any ASGI server here, and the task's
    `ledger` dependency resolves to something usable only if the startup hook
    really ran -- which is the failure a fake cannot reproduce. The order of the
    signals is the assertion: opened, handled, and only then shut down.
    """
    signal_key = f"{{{namespace}}}:signal"
    reader: Redis = Redis.from_url(redis_url, socket_timeout=30.0)
    control: Redis = Redis.from_url(redis_url)

    async def signalled() -> bytes:
        taken = await control.blpop([signal_key], timeout=60)
        assert taken is not None, "the worker went quiet"
        value = taken[1]
        assert isinstance(value, bytes)
        return value

    try:
        seed = RedisStreamsTransport(
            reader=reader, control=control, consumer="seed", namespace=namespace
        )
        await seed.ensure_group()
        await seed.enqueue(
            Envelope(
                id="job-1",
                task="record",
                payload=b'{"note":"n1"}',
                enqueued_at=1712345678901,
            ),
            queue="default",
        )

        worker = await anyio.open_process(
            [
                sys.executable,
                "-m",
                "litestar",
                "--app",
                WORKER_APP,
                "workers",
                "run",
                "--consumer",
                "w-1",
            ],
            cwd=str(ROOT),
            env={
                **os.environ,
                "LRS_TEST_REDIS_URL": redis_url,
                "LRS_TEST_NAMESPACE": namespace,
                "LRS_TEST_SIGNAL": signal_key,
            },
        )
        with anyio.fail_after(120):
            assert await signalled() == b"startup"
            assert await signalled() == b"handled:n1"
            worker.send_signal(signal.SIGTERM)
            await worker.wait()
            assert await signalled() == b"shutdown"

        assert worker.returncode == 0
    finally:
        await reader.aclose()
        await control.aclose()


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
    # A web process runs no tasks, so its own counters are all zero -- which is
    # the honest answer, not a missing section.
    assert body["stats"]["handled"] == 0
    assert body["stats"]["unknown_task"] == 0


def test_the_worker_serves_the_same_health_route(
    redis_url: str, namespace: str
) -> None:
    """A readiness probe must ask a worker exactly what it asks a web process.

    Structural on purpose: the body is already asserted through the web app, and
    what could drift is the worker serving a lookalike route instead of this one.
    """
    app, plugin = make_app(redis_url, namespace)
    worker_app = plugin.health_app()

    served = {route.path for route in worker_app.routes}
    assert served == {plugin.config.health_path}
    assert plugin.config.health_path in {route.path for route in app.routes}


def test_using_the_queue_before_it_opens_says_so(
    redis_url: str, namespace: str
) -> None:
    """Connections open with the application, and an attribute error would not
    explain why."""
    _, plugin = make_app(redis_url, namespace)

    for attribute in ("transport", "scheduler", "results"):
        with pytest.raises(ConfigurationError, match="not connected yet"):
            getattr(plugin, attribute)


def test_the_plugin_extends_the_cli(redis_url: str, namespace: str) -> None:
    import click

    _, plugin = make_app(redis_url, namespace)
    group = click.Group()

    plugin.on_cli_init(group)

    assert "workers" in group.commands


async def test_a_foreign_entry_reaches_its_handler_through_the_plugin(
    redis_url: str, namespace: str
) -> None:
    """The wiring, end to end.

    The handler is registered on the config and nowhere else, exactly as an
    application would do it, and a real entry on the foreign stream has to reach
    it -- which is the part that was decorative before.
    """
    seen: list[bytes] = []
    arrived = anyio.Event()
    foreign = f"{{{namespace}}}:orders"

    async def on_order(record: Record) -> None:
        seen.append(record.fields[b"sku"])
        arrived.set()

    registry = TaskRegistry()
    plugin = QueuePlugin(
        QueueConfig(
            registry=registry,
            redis_url=redis_url,
            namespace=namespace,
            block_ms=100,
            brokers={foreign: on_order},
            worker=WorkerConfig(
                concurrency=2,
                min_idle_ms=0,
                reclaim_interval_s=0.05,
                trim_interval_s=60.0,
                scheduler_interval_s=0.05,
            ),
        )
    )

    async with plugin.connected(consumer="test-worker"):
        registry.bind({}, enqueuer=plugin)
        await plugin.transport.control.xadd(foreign, {b"sku": b"A1"})

        with anyio.fail_after(30):
            async with worker_running(
                plugin.transport,
                registry.handlers(),
                plugin.config.worker,
                scheduler=plugin.scheduler,
                brokers=plugin.config.brokers,
            ):
                await arrived.wait()

    assert seen == [b"A1"]


async def test_the_cli_passes_the_worker_everything_it_takes(
    redis_url: str, namespace: str
) -> None:
    """Broker handlers were configurable and then dropped on the way to the worker.

    Anything optional the worker grows must be wired here, and this is what says
    so before somebody debugs a handler that never runs.
    """
    import inspect

    from litestar_rs.core.worker import run_with_signals
    from litestar_rs.plugin.cli import worker_arguments

    _, plugin = make_app(redis_url, namespace)

    async with plugin.connected(consumer="test-worker"):
        wired = set(worker_arguments(plugin, plugin.config))

    accepted = {
        name
        for name, parameter in inspect.signature(run_with_signals).parameters.items()
        if parameter.kind is inspect.Parameter.KEYWORD_ONLY
    }

    assert wired == accepted, f"the CLI never passes: {sorted(accepted - wired)}"


def test_a_path_already_taken_fails_at_startup(redis_url: str, namespace: str) -> None:
    """Loudly, rather than one handler quietly shadowing the other."""
    from litestar.exceptions import ImproperlyConfiguredException

    @get("/health/queue")
    async def theirs() -> str:
        return "theirs"

    plugin = QueuePlugin(
        QueueConfig(registry=TaskRegistry(), redis_url=redis_url, namespace=namespace)
    )

    with pytest.raises(ImproperlyConfiguredException, match="already registered"):
        Litestar(route_handlers=[theirs], plugins=[plugin])


def test_turning_the_health_route_off_registers_nothing(
    redis_url: str, namespace: str
) -> None:
    @get("/health/queue")
    async def theirs() -> str:
        return "theirs"

    plugin = QueuePlugin(
        QueueConfig(
            registry=TaskRegistry(),
            redis_url=redis_url,
            namespace=namespace,
            health_path=None,
        )
    )

    app = Litestar(route_handlers=[theirs], plugins=[plugin])

    assert "/health/queue" in {route.path for route in app.routes}
    with pytest.raises(ConfigurationError, match="no health route"):
        plugin.health_app()


async def test_the_probe_fails_when_the_queue_is_unhealthy(
    redis_url: str, namespace: str
) -> None:
    """A readiness probe reads the status code; 200 while unhealthy never fails."""
    app, plugin = make_app(redis_url, namespace)
    stream = stream_key(namespace, "default", 0)

    # A client of this test's own, because the app's belong to the test client's
    # event loop and pools do not cross loops.
    setup: Redis = Redis.from_url(redis_url)
    try:
        await setup.xgroup_create(stream, "workers", id="0", mkstream=True)
        async with AsyncTestClient(app=app) as client:
            healthy = await client.get(plugin.config.health_path or "")
            assert healthy.status_code == 200
            assert healthy.json()["healthy"] is True

        # Redis gives up on lag once entries are deleted before being delivered.
        entries = [await setup.xadd(stream, {b"v": b"1"}) for _ in range(3)]
        await setup.xdel(stream, entries[1])

        async with AsyncTestClient(app=app) as client:
            unhealthy = await client.get(plugin.config.health_path or "")
    finally:
        await setup.aclose()

    assert unhealthy.status_code == 503
    assert unhealthy.json()["lag"] is None
    assert unhealthy.json()["healthy"] is False
