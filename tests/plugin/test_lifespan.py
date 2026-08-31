"""Where in the application's lifecycle the queue is open.

It matters in a worker most: `litestar workers run` enters the application's
lifespan, so whatever the application opens and closes there now runs beside the
queue rather than in a different process from it.
"""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import pytest
from litestar import Litestar

from litestar_rs.core.errors import ConfigurationError
from litestar_rs.core.transport import RedisStreamsTransport
from litestar_rs.plugin import QueueConfig, QueuePlugin, TaskRegistry

pytestmark = pytest.mark.unit


def probing_app(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Litestar, QueuePlugin, list[str]]:
    """An application that records whether the queue was open at each step.

    Only `ensure_group` is replaced -- redis-py connects lazily, so the transport
    is built for real and the plugin's lifespan manager runs as it always does.
    """
    seen: list[str] = []
    plugin = QueuePlugin(QueueConfig(registry=TaskRegistry()))

    async def opened(self: RedisStreamsTransport) -> None:
        pass

    monkeypatch.setattr(RedisStreamsTransport, "ensure_group", opened)

    def note(step: str) -> None:
        try:
            _ = plugin.transport
        except ConfigurationError:
            seen.append(f"{step}: closed")
        else:
            seen.append(f"{step}: open")

    @asynccontextmanager
    async def theirs(app: Litestar) -> AsyncGenerator[None]:
        note("their manager enters")
        yield
        note("their manager exits")

    async def on_startup() -> None:
        note("on_startup")

    async def on_shutdown() -> None:
        note("on_shutdown")

    app = Litestar(
        route_handlers=[],
        lifespan=[theirs],
        on_startup=[on_startup],
        on_shutdown=[on_shutdown],
        plugins=[plugin],
    )
    return app, plugin, seen


async def test_the_queue_outlives_every_manager_the_application_registers(
    anyio_backend: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Registered first so it is the outermost, which is what lets a manager
    enqueue on the way in and drain on the way out. Registered last, it would
    close before any of their exits ran."""
    app, _, seen = probing_app(monkeypatch)

    async with app.lifespan():
        pass

    assert seen[:3] == [
        "their manager enters: open",
        "on_startup: open",
        "their manager exits: open",
    ]


async def test_an_on_shutdown_hook_finds_the_queue_already_closed(
    anyio_backend: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Litestar pushes `on_shutdown` onto its exit stack before any context
    manager, so those hooks run last however the queue is registered. Documented
    rather than worked around: shutdown work that needs the queue belongs in a
    lifespan manager, whose exit does have it."""
    app, _, seen = probing_app(monkeypatch)

    async with app.lifespan():
        pass

    assert seen[-1] == "on_shutdown: closed"
