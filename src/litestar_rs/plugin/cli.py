"""``litestar workers ...``.

The command reads the application the CLI was already invoked with. Asking for
a ``"module:app"`` string in the plugin constructor would make every user solve
an import-ordering problem the CLI has already solved.
"""

from __future__ import annotations

from dataclasses import replace
from functools import partial
from typing import TYPE_CHECKING, Any

import anyio
import click

from litestar_rs.core.worker import run_with_signals
from litestar_rs.plugin.config import QueueConfig
from litestar_rs.plugin.plugin import QueuePlugin

if TYPE_CHECKING:
    from litestar import Litestar


def worker_arguments(plugin: QueuePlugin, config: QueueConfig) -> dict[str, Any]:
    """Everything beyond the transport, the registry and the worker settings.

    Built in one place so that a feature added to the worker and forgotten here
    is caught by a test rather than by somebody wondering why their handlers
    never run.
    """
    return {
        "scheduler": plugin.scheduler,
        "results": plugin.results,
        "brokers": config.brokers,
        "stats": plugin.stats,
        "cron": config.cron,
    }


async def serve_health(plugin: QueuePlugin, host: str, port: int) -> None:
    """Run the plugin's own health route beside the worker."""
    try:
        import uvicorn
    except ImportError as exc:  # pragma: no cover - depends on the install
        raise click.ClickException(
            "--health-port needs a server; install litestar[standard]"
        ) from exc

    server = uvicorn.Server(
        uvicorn.Config(plugin.health_app(), host=host, port=port, log_level="warning")
    )
    await server.serve()


def with_overrides(
    config: QueueConfig,
    *,
    queues: tuple[str, ...],
    concurrency: int | None,
    consumer: str | None,
) -> QueueConfig:
    """Apply command line overrides to the application's own configuration."""
    if queues:
        config = replace(config, queues=queues)
    if concurrency is not None:
        config = replace(config, worker=replace(config.worker, concurrency=concurrency))
    if consumer is not None:
        config = replace(config, consumer=consumer)
    return config


@click.group(name="workers")
def workers() -> None:
    """Run and inspect queue workers."""


def plugin_of(app: Litestar) -> QueuePlugin:
    for plugin in app.plugins:
        if isinstance(plugin, QueuePlugin):
            return plugin
    raise click.ClickException("the application does not register a QueuePlugin")


@workers.command(name="run")
@click.option(
    "--queue", "queues", multiple=True, help="Queues, highest priority first."
)
@click.option("--concurrency", type=int, help="Jobs to run at once.")
@click.option("--consumer", help="Consumer name; must be unique per running worker.")
@click.option("--health-port", type=int, help="Serve the health endpoint here.")
@click.option(
    "--health-host",
    default="0.0.0.0",  # noqa: S104  # a probe reaches a container from outside it
    show_default=True,
    help="Interface for the health endpoint.",
)
@click.pass_obj
def run_workers(
    env: object,
    queues: tuple[str, ...],
    concurrency: int | None,
    consumer: str | None,
    health_port: int | None,
    health_host: str,
) -> None:
    """Consume tasks until interrupted."""
    app: Litestar = env.app  # type: ignore[attr-defined]  # LitestarEnv carries it
    plugin = plugin_of(app)
    config = with_overrides(
        plugin.config, queues=queues, concurrency=concurrency, consumer=consumer
    )
    plugin.config = config

    async def main() -> None:
        # Exactly one of the two, never both: the application's lifespan opens
        # the queue through the plugin, so entering it and connecting as well
        # would leave a worker holding two sets of connections.
        opened = app.lifespan() if config.run_app_lifespan else plugin.connected()
        async with opened, anyio.create_task_group() as tg:
            if health_port is not None:
                tg.start_soon(partial(serve_health, plugin, health_host, health_port))
            await run_with_signals(
                plugin.transport,
                config.registry.handlers(),
                config.worker,
                **worker_arguments(plugin, config),
            )
            tg.cancel_scope.cancel()

    anyio.run(main, backend="asyncio", backend_options={"use_uvloop": True})
