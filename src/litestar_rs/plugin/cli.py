"""``litestar workers ...``.

The command reads the application the CLI was already invoked with. Asking for
a ``"module:app"`` string in the plugin constructor would make every user solve
an import-ordering problem the CLI has already solved.
"""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

import anyio
import click

from litestar_rs.core.worker import run_with_signals
from litestar_rs.plugin.config import QueueConfig
from litestar_rs.plugin.plugin import QueuePlugin

if TYPE_CHECKING:
    from litestar import Litestar


def with_overrides(
    config: QueueConfig, *, queues: tuple[str, ...], concurrency: int | None
) -> QueueConfig:
    """Apply command line overrides to the application's own configuration."""
    if queues:
        config = replace(config, queues=queues)
    if concurrency is not None:
        config = replace(config, worker=replace(config.worker, concurrency=concurrency))
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
@click.pass_obj
def run_workers(
    env: object, queues: tuple[str, ...], concurrency: int | None, consumer: str | None
) -> None:
    """Consume tasks until interrupted."""
    app: Litestar = env.app  # type: ignore[attr-defined]  # LitestarEnv carries it
    plugin = plugin_of(app)
    config = with_overrides(plugin.config, queues=queues, concurrency=concurrency)
    plugin.config = config

    async def main() -> None:
        async with plugin.connected(consumer=consumer):
            await run_with_signals(
                plugin.transport,
                config.registry.handlers(),
                config.worker,
                scheduler=plugin.scheduler,
                cron=config.cron,
            )

    anyio.run(main, backend="asyncio", backend_options={"use_uvloop": True})
