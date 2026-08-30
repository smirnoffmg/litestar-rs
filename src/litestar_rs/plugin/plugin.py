"""The Litestar plugin: one object the application registers, nothing else."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from litestar import Litestar, get
from litestar.di import Provide
from litestar.plugins import CLIPlugin, InitPlugin
from redis.asyncio import Redis

from litestar_rs.core.envelope import Envelope
from litestar_rs.core.errors import ConfigurationError
from litestar_rs.core.scheduler import RedisScheduler
from litestar_rs.core.transport import RedisStreamsTransport
from litestar_rs.plugin.config import QueueConfig
from litestar_rs.plugin.health import QueueHealth, queue_health

if TYPE_CHECKING:
    from click import Group
    from litestar.config.app import AppConfig


class QueuePlugin(InitPlugin, CLIPlugin):
    """Wires the queue into an application, and the application into the CLI.

    The plugin is itself the enqueuer the registry binds to, which is how tasks
    can be declared at import time while their Redis connections are opened by
    the application lifespan.
    """

    def __init__(self, config: QueueConfig) -> None:
        self.config = config
        self._transport: RedisStreamsTransport | None = None
        self._scheduler: RedisScheduler | None = None

    @property
    def transport(self) -> RedisStreamsTransport:
        if self._transport is None:
            raise ConfigurationError(
                "the queue is not connected yet; it opens with the application"
            )
        return self._transport

    @property
    def scheduler(self) -> RedisScheduler:
        if self._scheduler is None:
            raise ConfigurationError(
                "the queue is not connected yet; it opens with the application"
            )
        return self._scheduler

    async def enqueue(self, envelope: Envelope, *, queue: str) -> bytes:
        return await self.transport.enqueue(envelope, queue=queue)

    def on_app_init(self, app_config: AppConfig) -> AppConfig:
        self.config.registry.bind(
            _providers(app_config.dependencies),
            enqueuer=self,
            type_encoders=app_config.type_encoders,
            type_decoders=app_config.type_decoders,
            traceparent=self.config.traceparent,
        )
        app_config.lifespan.append(self._lifespan)
        app_config.route_handlers.append(self._health_route())
        return app_config

    def on_cli_init(self, cli: Group) -> None:
        from litestar_rs.plugin.cli import workers

        cli.add_command(workers)

    @asynccontextmanager
    async def _lifespan(self, app: Litestar) -> AsyncIterator[None]:
        async with self.connected():
            yield

    @asynccontextmanager
    async def connected(self, *, consumer: str | None = None) -> AsyncIterator[None]:
        """Open the queue's connections for the duration of the block.

        Used by the application lifespan and by the worker command alike, so a
        worker and a web process reach Redis in exactly the same shape.
        """
        config = self.config
        # The reader owns its connection for a whole block window; a socket
        # timeout below that would kill every healthy read, above it is the only
        # thing that surfaces a connection hung by a failover.
        reader: Redis = Redis.from_url(
            config.redis_url, socket_timeout=config.block_ms / 1000 + 30
        )
        control: Redis = Redis.from_url(config.redis_url)
        try:
            self._transport = RedisStreamsTransport(
                reader=reader,
                control=control,
                consumer=consumer or f"{config.consumer_prefix}-{uuid4().hex[:8]}",
                namespace=config.namespace,
                queues=config.queues,
                shards=config.shards,
                group=config.group,
                block_ms=config.block_ms,
            )
            self._scheduler = RedisScheduler(
                control=control, namespace=config.namespace, shards=config.shards
            )
            await self._transport.ensure_group()
            yield
        finally:
            self._transport = None
            self._scheduler = None
            await reader.aclose()
            await control.aclose()

    def _health_route(self) -> Any:
        plugin = self

        @get(self.config.health_path)
        async def health() -> QueueHealth:
            return await queue_health(plugin.transport)

        return health


def _providers(dependencies: dict[str, Any]) -> dict[str, Provide]:
    """Litestar accepts a bare callable as a dependency; normalise to Provide."""
    return {
        key: value if isinstance(value, Provide) else Provide(value)
        for key, value in dependencies.items()
    }
