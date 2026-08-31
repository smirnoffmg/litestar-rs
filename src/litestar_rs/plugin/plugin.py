"""The Litestar plugin: one object the application registers, nothing else."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from litestar import Litestar
from litestar.di import Provide
from litestar.plugins import CLIPlugin, InitPlugin
from redis.asyncio import Redis

from litestar_rs.core.envelope import Envelope, TaskResult
from litestar_rs.core.errors import ConfigurationError
from litestar_rs.core.results import RedisResultStore
from litestar_rs.core.scheduler import RedisScheduler
from litestar_rs.core.stats import WorkerStats
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
        self._results: RedisResultStore | None = None
        self.stats = WorkerStats()
        """Counters for this process, served by the health endpoint."""

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

    @property
    def results(self) -> RedisResultStore:
        if self._results is None:
            raise ConfigurationError(
                "the queue is not connected yet; it opens with the application"
            )
        return self._results

    async def enqueue(self, envelope: Envelope, *, queue: str) -> bytes:
        return await self.transport.enqueue(envelope, queue=queue)

    async def store(
        self, job_id: str, result: TaskResult, *, ttl_ms: int | None = None
    ) -> None:
        await self.results.store(job_id, result, ttl_ms=ttl_ms)

    async def get(self, job_id: str) -> TaskResult | None:
        return await self.results.get(job_id)

    def on_app_init(self, app_config: AppConfig) -> AppConfig:
        self.config.registry.bind(
            _providers(app_config.dependencies),
            enqueuer=self,
            type_encoders=app_config.type_encoders,
            type_decoders=app_config.type_decoders,
            traceparent=self.config.traceparent,
            results=self,
            payloads=self.config.payloads,
            thread_limit=self.config.thread_limit,
            offload_over_bytes=self.config.offload_over_bytes,
        )
        # First, so the queue is the outermost manager: everything the
        # application opens after it can enqueue, and can still enqueue while
        # unwinding. Appended, it would close before their exits ran.
        app_config.lifespan.insert(0, self._lifespan)
        return app_config

    def on_cli_init(self, cli: Group) -> None:
        from litestar_rs.plugin.cli import workers

        cli.add_command(workers)

    @asynccontextmanager
    async def _lifespan(self, app: Litestar) -> AsyncGenerator[None]:
        async with self.connected():
            yield

    @asynccontextmanager
    async def connected(self, *, consumer: str | None = None) -> AsyncGenerator[None]:
        """Open the queue's connections for the duration of the block.

        Used by the application lifespan and by the worker command alike, so a
        worker and a web process reach Redis in exactly the same shape.

        The consumer name comes from `config.consumer` unless a caller opening
        the queue itself passes one: a worker that enters the application's
        lifespan never opens the queue, so it has nowhere to pass an argument.
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
                consumer=_consumer_name(config, consumer),
                namespace=config.namespace,
                queues=config.queues,
                shards=config.shards,
                group=config.group,
                block_ms=config.block_ms,
                fairness_every=config.fairness_every,
                max_payload_bytes=config.max_payload_bytes,
                external=config.external,
            )
            self._scheduler = RedisScheduler(
                control=control, namespace=config.namespace, shards=config.shards
            )
            self._results = RedisResultStore(
                control=control,
                namespace=config.namespace,
                default_ttl_ms=config.result_ttl_ms,
            )
            await self._transport.ensure_group()
            yield
        finally:
            self._transport = None
            self._scheduler = None
            self._results = None
            await reader.aclose()
            await control.aclose()

    async def health(self) -> QueueHealth:
        """Everything a readiness probe needs, for whoever is serving one.

        The plugin registers no route of its own: which path a probe lives on,
        whether it is public, and what else belongs in the same response are the
        application's to decide, and a worker has no server to answer on at all.
        `QueueHealth.healthy` is the field to turn into a status code -- a probe
        reads the code, not the body, and answering 200 while unhealthy is a
        probe that can never fail.
        """
        return await queue_health(self.transport, self.stats)


def _consumer_name(config: QueueConfig, override: str | None) -> str:
    """A name given as empty is passed through rather than replaced: the
    transport refuses it, and quietly generating one instead would turn a typo in
    `--consumer` into a second worker under an unrelated name.
    """
    named = override if override is not None else config.consumer
    if named is None:
        return f"{config.consumer_prefix}-{uuid4().hex[:8]}"
    return named


def _providers(dependencies: dict[str, Any]) -> dict[str, Provide]:
    """Litestar accepts a bare callable as a dependency; normalise to Provide."""
    return {
        key: value if isinstance(value, Provide) else Provide(value)
        for key, value in dependencies.items()
    }
