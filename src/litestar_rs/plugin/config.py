"""Everything the plugin needs, in one place the application constructs itself."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

from litestar_rs.core.cron import CronJob
from litestar_rs.core.errors import ConfigurationError
from litestar_rs.core.protocols import BrokerHandler
from litestar_rs.core.worker import WorkerConfig
from litestar_rs.plugin.registry import TaskRegistry
from litestar_rs.plugin.tracing import TraceparentSource, no_traceparent


@dataclass(frozen=True, slots=True)
class QueueConfig:
    """How this application talks to its queue.

    There is deliberately no string path to the app object here. Requiring
    ``"module:app"`` in a constructor pushes an import-time ordering problem onto
    every user; the CLI already has the application it was invoked with.
    """

    registry: TaskRegistry
    redis_url: str = "redis://localhost:6379/0"
    namespace: str = "lrs"
    queues: Sequence[str] = ("default",)
    shards: int = 1
    group: str = "workers"
    consumer_prefix: str = "worker"
    block_ms: int = 5_000
    worker: WorkerConfig = field(default_factory=WorkerConfig)
    cron: Sequence[CronJob] = ()
    health_path: str | None = "/health/queue"
    """Where the plugin serves queue health, or None to register no route.

    Registering a path the application already uses fails at startup rather than
    shadowing one of them, so move it or turn it off; `queue_health()` gives the
    same data to an application that would rather serve it itself.
    """
    result_ttl_ms: int = 3_600_000
    thread_limit: int = 20
    """Threads for synchronous tasks. The asyncio default is a silent bottleneck."""
    offload_over_bytes: int = 128 * 1024
    brokers: Mapping[str, BrokerHandler] = field(default_factory=dict)
    """Streams somebody else writes, and what handles each of them.

    One field rather than a list of streams beside a dictionary of handlers: a
    stream with no handler is read and dropped, a handler for a stream nobody
    subscribes to never runs, and two fields that must agree eventually will not.
    """

    @property
    def external(self) -> tuple[str, ...]:
        return tuple(self.brokers)

    traceparent: TraceparentSource = no_traceparent
    """Supplies the current W3C traceparent, so a task can join the request's trace."""

    def __post_init__(self) -> None:
        if not self.redis_url:
            raise ConfigurationError("redis_url must not be empty")
        if self.health_path is not None and not self.health_path.startswith("/"):
            raise ConfigurationError(
                f"health_path must start with '/', got {self.health_path!r}"
            )
