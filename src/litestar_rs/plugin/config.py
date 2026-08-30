"""Everything the plugin needs, in one place the application constructs itself."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

from litestar_rs.core.cron import CronJob
from litestar_rs.core.errors import ConfigurationError
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
    health_path: str = "/health/queue"
    result_ttl_ms: int = 3_600_000
    thread_limit: int = 20
    """Threads for synchronous tasks. The asyncio default is a silent bottleneck."""
    offload_over_bytes: int = 128 * 1024
    external: Sequence[str] = ()
    """Streams somebody else writes, consumed by the same worker and group."""
    traceparent: TraceparentSource = no_traceparent
    """Supplies the current W3C traceparent, so a task can join the request's trace."""

    def __post_init__(self) -> None:
        if not self.redis_url:
            raise ConfigurationError("redis_url must not be empty")
        if not self.health_path.startswith("/"):
            raise ConfigurationError(
                f"health_path must start with '/', got {self.health_path!r}"
            )
