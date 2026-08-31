"""Everything the plugin needs, in one place the application constructs itself."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

from smallage.core.cron import CronJob
from smallage.core.errors import ConfigurationError
from smallage.core.protocols import BrokerHandler, PayloadStore
from smallage.core.worker import WorkerConfig
from smallage.litestar.registry import TaskRegistry
from smallage.litestar.tracing import TraceparentSource, no_traceparent


@dataclass(frozen=True, slots=True, kw_only=True)
class QueueConfig:
    """How this application talks to its queue.

    There is deliberately no string path to the app object here. Requiring
    ``"module:app"`` in a constructor pushes an import-time ordering problem onto
    every user; the CLI already has the application it was invoked with.

    Keyword-only: fields are grouped by what they configure rather than appended,
    so a new one lands mid-class and positional order was never a contract worth
    keeping.
    """

    registry: TaskRegistry
    redis_url: str = "redis://localhost:6379/0"
    namespace: str = "lrs"
    queues: Sequence[str] = ("default",)
    shards: int = 1
    group: str = "workers"
    consumer_prefix: str = "worker"
    consumer: str | None = None
    """The worker's consumer name, or None to derive one from `consumer_prefix`.

    Must be unique per running worker: Redis groups pending entries by consumer
    name, and reclaim uses that name to tell a worker's own work from a peer's.
    `--consumer` sets this, which is how the name survives being settled before
    the application's lifespan opens the queue.
    """
    block_ms: int = 5_000
    worker: WorkerConfig = field(default_factory=WorkerConfig)
    run_app_lifespan: bool = True
    """Whether a worker process enters the application's lifespan.

    On by default, so a dependency that closes over something the lifespan opens
    resolves to an opened one in a worker as well as in a web process. Decline it
    when the lifespan does work that belongs to a web process -- starting a
    scheduler, warming a cache, claiming a lease -- which would otherwise happen
    in every worker replica.
    """
    cron: Sequence[CronJob] = ()
    result_ttl_ms: int = 3_600_000
    fairness_every: int = 10
    """Passes between giving the lowest-priority queue first refusal. 0 for
    strict priority, and the starvation that comes with it."""
    max_payload_bytes: int = 128 * 1024
    """Refused above this when no payload store is configured."""
    payloads: PayloadStore | None = None
    """Where arguments above `offload_over_bytes` go. Without one, the transport
    refuses an oversized record rather than dropping it quietly."""
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
