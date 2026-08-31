"""One answer to "is the queue all right", wherever it is asked from.

The plugin's route, `QueuePlugin.health()` and anything built on the core
directly all come through here, so probes against a web process and against a
worker stay comparable instead of drifting into two lookalike answers.
"""

from __future__ import annotations

from dataclasses import dataclass

from litestar_rs.core.protocols import StreamTransport
from litestar_rs.core.stats import WorkerStats


@dataclass(frozen=True, slots=True)
class QueueHealth:
    namespace: str
    group: str
    queues: tuple[str, ...]
    lag: int | None
    """Depth from XINFO GROUPS. None when Redis cannot reconcile its counters."""
    stats: dict[str, int]
    """This process's own counters. All zero in a web process, which runs no tasks."""
    healthy: bool
    """A real field, not a property: a probe reads the response, not the object."""


async def queue_health(
    transport: StreamTransport, stats: WorkerStats | None = None
) -> QueueHealth:
    lag = await transport.lag()
    return QueueHealth(
        namespace=transport.namespace,
        group=transport.group,
        queues=tuple(transport.queues),
        lag=lag,
        stats=(stats or WorkerStats()).snapshot(),
        healthy=lag is not None,
    )
