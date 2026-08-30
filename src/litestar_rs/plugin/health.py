"""Queue health, served identically by the web app and by the worker.

Readiness probes for the two deployments then ask the same question and get the
answer computed the same way, which is the only way they stay comparable.
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
