"""Queue health, served identically by the web app and by the worker.

Readiness probes for the two deployments then ask the same question and get the
answer computed the same way, which is the only way they stay comparable.
"""

from __future__ import annotations

from dataclasses import dataclass

from litestar_rs.core.protocols import StreamTransport


@dataclass(frozen=True, slots=True)
class QueueHealth:
    namespace: str
    group: str
    queues: tuple[str, ...]
    lag: int | None
    """Depth from XINFO GROUPS. None when Redis cannot reconcile its counters."""

    @property
    def healthy(self) -> bool:
        return self.lag is not None


async def queue_health(transport: StreamTransport) -> QueueHealth:
    return QueueHealth(
        namespace=transport.namespace,
        group=transport.group,
        queues=tuple(transport.queues),
        lag=await transport.lag(),
    )
