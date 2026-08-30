"""Publishing jobs only once the transaction that justified them has committed.

A handler that writes a row and enqueues a job before ``COMMIT`` has a race it
cannot win: the worker is fast enough to read the row before it exists, and if
the transaction rolls back the job has already run against data that was never
written.

Buffering here and flushing from an ``after_commit`` hook covers that. It is not
a transactional outbox -- a crash between commit and flush loses the job -- but
it removes the ordering hazard, which is the one that bites in practice. Work
that must survive that crash needs an outbox in the same database.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextvars import ContextVar

from litestar_rs.core.envelope import Envelope
from litestar_rs.core.protocols import Enqueuer

current_enqueuer: ContextVar[Enqueuer | None] = ContextVar(
    "litestar_rs_enqueuer", default=None
)
"""The enqueuer in force for this unit of work, if one was bound.

Set it and every enqueue inside the block routes through it, which is what lets
publication know about the transaction without every call site being told.
"""


class DeferredEnqueuer:
    """Holds jobs until something says the transaction went through.

    Wrap the real enqueuer per unit of work, call ``flush`` from the commit hook
    and ``discard`` from the rollback one.
    """

    def __init__(self, target: Enqueuer) -> None:
        self.target = target
        self._pending: list[tuple[Envelope, str]] = []

    @property
    def pending(self) -> tuple[tuple[Envelope, str], ...]:
        return tuple(self._pending)

    async def enqueue(self, envelope: Envelope, *, queue: str) -> bytes:
        """Record the job. Nothing reaches Redis until ``flush``."""
        self._pending.append((envelope, queue))
        return envelope.id.encode()

    async def flush(self) -> list[bytes]:
        """Publish everything buffered, oldest first, and forget it.

        The buffer is cleared before publishing: a failure part way through must
        not leave jobs that would be published twice by the next flush.
        """
        buffered, self._pending = self._pending, []
        return [
            await self.target.enqueue(envelope, queue=queue)
            for envelope, queue in buffered
        ]

    def discard(self) -> None:
        """Drop everything buffered. For the rollback path."""
        self._pending.clear()

    @asynccontextmanager
    async def active(self) -> AsyncIterator[DeferredEnqueuer]:
        """Route every enqueue in this block through the buffer.

        Binding only. Whether the block ends in ``flush`` or ``discard`` is the
        caller's decision, because only the caller knows how the transaction
        ended.
        """
        token = current_enqueuer.set(self)
        try:
            yield self
        finally:
            current_enqueuer.reset(token)
