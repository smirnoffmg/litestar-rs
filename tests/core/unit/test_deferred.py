"""Jobs wait for the transaction that justified them."""

import pytest

from litestar_rs.core.deferred import DeferredEnqueuer
from litestar_rs.core.envelope import Envelope
from litestar_rs.core.testing import CollectingEnqueuer

pytestmark = pytest.mark.unit


def envelope(job_id: str) -> Envelope:
    return Envelope(id=job_id, task="reindex", payload=b"{}", enqueued_at=1)


async def test_nothing_is_published_before_the_flush() -> None:
    """The worker is quick enough to read a row that has not been committed."""
    target = CollectingEnqueuer()
    deferred = DeferredEnqueuer(target)

    await deferred.enqueue(envelope("job-1"), queue="default")

    assert target.enqueued == []
    assert [e.id for e, _ in deferred.pending] == ["job-1"]


async def test_flush_publishes_in_order_and_empties_the_buffer() -> None:
    target = CollectingEnqueuer()
    deferred = DeferredEnqueuer(target)
    await deferred.enqueue(envelope("job-1"), queue="default")
    await deferred.enqueue(envelope("job-2"), queue="high")

    await deferred.flush()

    assert [(e.id, q) for e, q in target.enqueued] == [
        ("job-1", "default"),
        ("job-2", "high"),
    ]
    assert deferred.pending == ()
    await deferred.flush()
    assert len(target.enqueued) == 2, "a second flush must publish nothing"


async def test_a_rolled_back_transaction_publishes_nothing() -> None:
    target = CollectingEnqueuer()
    deferred = DeferredEnqueuer(target)
    await deferred.enqueue(envelope("job-1"), queue="default")

    deferred.discard()
    await deferred.flush()

    assert target.enqueued == []


async def test_a_failed_flush_does_not_republish_on_the_next_one() -> None:
    """The buffer is cleared before publishing, on purpose."""

    class Broken:
        async def enqueue(self, envelope: Envelope, *, queue: str) -> bytes:
            raise ConnectionError("connection closed by server")

    deferred = DeferredEnqueuer(Broken())
    await deferred.enqueue(envelope("job-1"), queue="default")

    with pytest.raises(ConnectionError):
        await deferred.flush()

    assert deferred.pending == ()


async def test_a_bound_enqueuer_is_only_in_force_inside_its_block() -> None:
    """A unit of work must not leak its buffer into the next request."""
    from litestar_rs.core.deferred import current_enqueuer

    target = CollectingEnqueuer()
    deferred = DeferredEnqueuer(target)

    assert current_enqueuer.get() is None
    async with deferred.active() as bound:
        assert current_enqueuer.get() is bound
    assert current_enqueuer.get() is None
