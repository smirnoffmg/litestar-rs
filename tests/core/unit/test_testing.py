"""The helpers applications use to test their own code."""

import pytest

from litestar_rs.core.envelope import Envelope
from litestar_rs.core.testing import CollectingEnqueuer, EagerEnqueuer, UnknownTask

pytestmark = pytest.mark.unit


def envelope(task: str = "reindex") -> Envelope:
    return Envelope(id=f"job-{task}", task=task, payload=b"{}", enqueued_at=1)


async def test_collecting_records_without_running() -> None:
    enqueuer = CollectingEnqueuer()
    await enqueuer.enqueue(envelope(), queue="default")

    enqueuer.assert_enqueued("reindex")
    enqueuer.assert_not_enqueued("purge")


async def test_collecting_reports_the_actual_count() -> None:
    enqueuer = CollectingEnqueuer()
    await enqueuer.enqueue(envelope(), queue="default")
    await enqueuer.enqueue(envelope(), queue="default")

    with pytest.raises(AssertionError, match="got 2"):
        enqueuer.assert_enqueued("reindex")


async def test_eager_runs_the_task_inline() -> None:
    ran: list[str] = []

    async def handler(env: Envelope) -> None:
        ran.append(env.task)

    await EagerEnqueuer({"reindex": handler}).enqueue(envelope(), queue="default")

    assert ran == ["reindex"]


async def test_eager_refuses_an_unregistered_task() -> None:
    """Silently doing nothing would make the test pass for the wrong reason."""
    with pytest.raises(UnknownTask, match="reindex"):
        await EagerEnqueuer({}).enqueue(envelope(), queue="default")
