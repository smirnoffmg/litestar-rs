"""How a task actually runs: threads for sync work, timeouts only where real."""

import threading

import anyio
import pytest

from smallage.core.errors import ConfigurationError
from smallage.core.testing import CollectingEnqueuer
from smallage.litestar.registry import TaskRegistry

pytestmark = pytest.mark.unit


async def test_a_sync_task_runs_off_the_event_loop() -> None:
    """Blocking in the loop starves the heartbeat and gets the worker reclaimed."""
    registry = TaskRegistry()
    threads: list[int] = []

    @registry.task
    def crunch() -> None:
        threads.append(threading.get_ident())

    enqueuer = CollectingEnqueuer()
    registry.bind({}, enqueuer=enqueuer)
    await registry.enqueue("crunch", {})
    [(envelope, _)] = enqueuer.enqueued

    await registry.execute(envelope)

    assert threads and threads[0] != threading.get_ident()


async def test_an_async_task_honours_its_timeout() -> None:
    registry = TaskRegistry()

    @registry.task(timeout_s=0.05)
    async def slow() -> None:
        await anyio.sleep(30)

    enqueuer = CollectingEnqueuer()
    registry.bind({}, enqueuer=enqueuer)
    await registry.enqueue("slow", {})
    [(envelope, _)] = enqueuer.enqueued

    with pytest.raises(TimeoutError):
        await registry.execute(envelope)


def test_a_sync_task_may_not_claim_a_timeout() -> None:
    """Threads cannot be killed. Promising a timeout here would be a lie."""
    registry = TaskRegistry()

    @registry.task(timeout_s=1.0)
    def crunch() -> None: ...

    with pytest.raises(ConfigurationError, match="synchronous"):
        registry.bind({}, enqueuer=CollectingEnqueuer())


async def test_a_task_returns_its_value_for_the_result_store() -> None:
    registry = TaskRegistry()

    @registry.task
    async def compute() -> bytes:
        return b"42"

    enqueuer = CollectingEnqueuer()
    registry.bind({}, enqueuer=enqueuer)
    await registry.enqueue("compute", {})
    [(envelope, _)] = enqueuer.enqueued

    assert await registry.execute(envelope) == b"42"
