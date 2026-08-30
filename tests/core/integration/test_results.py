"""Results against a real Redis, including the blocking wait."""

from collections.abc import AsyncIterator

import anyio
import pytest
from redis.asyncio import Redis

from litestar_rs.core.envelope import TaskResult
from litestar_rs.core.errors import ConfigurationError
from litestar_rs.core.results import RedisResultStore

pytestmark = pytest.mark.integration


@pytest.fixture
async def results(redis_url: str, namespace: str) -> AsyncIterator[RedisResultStore]:
    control: Redis = Redis.from_url(redis_url)
    try:
        yield RedisResultStore(control=control, namespace=namespace)
    finally:
        await control.aclose()


async def test_a_stored_result_comes_back(results: RedisResultStore) -> None:
    await results.store("job-1", TaskResult(ok=True, value=b"\x00\xffdone"))

    stored = await results.get("job-1")

    assert stored == TaskResult(ok=True, value=b"\x00\xffdone")


async def test_an_unknown_job_has_no_result(results: RedisResultStore) -> None:
    assert await results.get("never-ran") is None


async def test_waiting_returns_immediately_when_it_is_already_there(
    results: RedisResultStore,
) -> None:
    await results.store("job-1", TaskResult(ok=True, value=b"done"))

    assert await results.wait("job-1", timeout_s=30) == TaskResult(
        ok=True, value=b"done"
    )


async def test_a_waiter_is_woken_by_the_store(results: RedisResultStore) -> None:
    """Blocking, not polling: the write is what wakes the reader."""
    woken: list[TaskResult | None] = []

    async def waiter() -> None:
        woken.append(await results.wait("job-1", timeout_s=30))

    with anyio.fail_after(30):
        async with anyio.create_task_group() as tg:
            tg.start_soon(waiter)
            await anyio.lowlevel.checkpoint()
            await results.store("job-1", TaskResult(ok=True, value=b"late"))

    assert woken == [TaskResult(ok=True, value=b"late")]


async def test_two_waiters_on_one_job_both_wake(results: RedisResultStore) -> None:
    woken: list[TaskResult | None] = []

    async def waiter() -> None:
        woken.append(await results.wait("job-1", timeout_s=30))

    with anyio.fail_after(30):
        async with anyio.create_task_group() as tg:
            tg.start_soon(waiter)
            tg.start_soon(waiter)
            await anyio.lowlevel.checkpoint()
            await results.store("job-1", TaskResult(ok=True, value=b"shared"))

    assert woken == [TaskResult(ok=True, value=b"shared")] * 2


async def test_waiting_gives_up(results: RedisResultStore) -> None:
    assert await results.wait("never-ran", timeout_s=0.05) is None


async def test_failures_are_recorded_too(results: RedisResultStore) -> None:
    await results.store("job-1", TaskResult(ok=False, error="RuntimeError: boom"))

    stored = await results.get("job-1")

    assert stored is not None
    assert stored.ok is False
    assert stored.error == "RuntimeError: boom"


async def test_a_decoding_client_is_refused(redis_url: str) -> None:
    control: Redis = Redis.from_url(redis_url, decode_responses=True)
    try:
        with pytest.raises(ConfigurationError, match="decode_responses"):
            RedisResultStore(control=control)
    finally:
        await control.aclose()
