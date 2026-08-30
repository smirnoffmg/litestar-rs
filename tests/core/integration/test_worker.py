"""Worker against a real Redis: handover on crash, and shutdown behaviour."""

import signal
import sys
from collections.abc import Awaitable, Callable
from functools import partial
from pathlib import Path

import anyio
import pytest

from litestar_rs.core.envelope import Envelope
from litestar_rs.core.scheduler import RedisScheduler
from litestar_rs.core.transport import RedisStreamsTransport
from litestar_rs.core.worker import WorkerConfig, run

pytestmark = pytest.mark.integration

Make = Callable[[str], Awaitable[RedisStreamsTransport]]

WORKER_MAIN = Path(__file__).parent / "_worker_main.py"


def envelope() -> Envelope:
    return Envelope(
        id="job-1", task="reindex", payload=b"{}", enqueued_at=1712345678901
    )


def config(**overrides: object) -> WorkerConfig:
    base: dict[str, object] = {
        "concurrency": 1,
        "min_idle_ms": 0,
        "reclaim_interval_s": 0.05,
        "trim_interval_s": 60.0,
        "scheduler_interval_s": 0.05,
    }
    return WorkerConfig(**(base | overrides))  # type: ignore[arg-type]  # test factory


async def test_completed_work_is_acked_on_graceful_shutdown(
    transport: RedisStreamsTransport,
) -> None:
    await transport.enqueue(envelope(), queue=transport.queue)
    stop = anyio.Event()
    ran = anyio.Event()
    done: list[str] = []

    async def handler(envelope: Envelope) -> None:
        done.append(envelope.id)
        ran.set()

    async with anyio.create_task_group() as tg:
        tg.start_soon(
            partial(run, transport, {"reindex": handler}, config(), shutdown=stop)
        )
        await ran.wait()
        stop.set()

    assert done == ["job-1"]
    assert await transport.control.xlen(transport.streams[0]) == 0


@pytest.mark.slow
async def test_watchdog_hands_back_work_it_had_to_cut_off(
    transport: RedisStreamsTransport,
) -> None:
    """Work cancelled by the drain watchdog is not an application failure.

    It stays in the PEL unacked, and its alive key is dropped so a peer picks it
    up at once instead of waiting out the TTL. The 200ms here is the watchdog
    budget under test, not a wait for something to settle.
    """
    await transport.enqueue(envelope(), queue=transport.queue)
    stop = anyio.Event()
    started = anyio.Event()

    async def handler(envelope: Envelope) -> None:
        started.set()
        await anyio.sleep_forever()

    async with anyio.create_task_group() as tg:
        tg.start_soon(
            partial(
                run,
                transport,
                {"reindex": handler},
                config(drain_timeout_s=0.2),
                shutdown=stop,
            )
        )
        await started.wait()
        stop.set()

    stream = transport.streams[0]
    entries = await transport.control.xpending_range(
        stream, transport.group, min="-", max="+", count=10
    )
    assert len(entries) == 1
    assert await transport.control.xlen(stream) == 1
    assert await transport.control.exists(transport.alive_key(b"x")) == 0


@pytest.mark.slow
async def test_killed_worker_hands_its_entry_to_a_peer(
    transport: RedisStreamsTransport,
    transports: Make,
    redis_url: str,
    namespace: str,
) -> None:
    """SIGKILL a worker holding an entry: a peer runs it, and runs it once.

    "Exactly once" here means what Redis Streams can actually give: one owner at
    a time, one execution of the side effect, one ack. The kill lands before the
    victim does any work, which is the case a reclaim can make good on.
    """
    signal_key = f"{{{namespace}}}:signal"
    counter_key = f"{{{namespace}}}:runs"
    await transport.enqueue(envelope(), queue=transport.queue)

    victim = await anyio.open_process(
        [
            sys.executable,
            str(WORKER_MAIN),
            redis_url,
            namespace,
            "victim",
            signal_key,
            "block",
        ]
    )
    try:
        taken = await transport.control.blpop([signal_key], timeout=30)
        assert taken is not None, "worker never took the entry"
    finally:
        victim.kill()
        await victim.wait()

    stream = transport.streams[0]
    [pending] = await transport.control.xpending_range(
        stream, transport.group, min="-", max="+", count=10
    )
    assert pending["consumer"] == b"victim"

    entry_id = pending["message_id"]
    assert isinstance(entry_id, bytes)
    # Drop the alive key the dead process can no longer refresh: exactly what its
    # TTL would do, done now so the test waits on nothing.
    await transport.clear_alive([entry_id])

    survivor = await transports("survivor")
    ran = anyio.Event()

    async def handler(envelope: Envelope) -> None:
        await survivor.control.incr(counter_key)
        ran.set()

    stop = anyio.Event()
    with anyio.fail_after(30):
        async with anyio.create_task_group() as tg:
            tg.start_soon(
                partial(run, survivor, {"reindex": handler}, config(), shutdown=stop)
            )
            await ran.wait()
            stop.set()

    assert await transport.control.get(counter_key) == b"1"
    summary = await transport.control.xpending(stream, transport.group)
    assert summary["pending"] == 0
    assert await transport.control.xlen(stream) == 0


@pytest.mark.slow
async def test_sigterm_stops_the_process_on_its_own(
    transport: RedisStreamsTransport, redis_url: str, namespace: str
) -> None:
    """The signal helper must actually wire SIGTERM to a graceful stop."""
    signal_key = f"{{{namespace}}}:signal"
    await transport.enqueue(envelope(), queue=transport.queue)

    worker = await anyio.open_process(
        [sys.executable, str(WORKER_MAIN), redis_url, namespace, "w", signal_key, "ack"]
    )
    with anyio.fail_after(30):
        assert await transport.control.blpop([signal_key], timeout=30) is not None
        worker.send_signal(signal.SIGTERM)
        await worker.wait()

    assert worker.returncode == 0
    assert await transport.control.xlen(transport.streams[0]) == 0


async def test_a_delayed_job_runs_end_to_end(
    transport: RedisStreamsTransport, scheduler: RedisScheduler
) -> None:
    """Schedule, promote, consume, ack -- with no separate scheduler process."""
    due = await scheduler.now_ms() - 1
    await scheduler.schedule_at(envelope(), queue=transport.queue, when_ms=due)

    stop = anyio.Event()
    ran = anyio.Event()
    seen: list[str] = []

    async def handler(env: Envelope) -> None:
        seen.append(env.id)
        ran.set()

    with anyio.fail_after(30):
        async with anyio.create_task_group() as tg:
            tg.start_soon(
                partial(
                    run,
                    transport,
                    {"reindex": handler},
                    config(),
                    shutdown=stop,
                    scheduler=scheduler,
                )
            )
            await ran.wait()
            stop.set()

    assert seen == ["job-1"]
    assert await scheduler.pending() == 0
    assert await transport.control.xlen(transport.streams[0]) == 0
    assert await scheduler.control.exists(scheduler.leader) == 0
