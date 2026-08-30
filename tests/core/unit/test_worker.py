"""The worker loops, with no Redis and no clock.

Time enters the core in exactly one place -- the injected ``Sleeper`` -- so a fake
that returns instantly and stops the loop after k turns covers the whole of it.
No test here sleeps.
"""

from collections.abc import Iterable, Sequence

import anyio
import pytest

from litestar_rs.core.cron import CronJob
from litestar_rs.core.envelope import Envelope, Record, to_fields
from litestar_rs.core.errors import ConfigurationError
from litestar_rs.core.worker import (
    Slots,
    WorkerConfig,
    _consume_loop,
    _heartbeat_loop,
    _reclaim_loop,
    _run_one,
    _scheduler_loop,
    credits,
    run,
)

pytestmark = pytest.mark.unit


class _StopLoop(Exception):
    """Ends an otherwise infinite supervisor loop at a known iteration."""


def stop_after(turns: int) -> object:
    calls: list[float] = []

    async def sleep(delay: float) -> None:
        calls.append(delay)
        if len(calls) >= turns:
            raise _StopLoop
        await anyio.lowlevel.checkpoint()

    sleep.calls = calls  # type: ignore[attr-defined]
    return sleep


def record(entry_id: bytes, task: str = "reindex") -> Record:
    envelope = Envelope(
        id=entry_id.decode(), task=task, payload=b"{}", enqueued_at=1712345678901
    )
    return Record(
        stream="{lrs}:q:default:0", entry_id=entry_id, fields=to_fields(envelope)
    )


class FakeTransport:
    def __init__(
        self,
        batches: list[list[Record]] | None = None,
        pending: list[tuple[str, bytes]] | None = None,
        claimable: set[bytes] | None = None,
    ) -> None:
        self.batches = list(batches or [])
        self.pending_entries = list(pending or [])
        self.claimable = claimable if claimable is not None else set()
        self.read_counts: list[int] = []
        self.pending_counts: list[int] = []
        self.marked: list[list[bytes]] = []
        self.refreshed: list[list[bytes]] = []
        self.cleared: list[list[bytes]] = []
        self.acked: list[tuple[str, list[bytes]]] = []
        self.trimmed = 0
        self.events: list[str] = []

    async def ensure_group(self) -> None:
        self.events.append("ensure_group")

    async def read(self, count: int) -> list[Record]:
        # A real read blocks on the socket; yielding here keeps the fake from
        # starving the very handlers the loop just spawned.
        await anyio.lowlevel.checkpoint()
        self.read_counts.append(count)
        self.events.append(f"read:{count}")
        return self.batches.pop(0) if self.batches else []

    async def mark_alive(self, entry_ids: Sequence[bytes], *, ttl_ms: int) -> None:
        self.marked.append(list(entry_ids))
        self.events.append("mark_alive")

    async def refresh_alive(self, entry_ids: Iterable[bytes], *, ttl_ms: int) -> None:
        self.refreshed.append(list(entry_ids))

    async def clear_alive(self, entry_ids: Iterable[bytes]) -> None:
        self.cleared.append(list(entry_ids))

    async def ack(self, stream: str, entry_ids: Sequence[bytes]) -> int:
        self.acked.append((stream, list(entry_ids)))
        return len(entry_ids)

    async def pending(self, *, count: int, min_idle_ms: int) -> list[tuple[str, bytes]]:
        self.pending_counts.append(count)
        return self.pending_entries if count > 0 else []

    async def reclaim(
        self, stream: str, entry_id: bytes, *, min_idle_ms: int, ttl_ms: int
    ) -> list[Record]:
        return [record(entry_id)] if entry_id in self.claimable else []

    async def trim(self, *, retention_ms: int) -> None:
        self.trimmed += 1


@pytest.mark.parametrize(
    ("concurrency", "in_flight", "expected"),
    [(10, 0, 10), (10, 4, 6), (10, 10, 0), (10, 12, 0), (1, 0, 1)],
)
def test_credits(concurrency: int, in_flight: int, expected: int) -> None:
    assert credits(concurrency, in_flight) == expected


@pytest.mark.parametrize(
    ("field", "value"),
    [("concurrency", 0), ("alive_ttl_ms", 0), ("drain_timeout_s", -1.0)],
)
def test_config_rejects_bad_values(field: str, value: object) -> None:
    with pytest.raises(ConfigurationError, match=field):
        WorkerConfig(**{field: value})  # type: ignore[arg-type]  # table-driven


def test_refresh_interval_is_a_third_of_the_ttl() -> None:
    assert WorkerConfig(alive_ttl_ms=30_000).refresh_interval_s == 10.0


async def test_read_count_always_equals_free_slots() -> None:
    transport = FakeTransport(batches=[[record(b"1-0"), record(b"2-0")], []])
    slots = Slots()
    stop = anyio.Event()
    spawned: list[bytes] = []

    def spawn(rec: Record) -> None:
        spawned.append(rec.entry_id)
        stop.set()

    await _consume_loop(transport, slots, WorkerConfig(concurrency=5), stop, spawn)

    assert transport.read_counts == [5]
    assert spawned == [b"1-0", b"2-0"]


async def test_liveness_is_claimed_before_the_handler_starts() -> None:
    transport = FakeTransport(batches=[[record(b"1-0")]])
    stop = anyio.Event()

    def spawn(rec: Record) -> None:
        transport.events.append("spawn")
        stop.set()

    await _consume_loop(transport, Slots(), WorkerConfig(), stop, spawn)

    assert transport.events.index("mark_alive") < transport.events.index("spawn")
    assert transport.marked == [[b"1-0"]]


async def test_no_read_is_issued_without_free_slots() -> None:
    """XREADGROUP COUNT 0 is not the same thing as not reading."""
    transport = FakeTransport(batches=[[record(b"9-0")]])
    slots = Slots()
    slots.take([b"busy-1", b"busy-2"])
    stop = anyio.Event()

    async def free_a_slot_then_stop() -> None:
        await anyio.lowlevel.checkpoint()
        stop.set()
        slots.release(b"busy-1")

    async with anyio.create_task_group() as tg:
        tg.start_soon(free_a_slot_then_stop)
        await _consume_loop(
            transport, slots, WorkerConfig(concurrency=2), stop, lambda rec: None
        )

    assert transport.read_counts == []


async def test_a_freed_slot_wakes_the_loop() -> None:
    transport = FakeTransport(batches=[[record(b"1-0")]])
    slots = Slots()
    slots.take([b"busy"])
    stop = anyio.Event()

    async def free_a_slot() -> None:
        await anyio.lowlevel.checkpoint()
        slots.release(b"busy")

    async with anyio.create_task_group() as tg:
        tg.start_soon(free_a_slot)
        await _consume_loop(
            transport,
            slots,
            WorkerConfig(concurrency=1),
            stop,
            lambda rec: stop.set(),
        )

    assert transport.read_counts == [1]


async def test_successful_handler_acks_and_frees_the_slot() -> None:
    transport = FakeTransport()
    slots = Slots()
    slots.take([b"1-0"])
    seen: list[str] = []

    async def handler(envelope: Envelope) -> None:
        seen.append(envelope.task)

    await _run_one(transport, {"reindex": handler}, slots, record(b"1-0"))

    assert seen == ["reindex"]
    assert transport.acked == [("{lrs}:q:default:0", [b"1-0"])]
    assert slots.ids == set()


async def test_failing_handler_does_not_ack() -> None:
    transport = FakeTransport()
    slots = Slots()
    slots.take([b"1-0"])

    async def handler(envelope: Envelope) -> None:
        raise RuntimeError("boom")

    await _run_one(transport, {"reindex": handler}, slots, record(b"1-0"))

    assert transport.acked == []
    assert transport.cleared == []
    assert slots.ids == set()


async def test_unknown_task_is_handed_back_not_acked() -> None:
    """Mid-rollout the other version knows this task; do not ack and do not DLQ."""
    transport = FakeTransport()
    slots = Slots()
    slots.take([b"1-0"])

    await _run_one(transport, {}, slots, record(b"1-0", task="from_the_future"))

    assert transport.acked == []
    assert transport.cleared == [[b"1-0"]]
    assert slots.unhandled == {b"1-0"}


async def test_heartbeat_refreshes_exactly_what_is_in_flight() -> None:
    transport = FakeTransport()
    slots = Slots()
    slots.take([b"1-0", b"2-0"])
    sleep = stop_after(3)

    with pytest.raises(_StopLoop):
        await _heartbeat_loop(transport, slots, WorkerConfig(), sleep)  # type: ignore[arg-type]

    assert [sorted(ids) for ids in transport.refreshed] == [[b"1-0", b"2-0"]] * 3
    assert sleep.calls == [10.0, 10.0, 10.0]  # type: ignore[attr-defined]


async def test_a_handler_that_never_returns_keeps_being_refreshed() -> None:
    """The structural proof that liveness is not refreshed from the task body."""
    transport = FakeTransport()
    slots = Slots()
    config = WorkerConfig(alive_ttl_ms=300)
    sleep = stop_after(4)

    async def never_returns(envelope: Envelope) -> None:
        await anyio.sleep_forever()

    with anyio.CancelScope() as scope:
        async with anyio.create_task_group() as tg:
            slots.take([b"1-0"])
            tg.start_soon(
                _run_one, transport, {"reindex": never_returns}, slots, record(b"1-0")
            )

            async def heartbeat() -> None:
                with pytest.raises(_StopLoop):
                    await _heartbeat_loop(transport, slots, config, sleep)  # type: ignore[arg-type]
                scope.cancel()

            tg.start_soon(heartbeat)

    assert transport.refreshed == [[b"1-0"]] * 4
    assert transport.acked == []


async def test_reclaim_respects_credits_and_skips_what_it_cannot_run() -> None:
    transport = FakeTransport(
        pending=[("{lrs}:q:default:0", b"7-0"), ("{lrs}:q:default:0", b"8-0")],
        claimable={b"7-0", b"8-0"},
    )
    slots = Slots()
    slots.take([b"busy"])
    slots.unhandled.add(b"8-0")
    spawned: list[bytes] = []
    sleep = stop_after(1)

    with pytest.raises(_StopLoop):
        await _reclaim_loop(
            transport,
            slots,
            WorkerConfig(concurrency=4),
            sleep,  # type: ignore[arg-type]
            lambda rec: spawned.append(rec.entry_id),
        )

    assert transport.pending_counts == [3]
    assert spawned == [b"7-0"]


async def test_run_drains_in_flight_work_on_shutdown() -> None:
    transport = FakeTransport(batches=[[record(b"1-0")]])
    stop = anyio.Event()
    finished: list[str] = []

    async def handler(envelope: Envelope) -> None:
        stop.set()
        await anyio.lowlevel.checkpoint()
        finished.append(envelope.id)

    await run(
        transport,
        {"reindex": handler},
        WorkerConfig(concurrency=2, reclaim_interval_s=60.0, trim_interval_s=60.0),
        sleep=anyio.sleep,
        shutdown=stop,
    )

    assert finished == ["1-0"]
    assert transport.acked == [("{lrs}:q:default:0", [b"1-0"])]
    assert transport.events[0] == "ensure_group"


async def test_shutdown_wakes_a_fully_loaded_consume_loop() -> None:
    """A worker at capacity must still notice SIGTERM, however long its tasks run."""
    transport = FakeTransport(batches=[[record(b"9-0")]])
    slots = Slots()
    slots.take([b"busy"])
    stop = anyio.Event()

    async def request_shutdown() -> None:
        await anyio.lowlevel.checkpoint()
        stop.set()

    with anyio.fail_after(5):
        async with anyio.create_task_group() as tg:
            tg.start_soon(request_shutdown)
            await _consume_loop(
                transport, slots, WorkerConfig(concurrency=1), stop, lambda rec: None
            )

    assert transport.read_counts == []
    assert slots.ids == {b"busy"}


async def test_undecodable_entry_is_handed_back() -> None:
    """Nothing this worker can do with it; a peer or the DLQ milestone decides."""
    transport = FakeTransport()
    slots = Slots()
    slots.take([b"1-0"])
    broken = Record(stream="s", entry_id=b"1-0", fields={b"v": b"1"})

    await _run_one(transport, {}, slots, broken)

    assert transport.acked == []
    assert transport.cleared == [[b"1-0"]]
    assert slots.unhandled == {b"1-0"}


class FakeScheduler:
    def __init__(self, *, lead: bool = True) -> None:
        self.lead = lead
        self.promotions = 0
        self.cron_passes = 0
        self.released: list[str] = []
        self.tokens: list[str] = []

    async def hold_leadership(self, token: str, *, ttl_ms: int) -> bool:
        self.tokens.append(token)
        return self.lead

    async def release_leadership(self, token: str) -> bool:
        self.released.append(token)
        return True

    async def schedule_cron(self, jobs: Sequence[CronJob]) -> list[str]:
        self.cron_passes += 1
        return []

    async def promote(self, *, limit: int = 100) -> list[bytes]:
        self.promotions += 1
        return []


def test_leader_lease_must_outlast_the_scheduler_interval() -> None:
    """A lease shorter than the pass makes leadership flap between workers."""
    with pytest.raises(ConfigurationError, match="leader_ttl_ms"):
        WorkerConfig(leader_ttl_ms=1_000, scheduler_interval_s=5.0)


async def test_the_leader_promotes_every_pass() -> None:
    scheduler = FakeScheduler(lead=True)
    job = CronJob(name="nightly", expression="30 2 * * *", task="reindex")
    sleep = stop_after(3)

    with pytest.raises(_StopLoop):
        await _scheduler_loop(scheduler, [job], WorkerConfig(), sleep)  # type: ignore[arg-type]

    assert scheduler.promotions == 3
    assert scheduler.cron_passes == 3


async def test_a_follower_promotes_nothing() -> None:
    scheduler = FakeScheduler(lead=False)
    sleep = stop_after(2)

    with pytest.raises(_StopLoop):
        await _scheduler_loop(scheduler, [], WorkerConfig(), sleep)  # type: ignore[arg-type]

    assert scheduler.promotions == 0


async def test_leadership_is_released_when_the_loop_ends() -> None:
    """Otherwise a stopped worker holds the lease until its TTL runs out."""
    scheduler = FakeScheduler(lead=True)
    sleep = stop_after(1)

    with pytest.raises(_StopLoop):
        await _scheduler_loop(scheduler, [], WorkerConfig(), sleep)  # type: ignore[arg-type]

    assert scheduler.released == scheduler.tokens[:1]


async def test_reclaim_never_takes_back_our_own_in_flight_entry() -> None:
    """The alive key is set one await after the read; that window must be shut."""
    transport = FakeTransport(
        pending=[("{lrs}:q:default:0", b"7-0")], claimable={b"7-0"}
    )
    slots = Slots()
    slots.take([b"7-0"])
    spawned: list[bytes] = []
    sleep = stop_after(1)

    with pytest.raises(_StopLoop):
        await _reclaim_loop(
            transport,
            slots,
            WorkerConfig(concurrency=4),
            sleep,  # type: ignore[arg-type]
            lambda rec: spawned.append(rec.entry_id),
        )

    assert spawned == []
