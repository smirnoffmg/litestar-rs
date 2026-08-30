"""The worker loops, with no Redis and no clock.

Time enters the core in exactly one place -- the injected ``Sleeper`` -- so a fake
that returns instantly and stops the loop after k turns covers the whole of it.
No test here sleeps.
"""

from collections.abc import Iterable, Sequence

import anyio
import pytest

from litestar_rs.core.cron import CronJob
from litestar_rs.core.envelope import (
    Envelope,
    Pending,
    Record,
    TaskResult,
    to_fields,
)
from litestar_rs.core.errors import ConfigurationError
from litestar_rs.core.retry import RetryPolicy
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


def pending(
    entry_id: bytes, times_delivered: int = 1, consumer: str = "peer"
) -> Pending:
    return Pending(
        stream="{lrs}:q:default:0",
        entry_id=entry_id,
        consumer=consumer,
        times_delivered=times_delivered,
    )


class FakeScheduler:
    def __init__(self, *, lead: bool = True, now: int = 1_712_345_678_901) -> None:
        self.lead = lead
        self.now = now
        self.promotions = 0
        self.cron_passes = 0
        self.released: list[str] = []
        self.tokens: list[str] = []
        self.scheduled: list[tuple[Envelope, int, str | None]] = []

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

    async def now_ms(self) -> int:
        return self.now

    async def schedule_at(
        self,
        envelope: Envelope,
        *,
        queue: str,
        when_ms: int,
        scheduled_id: str | None = None,
    ) -> str:
        self.scheduled.append((envelope, when_ms, scheduled_id))
        return scheduled_id or envelope.id


class FakeTransport:
    def __init__(
        self,
        batches: list[list[Record]] | None = None,
        pending: list[Pending] | None = None,
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
        self.buried: list[tuple[bytes, str, str]] = []
        self.dedup_claims: list[str] = []
        self.dedup_taken: set[str] = set()
        self.reported_lag: int | None = 0
        self.events: list[str] = []

    consumer = "worker-1"
    namespace = "lrs"
    group = "workers"
    queues: tuple[str, ...] = ("default",)

    def queue_of(self, stream: str) -> str:
        return "default"

    async def lag(self) -> int | None:
        return self.reported_lag

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

    async def claim_dedup(self, key: str, *, owner: str, ttl_ms: int) -> bool:
        self.dedup_claims.append(key)
        return key not in self.dedup_taken

    async def ack(self, stream: str, entry_ids: Sequence[bytes]) -> int:
        self.acked.append((stream, list(entry_ids)))
        return len(entry_ids)

    async def pending(self, *, count: int, min_idle_ms: int) -> list[Pending]:
        self.pending_counts.append(count)
        return self.pending_entries if count > 0 else []

    async def dead_letter(
        self, record: Record, *, reason: str, detail: str, times_delivered: int
    ) -> bytes:
        self.buried.append((record.entry_id, reason, detail))
        return b"dlq-1"

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

    await _run_one(
        transport,
        FakeScheduler(),
        None,
        {"reindex": handler},
        slots,
        WorkerConfig(),
        record(b"1-0"),
    )

    assert seen == ["reindex"]
    assert transport.acked == [("{lrs}:q:default:0", [b"1-0"])]
    assert slots.ids == set()


async def test_a_failing_task_is_rescheduled_with_backoff() -> None:
    """The entry is released only after the retry is safely on the clock."""
    transport = FakeTransport()
    scheduler = FakeScheduler()
    slots = Slots()
    slots.take([b"1-0"])

    async def handler(envelope: Envelope) -> None:
        raise RuntimeError("boom")

    await _run_one(
        transport,
        scheduler,
        None,
        {"reindex": handler},
        slots,
        WorkerConfig(),
        record(b"1-0"),
    )

    [(envelope, when_ms, scheduled_id)] = scheduler.scheduled
    assert envelope.attempt == 1
    assert when_ms > scheduler.now
    assert scheduled_id == "retry:1-0:1"
    assert transport.acked == [("{lrs}:q:default:0", [b"1-0"])]
    assert transport.buried == []
    assert slots.ids == set()


async def test_a_task_out_of_attempts_is_buried() -> None:
    transport = FakeTransport()
    scheduler = FakeScheduler()
    slots = Slots()
    slots.take([b"1-0"])
    spent = record(b"1-0")
    spent.fields[b"attempt"] = b"2"

    async def handler(envelope: Envelope) -> None:
        raise RuntimeError("boom")

    cfg = WorkerConfig(retry=RetryPolicy(max_attempts=3))
    await _run_one(transport, scheduler, None, {"reindex": handler}, slots, cfg, spent)

    [(entry_id, reason, detail)] = transport.buried
    assert entry_id == b"1-0"
    assert reason == "max_attempts"
    assert "RuntimeError: boom" in detail
    assert scheduler.scheduled == []
    assert transport.acked == [("{lrs}:q:default:0", [b"1-0"])]


async def test_unknown_task_is_deferred_rather_than_acked_away() -> None:
    """Mid-rollout the other version knows this task; hand it back on a delay."""
    transport = FakeTransport()
    scheduler = FakeScheduler()
    slots = Slots()
    slots.take([b"1-0"])

    await _run_one(
        transport,
        scheduler,
        None,
        {},
        slots,
        WorkerConfig(),
        record(b"1-0", task="from_the_future"),
    )

    [(envelope, when_ms, _)] = scheduler.scheduled
    assert envelope.attempt == 0, "a rollout is not an application failure"
    assert when_ms > scheduler.now
    assert transport.buried == []
    assert b"1-0" in slots.unhandled


async def test_a_task_unknown_for_too_long_is_buried() -> None:
    """The threshold is time: counting tries would bury work mid-deploy."""
    transport = FakeTransport()
    scheduler = FakeScheduler()
    slots = Slots()
    slots.take([b"1-0"])
    stale = record(b"1-0", task="from_the_future")
    stale.fields[b"enqueued_at"] = str(scheduler.now - 86_400_000).encode()

    cfg = WorkerConfig(retry=RetryPolicy(unknown_task_timeout_ms=3_600_000))
    await _run_one(transport, scheduler, None, {}, slots, cfg, stale)

    [(_, reason, detail)] = transport.buried
    assert reason == "unknown_task"
    assert detail == "from_the_future"
    assert scheduler.scheduled == []


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
                _run_one,
                transport,
                FakeScheduler(),
                None,
                {"reindex": never_returns},
                slots,
                config,
                record(b"1-0"),
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
        pending=[pending(b"7-0"), pending(b"8-0")],
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
        scheduler=FakeScheduler(),
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


async def test_an_undecodable_entry_is_buried_immediately() -> None:
    """No deployment will ever decode it, so retrying is pure noise."""
    transport = FakeTransport()
    slots = Slots()
    slots.take([b"1-0"])
    broken = Record(stream="s", entry_id=b"1-0", fields={b"v": b"1"})

    await _run_one(transport, FakeScheduler(), None, {}, slots, WorkerConfig(), broken)

    [(entry_id, reason, _)] = transport.buried
    assert entry_id == b"1-0"
    assert reason == "malformed"
    assert transport.acked == [("s", [b"1-0"])]


async def test_an_entry_delivered_too_often_is_buried_not_run() -> None:
    """Taken from this many dead owners means it kills whatever picks it up."""
    transport = FakeTransport(
        pending=[pending(b"7-0", times_delivered=9)], claimable={b"7-0"}
    )
    slots = Slots()
    spawned: list[bytes] = []
    sleep = stop_after(1)

    cfg = WorkerConfig(retry=RetryPolicy(max_deliveries=5))
    with pytest.raises(_StopLoop):
        await _reclaim_loop(
            transport,
            slots,
            cfg,
            sleep,  # type: ignore[arg-type]
            lambda rec: spawned.append(rec.entry_id),
        )

    assert spawned == []
    [(entry_id, reason, _)] = transport.buried
    assert entry_id == b"7-0"
    assert reason == "max_deliveries"


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
    transport = FakeTransport(pending=[pending(b"7-0")], claimable={b"7-0"})
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


async def test_we_never_reclaim_an_entry_redis_just_served_us() -> None:
    """An entry enters the pending list when Redis serves XREADGROUP.

    That is before the reply reaches us, so there is a window with no in-flight
    record and no liveness key yet. Ownership closes it: the entry is ours, and
    we did not inherit it from a previous run.
    """
    transport = FakeTransport(
        pending=[pending(b"7-0", consumer="worker-1")], claimable={b"7-0"}
    )
    spawned: list[bytes] = []
    sleep = stop_after(1)

    with pytest.raises(_StopLoop):
        await _reclaim_loop(
            transport,
            Slots(),
            WorkerConfig(concurrency=4),
            sleep,  # type: ignore[arg-type]
            lambda rec: spawned.append(rec.entry_id),
        )

    assert spawned == []


async def test_entries_left_by_a_previous_run_are_taken_back() -> None:
    """A restarted worker under the same name must still recover its own work."""
    transport = FakeTransport(
        pending=[pending(b"7-0", consumer="worker-1")], claimable={b"7-0"}
    )
    slots = Slots()
    slots.recoverable = {b"7-0"}
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

    assert spawned == [b"7-0"]
    assert slots.recoverable == set(), "taken once, not on every pass"


async def test_a_job_whose_dedup_key_is_taken_is_skipped_not_failed() -> None:
    """Delivery is at-least-once, so the gate sits right before the side effect."""
    transport = FakeTransport()
    transport.dedup_taken.add("invoice-42")
    slots = Slots()
    slots.take([b"1-0"])
    ran: list[str] = []

    async def handler(envelope: Envelope) -> None:
        ran.append(envelope.id)

    guarded = record(b"1-0")
    guarded.fields[b"dedup"] = b"invoice-42"

    await _run_one(
        transport,
        FakeScheduler(),
        None,
        {"reindex": handler},
        slots,
        WorkerConfig(),
        guarded,
    )

    assert ran == []
    assert transport.acked == [("{lrs}:q:default:0", [b"1-0"])]
    assert transport.buried == []


async def test_a_job_without_a_dedup_key_is_not_gated() -> None:
    transport = FakeTransport()
    slots = Slots()
    slots.take([b"1-0"])
    ran: list[str] = []

    async def handler(envelope: Envelope) -> None:
        ran.append(envelope.id)

    await _run_one(
        transport,
        FakeScheduler(),
        None,
        {"reindex": handler},
        slots,
        WorkerConfig(),
        record(b"1-0"),
    )

    assert ran == ["1-0"]
    assert transport.dedup_claims == []


class FakeResults:
    def __init__(self) -> None:
        self.stored: list[tuple[str, TaskResult, int | None]] = []

    async def store(
        self, job_id: str, result: TaskResult, *, ttl_ms: int | None = None
    ) -> None:
        self.stored.append((job_id, result, ttl_ms))

    async def get(self, job_id: str) -> TaskResult | None:
        return next((r for jid, r, _ in self.stored if jid == job_id), None)


def wants_result(entry_id: bytes, ttl_ms: int = 60_000) -> Record:
    entry = record(entry_id)
    entry.fields[b"result_ttl_ms"] = str(ttl_ms).encode()
    return entry


async def test_a_result_is_kept_only_when_someone_asked_for_one() -> None:
    """Most work is enqueued and forgotten; a key each would be spent on nobody."""
    results = FakeResults()
    slots = Slots()
    slots.take([b"1-0"])

    async def handler(envelope: Envelope) -> bytes:
        return b"done"

    await _run_one(
        FakeTransport(),
        FakeScheduler(),
        results,
        {"reindex": handler},
        slots,
        WorkerConfig(),
        record(b"1-0"),
    )

    assert results.stored == []


async def test_a_requested_result_is_kept_with_its_ttl() -> None:
    results = FakeResults()
    slots = Slots()
    slots.take([b"1-0"])

    async def handler(envelope: Envelope) -> bytes:
        return b"done"

    await _run_one(
        FakeTransport(),
        FakeScheduler(),
        results,
        {"reindex": handler},
        slots,
        WorkerConfig(),
        wants_result(b"1-0", ttl_ms=1234),
    )

    assert results.stored == [("1-0", TaskResult(ok=True, value=b"done"), 1234)]


async def test_a_buried_job_records_its_failure_for_the_waiter() -> None:
    """Otherwise a caller waiting on a dead job blocks until its own timeout."""
    results = FakeResults()
    slots = Slots()
    slots.take([b"1-0"])

    async def handler(envelope: Envelope) -> None:
        raise RuntimeError("boom")

    cfg = WorkerConfig(retry=RetryPolicy(max_attempts=1))
    await _run_one(
        FakeTransport(),
        FakeScheduler(),
        results,
        {"reindex": handler},
        slots,
        cfg,
        wants_result(b"1-0"),
    )

    [(job_id, result, _)] = results.stored
    assert job_id == "1-0"
    assert result.ok is False
    assert "RuntimeError: boom" in result.error


async def test_a_retry_does_not_record_a_result_yet() -> None:
    """The job is not over; a waiter must keep waiting."""
    results = FakeResults()
    slots = Slots()
    slots.take([b"1-0"])

    async def handler(envelope: Envelope) -> None:
        raise RuntimeError("boom")

    cfg = WorkerConfig(retry=RetryPolicy(max_attempts=5))
    await _run_one(
        FakeTransport(),
        FakeScheduler(),
        results,
        {"reindex": handler},
        slots,
        cfg,
        wants_result(b"1-0"),
    )

    assert results.stored == []
