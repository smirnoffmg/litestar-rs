"""The worker supervisor.

Liveness refresh is a sibling task of every handler, never a call inside one: a
handler that never returns still gets refreshed, and a handler that has finished
stops being refreshed the moment it leaves the in-flight set. Application code
sees no heartbeat API at all, so it cannot get this wrong.
"""

import logging
import random
import signal
import traceback
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from uuid import uuid4

import anyio
from msgspec import structs

from litestar_rs.core.cron import CronJob
from litestar_rs.core.envelope import Envelope, Record, TaskResult, from_fields
from litestar_rs.core.errors import ConfigurationError, MalformedEnvelope
from litestar_rs.core.protocols import (
    BrokerHandler,
    ResultStore,
    Scheduler,
    Sleeper,
    StreamTransport,
    TaskHandler,
)
from litestar_rs.core.retry import RetryPolicy

logger = logging.getLogger(__name__)

Spawn = Callable[[Record], None]


@dataclass(frozen=True, slots=True)
class WorkerConfig:
    concurrency: int = 10
    alive_ttl_ms: int = 30_000
    min_idle_ms: int = 60_000
    reclaim_interval_s: float = 5.0
    trim_interval_s: float = 60.0
    retention_ms: int = 24 * 60 * 60 * 1000
    drain_timeout_s: float = 30.0
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    dedup_ttl_ms: int = 24 * 60 * 60 * 1000
    leader_ttl_ms: int = 15_000
    scheduler_interval_s: float = 1.0
    promote_limit: int = 100

    def __post_init__(self) -> None:
        if self.concurrency < 1:
            raise ConfigurationError(
                f"concurrency must be at least 1, got {self.concurrency}"
            )
        if self.alive_ttl_ms <= 0:
            raise ConfigurationError(
                f"alive_ttl_ms must be positive, got {self.alive_ttl_ms}"
            )
        if self.drain_timeout_s < 0:
            raise ConfigurationError(
                f"drain_timeout_s must not be negative, got {self.drain_timeout_s}"
            )
        if self.leader_ttl_ms <= self.scheduler_interval_s * 1000:
            raise ConfigurationError(
                f"leader_ttl_ms ({self.leader_ttl_ms}) must outlast "
                f"scheduler_interval_s ({self.scheduler_interval_s}s), or the lease "
                "lapses between passes and leadership flaps"
            )

    @property
    def refresh_interval_s(self) -> float:
        return self.alive_ttl_ms / 3 / 1000


def credits(concurrency: int, in_flight: int) -> int:
    """Free slots, never negative: reading with none is reading nothing."""
    return max(0, concurrency - in_flight)


@dataclass(slots=True)
class Slots:
    """In-flight entry ids and the wake-up for a freed slot.

    A set rather than a counter because the refresh loop needs the ids themselves,
    and two sources of truth about what is running is a bug factory. The event is
    replaced on every wake because anyio events are single use.
    """

    ids: set[bytes] = field(default_factory=set)
    unhandled: set[bytes] = field(default_factory=set)
    recoverable: set[bytes] = field(default_factory=set)
    """Entries this consumer name left behind in an earlier run.

    Everything else owned by our own name is either running right now or was
    handed to us microseconds ago and has not reached ``ids`` yet.
    """
    freed: anyio.Event = field(default_factory=anyio.Event)

    def take(self, entry_ids: list[bytes]) -> None:
        self.ids.update(entry_ids)

    def release(self, entry_id: bytes) -> None:
        self.ids.discard(entry_id)
        self.freed.set()

    async def wait_for_slot(self, stop: anyio.Event) -> None:
        """Wake on a freed slot or on shutdown.

        Waiting on the slot alone would deafen a fully loaded worker to SIGTERM
        for as long as its longest task runs.
        """
        async with anyio.create_task_group() as tg:
            tg.start_soon(_wake_on, self.freed, tg.cancel_scope)
            tg.start_soon(_wake_on, stop, tg.cancel_scope)
        if self.freed.is_set():
            self.freed = anyio.Event()


async def _wake_on(event: anyio.Event, scope: anyio.CancelScope) -> None:
    await event.wait()
    scope.cancel()


async def run(
    transport: StreamTransport,
    registry: Mapping[str, TaskHandler],
    config: WorkerConfig | None = None,
    *,
    scheduler: Scheduler,
    results: ResultStore | None = None,
    brokers: Mapping[str, BrokerHandler] | None = None,
    sleep: Sleeper = anyio.sleep,
    shutdown: anyio.Event | None = None,
    cron: Sequence[CronJob] = (),
) -> None:
    cfg = config or WorkerConfig()
    stop = shutdown or anyio.Event()
    slots = Slots()

    await transport.ensure_group()
    slots.recoverable = await _orphans_of_previous_run(transport, cfg)

    async with anyio.create_task_group() as supervisors:
        supervisors.start_soon(_heartbeat_loop, transport, slots, cfg, sleep)
        supervisors.start_soon(_trim_loop, transport, cfg, sleep)
        supervisors.start_soon(_scheduler_loop, scheduler, cron, cfg, sleep)

        reclaiming = anyio.CancelScope()

        async with anyio.create_task_group() as handlers:

            def spawn(record: Record) -> None:
                handlers.start_soon(
                    _run_one,
                    transport,
                    scheduler,
                    results,
                    registry,
                    brokers or {},
                    slots,
                    cfg,
                    record,
                )

            async def reclaim() -> None:
                with reclaiming:
                    await _reclaim_loop(transport, slots, cfg, sleep, spawn)

            handlers.start_soon(reclaim)
            await _consume_loop(transport, slots, cfg, stop, spawn)

            # Shutdown: stop pulling in new work, let in-flight handlers finish,
            # cut them off when the drain budget runs out. The clock starts only
            # here, so the deadline covers draining and nothing else.
            reclaiming.cancel()
            handlers.cancel_scope.deadline = anyio.current_time() + cfg.drain_timeout_s

        supervisors.cancel_scope.cancel()


async def _orphans_of_previous_run(
    transport: StreamTransport, cfg: WorkerConfig
) -> set[bytes]:
    """Entries still pending under our own consumer name before we read anything.

    Nothing has been delivered to this process yet, so whatever Redis already
    lists against our name belongs to a run that died. Those we may take back;
    anything our name acquires later we must not, because an entry enters the
    pending list the moment Redis serves XREADGROUP, which is before the reply
    reaches us and therefore before we can record it as in flight.
    """
    pending = await transport.pending(count=cfg.concurrency, min_idle_ms=0)
    return {entry.entry_id for entry in pending if entry.consumer == transport.consumer}


async def _consume_loop(
    transport: StreamTransport,
    slots: Slots,
    cfg: WorkerConfig,
    stop: anyio.Event,
    spawn: Spawn,
) -> None:
    while not stop.is_set():
        free = credits(cfg.concurrency, len(slots.ids))
        if free == 0:
            await slots.wait_for_slot(stop)
            continue
        records = await transport.read(free)
        if not records:
            continue
        entry_ids = [record.entry_id for record in records]
        # Claim the slots and the liveness keys before anything can run, or a peer
        # may reclaim an entry this worker has already taken.
        slots.take(entry_ids)
        await transport.mark_alive(entry_ids, ttl_ms=cfg.alive_ttl_ms)
        for record in records:
            spawn(record)


async def _run_one(
    transport: StreamTransport,
    scheduler: Scheduler,
    results: ResultStore | None,
    registry: Mapping[str, TaskHandler],
    brokers: Mapping[str, BrokerHandler],
    slots: Slots,
    cfg: WorkerConfig,
    record: Record,
) -> None:
    try:
        if transport.is_external(record.stream):
            await _run_broker(transport, brokers, record)
            return
        try:
            envelope = from_fields(record.fields)
        except MalformedEnvelope as exc:
            # No deployment will ever decode this. Retrying is pure noise.
            logger.exception("undecodable entry %r", record.entry_id)
            await _dead_letter(transport, record, reason="malformed", error=exc)
            return

        handler = registry.get(envelope.task)
        if handler is None:
            await _hand_back_unknown(transport, scheduler, slots, cfg, record, envelope)
            return

        if not await _may_run(transport, cfg, envelope):
            await transport.ack(record.stream, [record.entry_id])
            return

        value = await handler(envelope)
        await _keep_result(
            results, envelope, TaskResult(ok=True, value=_encoded(value))
        )
        await transport.ack(record.stream, [record.entry_id])
    except anyio.get_cancelled_exc_class():
        # Cancelled by shutdown: not an application failure. The entry stays in
        # the PEL, and dropping the alive key hands it over without a TTL wait.
        with anyio.CancelScope(shield=True):
            await transport.clear_alive([record.entry_id])
        raise
    except Exception as exc:
        logger.exception("task from entry %r failed", record.entry_id)
        await _retry_or_bury(transport, scheduler, results, cfg, record, exc)
    finally:
        slots.release(record.entry_id)


def _encoded(value: object) -> bytes:
    """Only bytes travel as a result; anything else is the plugin's business."""
    return value if isinstance(value, bytes) else b""


async def _keep_result(
    results: ResultStore | None, envelope: Envelope, result: TaskResult
) -> None:
    if results is None or envelope.result_ttl_ms is None:
        return
    await results.store(envelope.id, result, ttl_ms=envelope.result_ttl_ms)


async def _may_run(
    transport: StreamTransport, cfg: WorkerConfig, envelope: Envelope
) -> bool:
    """Gate a job on its deduplication key, if it carries one.

    Checked here rather than at enqueue: delivery is at-least-once, so the only
    place a duplicate can be stopped is immediately before the side effect.
    """
    if envelope.dedup is None:
        return True
    won = await transport.claim_dedup(
        envelope.dedup, owner=envelope.id, ttl_ms=cfg.dedup_ttl_ms
    )
    if not won:
        logger.info("skipping %r, dedup key %r taken", envelope.id, envelope.dedup)
    return won


async def _run_broker(
    transport: StreamTransport,
    brokers: Mapping[str, BrokerHandler],
    record: Record,
) -> None:
    """Handle an entry from somebody else's stream.

    There is nothing to re-enqueue -- the stream is not ours to write to -- so a
    failure simply is not acked. Redelivery is the retry, and the delivery
    ceiling is what eventually stops it.
    """
    handler = brokers.get(record.stream)
    if handler is None:
        logger.error("no broker handler for stream %r", record.stream)
        return
    try:
        await handler(record)
    except Exception:
        # Deliberately not routed through the retry path: that rewrites the job
        # into our own stream, and this entry belongs to somebody else.
        logger.exception("broker handler for %r failed", record.stream)
        return
    await transport.ack(record.stream, [record.entry_id])


async def _retry_or_bury(
    transport: StreamTransport,
    scheduler: Scheduler,
    results: ResultStore | None,
    cfg: WorkerConfig,
    record: Record,
    exc: BaseException,
) -> None:
    envelope = from_fields(record.fields)
    attempt = envelope.attempt + 1
    if cfg.retry.exhausted(attempt):
        # A waiter must be told the job is over, not left blocking until timeout.
        await _keep_result(
            results,
            envelope,
            TaskResult(ok=False, error=f"{type(exc).__name__}: {exc}"),
        )
        await _dead_letter(transport, record, reason="max_attempts", error=exc)
        return

    # Jitter keeps a batch that failed together from retrying together forever;
    # it is spacing, not a secret, so the cheap generator is the right one.
    draw = random.random()  # noqa: S311
    delay = cfg.retry.delay_ms(envelope.attempt, rand=draw)
    await _reschedule(
        transport,
        scheduler,
        record,
        structs.replace(
            envelope,
            attempt=attempt,
            history=cfg.retry.record_failure(envelope.history, envelope.attempt, exc),
        ),
        delay_ms=delay,
        scheduled_id=f"retry:{envelope.id}:{attempt}",
    )


async def _hand_back_unknown(
    transport: StreamTransport,
    scheduler: Scheduler,
    slots: Slots,
    cfg: WorkerConfig,
    record: Record,
    envelope: Envelope,
) -> None:
    """A rolling deploy is in progress and another version knows this task.

    Not a failure, so the attempt counter is untouched. The threshold is time
    rather than attempts: a deploy takes minutes, and counting tries would bury
    perfectly good work halfway through one.
    """
    age_ms = await scheduler.now_ms() - envelope.enqueued_at
    if cfg.retry.unknown_task_expired(age_ms=age_ms):
        logger.error("task %r still unknown after %dms, burying", envelope.task, age_ms)
        await _dead_letter(
            transport, record, reason="unknown_task", error=None, detail=envelope.task
        )
        return

    logger.warning("unknown task %r, deferring for a peer", envelope.task)
    slots.unhandled.add(record.entry_id)
    await _reschedule(
        transport,
        scheduler,
        record,
        envelope,
        delay_ms=cfg.retry.unknown_task_backoff_ms,
        scheduled_id=f"unknown:{envelope.id}:{age_ms // 1000}",
    )


async def _reschedule(
    transport: StreamTransport,
    scheduler: Scheduler,
    record: Record,
    envelope: Envelope,
    *,
    delay_ms: int,
    scheduled_id: str,
) -> None:
    """Put the work back on the clock, then let go of the current entry.

    Scheduling before acking is deliberate. A crash in between duplicates the
    job, which at-least-once already allows; the other order would lose it.
    """
    when_ms = await scheduler.now_ms() + delay_ms
    await scheduler.schedule_at(
        envelope,
        queue=transport.queue_of(record.stream),
        when_ms=when_ms,
        scheduled_id=scheduled_id,
    )
    await transport.ack(record.stream, [record.entry_id])


async def _dead_letter(
    transport: StreamTransport,
    record: Record,
    *,
    reason: str,
    error: BaseException | None,
    detail: str = "",
    times_delivered: int = 1,
) -> None:
    """Park it, then ack. Writing first means a crash cannot lose the evidence."""
    if error is not None:
        detail = "".join(
            traceback.format_exception(type(error), error, error.__traceback__)
        )
    await transport.dead_letter(
        record, reason=reason, detail=detail, times_delivered=times_delivered
    )
    await transport.ack(record.stream, [record.entry_id])


async def _heartbeat_loop(
    transport: StreamTransport, slots: Slots, cfg: WorkerConfig, sleep: Sleeper
) -> None:
    while True:
        await transport.refresh_alive(list(slots.ids), ttl_ms=cfg.alive_ttl_ms)
        await sleep(cfg.refresh_interval_s)


async def _reclaim_loop(
    transport: StreamTransport,
    slots: Slots,
    cfg: WorkerConfig,
    sleep: Sleeper,
    spawn: Spawn,
) -> None:
    while True:
        free = credits(cfg.concurrency, len(slots.ids))
        candidates = await transport.pending(count=free, min_idle_ms=cfg.min_idle_ms)
        for candidate in candidates:
            # Skipping our own in-flight ids is not an optimisation. The alive key
            # is written one await after the read, and reclaiming inside that
            # window would hand this worker its own entry a second time.
            if candidate.entry_id in slots.unhandled:
                continue
            if candidate.entry_id in slots.ids:
                continue
            if candidate.consumer == transport.consumer and (
                candidate.entry_id not in slots.recoverable
            ):
                # Ours, and not left over from a previous run: either running now
                # or served to us so recently that the reply has not landed.
                continue
            slots.recoverable.discard(candidate.entry_id)
            records = await transport.reclaim(
                candidate.stream,
                candidate.entry_id,
                min_idle_ms=cfg.min_idle_ms,
                ttl_ms=cfg.alive_ttl_ms,
            )
            if not records:
                continue
            if cfg.retry.over_delivered(candidate.times_delivered):
                # Taken from this many dead owners means the entry is killing
                # whatever picks it up. Backing off would only spread the damage.
                for record in records:
                    logger.error(
                        "entry %r delivered %d times, burying",
                        record.entry_id,
                        candidate.times_delivered,
                    )
                    await _dead_letter(
                        transport,
                        record,
                        reason="max_deliveries",
                        error=None,
                        detail=f"delivered {candidate.times_delivered} times",
                        times_delivered=candidate.times_delivered,
                    )
                continue
            slots.take([record.entry_id for record in records])
            for record in records:
                spawn(record)
        await sleep(cfg.reclaim_interval_s)


async def _scheduler_loop(
    scheduler: Scheduler,
    cron: Sequence[CronJob],
    cfg: WorkerConfig,
    sleep: Sleeper,
) -> None:
    """Promote due jobs while this worker holds the lease.

    A lost lease is not an error: the new holder resumes from the same ZSET, and
    the promotion script makes a double pass a no-op.
    """
    token = uuid4().hex
    try:
        while True:
            if await scheduler.hold_leadership(token, ttl_ms=cfg.leader_ttl_ms):
                if cron:
                    await scheduler.schedule_cron(cron)
                await scheduler.promote(limit=cfg.promote_limit)
            await sleep(cfg.scheduler_interval_s)
    finally:
        with anyio.CancelScope(shield=True):
            await scheduler.release_leadership(token)


async def _trim_loop(
    transport: StreamTransport, cfg: WorkerConfig, sleep: Sleeper
) -> None:
    while True:
        await transport.trim(retention_ms=cfg.retention_ms)
        await sleep(cfg.trim_interval_s)


async def run_with_signals(
    transport: StreamTransport,
    registry: Mapping[str, TaskHandler],
    config: WorkerConfig | None = None,
    *,
    scheduler: Scheduler,
    results: ResultStore | None = None,
    brokers: Mapping[str, BrokerHandler] | None = None,
    cron: Sequence[CronJob] = (),
) -> None:
    """Run until SIGTERM or SIGINT; a second signal means now, not soon."""
    stop = anyio.Event()
    async with anyio.create_task_group() as tg:
        tg.start_soon(_watch_signals, stop, tg.cancel_scope)
        await run(
            transport,
            registry,
            config,
            scheduler=scheduler,
            results=results,
            brokers=brokers,
            cron=cron,
            shutdown=stop,
        )
        tg.cancel_scope.cancel()


async def _watch_signals(stop: anyio.Event, scope: anyio.CancelScope) -> None:
    with anyio.open_signal_receiver(signal.SIGTERM, signal.SIGINT) as signals:
        async for _ in signals:
            if stop.is_set():
                scope.cancel()
                return
            stop.set()
