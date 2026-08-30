"""The worker supervisor.

Liveness refresh is a sibling task of every handler, never a call inside one: a
handler that never returns still gets refreshed, and a handler that has finished
stops being refreshed the moment it leaves the in-flight set. Application code
sees no heartbeat API at all, so it cannot get this wrong.
"""

import logging
import signal
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field

import anyio

from litestar_rs.core.envelope import Record, from_fields
from litestar_rs.core.errors import ConfigurationError, MalformedEnvelope
from litestar_rs.core.protocols import Sleeper, StreamTransport, TaskHandler

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
    sleep: Sleeper = anyio.sleep,
    shutdown: anyio.Event | None = None,
) -> None:
    cfg = config or WorkerConfig()
    stop = shutdown or anyio.Event()
    slots = Slots()

    await transport.ensure_group()

    async with anyio.create_task_group() as supervisors:
        supervisors.start_soon(_heartbeat_loop, transport, slots, cfg, sleep)
        supervisors.start_soon(_trim_loop, transport, cfg, sleep)

        reclaiming = anyio.CancelScope()

        async with anyio.create_task_group() as handlers:

            def spawn(record: Record) -> None:
                handlers.start_soon(_run_one, transport, registry, slots, record)

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
    registry: Mapping[str, TaskHandler],
    slots: Slots,
    record: Record,
) -> None:
    try:
        try:
            envelope = from_fields(record.fields)
        except MalformedEnvelope:
            logger.exception("undecodable entry %r", record.entry_id)
            await _hand_over(transport, slots, record)
            return

        handler = registry.get(envelope.task)
        if handler is None:
            # Another version of the app is mid-rollout and knows this task. Do
            # not ack and do not DLQ: hand it back so a peer can run it.
            logger.warning("unknown task %r, handing back", envelope.task)
            await _hand_over(transport, slots, record)
            return

        await handler(envelope)
        await transport.ack(record.stream, [record.entry_id])
    except anyio.get_cancelled_exc_class():
        # Cancelled by shutdown: not an application failure. The entry stays in
        # the PEL, and dropping the alive key hands it over without a TTL wait.
        with anyio.CancelScope(shield=True):
            await transport.clear_alive([record.entry_id])
        raise
    except Exception:
        # Not acked. The alive key lapses and a peer reclaims it; retry counting
        # and the DLQ threshold belong to the retry milestone.
        logger.exception("task from entry %r failed", record.entry_id)
    finally:
        slots.release(record.entry_id)


async def _hand_over(transport: StreamTransport, slots: Slots, record: Record) -> None:
    """Give an entry back at once, and stop this worker from re-claiming it."""
    slots.unhandled.add(record.entry_id)
    await transport.clear_alive([record.entry_id])


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
        for stream, entry_id in candidates:
            if entry_id in slots.unhandled:
                continue
            records = await transport.reclaim(
                stream,
                entry_id,
                min_idle_ms=cfg.min_idle_ms,
                ttl_ms=cfg.alive_ttl_ms,
            )
            if not records:
                continue
            slots.take([record.entry_id for record in records])
            for record in records:
                spawn(record)
        await sleep(cfg.reclaim_interval_s)


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
) -> None:
    """Run until SIGTERM or SIGINT; a second signal means now, not soon."""
    stop = anyio.Event()
    async with anyio.create_task_group() as tg:
        tg.start_soon(_watch_signals, stop, tg.cancel_scope)
        await run(transport, registry, config, shutdown=stop)
        tg.cancel_scope.cancel()


async def _watch_signals(stop: anyio.Event, scope: anyio.CancelScope) -> None:
    with anyio.open_signal_receiver(signal.SIGTERM, signal.SIGINT) as signals:
        async for _ in signals:
            if stop.is_set():
                scope.cancel()
                return
            stop.set()
