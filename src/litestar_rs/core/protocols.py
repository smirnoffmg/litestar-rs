"""The seams other layers bind to instead of binding to a concrete subsystem."""

from collections.abc import Awaitable, Callable, Iterable, Sequence
from typing import Protocol

from litestar_rs.core.cron import CronJob
from litestar_rs.core.envelope import Envelope, Pending, Record


class Codec(Protocol):
    """Payload encoder/decoder. The plugin substitutes the app's msgspec pair."""

    def encode(self, value: object, /) -> bytes: ...
    def decode(self, raw: bytes, /) -> object: ...


class Enqueuer(Protocol):
    """Publishing seam.

    Deferred publication (flushing on the SQLAlchemy ``after_commit`` hook) and the
    eager mode used in application unit tests are implementations of this protocol,
    not forks of the transport. The scheduler binds here too, so that it never has
    to import a concrete transport.
    """

    async def enqueue(self, envelope: Envelope, *, queue: str) -> bytes: ...


class StreamTransport(Protocol):
    """Everything the worker needs from a transport.

    The worker binds here rather than to a concrete transport, which is what lets
    the consume loop and the supervisor loops be tested without a Redis at all.
    """

    consumer: str

    def queue_of(self, stream: str) -> str: ...
    async def ensure_group(self) -> None: ...
    async def read(self, count: int) -> list[Record]: ...
    async def mark_alive(self, entry_ids: Sequence[bytes], *, ttl_ms: int) -> None: ...
    async def refresh_alive(
        self, entry_ids: Iterable[bytes], *, ttl_ms: int
    ) -> None: ...
    async def clear_alive(self, entry_ids: Iterable[bytes]) -> None: ...
    async def claim_dedup(self, key: str, *, owner: str, ttl_ms: int) -> bool: ...
    async def ack(self, stream: str, entry_ids: Sequence[bytes]) -> int: ...
    async def pending(self, *, count: int, min_idle_ms: int) -> list[Pending]: ...
    async def dead_letter(
        self, record: Record, *, reason: str, detail: str, times_delivered: int
    ) -> bytes: ...
    async def reclaim(
        self, stream: str, entry_id: bytes, *, min_idle_ms: int, ttl_ms: int
    ) -> list[Record]: ...
    async def trim(self, *, retention_ms: int) -> None: ...


class Scheduler(Protocol):
    """What the worker needs from a scheduler, and nothing more."""

    async def now_ms(self) -> int: ...
    async def schedule_at(
        self,
        envelope: Envelope,
        *,
        queue: str,
        when_ms: int,
        scheduled_id: str | None = None,
    ) -> str: ...
    async def hold_leadership(self, token: str, *, ttl_ms: int) -> bool: ...
    async def release_leadership(self, token: str) -> bool: ...
    async def schedule_cron(self, jobs: Sequence[CronJob]) -> list[str]: ...
    async def promote(self, *, limit: int = 100) -> list[bytes]: ...


class TaskHandler(Protocol):
    async def __call__(self, envelope: Envelope, /) -> None: ...


type Sleeper = Callable[[float], Awaitable[None]]
