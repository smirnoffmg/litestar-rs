"""The seams other layers bind to instead of binding to a concrete subsystem."""

from collections.abc import Awaitable, Callable, Iterable, Sequence
from typing import Protocol

from smallage.core.cron import CronJob
from smallage.core.envelope import Envelope, Pending, Record, TaskResult


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
    namespace: str
    group: str
    queues: tuple[str, ...]

    def queue_of(self, stream: str) -> str: ...
    def is_external(self, stream: str) -> bool: ...
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
    async def lag(self) -> int | None: ...


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
    async def __call__(self, envelope: Envelope, /) -> object: ...


class BrokerHandler(Protocol):
    """Handles an entry this library did not write.

    Broker mode gets the raw record: the payload is in somebody else's format,
    so there is no envelope to decode and no result to store.
    """

    async def __call__(self, record: Record, /) -> None: ...


class PayloadStore(Protocol):
    """Somewhere large arguments live instead of in Redis.

    Redis holds the stream in memory, so a payload measured in megabytes is a
    direct route to an out-of-memory kill. Above a threshold the arguments go
    here and the entry carries a reference.
    """

    async def put(self, job_id: str, data: bytes) -> str: ...
    async def get(self, reference: str) -> bytes: ...


class ResultStore(Protocol):
    """Where a job's outcome goes when somebody is waiting for one."""

    async def store(
        self, job_id: str, result: TaskResult, *, ttl_ms: int | None = None
    ) -> None: ...
    async def get(self, job_id: str) -> TaskResult | None: ...


type Sleeper = Callable[[float], Awaitable[None]]
