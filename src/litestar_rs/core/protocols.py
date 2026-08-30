"""The seams other layers bind to instead of binding to a concrete subsystem."""

from collections.abc import Awaitable, Callable, Iterable, Sequence
from typing import Protocol

from litestar_rs.core.envelope import Envelope, Record


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

    async def ensure_group(self) -> None: ...
    async def read(self, count: int) -> list[Record]: ...
    async def mark_alive(self, entry_ids: Sequence[bytes], *, ttl_ms: int) -> None: ...
    async def refresh_alive(
        self, entry_ids: Iterable[bytes], *, ttl_ms: int
    ) -> None: ...
    async def clear_alive(self, entry_ids: Iterable[bytes]) -> None: ...
    async def ack(self, stream: str, entry_ids: Sequence[bytes]) -> int: ...
    async def pending(
        self, *, count: int, min_idle_ms: int
    ) -> list[tuple[str, bytes]]: ...
    async def reclaim(
        self, stream: str, entry_id: bytes, *, min_idle_ms: int, ttl_ms: int
    ) -> list[Record]: ...
    async def trim(self, *, retention_ms: int) -> None: ...


class TaskHandler(Protocol):
    async def __call__(self, envelope: Envelope, /) -> None: ...


type Sleeper = Callable[[float], Awaitable[None]]
