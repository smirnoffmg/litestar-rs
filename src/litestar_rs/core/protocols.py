"""The seams other layers bind to instead of binding to a concrete subsystem."""

from collections.abc import Awaitable, Callable
from typing import Protocol

from litestar_rs.core.envelope import Envelope


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


class TaskHandler(Protocol):
    async def __call__(self, envelope: Envelope) -> None: ...


type Sleeper = Callable[[float], Awaitable[None]]
