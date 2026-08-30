"""Conformance is proved by mypy; the assertions only keep pytest honest."""

import anyio
import pytest

from litestar_rs.core.envelope import Envelope, JsonCodec
from litestar_rs.core.protocols import Codec, Sleeper, TaskHandler

pytestmark = pytest.mark.unit


async def _handler(envelope: Envelope) -> None:
    return None


def test_implementations_satisfy_their_protocols() -> None:
    codec: Codec = JsonCodec()
    handler: TaskHandler = _handler
    sleeper: Sleeper = anyio.sleep
    assert (codec, handler, sleeper) is not None
