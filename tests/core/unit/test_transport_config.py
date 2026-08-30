"""Configuration errors belong at construction, never in the worker loop."""

from typing import Any

import pytest
from redis.asyncio import Redis

from litestar_rs.core.envelope import Envelope
from litestar_rs.core.errors import ConfigurationError, PayloadTooLarge
from litestar_rs.core.transport import RedisStreamsTransport

pytestmark = pytest.mark.unit

URL = "redis://localhost:6379/0"


def client(**kwargs: Any) -> Redis:
    """redis-py connects lazily, so nothing here touches the network."""
    return Redis.from_url(URL, **kwargs)


def build(**overrides: Any) -> RedisStreamsTransport:
    kwargs: dict[str, Any] = {
        "reader": client(socket_timeout=30.0),
        "control": client(),
        "consumer": "worker-1",
    }
    return RedisStreamsTransport(**(kwargs | overrides))


def test_defaults_are_accepted() -> None:
    assert build().streams == ["{lrs}:q:default:0"]


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({"namespace": "a:b"}, "namespace"),
        ({"queue": "a{b"}, "queue"),
        ({"shards": 0}, "shards"),
        ({"consumer": ""}, "consumer"),
        ({"max_payload_bytes": 0}, "max_payload_bytes"),
    ],
)
def test_invalid_values_name_their_field(
    overrides: dict[str, Any], expected: str
) -> None:
    with pytest.raises(ConfigurationError, match=expected):
        build(**overrides)


def test_reader_and_control_must_be_separate_clients() -> None:
    """A blocking XREADGROUP holds its connection; ack must not queue behind it."""
    shared = client(socket_timeout=30.0)
    with pytest.raises(ConfigurationError, match="separate"):
        RedisStreamsTransport(reader=shared, control=shared, consumer="w")


def test_decode_responses_is_rejected() -> None:
    with pytest.raises(ConfigurationError, match="decode_responses"):
        build(control=client(decode_responses=True))


def test_reader_socket_timeout_must_outlast_the_block_window() -> None:
    """Otherwise every healthy blocking read dies on the socket timeout."""
    with pytest.raises(ConfigurationError, match="socket_timeout"):
        build(reader=client(socket_timeout=1.0), block_ms=5000)


def test_reader_socket_timeout_is_required() -> None:
    """Without it a hung read after a Sentinel failover never returns."""
    with pytest.raises(ConfigurationError, match="socket_timeout"):
        build(reader=client())


async def test_read_at_zero_credits_issues_no_command(
    anyio_backend: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    """XREADGROUP COUNT 0 is not the same thing as not reading."""
    transport = build()
    calls: list[object] = []

    async def spy(*args: object, **kwargs: object) -> object:
        calls.append(args)
        raise AssertionError("must not read without free slots")

    monkeypatch.setattr(transport.reader, "xreadgroup", spy)
    assert await transport.read(0) == []
    assert await transport.read(-3) == []
    assert calls == []


async def test_payload_over_the_limit_is_refused(anyio_backend: object) -> None:
    """Caught before any command is sent: an oversized payload is an OOM in Redis."""
    transport = build(max_payload_bytes=8)
    envelope = Envelope(
        id="job-1", task="reindex", payload=b"x" * 9, enqueued_at=1712345678901
    )
    with pytest.raises(PayloadTooLarge, match="reindex"):
        await transport.enqueue(envelope, queue="default")
