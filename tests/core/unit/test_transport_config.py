"""Configuration errors belong at construction, never in the worker loop."""

from typing import Any

import pytest
from redis.asyncio import Redis

from smallage.core.envelope import Envelope
from smallage.core.errors import ConfigurationError, PayloadTooLarge
from smallage.core.transport import RedisStreamsTransport

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
        ({"queues": ["a{b"]}, "queue"),
        ({"queues": ["a", "a"]}, "queues"),
        ({"fairness_every": -1}, "fairness_every"),
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


def test_a_worker_that_would_read_nothing_is_refused() -> None:
    """No queues and no foreign streams is an idle process; say so at startup."""
    with pytest.raises(ConfigurationError, match="read nothing"):
        build(queues=[])


def test_queues_may_be_empty_when_a_foreign_stream_is_named() -> None:
    """A deployment that only consumes somebody else's streams owns no queue."""
    transport = build(queues=[], external=["{lrs}:orders"])

    assert transport.streams == []
    assert transport.external_streams == ("{lrs}:orders",)


def test_queues_and_foreign_streams_together_are_accepted() -> None:
    transport = build(external=["{lrs}:orders"])

    assert transport.streams == ["{lrs}:q:default:0"]
    assert transport.external_streams == ("{lrs}:orders",)


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


def spy_reads(
    transport: RedisStreamsTransport, monkeypatch: pytest.MonkeyPatch
) -> list[tuple[list[str], int | None]]:
    calls: list[tuple[list[str], int | None]] = []

    async def spy(**kwargs: Any) -> None:
        calls.append((list(kwargs["streams"]), kwargs["block"]))
        return

    monkeypatch.setattr(transport.reader, "xreadgroup", spy)
    return calls


async def test_a_single_queue_reads_once_and_blocks(
    anyio_backend: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With nothing to prioritise, a sweep would just be an extra round trip."""
    transport = build(block_ms=5000)
    calls = spy_reads(transport, monkeypatch)

    await transport.read(4)

    assert calls == [(["{lrs}:q:default:0"], 5000)]


async def test_priorities_sweep_high_to_low_before_blocking(
    anyio_backend: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    """BLOCK wakes on whichever stream has work, so priority needs a sweep first."""
    transport = build(queues=["high", "low"], block_ms=5000, fairness_every=0)
    calls = spy_reads(transport, monkeypatch)

    await transport.read(4)

    assert calls == [
        (["{lrs}:q:high:0"], None),
        (["{lrs}:q:low:0"], None),
        (["{lrs}:q:high:0", "{lrs}:q:low:0"], 5000),
    ]


async def test_a_non_blocking_sweep_never_passes_block_zero(
    anyio_backend: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    """BLOCK 0 waits forever; a non-blocking read must omit BLOCK entirely."""
    transport = build(queues=["high", "low"])
    calls = spy_reads(transport, monkeypatch)

    await transport.read(1)

    assert [block for _, block in calls[:-1]] == [None, None]


async def test_a_queueless_worker_blocks_on_its_foreign_streams(
    anyio_backend: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Without a block this read is a busy loop: it is the only one left."""
    transport = build(queues=[], external=["{lrs}:orders"], block_ms=5000)
    calls = spy_reads(transport, monkeypatch)

    await transport.read(4)

    assert calls == [(["{lrs}:orders"], 5000)]


async def test_a_worker_with_queues_still_reads_foreign_streams_without_blocking(
    anyio_backend: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Blocking there would wake on the wrong stream and lose queue priority."""
    transport = build(external=["{lrs}:orders"], block_ms=5000)
    calls = spy_reads(transport, monkeypatch)

    await transport.read(4)

    assert calls == [
        (["{lrs}:orders"], None),
        (["{lrs}:q:default:0"], 5000),
    ]


async def test_a_queueless_worker_trims_nothing(
    anyio_backend: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The trim loop still runs in such a worker; it must be a no-op, not an error."""
    transport = build(queues=[], external=["{lrs}:orders"])

    async def time() -> tuple[int, int]:
        return (1, 0)

    async def refuse(*args: object, **kwargs: object) -> object:
        raise AssertionError("a worker with no queues has nothing to trim")

    monkeypatch.setattr(transport.control, "time", time)
    monkeypatch.setattr(transport.control, "xpending", refuse)
    monkeypatch.setattr(transport.control, "xtrim", refuse)

    await transport.trim(retention_ms=1000)


async def test_the_low_queue_gets_first_refusal_every_so_often(
    anyio_backend: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Strict priority starves the low queue; the pass counter bounds the wait."""
    transport = build(queues=["high", "low"], fairness_every=3)
    calls = spy_reads(transport, monkeypatch)

    for _ in range(3):
        await transport.read(1)

    first_tried = [streams[0] for streams, block in calls if block is None][::2]
    assert first_tried == [
        "{lrs}:q:high:0",
        "{lrs}:q:high:0",
        "{lrs}:q:low:0",
    ]
