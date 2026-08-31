"""Large arguments leave Redis, which holds the whole stream in memory."""

import pytest

from smallage.core.errors import ConfigurationError
from smallage.core.testing import CollectingEnqueuer
from smallage.litestar.registry import TaskRegistry

pytestmark = pytest.mark.unit


class MemoryPayloads:
    def __init__(self) -> None:
        self.stored: dict[str, bytes] = {}

    async def put(self, job_id: str, data: bytes) -> str:
        self.stored[job_id] = data
        return f"memory://{job_id}"

    async def get(self, reference: str) -> bytes:
        return self.stored[reference.removeprefix("memory://")]


def build(
    store: MemoryPayloads | None, threshold: int
) -> tuple[TaskRegistry, CollectingEnqueuer, list[str]]:
    registry = TaskRegistry()
    seen: list[str] = []

    @registry.task
    async def ingest(blob: str) -> None:
        seen.append(blob)

    enqueuer = CollectingEnqueuer()
    registry.bind({}, enqueuer=enqueuer, payloads=store, offload_over_bytes=threshold)
    return registry, enqueuer, seen


async def test_a_small_payload_stays_in_the_entry() -> None:
    store = MemoryPayloads()
    registry, enqueuer, _ = build(store, threshold=1024)

    await registry.enqueue("ingest", {"blob": "small"})

    [(envelope, _)] = enqueuer.enqueued
    assert envelope.payload_ref is None
    assert b"small" in envelope.payload
    assert store.stored == {}


async def test_a_large_payload_is_offloaded_and_read_back() -> None:
    store = MemoryPayloads()
    registry, enqueuer, seen = build(store, threshold=16)
    blob = "x" * 500

    await registry.enqueue("ingest", {"blob": blob})
    [(envelope, _)] = enqueuer.enqueued

    assert envelope.payload == b""
    assert envelope.payload_ref is not None
    assert len(store.stored) == 1

    await registry.execute(envelope)
    assert seen == [blob]


async def test_without_a_store_a_large_payload_is_left_in_place() -> None:
    """The transport's own limit is what refuses it; nothing is silently dropped."""
    registry, enqueuer, _ = build(None, threshold=16)

    await registry.enqueue("ingest", {"blob": "x" * 500})

    [(envelope, _)] = enqueuer.enqueued
    assert envelope.payload_ref is None
    assert len(envelope.payload) > 500


async def test_a_reference_without_a_store_is_reported_clearly() -> None:
    store = MemoryPayloads()
    registry, enqueuer, _ = build(store, threshold=16)
    await registry.enqueue("ingest", {"blob": "x" * 500})
    [(envelope, _)] = enqueuer.enqueued

    plain, _, _ = build(None, threshold=16)
    with pytest.raises(ConfigurationError, match="payload store"):
        await plain.execute(envelope)
