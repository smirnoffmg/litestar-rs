"""Tasks, their payloads and what the application injects into them."""

from collections.abc import Iterator
from uuid import UUID, uuid4

import pytest
from litestar.di import Provide

from litestar_rs.core.envelope import Envelope
from litestar_rs.core.errors import ConfigurationError
from litestar_rs.core.testing import CollectingEnqueuer
from litestar_rs.plugin.registry import TaskRegistry

pytestmark = pytest.mark.unit


def settings() -> str:
    return "cfg"


def build() -> tuple[TaskRegistry, CollectingEnqueuer, list[tuple[UUID, str]]]:
    registry = TaskRegistry()
    seen: list[tuple[UUID, str]] = []

    @registry.task
    async def reindex(doc_id: UUID, settings: str) -> None:
        seen.append((doc_id, settings))

    enqueuer = CollectingEnqueuer()
    registry.bind(
        {"settings": Provide(settings, sync_to_thread=False)}, enqueuer=enqueuer
    )
    return registry, enqueuer, seen


async def test_dependencies_are_not_part_of_the_payload() -> None:
    registry, enqueuer, _ = build()
    doc_id = uuid4()

    await registry.enqueue("reindex", {"doc_id": doc_id})

    [(envelope, queue)] = enqueuer.enqueued
    assert queue == "default"
    assert str(doc_id).encode() in envelope.payload
    assert b"settings" not in envelope.payload


async def test_a_round_trip_injects_and_calls() -> None:
    registry, enqueuer, seen = build()
    doc_id = uuid4()
    await registry.enqueue("reindex", {"doc_id": doc_id})
    [(envelope, _)] = enqueuer.enqueued

    await registry.execute(envelope)

    assert seen == [(doc_id, "cfg")]


async def test_a_bad_argument_fails_at_enqueue_not_in_the_worker() -> None:
    registry, _, _ = build()

    with pytest.raises(ConfigurationError, match="bad arguments"):
        await registry.enqueue("reindex", {"wrong_name": uuid4()})


async def test_payload_types_are_checked_on_the_way_out() -> None:
    """Decoding is the application's own msgspec machinery, so this is caught."""
    registry, enqueuer, _ = build()
    await registry.enqueue("reindex", {"doc_id": uuid4()})
    [(envelope, _)] = enqueuer.enqueued
    broken = Envelope(
        id=envelope.id,
        task=envelope.task,
        payload=b'{"doc_id": "not a uuid"}',
        enqueued_at=envelope.enqueued_at,
    )

    with pytest.raises(Exception, match="doc_id"):
        await registry.execute(broken)


def test_a_task_registered_twice_is_refused() -> None:
    registry = TaskRegistry()

    @registry.task
    async def reindex() -> None: ...

    with pytest.raises(ConfigurationError, match="registered twice"):

        @registry.task(name="reindex")
        async def other() -> None: ...


def test_an_unannotated_payload_argument_is_refused_at_startup() -> None:
    registry = TaskRegistry()

    @registry.task
    async def reindex(doc_id) -> None:  # type: ignore[no-untyped-def]  # the defect
        ...

    with pytest.raises(ConfigurationError, match="without an "):
        registry.bind({}, enqueuer=CollectingEnqueuer())


def test_a_provider_needing_a_request_stops_the_worker_starting() -> None:
    registry = TaskRegistry()

    def needs_request(request: object) -> str:
        return "nope"

    @registry.task
    async def reindex(broken: str) -> None: ...

    with pytest.raises(ConfigurationError, match="only a request can supply"):
        registry.bind(
            {"broken": Provide(needs_request, sync_to_thread=False)},
            enqueuer=CollectingEnqueuer(),
        )


async def test_generator_dependencies_are_torn_down_around_the_task() -> None:
    registry = TaskRegistry()
    events: list[str] = []

    def session() -> Iterator[str]:
        events.append("open")
        yield "session"
        events.append("close")

    @registry.task
    async def reindex(session: str) -> None:
        events.append(f"run({session})")

    enqueuer = CollectingEnqueuer()
    registry.bind({"session": Provide(session)}, enqueuer=enqueuer)
    await registry.enqueue("reindex", {})
    [(envelope, _)] = enqueuer.enqueued

    await registry.execute(envelope)

    assert events == ["open", "run(session)", "close"]


async def test_calling_a_task_directly_still_works() -> None:
    """The decorator returns something with the original signature."""
    registry = TaskRegistry()
    ran: list[int] = []

    @registry.task
    async def reindex(count: int) -> None:
        ran.append(count)

    await reindex(3)

    assert ran == [3]


async def test_a_unit_of_work_can_hold_a_task_until_it_commits() -> None:
    """Publication has to be able to wait for the transaction that justified it."""
    from litestar_rs.core.deferred import DeferredEnqueuer

    registry, enqueuer, _ = build()
    deferred = DeferredEnqueuer(enqueuer)

    async with deferred.active():
        await registry.enqueue("reindex", {"doc_id": uuid4()})
        assert enqueuer.enqueued == [], "nothing may reach Redis before the commit"
        assert len(deferred.pending) == 1

    await deferred.flush()

    assert [envelope.task for envelope, _ in enqueuer.enqueued] == ["reindex"]


async def test_a_rolled_back_unit_of_work_publishes_nothing() -> None:
    from litestar_rs.core.deferred import DeferredEnqueuer

    registry, enqueuer, _ = build()
    deferred = DeferredEnqueuer(enqueuer)

    async with deferred.active():
        await registry.enqueue("reindex", {"doc_id": uuid4()})
    deferred.discard()
    await deferred.flush()

    assert enqueuer.enqueued == []


async def test_asking_for_a_result_without_a_store_says_so() -> None:
    registry, _, _ = build()

    with pytest.raises(ConfigurationError, match="result store"):
        await registry.result("job-1")


def test_an_unbound_task_is_reported_rather_than_missing() -> None:
    """Before the application starts, nothing is settled yet."""
    registry = TaskRegistry()

    @registry.task
    async def reindex() -> None: ...

    with pytest.raises(ConfigurationError, match="not bound"):
        registry.bound("reindex")
