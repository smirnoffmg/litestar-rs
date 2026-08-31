"""Tasks, their payloads and what the application injects into them."""

from collections.abc import Iterator
from uuid import UUID, uuid4

import pytest
from litestar.di import Provide

from smallage.core.cron import CronJob, occurrence_envelope
from smallage.core.envelope import Envelope
from smallage.core.errors import ConfigurationError
from smallage.core.testing import CollectingEnqueuer
from smallage.litestar.registry import TaskRegistry

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
    from smallage.core.deferred import DeferredEnqueuer

    registry, enqueuer, _ = build()
    deferred = DeferredEnqueuer(enqueuer)

    async with deferred.active():
        await registry.enqueue("reindex", {"doc_id": uuid4()})
        assert enqueuer.enqueued == [], "nothing may reach Redis before the commit"
        assert len(deferred.pending) == 1

    await deferred.flush()

    assert [envelope.task for envelope, _ in enqueuer.enqueued] == ["reindex"]


async def test_a_rolled_back_unit_of_work_publishes_nothing() -> None:
    from smallage.core.deferred import DeferredEnqueuer

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


async def test_a_task_receives_only_the_dependencies_it_named() -> None:
    """Resolving one dependency may require others the task never asked for.

    `search_client` needs `settings`; the task asked for `search_client` alone
    and must be called with exactly that.
    """
    registry = TaskRegistry()
    seen: list[tuple[str, ...]] = []

    def settings() -> str:
        return "cfg"

    def search_client(settings: str) -> str:
        return f"client({settings})"

    @registry.task
    async def reindex(doc_id: UUID, search_client: str) -> None:
        seen.append((str(doc_id), search_client))

    enqueuer = CollectingEnqueuer()
    registry.bind(
        {
            "settings": Provide(settings, sync_to_thread=False),
            "search_client": Provide(search_client, sync_to_thread=False),
        },
        enqueuer=enqueuer,
    )
    doc_id = uuid4()
    await registry.enqueue("reindex", {"doc_id": doc_id})
    [(envelope, _)] = enqueuer.enqueued

    await registry.execute(envelope)

    assert seen == [(str(doc_id), "client(cfg)")]
    assert registry.bound("reindex").plan.order == ("settings", "search_client")
    assert registry.bound("reindex").injected == ("search_client",)


async def test_a_cron_job_with_no_arguments_decodes() -> None:
    """A task taking nothing still has an argument struct, and null is not one."""
    registry = TaskRegistry()
    ran: list[int] = []

    @registry.task
    async def expire_sessions() -> None:
        ran.append(1)

    registry.bind({}, enqueuer=CollectingEnqueuer())
    job = CronJob(
        name="housekeeping", expression="*/15 * * * *", task="expire_sessions"
    )

    await registry.execute(occurrence_envelope(job, 1712345678901))

    assert ran == [1]


async def test_a_cron_job_can_carry_arguments() -> None:
    registry = TaskRegistry()
    seen: list[int] = []

    @registry.task
    async def trim(older_than_days: int) -> None:
        seen.append(older_than_days)

    registry.bind({}, enqueuer=CollectingEnqueuer())
    job = CronJob(
        name="trim",
        expression="0 3 * * *",
        task="trim",
        payload=b'{"older_than_days": 30}',
    )

    await registry.execute(occurrence_envelope(job, 1712345678901))

    assert seen == [30]
