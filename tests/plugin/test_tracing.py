"""The HTTP request that enqueued a job and the job itself stay one trace."""

import pytest
from litestar.di import Provide

from litestar_rs.core.testing import CollectingEnqueuer
from litestar_rs.plugin.registry import TaskRegistry
from litestar_rs.plugin.tracing import current_traceparent

pytestmark = pytest.mark.unit

TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"


def build(
    traceparent: str | None,
) -> tuple[TaskRegistry, CollectingEnqueuer, list[str | None]]:
    registry = TaskRegistry()
    seen: list[str | None] = []

    @registry.task
    async def reindex() -> None:
        seen.append(current_traceparent.get())

    enqueuer = CollectingEnqueuer()
    registry.bind({}, enqueuer=enqueuer, traceparent=lambda: traceparent)
    return registry, enqueuer, seen


async def test_the_traceparent_travels_beside_the_payload() -> None:
    registry, enqueuer, _ = build(TRACEPARENT)

    await registry.enqueue("reindex", {})

    [(envelope, _)] = enqueuer.enqueued
    assert envelope.traceparent == TRACEPARENT
    assert b"traceparent" not in envelope.payload


async def test_the_worker_restores_it_for_the_handler() -> None:
    registry, enqueuer, seen = build(TRACEPARENT)
    await registry.enqueue("reindex", {})
    [(envelope, _)] = enqueuer.enqueued

    await registry.execute(envelope)

    assert seen == [TRACEPARENT]
    assert current_traceparent.get() is None, "must not leak past the task"


async def test_an_untraced_application_writes_no_field() -> None:
    registry, enqueuer, seen = build(None)
    await registry.enqueue("reindex", {})
    [(envelope, _)] = enqueuer.enqueued

    await registry.execute(envelope)

    assert envelope.traceparent is None
    assert seen == [None]


def test_a_provider_may_also_read_it() -> None:
    """Providers run inside the task's context, so they see the same trace."""
    registry = TaskRegistry()
    seen: list[str | None] = []

    def tracer() -> str:
        seen.append(current_traceparent.get())
        return "tracer"

    @registry.task
    async def reindex(tracer: str) -> None: ...

    registry.bind(
        {"tracer": Provide(tracer, sync_to_thread=False)},
        enqueuer=CollectingEnqueuer(),
        traceparent=lambda: TRACEPARENT,
    )
    assert registry.bound("reindex").plan.order == ("tracer",)
