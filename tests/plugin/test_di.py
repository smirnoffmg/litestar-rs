"""Resolving the application's dependency graph with no request in sight."""

from collections.abc import AsyncIterator, Iterator

import pytest
from litestar.di import Provide

from litestar_rs.core.errors import ConfigurationError
from litestar_rs.plugin.di import plan_dependencies, resolved

pytestmark = pytest.mark.unit


def settings() -> str:
    return "cfg"


def repo(settings: str) -> str:
    return f"repo({settings})"


async def test_a_graph_resolves_in_dependency_order() -> None:
    dependencies = {
        "settings": Provide(settings, sync_to_thread=False),
        "repo": Provide(repo, sync_to_thread=False),
    }

    plan = plan_dependencies(["repo"], dependencies, task="reindex")

    assert plan.order == ("settings", "repo")
    async with resolved(plan) as values:
        assert values["repo"] == "repo(cfg)"


async def test_use_cache_is_honoured() -> None:
    calls: list[int] = []

    def counted() -> int:
        calls.append(1)
        return len(calls)

    dependencies = {"counted": Provide(counted, use_cache=True, sync_to_thread=False)}
    plan = plan_dependencies(["counted"], dependencies, task="reindex")

    async with resolved(plan) as first:
        pass
    async with resolved(plan) as second:
        pass

    assert first["counted"] == second["counted"] == 1
    assert len(calls) == 1


async def test_sync_to_thread_providers_still_have_their_arguments_read() -> None:
    """The wrapper hides the signature; the plan must look through it."""
    dependencies = {
        "settings": Provide(settings, sync_to_thread=True),
        "repo": Provide(repo, sync_to_thread=True),
    }

    plan = plan_dependencies(["repo"], dependencies, task="reindex")

    assert plan.arguments["repo"] == ("settings",)
    async with resolved(plan) as values:
        assert values["repo"] == "repo(cfg)"


async def test_generator_dependencies_are_torn_down_in_reverse() -> None:
    events: list[str] = []

    def outer() -> Iterator[str]:
        events.append("outer up")
        yield "outer"
        events.append("outer down")

    async def inner(outer: str) -> AsyncIterator[str]:
        events.append("inner up")
        yield f"inner({outer})"
        events.append("inner down")

    dependencies = {
        "outer": Provide(outer),
        "inner": Provide(inner),
    }
    plan = plan_dependencies(["inner"], dependencies, task="reindex")

    async with resolved(plan) as values:
        assert values["inner"] == "inner(outer)"

    assert events == ["outer up", "inner up", "inner down", "outer down"]


async def test_teardown_runs_even_when_the_task_fails() -> None:
    closed: list[str] = []

    def session() -> Iterator[str]:
        try:
            yield "session"
        finally:
            closed.append("session")

    plan = plan_dependencies(["session"], {"session": Provide(session)}, task="reindex")

    with pytest.raises(RuntimeError):
        async with resolved(plan):
            raise RuntimeError("task blew up")

    assert closed == ["session"]


def test_a_provider_that_needs_a_request_is_refused_at_startup() -> None:
    """The worker must not discover this on the first job."""

    def needs_request(request: object) -> str:
        return "nope"

    with pytest.raises(ConfigurationError, match="only a request can supply"):
        plan_dependencies(
            ["broken"],
            {"broken": Provide(needs_request, sync_to_thread=False)},
            task="reindex",
        )


def test_an_unknown_dependency_is_refused_at_startup() -> None:
    with pytest.raises(ConfigurationError, match="does not provide"):
        plan_dependencies(["missing"], {}, task="reindex")


def test_a_cycle_is_refused_at_startup() -> None:
    def left(right: str) -> str:
        return right

    def right(left: str) -> str:
        return left

    dependencies = {
        "left": Provide(left, sync_to_thread=False),
        "right": Provide(right, sync_to_thread=False),
    }

    with pytest.raises(ConfigurationError, match="cycle"):
        plan_dependencies(["left"], dependencies, task="reindex")


def test_defaulted_parameters_are_not_mistaken_for_request_data() -> None:
    def limited(limit: int = 10) -> int:
        return limit

    plan = plan_dependencies(
        ["limited"], {"limited": Provide(limited, sync_to_thread=False)}, task="t"
    )

    assert plan.arguments["limited"] == ()
