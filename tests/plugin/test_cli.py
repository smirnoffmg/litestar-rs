"""The worker command: what it exposes, how it merges configuration, and what
lifecycle it puts a worker process through."""

from dataclasses import replace
from types import SimpleNamespace
from typing import Any

import click
import pytest
from click.testing import CliRunner, Result
from litestar import Litestar

from litestar_rs.core.transport import RedisStreamsTransport
from litestar_rs.core.worker import WorkerConfig
from litestar_rs.plugin.cli import plugin_of, with_overrides, workers
from litestar_rs.plugin.config import QueueConfig
from litestar_rs.plugin.plugin import QueuePlugin
from litestar_rs.plugin.registry import TaskRegistry

pytestmark = pytest.mark.unit


def config() -> QueueConfig:
    return QueueConfig(
        registry=TaskRegistry(),
        queues=("default",),
        worker=WorkerConfig(concurrency=10),
    )


def test_the_command_is_one_entrypoint_with_the_documented_options() -> None:
    run = workers.commands["run"]
    assert {option.name for option in run.params} == {
        "queues",
        "concurrency",
        "consumer",
        "health_port",
        "health_host",
    }


def test_overrides_leave_the_application_config_alone_when_absent() -> None:
    original = config()

    merged = with_overrides(original, queues=(), concurrency=None, consumer=None)

    assert merged == original


def test_overrides_apply_queues_and_concurrency() -> None:
    merged = with_overrides(
        config(), queues=("high", "low"), concurrency=20, consumer=None
    )

    assert merged.queues == ("high", "low")
    assert merged.worker.concurrency == 20


def test_the_consumer_name_is_carried_in_the_configuration() -> None:
    """The lifespan opens the queue, so the command has nowhere else to say it."""
    merged = with_overrides(config(), queues=(), concurrency=None, consumer="w-1")

    assert merged.consumer == "w-1"


def test_an_app_without_the_plugin_is_reported_clearly() -> None:
    with pytest.raises(click.ClickException, match="QueuePlugin"):
        plugin_of(Litestar(route_handlers=[]))


def test_the_plugin_is_found_on_an_app_that_registers_it() -> None:
    plugin = QueuePlugin(config())
    app = Litestar(route_handlers=[], plugins=[plugin])

    assert plugin_of(app) is plugin


def test_the_health_endpoint_is_opt_in_and_configurable() -> None:
    """A worker deployment has nothing to probe unless it is asked for."""
    run = workers.commands["run"]
    options = {option.name: option for option in run.params}

    assert not options["health_port"].required
    assert options["health_host"].default == "0.0.0.0"  # noqa: S104  # see the option


def worker_app(
    monkeypatch: pytest.MonkeyPatch, **overrides: Any
) -> tuple[Litestar, list[str]]:
    """An application whose worker run is observable without a Redis.

    The transport is built for real -- redis-py connects lazily -- and only the
    two steps that would reach the network are replaced, so what is under test is
    the order the command puts things in rather than a stand-in for it.
    """
    events: list[str] = []
    plugin = QueuePlugin(replace(config(), **overrides))

    async def opened(self: RedisStreamsTransport) -> None:
        events.append(f"open:{self.consumer}")

    async def consume(*args: Any, **kwargs: Any) -> None:
        events.append("consume")

    monkeypatch.setattr(RedisStreamsTransport, "ensure_group", opened)
    monkeypatch.setattr("litestar_rs.plugin.cli.run_with_signals", consume)

    app = Litestar(
        route_handlers=[],
        plugins=[plugin],
        on_startup=[lambda: events.append("startup")],
        on_shutdown=[lambda: events.append("shutdown")],
    )
    return app, events


def run_the_worker(app: Litestar, *args: str) -> Result:
    result = CliRunner().invoke(workers, ["run", *args], obj=SimpleNamespace(app=app))
    assert result.exit_code == 0, result.exception or result.output
    return result


def opens(events: list[str]) -> list[str]:
    return [event.removeprefix("open:") for event in events if ":" in event]


def test_a_worker_runs_the_applications_own_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Otherwise "the same application, started differently" covers the
    configuration and not the lifecycle, and a dependency closing over a
    lifespan resource resolves to an unopened one."""
    app, events = worker_app(monkeypatch)

    run_the_worker(app, "--consumer", "w-1")

    assert events == ["open:w-1", "startup", "consume", "shutdown"]


def test_declining_the_lifespan_runs_none_of_it_and_still_consumes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """For a lifespan whose work belongs to a web process alone."""
    app, events = worker_app(monkeypatch, run_app_lifespan=False)

    run_the_worker(app, "--consumer", "w-1")

    assert events == ["open:w-1", "consume"]


@pytest.mark.parametrize("run_app_lifespan", [True, False])
def test_the_queue_opens_exactly_once_either_way(
    monkeypatch: pytest.MonkeyPatch, run_app_lifespan: bool
) -> None:
    """Opening it twice leaves the worker holding two sets of connections, one
    of them unreachable, and registers a second consumer in the group."""
    app, events = worker_app(monkeypatch, run_app_lifespan=run_app_lifespan)

    run_the_worker(app, "--consumer", "w-1")

    assert opens(events) == ["w-1"]


def test_without_a_name_each_run_derives_its_own(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two workers sharing a name share a pending list, and reclaim can no longer
    tell one's unfinished work from the other's."""
    app, events = worker_app(monkeypatch)

    run_the_worker(app)
    run_the_worker(app)

    first, second = opens(events)
    assert first.startswith("worker-")
    assert first != second
