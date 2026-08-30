"""The worker command: what it exposes and how it merges configuration."""

import click
import pytest
from litestar import Litestar

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

    merged = with_overrides(original, queues=(), concurrency=None)

    assert merged == original


def test_overrides_apply_queues_and_concurrency() -> None:
    merged = with_overrides(config(), queues=("high", "low"), concurrency=20)

    assert merged.queues == ("high", "low")
    assert merged.worker.concurrency == 20


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
