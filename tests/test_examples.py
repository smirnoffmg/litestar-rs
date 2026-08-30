"""The examples are imported here so they cannot rot unnoticed.

An example that no longer matches the API is worse than no example: it is a
confident, wrong answer to somebody's first question.
"""

import importlib

import pytest
from litestar import Litestar

from litestar_rs.plugin import QueuePlugin

pytestmark = pytest.mark.unit

EXAMPLES = [
    "examples.basic_app",
    "examples.cron_jobs",
    "examples.broker_mode",
    "examples.deferred_publication",
]


@pytest.mark.parametrize("name", EXAMPLES)
def test_an_example_builds_its_application(name: str) -> None:
    """Building the app binds the registry, which is where startup errors land."""
    module = importlib.import_module(name)

    app = module.app
    assert isinstance(app, Litestar)
    assert any(isinstance(plugin, QueuePlugin) for plugin in app.plugins)


def test_the_basic_example_registers_its_tasks() -> None:
    from examples import basic_app

    assert set(basic_app.tasks.names) == {"reindex", "rebuild_index"}
    bound = basic_app.tasks.bound("reindex")
    assert bound.payload_fields == ("doc_id",)
    assert bound.plan.order == ("settings", "search_client")


def test_the_cron_example_schedules_only_registered_tasks() -> None:
    from examples import cron_jobs

    scheduled = {job.task for job in (cron_jobs.NIGHTLY, cron_jobs.HOUSEKEEPING)}
    assert scheduled <= set(cron_jobs.tasks.names)


def test_the_broker_example_handles_the_stream_it_subscribes_to() -> None:
    from examples import broker_mode

    plugin = next(p for p in broker_mode.app.plugins if isinstance(p, QueuePlugin))
    assert set(broker_mode.brokers) == set(plugin.config.external)
