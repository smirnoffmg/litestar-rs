"""The examples are imported here so they cannot rot unnoticed.

An example that no longer matches the API is worse than no example: it is a
confident, wrong answer to somebody's first question.
"""

import importlib
import re
import warnings

import pytest
from litestar import Litestar

from litestar_rs.plugin import QueuePlugin

pytestmark = pytest.mark.unit

EXAMPLES = [
    "examples.basic_app",
    "examples.cron_jobs",
    "examples.broker_mode",
    "examples.deferred_publication",
    "examples.results",
]


@pytest.mark.parametrize("name", EXAMPLES)
def test_an_example_builds_its_application(name: str) -> None:
    """Building the app binds the registry, which is where startup errors land.

    Deprecations are errors here: an example that teaches a style the framework
    has deprecated is worse than no example.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
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


def test_the_broker_example_subscribes_to_what_it_handles() -> None:
    """The streams read and the handlers registered are one thing, not two."""
    from examples import broker_mode

    plugin = next(p for p in broker_mode.app.plugins if isinstance(p, QueuePlugin))

    assert set(plugin.config.brokers) == {broker_mode.ORDERS}
    assert plugin.config.external == (broker_mode.ORDERS,)


def test_the_core_example_reaches_for_no_web_framework() -> None:
    """The whole point of it: core works without Litestar, shown rather than said."""
    from pathlib import Path

    source = (
        Path(__file__).resolve().parents[1] / "examples" / "core_without_litestar.py"
    ).read_text()

    assert "litestar_rs" in source
    assert not re.search(r"^(from|import) litestar[^_]", source, re.M)


async def test_the_testing_example_actually_passes() -> None:
    """It is a worked example of testing, so it had better work.

    Run here rather than collected: these are somebody else's tests, written
    without this project's markers.
    """
    from examples import testing_your_app as example

    await example.test_the_request_queues_the_work()
    await example.test_the_task_does_its_job()


def test_the_results_example_asks_for_results() -> None:
    from examples import results

    assert results.plugin.config.result_ttl_ms == 300_000
    assert "summarise" in results.tasks.names
