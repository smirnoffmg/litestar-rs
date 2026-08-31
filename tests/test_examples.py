"""The examples are imported here so they cannot rot unnoticed.

An example that no longer matches the API is worse than no example: it is a
confident, wrong answer to somebody's first question.
"""

import importlib
import re
import warnings

import pytest

from litestar import Litestar
from smallage.litestar import QueuePlugin

pytestmark = pytest.mark.unit

EXAMPLES = [
    "examples.basic_app",
    "examples.cron_jobs",
    "examples.broker_mode",
    "examples.broker_only",
    "examples.health_endpoint",
    "examples.deferred_publication",
    "examples.results",
    "examples.retries_and_dlq",
    "examples.priorities",
    "examples.deduplication",
    "examples.delayed_jobs",
    "examples.large_payloads",
    "examples.sync_tasks_and_timeouts",
    "examples.tracing",
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


def test_the_broker_only_example_owns_no_queue() -> None:
    """The whole point of it: a deployment that reads nothing of its own."""
    from examples import broker_only

    plugin = next(p for p in broker_only.app.plugins if isinstance(p, QueuePlugin))

    assert plugin.config.queues == ()
    assert plugin.config.external == (broker_only.PAYMENTS,)


def test_the_health_example_serves_routes_of_its_own() -> None:
    """The plugin adds none, so every route here is one the example wrote.

    That the handlers actually answer is asserted against a real Redis, in
    `tests/litestar/test_plugin.py` -- this file has none.
    """
    from examples import health_endpoint

    app = health_endpoint.app
    plugin = next(p for p in app.plugins if isinstance(p, QueuePlugin))
    bare = Litestar(route_handlers=[], plugins=[plugin])
    served = {route.path for route in app.routes}

    assert served - {route.path for route in bare.routes} == {
        "/health/queue",
        "/readyz",
    }


def test_the_core_example_reaches_for_no_web_framework() -> None:
    """The whole point of it: core works without Litestar, shown rather than said."""
    from pathlib import Path

    source = (
        Path(__file__).resolve().parents[1] / "examples" / "core_without_litestar.py"
    ).read_text()

    assert "smallage" in source
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


def test_every_example_module_is_covered_here() -> None:
    """An example nobody imports is an example nobody notices going stale."""
    from pathlib import Path

    directory = Path(__file__).resolve().parents[1] / "examples"
    modules = {
        f"examples.{path.stem}"
        for path in directory.glob("*.py")
        if path.stem != "__init__"
    }
    # These two are not applications, and have tests of their own above.
    standalone = {"examples.core_without_litestar", "examples.testing_your_app"}

    assert modules - standalone == set(EXAMPLES)


def test_every_example_is_listed_in_the_readme() -> None:
    """An example nobody lists is an example nobody finds.

    The table presents itself as the index of the directory, so half a table is
    worse than none: a reader takes what is there for the whole of it.
    """
    import re
    from pathlib import Path

    directory = Path(__file__).resolve().parents[1] / "examples"
    readme = (directory / "README.md").read_text()
    on_disk = {p.name for p in directory.glob("*.py") if p.stem != "__init__"}

    assert on_disk - set(re.findall(r"`([a-z_]+\.py)`", readme)) == set()


def test_the_priorities_example_configures_both_axes() -> None:
    """Priorities are about kinds of work, shards about sources."""
    from examples import priorities

    assert priorities.plugin.config.queues == ("high", "low")
    assert priorities.plugin.config.shards == 4
    assert priorities.plugin.config.fairness_every == 10


def test_the_payload_example_configures_a_store_not_just_a_threshold() -> None:
    """A threshold with no store offloads nothing at all."""
    from examples import large_payloads

    assert large_payloads.plugin.config.payloads is not None
    assert large_payloads.plugin.config.offload_over_bytes == 64 * 1024


def test_the_sync_example_keeps_its_blocking_task_synchronous() -> None:
    """A `def` task is what puts it in the thread pool."""
    import inspect

    from examples import sync_tasks_and_timeouts as example

    assert not inspect.iscoroutinefunction(example.render_report.function)
    assert inspect.iscoroutinefunction(example.call_a_slow_api.function)
    assert example.plugin.config.thread_limit == 8
