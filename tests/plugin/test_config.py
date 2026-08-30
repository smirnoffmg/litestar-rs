"""Plugin configuration is checked when it is built, not when it is used."""

import pytest

from litestar_rs.core.errors import ConfigurationError
from litestar_rs.plugin.config import QueueConfig
from litestar_rs.plugin.health import QueueHealth
from litestar_rs.plugin.registry import TaskRegistry

pytestmark = pytest.mark.unit


def test_an_empty_redis_url_is_refused() -> None:
    with pytest.raises(ConfigurationError, match="redis_url"):
        QueueConfig(registry=TaskRegistry(), redis_url="")


def test_a_health_path_without_a_leading_slash_is_refused() -> None:
    with pytest.raises(ConfigurationError, match="health_path"):
        QueueConfig(registry=TaskRegistry(), health_path="queue")


def test_health_is_unhealthy_when_redis_cannot_report_lag() -> None:
    """A missing depth reading is not a zero-depth queue."""
    reported = QueueHealth(namespace="lrs", group="workers", queues=(), lag=0)
    unknown = QueueHealth(namespace="lrs", group="workers", queues=(), lag=None)

    assert reported.healthy is True
    assert unknown.healthy is False
