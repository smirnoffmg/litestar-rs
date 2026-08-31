"""Scheduler construction checks. redis-py connects lazily, so nothing dials out."""

from typing import Any

import pytest
from redis.asyncio import Redis

from smallage.core.errors import ConfigurationError
from smallage.core.scheduler import RedisScheduler

pytestmark = pytest.mark.unit

URL = "redis://localhost:6379/0"


def build(**overrides: Any) -> RedisScheduler:
    kwargs: dict[str, Any] = {"control": Redis.from_url(URL)}
    return RedisScheduler(**(kwargs | overrides))


def test_defaults_are_accepted() -> None:
    assert build().zset == "{lrs}:sched"
    assert build().leader == "{lrs}:leader"


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({"shards": 0}, "shards"),
        ({"namespace": "a:b"}, "namespace"),
        ({"control": Redis.from_url(URL, decode_responses=True)}, "decode_responses"),
    ],
)
def test_invalid_values_name_their_field(
    overrides: dict[str, Any], expected: str
) -> None:
    with pytest.raises(ConfigurationError, match=expected):
        build(**overrides)
