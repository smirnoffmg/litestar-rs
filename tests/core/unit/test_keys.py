"""The key schema cannot be changed later without a migration, so it is pinned here."""

import pytest

from litestar_rs.core.errors import ConfigurationError
from litestar_rs.core.keys import (
    alive_key,
    dedup_key,
    dlq_key,
    sched_key,
    stream_key,
    stream_keys,
    validate_namespace,
    validate_queue,
)

pytestmark = pytest.mark.unit


def test_key_literals() -> None:
    assert stream_key("lrs", "default", 0) == "{lrs}:q:default:0"
    assert alive_key("lrs", b"1712345678901-0") == "{lrs}:alive:1712345678901-0"
    assert dedup_key("lrs", "invoice-42") == "{lrs}:dedup:invoice-42"
    assert sched_key("lrs") == "{lrs}:sched"
    assert dlq_key("lrs") == "{lrs}:dlq"


def test_every_key_shares_one_hash_tag() -> None:
    """Multi-key XREADGROUP and the Lua scripts need a single Cluster slot."""
    keys = [
        stream_key("lrs", "high", 3),
        alive_key("lrs", b"1-0"),
        dedup_key("lrs", "k"),
        sched_key("lrs"),
        dlq_key("lrs"),
    ]
    tags = {key[key.index("{") : key.index("}") + 1] for key in keys}
    assert tags == {"{lrs}"}


def test_stream_keys_covers_every_shard_in_order() -> None:
    assert stream_keys("lrs", "default", 3) == [
        "{lrs}:q:default:0",
        "{lrs}:q:default:1",
        "{lrs}:q:default:2",
    ]


def test_alive_key_accepts_str_entry_id() -> None:
    assert alive_key("lrs", "1-0") == alive_key("lrs", b"1-0")


@pytest.mark.parametrize("bad", ["", "a{b", "a}b", "a:b"])
def test_namespace_rejected(bad: str) -> None:
    with pytest.raises(ConfigurationError, match="namespace"):
        validate_namespace(bad)


@pytest.mark.parametrize("bad", ["", "a{b", "a}b", "a:b"])
def test_queue_rejected(bad: str) -> None:
    with pytest.raises(ConfigurationError, match="queue"):
        validate_queue(bad)


def test_validators_return_the_value() -> None:
    assert validate_namespace("lrs") == "lrs"
    assert validate_queue("default") == "default"


def test_shard_count_must_be_positive() -> None:
    with pytest.raises(ConfigurationError, match="shards"):
        stream_keys("lrs", "default", 0)
