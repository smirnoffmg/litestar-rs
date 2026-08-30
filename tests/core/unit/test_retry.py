"""Backoff arithmetic and the two independent thresholds."""

import pytest

from litestar_rs.core.errors import ConfigurationError
from litestar_rs.core.retry import RetryPolicy

pytestmark = pytest.mark.unit


def policy(**overrides: object) -> RetryPolicy:
    base: dict[str, object] = {
        "max_attempts": 3,
        "max_deliveries": 5,
        "initial_backoff_ms": 1_000,
        "max_backoff_ms": 60_000,
        "multiplier": 2.0,
        "jitter": 0.0,
    }
    return RetryPolicy(**(base | overrides))  # type: ignore[arg-type]  # test factory


def test_backoff_grows_geometrically() -> None:
    p = policy()
    assert [p.delay_ms(n) for n in range(5)] == [1_000, 2_000, 4_000, 8_000, 16_000]


def test_backoff_is_capped() -> None:
    p = policy(max_backoff_ms=5_000)
    assert [p.delay_ms(n) for n in range(5)] == [1_000, 2_000, 4_000, 5_000, 5_000]


def test_jitter_spreads_around_the_base_delay() -> None:
    """Without jitter a batch that fails together retries together, forever."""
    p = policy(jitter=0.5)
    assert p.delay_ms(1, rand=0.0) == 1_000
    assert p.delay_ms(1, rand=0.5) == 2_000
    assert p.delay_ms(1, rand=1.0) == 3_000


def test_jitter_never_yields_a_negative_delay() -> None:
    assert policy(jitter=1.0).delay_ms(0, rand=0.0) == 0


def test_application_retries_and_reclaims_are_counted_separately() -> None:
    """delivery_count from XPENDING counts reclaims, not application failures."""
    p = policy(max_attempts=3, max_deliveries=5)
    assert p.exhausted(2) is False
    assert p.exhausted(3) is True
    assert p.over_delivered(5) is False
    assert p.over_delivered(6) is True


def test_unknown_task_grace_is_measured_in_time() -> None:
    """A rolling deploy takes minutes; a threshold in attempts would DLQ mid-roll."""
    p = policy(unknown_task_timeout_ms=3_600_000)
    assert p.unknown_task_expired(age_ms=3_599_999) is False
    assert p.unknown_task_expired(age_ms=3_600_001) is True


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({"max_attempts": -1}, "max_attempts"),
        ({"max_deliveries": 0}, "max_deliveries"),
        ({"initial_backoff_ms": 0}, "initial_backoff_ms"),
        ({"max_backoff_ms": 500}, "max_backoff_ms"),
        ({"multiplier": 0.5}, "multiplier"),
        ({"jitter": 1.5}, "jitter"),
    ],
)
def test_invalid_values_name_their_field(
    overrides: dict[str, object], expected: str
) -> None:
    with pytest.raises(ConfigurationError, match=expected):
        policy(**overrides)


def test_failure_history_is_capped_and_truncated() -> None:
    """A job that keeps failing must not grow its own stream entry."""
    p = policy(history_limit=3, history_line_bytes=20)
    history: tuple[str, ...] = ()
    for attempt in range(5):
        history = p.record_failure(history, attempt, RuntimeError("x" * 500))

    assert len(history) == 3
    assert all(len(line) <= 20 for line in history)
    assert history[-1].startswith("4: RuntimeError")


def test_failure_history_keeps_newlines_out_of_the_field() -> None:
    """Lines are joined by newlines in the stream field, so they cannot contain one."""
    line = policy().record_failure((), 0, RuntimeError("first\nsecond"))[0]
    assert "\n" not in line
