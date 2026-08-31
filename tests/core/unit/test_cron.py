"""Cron resolution, including the two days a year that break naive schedulers."""

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from smallage.core.cron import CronJob, next_fire_ms, occurrence_id
from smallage.core.errors import ConfigurationError

pytestmark = pytest.mark.unit

BERLIN = ZoneInfo("Europe/Berlin")


def at(year: int, month: int, day: int, hour: int = 0, minute: int = 0) -> int:
    moment = datetime(year, month, day, hour, minute, tzinfo=BERLIN)
    return int(moment.timestamp() * 1000)


def fires(job: CronJob, start_ms: int, count: int) -> list[str]:
    moments = []
    cursor = start_ms
    for _ in range(count):
        nxt = next_fire_ms(job, cursor)
        assert nxt is not None
        moments.append(datetime.fromtimestamp(nxt / 1000, BERLIN).isoformat())
        cursor = nxt
    return moments


def daily_at_0230() -> CronJob:
    return CronJob(
        name="nightly",
        expression="30 2 * * *",
        task="reindex",
        timezone="Europe/Berlin",
    )


def test_spring_forward_does_not_skip_a_day() -> None:
    """02:30 does not exist on the spring-forward day; the job still runs once."""
    assert fires(daily_at_0230(), at(2026, 3, 27), 4) == [
        "2026-03-27T02:30:00+01:00",
        "2026-03-28T02:30:00+01:00",
        "2026-03-29T03:00:00+02:00",
        "2026-03-30T02:30:00+02:00",
    ]


def test_fall_back_does_not_run_twice() -> None:
    """02:30 happens twice on the fall-back day; the job runs once."""
    assert fires(daily_at_0230(), at(2026, 10, 23), 4) == [
        "2026-10-23T02:30:00+02:00",
        "2026-10-24T02:30:00+02:00",
        "2026-10-25T02:30:00+02:00",
        "2026-10-26T02:30:00+01:00",
    ]


def test_occurrence_is_strictly_in_the_future() -> None:
    """Asking again at the moment a job fires must not return that same moment."""
    job = daily_at_0230()
    exactly_now = at(2026, 6, 1, 2, 30)
    assert next_fire_ms(job, exactly_now) == at(2026, 6, 2, 2, 30)


def test_occurrence_id_is_the_same_for_every_leader() -> None:
    job = daily_at_0230()
    fire_ms = at(2026, 6, 1, 2, 30)
    assert occurrence_id(job, fire_ms) == occurrence_id(job, fire_ms)
    assert occurrence_id(job, fire_ms) != occurrence_id(job, fire_ms + 60_000)


def test_impossible_date_is_refused_at_construction() -> None:
    """February 30th never comes; say so on startup, not on the first fire."""
    with pytest.raises(ConfigurationError, match="expression"):
        CronJob(name="never", expression="0 0 30 2 *", task="reindex")


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({"name": ""}, "name"),
        ({"name": "a:b"}, "name"),
        ({"expression": "not a cron"}, "expression"),
        ({"timezone": "Mars/Olympus"}, "timezone"),
    ],
)
def test_invalid_job_names_its_field(overrides: dict[str, str], expected: str) -> None:
    base = {"name": "nightly", "expression": "30 2 * * *", "task": "reindex"}
    with pytest.raises(ConfigurationError, match=expected):
        CronJob(**(base | overrides))  # type: ignore[arg-type]  # table-driven
