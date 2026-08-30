"""Cron expressions, resolved in a real time zone.

Occurrences are always strictly after the moment asked about, so a job cannot
fire twice for the same minute no matter how often the leader looks.
"""

from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from cronsim import CronSim, CronSimError

from litestar_rs.core.envelope import Envelope
from litestar_rs.core.errors import ConfigurationError


@dataclass(frozen=True, slots=True)
class CronJob:
    name: str
    expression: str
    task: str
    payload: bytes = b"{}"
    """Encoded arguments. An empty object, not null: it decodes into the task's
    argument struct, and a task taking nothing still has one."""
    queue: str = "default"
    timezone: str = "UTC"

    def __post_init__(self) -> None:
        if not self.name or ":" in self.name:
            raise ConfigurationError(
                f"cron job name must be non-empty and free of ':', got {self.name!r}"
            )
        try:
            zone = ZoneInfo(self.timezone)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise ConfigurationError(
                f"cron job {self.name!r} has an unknown timezone {self.timezone!r}"
            ) from exc
        try:
            CronSim(self.expression, datetime.now(zone))
        except CronSimError as exc:
            raise ConfigurationError(
                f"cron job {self.name!r} has an invalid expression "
                f"{self.expression!r}: {exc}"
            ) from exc


def next_fire_ms(job: CronJob, after_ms: int) -> int | None:
    """Next occurrence strictly after ``after_ms``, or None if there is none.

    Resolving in the job's own zone is what makes daylight saving behave: a time
    that does not exist on the spring-forward day moves to the following instant
    rather than being skipped, and one that happens twice in autumn fires once.
    """
    zone = ZoneInfo(job.timezone)
    after = datetime.fromtimestamp(after_ms / 1000, zone)
    try:
        occurrence = next(CronSim(job.expression, after))
    except StopIteration:
        # Expressions that can never match are refused when the job is built, so
        # this is unreachable in practice. It stays because an escaping
        # StopIteration inside a coroutine surfaces as a bare RuntimeError.
        return None
    return int(occurrence.timestamp() * 1000)


def occurrence_id(job: CronJob, fire_ms: int) -> str:
    """Identify an occurrence by job and instant.

    Two leaders computing the same occurrence produce the same id, so scheduling
    it twice is a no-op rather than a duplicate job.
    """
    return f"cron:{job.name}:{fire_ms}"


def occurrence_envelope(job: CronJob, fire_ms: int) -> Envelope:
    """Build the entry for one occurrence.

    ``enqueued_at`` is the instant the job was due, not the instant it reached
    the stream, so a run delayed by an outage can be recognised as late.
    """
    return Envelope(
        id=occurrence_id(job, fire_ms),
        task=job.task,
        payload=job.payload,
        enqueued_at=fire_ms,
    )
