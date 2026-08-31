"""Retry thresholds and backoff.

Application retries and reclaims are counted apart on purpose. ``delivery_count``
from ``XPENDING`` rises every time an entry is taken from a worker that stopped
refreshing its liveness key -- a crashed pod, a rolling restart -- which says
nothing about whether the task itself is failing. Mixing them sends healthy work
to the dead letter queue after a few deploys.
"""

from dataclasses import dataclass

from smallage.core.errors import ConfigurationError


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 3
    max_deliveries: int = 5
    initial_backoff_ms: int = 1_000
    max_backoff_ms: int = 300_000
    multiplier: float = 2.0
    jitter: float = 0.2
    unknown_task_timeout_ms: int = 6 * 60 * 60 * 1000
    unknown_task_backoff_ms: int = 30_000
    history_limit: int = 10
    history_line_bytes: int = 200

    def __post_init__(self) -> None:
        if self.max_attempts < 0:
            raise ConfigurationError(
                f"max_attempts must not be negative, got {self.max_attempts}"
            )
        if self.max_deliveries < 1:
            raise ConfigurationError(
                f"max_deliveries must be at least 1, got {self.max_deliveries}"
            )
        if self.initial_backoff_ms < 1:
            raise ConfigurationError(
                f"initial_backoff_ms must be positive, got {self.initial_backoff_ms}"
            )
        if self.max_backoff_ms < self.initial_backoff_ms:
            raise ConfigurationError(
                f"max_backoff_ms ({self.max_backoff_ms}) must not be below "
                f"initial_backoff_ms ({self.initial_backoff_ms})"
            )
        if self.multiplier < 1:
            raise ConfigurationError(
                f"multiplier must be at least 1, got {self.multiplier}"
            )
        if not 0 <= self.jitter <= 1:
            raise ConfigurationError(f"jitter must be within 0..1, got {self.jitter}")
        if self.unknown_task_timeout_ms < 0:
            raise ConfigurationError(
                "unknown_task_timeout_ms must not be negative, got "
                f"{self.unknown_task_timeout_ms}"
            )

    def delay_ms(self, attempt: int, *, rand: float = 0.5) -> int:
        """Backoff before retry number ``attempt``.

        ``rand`` is the caller's draw from [0, 1), kept as an argument so the
        arithmetic stays pure and the spread stays testable. Jitter matters:
        a batch that fails together would otherwise retry together forever.
        """
        base = min(
            self.initial_backoff_ms * self.multiplier**attempt,
            float(self.max_backoff_ms),
        )
        spread = base * self.jitter * (2 * rand - 1)
        return max(0, int(base + spread))

    def exhausted(self, attempt: int) -> bool:
        return attempt >= self.max_attempts

    def over_delivered(self, times_delivered: int) -> bool:
        return times_delivered > self.max_deliveries

    def record_failure(
        self, history: tuple[str, ...], attempt: int, error: BaseException
    ) -> tuple[str, ...]:
        """Append one line about this failure, keeping the tail bounded.

        ``attempt`` is the attempt that just failed, not the one about to run.

        Truncating and capping matters: without it a job that keeps failing grows
        its own stream entry on every attempt.
        """
        line = f"{attempt}: {type(error).__name__}: {error}".replace("\n", " ")
        return (*history, line[: self.history_line_bytes])[-self.history_limit :]

    def unknown_task_expired(self, *, age_ms: int) -> bool:
        """A rolling deploy is measured in minutes, so this threshold is time."""
        return age_ms > self.unknown_task_timeout_ms
