"""What a worker has done, for whoever is watching it.

Counters rather than logs: "unknown task" in a log line is noise during a
rollout and an incident afterwards, and only a number tells you which.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class WorkerStats:
    """Live counters for one worker process.

    Plain integers on one event loop: incremented between awaits, read whenever.
    """

    handled: int = 0
    failed: int = 0
    retried: int = 0
    buried: int = 0
    unknown_task: int = 0
    reclaimed: int = 0
    skipped_duplicate: int = 0
    in_flight: int = 0
    buried_by_reason: dict[str, int] = field(default_factory=dict)

    def bury(self, reason: str) -> None:
        self.buried += 1
        self.buried_by_reason[reason] = self.buried_by_reason.get(reason, 0) + 1

    def snapshot(self) -> dict[str, int]:
        return {
            "handled": self.handled,
            "failed": self.failed,
            "retried": self.retried,
            "buried": self.buried,
            "unknown_task": self.unknown_task,
            "reclaimed": self.reclaimed,
            "skipped_duplicate": self.skipped_duplicate,
            "in_flight": self.in_flight,
        }
