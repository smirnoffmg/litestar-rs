"""Carrying trace context from the request that enqueued a job into the worker.

No tracing SDK is imported here. The library moves a W3C ``traceparent`` from one
side to the other and exposes it; binding that to OpenTelemetry, or to anything
else, is one function the application supplies.
"""

from __future__ import annotations

from collections.abc import Callable
from contextvars import ContextVar

TraceparentSource = Callable[[], str | None]
"""Returns the traceparent of whatever is running now, or None outside a trace."""

current_traceparent: ContextVar[str | None] = ContextVar(
    "smallage_traceparent", default=None
)
"""The traceparent of the job being executed, for the duration of the handler.

Read it to attach the task's span to the request that enqueued it. It is a
context variable rather than an argument so that a task signature stays about
the task.
"""


def no_traceparent() -> None:
    """Default source: applications that do not trace pay nothing for the field."""
    return
