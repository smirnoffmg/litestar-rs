"""Keeping the request that queued a job and the job itself in one trace.

The library moves a W3C `traceparent` from one side to the other and exposes it.
Binding that to OpenTelemetry, or to anything else, is one function you supply --
no tracing SDK is imported here, and an application that does not trace pays
nothing, because an absent traceparent writes no field at all.

    litestar --app examples.tracing:app workers run
"""

from __future__ import annotations

import os
from contextvars import ContextVar
from uuid import UUID, uuid4

from litestar import Litestar, post

from litestar_rs.plugin import (
    QueueConfig,
    QueuePlugin,
    TaskRegistry,
    current_traceparent,
)

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

# Stands in for your tracing SDK's notion of "the span running right now".
# With OpenTelemetry this would read the current span context instead.
REQUEST_TRACEPARENT: ContextVar[str | None] = ContextVar("traceparent", default=None)


def current_request_traceparent() -> str | None:
    """Supplied to the plugin; called once per enqueue."""
    return REQUEST_TRACEPARENT.get()


tasks = TaskRegistry()


@tasks.task
async def reindex(doc_id: UUID) -> None:
    """Read it here to attach this task's span to the request that queued it.

    A context variable rather than an argument, so a task signature stays about
    the task. Providers see it too, because they resolve inside the same context.
    """
    print(f"reindexing {doc_id} within trace {current_traceparent.get()}")


plugin = QueuePlugin(
    QueueConfig(
        registry=tasks,
        redis_url=REDIS_URL,
        namespace="example-tracing",
        traceparent=current_request_traceparent,
    )
)


@post("/documents")
async def create() -> dict[str, str]:
    # Your tracing middleware sets this; here it is faked so the example runs.
    traceparent = f"00-{uuid4().hex}-{uuid4().hex[:16]}-01"
    REQUEST_TRACEPARENT.set(traceparent)

    doc_id = uuid4()
    await reindex.enqueue(doc_id=doc_id)
    return {"queued": str(doc_id), "traceparent": traceparent}


app = Litestar(route_handlers=[create], plugins=[plugin])
