"""Asking for a job's outcome and waiting for it.

Results are opt-in per job: most work is enqueued and forgotten, and keeping an
outcome for all of it would be a key and a TTL spent on nobody. Waiting blocks
rather than polls.

Run a worker beside it:  litestar --app examples.results:app workers run
"""

from __future__ import annotations

import os
from uuid import UUID

from litestar.params import FromPath
from litestar.status_codes import HTTP_504_GATEWAY_TIMEOUT

from litestar import Litestar, post
from smallage.litestar import QueueConfig, QueuePlugin, TaskRegistry

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

tasks = TaskRegistry()


@tasks.task
async def summarise(doc_id: UUID) -> bytes:
    """Only bytes travel back; anything richer is the application's encoding."""
    return f"summary of {doc_id}".encode()


plugin = QueuePlugin(
    QueueConfig(
        registry=tasks,
        redis_url=REDIS_URL,
        namespace="example-results",
        # How long an outcome is kept once written, unless a job asks for less.
        result_ttl_ms=300_000,
    )
)


@post("/summaries/{doc_id:uuid}")
async def create_summary(doc_id: FromPath[UUID]) -> dict[str, str]:
    """Enqueue and wait, turning an asynchronous job into a synchronous request.

    Worth doing sparingly: the request now lasts as long as the job does, and a
    timeout here says nothing about whether the work happened.
    """
    job_id = await summarise.enqueue(doc_id=doc_id, result_ttl_ms=300_000)

    outcome = await plugin.results.wait(job_id, timeout_s=30)
    if outcome is None:
        return {"job_id": job_id, "status": "still running"}
    if not outcome.ok:
        return {"job_id": job_id, "status": "failed", "error": outcome.error}
    return {"job_id": job_id, "summary": outcome.value.decode()}


@post("/summaries/{doc_id:uuid}/detached")
async def start_summary(doc_id: FromPath[UUID]) -> dict[str, str]:
    """The usual shape: hand back the id, let the caller poll for it later."""
    job_id = await summarise.enqueue(doc_id=doc_id, result_ttl_ms=300_000)
    return {"job_id": job_id}


app = Litestar(route_handlers=[create_summary, start_summary], plugins=[plugin])

TIMEOUT_MEANS = HTTP_504_GATEWAY_TIMEOUT
"""A reminder: a timeout is about this request, not about the job, which runs on."""
