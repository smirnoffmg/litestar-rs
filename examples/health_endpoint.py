"""Serving queue health, on a path this application chose itself.

Run the application:      litestar --app examples.health_endpoint:app run
Run a worker beside it:   litestar --app examples.health_endpoint:app workers run

The plugin registers no route. `QueuePlugin.health()` answers what a readiness
probe needs, and where that answer is served -- on its own path, or folded into
one the application already has -- is the application's decision. Both are below.

A worker has no server at all, so a probe against a worker deployment is that
deployment's own arrangement, built on the same `health()`.
"""

from __future__ import annotations

import os

from litestar import Litestar, Response, get
from smallage.litestar import QueueConfig, QueueHealth, QueuePlugin, TaskRegistry

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

tasks = TaskRegistry()

plugin = QueuePlugin(
    QueueConfig(registry=tasks, redis_url=REDIS_URL, namespace="example-health")
)


@tasks.task
async def send_invoice(invoice_id: str) -> None:
    print(f"sending {invoice_id}")


@get("/health/queue")
async def queue_health() -> Response[QueueHealth]:
    """The queue on its own, for a probe that watches the queue on its own."""
    report = await plugin.health()
    # A probe reads the status code, not the body. Answering 200 while unhealthy
    # is a probe that can never fail.
    return Response(report, status_code=200 if report.healthy else 503)


async def database_reachable() -> bool:
    return True


@get("/readyz")
async def readyz() -> Response[dict[str, object]]:
    """The queue as one line in an answer about the whole process.

    Which is what a readiness probe usually wants: Kubernetes takes the pod out
    of rotation on one endpoint, so everything the pod needs in order to serve
    belongs in the same one.
    """
    queue = await plugin.health()
    database = await database_reachable()
    ready = queue.healthy and database

    return Response(
        {"queue": queue, "database": database},
        status_code=200 if ready else 503,
    )


app = Litestar(route_handlers=[queue_health, readyz], plugins=[plugin])
