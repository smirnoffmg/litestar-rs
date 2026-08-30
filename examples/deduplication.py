"""Making sure a side effect happens once, when delivery cannot promise it.

Delivery is at-least-once and no protocol change makes it otherwise: a worker
that charged the card and died before its XACK will be reclaimed and the work
repeated. The gate is the application's, because only the application knows what
"the same job" means.

    litestar --app examples.deduplication:app workers run
"""

from __future__ import annotations

import os
from uuid import UUID

from litestar import Litestar, post
from litestar.params import FromPath

from litestar_rs import WorkerConfig
from litestar_rs.plugin import QueueConfig, QueuePlugin, TaskRegistry

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

tasks = TaskRegistry()
CHARGED: list[UUID] = []


@tasks.task
async def charge_invoice(invoice_id: UUID) -> None:
    """The kind of side effect nobody wants twice."""
    CHARGED.append(invoice_id)
    print(f"charged {invoice_id}")


plugin = QueuePlugin(
    QueueConfig(
        registry=tasks,
        redis_url=REDIS_URL,
        namespace="example-dedup",
        worker=WorkerConfig(
            # How long a key holds. Choose it to cover the longest period over
            # which a repeat would be wrong -- not the longest a job takes.
            dedup_ttl_ms=24 * 60 * 60 * 1000
        ),
    )
)


@post("/invoices/{invoice_id:uuid}/charge")
async def charge(invoice_id: FromPath[UUID]) -> str:
    """Queue it twice on purpose; the gate lets one through.

    The key is claimed with SET NX PX immediately before the handler runs, which
    is the only point a duplicate can still be stopped: by the time a worker has
    the job, both copies are already in the stream.
    """
    for _ in range(2):
        await charge_invoice.enqueue(
            invoice_id=invoice_id, dedup=f"charge:{invoice_id}"
        )
    return "queued twice, will run once"


@post("/invoices/{invoice_id:uuid}/charge-unguarded")
async def charge_unguarded(invoice_id: FromPath[UUID]) -> str:
    """The same without a key, to see the difference."""
    for _ in range(2):
        await charge_invoice.enqueue(invoice_id=invoice_id)
    return "queued twice, will run twice"


app = Litestar(route_handlers=[charge, charge_unguarded], plugins=[plugin])
