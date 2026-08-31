"""Consuming somebody else's streams and owning no queue at all.

Nothing is read but the foreign streams, and nothing is created in order to read
them -- no queue stream to consume, no consumer group on one. With no queues to
prioritise, the foreign streams are the blocking read, so an entry is picked up
as it is written rather than at the end of a `block_ms` window.

`queues=()` says what this deployment *reads*, not what it writes. `send_receipt`
below is still enqueued as usual -- which does create that queue's stream here --
and those jobs are for whichever deployment consumes it.
"""

from __future__ import annotations

import os

from litestar import Litestar
from smallage import Record
from smallage.litestar import QueueConfig, QueuePlugin, TaskRegistry

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

PAYMENTS = "{example-broker-only}:payments"

tasks = TaskRegistry()


@tasks.task
async def send_receipt(payment_id: str) -> None:
    print(f"receipt for {payment_id}")


async def on_payment(record: Record) -> None:
    payment_id = record.fields[b"payment_id"].decode()
    print(f"payment {payment_id} from {record.stream}")
    # Onto the queue as usual: another deployment is what reads it.
    await send_receipt.enqueue(payment_id=payment_id)


app = Litestar(
    route_handlers=[],
    plugins=[
        QueuePlugin(
            QueueConfig(
                registry=tasks,
                redis_url=REDIS_URL,
                namespace="example-broker-only",
                queues=(),
                brokers={PAYMENTS: on_payment},
            )
        )
    ],
)
