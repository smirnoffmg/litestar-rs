"""Consuming a stream this application does not own.

The same worker process and the same consumer group handle both your own tasks
and the foreign stream. Broker handlers are dispatched by stream, not by task
name, and get the record exactly as Redis returned it -- there is no envelope,
because the payload is in a format you did not choose.

A foreign stream is read between blocking reads of your own queues, so an entry
waits up to `block_ms` to be picked up. Lower it if that matters.
"""

from __future__ import annotations

import os

from litestar import Litestar
from smallage import Record
from smallage.litestar import QueueConfig, QueuePlugin, TaskRegistry

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

# Named in full: this library did not choose the name, so it does not build it.
# Sharing the namespace's hash tag keeps it in the same cluster slot.
ORDERS = "{example-broker}:orders"

tasks = TaskRegistry()


@tasks.task
async def send_receipt(order_id: str) -> None:
    print(f"receipt for {order_id}")


async def on_order(record: Record) -> None:
    """A failure here is not acked; redelivery is the only retry available."""
    order_id = record.fields[b"order_id"].decode()
    print(f"order {order_id} from {record.stream}")
    await send_receipt.enqueue(order_id=order_id)


app = Litestar(
    route_handlers=[],
    plugins=[
        QueuePlugin(
            QueueConfig(
                registry=tasks,
                redis_url=REDIS_URL,
                namespace="example-broker",
                # One mapping: which streams to consume, and what handles each.
                brokers={ORDERS: on_order},
            )
        )
    ],
)
