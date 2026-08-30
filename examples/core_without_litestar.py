"""The queue with no web framework at all.

The design says core must be usable without Litestar, and two import-linter
contracts enforce it -- but a rule nobody exercises is a rule nobody trusts.
This module imports no Litestar and runs a worker anyway: a transport, a
scheduler, a handler, and the loop.

    python -m examples.core_without_litestar
"""

from __future__ import annotations

import os

import anyio
from redis.asyncio import Redis

from litestar_rs import (
    Envelope,
    JsonCodec,
    RedisScheduler,
    RedisStreamsTransport,
    WorkerConfig,
    run_with_signals,
)

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
NAMESPACE = "example-core"
codec = JsonCodec()


async def reindex(envelope: Envelope) -> None:
    """A handler here takes the whole record: there is no registry to unpack it."""
    print(f"reindexing {codec.decode(envelope.payload)}")


async def main() -> None:
    # Two clients on purpose: a blocking read owns its connection for the whole
    # block window, and an ack queued behind it makes a live worker look dead.
    reader: Redis = Redis.from_url(REDIS_URL, socket_timeout=35.0)
    control: Redis = Redis.from_url(REDIS_URL)
    try:
        transport = RedisStreamsTransport(
            reader=reader,
            control=control,
            consumer="core-worker-1",
            namespace=NAMESPACE,
        )
        scheduler = RedisScheduler(control=control, namespace=NAMESPACE)
        await transport.ensure_group()

        await transport.enqueue(
            Envelope(
                id="job-1",
                task="reindex",
                payload=codec.encode({"doc_id": 7}),
                enqueued_at=await scheduler.now_ms(),
            ),
            queue="default",
        )

        await run_with_signals(
            transport,
            {"reindex": reindex},
            WorkerConfig(concurrency=4),
            scheduler=scheduler,
        )
    finally:
        await reader.aclose()
        await control.aclose()


if __name__ == "__main__":
    anyio.run(main, backend="asyncio", backend_options={"use_uvloop": True})
