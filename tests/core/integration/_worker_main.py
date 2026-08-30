"""A worker in its own process, so tests can signal it for real.

It announces every entry it takes on ``signal_key``. In ``block`` mode it then
hangs, which is the state a crash leaves behind: an entry in the PEL whose owner
will never ack. In ``ack`` mode it returns, so the entry is acknowledged.
"""

import sys
from functools import partial

import anyio
from redis.asyncio import Redis

from litestar_rs.core.envelope import Envelope
from litestar_rs.core.scheduler import RedisScheduler
from litestar_rs.core.transport import RedisStreamsTransport
from litestar_rs.core.worker import WorkerConfig, run_with_signals


async def main(
    url: str, namespace: str, consumer: str, signal_key: str, mode: str
) -> None:
    reader = Redis.from_url(url, socket_timeout=30.0)
    control = Redis.from_url(url)
    transport = RedisStreamsTransport(
        reader=reader,
        control=control,
        consumer=consumer,
        namespace=namespace,
        block_ms=100,
    )

    async def handler(envelope: Envelope) -> None:
        await control.rpush(signal_key, b"taken")
        if mode == "block":
            await anyio.sleep_forever()

    await run_with_signals(
        transport,
        {"reindex": handler},
        scheduler=RedisScheduler(control=control, namespace=namespace),
        config=WorkerConfig(
            concurrency=1,
            alive_ttl_ms=60_000,
            min_idle_ms=0,
            reclaim_interval_s=60.0,
            trim_interval_s=60.0,
        ),
    )


if __name__ == "__main__":
    anyio.run(partial(main, *sys.argv[1:]))
