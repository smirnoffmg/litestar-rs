"""Real Redis for the integration suite: one container per session."""

import os
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from uuid import uuid4

import pytest
from redis.asyncio import Redis
from testcontainers.community.redis import RedisContainer

from smallage.core.scheduler import RedisScheduler
from smallage.core.transport import RedisStreamsTransport

# Lowest version we support: XINFO GROUPS reports `lag` from Redis 7 on. CI runs
# the suite against each supported major by setting REDIS_IMAGE.
REDIS_IMAGE = os.environ.get("REDIS_IMAGE", "redis:7-alpine")


@pytest.fixture(scope="session")
def redis_url() -> Iterator[str]:
    with RedisContainer(REDIS_IMAGE) as container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(container.port)
        yield f"redis://{host}:{port}/0"


@pytest.fixture
async def redis_client(redis_url: str) -> AsyncIterator[Redis]:
    client = Redis.from_url(redis_url)
    try:
        yield client
    finally:
        await client.aclose()


@pytest.fixture
def namespace() -> str:
    """A fresh namespace per test keeps them order-independent without FLUSHDB."""
    return f"t{uuid4().hex[:8]}"


@pytest.fixture
async def transports(
    redis_url: str, namespace: str
) -> AsyncIterator[Callable[[str], Awaitable[RedisStreamsTransport]]]:
    """Build transports that share a namespace but act as different workers."""
    clients: list[Redis] = []

    async def make(consumer: str, *, shards: int = 1) -> RedisStreamsTransport:
        reader = Redis.from_url(redis_url, socket_timeout=30.0)
        control = Redis.from_url(redis_url)
        clients.extend((reader, control))
        transport = RedisStreamsTransport(
            reader=reader,
            control=control,
            consumer=consumer,
            namespace=namespace,
            shards=shards,
            block_ms=100,
        )
        await transport.ensure_group()
        return transport

    try:
        yield make
    finally:
        for client in clients:
            await client.aclose()


@pytest.fixture
async def transport(
    transports: Callable[[str], Awaitable[RedisStreamsTransport]],
) -> RedisStreamsTransport:
    return await transports("worker-1")


@pytest.fixture
async def schedulers(
    redis_url: str, namespace: str
) -> AsyncIterator[Callable[[], Awaitable[RedisScheduler]]]:
    """Independent schedulers over one namespace, to race them against each other."""
    clients: list[Redis] = []

    async def make() -> RedisScheduler:
        control = Redis.from_url(redis_url)
        clients.append(control)
        return RedisScheduler(control=control, namespace=namespace)

    try:
        yield make
    finally:
        for client in clients:
            await client.aclose()


@pytest.fixture
async def scheduler(
    schedulers: Callable[[], Awaitable[RedisScheduler]],
) -> RedisScheduler:
    return await schedulers()


@pytest.fixture
async def prioritised(
    redis_url: str, namespace: str
) -> AsyncIterator[RedisStreamsTransport]:
    """A transport over two queues, high first."""
    reader = Redis.from_url(redis_url, socket_timeout=30.0)
    control = Redis.from_url(redis_url)
    transport = RedisStreamsTransport(
        reader=reader,
        control=control,
        consumer="worker-1",
        namespace=namespace,
        queues=("high", "low"),
        block_ms=100,
        fairness_every=0,
    )
    await transport.ensure_group()
    try:
        yield transport
    finally:
        await reader.aclose()
        await control.aclose()
