"""Real Redis for the integration suite: one container per session."""

from collections.abc import AsyncIterator, Iterator
from uuid import uuid4

import pytest
from redis.asyncio import Redis
from testcontainers.redis import RedisContainer

# Lowest version we support: XINFO GROUPS reports `lag` from Redis 7 on.
REDIS_IMAGE = "redis:7-alpine"


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
