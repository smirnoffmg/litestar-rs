"""Real Redis for the plugin's integration tests."""

import os
from collections.abc import Iterator

import pytest
from testcontainers.community.redis import RedisContainer

REDIS_IMAGE = os.environ.get("REDIS_IMAGE", "redis:7-alpine")


@pytest.fixture(scope="session")
def redis_url() -> Iterator[str]:
    with RedisContainer(REDIS_IMAGE) as container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(container.port)
        yield f"redis://{host}:{port}/0"


@pytest.fixture
def namespace() -> str:
    from uuid import uuid4

    return f"t{uuid4().hex[:8]}"
