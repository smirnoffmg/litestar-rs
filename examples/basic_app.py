"""A task, the dependencies it is given, and an endpoint that queues it.

Run the application:      litestar --app examples.basic_app:app run
Run a worker beside it:   litestar --app examples.basic_app:app workers run
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from dataclasses import dataclass
from uuid import UUID, uuid4

from litestar import Litestar, post
from litestar.di import Provide

from litestar_rs.plugin import QueueConfig, QueuePlugin, TaskRegistry

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

tasks = TaskRegistry()


@dataclass(frozen=True, slots=True)
class Settings:
    index_name: str = "documents"


def settings() -> Settings:
    return Settings()


async def search_client(settings: Settings) -> AsyncIterator[str]:
    """A generator dependency, torn down after the task finishes or fails."""
    client = f"client({settings.index_name})"
    try:
        yield client
    finally:
        pass  # close the client here


@tasks.task
async def reindex(doc_id: UUID, search_client: str) -> None:
    """`doc_id` travels in the payload; `search_client` is injected in the worker."""
    print(f"reindexing {doc_id} via {search_client}")


@tasks.task(queue="default", timeout_s=30)
async def rebuild_index(reason: str, search_client: str) -> None:
    print(f"rebuilding because {reason} via {search_client}")


@post("/documents")
async def create_document() -> dict[str, str]:
    doc_id = uuid4()
    await reindex.enqueue(doc_id=doc_id)
    return {"queued": str(doc_id)}


app = Litestar(
    route_handlers=[create_document],
    dependencies={
        "settings": Provide(settings, sync_to_thread=False),
        "search_client": Provide(search_client),
    },
    plugins=[
        QueuePlugin(
            QueueConfig(registry=tasks, redis_url=REDIS_URL, namespace="example")
        )
    ],
)
