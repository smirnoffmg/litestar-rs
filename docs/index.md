# litestar-rs

A distributed task queue on Redis Streams, with first-class Litestar
integration.

**Delivery is at-least-once.** Handlers must be idempotent; a `dedup` key is
provided for the cases where that is not naturally true. Treating delivery as
exactly-once is a design error in the application, not a bug here.

## Install

```bash
uv add "litestar-rs[litestar]"
```

The `litestar` extra is only needed for the plugin. The core — transport,
worker, scheduler — has no Litestar dependency and is usable on its own.

## Quickstart

```python
from uuid import UUID

from litestar import Litestar, post
from litestar.di import Provide

from litestar_rs.plugin import QueueConfig, QueuePlugin, TaskRegistry

tasks = TaskRegistry()


@tasks.task
async def reindex(doc_id: UUID, session: AsyncSession) -> None:
    ...


@post("/documents")
async def create(doc_id: UUID) -> str:
    await reindex.enqueue(doc_id=doc_id)
    return "queued"


app = Litestar(
    route_handlers=[create],
    dependencies={"session": Provide(session)},
    plugins=[QueuePlugin(QueueConfig(registry=tasks, redis_url="redis://localhost"))],
)
```

Workers are the same application, started differently:

```bash
litestar workers run --queue high --concurrency 20
```

## Two layers

- **`litestar_rs.core`** — transport, worker, scheduler, retries, results.
  Depends on `redis`, `msgspec`, `anyio`. Importing Litestar from here is
  forbidden, and an import-linter contract enforces it.
- **`litestar_rs.plugin`** — dependency injection, CLI, serialization, health,
  tracing.

## Guides

- [Tasks](tasks.md) — arguments, dependencies, what travels and what is injected
- [Scheduling](scheduling.md) — delayed jobs, cron, missed occurrences
- [Retries and the DLQ](retries.md) — the two counters, backoff, reading a buried job
- [Priorities](priorities.md) — queues, fairness, shards
- [Broker mode](broker.md) — consuming streams somebody else writes
- [Results and deduplication](results.md) — waiting for an outcome, running once
- [Testing](testing.md) — eager mode, assertions, a real worker in a fixture
- [Operations](operations.md) — deployment, health, shutdown, depth
- [API reference](api.md)

## Redis

Redis 7 or newer: depth comes from the consumer group's `lag`, which earlier
versions do not report. Standalone, Sentinel and Cluster deployments are all
covered by the test suite, against both Redis 7 and 8.
