# litestar-rs

A distributed task queue on Redis Streams, with first-class Litestar
integration.

**Delivery is at-least-once.** Handlers must be idempotent; a `dedup` key is
provided for the cases where that is not naturally true. Treating delivery as
exactly-once is a design error in the application, not a bug here.

## Install

```bash
uv add litestar-rs
```

The core — transport, worker, scheduler — never imports Litestar, and an
import-linter contract keeps it that way, so `litestar_rs.core` is usable behind
another framework or none. That is a rule about imports rather than about
installation: Litestar is an ordinary dependency here, because a package called
litestar-rs that made it optional would only be surprising.

## Quickstart

```python
from dataclasses import dataclass
from uuid import UUID, uuid4

from litestar import Litestar, post
from litestar.di import Provide

from litestar_rs.plugin import QueueConfig, QueuePlugin, TaskRegistry

tasks = TaskRegistry()


@dataclass
class Settings:
    index_name: str = "documents"


def settings() -> Settings:
    return Settings()


@tasks.task
async def reindex(doc_id: UUID, settings: Settings) -> None:
    """`doc_id` is serialised; `settings` comes from the application."""


@post("/documents")
async def create() -> str:
    await reindex.enqueue(doc_id=uuid4())
    return "queued"


app = Litestar(
    route_handlers=[create],
    dependencies={"settings": Provide(settings, sync_to_thread=False)},
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

- [Design and invariants](design.md) — the rules, and why each is a rule

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
