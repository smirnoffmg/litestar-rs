# litestar-rs

[![PyPI version](https://img.shields.io/pypi/v/litestar-rs)](https://pypi.org/project/litestar-rs/)
[![Python versions](https://img.shields.io/pypi/pyversions/litestar-rs)](https://pypi.org/project/litestar-rs/)
[![License](https://img.shields.io/pypi/l/litestar-rs)](https://pypi.org/project/litestar-rs/)
[![CI](https://img.shields.io/github/actions/workflow/status/smirnoffmg/litestar-rs/ci.yml?branch=main)](https://github.com/smirnoffmg/litestar-rs/actions/workflows/ci.yml)
[![Checked with mypy](https://img.shields.io/badge/mypy-strict-2a6db2)](https://mypy-lang.org/)

Distributed task queue on Redis Streams for [Litestar](https://litestar.dev/) —
typed tasks, real dependency injection, retries, scheduling and cron, through
Litestar's native plugin protocol.

📖 **[Documentation](https://smirnoffmg.dev/litestar-rs/)**

## Features

- Tasks are ordinary functions: arguments are serialised, dependencies injected
- Retries with backoff, a delivery ceiling, and a dead letter queue that keeps
  the payload, the traceback and the attempt history
- Delayed jobs and cron with no scheduler process, correct across DST
- Priority queues with a bounded starvation window, and shards for fairness
- Broker mode: consume streams somebody else writes, in the same worker
- Optional results, a deduplication gate, and trace context carried into the task
- Health endpoint served identically by the web process and the worker
- Eager mode, `assert_enqueued` and a real-worker fixture for your own tests

## Installation

```bash
uv add litestar-rs
```

Redis 7 or newer. Standalone, Sentinel and Cluster are all covered by the test
suite.

## Quick start

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

`doc_id` travels in the payload; `settings` is injected in the worker from the
application's own dependency graph, and a real one would be a database session
or a client. There is no context dictionary.

## Workers

A worker is the same application, started differently:

```bash
litestar workers run --queue high --concurrency 20 --health-port 8081
```

Anything that can be settled at startup is — a task registered twice, an
argument with no annotation, a dependency the application does not provide, a
cycle, a provider only a request could satisfy — rather than on the first job in
production.

## Delivery guarantee

**At-least-once.** A worker that completed its side effect and died before
`XACK` will be reclaimed and the work repeated. No amount of protocol work
removes that; only an idempotent handler does. A `dedup` key is provided for the
cases where that is not naturally true.

## Documentation

Tasks, scheduling, retries, priorities, broker mode, results, testing and
operations are covered in the
**[full documentation](https://smirnoffmg.dev/litestar-rs/)**. The rules the
library is built to are in
**[Design and invariants](https://smirnoffmg.dev/litestar-rs/design/)**.
Runnable [examples](examples/) are included.

## Development

```bash
make install   # dependencies and git hooks
make check     # lint, types, import contracts, unit tests
make test-int  # integration suite, needs Docker
```

## License

MIT
