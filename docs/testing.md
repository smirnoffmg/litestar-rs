# Testing application code

A queue that is hard to test is a queue people work around, so these ship with
the library rather than as an appendix.

## Assert what was queued

`CollectingEnqueuer` records what would have been enqueued and runs nothing —
no worker, no Redis:

```python
from litestar_rs import CollectingEnqueuer

enqueuer = CollectingEnqueuer()
registry.bind(dependencies, enqueuer=enqueuer)

await handle_request()

enqueuer.assert_enqueued("reindex")
enqueuer.assert_not_enqueued("purge")
```

`assert_enqueued` takes `times=` and reports the actual count when it fails.

## Run the task inline

`EagerEnqueuer` runs each task at the moment it is enqueued:

```python
from litestar_rs import EagerEnqueuer

registry.bind(dependencies, enqueuer=EagerEnqueuer(handlers))
```

It reproduces nothing about retries, ordering or concurrency, and that is the
point: anything depending on real queue behaviour belongs in an integration
test, and this keeps everything else cheap. An unregistered task raises
`UnknownTask` rather than quietly doing nothing, which would make a test pass
for the wrong reason.

## A real worker in a fixture

```python
import pytest
from litestar_rs import worker_running


@pytest.fixture
async def worker(transport, scheduler, handlers):
    async with worker_running(transport, handlers, config, scheduler=scheduler):
        yield
```

Leaving the block asks the worker to drain, so in-flight work finishes instead
of vanishing mid-assertion.

## Time

Nothing in the core reads a clock. Deadlines are enforced by Redis — liveness by
a key TTL, reclaim eligibility by `XCLAIM MINIDLETIME`, the trim floor and the
scheduler by Redis's own `TIME` — and the only time seam is an injected
`Sleeper`, which a test replaces with one that returns immediately.

Cron is a pure function of an instant: `next_fire_ms(job, after_ms)` takes the
moment to resolve from, so a DST transition is tested by passing a date rather
than by waiting for one.

For integration tests, set `min_idle_ms=0`. Every pending entry then becomes
eligible by idle time, which leaves the liveness key as the only thing gating a
reclaim — so a test manipulates that key instead of waiting out a timeout.

## Running this project's own suite

```bash
make test        # unit only, no Docker needed
make test-int    # integration, starts containers via testcontainers
make check       # lint, types, import contracts, unit tests
```

Integration tests need a working Docker daemon. `REDIS_IMAGE` selects the server
version; CI runs the suite against Redis 7 and 8.
