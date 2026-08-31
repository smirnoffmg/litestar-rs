# Operations

## Deploying workers

A worker is the same application, started differently:

```bash
litestar workers run --queue high --queue default --concurrency 20
```

`--queue` may be repeated, highest priority first, and overrides the
application's own configuration. `--consumer` names the worker.

**Consumer names must be unique per running worker.** Redis groups pending
entries by consumer name, and reclaim uses that name to tell its own work from a
peer's. The default appends a random suffix to `consumer_prefix`; if you set the
name yourself, keep it unique per process.

### What starts with it

The same application means the same lifecycle. A worker enters
`Litestar.lifespan()` before it consumes anything and leaves it after draining,
so `on_startup` hooks, `on_shutdown` hooks and custom lifespan managers all run
in a worker process. A dependency that closes over something opened there — a
database pool, an HTTP client, a broker connection — resolves to an opened one,
and a failure to open it surfaces at startup rather than on the first job.

The queue is the outermost of those managers, so a manager the application
registers has it open on the way in and on the way out — it can enqueue at
startup and drain on shutdown. `on_shutdown` hooks are the exception: Litestar
puts them on its exit stack before any context manager, so they run last, after
the queue has closed, and a hook that reaches for `plugin.transport` there is
told so. Shutdown work that needs the queue belongs in a lifespan manager.

### Declining it

Entering the lifespan happens in every worker replica, and part of a lifespan is
often meant for one process only — starting a scheduler, warming a cache,
claiming a lease. Guard that part where it is written; the rest of the lifespan,
the pool a task needs among it, still has to run:

```python
import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from litestar import Litestar


@asynccontextmanager
async def run_the_scheduler(app: Litestar) -> AsyncGenerator[None]:
    if os.environ.get("ROLE") != "web":
        yield
        return
    async with scheduler:
        yield
```

`run_app_lifespan=False` is the blunter instrument, for a lifespan that is
web-only in its entirety:

```python
from smallage.litestar import QueueConfig, TaskRegistry

QueueConfig(registry=TaskRegistry(), run_app_lifespan=False)
```

A worker that declines opens the queue's own connections and nothing else, and
runs none of the application's hooks — including the ones its tasks depend on,
which is why it is the second answer and not the first.

## Connections

Each worker opens two clients. A blocking `XREADGROUP` occupies its connection
for the whole block window, and an ack or a liveness refresh queued behind it
makes a healthy worker look dead to its peers.

The reader's `socket_timeout` is derived from the block window: below it every
healthy blocking read would die, and above it is the only thing that surfaces a
connection hung by a failover.

## Health

`QueuePlugin.health()` answers everything a readiness probe needs. The plugin
registers no route for it: where a probe lives, whether it is public, and what
else belongs in the same response are the application's to decide, and it would
be an odd plugin that could fail your startup over a path it added itself.

```python
from litestar import Litestar, Response, get

from smallage.litestar import QueueConfig, QueueHealth, QueuePlugin, TaskRegistry

queue = QueuePlugin(QueueConfig(registry=TaskRegistry()))


@get("/health/queue")
async def health() -> Response[QueueHealth]:
    report = await queue.health()
    # A probe reads the status code, not the body. Answering 200 while unhealthy
    # is a probe that can never fail.
    return Response(report, status_code=200 if report.healthy else 503)


app = Litestar(route_handlers=[health], plugins=[queue])
```

The handler closes over the plugin rather than taking it as an argument: the
plugin registers no dependency of its own, so a `plugin: QueuePlugin` parameter
would be read as a query parameter and the request answered with a 400. Folding
the queue into a health endpoint the application already has works the same way
— `await queue.health()` beside whatever else that endpoint reports.

**A worker serves nothing.** It has no server, and this library does not start
one: an HTTP probe against a worker deployment is that deployment's own decision
— which port, which interface, behind what — and `health()` is the whole of what
it needs from here, computed exactly as it is for a web process. Liveness has an
answer that needs no HTTP at all: a worker refreshes a key per entry it holds,
and a peer reclaims what stops being refreshed — see [Retries](retries.md).

The response:

```json
{
  "namespace": "lrs",
  "group": "workers",
  "queues": ["default"],
  "lag": 0,
  "stats": {
    "handled": 12, "failed": 1, "retried": 1, "buried": 0,
    "unknown_task": 0, "reclaimed": 0, "skipped_duplicate": 2, "in_flight": 3
  }
}
```

`stats` are this process's own counters, so a web process reports zeros — it
runs no tasks. `unknown_task` is the one to watch during a rollout: a few are
normal while two versions overlap, and a number that keeps climbing after the
rollout finished means a task name was removed rather than renamed.

`lag` is the consumer group's depth from `XINFO GROUPS`. Redis reports it as
null when it cannot reconcile its counters after entries were deleted — and a
missing reading is not a zero-depth queue, so `healthy` is false in that case.
`XLEN` is meaningless here: acked entries are deleted, so the stream length is
near zero regardless of backlog.

## Shutdown

SIGTERM stops new reads and lets in-flight work finish. When
`drain_timeout_s` runs out, the watchdog cuts it off. A second signal means
"now".

Work cancelled by the watchdog is **not** an application failure: it stays in the
PEL unacked, and its liveness key is dropped so a peer takes it immediately
rather than waiting out the TTL.

Worst-case shutdown is one block window plus `drain_timeout_s`. Size
`terminationGracePeriodSeconds` against that sum, not against the drain timeout
alone:

```yaml
terminationGracePeriodSeconds: 45   # block_ms 5s + drain_timeout_s 30 + margin
```

An application-level bare `except:` swallows `CancelledError` and the worker will
never exit. `ruff`'s `B` and `E722` rules catch that in this project's own code.

## Failure modes worth knowing

**A dropped connection does not kill the worker.** Every loop logs the error,
pauses for `recovery_interval_s` and carries on; redis-py reconnects on the next
command, and records already taken stay in the PEL to be reclaimed. This is what
a Sentinel failover looks like from inside a worker.

**Rescheduling can fail too**, on the same connection that just died. When it
does, the entry is left unacked — the safe outcome, because reclaim is exactly
the mechanism for entries whose owner stopped responding.

**A worker never reclaims what Redis just served it.** An entry joins the
pending list the moment Redis executes `XREADGROUP`, before the reply reaches
the worker, so ownership rather than timing is what decides. Entries left under
the same consumer name by a previous run are snapshotted at startup and taken
back once.

## Stream growth

Acking deletes the entry, so a healthy stream stays near empty. A background
`XTRIM MINID` by `retention_ms` removes what acking did not, floored at the
oldest unacknowledged entry — `MINID` has the same hazard as `MAXLEN` once
pending work falls outside the retention window.

## Consumer names

The group grows too, and for a longer-lived reason: a worker registers its
consumer name on its first read, a name is derived per process start, and Redis
expires none of them. Four replicas across a year of daily deploys would leave
some fifteen hundred dead names in the group — enough to make `XINFO CONSUMERS`
useless exactly when an operator reaches for it.

The same loop that trims sweeps them: a consumer holding no pending entries and
idle longer than `consumer_idle_ms` — an hour by default — is removed. A live
worker touches the group at least every `block_ms`, so an hour is far above
anything a running process produces, and deleting one early would be harmless
anyway because Redis recreates it on the next read.

**Holding nothing is the hard condition, not idleness.** Redis makes the pending
entries of a deleted consumer unclaimable, so a consumer that still owns work is
never touched however long it has been quiet — that is where reclaim goes
looking for a dead worker's orphans. The check and the deletion are one Lua
script rather than two commands, because between them a live worker could take
an entry and the sweep would destroy it.

## Redis topologies

Standalone, Sentinel and Cluster are all covered by the test suite. Cluster is
why every key of a namespace carries the same hash tag; if you shard by
namespace, each namespace occupies its own slot.

Because a namespace lives in one slot, resharding moves the entire queue —
stream, liveness keys, scheduler ZSET and all — in a single migration. A worker
consuming through one is covered too: the test moves the slot under an in-flight
job and asserts the worker follows.

Cluster and Sentinel are alternative high-availability models — Cluster does its
own failover and does not use Sentinel — so there is no deployment running both.
