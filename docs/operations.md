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

## Connections

Each worker opens two clients. A blocking `XREADGROUP` occupies its connection
for the whole block window, and an ack or a liveness refresh queued behind it
makes a healthy worker look dead to its peers.

The reader's `socket_timeout` is derived from the block window: below it every
healthy blocking read would die, and above it is the only thing that surfaces a
connection hung by a failover.

## Health

The plugin serves `QueueConfig.health_path` (`/health/queue` by default). If the
application already uses that path, startup fails with the clash named rather
than one handler shadowing the other — move it, or set `health_path=None` and
serve the same data yourself:

```python
from litestar import get

from litestar_rs.plugin import QueuePlugin, queue_health


@get("/healthz")
async def healthz(plugin: QueuePlugin) -> dict[str, object]:
    queue = await queue_health(plugin.transport, plugin.stats)
    return {"queue": queue, "database": ...}
```

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

A worker deployment has nothing to probe unless you ask for it:

```bash
litestar workers run --health-port 8081
```

That serves the same route, built by the same function, on the worker. A
readiness probe against a worker then asks an identical question to one against
a web process rather than a lookalike. It needs a server —
`litestar[standard]`, which the `litestar` command itself already requires.

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
