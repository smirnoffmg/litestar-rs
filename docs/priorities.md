# Priorities and fairness

```python
QueueConfig(registry=tasks, queues=("high", "default", "low"))
```

Queues are listed highest first. A task picks its queue at declaration:

```python
@tasks.task(queue="high")
async def urgent(...) -> None: ...
```

## Why a sweep

`XREADGROUP` with `BLOCK` wakes on whichever stream has work first, so it cannot
express priority at all. A worker configured with more than one queue therefore
sweeps them without blocking, highest first, and only when every one comes back
empty does it issue a single blocking read across all of them.

The non-blocking sweep passes no `BLOCK` argument rather than `BLOCK 0` — the
latter waits forever, which is the opposite of what it reads like.

A single-queue worker keeps the simpler shape: one blocking read, no sweep, no
extra round trip.

## Bounding starvation

Strict priority starves the low queue outright. A pass counter inverts the order
every `fairness_every` passes and gives the low queue first refusal, which puts
a bound on how long anything can sit behind a busy high-priority queue. Set it
to `0` to disable and accept strict priority.

## Shards

```python
QueueConfig(registry=tasks, shards=4)
```

Each queue is spread over `shards` streams, and a job lands on one of them by a
deterministic hash of its id. Priorities are about *kinds* of work; shards are
about *sources*, which is a different problem: one tenant flooding a queue with
a million records starves everyone else regardless of priority.

The shard is part of the key from the first release — `{ns}:q:default:0` — so
the number can grow later without a schema migration.

## Keys and Redis Cluster

Every key of a namespace carries the same literal `{ns}` hash tag, so all of
them land in one Cluster slot. Multi-key `XREADGROUP` across queues and shards,
and both Lua scripts, require it. The test suite runs the whole multi-key
surface against a real cluster.

Separate namespaces need not share a slot: they are separate deployments.
