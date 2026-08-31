# Broker mode

Subscribing to streams somebody else writes runs in the process that already
consumes your own queues, through the same consumer group. That is the point of
the design: one worker loop, not a second deployment.

The snippets on this page assume:

```python
from litestar_rs import Record
from litestar_rs.plugin import QueueConfig, TaskRegistry

tasks = TaskRegistry()


async def on_order(record: Record) -> None: ...
async def on_payment(record: Record) -> None: ...
```

```python
QueueConfig(
    registry=tasks,
    brokers={"{lrs}:orders": on_order, "{lrs}:payments": on_payment},
)
```

One mapping rather than a list of streams beside a dictionary of handlers: a
stream with no handler is read and dropped, a handler for a stream nobody
subscribes to never runs, and two fields that must agree eventually will not.

## Handlers get the raw record

A foreign entry has no envelope — the payload is in a format you did not choose
— so broker handlers are dispatched by **stream** rather than by task name, and
receive the record as Redis returned it:

```python
async def on_order(record: Record) -> None:
    sku = record.fields[b"sku"]
    print(sku)
```

`litestar workers run` passes them to the worker for you. Using the core
directly, they go to `run(...)` or `run_with_signals(...)` as `brokers=`.

## What is different

- **No result and no envelope decoding.** There is nothing to store an outcome
  against and nothing to decode.
- **No re-enqueue on failure.** The stream is not yours to write to, so a failing
  handler simply does not ack. Redelivery is the retry, and the delivery ceiling
  is what eventually moves a poisonous entry to the dead letter queue.
- **A separate read, and — in a worker that owns queues — a latency floor with
  it.** Foreign stream names carry none of your hash tag, so they are read in
  their own `XREADGROUP`. Joining them to yours would break the moment anyone
  runs this on a cluster. Where you have queues of your own, that separate read
  is a non-blocking one: `XREADGROUP` with `BLOCK` wakes on whichever stream has
  something and so cannot express priority between your queues. A foreign entry
  therefore waits up to `block_ms` before it is seen, five seconds by default.
  Lower `block_ms` if broker latency matters; it costs more round trips while
  the queues are idle. A worker with no queues of its own pays none of this —
  see below.
- **The group still applies.** `ensure_group` creates the consumer group on
  external streams too, so reclaim, liveness and the delivery ceiling work there
  exactly as they do on your own.

## A worker that owns no queues

A deployment that only consumes streams somebody else writes names no queue at
all:

```python
QueueConfig(registry=tasks, queues=(), brokers={"{lrs}:orders": on_order})
```

Nothing is read but the foreign streams, and nothing is created in order to read
them: no queue stream, no consumer group on one, and nothing for the trim loop to
visit. Such a deployment previously had to declare a queue nobody wrote to, and
the stream and group it left behind were indistinguishable, to anyone reading the
keyspace later, from ones something was meant to be writing to.

There is no latency floor here either. With no queues of its own there is no
priority to protect, so the read of the foreign streams is the blocking one and
an entry is picked up as it is written.

A worker with neither queues nor broker streams is refused when it is built: it
would read nothing, and saying so at startup beats a silent idle process.

**Enqueueing still works.** `queues=()` says what this deployment *reads*, not
what it writes. A task enqueued from it goes onto its queue exactly as before —
creating that queue's stream in your namespace, which is then read by whichever
deployment consumes it. That split across deployments is a legitimate topology
and is deliberately not refused.

## Naming

An external stream is named in full — this library does not build the key for
you, because it did not choose the name. If the stream lives in the same Redis
and you want it in the same cluster slot as your queues, give it your namespace's
hash tag.
