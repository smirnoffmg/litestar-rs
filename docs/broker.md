# Broker mode

Subscribing to streams somebody else writes runs in the process that already
consumes your own queues, through the same consumer group. That is the point of
the design: one worker loop, not a second deployment.

```python
QueueConfig(
    registry=tasks,
    external=("{lrs}:orders", "{lrs}:payments"),
)
```

## Handlers get the raw record

A foreign entry has no envelope — the payload is in a format you did not choose
— so broker handlers are dispatched by **stream** rather than by task name, and
receive the record as Redis returned it:

```python
from litestar_rs import Record

async def on_order(record: Record) -> None:
    sku = record.fields[b"sku"]
    ...

brokers = {"{lrs}:orders": on_order}
```

Pass them to `run(...)` or `run_with_signals(...)` as `brokers=`.

## What is different

- **No result and no envelope decoding.** There is nothing to store an outcome
  against and nothing to decode.
- **No re-enqueue on failure.** The stream is not yours to write to, so a failing
  handler simply does not ack. Redelivery is the retry, and the delivery ceiling
  is what eventually moves a poisonous entry to the dead letter queue.
- **A separate read.** Foreign stream names carry none of your hash tag, so they
  are read in their own `XREADGROUP`. Joining them to yours would break the
  moment anyone runs this on a cluster.
- **The group still applies.** `ensure_group` creates the consumer group on
  external streams too, so reclaim, liveness and the delivery ceiling work there
  exactly as they do on your own.

## Naming

An external stream is named in full — this library does not build the key for
you, because it did not choose the name. If the stream lives in the same Redis
and you want it in the same cluster slot as your queues, give it your namespace's
hash tag.
