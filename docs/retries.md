# Retries and the dead letter queue

## Two counters, deliberately apart

`delivery_count` from `XPENDING` rises every time an entry is taken from a
worker that stopped refreshing its liveness key — a crashed pod, a rolling
restart. It says nothing about whether the task itself is failing. Mixing it
with application failures sends healthy work to the DLQ after a few deploys.

- **`attempt`**, carried in the record, counts application failures. Threshold:
  `RetryPolicy.max_attempts`.
- **`delivery_count`**, from Redis, counts reclaims. Threshold:
  `RetryPolicy.max_deliveries`.

An entry taken from that many dead owners goes straight to the DLQ rather than
being handed to another victim: backing off would only spread the damage.

## Backoff

```python
from litestar_rs import RetryPolicy, WorkerConfig

WorkerConfig(retry=RetryPolicy(max_attempts=5, initial_backoff_ms=2_000))
```

Delay grows geometrically to `max_backoff_ms`, with jitter on by default —
without it a batch that fails together retries together, forever.

A retry is scheduled through the scheduler, because exponential backoff needs
delayed delivery and streams have none. The retry is put on the clock **before**
the current entry is acked: a crash in between duplicates the job, which
at-least-once already permits, whereas the other order would lose it.

## What lands in the DLQ

| `dlq_reason` | Cause |
| --- | --- |
| `max_attempts` | the task kept failing |
| `max_deliveries` | the entry kept outliving its owners |
| `unknown_task` | no worker recognised the name within the time threshold |
| `malformed` | the record cannot be decoded by any deployment |

A buried record carries everything from the original plus `dlq_detail` (the
traceback of the final failure), `dlq_source`, `dlq_deliveries` and `dlq_at`.
Earlier attempts ride in the record's own `history` field — one truncated line
per failed attempt, capped in count, or a job failing in a loop would grow its
own record.

## Reading and replaying

The DLQ is a stream at `{ns}:dlq`:

```python
entries = await client.xrange(f"{{{namespace}}}:dlq", count=100)
for entry_id, fields in entries:
    print(fields[b"dlq_reason"], fields[b"task"], fields[b"dlq_detail"])
```

The original payload is untouched, so replaying is re-enqueueing it. Reset
`attempt` unless you want the replay to inherit the exhausted budget:

```python
from litestar_rs import from_fields  # litestar_rs.core
import msgspec.structs

envelope = from_fields(fields)
await transport.enqueue(msgspec.structs.replace(envelope, attempt=0), queue="default")
```

## Unknown task names

During a rollout, a v1 worker will meet a task only v2 knows. That is not a
failure: the job is deferred back with a delay, its `attempt` untouched, and the
worker remembers not to reclaim it itself.

The threshold for burying it is **time**, not attempts —
`RetryPolicy.unknown_task_timeout_ms`. A deploy is measured in minutes, and
counting tries would bury good work halfway through one.

## What is not retried

- A record nothing can decode is buried immediately. No deployment will ever
  read it, so retrying is noise.
- Work cancelled by the shutdown watchdog is not an application failure: it stays
  in the PEL unacked with its liveness key dropped, so a peer takes it at once.
- A broker-mode entry is on somebody else's stream and cannot be rewritten;
  redelivery is its only retry, bounded by the delivery ceiling.
