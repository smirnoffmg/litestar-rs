# Results and deduplication

## Results are asked for

Most work is enqueued and forgotten. Keeping an outcome for every job would be a
key and a TTL spent on nobody, so a result is opt-in per job:

```python
job_id = await reindex.enqueue(doc_id=doc_id, result_ttl_ms=300_000)
```

`enqueue` returns the job id whether or not a result was requested — that is
what an outcome is looked up by.

```python
result = await registry.result(job_id)
if result is not None and result.ok:
    ...
```

Waiting blocks rather than polls. The outcome is written to a key with a token
pushed to a list beside it; a waiter that wakes puts the token back, so several
waiters on one job all get it:

```python
result = await store.wait(job_id, timeout_s=30)
```

`wait` returns `None` on timeout.

## Failures are recorded too

A job that exhausts its retries records the failure before it is buried.
Otherwise a caller waiting on a job that will never finish blocks until its own
timeout and learns nothing about why. A job that is merely being retried records
nothing — it is not over, and the waiter should keep waiting.

```python
result = await store.wait(job_id, timeout_s=30)
if result and not result.ok:
    log.error("job failed: %s", result.error)
```

Only `bytes` travel as a result value. Anything richer is the application's own
encoding decision.

## Deduplication

Delivery is at-least-once and no protocol change makes it otherwise, so an
application that must not repeat a side effect needs a gate it controls:

```python
await charge.enqueue(invoice_id=invoice_id, dedup=f"charge:{invoice_id}")
```

The worker claims the key with `SET NX PX` immediately before calling the
handler. A job whose key is already taken is skipped and acknowledged, not
failed. The check sits there because that is the only point a duplicate can
still be stopped — by the time a worker has the job, both copies are already in
the stream.

The window is `WorkerConfig.dedup_ttl_ms`, one day by default. Choose it to
cover the longest period over which a repeat would be wrong, not the longest a
job takes.
