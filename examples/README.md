# Examples

Each file is a complete, importable application. They are imported by
`tests/test_examples.py`, so they cannot drift out of date silently.

| File                         | Shows                                                            |
| ---------------------------- | ---------------------------------------------------------------- |
| `basic_app.py`               | a task, its dependencies, and an endpoint that queues it         |
| `priorities.py`              | queues by kind of work, shards by source                         |
| `sync_tasks_and_timeouts.py` | blocking work off the event loop, and what a timeout can promise |
| `large_payloads.py`          | arguments too big for a stream                                   |
| `cron_jobs.py`               | scheduled work, and what a late run looks like                   |
| `delayed_jobs.py`            | one job, later — without a scheduler process                     |
| `results.py`                 | asking for an outcome and waiting for it                         |
| `deduplication.py`           | a side effect that must happen once                              |
| `retries_and_dlq.py`         | failing work, the dead letter queue, and replaying from it       |
| `deferred_publication.py`    | queueing inside a database transaction                           |
| `broker_mode.py`             | consuming a stream somebody else writes                          |
| `broker_only.py`             | the same, in a deployment that owns no queue at all              |
| `health_endpoint.py`         | serving queue health on a path you choose                        |
| `tracing.py`                 | keeping a request and the job it queued in one trace             |

Run any of them as a web application:

```bash
uv run litestar --app examples.basic_app:app run
```

and its worker, from the same module:

```bash
uv run litestar --app examples.basic_app:app workers run --concurrency 10
```

Both need a Redis at `redis://localhost:6379/0`, or set `REDIS_URL`.

Two are not web applications. `core_without_litestar.py` runs on its own:

```bash
uv run python -m examples.core_without_litestar
```

and `testing_your_app.py` is a pytest module:

```bash
uv run pytest examples/testing_your_app.py
```

Its last test is skipped unless `REDIS_URL` is set, because it runs a real
worker.
