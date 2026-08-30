# Examples

Each file is a complete, importable application. They are imported by
`tests/test_examples.py`, so they cannot drift out of date silently.

| File                      | Shows                                                    |
| ------------------------- | -------------------------------------------------------- |
| `basic_app.py`            | a task, its dependencies, and an endpoint that queues it |
| `cron_jobs.py`            | scheduled work, and what a late run looks like           |
| `broker_mode.py`          | consuming a stream somebody else writes                  |
| `deferred_publication.py` | queueing inside a database transaction                   |

Run any of them as a web application:

```bash
uv run litestar --app examples.basic_app:app run
```

and its worker, from the same module:

```bash
uv run litestar --app examples.basic_app:app workers run --concurrency 10
```

Both need a Redis at `redis://localhost:6379/0`, or set `REDIS_URL`.
