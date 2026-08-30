# Design and invariants

The rules this library is built to, and why each one is a rule rather than a
preference. Violating any invariant here is a bug, not a trade-off.

## Delivery guarantee

**Delivery is at-least-once.** A task can run more than once: a worker that
completed its side effect and died before `XACK` will be reclaimed and the work
repeated. No amount of protocol work removes that — only an idempotent handler
does. The library gives you a gate for it: a `dedup` key on the record, claimed
by the worker with `SET NX PX` immediately before the handler is called. A key
already taken means the job is skipped and acknowledged. Treating delivery as
exactly-once is a design error in the application, not a bug here.

## What this is

Two layers, hard-separated:

- **core** — transport, worker, scheduler. Depends on `redis`, `msgspec`,
  `anyio`. **Importing Litestar here is forbidden.**
- **plugin** — DI, CLI, serialization, health. Imports core.

Core must be usable without Litestar.

## Modes

1. **Task queue**: `enqueue → result`, typed arguments, retries, delayed
   execution, cron.
2. **Broker**: subscription to external streams carrying somebody else's payload
   format, with no result backend.

Both modes run over one consumer group and one worker process. That is the main
thing separating this from the alternatives; do not break it in a refactor.

## Transport invariants

Violating any of these is a bug, not a trade-off.

**Credit-based prefetch.** `XREADGROUP COUNT = max(0, concurrency - in_flight)`.
Do not read with no free slots. A fixed COUNT skews distribution between workers
(see taskiq-redis#91).

**Ack removes the record.** `XACK` + `XDEL` in one Lua script. `XACK` alone
leaves the record in the stream and the stream leaks. A background `XTRIM MINID`
by time runs alongside. `MAXLEN ~` is not used: trimming by length can discard
unacknowledged work.

**Liveness is separate from min-idle-time.** `XAUTOCLAIM` cannot tell a dead
worker from a live one running a long task. A worker holds
`SET {ns}:alive:<entry> PX ttl` and refreshes it from the worker supervisor, not
from the task body. Reclaim is a Lua script: check the key is absent, then
`XCLAIM`. Application code knows nothing about the heartbeat, and should not.

**Retries and reclaims are different counters.** `delivery_count` from
`XPENDING` counts reclaims, not application retries. They are stored separately
with a threshold each. Exceeding either → `XACK` + `XADD` to the DLQ stream with
the original payload, a reason, a traceback and the attempt history. DLQ record
fields: everything from the original record plus `dlq_reason` (`max_attempts`,
`max_deliveries`, `unknown_task`, `malformed`), `dlq_detail` (the traceback of
the final failure), `dlq_source`, `dlq_deliveries`, `dlq_at`. Earlier attempts
ride in the record itself as `history` — one truncated line per failed attempt,
capped in count, or a job failing in a loop grows its own record.

The DLQ write and the retry scheduling both happen **before** the `XACK`. The
other order would lose work on a crash between the steps; this one only costs
idempotency, which was never promised.

**A scheduler with no dedicated process.** A ZSET holds delayed jobs and cron.
Moving ZSET → stream is one Lua script: `ZRANGEBYSCORE LIMIT` → `XADD` → `ZREM`,
atomically. The leader is elected with `SET NX PX` and renewed by
compare-and-set; any worker can become one. No separate scheduler process.

**Cron: late rather than lost, and missed occurrences collapse.** The next
occurrence goes into the ZSET as soon as the previous one fires, so an outage
delays a job rather than dropping it. `enqueued_at` carries the instant the job
was due rather than the instant it reached the stream, which is what makes a
late run recognisable as late.

An outage longer than the schedule interval collapses the missed occurrences
into a single run: a daily job comes back from three days down and runs once,
not three times. Catch-up runs are not planned. Running every missed occurrence
means keeping a history of firings, and what an application usually wants is not
that but to bring state up to date. A job that genuinely needs to work through
the gap must take the interval as an argument and handle it itself.

**Priorities.** `XREADGROUP` cannot express strict priority: with `BLOCK` it
wakes on whichever stream has something. The scheme is a non-blocking sweep from
high to low, and only when everything is empty a single blocking read across all
of them. A pass counter bounds starvation of the low queue.

**Hash-tagged keys from the first commit.** `{ns}:q:high:0`, `{ns}:q:low:0`,
`{ns}:sched`. A multi-key `XREADGROUP` in Redis Cluster requires a common slot.
The key schema cannot be changed later without a migration.

**Depth is `lag` from `XINFO GROUPS`** (Redis 7+). `XLEN` after a trim is
meaningless.

## Litestar layer invariants

**Task scope in DI.** Resolve the application's `Provide` graph outside a
request: `sync_to_thread`, `use_cache`, generator dependencies with correct
teardown. A provider that genuinely needs a request or headers is an error at
worker startup, not at runtime. The target shape of a task:

```python
@app_tasks.task
async def reindex(doc_id: UUID, db: AsyncSession, cfg: Settings) -> None: ...
```

A `ctx` dictionary as a way to smuggle dependencies through is not introduced in
any form.

**Serialization is the application's msgspec.** The same encoder/decoder and
`type_encoders` Litestar uses. Task signatures are settled by the same machinery
as handlers; incompatible arguments are caught at registration, not in a worker.

**CLI through `CLIPluginProtocol`.** One entrypoint:
`litestar workers run --queue high --concurrency 20`. A string path to the
application (`"module:app"`) as a required init parameter is an anti-pattern;
do not repeat it.

**Tracing in stream fields.** A stream record is a flat hash. `traceparent` goes
in a field beside the payload, never inside it. The worker restores the span
context, so the HTTP → task chain is not broken.

**A dropped connection does not kill the worker.** The consume loop and every
supervisor loop survive a connection error, log it and carry on after a pause:
redis-py reconnects on the next command, and records the worker had already
taken stay in the PEL to be reclaimed. This is the failure a Sentinel failover
produces — a blocking `XREADGROUP` does not return an error straight away. It is
tested twice: with `CLIENT KILL` against a standalone server, and with a real
`SENTINEL FAILOVER` under an in-flight job.

**Health.** The plugin serves an endpoint with consumer group state and lag, and
the worker serves the same route from the same function
(`litestar workers run --health-port 8081`), so readiness probes for the web and
worker deployments ask an identical question.

## Execution contracts

**A timeout is guaranteed for async tasks only.** `anyio.fail_after` cancels at
the next await; it will not interrupt a CPU loop with no await, a call inside a C
extension, or a task in the thread pool — threads cannot be killed. A
synchronous task that declares a timeout is therefore **refused at startup**
rather than quietly ignoring it. Promising a timeout and not delivering it is
worse than not offering one.

**Synchronous tasks run in a thread pool** of an explicitly configured size
(`thread_limit`). The asyncio default of `min(32, cpu + 4)` is a silent
bottleneck. Blocking code inside an async task remains the application's problem:
it starves the liveness refresh, and a worker that is alive looks dead.

**Large arguments leave Redis.** Above a threshold the payload goes to the
application's `PayloadStore` and the record carries a `payload_ref`. Without a
store the transport's own limit refuses the record — nothing is dropped
silently. Redis keeps the stream in memory; a payload measured in megabytes is a
direct route to an OOM kill.

## Payload safety

**`pickle` is not introduced in any form.** Deserializing a payload taken out of
Redis is remote code execution the moment Redis is compromised, and a queue is
exactly the component an attacker reaches first. Payloads are msgspec against an
explicit schema, and an import-linter contract forbids `pickle` anywhere in the
package rather than trusting anyone to remember.

**Task names resolve through the registry**, never by importing a string taken
from the message. A name that is not registered is deferred and eventually
buried; it is never a path to something importable.

**Large arguments leave Redis** rather than being trusted to be small. See the
execution contracts above.

## Not doing

A web UI, non-Redis backends, a synchronous API, Python < 3.13, a serialization
format of our own.

## Tests

Mandatory, written with the code rather than after it:

- SIGKILL a worker with entries in the PEL → the job is picked up by another
  worker, exactly once
- a long task (longer than min-idle-time) is not reclaimed while it is alive
- scheduler split brain: two leaders do not move the same job out of the ZSET
  twice
- the stream does not grow under a steady flow of acks
- a delayed job is not lost by a trim
- a dropped connection under in-flight work: the worker keeps going rather than
  going quiet
- the whole multi-key surface against Redis Cluster: reads across streams, both
  Lua scripts, the ZSET promotion
- `SENTINEL FAILOVER` under an in-flight job: the worker survives the promotion
- a cluster slot carrying the whole namespace moves under an in-flight job: the
  worker follows it
- a failing task comes back with `attempt` raised rather than looping forever
- a task out of attempts lands in the DLQ with its payload, reason, traceback and
  history
- an entry reclaimed past its ceiling lands in the DLQ rather than running again
- an unknown task name is deferred, and buried only on a time threshold
- cron across a DST transition
- a missed cron occurrence runs late rather than being lost
- an outage longer than the schedule interval produces one run, not one per
  missed occurrence
- a synchronous task runs off the event loop
- a synchronous task declaring a timeout is refused at startup

Integration tests run against a real Redis in a container, never a mock. Lua
scripts are tested separately for atomicity under concurrent calls.

## Style

Full typing, `mypy --strict`. The public API is only what `__init__` exports
explicitly. Configuration errors surface at process startup, naming the specific
provider or task.

## Tooling

All four run in pre-commit and in CI. Any of them red blocks the merge; `# noqa`,
`# type: ignore` and `ignore_imports` without a comment giving the reason are not
accepted.

**ruff** — linter and formatter, no others. `target-version = "py313"`,
formatting by `ruff format`. Rule set, minimum: `E`, `F`, `I`, `UP`, `B`,
`ASYNC`, `S`, `RET`, `SIM`, `TID`, `PTH`, `RUF`. `ASYNC` and `B` are
non-negotiable: a blocking call in a worker coroutine and a mutable default in a
task signature are real bugs of this project, not style. Tests are relaxed on
`S101` alone.

**mypy** — `python_version = "3.13"`, `strict = true` over `src` and `tests`.
`disallow_untyped_defs`, `warn_return_any`, `warn_unused_ignores` are on.
`ignore_missing_imports` is never set globally — only per module, with a comment.
The `@task` decorator must preserve the signature: an argument type mismatch is
caught where `enqueue` is called, not in a worker.

**pytest** — `--strict-markers`, `--strict-config`, `-ra`. Markers: `unit` (no
external dependencies), `integration` (a real Redis in a container), `slow`.
Async tests run through `anyio` — the same runtime as the core — not
pytest-asyncio. The coverage floor is 90%, and Lua coverage is counted through
the integration tests. Tests do not depend on execution order or on real time:
clocks and timers are injected, and a `sleep` in a test is grounds for review.

**import-linter** — the core/plugin boundary from the first section is checked
by machine rather than asserted in prose. Contracts:

- `forbidden`: core imports neither `litestar` nor `click`, transitively or under
  `TYPE_CHECKING`
- `layers`: plugin → core, never the reverse
- `independence`: transport, scheduler and result backend do not import each
  other, only shared protocols
- `forbidden`: core tests do not import `litestar` — otherwise "core works
  without Litestar" is confirmed by nothing

Contracts land with the first commit of a layer, not after the boundary has
already been crossed.
