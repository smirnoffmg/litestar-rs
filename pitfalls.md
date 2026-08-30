# PITFALLS.md

Problems that have nothing to do with Redis, and therefore surface in
production. A companion to `README.md`, which covers the transport layer.

Every item here is a design requirement, not a note for later. Decisions marked
architectural cannot be retrofitted.

---

## 1. Enqueueing inside a database transaction

A handler writes a row and calls `enqueue` before `COMMIT`. The worker takes the
job within milliseconds, goes to the database and does not find the row. Or the
transaction rolls back and the job has already run.

Two options:

- **Transactional outbox** — write the job to the same database in the same
  transaction, with a separate relay moving it to the stream. Reliable, more
  expensive.
- **Deferred publication** — buffer the `enqueue` calls and flush them from
  SQLAlchemy's `after_commit` hook. Covers most cases.

The second is the default. It requires `enqueue` to know about the current
session, which means it **cannot be a free function with no context**. Build that
into the signature from the start.

## 2. Graceful shutdown

- SIGTERM → stop reading the stream, let in-flight work finish. The maximum task
  timeout and `terminationGracePeriodSeconds` in the manifest are related
  quantities; document the relationship explicitly.
- A task cancelled at shutdown is **not acked** and stays in the PEL.
  Distinguish it from an application failure.
- A watchdog after the soft cancellation: wait N seconds, then cut it off.
  An application-level bare `except:` swallows `CancelledError` and the worker
  never exits.
- A second SIGTERM/SIGINT means "now", and is not ignored.

## 3. Rolling deploys and unknown task names

During a rollout, v1 and v2 workers run at the same time. A v1 worker pulls a
task that is not in its registry.

The DLQ loses valid work. An infinite loop wedges.

The right answer: do not ack, put it back on the stream with a delay, count
`unknown_task` separately and expose the metric. The task will reach v2. The DLQ
threshold on that counter is measured in time (hours), not attempts.

Argument evolution: adding a field with a default is safe, removing one is not.
Argument deserialization is **not strict about unknown fields**.

## 4. Blocking code and starving the service coroutines

A synchronous driver or a CPU-heavy body blocks the event loop. What suffers is
not throughput but the ack and the liveness key refresh: the worker is alive but
looks dead, its tasks are reclaimed and the work is duplicated.

- Service operations run off the event loop (a separate thread, later the Rust
  side).
- An explicit `sync` mode for tasks, executed in a thread pool.
- Size the thread pool yourself. The asyncio default of `min(32, cpu + 4)` is a
  source of silent degradation.

## 5. Timeouts that do not work

`asyncio.timeout` cancels at the next await. It will not interrupt a CPU loop
with no await, a call inside a C extension, or a task in the thread pool —
threads cannot be killed.

The contract, in the documentation: a timeout is guaranteed **for async tasks
only**. For sync tasks what is guaranteed is execution in a thread pool without
interruption. A hard timeout requires process isolation, and forking with open
Redis connections and a live event loop means closing and recreating them in the
child.

Promising a timeout and not delivering it is worse than not offering one.

## 6. Time and connections

- The scheduler does not use the workers' local time: clock skew between pods
  produces jobs that start early or late. The time source is Redis's own `TIME`,
  inside Lua.
- A blocking `XREADGROUP BLOCK` occupies its connection entirely. Do not take it
  from the shared pool, or acks and service commands queue behind it. **At least
  two connections per worker.**
- During a Sentinel failover a parked `XREADGROUP` does not return an error
  straight away. A finite `socket_timeout` plus an explicit reconnect, or the
  worker silently stops working while still looking healthy to its probes.

## 7. Idempotency as a contract

The guarantee is at-least-once. That belongs in the first paragraph of the
README, not in an FAQ section.

Provide the tool: a per-task dedup key (`SET NX EX`) checked before execution,
with an explicit choice of what a collision does — skip, or wait for the first
one's result. Without it users will write non-idempotent tasks and consider the
resulting bugs ours.

## 8. Fairness between sources

One tenant, or one kind of task, floods the queue with a million records and
everyone else waits for hours. Priorities do not help: they are about kinds, not
sources.

Shard the streams by key with round-robin reads, or use quotas. **An
architectural decision; it cannot be added later.**

## 9. Payload

- Redis is in memory: large arguments are a direct OOM. A threshold (64–256 KB)
  above which the payload goes to S3 or a database and the stream carries a
  reference. Needed from the start, or the first "let's pass a dataframe through
  here" takes the instance down.
- Serialization is msgspec/JSON against an explicit schema. **Do not introduce
  `pickle` in any form**: deserializing a payload out of Redis is remote code
  execution the moment Redis is compromised.
- The task-name → function mapping goes strictly through a registry. Never import
  by a string taken from the message.

## 10. Testability

Without this, nobody chooses the library:

- an eager mode, where the task runs synchronously at enqueue time, for unit
  tests;
- a pytest fixture running a real worker in the background, for integration
  tests;
- deterministic time for cron and delayed jobs;
- `assert_enqueued` — checking that a job was queued without running it.

This is not secondary. It is what distinguishes the library from somebody else's
in the eyes of whoever is choosing.
