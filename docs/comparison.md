# How this differs

Every claim below about another library is linked to that library's own
documentation. Where a claim is about a default, the default is named — several
of these differences only bite at the default and disappear once someone tunes
the setting.

This is not a feature matrix. The libraries here are all capable of running
background work; what separates them is the mechanism they use when something
goes wrong, and that is what this page is about.

## Detecting a worker that died

This is the difference that matters most, because it decides what happens to a
task that takes a long time.

**Celery on Redis** does not use acknowledgements. Redis has no ack for a list
pop, so Celery emulates one: a message is kept on consumption and removed once
acknowledged, and anything unacknowledged is redelivered after the
**visibility timeout**, one hour by default. Celery's own documentation names
the consequence — a task that runs longer than the window "can cause execution
loops for long-running tasks", and a cold shutdown leaves work waiting out the
whole window before a peer picks it up
([Redis caveats](https://docs.celeryq.dev/en/stable/getting-started/backends-and-brokers/redis.html)).

**taskiq-redis**, with its `RedisStreamBroker`, uses real consumer groups and
real `XACK`, and reclaims what has been idle longer than `idle_timeout` —
600 000 ms, ten minutes, by default
([architecture](https://github.com/taskiq-python/taskiq-redis)).
That is a genuine improvement on emulated acks, but idle time is still the only
signal: `XAUTOCLAIM` cannot tell a worker that died from a worker that is busy.

**SAQ** does not have this problem: it keeps a heartbeat monitor for abandoned
jobs and sweeps stuck ones
([comparison](https://github.com/tobymao/saq/blob/main/docs/comparison.md)), so
a long task with a live owner is not taken away from it. On this axis SAQ and
smallage agree, and both differ from Celery-on-Redis.

**smallage** treats those as different questions. Idle time is a floor, not the
signal. A worker holds `SET {ns}:alive:<entry> PX ttl` per entry it is running
and refreshes it from the supervisor rather than from the task body, so the
refresh keeps happening while the handler is busy. Reclaim is a Lua script that
checks the key is *absent* before `XCLAIM`. A task may run for an hour without
anyone taking it, and a worker that dies is noticed within one TTL.

The cost is honest: an extra key per in-flight entry and a refresh loop. The
benefit is that "how long may a task run" and "how fast do we notice a death"
stop being the same number.

## Prefetch

`XREADGROUP COUNT` decides how many entries a worker takes per read.
taskiq-redis takes a fixed `xread_count`, 100 by default
([configuration](https://github.com/taskiq-python/taskiq-redis)). A fixed count
is what skews distribution between workers: a worker asks for 100 whether or not
it has anywhere to put them, and the entries sit in its buffer while an idle peer
reads nothing (taskiq-redis#91).

smallage reads `max(0, concurrency - in_flight)` and does not read at all with no
free slot. Entries stay in the stream, where any worker can take them, until a
worker actually has capacity.

## Trimming

Acking deletes the entry here — `XACK` and `XDEL` in one script — so a healthy
stream stays near empty and trimming is a backstop rather than the mechanism.

The backstop is `XTRIM MINID` by time, floored at the oldest unacknowledged
entry. taskiq-redis offers `maxlen` with `approximate=True`
([configuration](https://github.com/taskiq-python/taskiq-redis)). Trimming by
length is the hazard: `MAXLEN` counts entries without knowing which of them are
unacknowledged, so a burst can push pending work past the cap and drop it. The
same hazard exists for `MINID` once pending work is older than the retention
window, which is why the floor is there.

## Two counters instead of one

An application failure and a reclaim are different events, and merging them
buries healthy work after a few deploys.

**dramatiq** keeps a dead letter queue and moves a message there when it exceeds
its retry limit or its `max_age`, holding it seven days
([guide](https://github.com/bogdanp/dramatiq/blob/master/docs/source/guide.md)).
It acknowledges only after successful processing, and redelivers what a crashed
worker was holding
([advanced](https://github.com/bogdanp/dramatiq/blob/master/docs/source/advanced.md)).

smallage separates the two thresholds. `attempt` travels in the record and counts
application failures against `max_attempts`; `delivery_count` comes from
`XPENDING` and counts reclaims against `max_deliveries`. An entry taken from five
dead owners goes to the dead letter queue directly rather than being handed to a
sixth victim, and a task that failed three times goes there for a different
reason, recorded as a different `dlq_reason`. A buried record keeps the original
payload, the final traceback and one line per earlier attempt, so a replay is a
re-enqueue rather than a reconstruction.

## SAQ, which is the closest neighbour

SAQ is the comparison worth making carefully, because it agrees with this
library about more than it disagrees: both are async-native, both keep a
heartbeat rather than a visibility timeout, both do cron without a separate
process, and both are aimed at people who found Celery heavier than their
problem.

Where it differs is the primitive. SAQ stores a job as a key, tracks it in an
`incomplete` sorted set and pushes its id onto a list, dequeuing with `BLMOVE`
or `RPOPLPUSH` plus a pubsub notification; its enqueue is a Lua script that
refuses a duplicate id outright
([queue/redis.py](https://github.com/tobymao/saq/blob/main/queue/redis.py)).
That buys it things this library does not have: job uniqueness enforced at
enqueue, abort, and a claimed sub-5ms latency that it measures at up to eight
times arq's
([comparison](https://github.com/tobymao/saq/blob/main/docs/comparison.md)).

This library stores a job as a stream entry read through a consumer group. What
that buys, and SAQ's layout does not offer:

- **Somebody else's streams.** A consumer group can be created on a stream this
  application never writes to, so foreign traffic is consumed by the same worker
  loop, with the same reclaim and the same delivery ceiling. A list you did not
  push to is not something you can join.
- **The pending list as the source of truth.** Ownership, delivery count and
  idle time come from Redis itself via `XPENDING`, not from a structure the
  library maintains alongside the job.
- **Priority and sharding in the key schema**, hash-tagged so the whole
  namespace lands in one Cluster slot, and exercised against a real cluster and
  a real Sentinel failover in the test suite.

And what SAQ has that this does not: a web interface, a PostgreSQL backend, and
job abort. If those matter, that is the answer.

## Priorities

taskiq-redis names one `queue_name` per broker
([configuration](https://github.com/taskiq-python/taskiq-redis)). Priority
between kinds of work is then a matter of running more than one broker.

smallage takes an ordered list of queues and sweeps them without blocking, high
to low, blocking across all of them only when every one is empty — because
`XREADGROUP` with `BLOCK` wakes on whichever stream has something and cannot
express priority at all. A pass counter periodically inverts the order so the low
queue cannot starve. Shards are the second axis: each queue spreads over N
streams and a job lands on one by a hash of its id, so one noisy tenant occupies
one shard rather than the queue.

## Shutdown

**arq** cancels any job still running when a worker shuts down, raising
`CancelledError` inside it, and marks the job to be retried when a worker starts
again ([docs](https://arq-docs.helpmanual.io/)). Nothing is lost, but nothing is
finished either: a job two seconds from done is restarted from the beginning on
the next deploy.

smallage drains. SIGTERM stops new reads and lets in-flight work finish; only
when `drain_timeout_s` runs out does the watchdog cut it, and what it cuts stays
unacked in the pending list with its liveness key dropped, so a peer takes it at
once rather than waiting out a TTL. A second signal means now.

## Scheduling

Celery's periodic tasks run from `celery beat`, a process separate from the
workers ([periodic tasks](https://docs.celeryq.dev/en/stable/userguide/periodic-tasks.html)).

smallage has no scheduler process. Delayed jobs and cron share one ZSET, and
whichever worker holds a short lease promotes what is due; the promotion is a Lua
script, so a second promoter is a no-op rather than a duplicate. Losing the lease
costs nothing because the next holder resumes from the same ZSET. An outage
collapses missed occurrences into a single run rather than replaying them.

## What is deliberately absent

- **No result backend by default.** A result is opt-in per job, because keeping
  an outcome for work nobody waits on is a key and a TTL spent on nobody.
- **No `pickle`, in any form.** Payloads are msgspec against an explicit schema,
  and an import contract forbids `pickle`, `shelve`, `dill` and `cloudpickle`
  anywhere in the package. Deserialising a payload out of Redis is remote code
  execution the moment Redis is compromised.
- **No non-Redis backends.** The key schema, both Lua scripts and the cluster
  hash tags are Redis Streams specific. A second backend would either be a worse
  version of this one or force the design down to a common denominator.
- **No web UI.**

## When another one is the better answer

- **You are not on Redis.** Celery and dramatiq both speak RabbitMQ, and a real
  broker does durably what this library reconstructs out of streams and keys.
- **You need Python older than 3.13**, or a synchronous API. Neither is offered.
- **You are already on taskiq** and its broker abstraction is what you want; this
  library has no broker abstraction, and that is the point of it.
- **You want a web UI, job abort, or Postgres rather than Redis.** SAQ has all
  three and this library has none of them.
- **Enqueue-time uniqueness is what you need.** arq and SAQ both refuse a
  duplicate job id outright; here the gate is `dedup`, claimed by the worker
  immediately before the handler, which stops the side effect rather than the
  enqueue.
- **Raw throughput is the constraint and Redis is not negotiable.** The
  [benchmarks](https://github.com/smirnoffmg/smallage/tree/main/benchmarks)
  put this library at roughly a quarter of what the same two Redis commands
  manage with no queue on top. That gap is what durability, retries, liveness
  and dispatch cost, and nothing here will close it.
- **Your work is message routing rather than task execution.** FastStream is
  built for consuming somebody else's streams as a first-class activity; here
  that is one mode beside a task queue, sharing its worker loop.
