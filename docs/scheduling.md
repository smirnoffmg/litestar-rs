# Scheduling

There is no scheduler process. Every worker tries to take a short lease; the one
holding it moves due jobs from a ZSET into their streams. Losing the lease costs
nothing — the next holder resumes from the same ZSET, and the promotion script
makes a double pass a no-op.

## Delayed jobs

The snippets on this page assume:

```python
from litestar_rs import CronJob, Envelope, RedisScheduler
from litestar_rs.plugin import QueueConfig, TaskRegistry

tasks = TaskRegistry()
envelope = Envelope(id="job-1", task="reindex", payload=b"{}", enqueued_at=0)
due_at_ms = 1_712_345_678_901
```

```python
from litestar_rs import RedisScheduler

await scheduler.schedule_in(envelope, queue="default", delay_ms=60_000)
await scheduler.schedule_at(envelope, queue="default", when_ms=due_at_ms)
```

Delays are measured by Redis, not by the worker. Clock skew between pods would
otherwise start jobs early or late; the promotion script reads Redis's own
`TIME`.

The entry itself lives in a hash beside the ZSET, which keeps payloads as opaque
bytes rather than encoding them a second time to fit in a sorted-set member.

## Cron

Arguments, if the task takes any, are the encoded payload:

```python
CronJob(name="trim", expression="0 3 * * *", task="trim",
        payload=b'{"older_than_days": 30}')
```

A task taking no arguments needs nothing: the default is an empty object.

```python
from litestar_rs import CronJob

nightly = CronJob(
    name="nightly-reindex",
    expression="30 2 * * *",
    task="reindex",
    timezone="Europe/Berlin",
)

QueueConfig(registry=tasks, cron=[nightly])
```

The expression, the timezone and the job name are validated when `CronJob` is
built, so a typo is a startup error rather than a schedule that never fires.

Occurrences are resolved in the job's own timezone, which is what makes daylight
saving behave: a time that does not exist on the spring-forward day moves to the
next instant instead of being skipped, and a time that happens twice in autumn
fires once.

An occurrence's identifier encodes the instant it is due, so two leaders
computing the same occurrence write the same entry rather than a duplicate.

## Missed occurrences

**Late rather than lost.** The next occurrence goes into the ZSET as soon as the
previous one fires, so an outage delays a job rather than dropping it.
`enqueued_at` carries the instant the job was due rather than the instant it
reached the stream, which is what lets a handler recognise a late run.

**Missed occurrences collapse into one.** A daily job comes back from three days
down and runs once, not three times. Catch-up runs are not planned: running
every missed occurrence means keeping a history of firings, and what an
application usually wants is to bring state up to date. A job that genuinely
needs to work through the gap must take the interval as an argument and handle
it itself.

## The lease

`WorkerConfig.leader_ttl_ms` must outlast `scheduler_interval_s`, or the lease
lapses between passes and leadership flaps between workers. That relationship is
checked when the configuration is built.

Renewal is a compare-and-set: a worker that has already lost the lease cannot
keep extending the new holder's key, which is how two leaders would otherwise
both come to believe in themselves.
