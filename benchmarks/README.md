# Benchmarks

```bash
uv run --group bench python -m benchmarks                    # starts a container
REDIS_URL=redis://localhost:6379/0 uv run --group bench python -m benchmarks
```

## What is measured

**throughput** — fill the stream, then drain it. The worker is saturated
throughout, so the number says how fast the loop moves entries and nothing about
how long anything waited. Enqueue and drain are reported apart because they
bottleneck at different ends.

**latency** — an idle worker, one job at a time, timed from enqueue to handler.
This is what a request-shaped workload feels. Measured with an event rather than
a polling loop, because a poll interval would quantise every sample to its own
step.

The handler does nothing. A benchmark whose handler works measures the work.

## What is not measured

Failure. Everything interesting about a queue happens when a worker dies, and
none of these numbers say anything about that — which is the reason
[the comparison](../docs/comparison.md) is about mechanisms and this is about
speed. A queue that loses your job is arbitrarily fast.

Also absent: multiple workers, contention between them, payloads of a realistic
size, and a Redis that is not on the same machine. Numbers from one laptop and a
container are useful for comparing *shapes* — one queue against four, a queue
against the raw commands — and close to worthless as an absolute claim.

## The control

`raw XADD/XREADGROUP` is not a competitor. It is the floor: the same two Redis
commands with no queue on top, no durability, no retries, no liveness, no
dispatch. Every gap between it and a real queue is what those features cost. A
throughput number for any queue means nothing without it.

## Numbers, and the bug that produced the wrong ones

One laptop, Redis 8 in Docker, 5 000 jobs for throughput and 150 for latency,
worker concurrency 10.

| drain                         |  jobs/s |
| ----------------------------- | ------: |
| raw XADD/XREADGROUP, batch 10 |  30 726 |
| smallage, one queue           |   7 886 |
| smallage, four queues         |   7 245 |
| saq                           |   2 873 |

| end-to-end latency    | median |   p95 |   p99 |
| --------------------- | -----: | ----: | ----: |
| raw XADD/XREADGROUP   |  0.5ms | 1.1ms | 1.3ms |
| smallage, one queue   |  0.6ms | 0.7ms | 0.8ms |
| saq                   |  0.9ms | 1.0ms | 1.2ms |
| smallage, four queues |  1.1ms | 2.6ms | 2.9ms |

The four-queue row is the priority sweep, priced. A worker with more than one
queue cannot block on a single read, so it sweeps high to low without blocking
and only blocks when everything is empty; that costs about half a millisecond at
the median and two at p95, and buys ordering between kinds of work.

### How this benchmark lied the first time

The first version of these numbers had this library draining at 100–370 jobs/s,
twenty times slower than the raw control and eight times slower than saq. That
was the harness, not the queue.

The clock was stopped *after* leaving the worker's context manager, so each
subject was also billed for its own shutdown — and a worker parked in a blocking
`XREADGROUP` pays its entire `block_ms` window there. With `block_ms` at the
library default of five seconds, a 400-job drain measured 5.06 s, of which 5.03 s
was one read waiting at the end. The control, blocking for one second, and saq,
which does not block that way, were barely touched by the same bug: the harness
was penalising each subject in proportion to a setting that has nothing to do
with throughput.

It survived a concurrency sweep (flat at 10, 40 and 100 — because the constant
five seconds dominated), a command-count audit (~2 commands per job, correct and
irrelevant), a hypothesis about the two-way slot wait (disproved, and rightly),
and a scalene profile (83% system time — true, and it was the blocking read).
What found it was timing each transport call separately: 82 reads, 5.03 s total,
median 0.25 ms. A median three orders of magnitude below the mean is the whole
story in one line.

The lesson is in the harness now as a comment, because the next person to move
that line will reintroduce it.
