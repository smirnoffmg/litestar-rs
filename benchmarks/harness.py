"""What a queue benchmark can and cannot say, expressed as code.

Two experiments, because throughput and latency answer different questions and a
single number that mixes them answers neither:

``throughput``  fill the stream first, then drain it. The worker is saturated
                throughout, so this measures how fast the loop can move entries
                and nothing about how long anything waited.

``latency``     an idle worker, one job at a time, timed end to end. This is the
                number a request-shaped workload feels, and it is dominated by
                whether the read blocks or sweeps -- which is exactly what the
                queue layout decides.

The handler does nothing on purpose. A benchmark whose handler works measures
the work.
"""

from __future__ import annotations

import statistics
import time
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

import anyio


@dataclass(frozen=True, slots=True, kw_only=True)
class Result:
    name: str
    experiment: str
    jobs: int
    seconds: float
    samples: Sequence[float] = field(default_factory=tuple)
    """Per-job end-to-end seconds, for the latency experiment only."""

    @property
    def per_second(self) -> float:
        return self.jobs / self.seconds if self.seconds else float("nan")

    def percentile(self, p: float) -> float:
        """Nearest-rank, so a reported figure is a measurement and not a blend."""
        if not self.samples:
            return float("nan")
        ordered = sorted(self.samples)
        rank = max(1, min(len(ordered), round(p / 100 * len(ordered))))
        return ordered[rank - 1]


class Adapter:
    """What a queue has to offer to be measured here.

    Deliberately small: anything richer would start measuring the adapter.
    """

    name: str

    @asynccontextmanager
    async def prepared(self) -> AsyncIterator[None]:
        """Open connections, create groups, and tear it all down after."""
        raise NotImplementedError
        yield

    async def enqueue(self, index: int) -> None:
        raise NotImplementedError

    @asynccontextmanager
    async def consuming(self, on_handled: Callable[[], None]) -> AsyncIterator[None]:
        """Run a worker for the duration of the block, calling `on_handled` once
        per job as it finishes."""
        raise NotImplementedError
        yield


class Counter:
    """Counts handled jobs and signals when the target is reached.

    An event rather than a polling loop: a poll interval would quantise every
    measurement to its own step, which for the latency experiment is the
    measurement.
    """

    def __init__(self, target: int) -> None:
        self.target = target
        self.count = 0
        self.reached = anyio.Event()

    def handled(self) -> None:
        self.count += 1
        if self.count >= self.target and not self.reached.is_set():
            self.reached.set()


async def measure_throughput(
    adapter: Adapter, *, jobs: int, timeout_s: float = 120.0
) -> tuple[Result, Result]:
    """Fill, then drain. Reported apart, because they bottleneck on different ends."""
    counter = Counter(jobs)

    async with adapter.prepared():
        started = time.perf_counter()
        for i in range(jobs):
            await adapter.enqueue(i)
        enqueue_s = time.perf_counter() - started

        started = time.perf_counter()
        async with adapter.consuming(counter.handled):
            with anyio.fail_after(timeout_s):
                await counter.reached.wait()
            # Inside the block on purpose. Stopping the clock after it would
            # bill each queue for its own shutdown, and a worker parked in a
            # blocking read pays its whole block window there -- which is a
            # measurement of `block_ms`, not of throughput.
            drain_s = time.perf_counter() - started

    return (
        Result(name=adapter.name, experiment="enqueue", jobs=jobs, seconds=enqueue_s),
        Result(name=adapter.name, experiment="drain", jobs=jobs, seconds=drain_s),
    )


async def measure_latency(
    adapter: Adapter, *, jobs: int, warmup: int = 5, timeout_s: float = 30.0
) -> Result:
    """One job at a time against an idle worker, timed end to end.

    The worker keeps running between jobs: starting one per sample would measure
    startup. The warmup jobs are discarded because the first read of a fresh
    consumer group is not representative of a running one.
    """
    samples: list[float] = []
    arrived = anyio.Event()

    def handled() -> None:
        arrived.set()

    async with adapter.prepared(), adapter.consuming(handled):
        for i in range(warmup + jobs):
            arrived = anyio.Event()
            started = time.perf_counter()
            await adapter.enqueue(i)
            with anyio.fail_after(timeout_s):
                await arrived.wait()
            elapsed = time.perf_counter() - started
            if i >= warmup:
                samples.append(elapsed)

    return Result(
        name=adapter.name,
        experiment="latency",
        jobs=jobs,
        seconds=sum(samples),
        samples=samples,
    )


def render(results: Sequence[Result]) -> str:
    lines: list[str] = []
    rates = [r for r in results if r.experiment in {"enqueue", "drain"}]
    if rates:
        lines.append(f"{'':<22}{'jobs':>8}{'seconds':>10}{'jobs/s':>12}")
        for r in rates:
            label = f"{r.name} {r.experiment}"
            lines.append(
                f"{label:<22}{r.jobs:>8}{r.seconds:>10.3f}{r.per_second:>12,.0f}"
            )
    latencies = [r for r in results if r.experiment == "latency"]
    if latencies:
        if rates:
            lines.append("")
        lines.append(
            f"{'end-to-end latency':<22}{'n':>8}{'median':>10}{'p95':>10}{'p99':>10}"
        )
        for r in latencies:
            lines.append(
                f"{r.name:<22}{r.jobs:>8}"
                f"{statistics.median(r.samples) * 1000:>9.1f}ms"
                f"{r.percentile(95) * 1000:>9.1f}ms"
                f"{r.percentile(99) * 1000:>9.1f}ms"
            )
    return "\n".join(lines)
