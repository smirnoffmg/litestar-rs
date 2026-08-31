"""Run the benchmarks.

    python -m benchmarks                      # starts a Redis container
    REDIS_URL=redis://... python -m benchmarks  # uses one you already have

Numbers from this are about one machine and one Redis. They are useful for
comparing shapes -- one queue against four, a queue against the raw commands --
and close to worthless as an absolute claim about anyone's production.
"""

from __future__ import annotations

import argparse
import os
import platform
import sys
from collections.abc import Iterator
from contextlib import contextmanager

import anyio

from benchmarks.adapters import RawRedisStream, Saq, Smallage
from benchmarks.harness import Result, measure_latency, measure_throughput, render


@contextmanager
def redis_url() -> Iterator[str]:
    given = os.environ.get("REDIS_URL")
    if given:
        yield given
        return
    from testcontainers.community.redis import RedisContainer

    image = os.environ.get("REDIS_IMAGE", "redis:8-alpine")
    with RedisContainer(image) as container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(container.port)
        yield f"redis://{host}:{port}/0"


async def main(url: str, *, jobs: int, latency_jobs: int, concurrency: int) -> int:
    results: list[Result] = []

    subjects = [
        RawRedisStream(url),
        Saq(url, concurrency=concurrency),
        Smallage(url, concurrency=concurrency, label="smallage 1 queue"),
        Smallage(
            url,
            concurrency=concurrency,
            queues=("high", "default", "low", "bulk"),
            label="smallage 4 queues",
        ),
    ]

    for subject in subjects:
        enqueue, drain = await measure_throughput(subject, jobs=jobs)
        results += [enqueue, drain]

    for subject in subjects:
        results.append(await measure_latency(subject, jobs=latency_jobs))

    print(f"python {platform.python_version()} on {platform.platform()}")
    print(f"redis  {url.rsplit('@', 1)[-1]}")
    print(f"jobs   {jobs} for throughput, {latency_jobs} for latency")
    print(f"worker concurrency {concurrency}\n")
    print(render(results))
    return 0


def cli() -> int:
    parser = argparse.ArgumentParser(prog="benchmarks")
    parser.add_argument("--jobs", type=int, default=5_000)
    parser.add_argument("--latency-jobs", type=int, default=200)
    parser.add_argument("--concurrency", type=int, default=10)
    args = parser.parse_args()

    with redis_url() as url:
        return anyio.run(
            lambda: main(
                url,
                jobs=args.jobs,
                latency_jobs=args.latency_jobs,
                concurrency=args.concurrency,
            ),
            backend="asyncio",
            backend_options={"use_uvloop": True},
        )


if __name__ == "__main__":
    sys.exit(cli())
