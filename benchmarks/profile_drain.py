"""Drain a fixed number of jobs, for a profiler to look at.

    docker run -d --name bench-redis -p 63799:6379 redis:8-alpine
    REDIS_URL=redis://127.0.0.1:63799/0 \
      uv run --group bench scalene run --cpu-only -o /tmp/scalene.json profile_drain.py

Redis runs outside the profiled process on purpose: starting a container inside
it puts the container's startup in the profile.
"""

import os

import anyio

from benchmarks.adapters import Smallage
from benchmarks.harness import measure_throughput

URL = os.environ["REDIS_URL"]


async def main() -> None:
    _, drain = await measure_throughput(
        Smallage(URL, concurrency=10), jobs=400, timeout_s=300
    )
    print(f"drain {drain.per_second:,.0f} jobs/s")


anyio.run(main, backend="asyncio", backend_options={"use_uvloop": True})
