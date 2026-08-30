"""Arguments too large to keep in a stream.

Redis holds the whole stream in memory, so a payload measured in megabytes is a
direct route to an OOM kill. Above a threshold the arguments go to a payload
store and the record carries a reference.

    litestar --app examples.large_payloads:app workers run
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

from litestar import Litestar, post

from litestar_rs import FilePayloadStore
from litestar_rs.plugin import QueueConfig, QueuePlugin, TaskRegistry

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

# Every worker must see the same directory -- a network volume, or a single
# host. A path local to one pod means the job runs wherever the file happens to
# be and fails everywhere else.
PAYLOAD_ROOT = Path(os.environ.get("PAYLOAD_ROOT", tempfile.gettempdir())) / "payloads"

tasks = TaskRegistry()


@tasks.task
async def ingest(rows: list[dict[str, str]]) -> None:
    print(f"ingesting {len(rows)} rows")


plugin = QueuePlugin(
    QueueConfig(
        registry=tasks,
        redis_url=REDIS_URL,
        namespace="example-payloads",
        payloads=FilePayloadStore(PAYLOAD_ROOT),
        # Above this the encoded payload goes to the store. Without a store, the
        # transport refuses an oversized record rather than dropping it quietly.
        offload_over_bytes=64 * 1024,
    )
)


@post("/imports/small")
async def small_import() -> str:
    """Stays in the stream: it is well under the threshold."""
    await ingest.enqueue(rows=[{"sku": "A1"}])
    return "queued inline"


@post("/imports/large")
async def large_import() -> str:
    """Goes to the store, and the record carries a reference instead."""
    rows = [{"sku": f"SKU-{n:06d}", "name": "x" * 40} for n in range(2_000)]
    await ingest.enqueue(rows=rows)
    return "queued out of band"


app = Litestar(route_handlers=[small_import, large_import], plugins=[plugin])
