"""Redis key schema.

Every key of a namespace carries the same literal ``{ns}`` hash tag, so they all
land in one Cluster slot: multi-key ``XREADGROUP`` and the Lua scripts require it.
The schema cannot be changed after the first release without a data migration.
"""

import zlib

from litestar_rs.core.errors import ConfigurationError

_FORBIDDEN = ("{", "}", ":")


def _validate(name: str, value: str) -> str:
    if not value:
        raise ConfigurationError(f"{name} must not be empty")
    for char in _FORBIDDEN:
        if char in value:
            raise ConfigurationError(f"{name} must not contain {char!r}, got {value!r}")
    return value


def validate_namespace(namespace: str) -> str:
    return _validate("namespace", namespace)


def validate_queue(queue: str) -> str:
    return _validate("queue", queue)


def stream_key(namespace: str, queue: str, shard: int) -> str:
    return f"{{{namespace}}}:q:{queue}:{shard}"


def stream_keys(namespace: str, queue: str, shards: int) -> list[str]:
    if shards < 1:
        raise ConfigurationError(f"shards must be at least 1, got {shards}")
    return [stream_key(namespace, queue, shard) for shard in range(shards)]


def stream_for(namespace: str, queue: str, shards: int, routing_key: str) -> str:
    """Pick a shard deterministically, so one routing key always lands in one stream."""
    streams = stream_keys(namespace, queue, shards)
    return streams[zlib.crc32(routing_key.encode()) % len(streams)]


def alive_key(namespace: str, entry_id: bytes | str) -> str:
    if isinstance(entry_id, bytes):
        entry_id = entry_id.decode("ascii")
    return f"{{{namespace}}}:alive:{entry_id}"


def dedup_key(namespace: str, key: str) -> str:
    return f"{{{namespace}}}:dedup:{key}"


def sched_key(namespace: str) -> str:
    return f"{{{namespace}}}:sched"


def sched_job_key(namespace: str, scheduled_id: str) -> str:
    return f"{{{namespace}}}:sched:job:{scheduled_id}"


def leader_key(namespace: str) -> str:
    return f"{{{namespace}}}:leader"


def result_key(namespace: str, job_id: str) -> str:
    return f"{{{namespace}}}:result:{job_id}"


def result_wait_key(namespace: str, job_id: str) -> str:
    return f"{{{namespace}}}:result:wait:{job_id}"


def dlq_key(namespace: str) -> str:
    return f"{{{namespace}}}:dlq"
