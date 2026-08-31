"""Lua scripts, kept as real files so their atomic sections stay reviewable."""

from dataclasses import dataclass
from importlib.resources import files
from typing import Any

from redis.asyncio import Redis
from redis.commands.core import AsyncScript

from smallage.core.envelope import Record

SCRIPT_NAMES = (
    "ack",
    "reclaim",
    "sweep_consumers",
    "promote",
    "renew_leader",
    "release_leader",
)


def load_script(name: str) -> str:
    return files(__package__).joinpath(f"{name}.lua").read_text(encoding="utf-8")


@dataclass(frozen=True, slots=True)
class TransportScripts:
    ack: AsyncScript
    reclaim: AsyncScript
    sweep_consumers: AsyncScript


@dataclass(frozen=True, slots=True)
class SchedulerScripts:
    promote: AsyncScript
    renew_leader: AsyncScript
    release_leader: AsyncScript


def register_transport(client: Redis) -> TransportScripts:
    return TransportScripts(
        ack=client.register_script(load_script("ack")),
        reclaim=client.register_script(load_script("reclaim")),
        sweep_consumers=client.register_script(load_script("sweep_consumers")),
    )


def register_scheduler(client: Redis) -> SchedulerScripts:
    return SchedulerScripts(
        promote=client.register_script(load_script("promote")),
        renew_leader=client.register_script(load_script("renew_leader")),
        release_leader=client.register_script(load_script("release_leader")),
    )


def parse_xclaim_reply(stream: str, reply: list[Any]) -> list[Record]:
    """Turn the raw EVALSHA reply into records.

    Going through EVALSHA bypasses redis-py's XCLAIM response callback, so the
    reply arrives as nested lists of bytes rather than parsed entries.
    """
    records = []
    for entry in reply:
        if entry is None:
            continue
        entry_id, flat = entry
        fields = dict(zip(flat[::2], flat[1::2], strict=True))
        records.append(Record(stream=stream, entry_id=entry_id, fields=fields))
    return records
