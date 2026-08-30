"""Lua scripts, kept as real files so their atomic sections stay reviewable."""

from dataclasses import dataclass
from importlib.resources import files
from typing import Any

from redis.asyncio import Redis
from redis.commands.core import AsyncScript

from litestar_rs.core.envelope import Record

SCRIPT_NAMES = ("ack", "reclaim")


def load_script(name: str) -> str:
    return files(__package__).joinpath(f"{name}.lua").read_text(encoding="utf-8")


@dataclass(frozen=True, slots=True)
class Scripts:
    ack: AsyncScript
    reclaim: AsyncScript


def register(client: Redis) -> Scripts:
    return Scripts(
        ack=client.register_script(load_script("ack")),
        reclaim=client.register_script(load_script("reclaim")),
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
