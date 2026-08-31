"""Script packaging and the raw reply shape EVALSHA hands back."""

import pytest

from smallage.core.envelope import Record
from smallage.core.scripts import SCRIPT_NAMES, load_script, parse_xclaim_reply

pytestmark = pytest.mark.unit


@pytest.mark.parametrize("name", SCRIPT_NAMES)
def test_scripts_are_packaged(name: str) -> None:
    """Guards the wheel: a missing .lua must fail in CI, not in production."""
    assert load_script(name).strip()


def test_parse_xclaim_reply() -> None:
    """XCLAIM run through EVALSHA bypasses redis-py's response callback."""
    reply = [
        [b"1-0", [b"task", b"reindex", b"payload", b"{}"]],
        [b"2-0", [b"task", b"purge", b"payload", b"[]"]],
    ]
    assert parse_xclaim_reply("{lrs}:q:default:0", reply) == [
        Record(
            stream="{lrs}:q:default:0",
            entry_id=b"1-0",
            fields={b"task": b"reindex", b"payload": b"{}"},
        ),
        Record(
            stream="{lrs}:q:default:0",
            entry_id=b"2-0",
            fields={b"task": b"purge", b"payload": b"[]"},
        ),
    ]


def test_parse_xclaim_reply_empty() -> None:
    assert parse_xclaim_reply("s", []) == []


def test_parse_xclaim_reply_skips_vanished_entries() -> None:
    """Older Redis returns nil for an entry deleted between XPENDING and XCLAIM."""
    reply = [None, [b"2-0", [b"task", b"purge"]]]
    records = parse_xclaim_reply("s", reply)
    assert [record.entry_id for record in records] == [b"2-0"]
