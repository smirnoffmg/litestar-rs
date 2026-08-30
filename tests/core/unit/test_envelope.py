"""The stream field layout is as un-migratable as the key schema; pinned here."""

import pytest

from litestar_rs.core.envelope import (
    ENVELOPE_VERSION,
    Envelope,
    JsonCodec,
    from_fields,
    to_fields,
)
from litestar_rs.core.errors import MalformedEnvelope

pytestmark = pytest.mark.unit


def make(**overrides: object) -> Envelope:
    base: dict[str, object] = {
        "id": "3f2b",
        "task": "reindex",
        "payload": b'{"doc_id":1}',
        "enqueued_at": 1712345678901,
    }
    return Envelope(**(base | overrides))  # type: ignore[arg-type]  # test factory


def test_round_trip() -> None:
    envelope = make(attempt=2, traceparent="00-abc-def-01", tracestate="a=1")
    assert from_fields(to_fields(envelope)) == envelope


def test_version_is_stamped() -> None:
    assert to_fields(make())[b"v"] == ENVELOPE_VERSION


def test_payload_bytes_pass_through_untouched() -> None:
    payload = b"\x00\xff not json at all"
    assert to_fields(make(payload=payload))[b"payload"] == payload


def test_traceparent_is_a_sibling_of_payload_not_nested_in_it() -> None:
    fields = to_fields(make(traceparent="00-abc-def-01"))
    assert fields[b"traceparent"] == b"00-abc-def-01"
    assert b"traceparent" not in fields[b"payload"]


@pytest.mark.parametrize("name", ["payload_ref", "traceparent", "tracestate"])
def test_optional_fields_are_omitted_when_unset(name: str) -> None:
    """Redis has no null, so an unset field must not be written at all."""
    assert name.encode() not in to_fields(make())


def test_numbers_are_decimal_ascii() -> None:
    fields = to_fields(make(attempt=7))
    assert fields[b"attempt"] == b"7"
    assert fields[b"enqueued_at"] == b"1712345678901"


@pytest.mark.parametrize(
    "missing", [b"v", b"id", b"task", b"payload", b"enqueued_at", b"attempt"]
)
def test_missing_required_field_is_rejected(missing: bytes) -> None:
    fields = to_fields(make())
    del fields[missing]
    with pytest.raises(MalformedEnvelope, match=missing.decode()):
        from_fields(fields)


def test_unknown_version_is_rejected() -> None:
    fields = to_fields(make())
    fields[b"v"] = b"99"
    with pytest.raises(MalformedEnvelope, match="version"):
        from_fields(fields)


def test_unknown_fields_are_tolerated() -> None:
    """A newer producer may add fields; an older worker must still run the task."""
    fields = to_fields(make())
    fields[b"future_field"] = b"whatever"
    assert from_fields(fields).task == "reindex"


def test_json_codec_round_trip() -> None:
    codec = JsonCodec()
    assert codec.decode(codec.encode({"doc_id": 1})) == {"doc_id": 1}


def test_history_round_trips_and_is_omitted_when_empty() -> None:
    assert b"history" not in to_fields(make())
    with_history = make(history=("0: RuntimeError: boom", "1: ValueError: nope"))
    assert from_fields(to_fields(with_history)).history == with_history.history
