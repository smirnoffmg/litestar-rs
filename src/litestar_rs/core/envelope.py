"""The stream entry layout.

A stream entry is a flat hash. ``traceparent`` sits beside ``payload`` rather than
inside it, so restoring the span context never requires decoding the payload — and
broker mode, which reads foreign payloads, can ignore the payload entirely.

Adding a field after the first release means parsing two record formats at once,
so every field a later milestone needs is written from the start.
"""

from collections.abc import Mapping

import msgspec

from litestar_rs.core.errors import MalformedEnvelope

ENVELOPE_VERSION = b"1"

_REQUIRED = (b"v", b"id", b"task", b"payload", b"enqueued_at", b"attempt")


class Envelope(msgspec.Struct, frozen=True, kw_only=True):
    id: str
    task: str
    payload: bytes
    enqueued_at: int
    attempt: int = 0
    payload_ref: str | None = None
    traceparent: str | None = None
    tracestate: str | None = None
    history: tuple[str, ...] = ()
    """One compact line per failed attempt, oldest first.

    Bounded by the retry policy, and each line is truncated, so a job that keeps
    failing cannot grow its own entry without limit.
    """


class JsonCodec:
    """Default payload codec. The plugin layer substitutes the app's msgspec pair."""

    def encode(self, value: object, /) -> bytes:
        return msgspec.json.encode(value)

    def decode(self, raw: bytes, /) -> object:
        return msgspec.json.decode(raw)


def to_fields(envelope: Envelope) -> dict[bytes, bytes]:
    fields = {
        b"v": ENVELOPE_VERSION,
        b"id": envelope.id.encode(),
        b"task": envelope.task.encode(),
        b"payload": envelope.payload,
        b"enqueued_at": str(envelope.enqueued_at).encode(),
        b"attempt": str(envelope.attempt).encode(),
    }
    optional = {
        b"payload_ref": envelope.payload_ref,
        b"traceparent": envelope.traceparent,
        b"tracestate": envelope.tracestate,
    }
    # Redis has no null: an unset field is written as no field at all.
    fields.update({k: v.encode() for k, v in optional.items() if v is not None})
    if envelope.history:
        fields[b"history"] = "\n".join(envelope.history).encode()
    return fields


def from_fields(fields: Mapping[bytes, bytes]) -> Envelope:
    for name in _REQUIRED:
        if name not in fields:
            raise MalformedEnvelope(f"stream entry is missing field {name.decode()!r}")
    if fields[b"v"] != ENVELOPE_VERSION:
        raise MalformedEnvelope(
            f"unsupported envelope version {fields[b'v']!r}, "
            f"expected {ENVELOPE_VERSION!r}"
        )
    optional = {
        name: fields[key].decode()
        for name, key in (
            ("payload_ref", b"payload_ref"),
            ("traceparent", b"traceparent"),
            ("tracestate", b"tracestate"),
        )
        if key in fields
    }
    # Unknown fields are ignored on purpose: a newer producer must not break an
    # older worker during a rolling deploy.
    raw_history = fields.get(b"history")
    return Envelope(
        history=tuple(raw_history.decode().split("\n")) if raw_history else (),
        id=fields[b"id"].decode(),
        task=fields[b"task"].decode(),
        payload=fields[b"payload"],
        enqueued_at=int(fields[b"enqueued_at"]),
        attempt=int(fields[b"attempt"]),
        **optional,
    )


class Record(msgspec.Struct, frozen=True):
    """A stream entry as Redis returns it.

    Broker mode consumes foreign payloads and never builds an ``Envelope``, so the
    transport hands back raw records and decoding is a separate step in the worker.
    """

    stream: str
    entry_id: bytes
    fields: dict[bytes, bytes]


class Pending(msgspec.Struct, frozen=True):
    """One unacknowledged entry, as ``XPENDING`` describes it."""

    stream: str
    entry_id: bytes
    consumer: str
    times_delivered: int
