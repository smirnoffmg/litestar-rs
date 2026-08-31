"""Somewhere for arguments too large to keep in a stream.

Redis holds the whole stream in memory, so a payload measured in megabytes is a
direct route to an OOM kill. Above a threshold the arguments go here and the
record carries a reference instead.
"""

from __future__ import annotations

from pathlib import Path

import anyio

from smallage.core.errors import ConfigurationError, SmallageError

SCHEME = "file://"


class PayloadMissing(SmallageError):
    """A record references a payload the store no longer has."""


class FilePayloadStore:
    """Payloads as files under one directory.

    Every worker must see the same directory -- a network volume, or a single
    host. A path local to one pod means the job runs wherever the file happens to
    be and fails everywhere else, so the reference records the path and reading
    it back from the wrong place says exactly that.
    """

    def __init__(self, root: Path | str) -> None:
        self.root = Path(root)
        if not self.root.is_absolute():
            # A relative path resolves against each process's working directory,
            # which is rarely the same one twice.
            raise ConfigurationError(f"root must be absolute, got {self.root}")
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, job_id: str) -> Path:
        if "/" in job_id or job_id in {"", ".", ".."}:
            raise ConfigurationError(f"job id is not a usable file name: {job_id!r}")
        return self.root / f"{job_id}.payload"

    async def put(self, job_id: str, data: bytes) -> str:
        path = self._path(job_id)
        # Write beside it and rename: a reader must never see half a payload,
        # and rename within a directory is atomic.
        staging = path.with_suffix(".partial")
        await anyio.Path(staging).write_bytes(data)
        await anyio.Path(staging).rename(path)
        return f"{SCHEME}{path}"

    async def get(self, reference: str) -> bytes:
        path = Path(reference.removeprefix(SCHEME))
        try:
            return await anyio.Path(path).read_bytes()
        except FileNotFoundError as exc:
            raise PayloadMissing(
                f"no payload at {reference}; the store is not the one that wrote "
                "it, or it has been cleaned up"
            ) from exc

    async def discard(self, reference: str) -> None:
        """Remove a payload once its job is finished with it."""
        await anyio.Path(Path(reference.removeprefix(SCHEME))).unlink(missing_ok=True)
