"""The payload store that ships, and what it refuses."""

from pathlib import Path

import anyio
import pytest

from smallage.core.errors import ConfigurationError
from smallage.core.payloads import FilePayloadStore, PayloadMissing

pytestmark = pytest.mark.unit


async def test_a_payload_survives_the_round_trip(tmp_path: Path) -> None:
    store = FilePayloadStore(tmp_path)
    blob = b"\x00\xff not text at all" * 1000

    reference = await store.put("job-1", blob)

    assert reference.startswith("file://")
    assert await store.get(reference) == blob


async def test_a_partial_write_is_never_visible(tmp_path: Path) -> None:
    """Written beside and renamed, so a reader cannot see half a payload."""
    store = FilePayloadStore(tmp_path)

    await store.put("job-1", b"complete")

    written = sorted([entry.suffix async for entry in anyio.Path(tmp_path).iterdir()])
    assert written == [".payload"]


async def test_a_missing_payload_says_which_reference(tmp_path: Path) -> None:
    """A wrong volume mount looks exactly like this, and should read that way."""
    store = FilePayloadStore(tmp_path)

    with pytest.raises(PayloadMissing, match="file://"):
        await store.get(f"file://{tmp_path}/never-written.payload")


async def test_discarding_is_idempotent(tmp_path: Path) -> None:
    store = FilePayloadStore(tmp_path)
    reference = await store.put("job-1", b"done")

    await store.discard(reference)
    await store.discard(reference)

    assert [entry async for entry in anyio.Path(tmp_path).iterdir()] == []


def test_a_relative_root_is_refused(tmp_path: Path) -> None:
    """It resolves against each process's working directory, which varies."""
    with pytest.raises(ConfigurationError, match="absolute"):
        FilePayloadStore("payloads")


@pytest.mark.parametrize("job_id", ["", ".", "..", "a/b"])
async def test_a_job_id_that_is_not_a_file_name_is_refused(
    tmp_path: Path, job_id: str
) -> None:
    with pytest.raises(ConfigurationError, match="file name"):
        await FilePayloadStore(tmp_path).put(job_id, b"x")


async def test_it_satisfies_the_payload_store_protocol(tmp_path: Path) -> None:
    from smallage.core.protocols import PayloadStore

    store: PayloadStore = FilePayloadStore(tmp_path)

    assert await store.get(await store.put("job-1", b"x")) == b"x"
