"""Shared pytest configuration.

Every test must carry either the ``unit`` or the ``integration`` marker: the
pre-commit hook selects on them, and an unmarked test would silently pass there.
"""

from collections.abc import Sequence

import pytest


@pytest.fixture
def anyio_backend() -> tuple[str, dict[str, bool]]:
    """Pin the runtime the worker actually uses; uvloop is asyncio-only."""
    return ("asyncio", {"use_uvloop": True})


def pytest_collection_modifyitems(
    config: pytest.Config, items: Sequence[pytest.Item]
) -> None:
    unmarked = [
        item.nodeid
        for item in items
        if not ({"unit", "integration"} & {m.name for m in item.iter_markers()})
    ]
    if unmarked:
        listed = "\n  ".join(unmarked)
        raise pytest.UsageError(
            f"tests must be marked 'unit' or 'integration':\n  {listed}"
        )
