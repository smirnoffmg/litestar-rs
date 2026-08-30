"""The public API is only what __init__ exports, so pin it."""

import pytest

import litestar_rs
from litestar_rs import core

pytestmark = pytest.mark.unit


def test_every_exported_name_resolves() -> None:
    for name in litestar_rs.__all__:
        assert getattr(litestar_rs, name) is not None


def test_top_level_is_a_subset_of_the_core_surface() -> None:
    exported = set(litestar_rs.__all__) - {"__version__"}
    assert exported <= set(core.__all__)


def test_version_matches_the_one_place_it_is_declared() -> None:
    """A version written twice is a version that will disagree with itself."""
    import tomllib
    from pathlib import Path

    pyproject = Path(__file__).resolve().parents[3] / "pyproject.toml"
    with pyproject.open("rb") as handle:
        declared = tomllib.load(handle)["project"]["version"]

    assert litestar_rs.__version__ == declared
