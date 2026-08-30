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


def test_version() -> None:
    assert litestar_rs.__version__ == "0.1.0"
