"""The documentation is checked against the API it documents.

A guide that names something the library does not export is a confident, wrong
answer to somebody's first question, and nothing else would catch it.
"""

import importlib
import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parent.parent
PAGES = [*sorted((ROOT / "docs").glob("*.md")), ROOT / "README.md"]
IMPORT = re.compile(r"^\s*from (litestar_rs[\w.]*) import (.+)$")


def documented_imports() -> list[tuple[str, str, str]]:
    found = []
    for page in PAGES:
        for line in page.read_text().splitlines():
            match = IMPORT.match(line)
            if match is None:
                continue
            module, names = match.group(1), match.group(2)
            for name in names.split("#")[0].split(","):
                if name.strip():
                    found.append((page.name, module, name.strip()))
    return found


@pytest.mark.parametrize(
    ("page", "module", "name"),
    documented_imports(),
    ids=lambda value: str(value),
)
def test_a_documented_import_resolves(page: str, module: str, name: str) -> None:
    assert hasattr(importlib.import_module(module), name), (
        f"{page} imports {name!r} from {module}, which does not export it"
    )


def test_the_pages_the_navigation_promises_exist() -> None:
    nav = (ROOT / "mkdocs.yml").read_text()
    linked = set(re.findall(r": (\w+\.md)$", nav, re.M))
    present = {page.name for page in (ROOT / "docs").glob("*.md")}

    assert linked == present, "a page is either unlinked or missing"
