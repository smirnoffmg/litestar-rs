"""The documentation is checked against the API it documents.

A guide that names something the library does not export is a confident, wrong
answer to somebody's first question, and nothing else would catch it.
"""

import ast
import asyncio
import importlib
import re
import sys
import textwrap
import types
import warnings
from pathlib import Path

import anyio
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


def test_the_documentation_url_agrees_everywhere() -> None:
    """It ends up in package metadata, which cannot be corrected after a release."""
    import tomllib

    with (ROOT / "pyproject.toml").open("rb") as handle:
        documented = tomllib.load(handle)["project"]["urls"]["Documentation"]

    site_url = next(
        line.split(":", 1)[1].strip()
        for line in (ROOT / "mkdocs.yml").read_text().splitlines()
        if line.startswith("site_url:")
    )

    assert documented.rstrip("/") == site_url.rstrip("/")
    assert documented.rstrip("/") in (ROOT / "README.md").read_text()


PYTHON_BLOCK = re.compile(
    r"(?:^<!-- docs-test: skip -- (?P<reason>[^>]+?) -->\n)?"
    r"^```python\n(?P<code>.*?)^```",
    re.M | re.S,
)
"""A block may be marked unrunnable, in the documentation, with the reason.

In a comment rather than a list in this file: whoever edits the page sees why it
is exempt, and a marker cannot silently start matching a different block.
"""

APPLICATION_STUBS = '''
class AsyncSession: ...
class Settings: ...
async def session() -> AsyncSession: return AsyncSession()
def settings() -> Settings: return Settings()  # noqa: E704


class _Whatever:
    """Stands in for an object the reader already has: a Redis client, a store."""

    def __getattr__(self, name: str) -> "_Whatever": return self
    def __call__(self, *args: object, **kwargs: object) -> "_Whatever": return self
    def __await__(self): yield; return self


client = store = scheduler = plugin = transport = config = _Whatever()
doc_id = invoice_id = job_id = "job-1"
'''


def documented_blocks() -> list[tuple[Path, int, str, str | None]]:
    found = []
    for page in PAGES:
        text = page.read_text()
        for match in PYTHON_BLOCK.finditer(text):
            line = text[: match.start()].count("\n") + 1
            found.append((page, line, match.group("code"), match.group("reason")))
    return found


def test_every_documented_snippet_parses() -> None:
    for page, line, code, _ in documented_blocks():
        try:
            ast.parse(code)
        except SyntaxError as exc:
            pytest.fail(f"{page.name}:{line} does not parse: {exc}")


@pytest.mark.parametrize("page", PAGES, ids=lambda p: p.name)
def test_a_page_runs_top_to_bottom(page: Path) -> None:
    """A reader works through a page in order, so the snippets must too.

    Only the reader's own objects are supplied. Anything this library provides
    has to be imported by the documentation itself, which is what stops a guide
    from quietly referring to something it never showed.
    """
    blocks = [
        (line, code, reason)
        for p, line, code, reason in documented_blocks()
        if p == page
    ]
    if not blocks:
        pytest.skip("no python in this page")

    module = types.ModuleType(f"docs_{page.stem}")
    sys.modules[module.__name__] = module
    exec(compile(APPLICATION_STUBS, "stubs", "exec"), module.__dict__)  # noqa: S102
    try:
        for line, code, reason in blocks:
            if reason is not None:
                continue
            source = code
            tree = ast.parse(code)
            if _has_top_level_await(tree):
                source = "async def __block():\n" + textwrap.indent(code, "    ")
            try:
                with warnings.catch_warnings():
                    # Documentation must not teach an API its own framework has
                    # deprecated; a reader copies what is in front of them.
                    warnings.simplefilter("error", DeprecationWarning)
                    code_object = compile(source, f"{page.name}:{line}", "exec")
                    exec(code_object, module.__dict__)  # noqa: S102
                if source is not code:
                    anyio.from_thread  # noqa: B018 - keep anyio imported
                    asyncio.run(module.__dict__["__block"]())
            except Exception as exc:
                pytest.fail(
                    f"{page.name}:{line} failed to run: {type(exc).__name__}: {exc}"
                )
    finally:
        del sys.modules[module.__name__]


def _has_top_level_await(tree: ast.Module) -> bool:
    for node in tree.body:
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
            continue
        if any(isinstance(sub, ast.Await) for sub in ast.walk(node)):
            return True
    return False
