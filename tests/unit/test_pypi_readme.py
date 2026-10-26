"""PyPI-Fassung der README: ohne Logo, ohne Links ins Repo.

Das Repo ist privat, PyPI lädt relative Bilder und Links aus dem Repo nicht.
Die PyPI-Fassung baut ``hatch-fancy-pypi-readme`` beim Paketbau aus der
README (``[tool.hatch.metadata.hooks.fancy-pypi-readme]`` in
``pyproject.toml``); der Test rendert sie mit demselben Hook.

Testliste:
# [x] keine relativen Bilder oder Links (Markdown und HTML), keine Sprungmarken
# [x] Titel „SwarmQ“ statt Logo, Inhalt (Quick start, Vergleich) bleibt
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

import pytest
from hatch_fancy_pypi_readme.hooks import FancyReadmeMetadataHook

_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def pypi_readme() -> str:
    pyproject = tomllib.loads((_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    config = pyproject["tool"]["hatch"]["metadata"]["hooks"]["fancy-pypi-readme"]
    metadata: dict[str, object] = {}
    FancyReadmeMetadataHook(str(_ROOT), config).update(metadata)
    readme = metadata["readme"]
    assert isinstance(readme, dict)
    assert readme["content-type"] == "text/markdown"
    text = readme["text"]
    assert isinstance(text, str)
    return text


def test_the_pypi_readme_has_no_repo_relative_targets(pypi_readme: str) -> None:
    markdown_targets = re.findall(r"\]\(([^)]*)\)", pypi_readme)
    html_targets = re.findall(r"""(?:href|src|srcset)="([^"]*)\"""", pypi_readme)

    assert [t for t in markdown_targets + html_targets if not t.startswith("https://")] == []


def test_the_pypi_readme_keeps_the_content_under_a_plain_title(pypi_readme: str) -> None:
    assert pypi_readme.startswith("# SwarmQ\n")
    assert "<picture>" not in pypi_readme
    assert "The async-first Python task queue that does not lose your work." in pypi_readme
    assert "## Quick start" in pypi_readme
    assert "## How it compares" in pypi_readme
