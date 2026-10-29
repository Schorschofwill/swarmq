"""PyPI-Fassung der README: Logo und Links als absolute Adressen.

PyPI lädt relative Bilder und Links nicht. Die PyPI-Fassung baut
``hatch-fancy-pypi-readme`` beim Paketbau aus der README
(``[tool.hatch.metadata.hooks.fancy-pypi-readme]`` in ``pyproject.toml``);
der Test rendert sie mit demselben Hook. Seit das Repo öffentlich ist,
zeigen Doku-Links auf Read the Docs und Repo-Links auf GitHub.

Testliste:
# [x] keine relativen Bilder oder Links (Markdown und HTML), keine Sprungmarken
# [x] Logo als PNG aus dem Repo statt Titelzeile, Inhalt (Quick start, Vergleich, Dokumentation) bleibt
# [x] Doku-Links zeigen auf Read the Docs, und zu jedem gibt es die Seite in website/
# [x] Repo-Links zeigen auf main im GitHub-Repo, und die Datei gibt es
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

import pytest
from hatch_fancy_pypi_readme.hooks import FancyReadmeMetadataHook

_ROOT = Path(__file__).resolve().parents[2]
_DOCS = "https://swarmq.readthedocs.io/en/latest/"
_BLOB = "https://github.com/Schorschofwill/swarmq/blob/main/"
_RAW = "https://raw.githubusercontent.com/Schorschofwill/swarmq/main/"


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


def _targets(text: str) -> list[str]:
    return re.findall(r"\]\(([^)]*)\)", text) + re.findall(r"""(?:href|src|srcset)="([^"]*)\"""", text)


def test_the_pypi_readme_has_no_repo_relative_targets(pypi_readme: str) -> None:
    assert [t for t in _targets(pypi_readme) if not t.startswith("https://")] == []


def test_the_pypi_readme_starts_with_the_logo_and_keeps_the_content(pypi_readme: str) -> None:
    logo = f'<img src="{_RAW}docs/assets/logo/png/swarmq-horizontal-light-256h.png" alt="SwarmQ" width="420">'
    assert pypi_readme.startswith(f'<p align="center">\n  {logo}\n</p>\n')
    assert (_ROOT / "docs/assets/logo/png/swarmq-horizontal-light-256h.png").is_file()
    assert "<picture>" not in pypi_readme
    assert "The async-first Python task queue that does not lose your work." in pypi_readme
    assert "## Quick start" in pypi_readme
    assert "## How it compares" in pypi_readme
    assert "## Documentation" in pypi_readme


def test_documentation_links_point_to_existing_pages_on_read_the_docs(pypi_readme: str) -> None:
    pages = [t.removeprefix(_DOCS) for t in _targets(pypi_readme) if t.startswith(_DOCS)]

    assert "getting-started/" in pages
    for page in pages:
        stem = page.split("#")[0].rstrip("/")
        assert (_ROOT / "website" / f"{stem}.md").is_file() or (_ROOT / "website" / stem / "index.md").is_file(), page


def test_repository_links_point_to_existing_files_on_main(pypi_readme: str) -> None:
    files = [t.removeprefix(_BLOB) for t in _targets(pypi_readme) if t.startswith(_BLOB)]

    assert "LICENSE" in files
    for path in files:
        assert (_ROOT / path).exists(), path
