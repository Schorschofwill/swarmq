"""Release-Notizen für GitHub Releases aus dem Changelog.

``scripts/release_notes.py`` schneidet den Abschnitt einer Version aus
``website/reference/changelog.md``; ``release.yml`` legt damit das GitHub
Release zu einem Tag an.

Testliste:
# [x] der Abschnitt einer Version, ohne ihre Überschrift, bis zur nächsten Version
# [x] ein Release Candidate ist eine eigene Version, nicht der Abschnitt der finalen
# [x] eine Version ohne Abschnitt: Abbruch mit Meldung
# [x] relative Doku-Links und Sprungmarken zeigen auf Read the Docs (das Release liegt auf GitHub)
# [x] jede Version aus pyproject.toml hat einen Abschnitt im echten Changelog
"""

from __future__ import annotations

import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

pytestmark = pytest.mark.timeout(10)

_ROOT = Path(__file__).resolve().parents[2]
_SCRIPT = _ROOT / "scripts" / "release_notes.py"

_CHANGELOG = """# Changelog

## [Unreleased]

- offen

## [1.1.0] — 2026-10-02

### Fixed

- **B.** zwei

## [1.1.0rc1] — 2026-10-01

- **A.** eins

## [1.0.0] — 2026-09-01

- alt

## [0.9.0] — 2026-08-01

- [API](api.md), [CLI](cli.md#exit-codes), [Agent](agent/index.md),
  [Leitfaden](../guides/cpu-mode.md#sizing), [früher](#100-2026-09-01),
  [extern](https://example.org/x.md)
"""


def _notes(tmp_path: Path, version: str) -> subprocess.CompletedProcess[str]:
    changelog = tmp_path / "changelog.md"
    changelog.write_text(_CHANGELOG, encoding="utf-8")
    return subprocess.run(
        [sys.executable, str(_SCRIPT), version, str(changelog)], capture_output=True, text=True, check=False
    )


def test_the_section_of_a_version_without_its_heading(tmp_path: Path) -> None:
    result = _notes(tmp_path, "1.1.0")

    assert result.returncode == 0, result.stderr
    assert result.stdout == "### Fixed\n\n- **B.** zwei\n"


def test_a_release_candidate_is_its_own_version(tmp_path: Path) -> None:
    result = _notes(tmp_path, "1.1.0rc1")

    assert result.returncode == 0, result.stderr
    assert result.stdout == "- **A.** eins\n"


def test_a_version_without_a_section_is_an_error(tmp_path: Path) -> None:
    result = _notes(tmp_path, "2.0.0")

    assert result.returncode == 1
    assert "2.0.0" in result.stderr


def test_the_current_version_has_a_section_in_the_real_changelog() -> None:
    version = tomllib.loads((_ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]["version"]

    result = subprocess.run(
        [sys.executable, str(_SCRIPT), version, str(_ROOT / "website/reference/changelog.md")],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip()


def test_relative_links_point_to_read_the_docs(tmp_path: Path) -> None:
    result = _notes(tmp_path, "0.9.0")

    docs = "https://swarmq.readthedocs.io/en/latest/"
    assert result.returncode == 0, result.stderr
    assert result.stdout == (
        f"- [API]({docs}reference/api/), [CLI]({docs}reference/cli/#exit-codes), "
        f"[Agent]({docs}reference/agent/),\n"
        f"  [Leitfaden]({docs}guides/cpu-mode/#sizing), [früher]({docs}reference/changelog/#100-2026-09-01),\n"
        "  [extern](https://example.org/x.md)\n"
    )
