"""Coverage-Badge in README und Doku-Startseite.

Das Badge zeigt die Untergrenze ``fail_under`` aus ``pyproject.toml``, nicht
eine Momentaufnahme: Diese Zahl erzwingt die CI bei jedem Lauf, sie kann
also nicht veralten, ohne dass dieser Test rot wird. ``just ratchet`` hebt
Untergrenze und Badges gemeinsam an.

Testliste:
# [x] README und beide Doku-Startseiten tragen genau ein Coverage-Badge mit dem Wert von fail_under
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
_BADGE = re.compile(r"img\.shields\.io/badge/coverage-%E2%89%A5(\d+)%25-")


@pytest.mark.parametrize("datei", ["README.md", "website/index.md", "website/index.de.md"])
def test_the_coverage_badge_shows_the_enforced_floor(datei: str) -> None:
    pyproject = tomllib.loads((_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    floor = pyproject["tool"]["coverage"]["report"]["fail_under"]

    werte = _BADGE.findall((_ROOT / datei).read_text(encoding="utf-8"))

    assert werte == [str(floor)]
