"""Lieferkette: gepinnte Actions, Dependabot, Nachweise im Release.

Ein Tag wie ``@v4`` lässt sich auf der Gegenseite umhängen; ein voller
Commit-SHA nicht. Dependabot hält die SHAs und ``uv.lock`` aktuell, damit
das Pinnen nicht zu veralteten Ständen führt. Das Release belegt mit
Nachweisen (Provenienz, SBOM), woraus und wo Wheel und sdist gebaut wurden.

Testliste:
# [x] Jede Action in jedem Workflow ist auf einen vollen Commit-SHA mit Versionskommentar gepinnt
# [x] Dependabot pflegt GitHub Actions und uv
# [x] Der Build-Job erzeugt Provenienz- und SBOM-Nachweise für Wheel und sdist
# [x] Die SBOM liegt nicht in dist/, das komplett nach PyPI geht
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
import yaml

_ROOT = Path(__file__).resolve().parents[2]
_WORKFLOWS = sorted((_ROOT / ".github/workflows").glob("*.yml"))
_USES = re.compile(r"^\s*(?:-\s*)?uses:\s*(\S+)(.*)$", re.MULTILINE)
_PINNED = re.compile(r"^[\w.-]+/[\w./-]+@[0-9a-f]{40}$")


@pytest.mark.parametrize("workflow", _WORKFLOWS, ids=lambda p: p.name)
def test_every_action_is_pinned_to_a_commit(workflow: Path) -> None:
    eintraege = _USES.findall(workflow.read_text(encoding="utf-8"))

    lose = [ref for ref, kommentar in eintraege if not (_PINNED.match(ref) and re.match(r"\s+# v\d", kommentar))]

    assert eintraege
    assert lose == []


def test_dependabot_covers_actions_and_uv() -> None:
    daten = yaml.safe_load((_ROOT / ".github/dependabot.yml").read_text(encoding="utf-8"))

    systeme = {eintrag["package-ecosystem"] for eintrag in daten["updates"]}

    assert {"github-actions", "uv"} <= systeme


def _build_steps() -> list[dict[str, Any]]:
    release = yaml.safe_load((_ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8"))
    steps: list[dict[str, Any]] = release["jobs"]["build"]["steps"]
    return steps


def test_release_build_attests_provenance_and_sbom() -> None:
    nachweise = [s for s in _build_steps() if s.get("uses", "").startswith("actions/attest@")]

    arten = sorted("sbom-path" in s["with"] for s in nachweise)
    assert arten == [False, True]
    assert all("dist/*.whl" in s["with"]["subject-path"] for s in nachweise)
    assert all("dist/*.tar.gz" in s["with"]["subject-path"] for s in nachweise)


def test_sbom_is_written_outside_dist() -> None:
    sbom = [s for s in _build_steps() if "uv export" in s.get("run", "")]

    assert len(sbom) == 1
    assert "cyclonedx" in sbom[0]["run"]
    assert "dist/" not in sbom[0]["run"]
