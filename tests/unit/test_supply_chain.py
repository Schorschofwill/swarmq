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
# [x] Jedes Basis-Image in einem Dockerfile ist per Digest gepinnt, und Dependabot pflegt es
# [x] pip installiert die Benchmark-Abhängigkeiten nur gegen Hashes
# [x] CodeQL prüft Python bei jedem Push und PR auf einem Runner von GitHub
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
_DOCKERFILES = sorted(p for p in _ROOT.glob("**/Dockerfile") if ".venv" not in p.parts)
_FROM = re.compile(r"^FROM\s+(\S+)", re.MULTILINE)


@pytest.mark.parametrize("workflow", _WORKFLOWS, ids=lambda p: p.name)
def test_every_action_is_pinned_to_a_commit(workflow: Path) -> None:
    eintraege = _USES.findall(workflow.read_text(encoding="utf-8"))

    lose = [ref for ref, kommentar in eintraege if not (_PINNED.match(ref) and re.match(r"\s+# v\d", kommentar))]

    assert eintraege
    assert lose == []


def _dependabot() -> list[dict[str, Any]]:
    daten = yaml.safe_load((_ROOT / ".github/dependabot.yml").read_text(encoding="utf-8"))
    eintraege: list[dict[str, Any]] = daten["updates"]
    return eintraege


def test_dependabot_covers_actions_and_uv() -> None:
    systeme = {eintrag["package-ecosystem"] for eintrag in _dependabot()}

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


@pytest.mark.parametrize("dockerfile", _DOCKERFILES, ids=lambda p: str(p.relative_to(_ROOT)))
def test_base_images_are_pinned_by_digest(dockerfile: Path) -> None:
    images = _FROM.findall(dockerfile.read_text(encoding="utf-8"))

    assert images
    assert [i for i in images if not re.search(r":[\w.-]+@sha256:[0-9a-f]{64}$", i)] == []


def test_dependabot_updates_every_dockerfile() -> None:
    docker = [e for e in _dependabot() if e["package-ecosystem"] == "docker"]
    verzeichnisse = {d for e in docker for d in e.get("directories", [e.get("directory")])}

    assert {"/" + str(p.parent.relative_to(_ROOT)) for p in _DOCKERFILES} <= verzeichnisse


def test_benchmark_requirements_are_installed_by_hash() -> None:
    anforderungen = (_ROOT / "benchmarks/requirements.txt").read_text(encoding="utf-8")
    pakete = re.findall(r"^([A-Za-z0-9_.-]+)==", anforderungen, re.MULTILINE)
    skript = (_ROOT / "benchmarks/run.sh").read_text(encoding="utf-8")

    assert pakete
    assert anforderungen.count("--hash=sha256:") >= len(pakete)
    assert [z for z in skript.splitlines() if "pip install" in z and "--require-hashes" not in z] == []


def test_codeql_scans_python_on_github_runners() -> None:
    codeql = yaml.safe_load((_ROOT / ".github/workflows/codeql.yml").read_text(encoding="utf-8"))
    ausloeser = codeql[True]  # YAML 1.1 liest den Schlüssel ``on`` als True
    job = next(iter(codeql["jobs"].values()))

    assert {"push", "pull_request"} <= set(ausloeser)
    assert job["runs-on"] == "ubuntu-latest"
    init = next(s for s in job["steps"] if s.get("uses", "").startswith("github/codeql-action/init@"))
    assert init["with"]["languages"] == "python"
