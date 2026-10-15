"""Anpassung der Coverage-Untergrenze (Plan 2026-10-13-0902, U9, KTD7).

``scripts/coverage_ratchet.py`` leitet ``fail_under`` aus drei Messwerten
von ``main`` ab: kleinster Wert minus 0,5 Prozentpunkte, auf ganze Prozent
abgerundet. Es hebt nur an und weigert sich bei verändertem Arbeitsbaum.
Geprüft am Skript in einem eigenen Git-Repository im ``tmp_path``.

Über 100 ms je Test, bewusst: Das Skript läuft als echter Prozess gegen
ein echtes Git-Repository (Arbeitsbaum-Prüfung). Keine Dienste, deshalb
Unit-Tests (AGENTS.md: Begründung im Test).

Test-Liste:
- [x] 93,40 / 93,10 / 93,70 bei bisher 90 ergeben ``fail_under = 92``
- [x] ohne bisherigen Wert wird ``fail_under`` eingetragen
- [x] ein niedrigerer abgeleiteter Wert lässt die Datei unverändert und meldet das
- [x] veränderter Arbeitsbaum (versionierte Datei geändert): Abbruch, Datei unverändert
- [x] eine unversionierte Datei blockiert nicht
- [x] weniger als drei Werte: Abbruch
- [x] der Eintrag landet im Report-Abschnitt, ein folgender Abschnitt bleibt unberührt
- [x] vor dem folgenden Abschnitt bleibt genau eine Leerzeile (keine zusätzliche)
"""

from __future__ import annotations

import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

pytestmark = pytest.mark.timeout(10)

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "coverage_ratchet.py"


def _repo(tmp_path: Path, fail_under: int | None) -> Path:
    report = "[tool.coverage.report]\nprecision = 2\n"
    if fail_under is not None:
        report += f"fail_under = {fail_under}\n"
    (tmp_path / "pyproject.toml").write_text("[project]\nname = 'x'\n\n" + report, encoding="utf-8")
    for args in (
        ["init", "-q"],
        ["add", "pyproject.toml"],
        ["-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "init"],
    ):
        subprocess.run(["git", *args], cwd=tmp_path, check=True)
    return tmp_path


def _ratchet(repo: Path, *values: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(_SCRIPT), *values], cwd=repo, capture_output=True, text=True, check=False
    )


def _floor(repo: Path) -> object:
    report = tomllib.loads((repo / "pyproject.toml").read_text(encoding="utf-8"))["tool"]["coverage"]["report"]
    return report.get("fail_under")


def test_the_smallest_value_minus_half_a_point_rounded_down_becomes_the_floor(tmp_path: Path) -> None:
    repo = _repo(tmp_path, 90)

    result = _ratchet(repo, "93.40", "93.10", "93.70")

    assert result.returncode == 0, result.stderr
    assert _floor(repo) == 92


def test_without_a_floor_one_is_added(tmp_path: Path) -> None:
    repo = _repo(tmp_path, None)

    result = _ratchet(repo, "96.50", "96.51", "96.50")

    assert result.returncode == 0, result.stderr
    assert _floor(repo) == 96


def test_a_lower_derived_value_leaves_the_file_unchanged(tmp_path: Path) -> None:
    repo = _repo(tmp_path, 95)
    before = (repo / "pyproject.toml").read_text(encoding="utf-8")

    result = _ratchet(repo, "95.20", "95.40", "95.10")

    assert result.returncode == 0, result.stderr
    assert (repo / "pyproject.toml").read_text(encoding="utf-8") == before
    assert "unveraendert" in result.stdout


def test_a_dirty_worktree_aborts_without_touching_the_file(tmp_path: Path) -> None:
    repo = _repo(tmp_path, 90)
    path = repo / "pyproject.toml"
    path.write_text(path.read_text(encoding="utf-8") + "# lokale Aenderung\n", encoding="utf-8")
    before = path.read_text(encoding="utf-8")

    result = _ratchet(repo, "93.40", "93.10", "93.70")

    assert result.returncode != 0
    assert (repo / "pyproject.toml").read_text(encoding="utf-8") == before


def test_fewer_than_three_values_abort(tmp_path: Path) -> None:
    repo = _repo(tmp_path, 90)

    result = _ratchet(repo, "93.40", "93.10")

    assert result.returncode != 0
    assert _floor(repo) == 90


def test_the_floor_lands_in_the_report_section_before_the_next_one(tmp_path: Path) -> None:
    repo = _repo(tmp_path, None)
    path = repo / "pyproject.toml"
    path.write_text(path.read_text(encoding="utf-8") + "\n[tool.mypy]\nstrict = true\n", encoding="utf-8")
    subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qam", "mypy"], cwd=repo, check=True)

    result = _ratchet(repo, "96.50", "96.51", "96.50")

    assert result.returncode == 0, result.stderr
    parsed = tomllib.loads(path.read_text(encoding="utf-8"))
    assert parsed["tool"]["coverage"]["report"] == {"precision": 2, "fail_under": 96}
    assert parsed["tool"]["mypy"] == {"strict": True}


def test_exactly_one_blank_line_stays_before_the_next_section(tmp_path: Path) -> None:
    repo = _repo(tmp_path, None)
    path = repo / "pyproject.toml"
    path.write_text(path.read_text(encoding="utf-8") + "\n[tool.mypy]\nstrict = true\n", encoding="utf-8")
    subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qam", "mypy"], cwd=repo, check=True)

    result = _ratchet(repo, "96.50", "96.51", "96.50")

    assert result.returncode == 0, result.stderr
    assert "fail_under = 96\n\n[tool.mypy]" in path.read_text(encoding="utf-8")


def test_an_untracked_file_does_not_block(tmp_path: Path) -> None:
    repo = _repo(tmp_path, 90)
    (repo / "notizen.md").write_text("x", encoding="utf-8")

    result = _ratchet(repo, "93.40", "93.10", "93.70")

    assert result.returncode == 0, result.stderr
    assert _floor(repo) == 92
