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
- [x] das Coverage-Badge in README und Doku-Startseiten steigt mit; fehlt eine Datei, stört das nicht

Ohne Werte, über die GitHub-API (Plan 2026-10-28-1308, U2, KTD8), mit
gefälschtem ``gh`` im ``PATH``:
- [x] die Quoten-Artefakte der letzten drei erfolgreichen ``main``-Läufe zählen, ältere nicht
- [x] ein Lauf ohne Artefakt zählt nicht; bleiben weniger als drei, bricht das Skript ab
- [x] ein Fork-PR-Lauf, dessen Branch main heißt, zählt nicht (Review #6)
"""

from __future__ import annotations

import io
import json
import os
import subprocess
import sys
import tomllib
import zipfile
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


def _ratchet(repo: Path, *values: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(_SCRIPT), *values], cwd=repo, capture_output=True, text=True, check=False, env=env
    )


# Gefälschtes ``gh``: beantwortet die drei ``gh api``-Pfade aus Dateien im
# selben Verzeichnis (runs.json, artifacts-<run>.json, zip-<artefakt>).
_FAKE_GH = """#!{python}
import pathlib, sys
here = pathlib.Path(__file__).parent
path = sys.argv[2]
if "/runs?" in path:
    name = "runs.json"
elif "/artifacts?" in path:
    name = "artifacts-" + path.split("/runs/")[1].split("/")[0] + ".json"
else:
    name = "zip-" + path.split("/artifacts/")[1].split("/")[0]
sys.stdout.buffer.write((here / name).read_bytes())
"""


def _github(tmp_path: Path, totals: list[str | None], forks: frozenset[int] = frozenset()) -> dict[str, str]:
    """Fälscht ``gh`` mit erfolgreichen main-Läufen (neueste zuerst); ``None``: Lauf ohne Artefakt.

    Die Indizes in ``forks`` sind Pull Requests aus einem Fork, dessen Branch ``main`` heißt.
    """
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    gh = bin_dir / "gh"
    gh.write_text(_FAKE_GH.format(python=sys.executable), encoding="utf-8")
    gh.chmod(0o755)
    eigen = {"full_name": "Schorschofwill/swarmq"}
    fremd = {"full_name": "fremd/swarmq"}
    ids = [100 + i for i in range(len(totals))]
    runs = [
        {
            "id": run_id,
            "event": "pull_request" if i in forks else "push",
            "repository": eigen,
            "head_repository": fremd if i in forks else eigen,
        }
        for i, run_id in enumerate(ids)
    ]
    (bin_dir / "runs.json").write_text(json.dumps({"workflow_runs": runs}), encoding="utf-8")
    for run_id, total in zip(ids, totals, strict=True):
        artifacts = [] if total is None else [{"id": run_id + 1000}]
        (bin_dir / f"artifacts-{run_id}.json").write_text(json.dumps({"artifacts": artifacts}), encoding="utf-8")
        if total is not None:
            buffer = io.BytesIO()
            with zipfile.ZipFile(buffer, "w") as archive:
                archive.writestr("coverage-total.txt", f"{total}\n")
            (bin_dir / f"zip-{run_id + 1000}").write_bytes(buffer.getvalue())
    return {**os.environ, "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}"}


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


def test_without_values_the_last_three_successful_main_runs_count(tmp_path: Path) -> None:
    (tmp_path / "repo").mkdir()
    repo = _repo(tmp_path / "repo", 90)
    env = _github(tmp_path, ["96.40", "96.70", "96.50", "90.00"])

    result = _ratchet(repo, env=env)

    assert result.returncode == 0, result.stderr
    assert _floor(repo) == 95


def test_a_run_without_an_artifact_does_not_count(tmp_path: Path) -> None:
    (tmp_path / "repo").mkdir()
    repo = _repo(tmp_path / "repo", 90)
    env = _github(tmp_path, ["96.40", None, "96.50"])

    result = _ratchet(repo, env=env)

    assert result.returncode != 0
    assert "gefunden 2" in result.stderr
    assert _floor(repo) == 90


def test_ein_fork_pr_lauf_mit_branch_main_zaehlt_nicht(tmp_path: Path) -> None:
    """Der Filter ``branch=main`` der API vergleicht nur den Branchnamen; ein Fork-Branch main passt auch."""
    (tmp_path / "repo").mkdir()
    repo = _repo(tmp_path / "repo", 90)
    env = _github(tmp_path, ["90.00", "96.40", "96.70", "96.50"], forks=frozenset({0}))

    result = _ratchet(repo, env=env)

    assert result.returncode == 0, result.stderr
    assert _floor(repo) == 95


_BADGE = "https://img.shields.io/badge/coverage-%E2%89%A5{}%25-1F2B47"


def test_the_coverage_badges_follow_the_new_floor(tmp_path: Path) -> None:
    repo = _repo(tmp_path, 90)
    (repo / "website").mkdir()
    for datei in ("README.md", "website/index.md"):
        (repo / datei).write_text(f'<img alt="Coverage" src="{_BADGE.format(90)}">\n', encoding="utf-8")
    subprocess.run(["git", "add", "-A"], cwd=repo, check=True)
    subprocess.run(
        ["git", "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "badges"], cwd=repo, check=True
    )

    result = _ratchet(repo, "93.40", "93.10", "93.70")

    assert result.returncode == 0, result.stderr
    for datei in ("README.md", "website/index.md"):
        assert (repo / datei).read_text(encoding="utf-8") == f'<img alt="Coverage" src="{_BADGE.format(92)}">\n'
