"""Ratchet-Vergleich des Coverage-Gates (Plan 2026-10-13-0902, U8, KTD7).

Ein Branch darf ``[tool.coverage.report] fail_under`` gegenüber seinem
Merge-Base mit ``main`` nicht senken. Fehlt der Wert im Merge-Base, gilt 0.

Test-Liste:
- [x] gleicher Wert: kein Fehler
- [x] angehoben: kein Fehler
- [x] gesenkt: Fehler nennt beide Werte
- [x] im Merge-Base noch kein Wert: gilt 0
- [x] im Branch entfernt, im Merge-Base gesetzt: Fehler

``main()`` mit ersetztem ``git`` und diff-cover:
- [x] Tag-Pipeline: 0, ohne git
- [x] ``origin/main`` fehlt nach dem Fetch: 1
- [x] auf ``main``: 0, ohne Vergleich
- [x] kein Merge-Base: 1
- [x] gesenkte Untergrenze: 1, diff-cover läuft nicht
- [x] sonst: Exit-Code von diff-cover, gegen den Merge-Base

Ref-Art und Branch aus ``GITHUB_REF_TYPE``/``GITHUB_REF_NAME`` (Plan
2026-10-28-1308, KTD8); seit dem Umzug (U15):
- [x] ``CI_COMMIT_TAG``/``CI_COMMIT_BRANCH`` aus der GitLab-Zeit werden ignoriert
"""

from __future__ import annotations

import importlib.util
import subprocess
from pathlib import Path
from types import ModuleType

import pytest

pytestmark = pytest.mark.timeout(5)


def _load_gate() -> ModuleType:
    path = Path(__file__).resolve().parents[2] / "scripts" / "coverage_gate.py"
    spec = importlib.util.spec_from_file_location("coverage_gate", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


gate = _load_gate()


def _toml(fail_under: float | None) -> str:
    if fail_under is None:
        return "[tool.coverage.report]\nprecision = 2\n"
    return f"[tool.coverage.report]\nprecision = 2\nfail_under = {fail_under}\n"


def test_an_unchanged_floor_passes() -> None:
    assert gate.ratchet_error(_toml(95), _toml(95)) is None


def test_a_lowered_floor_fails_and_names_both_values() -> None:
    error = gate.ratchet_error(_toml(95), _toml(94.5))

    assert error is not None
    assert "95" in error
    assert "94.5" in error


def test_a_raised_floor_passes() -> None:
    assert gate.ratchet_error(_toml(95), _toml(96)) is None


def test_a_base_without_a_floor_counts_as_zero() -> None:
    assert gate.ratchet_error(_toml(None), _toml(90)) is None


def test_removing_the_floor_on_the_branch_fails() -> None:
    assert gate.ratchet_error(_toml(95), _toml(None)) is not None


class _Git:
    """Ersatz fuer ``gate._git``: Antworten je Unterbefehl, Aufrufe protokolliert."""

    def __init__(self, *, main_ref: bool = True, merge_base: str = "abc123", base_floor: float | None = 95) -> None:
        self.calls: list[tuple[str, ...]] = []
        self._main_ref = main_ref
        self._merge_base = merge_base
        self._base_floor = base_floor

    def __call__(self, *args: str) -> subprocess.CompletedProcess[str]:
        self.calls.append(args)
        stdout, code = "", 0
        if args[0] == "rev-parse" and "--verify" in args:
            code = 0 if self._main_ref else 1
        elif args[0] == "rev-parse":
            stdout = "feature"
        elif args[0] == "merge-base":
            stdout = self._merge_base
        elif args[0] == "show":
            stdout = _toml(self._base_floor)
        return subprocess.CompletedProcess(["git", *args], code, stdout, "")


@pytest.fixture
def run_gate(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """Ruft ``gate.main()`` im ``tmp_path`` mit gegebenem Branch-pyproject auf."""
    diff_cover: list[list[str]] = []

    def fake_run(argv: list[str], **_kw: object) -> subprocess.CompletedProcess[str]:
        diff_cover.append(argv)
        return subprocess.CompletedProcess(argv, 7, "", "")

    def run(
        git: _Git,
        *,
        branch: str = "feature",
        tag: str = "",
        head_floor: float | None = 95,
        extra_env: dict[str, str] | None = None,
    ) -> int:
        (tmp_path / "pyproject.toml").write_text(_toml(head_floor), encoding="utf-8")
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(gate, "_git", git)
        monkeypatch.setattr(gate.subprocess, "run", fake_run)
        for name in ("CI_COMMIT_BRANCH", "CI_COMMIT_TAG"):
            monkeypatch.delenv(name, raising=False)
        monkeypatch.setenv("GITHUB_REF_TYPE", "tag" if tag else "branch")
        monkeypatch.setenv("GITHUB_REF_NAME", tag or branch)
        for name, value in (extra_env or {}).items():
            monkeypatch.setenv(name, value)
        return int(gate.main())

    run.diff_cover = diff_cover  # type: ignore[attr-defined]  # Protokoll fuer die Tests
    return run


def test_a_tag_pipeline_passes_without_git(run_gate) -> None:
    git = _Git()

    assert run_gate(git, tag="v1.0.0") == 0
    assert git.calls == []


def test_a_missing_origin_main_fails(run_gate) -> None:
    assert run_gate(_Git(main_ref=False)) == 1


def test_main_itself_passes_without_comparison(run_gate) -> None:
    git = _Git()

    assert run_gate(git, branch="main") == 0
    assert not any(call[0] == "merge-base" for call in git.calls)
    assert run_gate.diff_cover == []


def test_a_missing_merge_base_fails(run_gate) -> None:
    assert run_gate(_Git(merge_base="")) == 1


def test_a_lowered_floor_fails_before_diff_cover(run_gate) -> None:
    assert run_gate(_Git(base_floor=95), head_floor=90) == 1
    assert run_gate.diff_cover == []


def test_otherwise_diff_cover_decides_against_the_merge_base(run_gate) -> None:
    assert run_gate(_Git(merge_base="abc123")) == 7
    [argv] = run_gate.diff_cover
    assert argv[:2] == ["diff-cover", "coverage.xml"]
    assert "--compare-branch=abc123" in argv
    assert "--fail-under=100" in argv


def test_gitlab_variables_are_ignored(run_gate) -> None:
    """Nach dem Umzug gibt es keine GitLab-CI mehr (Plan 2026-10-28-1308, U15).

    Ein übrig gebliebenes ``CI_COMMIT_TAG`` oder ``CI_COMMIT_BRANCH`` in der
    Umgebung darf den Vergleich eines Branch-Laufs nicht abschalten.
    """
    gitlab = {"CI_COMMIT_TAG": "v1.0.0", "CI_COMMIT_BRANCH": "main"}

    assert run_gate(_Git(merge_base="abc123"), branch="feature", extra_env=gitlab) == 7
