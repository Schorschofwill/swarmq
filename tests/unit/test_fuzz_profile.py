"""Hypothesis-Profil ``ci`` der Fuzz-Suite (Plan 2026-10-24-1118, KTD1, R10).

``tests/fuzz/conftest.py`` lädt das Profil, wenn ``SWARMQ_FUZZ_PROFILE`` es
nennt. Die Fuzz-Ziele setzen eigene ``@settings(max_examples=500,
deadline=None)`` und erben die übrigen Felder, etwa ``derandomize``, vom
geladenen Profil. Geprüft wird das an einem Ziel derselben Form, das seine
Eingaben aufzeichnet, in zwei echten pytest-Läufen mit verschiedenem Seed
für pytest-randomly.

Über 100 ms, bewusst: Jeder Lauf ist ein frischer pytest-Prozess, denn das
Profil wird beim Import der conftest global geladen; in diesem Prozess
verfälschte es alle späteren Hypothesis-Tests (AGENTS.md: Begründung im Test).

Test-Liste:
- [x] mit ``SWARMQ_FUZZ_PROFILE=ci`` erzeugen zwei Läufe dieselbe Beispielfolge
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.timeout(60)

_REPO = Path(__file__).resolve().parents[2]

# Dieselbe Form wie die Fuzz-Ziele: eigene @settings ohne derandomize.
_RECORDING_TARGET = """
import os

from hypothesis import given, settings, strategies as st


@given(st.binary())
@settings(max_examples=500, deadline=None)
def test_record(raw):
    with open(os.environ["RECORD"], "a", encoding="ascii") as handle:
        handle.write(raw.hex() + "\\n")
"""


def _recorded_examples(tmp_path: Path, name: str, seed: int, profile: str | None) -> list[str]:
    run_dir = tmp_path / name
    run_dir.mkdir()
    (run_dir / "test_record.py").write_text(_RECORDING_TARGET, encoding="utf-8")
    record = run_dir / "record.txt"
    env = {key: value for key, value in os.environ.items() if key != "SWARMQ_FUZZ_PROFILE"}
    env |= {"PYTHONPATH": str(_REPO), "RECORD": str(record)}
    if profile is not None:
        env["SWARMQ_FUZZ_PROFILE"] = profile
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "-c",
            os.devnull,
            "-p",
            "no:cacheprovider",
            "-p",
            "tests.fuzz.conftest",
            f"--randomly-seed={seed}",
            "test_record.py",
        ],
        cwd=run_dir,
        env=env,
        capture_output=True,
        text=True,
        timeout=50,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return record.read_text(encoding="ascii").splitlines()


def test_two_runs_with_the_ci_profile_generate_the_same_examples(tmp_path: Path) -> None:
    first = _recorded_examples(tmp_path, "a", seed=1, profile="ci")
    second = _recorded_examples(tmp_path, "b", seed=2, profile="ci")

    assert len(first) > 1
    assert first == second
