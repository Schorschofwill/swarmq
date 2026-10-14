"""Ratchet-Vergleich des Coverage-Gates (Plan 2026-10-13-0902, U8, KTD7).

Ein Branch darf ``[tool.coverage.report] fail_under`` gegenüber seinem
Merge-Base mit ``main`` nicht senken. Fehlt der Wert im Merge-Base, gilt 0.

Test-Liste:
- [x] gleicher Wert: kein Fehler
- [x] angehoben: kein Fehler
- [x] gesenkt: Fehler nennt beide Werte
- [x] im Merge-Base noch kein Wert: gilt 0
- [x] im Branch entfernt, im Merge-Base gesetzt: Fehler
"""

from __future__ import annotations

import importlib.util
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
