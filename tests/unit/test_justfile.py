"""``justfile`` als Einstiegspunkt (Plan 2026-10-13-0902, U7, KTD9/KTD10).

Die Rezepte laufen echt über ``just``; ein Stub für ``uv`` im ``PATH``
protokolliert Argumente und Umgebung, statt pytest zu starten. Die Sperre ist
eine Datei im ``tmp_path``.

Über 100 ms je Test, bewusst: Geprüft wird das echte ``just`` mit echtem
``flock``, ein Nachbau bewiese nichts. Keine Dienste, deshalb Unit-Tests
(AGENTS.md: Begründung im Test).

Test-Liste:
- [x] ``just e2e`` setzt die Ports 5699/6399, wenn keine gesetzt sind
- [x] gesetzte Ports übernimmt ``just e2e`` unverändert
- [x] der Seed wird ausdrücklich übergeben und ausgegeben, auch ohne Vorgabe
- [x] ``SWARMQ_TEST_SEED`` legt den Seed fest
- [x] ``just e2e`` hält die Sperre, solange pytest läuft
- [x] eine gehaltene Sperre: kurze Frist, Abbruch mit Hinweis
- [x] ``just all`` hält die Sperre über alle Suiten und sperrt nicht erneut
- [x] mit ``CI=true`` sperrt kein Rezept
- [x] ein verwaister Kindprozess hält die Sperre nicht
- [x] ``just unit`` sperrt nicht (keine Dienste)
- [x] ``just unit`` verteilt auf 8 Prozesse (pytest-xdist), ``SWARMQ_TEST_WORKERS`` ändert die Zahl
- [x] ``just coverage`` misst Unit, Integration und e2e unter einer Sperre
- [x] ``lint``, ``types``, ``chaos``, ``stress``, ``coverage-report``, ``coverage-gate``, ``ratchet`` rufen die CI-Befehle
"""

from __future__ import annotations

import fcntl
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.timeout(30)

_REPO = Path(__file__).resolve().parents[2]
_JUST = shutil.which("just", path=str(Path(sys.executable).parent)) or "just"

# Protokolliert jeden Aufruf als eine Zeile: Argumente, Ports, Seed-Umgebung,
# ob die Sperre gerade gehalten wird (``flock -n`` scheitert dann).
_UV_STUB = """#!/usr/bin/env bash
held=free
if [[ -n "${STUB_LOCK:-}" ]] && ! flock -n "$STUB_LOCK" true 2>/dev/null; then held=held; fi
echo "args=$* rmq=${SWARMQ_TEST_RABBITMQ_PORT:-} valkey=${SWARMQ_TEST_VALKEY_PORT:-} \
lockheld=${SWARMQ_TEST_LOCK_HELD:-} lock=$held" >> "$STUB_LOG"
if [[ -n "${STUB_ORPHAN:-}" ]]; then (sleep 5 >/dev/null 2>&1 &) ; fi
if [[ -n "${STUB_SLEEP:-}" ]]; then sleep "$STUB_SLEEP"; fi
"""


@pytest.fixture
def stub(tmp_path: Path) -> dict[str, str]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    uv = bin_dir / "uv"
    uv.write_text(_UV_STUB, encoding="utf-8")
    uv.chmod(0o755)
    lock = tmp_path / "itest.lock"
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("SWARMQ_TEST_") and key not in {"CI", "COVERAGE_RUN"}
    }
    env |= {
        "PATH": f"{bin_dir}{os.pathsep}{env['PATH']}",
        "STUB_LOG": str(tmp_path / "uv.log"),
        "STUB_LOCK": str(lock),
        "SWARMQ_TEST_LOCK": str(lock),
    }
    return env


def _just(env: dict[str, str], *args: str, timeout: float = 20) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [_JUST, "--justfile", str(_REPO / "justfile"), "--working-directory", str(_REPO), *args],
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def _calls(env: dict[str, str]) -> list[str]:
    return Path(env["STUB_LOG"]).read_text(encoding="utf-8").splitlines()


def test_e2e_sets_the_test_ports_when_none_are_given(stub: dict[str, str]) -> None:
    result = _just(stub, "e2e")

    assert result.returncode == 0, result.stderr
    [call] = _calls(stub)
    assert "pytest tests/e2e" in call
    assert "rmq=5699 valkey=6399" in call


def test_e2e_keeps_ports_from_the_environment(stub: dict[str, str]) -> None:
    stub |= {"SWARMQ_TEST_RABBITMQ_PORT": "5672", "SWARMQ_TEST_VALKEY_PORT": "6380"}

    result = _just(stub, "e2e")

    assert result.returncode == 0, result.stderr
    [call] = _calls(stub)
    assert "rmq=5672 valkey=6380" in call


def test_the_seed_is_passed_explicitly_and_printed(stub: dict[str, str]) -> None:
    result = _just(stub, "e2e", "-q")

    assert result.returncode == 0, result.stderr
    [call] = _calls(stub)
    seed = call.split("--randomly-seed=")[1].split()[0]
    assert seed.isdigit()
    assert f"SWARMQ_TEST_SEED={seed}" in result.stdout + result.stderr


def test_swarmq_test_seed_fixes_the_seed(stub: dict[str, str]) -> None:
    stub["SWARMQ_TEST_SEED"] = "12345"

    _just(stub, "e2e")

    [call] = _calls(stub)
    assert "--randomly-seed=12345" in call


def test_e2e_holds_the_lock_while_pytest_runs(stub: dict[str, str]) -> None:
    result = _just(stub, "e2e")

    assert result.returncode == 0, result.stderr
    [call] = _calls(stub)
    assert "lock=held" in call


def test_a_held_lock_aborts_with_a_hint_after_the_wait(stub: dict[str, str]) -> None:
    stub["SWARMQ_TEST_LOCK_WAIT"] = "1"
    with open(stub["SWARMQ_TEST_LOCK"], "w") as handle:  # noqa: PTH123 - Datei fuer fcntl.flock
        fcntl.flock(handle, fcntl.LOCK_EX)
        result = _just(stub, "e2e")

    assert result.returncode != 0
    assert "Testsperre" in result.stderr
    assert not Path(stub["STUB_LOG"]).exists()


def test_all_holds_the_lock_once_across_every_suite(stub: dict[str, str]) -> None:
    result = _just(stub, "all")

    assert result.returncode == 0, result.stderr
    calls = _calls(stub)
    suites = [call.split("pytest ")[1].split()[0] for call in calls]
    assert suites == [
        "tests/unit",
        "tests/property",
        "tests/fuzz/test_parser_fuzz.py",
        "tests/integration",
        "tests/e2e",
    ]
    assert all("lockheld=1 lock=held" in call for call in calls)


def test_with_ci_set_no_recipe_locks(stub: dict[str, str]) -> None:
    stub["CI"] = "true"

    result = _just(stub, "e2e")

    assert result.returncode == 0, result.stderr
    [call] = _calls(stub)
    assert "lockheld= lock=free" in call


def test_an_orphaned_child_does_not_keep_the_lock(stub: dict[str, str]) -> None:
    stub["STUB_ORPHAN"] = "1"

    result = _just(stub, "e2e")

    assert result.returncode == 0, result.stderr
    with open(stub["SWARMQ_TEST_LOCK"], "w") as handle:  # noqa: PTH123 - Datei fuer fcntl.flock
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)  # wirft, solange jemand sperrt


def test_unit_runs_without_the_lock(stub: dict[str, str]) -> None:
    result = _just(stub, "unit")

    assert result.returncode == 0, result.stderr
    [call] = _calls(stub)
    assert "lockheld= lock=free" in call


def test_coverage_measures_every_suite_under_one_lock(stub: dict[str, str]) -> None:
    result = _just(stub, "coverage")

    assert result.returncode == 0, result.stderr
    calls = [call.split("reload ")[-1].split(" rmq=")[0] for call in _calls(stub)]
    assert calls[0] == "coverage erase"
    measured = [call.split(" --randomly-seed")[0] for call in calls[1:4]]
    assert measured == [
        "coverage run -m pytest tests/unit",
        "coverage run -m pytest tests/integration",
        "coverage run -m pytest tests/e2e",
    ]
    assert calls[4:] == ["coverage combine", "coverage report"]
    assert all("lockheld=1 lock=held" in call for call in _calls(stub))


@pytest.mark.parametrize(
    ("recipe", "expected", "locked"),
    [
        ("lint", ["ruff format --check .", "ruff check src/ tests/ typings/"], False),
        (
            "types",
            [
                "mypy src/swarmq tests",
                "basedpyright",
                "python -m mypy.stubtest msgpack croniter --mypy-config-file pyproject.toml --ignore-missing-stub",
            ],
            False,
        ),
        ("chaos", ["pytest -m chaos"], True),
        ("stress", ["pytest tests/stress -m stress"], True),
        ("coverage-report", ["coverage combine", "coverage xml", "coverage report"], False),
        ("coverage-gate", ["--group coverage python scripts/coverage_gate.py"], False),
        ("ratchet", ["python scripts/coverage_ratchet.py"], False),
    ],
)
def test_recipes_run_the_ci_commands(stub: dict[str, str], recipe: str, expected: list[str], locked: bool) -> None:
    result = _just(stub, recipe)

    assert result.returncode == 0, result.stderr
    calls = _calls(stub)
    commands = [call.split("reload ")[-1].split(" rmq=")[0].split(" --randomly-seed")[0] for call in calls]
    assert commands == expected
    assert all(("lock=held" in call) is locked for call in calls)


def test_unit_runs_on_eight_workers_by_default(stub: dict[str, str]) -> None:
    _just(stub, "unit")

    [call] = _calls(stub)
    assert "pytest tests/unit -n 8" in call


def test_swarmq_test_workers_sets_the_worker_count(stub: dict[str, str]) -> None:
    stub["SWARMQ_TEST_WORKERS"] = "3"

    _just(stub, "unit")

    [call] = _calls(stub)
    assert "pytest tests/unit -n 3" in call
