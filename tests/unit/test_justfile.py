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
- [x] ``just unit`` verteilt den parallelen Durchgang auf 8 Prozesse (pytest-xdist), ``SWARMQ_TEST_WORKERS`` ändert die Zahl
- [x] ``just coverage`` misst Unit, Integration und e2e unter einer Sperre, parallel wie die Einzelrezepte
- [x] ``just e2e``: paralleler Durchgang (8 Prozesse, loadgroup) ohne exclusive-Tests, dann serieller nur mit ihnen
- [x] ``just unit``: ebenso paralleler, dann serieller Durchgang (``timing``-Tests), beide ohne Sperre
- [x] abgebrochener paralleler Durchgang (Exit 2, Strg-C): kein serieller, Exit 2
- [x] ``-n`` oder ``--numprocesses`` des Aufrufers gilt nur für den parallelen Durchgang
- [x] beide Durchgänge unter einer Sperre
- [x] junit-Datei des seriellen Durchgangs mit Endung ``-serial``
- [x] ein roter Durchgang macht den Lauf rot, der andere läuft trotzdem
- [x] ein leerer Durchgang (Exit 5) zählt nicht als Fehler, zwei leere schon
- [x] ``lint``, ``types``, ``chaos``, ``stress``, ``coverage-report``, ``coverage-gate``, ``ratchet`` rufen die CI-Befehle

Eingrenzen per Pfad (Plan 2026-10-15-1138, U16, R33):
- [x] ``just integration <pfad>``: beide Durchgänge laufen nur den Pfad
- [x] ``just e2e <pfad>::<test>`` grenzt auf die Node-ID ein
- [x] ``just integration -k foo`` grenzt nicht ein
- [x] ``just integration -k tests`` grenzt nicht ein, obwohl ``tests`` existiert
- [x] ``just integration --ignore <pfad>`` grenzt nicht ein
- [x] ``just unit <pfad>`` grenzt beide Durchgänge ein, keine Sperre
- [x] ``just property <pfad>`` und ``just stress <pfad>`` grenzen ein, der Marker bleibt
- [x] ein nicht existierender Pfad: die Suite bleibt davor
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
lockheld=${SWARMQ_TEST_LOCK_HELD:-} lock=$held excl=${SWARMQ_TEST_EXCLUSIVE:-}" >> "$STUB_LOG"
if [[ -n "${STUB_ORPHAN:-}" ]]; then (sleep 5 >/dev/null 2>&1 &) ; fi
if [[ -n "${STUB_SLEEP:-}" ]]; then sleep "$STUB_SLEEP"; fi
rc="STUB_RC_${SWARMQ_TEST_EXCLUSIVE:-none}"
exit "${!rc:-0}"
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
    calls = _calls(stub)
    assert all("tests/e2e" in call and "rmq=5699 valkey=6399" in call for call in calls)


def test_e2e_keeps_ports_from_the_environment(stub: dict[str, str]) -> None:
    stub |= {"SWARMQ_TEST_RABBITMQ_PORT": "5672", "SWARMQ_TEST_VALKEY_PORT": "6380"}

    result = _just(stub, "e2e")

    assert result.returncode == 0, result.stderr
    assert all("rmq=5672 valkey=6380" in call for call in _calls(stub))


def test_the_seed_is_passed_explicitly_and_printed(stub: dict[str, str]) -> None:
    result = _just(stub, "e2e", "-q")

    assert result.returncode == 0, result.stderr
    call = _calls(stub)[0]
    seed = call.split("--randomly-seed=")[1].split()[0]
    assert seed.isdigit()
    assert f"SWARMQ_TEST_SEED={seed}" in result.stdout + result.stderr


def test_swarmq_test_seed_fixes_the_seed(stub: dict[str, str]) -> None:
    stub["SWARMQ_TEST_SEED"] = "12345"

    _just(stub, "e2e")

    assert all("--randomly-seed=12345" in call for call in _calls(stub))


def test_e2e_holds_the_lock_while_pytest_runs(stub: dict[str, str]) -> None:
    result = _just(stub, "e2e")

    assert result.returncode == 0, result.stderr
    assert all("lock=held" in call for call in _calls(stub))


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
    suites = [next(arg for arg in call.split("pytest ")[1].split() if arg.startswith("tests/")) for call in calls]
    assert suites == [
        "tests/unit",
        "tests/unit",
        "tests/property",
        "tests/fuzz/test_parser_fuzz.py",
        "tests/integration",
        "tests/integration",
        "tests/e2e",
        "tests/e2e",
    ]
    assert all("lockheld=1 lock=held" in call for call in calls)


def test_with_ci_set_no_recipe_locks(stub: dict[str, str]) -> None:
    stub["CI"] = "true"

    result = _just(stub, "e2e")

    assert result.returncode == 0, result.stderr
    assert all("lockheld= lock=free" in call for call in _calls(stub))


def test_an_orphaned_child_does_not_keep_the_lock(stub: dict[str, str]) -> None:
    stub["STUB_ORPHAN"] = "1"

    result = _just(stub, "e2e")

    assert result.returncode == 0, result.stderr
    with open(stub["SWARMQ_TEST_LOCK"], "w") as handle:  # noqa: PTH123 - Datei fuer fcntl.flock
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)  # wirft, solange jemand sperrt


def test_unit_runs_without_the_lock(stub: dict[str, str]) -> None:
    result = _just(stub, "unit")

    assert result.returncode == 0, result.stderr
    calls = _calls(stub)
    assert len(calls) == 2
    assert all("lockheld= lock=free" in call for call in calls)


def test_coverage_measures_every_suite_under_one_lock(stub: dict[str, str]) -> None:
    result = _just(stub, "coverage")

    assert result.returncode == 0, result.stderr
    calls = [call.split("reload ")[-1].split(" rmq=")[0] for call in _calls(stub)]
    assert calls[0] == "coverage erase"
    measured = [call.split(" --randomly-seed")[0] for call in calls[1:7]]
    assert measured == [
        "coverage run -m pytest -n 8 --dist loadgroup tests/unit",
        "coverage run -m pytest tests/unit",
        "coverage run -m pytest -n 8 --dist loadgroup tests/integration",
        "coverage run -m pytest tests/integration",
        "coverage run -m pytest -n 8 --dist loadgroup tests/e2e",
        "coverage run -m pytest tests/e2e",
    ]
    assert calls[7:] == ["coverage combine", "coverage report"]
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

    parallel, _serial = _calls(stub)
    assert "pytest -n 8 --dist loadgroup tests/unit" in parallel


def test_swarmq_test_workers_sets_the_worker_count(stub: dict[str, str]) -> None:
    stub["SWARMQ_TEST_WORKERS"] = "3"

    _just(stub, "unit")

    parallel, _serial = _calls(stub)
    assert "pytest -n 3 --dist loadgroup tests/unit" in parallel


def test_e2e_runs_a_parallel_pass_then_the_exclusive_tests_alone(stub: dict[str, str]) -> None:
    result = _just(stub, "e2e", "-q")

    assert result.returncode == 0, result.stderr
    parallel, serial = _calls(stub)
    assert "pytest -n 8 --dist loadgroup tests/e2e -q" in parallel
    assert parallel.endswith("excl=skip")
    assert "pytest tests/e2e -q" in serial
    assert serial.endswith("excl=only")


def test_unit_runs_a_parallel_pass_then_the_timing_tests_alone_without_the_lock(stub: dict[str, str]) -> None:
    result = _just(stub, "unit", "-q")

    assert result.returncode == 0, result.stderr
    parallel, serial = _calls(stub)
    assert "pytest -n 8 --dist loadgroup tests/unit -q" in parallel
    assert parallel.endswith("lock=free excl=skip")
    assert "pytest tests/unit -q" in serial
    assert serial.endswith("lock=free excl=only")


@pytest.mark.parametrize("workers", [["-n", "4"], ["-n4"], ["--numprocesses", "4"], ["--numprocesses=4"]])
def test_a_worker_count_from_the_caller_stays_out_of_the_serial_pass(stub: dict[str, str], workers: list[str]) -> None:
    result = _just(stub, "unit", *workers, "-q")

    assert result.returncode == 0, result.stderr
    parallel, serial = _calls(stub)
    assert " ".join(workers) in parallel
    assert "pytest tests/unit -q --randomly-seed" in serial


def test_an_interrupted_parallel_pass_skips_the_serial_pass(stub: dict[str, str]) -> None:
    stub["STUB_RC_skip"] = "2"

    result = _just(stub, "unit")

    assert result.returncode == 2
    [parallel] = _calls(stub)
    assert parallel.endswith("excl=skip")


@pytest.mark.parametrize("junit", [["--junitxml=junit-e2e.xml"], ["--junitxml", "junit-e2e.xml"]])
def test_the_serial_pass_writes_its_own_junit_file(stub: dict[str, str], junit: list[str]) -> None:
    result = _just(stub, "e2e", *junit)

    assert result.returncode == 0, result.stderr
    parallel, serial = _calls(stub)
    assert " ".join(junit) in parallel
    assert " ".join(junit).replace("junit-e2e.xml", "junit-e2e-serial.xml") in serial


@pytest.mark.parametrize(("parallel_rc", "serial_rc"), [("1", "0"), ("0", "1")])
def test_a_red_pass_fails_the_run_and_the_other_pass_still_runs(
    stub: dict[str, str], parallel_rc: str, serial_rc: str
) -> None:
    stub |= {"STUB_RC_skip": parallel_rc, "STUB_RC_only": serial_rc}

    result = _just(stub, "e2e")

    assert result.returncode != 0
    assert len(_calls(stub)) == 2


@pytest.mark.parametrize(
    ("parallel_rc", "serial_rc", "expected"),
    [("0", "5", 0), ("5", "0", 0), ("5", "5", 5), ("5", "1", 1)],
)
def test_an_empty_pass_is_no_failure_unless_both_are_empty(
    stub: dict[str, str], parallel_rc: str, serial_rc: str, expected: int
) -> None:
    stub |= {"STUB_RC_skip": parallel_rc, "STUB_RC_only": serial_rc}

    result = _just(stub, "e2e")

    assert result.returncode == expected, result.stderr


def test_integration_with_a_path_runs_only_that_path(stub: dict[str, str], tmp_path: Path) -> None:
    target = tmp_path / "test_x.py"
    target.touch()

    result = _just(stub, "integration", str(target))

    assert result.returncode == 0, result.stderr
    parallel, serial = _calls(stub)
    assert f"pytest -n 8 --dist loadgroup {target} --randomly-seed" in parallel
    assert f"pytest {target} --randomly-seed" in serial
    assert "tests/integration" not in parallel + serial


def test_e2e_with_a_node_id_runs_only_that_test(stub: dict[str, str], tmp_path: Path) -> None:
    target = tmp_path / "test_x.py"
    target.touch()
    node = f"{target}::test_y[a-b]"

    result = _just(stub, "e2e", node)

    assert result.returncode == 0, result.stderr
    parallel, serial = _calls(stub)
    assert f"pytest -n 8 --dist loadgroup {node} --randomly-seed" in parallel
    assert f"pytest {node} --randomly-seed" in serial


def test_integration_with_a_keyword_keeps_the_suite(stub: dict[str, str]) -> None:
    result = _just(stub, "integration", "-k", "foo")

    assert result.returncode == 0, result.stderr
    parallel, serial = _calls(stub)
    assert "pytest -n 8 --dist loadgroup tests/integration -k foo --randomly-seed" in parallel
    assert "pytest tests/integration -k foo --randomly-seed" in serial


def test_a_keyword_naming_an_existing_directory_keeps_the_suite(stub: dict[str, str]) -> None:
    result = _just(stub, "integration", "-k", "tests")

    assert result.returncode == 0, result.stderr
    parallel, serial = _calls(stub)
    assert "pytest -n 8 --dist loadgroup tests/integration -k tests --randomly-seed" in parallel
    assert "pytest tests/integration -k tests --randomly-seed" in serial


def test_an_ignored_path_keeps_the_suite(stub: dict[str, str], tmp_path: Path) -> None:
    ignored = tmp_path / "x.py"
    ignored.touch()

    result = _just(stub, "integration", "--ignore", str(ignored))

    assert result.returncode == 0, result.stderr
    parallel, serial = _calls(stub)
    assert f"pytest -n 8 --dist loadgroup tests/integration --ignore {ignored} --randomly-seed" in parallel
    assert f"pytest tests/integration --ignore {ignored} --randomly-seed" in serial


@pytest.mark.parametrize("option", ["--config-file", "--override-ini", "--numprocesses", "--pythonwarnings"])
def test_a_long_option_value_naming_an_existing_file_keeps_the_suite(stub: dict[str, str], option: str) -> None:
    result = _just(stub, "integration", option, "pyproject.toml")

    assert result.returncode == 0, result.stderr
    parallel, serial = _calls(stub)
    assert f"tests/integration {option} pyproject.toml --randomly-seed" in parallel
    # ``--numprocesses`` gilt nur parallel; die Suite bleibt in beiden Durchgängen.
    kept = "" if option == "--numprocesses" else f" {option} pyproject.toml"
    assert f"pytest tests/integration{kept} --randomly-seed" in serial


def test_a_path_after_an_option_value_still_narrows(stub: dict[str, str], tmp_path: Path) -> None:
    target = tmp_path / "test_x.py"
    target.touch()

    result = _just(stub, "integration", "-k", "foo", str(target))

    assert result.returncode == 0, result.stderr
    parallel, serial = _calls(stub)
    assert f"pytest -n 8 --dist loadgroup -k foo {target} --randomly-seed" in parallel
    assert "tests/integration" not in parallel + serial


def test_unit_with_a_path_runs_only_that_path_without_the_lock(stub: dict[str, str], tmp_path: Path) -> None:
    target = tmp_path / "test_x.py"
    target.touch()

    result = _just(stub, "unit", str(target))

    assert result.returncode == 0, result.stderr
    parallel, serial = _calls(stub)
    assert f"pytest -n 8 --dist loadgroup {target} --randomly-seed" in parallel
    assert f"pytest {target} --randomly-seed" in serial
    assert "tests/unit" not in parallel + serial
    assert all("lockheld= lock=free" in call for call in (parallel, serial))


@pytest.mark.parametrize(("recipe", "marker", "locked"), [("property", "property", False), ("stress", "stress", True)])
def test_property_and_stress_with_a_path_keep_their_marker(
    stub: dict[str, str], tmp_path: Path, recipe: str, marker: str, locked: bool
) -> None:
    target = tmp_path / "test_x.py"
    target.touch()

    result = _just(stub, recipe, str(target))

    assert result.returncode == 0, result.stderr
    [call] = _calls(stub)
    assert f"pytest -m {marker} {target} --randomly-seed" in call
    assert ("lock=held" in call) is locked


def test_a_missing_path_keeps_the_suite_so_pytest_reports_it(stub: dict[str, str], tmp_path: Path) -> None:
    missing = tmp_path / "test_tippfehler.py"

    result = _just(stub, "integration", str(missing))

    assert result.returncode == 0, result.stderr
    parallel, serial = _calls(stub)
    assert f"pytest -n 8 --dist loadgroup tests/integration {missing} --randomly-seed" in parallel
    assert f"pytest tests/integration {missing} --randomly-seed" in serial
