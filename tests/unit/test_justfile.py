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
- [x] ``lint``, ``types``, ``chaos``, ``stress``, ``coverage-report``, ``coverage-gate``, ``ratchet``, ``saettigung`` rufen die CI-Befehle
- [x] ``ab`` ruft das A/B-Messwerkzeug mit der Gruppe ``messung`` und reicht die Argumente durch (Plan 2026-10-24-1118)

Kostensuite (Plan 2026-10-24-1118):
- [x] ``kosten`` ruft die Suite unter ``tests/kosten`` mit Marker ``kosten``, ohne Sperre
- [x] ``just all`` fährt ``kosten`` nach ``fuzz``
- [x] ``kosten-basislinie <grund>`` misst in eine Datei und übergibt Grund und Datei dem Skript
- [x] ein roter Messlauf schreibt keine Basislinie

Valkey-Kosten (Plan 2026-10-24-1118):
- [x] ``kosten-valkey`` ruft die Suite unter ``tests/kosten/valkey`` mit Marker ``kosten_valkey``, unter der Sperre
- [x] ``just all`` fährt ``kosten-valkey`` nach ``integration``
- [x] ``kosten-basislinie <grund>`` misst beide Teile in dieselbe Datei, den Valkey-Teil unter der Sperre
- [x] ``kosten-basislinie``: ein Valkey-Teil mit übersprungenen Tests (kein Test-Valkey) schreibt keine Basislinie

Eingrenzen per Pfad (Plan 2026-10-15-1138, U16, R33):
- [x] ``just integration <pfad>``: beide Durchgänge laufen nur den Pfad
- [x] ``just e2e <pfad>::<test>`` grenzt auf die Node-ID ein
- [x] ``just integration -k foo`` grenzt nicht ein
- [x] ``just integration -k tests`` grenzt nicht ein, obwohl ``tests`` existiert
- [x] ``just integration --ignore <pfad>`` grenzt nicht ein
- [x] ``just unit <pfad>`` grenzt beide Durchgänge ein, keine Sperre
- [x] ``just property <pfad>`` und ``just stress <pfad>`` grenzen ein, der Marker bleibt
- [x] ein nicht existierender Pfad: die Suite bleibt davor

Wächter gegen leere Läufe (Plan 2026-10-24-1118, KTD1, AE7):
- [x] ``SWARMQ_TEST_MIN_TESTS``: weniger ausgeführte Tests als verlangt (nur übersprungene) machen den Lauf rot
- [x] genug ausgeführte Tests: der Lauf bleibt grün
- [x] ohne ``SWARMQ_TEST_MIN_TESTS`` bleibt ein Lauf ohne ausgeführte Tests grün wie bisher
- [x] ohne ``--junitxml`` des Aufrufers liest der Wächter eine eigene junit-Datei

Garantie-Vergleich (Plan 2026-10-24-1118):
- [x] ``vergleich-garantie`` ruft ``stoerungen.garantie`` in ``benchmarks/comparison`` ohne Sperre und reicht die Argumente durch

Abhängigkeitsprüfung (Plan 2026-10-24-1118, KTD2):
- [x] ``just audit`` ruft ``uv audit --frozen``
- [x] der Exitcode von ``uv audit`` wird unverändert weitergereicht
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
if [[ -n "${SWARMQ_KOSTEN_MESSUNG:-}" ]]; then echo "$SWARMQ_KOSTEN_MESSUNG" >> "$STUB_LOG.messung"; fi
if [[ -n "${STUB_ORPHAN:-}" ]]; then (sleep 5 >/dev/null 2>&1 &) ; fi
if [[ -n "${STUB_SLEEP:-}" ]]; then sleep "$STUB_SLEEP"; fi
# STUB_JUNIT: Inhalt der junit-Datei, die pytest unter --junitxml schriebe
if [[ -n "${STUB_JUNIT:-}" ]]; then
    previous=""
    for arg in "$@"; do
        case "$previous|$arg" in
            --junitxml\\|*) printf '%s' "$STUB_JUNIT" > "$arg" ;;
            *"|--junitxml="*) printf '%s' "$STUB_JUNIT" > "${arg#--junitxml=}" ;;
        esac
        previous="$arg"
    done
fi
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
        "tests/fuzz",
        "tests/kosten",
        "tests/integration",
        "tests/integration",
        "tests/kosten/valkey",
        "tests/e2e",
        "tests/e2e",
    ]
    # Fuzz ohne Dienste: die Datei mit chaos_stack bleibt draussen (Plan 2026-10-17-0941, R21).
    assert "--ignore tests/fuzz/test_external_publish_fuzz.py" in calls[3]
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
        ("kosten", ["pytest tests/kosten -m kosten"], False),
        ("kosten-valkey", ["pytest tests/kosten/valkey -m kosten_valkey"], True),
        ("coverage-report", ["coverage combine", "coverage xml", "coverage report"], False),
        ("coverage-gate", ["--group coverage python scripts/coverage_gate.py"], False),
        ("ratchet", ["python scripts/coverage_ratchet.py"], False),
        ("saettigung", ["python scripts/saettigung.py"], False),
    ],
)
def test_recipes_run_the_ci_commands(stub: dict[str, str], recipe: str, expected: list[str], locked: bool) -> None:
    result = _just(stub, recipe)

    assert result.returncode == 0, result.stderr
    calls = _calls(stub)
    commands = [call.split("reload ")[-1].split(" rmq=")[0].split(" --randomly-seed")[0] for call in calls]
    assert commands == expected
    assert all(("lock=held" in call) is locked for call in calls)


def test_ab_runs_the_measuring_tool_with_the_messung_group(stub: dict[str, str]) -> None:
    result = _just(stub, "ab", "--a", "v1", "--b", "HEAD", "--befehl", "echo '{}'")

    assert result.returncode == 0, result.stderr
    [call] = _calls(stub)
    assert "--group messung python scripts/ab_messung.py --a v1 --b HEAD --befehl echo '{}' rmq=" in call
    assert "lock=free" in call


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


def _junit(tests: int, skipped: int) -> str:
    return (
        '<?xml version="1.0" encoding="utf-8"?><testsuites name="pytest tests">'
        f'<testsuite name="pytest" errors="0" failures="0" skipped="{skipped}" tests="{tests}" time="0.1">'
        "</testsuite></testsuites>"
    )


def test_a_run_with_fewer_executed_tests_than_the_minimum_fails(stub: dict[str, str], tmp_path: Path) -> None:
    stub |= {"SWARMQ_TEST_MIN_TESTS": "1", "STUB_JUNIT": _junit(tests=31, skipped=31)}

    result = _just(stub, "fuzz", f"--junitxml={tmp_path / 'junit-fuzz.xml'}")

    assert result.returncode != 0
    assert "0 Tests ausgeführt" in result.stderr


def test_a_run_with_enough_executed_tests_passes(stub: dict[str, str], tmp_path: Path) -> None:
    stub |= {"SWARMQ_TEST_MIN_TESTS": "30", "STUB_JUNIT": _junit(tests=31, skipped=1)}

    result = _just(stub, "fuzz", f"--junitxml={tmp_path / 'junit-fuzz.xml'}")

    assert result.returncode == 0, result.stderr


def test_without_a_minimum_a_run_of_skipped_tests_stays_green(stub: dict[str, str], tmp_path: Path) -> None:
    stub["STUB_JUNIT"] = _junit(tests=31, skipped=31)

    result = _just(stub, "fuzz", f"--junitxml={tmp_path / 'junit-fuzz.xml'}")

    assert result.returncode == 0, result.stderr


def test_without_a_junit_file_from_the_caller_the_guard_uses_its_own(stub: dict[str, str]) -> None:
    stub |= {"SWARMQ_TEST_MIN_TESTS": "1", "STUB_JUNIT": _junit(tests=31, skipped=31)}

    result = _just(stub, "fuzz", "-q")

    assert result.returncode != 0
    assert "0 Tests ausgeführt" in result.stderr
    [call] = _calls(stub)
    assert "--junitxml=" in call


def test_audit_runs_uv_audit_against_the_frozen_lock(stub: dict[str, str]) -> None:
    result = _just(stub, "audit")

    assert result.returncode == 0, result.stderr
    [call] = _calls(stub)
    assert call.startswith("args=audit --frozen rmq=")
    assert "lock=free" in call


@pytest.mark.parametrize("rc", [1, 2])
def test_audit_passes_the_exit_code_of_uv_audit_through(stub: dict[str, str], rc: int) -> None:
    stub["STUB_RC_none"] = str(rc)

    result = _just(stub, "audit")

    assert result.returncode == rc


def test_kosten_basislinie_measures_into_a_file_and_hands_reason_and_file_to_the_script(stub: dict[str, str]) -> None:
    stub["STUB_JUNIT"] = _junit(tests=6, skipped=0)

    result = _just(stub, "kosten-basislinie", "Grund E-026")

    assert result.returncode == 0, result.stderr
    messen, messen_valkey, schreiben = _calls(stub)
    assert "pytest tests/kosten -m kosten" in messen
    assert "lock=free" in messen
    # Der Valkey-Teil misst in dieselbe Datei, unter der Sperre und gegen die Testports.
    assert "pytest tests/kosten/valkey -m kosten_valkey" in messen_valkey
    assert "valkey=6399 lockheld=1 lock=held" in messen_valkey
    messung, messung_valkey = Path(stub["STUB_LOG"] + ".messung").read_text(encoding="utf-8").splitlines()
    assert messung_valkey == messung
    assert f"python scripts/kosten_basislinie.py Grund E-026 {messung} rmq=" in schreiben


def test_a_red_measurement_run_writes_no_baseline(stub: dict[str, str]) -> None:
    stub["STUB_RC_none"] = "1"

    result = _just(stub, "kosten-basislinie", "Grund E-026")

    assert result.returncode != 0
    [messen] = _calls(stub)
    assert "pytest tests/kosten -m kosten" in messen


def test_kosten_basislinie_writes_no_baseline_when_the_valkey_part_skipped_its_tests(stub: dict[str, str]) -> None:
    # Ohne Test-Valkey überspringt die Session-Fixture alle sechs Valkey-Kostentests (Exit 0).
    stub["STUB_JUNIT"] = _junit(tests=6, skipped=6)

    result = _just(stub, "kosten-basislinie", "Grund E-026")

    assert result.returncode != 0
    assert "0 Tests ausgeführt" in result.stderr
    _, messen_valkey = _calls(stub)  # kein dritter Aufruf: Das Skript schrieb nichts
    assert "pytest tests/kosten/valkey -m kosten_valkey" in messen_valkey


def test_vergleich_garantie_runs_the_guarantee_comparison_without_the_lock(stub: dict[str, str]) -> None:
    result = _just(stub, "vergleich-garantie", "--runs", "2")

    assert result.returncode == 0, result.stderr
    [call] = _calls(stub)
    # Eigener Stack mit eigenen Ports (stoerungen/compose.yml), deshalb keine Testsperre.
    assert call.startswith("args=run python -m stoerungen.garantie --runs 2 rmq=")
    assert "lock=free" in call
