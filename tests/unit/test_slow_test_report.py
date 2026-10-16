"""Laufzeitbericht langsamer Unit-Tests (Plan 2026-10-13-0902, U5, KTD4).

Setup- und Call-Phasen über 100 ms erscheinen in einer Zusammenfassung und
in ``slow-unit-tests.json``; der Exit-Code bleibt unberührt. Geprüft mit
``pytester`` gegen das Plugin ``tests.support.slow_tests``.

Über 100 ms je Test, bewusst: Der Bericht misst echte Laufzeiten, die
inneren Tests müssen deshalb wirklich 150 ms schlafen. Sie brauchen keine
Dienste und bleiben darum Unit-Tests (AGENTS.md: Begründung im Test).

Test-Liste:
- [x] Test mit 150 ms Call-Phase: im Bericht und in der Datei, Exit-Code 0
- [x] langsame Fixture: Setup-Phase getrennt vom Call
- [x] Test mit 50 ms: nicht im Bericht
- [x] Lauf ohne Überschreitung: leere Liste, keine Zusammenfassungszeile
- [x] fehlschlagender langsamer Test: im Bericht, Exit-Code unverändert
- [x] unter aktiver Coverage-Messung: kein Bericht, keine Datei
- [x] langsamer Teardown: nicht im Bericht (R8 nennt Setup und Call)
- [x] serieller Durchgang (``SWARMQ_TEST_EXCLUSIVE=only``): behält die
      Einträge des parallelen Durchgangs und hängt seine an
"""

from __future__ import annotations

import json

import pytest

from tests.support.slow_tests import SlowPhase

pytestmark = pytest.mark.timeout(10)

# Der innere Lauf teilt den Prozess und damit ``filterwarnings = error``;
# ohne gesetzten Loop-Scope warnt pytest-asyncio beim Konfigurieren.
_PLUGIN = ("-p", "tests.support.slow_tests", "-o", "asyncio_default_fixture_loop_scope=function")


@pytest.fixture(autouse=True)
def _without_coverage(monkeypatch: pytest.MonkeyPatch) -> None:
    """Gemessen wird in der CI auch dieser Lauf; ``COVERAGE_RUN`` erbte der
    innere Lauf, und das Plugin schwiege in jedem Test."""
    monkeypatch.delenv("COVERAGE_RUN", raising=False)


def _report(pytester: pytest.Pytester) -> list[SlowPhase]:
    entries: list[SlowPhase] = json.loads((pytester.path / "slow-unit-tests.json").read_text(encoding="utf-8"))
    return entries


def test_a_slow_call_is_reported_without_changing_the_exit_code(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(
        """
        import time

        def test_slow():
            time.sleep(0.15)
        """
    )

    result = pytester.runpytest(*_PLUGIN)

    assert result.ret == 0
    result.stdout.fnmatch_lines(["*Unit-Tests über 100 ms*", "*call*test_slow*"])
    [entry] = _report(pytester)
    assert entry["nodeid"] == "test_a_slow_call_is_reported_without_changing_the_exit_code.py::test_slow"
    assert entry["phase"] == "call"
    assert entry["duration_s"] >= 0.15


def test_a_slow_fixture_is_reported_as_its_own_setup_phase(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(
        """
        import time

        import pytest

        @pytest.fixture
        def slow_resource():
            time.sleep(0.15)

        def test_fast_body(slow_resource):
            pass
        """
    )

    result = pytester.runpytest(*_PLUGIN)

    assert result.ret == 0
    assert [(e["nodeid"].split("::")[-1], e["phase"]) for e in _report(pytester)] == [("test_fast_body", "setup")]


def test_a_run_without_overruns_writes_an_empty_list_and_no_summary(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(
        """
        import time

        def test_quick():
            time.sleep(0.05)
        """
    )

    result = pytester.runpytest(*_PLUGIN)

    assert result.ret == 0
    assert _report(pytester) == []
    result.stdout.no_fnmatch_line("*Unit-Tests über 100 ms*")


def test_a_failing_slow_test_is_reported_and_keeps_its_exit_code(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(
        """
        import time

        def test_slow_and_broken():
            time.sleep(0.15)
            assert False
        """
    )

    result = pytester.runpytest(*_PLUGIN)

    assert result.ret == pytest.ExitCode.TESTS_FAILED
    assert [e["phase"] for e in _report(pytester)] == ["call"]


def test_under_coverage_nothing_is_reported(pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COVERAGE_RUN", "true")  # setzt ``coverage run`` für sich und alle Kinder
    pytester.makepyfile(
        """
        import time

        def test_slow():
            time.sleep(0.15)
        """
    )

    result = pytester.runpytest(*_PLUGIN)

    assert result.ret == 0
    assert not (pytester.path / "slow-unit-tests.json").exists()
    result.stdout.no_fnmatch_line("*Unit-Tests über 100 ms*")


def test_only_setup_and_call_phases_are_reported(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(
        """
        import time

        import pytest

        @pytest.fixture
        def slow_cleanup():
            yield
            time.sleep(0.15)

        def test_fast_body(slow_cleanup):
            pass
        """
    )

    pytester.runpytest(*_PLUGIN)

    assert _report(pytester) == []


def test_the_serial_pass_keeps_the_entries_of_the_parallel_pass(
    pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch
) -> None:
    earlier = SlowPhase(nodeid="test_parallel.py::test_slow", phase="call", duration_s=0.2)
    (pytester.path / "slow-unit-tests.json").write_text(json.dumps([earlier]), encoding="utf-8")
    monkeypatch.setenv("SWARMQ_TEST_EXCLUSIVE", "only")
    pytester.makepyfile(
        """
        import time

        def test_slow():
            time.sleep(0.15)
        """
    )

    pytester.runpytest(*_PLUGIN)

    first, second = _report(pytester)
    assert first == earlier
    assert second["nodeid"].endswith("::test_slow")
