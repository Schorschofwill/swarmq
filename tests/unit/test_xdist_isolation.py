"""Parallele Service-Tests mit pytest-xdist (Beschleunigung Stufe 3).

Integration und e2e laufen auf mehreren xdist-Prozessen, jeder in eigener
vhost und Valkey-Datenbank (``tests/support/services.py``). Was der
Namensraum nicht trennt, regelt das Plugin ``tests.support.xdist_isolation``:
Tests mit festen Toxiproxy-Namen landen in einer xdist-Gruppe, und Tests,
die den ganzen Dienst verändern (Speicheralarm, Skript-Cache), tragen
``exclusive`` und laufen in einem zweiten, seriellen Durchgang.

Test-Liste:
- [x] SWARMQ_TEST_EXCLUSIVE=skip: exclusive-Tests abgewählt, andere laufen
- [x] SWARMQ_TEST_EXCLUSIVE=only: nur exclusive-Tests laufen
- [x] ohne Variable: alle Tests laufen
- [x] unbekannter Wert: Abbruch mit Nutzungsfehler
- [x] Test mit Fixture ``toxiproxy``: xdist-Gruppe ``toxiproxy``
- [x] eigene xdist-Gruppe bleibt erhalten
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.timeout(10)

_PLUGIN = ("-p", "tests.support.xdist_isolation", "-o", "asyncio_default_fixture_loop_scope=function")

_TWO_TESTS = """
import pytest

@pytest.mark.exclusive
def test_exclusive():
    pass

def test_shared():
    pass
"""


def test_skip_deselects_exclusive_tests(pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SWARMQ_TEST_EXCLUSIVE", "skip")
    pytester.makepyfile(_TWO_TESTS)

    result = pytester.runpytest("-v", *_PLUGIN)

    result.assert_outcomes(passed=1, deselected=1)
    result.stdout.fnmatch_lines(["*test_shared PASSED*"])


def test_only_keeps_just_the_exclusive_tests(pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SWARMQ_TEST_EXCLUSIVE", "only")
    pytester.makepyfile(_TWO_TESTS)

    result = pytester.runpytest("-v", *_PLUGIN)

    result.assert_outcomes(passed=1, deselected=1)
    result.stdout.fnmatch_lines(["*test_exclusive PASSED*"])


def test_without_the_variable_every_test_runs(pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SWARMQ_TEST_EXCLUSIVE", raising=False)
    pytester.makepyfile(_TWO_TESTS)

    result = pytester.runpytest(*_PLUGIN)

    result.assert_outcomes(passed=2)


def test_an_unknown_value_is_a_usage_error(pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SWARMQ_TEST_EXCLUSIVE", "onyl")
    pytester.makepyfile(_TWO_TESTS)

    result = pytester.runpytest(*_PLUGIN)

    assert result.ret == pytest.ExitCode.USAGE_ERROR
    result.stderr.fnmatch_lines(["*SWARMQ_TEST_EXCLUSIVE*onyl*"])


_GROUP_PROBE = """
import pytest

@pytest.fixture
def toxiproxy():
    return None

def _group(request):
    marker = request.node.get_closest_marker("xdist_group")
    return marker.args[0] if marker else None

def test_proxied(toxiproxy, request):
    assert _group(request) == "toxiproxy"

@pytest.mark.xdist_group("own")
def test_own_group(toxiproxy, request):
    assert _group(request) == "own"

def test_plain(request):
    assert _group(request) is None
"""


def test_toxiproxy_users_share_one_xdist_group(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(_GROUP_PROBE)

    result = pytester.runpytest("-p", "xdist", *_PLUGIN)

    result.assert_outcomes(passed=3)
