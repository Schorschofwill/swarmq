"""Nachweis TS-04: Die xdist-Gruppe ``toxiproxy`` legt die Toxiproxy-Tests
nicht mehr auf einen Prozess; sie laufen parallel und stören sich.

Register: docs/befunde/teststrenge-2026-10.md (TS-04; Gegenprüfung
2026-10-31).

pytest-xdist 3.8.0 hängt die Gruppe an die Node-ID an
(``item._nodeid = f"{item.nodeid}@{group}"``, ``xdist/remote.py``), der
Scheduler ``loadgroup`` liest sie aus diesem Suffix. Seit pytest 9.2
(``uv.lock`` ab 868960b7) bildet ``Item.nodeid`` die ID aus ``_id``; die
Zuweisung an ``_nodeid`` wirkt nicht mehr, jede Gruppe fällt weg (auch
``metrics-ports``). ``test_xdist_isolation.py`` prüft nur den Marker, nicht die
Verteilung.

Erwartet: ``docs/testing/test-strategy.md`` (Toxiproxy-Tests „laufen unter
``--dist loadgroup`` nacheinander auf einem Prozess“), Docstring
``tests/support/xdist_isolation.py``, ``tests/integration/broker/conftest.py``.

Startet echte xdist-Prozesse und braucht deshalb über 100 ms (AGENTS.md:
lieber langsam und genau); ohne Dienste, darum unter ``tests/unit``.

Testliste:
# [ ] sechs Tests mit Fixture ``toxiproxy`` unter ``-n 2 --dist loadgroup`` -> alle auf einem Prozess
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

pytestmark = pytest.mark.timeout(60)

_PLUGIN = ("-p", "tests.support.xdist_isolation", "-o", "asyncio_default_fixture_loop_scope=function")

_PROXIED = """
import os
import pathlib

import pytest

@pytest.fixture
def toxiproxy():
    return None

@pytest.mark.parametrize("n", range(6))
def test_proxied(toxiproxy, n):
    with pathlib.Path(os.environ["TS04_OUT"]).open("a") as out:
        out.write(os.environ["PYTEST_XDIST_WORKER"] + "\\n")
"""


@pytest.mark.befund("TS-04")
@pytest.mark.xfail(strict=True, reason="TS-04: xdist 3.8 setzt _nodeid, pytest 9.2 liest _id")
def test_toxiproxy_users_run_on_one_xdist_worker(pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch) -> None:
    out = pytester.path / "workers.txt"
    monkeypatch.setenv("TS04_OUT", str(out))
    # Der Unterprozess startet im pytester-Verzeichnis; ``tests.support`` liegt im Repo.
    monkeypatch.setenv(
        "PYTHONPATH", os.pathsep.join([str(Path(__file__).parents[2]), os.environ.get("PYTHONPATH", "")])
    )
    pytester.makepyfile(_PROXIED)

    result = pytester.runpytest_subprocess(
        "-p", "xdist", "-n", "2", "--dist", "loadgroup", "-p", "no:randomly", *_PLUGIN
    )

    result.assert_outcomes(passed=6)
    workers = set(out.read_text().split())
    assert len(workers) == 1, f"toxiproxy tests spread over {sorted(workers)}"
