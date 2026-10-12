"""Unit-test conftest — fixtures that keep unit tests fast and isolated."""

from __future__ import annotations

# Task registry reset is handled by root conftest.py (_reset_task_registry).
import sys
import threading
import weakref
from pathlib import Path

import pytest

from swarmq.worker.compute.forkserver import SENTINEL_ENV
from tests.unit._log_capture import capture_swarmq_logs, preserved_root_logger

# Logging-Umgebungsvariablen, die swarmq auswertet (Plan 2026-10-06-0831, U4).
_SWARMQ_LOGGING_ENV_VARS = (
    "SWARMQ_LOG_LEVEL",
    "SWARMQ_LOG_JSON",
    "SWARMQ_LOG_FORMAT",
    "SWARMQ_LOG_CONFIG",
    "SWARMQ_CONFIGURE_LOGGING",
    "SWARMQ_LOGGER_LEVELS",
    "SWARMQ_LOG_QUEUE_SIZE",
    "SWARMQ_LOG_LIFECYCLE_LEVEL",
    "SWARMQ_LOG_QUEUE",
    "SWARMQ_LOG_DRAIN_TIMEOUT",
    SENTINEL_ENV,
)


@pytest.fixture(autouse=True)
def _clear_swarmq_logging_env(monkeypatch):
    """Plan 2026-10-06-0831, U4: Logging-Variablen aus der Shell des
    Entwicklers (oder aus CI) duerfen Unit-Tests nicht beeinflussen. Tests,
    die eine Variable brauchen, setzen sie selbst per ``monkeypatch.setenv``.
    """
    for name in _SWARMQ_LOGGING_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def swarmq_logs():
    """Erfasste swarmq-Logs als Liste von Dicts (siehe ``_log_capture``).

    Ersatz fuer ``structlog.testing.capture_logs``, seit die
    Bibliotheks-Logger ans Standard-``logging`` uebergeben (Plan
    2026-10-06-0831, U4).
    """
    with capture_swarmq_logs() as entries:
        yield entries


@pytest.fixture(autouse=True)
def _no_hard_exit_from_logging(monkeypatch):
    """Plan 2026-10-06-1811, U4: Haengt ein aufgegebener Listener-Thread
    eines Tests noch, wenn ein anderer Test einen Einstiegspunkt durchlaeuft,
    beendete ``exit_if_log_thread_stuck`` sonst den Testprozess. Stattdessen
    scheitert der Test."""

    def refuse(code: int) -> None:
        msg = f"unexpected hard exit({code}) from swarmq.logging_drain"
        raise AssertionError(msg)

    monkeypatch.setattr("swarmq.logging_drain._hard_exit", refuse)


@pytest.fixture(autouse=True)
def _fresh_abandoned_log_threads(monkeypatch):
    """Review B4: Aufgegebene Threads und steckende Streams sind
    Prozesszustand von ``swarmq.logging_drain``. Jeder Test beginnt ohne sie,
    damit keiner von den Resten eines frueheren abhaengt (etwa ein
    uebersprungenes ``flush`` oder ein vermeintlich haengender Thread)."""
    monkeypatch.setattr("swarmq.logging_drain._ABANDONED", [])
    monkeypatch.setattr("swarmq.logging_drain._STUCK_HANDLERS", weakref.WeakKeyDictionary())
    monkeypatch.setattr("swarmq.logging_drain._STUCK_STREAMS", set())
    monkeypatch.setattr("swarmq.logging_drain._STUCK_FILES", set())


@pytest.fixture(autouse=True)
def _fresh_fork_held_locks(monkeypatch):
    """Schluss-Review: Die vom ``before``-Fork-Hook gehaltenen Locks sind
    Prozesszustand von ``swarmq.logging_fork``. Jeder Test beginnt ohne die
    Reste eines frueheren (etwa eines Tests, der den Hook direkt ruft)."""
    monkeypatch.setattr("swarmq.logging_fork._HELD", threading.local())


@pytest.fixture(autouse=True)
def _restore_logging_state():
    """Gibt das Logging nach jedem Test frei und stellt Root wieder her
    (Plan 2026-10-06-0831, U4 Teil 2, U6).

    ``shutdown_logging()`` stoppt jeden Listener, den ein Test gestartet hat:
    Er schreibt aus einem eigenen Thread nach ``sys.stderr``, den ``capsys``
    beim Teardown schliesst -- die Folge waere ein ``ValueError: I/O
    operation on closed file`` als Rauschen in jedem Testlauf. Danach baut
    ein ``configure_logging`` im naechsten Test neu auf, und Root-Handler
    sowie Root-Level sind wieder im Ausgangszustand.
    """
    with preserved_root_logger():
        yield


@pytest.fixture(autouse=True)
def _no_real_redeliver_pause(monkeypatch):
    """Plan 0.12.0, MR 3 (BZ-29): Der Worker wartet vor jedem ``redeliver``
    bis zu 5 s. Unit-Tests warten nicht real: die Zeitquelle der Pause kehrt
    sofort zurueck. Tests der Pause selbst injizieren ihre eigene."""

    async def instant(_delay_s: float) -> None:
        return None

    monkeypatch.setattr("swarmq.worker.redelivery._sleep", instant)


def _is_below(file: str, roots: list[Path]) -> bool:
    path = Path(file).resolve()
    return any(path.is_relative_to(root) for root in roots)


@pytest.fixture(autouse=True)
def _restore_sys_path():
    """Plan 2026-10-12-1105, MR 6 (Review P1-13): ``main(["worker"|"inspect",
    ...])`` stellt das App-Verzeichnis an ``sys.path[0]``. Nach jedem Test gilt
    wieder der alte ``sys.path``.

    Aus ``sys.modules`` fallen nur Module, die waehrend des Tests aus einem
    neu hinzugekommenen Pfad geladen wurden und nicht unter einem schon
    vorhandenen liegen. ``swarmq.*``, ``tests.*`` und Bibliotheken bleiben:
    ein erneuter Import ergaebe doppelte Klassenobjekte.
    """
    saved_path = list(sys.path)
    saved_modules = set(sys.modules)
    yield
    added = [entry for entry in sys.path if entry and entry not in saved_path]
    sys.path[:] = saved_path
    if not added:
        return
    added_roots = [Path(entry).resolve() for entry in added]
    known_roots = [Path(entry).resolve() for entry in saved_path if entry]
    for name in set(sys.modules) - saved_modules:
        if name.partition(".")[0] in {"swarmq", "tests"}:
            continue
        file = getattr(sys.modules[name], "__file__", None)
        if file and _is_below(file, added_roots) and not _is_below(file, known_roots):
            del sys.modules[name]
