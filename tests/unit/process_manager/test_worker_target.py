"""Kindziel der Worker-Prozesse: Bindung an den Supervisor (``worker_target``).

Ohne Fork: ``ctypes.CDLL``, ``multiprocessing.parent_process`` und
``os.getppid`` sind gefälscht; ``prctl`` des Testprozesses bleibt unberührt.
Das echte Parent-Death-Signal prüft
``test_supervisor.py::TestProcessManagerCore::test_child_target_arms_the_parent_death_signal``.

Test-Liste (Testlücken Runde 4, U6):
- [x] T-F-26: Ist der Supervisor zwischen ``fork`` und ``prctl`` gestorben
      (Elternprozess nicht mehr der startende), endet das Kind mit Exit 0,
      ohne den Entry zu rufen
- [x] Lebt der startende Supervisor noch, läuft der Entry und sein Ergebnis
      kommt zurück
"""

from __future__ import annotations

import ctypes
import multiprocessing
import signal
from collections.abc import Iterator
from types import SimpleNamespace

import pytest

from swarmq.process_manager.worker_target import run_worker_entry

_SUPERVISOR_PID = 4242


class _FakeLibc:
    def prctl(self, *_args: int) -> int:
        return 0


@pytest.fixture
def _child_side(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Gefälschtes ``prctl`` und ein Elternprozess mit ``_SUPERVISOR_PID``;
    die Signalmaske des Testthreads wird danach wiederhergestellt."""
    monkeypatch.setattr(ctypes, "CDLL", lambda *_a, **_kw: _FakeLibc())
    monkeypatch.setattr(multiprocessing, "parent_process", lambda: SimpleNamespace(pid=_SUPERVISOR_PID))
    mask = signal.pthread_sigmask(signal.SIG_BLOCK, [])
    try:
        yield
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, mask)


@pytest.mark.usefixtures("_child_side")
def test_an_orphaned_child_exits_zero_without_running_the_entry(monkeypatch: pytest.MonkeyPatch) -> None:
    """T-F-26 (Mutant ``worker_target.py:64`` ``SystemExit(0)`` →
    ``SystemExit(1)``): Die Waise endet sauber; ein Exit ungleich 0 läse ein
    noch lebender Supervisor nicht, aber eine Orchestrierung als Absturz."""
    monkeypatch.setattr("swarmq.process_manager.worker_target.os.getppid", lambda: 1)  # reparented zu init
    calls: list[tuple[object, ...]] = []

    with pytest.raises(SystemExit) as exc_info:
        run_worker_entry(lambda *a: calls.append(a), "cfg")

    assert exc_info.value.code == 0
    assert calls == []


@pytest.mark.usefixtures("_child_side")
def test_a_child_of_the_living_supervisor_runs_the_entry(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("swarmq.process_manager.worker_target.os.getppid", lambda: _SUPERVISOR_PID)

    assert run_worker_entry(lambda *a: ("ran", *a), "cfg") == ("ran", "cfg")
