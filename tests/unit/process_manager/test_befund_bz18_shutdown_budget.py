"""Befund BZ-18: der Supervisor-Shutdown passt mit Default-Konfiguration nicht in 30 s.

Register: docs/befunde/betrieb-ueber-zeit-2026-10.md (BZ-18; E-7, F-6).

Kubernetes schickt nach ``terminationGracePeriodSeconds`` (Default 30 s)
SIGKILL an den Container, Docker/Compose nach 10 s. swarmq setzt
``shutdown_timeout`` = 30 s und gibt ihn dem Supervisor als Phase-1-Frist
(``process_manager/run.py``). Danach folgen SIGTERM + ``SHUTDOWN_TERM_S``
(5 s), SIGKILL und ``join(timeout=2)`` je Worker
(``supervisor.py`` ``_shutdown_all_workers``). Bei einem Worker, der seinen
Drain ausschöpft, endet der Supervisor-Shutdown also frühestens nach 35 s --
die eigene SIGTERM-/SIGKILL-Eskalation des Supervisors liegt komplett hinter
dem SIGKILL der Orchestrierung.

Erwartet: Mit Default-Konfiguration bleibt die Summe der Phasen unter der
Kubernetes-Default-Frist (30 s) -- oder swarmq warnt/validiert das Budget.

Zeit wird nicht abgewartet: ``time.monotonic``/``time.sleep`` des Supervisors
laufen über eine virtuelle Uhr, Worker-Prozesse sind Fakes, die erst auf
SIGKILL enden (Worker im Drain eines langen Tasks).

Test-Liste:
- [x] Default-Konfiguration, ein Worker im Drain -> Supervisor fertig (inkl.
      SIGKILL und Join) spätestens 30 s nach SIGTERM (xfail, BZ-18)
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.process_manager import ProcessManager, WorkerInfo
from tests.unit.process_manager._helpers import _entry_stub

_K8S_TERMINATION_GRACE_S = 30.0


class _VirtualTime:
    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def _draining_worker(clock: _VirtualTime, pid: int, events: list[tuple[str, float]]) -> MagicMock:
    """Ein Worker, der seinen Drain ausschoepft: endet erst auf SIGKILL."""
    proc = MagicMock()
    proc.pid = pid
    state = {"alive": True}
    proc.is_alive.side_effect = lambda: state["alive"]

    def _terminate() -> None:
        events.append(("sigterm", clock.now))

    def _kill() -> None:
        events.append(("sigkill", clock.now))
        state["alive"] = False

    def _join(timeout: float | None = None) -> None:
        if state["alive"] and timeout is not None:
            clock.now += timeout

    proc.terminate.side_effect = _terminate
    proc.kill.side_effect = _kill
    proc.join.side_effect = _join
    return proc


@pytest.mark.entscheidung("E-018")
def test_default_shutdown_fits_into_kubernetes_termination_grace(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = _VirtualTime()
    monkeypatch.setattr(
        "swarmq.process_manager.supervisor.time",
        SimpleNamespace(monotonic=clock.monotonic, sleep=clock.sleep),
    )
    # Wie ``process_manager/run.py``: Phase-1-Frist = SwarmQConfig.shutdown_timeout.
    defaults = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x")
    pm = ProcessManager(num_workers=1, worker_entry=_entry_stub, shutdown_grace_s=float(defaults.shutdown_timeout))
    events: list[tuple[str, float]] = []
    pm.workers = {101: WorkerInfo(process=_draining_worker(clock, 101, events), worker_id=0)}

    pm._shutdown_all_workers()  # t=0: SIGTERM vom Orchestrator ist eben angekommen

    assert clock.now <= _K8S_TERMINATION_GRACE_S, (
        f"Supervisor-Shutdown endet erst nach {clock.now:.1f} s (Phasen: {events}); "
        f"Kubernetes schickt SIGKILL nach {_K8S_TERMINATION_GRACE_S:.0f} s, "
        "die SIGTERM-/SIGKILL-Eskalation des Supervisors laeuft nie"
    )
