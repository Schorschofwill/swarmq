"""Nachweis BZ-27: Ein Worker-Slot bleibt nach 5 Abstuerzen pro Stunde dauerhaft gesperrt.

Register: docs/befunde/betrieb-ueber-zeit-2026-10.md (BZ-27).

Mechanismus: ``ProcessManager._maybe_restart`` sperrt den Slot ueber
``SlotTable.lock``; ``SlotTable.refresh`` hebt die Sperre nie auf, und das
Nachfuell-Ziel bleibt ``initial_workers - gesperrte Slots``. Nach Ablauf von
``RESTART_WINDOW`` ohne neue Abstuerze erwartet ein Betreiber, dass der Slot
wieder anlaeuft (Abkuehlzeit statt Dauersperre).

Testliste:
# [x] Slot nach Crash-Budget gesperrt; nach RESTART_WINDOW ohne Absturz
#     wird er wieder belegt (Monitor-Schleife mit Fake-Uhr, kein echter Prozess)
"""

from __future__ import annotations

import types
from unittest.mock import MagicMock

import pytest

from swarmq.process_manager import ProcessManager, WorkerInfo
from swarmq.process_manager import supervisor as supervisor_module
from swarmq.process_manager.slots import SlotState
from tests.unit.process_manager._helpers import FakeClock, _entry_stub, _fake_process, _register


@pytest.mark.befund("BZ-27")
@pytest.mark.xfail(
    strict=True,
    reason="BZ-27: Slot-Sperre nach MAX_RESTARTS Abstuerzen wird nach Ablauf von RESTART_WINDOW nie aufgehoben",
)
def test_locked_slot_is_refilled_after_the_restart_window(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = FakeClock()
    pm = ProcessManager(num_workers=2, worker_entry=_entry_stub)
    pm._ctx = MagicMock()
    healthy = WorkerInfo(process=_fake_process(alive=True, pid=1), worker_id=0)
    crashed = WorkerInfo(
        process=_fake_process(alive=False, exitcode=1, pid=2),
        worker_id=1,
        restart_timestamps=[clock.now - 10.0] * pm.MAX_RESTARTS,
    )
    _register(pm, healthy, crashed)

    ticks: list[SlotState] = []

    def fake_sleep(_seconds: float) -> None:
        # Nach jedem Monitor-Takt: Zustand merken, Uhr ueber das Fenster stellen.
        ticks.append(pm.slot_state(1))
        clock.now += pm.RESTART_WINDOW + 1.0
        if len(ticks) >= 3:
            pm._shutting_down = True

    monkeypatch.setattr(supervisor_module, "time", types.SimpleNamespace(monotonic=clock, sleep=fake_sleep))

    pm._monitor_loop()

    assert ticks[0] is SlotState.LOCKED, "Voraussetzung: Crash-Budget sperrt den Slot"
    assert pm.slot_state(1) is not SlotState.LOCKED, (
        f"Slot 1 nach {len(ticks) - 1} ruhigen Fenstern weiter gesperrt: {[s.value for s in ticks]}"
    )
    assert pm._ctx.Process.called, "kein Ersatzprozess fuer Slot 1 gestartet"
