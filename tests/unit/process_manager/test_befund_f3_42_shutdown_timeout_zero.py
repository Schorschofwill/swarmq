"""Nachweis F3-42: ``SWARMQ_SHUTDOWN_TIMEOUT=0`` bricht ``swarmq worker`` mit
``ValueError`` ab.

Register: docs/befunde/fehlersuche-runde-3-2026-10.md (F3-42, Review U7).

``SwarmQConfig(shutdown_timeout=0)`` ist zulässig (E-018, Nachtrag
2026-10-15 (2): „sofort abbrechen … wird so auch genutzt“).
``run_multi_process_worker`` reicht den Wert als ``shutdown_grace_s=0.0`` an
``ProcessManager``, dessen Konstruktor bis 0.12.2 ``shutdown_grace_s <= 0``
mit ``ValueError`` ablehnte. Die CLI fängt ``ValueError`` am Worker-Pfad
nicht, der Start endete mit Traceback. Behoben in 0.12.3:
``ProcessManager`` nimmt 0 an; ``shutdown_budget(0)`` legt alle Phasen auf
0, der Supervisor schickt SIGTERM und SIGKILL ohne Wartezeit.

Testliste:
# [x] run_multi_process_worker mit SWARMQ_SHUTDOWN_TIMEOUT=0 erreicht
#     ProcessManager.run, shutdown_grace_s == 0.0
# [x] Herunterfahren mit shutdown_grace_s=0: SIGTERM und SIGKILL sofort,
#     ohne Wartezeit (virtuelle Uhr)
# [x] negatives shutdown_grace_s bleibt ValueError
# [x] Review cc1782e: shutdown_grace_s=0 mit --reload -- ein per SIGKILL
#     beendetes Kind, das nach join(timeout=0) noch lebt, bricht den Reload
#     nicht ab; reexec sammelt es begrenzt ein und ruft execvpe
"""

from __future__ import annotations

import argparse
import os
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from swarmq.process_manager import ProcessManager, WorkerInfo
from tests.unit.process_manager._helpers import _entry_stub


def _args() -> argparse.Namespace:
    return argparse.Namespace(
        module="json",  # stdlib, immer auflösbar
        queues=["default"],
        concurrency=1,
        log_level="INFO",
        log_format="human",
        burst=False,
        burst_idle_timeout=5.0,
        priorities=False,
        metrics_port=None,
        health_port=None,
        processes=1,
        autoscale=None,
        reload=False,
        watch_dir=None,
    )


@pytest.mark.befund("F3-42")
@pytest.mark.entscheidung("E-018")
def test_shutdown_timeout_zero_starts_the_supervisor() -> None:
    from swarmq.process_manager import run_multi_process_worker

    env = {
        "SWARMQ_BROKER_URL": "amqp://f3-42.invalid",
        "SWARMQ_BACKEND_URL": "valkey://f3-42.invalid",
        "SWARMQ_SHUTDOWN_TIMEOUT": "0",
    }
    seen: dict[str, float] = {}

    def _run(pm: ProcessManager) -> int:
        seen["grace"] = pm.shutdown_grace_s
        return 0

    with (
        patch.dict(os.environ, env, clear=False),
        patch.object(ProcessManager, "run", _run),
        patch("swarmq.process_manager.check._validate_module_importable"),
    ):
        rc = run_multi_process_worker(_args())

    assert rc == 0
    assert seen["grace"] == 0.0


@pytest.mark.entscheidung("E-018")
def test_zero_grace_escalates_to_sigkill_without_waiting(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = {"now": 0.0}

    def _sleep(seconds: float) -> None:
        clock["now"] += seconds

    monkeypatch.setattr(
        "swarmq.process_manager.supervisor.time",
        SimpleNamespace(monotonic=lambda: clock["now"], sleep=_sleep),
    )
    events: list[tuple[str, float]] = []
    proc = MagicMock()
    proc.pid = 7
    killed = {"done": False}
    proc.is_alive.side_effect = lambda: not killed["done"]
    proc.terminate.side_effect = lambda: events.append(("sigterm", clock["now"]))

    def _kill() -> None:
        events.append(("sigkill", clock["now"]))
        killed["done"] = True

    proc.kill.side_effect = _kill
    pm = ProcessManager(num_workers=1, worker_entry=_entry_stub, shutdown_grace_s=0)
    pm.workers = {7: WorkerInfo(process=proc, worker_id=0)}

    pm._shutdown_all_workers()

    assert events == [("sigterm", 0.0), ("sigkill", 0.0)]
    assert clock["now"] == 0.0


def test_negative_grace_is_still_rejected() -> None:
    with pytest.raises(ValueError, match="shutdown_grace_s must be >= 0"):
        ProcessManager(num_workers=1, worker_entry=_entry_stub, shutdown_grace_s=-1)


@pytest.mark.entscheidung("E-018")
def test_zero_grace_reload_reaps_killed_children_before_the_reexec() -> None:
    """Review cc1782e: Mit Budget 0 joint der Stopp mit ``timeout=0``; ein
    gerade per SIGKILL beendetes Kind ist dann oft noch nicht abgeraeumt.
    Der Orphan-Guard in ``reexec`` darf den Reload deshalb nicht verwerfen,
    sondern sammelt das Kind erst begrenzt ein."""
    proc = MagicMock()
    proc.pid = 7
    state = {"dead": False}
    proc.is_alive.side_effect = lambda: not state["dead"]
    joins: list[float | None] = []

    def _join(timeout: float | None = None) -> None:
        joins.append(timeout)
        if timeout is None or timeout > 0:  # der Kernel braucht Millisekunden
            state["dead"] = True

    proc.join.side_effect = _join
    pm = ProcessManager(
        num_workers=1,
        worker_entry=_entry_stub,
        shutdown_grace_s=0,
        reload=True,
        watch_dirs=("/tmp",),
    )
    pm._ctx = MagicMock()

    def _start_all() -> None:
        pm.workers = {7: WorkerInfo(process=proc, worker_id=0)}

    def _loop() -> int:
        pm._reload_requested = True  # Dateiaenderung: Reload beschlossen
        return 0

    with (
        patch.object(pm, "_start_all_workers", side_effect=_start_all),
        patch.object(pm, "_setup_signals"),
        patch.object(pm, "_monitor_loop", side_effect=_loop),
        patch("swarmq.hot_reload.FileWatcher"),
        patch("swarmq.process_manager.reload.shutdown_logging"),
        patch("swarmq.process_manager.reload.os.execvpe") as execvpe,
    ):
        pm.run()

    execvpe.assert_called_once()
    proc.kill.assert_called_once()
    last = joins[-1]
    assert last is not None
    assert 0 < last <= 2.0  # begrenzt
