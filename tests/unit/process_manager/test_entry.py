"""Subprozess-Entry-Tests (Plan U21) — verbatim."""

from __future__ import annotations

import os

import pytest

from swarmq.process_manager import (
    worker_subprocess_entry,
)
from tests.unit.process_manager._helpers import _make_test_cfg


class TestWorkerSubprocessEntry:
    """ce-review T-04: cover the spawn-boundary entry + shutdown watcher.

    The function runs inside a subprocess in production; the unit test
    drives the same code path in-process with the cross-process Event
    swapped for ``threading.Event`` (duck-type compatible).
    """

    def test_shutdown_event_triggers_sigterm_on_self(self, monkeypatch):
        """Worker ignores the event (simulating a stuck worker) — the
        watcher must still deliver SIGTERM so the Worker's own signal
        handler can drive graceful shutdown."""
        import asyncio
        import threading

        shutdown_event = threading.Event()
        kills: list[tuple[int, int]] = []
        worker_exited = threading.Event()

        def fake_kill(pid, sig):
            kills.append((pid, sig))
            # Simulate SIGTERM causing the worker to exit.
            worker_exited.set()

        async def fakerun_worker_async(ns):
            # Model a non-cooperative worker that does NOT consult
            # shutdown_event. Exit only when SIGTERM is "delivered".
            while not worker_exited.is_set():
                await asyncio.sleep(0.02)

        monkeypatch.setattr("swarmq.process_manager.supervisor.os.kill", fake_kill)
        monkeypatch.setattr(
            "swarmq.cli.run_worker_async", fakerun_worker_async,
        )

        timer = threading.Timer(0.1, shutdown_event.set)
        timer.start()
        try:
            worker_subprocess_entry(
                worker_id=0,
                shutdown_event=shutdown_event,
                cfg=_make_test_cfg(),
            )
        finally:
            timer.cancel()

        import signal as _signal
        assert kills, "shutdown watcher never called os.kill"
        pid, sig = kills[0]
        assert pid == os.getpid()
        assert sig == _signal.SIGTERM

    def test_keyboard_interrupt_returns_cleanly(self, monkeypatch):
        """KeyboardInterrupt from inside the worker must not propagate
        out of the entry — otherwise multiprocessing logs a traceback."""
        import threading

        async def raise_kbd(ns):
            raise KeyboardInterrupt

        monkeypatch.setattr("swarmq.cli.run_worker_async", raise_kbd)

        # Should return None, not raise.
        worker_subprocess_entry(
            worker_id=0,
            shutdown_event=threading.Event(),
            cfg=_make_test_cfg(),
        )

    def test_reload_and_watch_dirs_stripped_from_namespace(self, monkeypatch):
        """R5.3 ce-review #18: reload/watch_dirs are supervisor-only.
        The subprocess must never see them on its Namespace — otherwise
        a future `if ns.reload: ...` inside run_worker_async would
        spawn N watchers, one per worker, each killing its own PID."""
        import threading

        captured: dict = {}

        async def capture_ns(ns):
            captured["ns"] = ns

        monkeypatch.setattr("swarmq.cli.run_worker_async", capture_ns)

        cfg = _make_test_cfg(reload=True, watch_dirs=("/tmp",))
        worker_subprocess_entry(
            worker_id=0,
            shutdown_event=threading.Event(),
            cfg=cfg,
        )

        ns = captured["ns"]
        assert not hasattr(ns, "reload"), (
            "reload leaked to worker Namespace — "
            "kwargs.pop('reload') missing in worker_subprocess_entry"
        )
        assert not hasattr(ns, "watch_dirs"), (
            "watch_dirs leaked to worker Namespace"
        )


    def test_slot_reaches_the_worker_namespace(self, monkeypatch):
        """U2 (R3/KTD2): Der Supervisor kennt den Slot bereits und uebergibt
        ihn dem Kind — das Kind verwarf ihn bisher und nannte sich nach seiner
        Speicheradresse."""
        import threading

        captured: dict = {}

        async def capture_ns(ns):
            captured["ns"] = ns

        monkeypatch.setattr("swarmq.cli.run_worker_async", capture_ns)

        worker_subprocess_entry(
            worker_id=2,
            shutdown_event=threading.Event(),
            cfg=_make_test_cfg(),
        )

        assert captured["ns"].worker_slot == 2


def _capture_entry_logs(monkeypatch) -> list[dict]:
    """Faengt die Logsaetze des Entry ab.

    ``structlog.testing.capture_logs`` hilft hier nicht: das Entry ruft
    ``setup_logging`` und konfiguriert structlog dabei neu, womit die
    Capture-Kette wieder verschwindet. Das Format prueft stattdessen der
    Integrationstest mit echtem Subprozess (AE8/AE9).
    """
    from swarmq.process_manager import entry as entry_module

    events: list[dict] = []

    class _Recorder:
        def error(self, event, **kwargs):
            events.append({"event": event, **kwargs})

        def __getattr__(self, _name):
            return lambda *a, **kw: None

    monkeypatch.setattr(entry_module, "logger", _Recorder())
    return events


class TestConfigErrorExitCode:
    """R29 (Plan 2026-09-03-001): Konfigurationsfehler enden mit Exit 78."""

    def test_configuration_error_exits_with_config_code(self, monkeypatch):
        """Bewusst geaenderter Test (U4/R21): der Fehlertext erscheint als
        strukturierter Logsatz ``worker_config_error`` im konfigurierten
        Format, nicht als rohe stderr-Zeile. Eine rohe Zeile war in einer
        JSON-Logpipeline eine kaputte Zeile -- gerade im Fehlerfall, wo sie
        gelesen werden muss."""
        from swarmq.exceptions import ConfigurationError
        from swarmq.supervision import CONFIG_ERROR_EXIT_CODE

        async def boom(ns):
            raise ConfigurationError("FromDishka annotation(s) that never inject: t: run.db")

        monkeypatch.setattr("swarmq.cli.run_worker_async", boom)
        events = _capture_entry_logs(monkeypatch)
        shutdown_event = type("Ev", (), {"is_set": staticmethod(lambda: False)})()
        with pytest.raises(SystemExit) as exc_info:
            worker_subprocess_entry(0, shutdown_event, _make_test_cfg())
        assert exc_info.value.code == CONFIG_ERROR_EXIT_CODE == 78
        entry = next(e for e in events if e["event"] == "worker_config_error")
        assert entry["source"] == "worker"
        assert "never inject" in entry["error"]

    def test_import_error_in_the_child_is_a_configuration_error(self, monkeypatch):
        """R19: Eine fehlende Abhaengigkeit im Task-Modul endete mit Traceback
        und fuenf Neustarts -- der Supervisor sah einen Absturz, nicht einen
        Konfigurationsfehler."""
        from swarmq.supervision import CONFIG_ERROR_EXIT_CODE

        async def boom(ns):
            raise ModuleNotFoundError("No module named 'pandas'", name="pandas")

        monkeypatch.setattr("swarmq.cli.run_worker_async", boom)
        events = _capture_entry_logs(monkeypatch)
        shutdown_event = type("Ev", (), {"is_set": staticmethod(lambda: False)})()
        with pytest.raises(SystemExit) as exc_info:
            worker_subprocess_entry(0, shutdown_event, _make_test_cfg())
        assert exc_info.value.code == CONFIG_ERROR_EXIT_CODE
        entry = next(e for e in events if e["event"] == "worker_config_error")
        assert "pandas" in entry["error"]


class TestHeartbeatWriter:
    """R21/KTD10: der Entry schreibt das Lebenszeichen aus dem Event-Loop."""

    def test_channel_receives_beats_and_is_exposed(self, monkeypatch):
        import asyncio
        import multiprocessing

        from swarmq import supervision as sv

        ch = sv.SupervisorChannel.create(multiprocessing.get_context("spawn"))
        seen: list[object] = []

        async def fake_run(ns):
            seen.append(sv.current_channel())
            await asyncio.sleep(0.05)

        monkeypatch.setattr("swarmq.cli.run_worker_async", fake_run)
        shutdown_event = type("Ev", (), {"is_set": staticmethod(lambda: False)})()
        worker_subprocess_entry(0, shutdown_event, _make_test_cfg(heartbeat_interval_s=0.01), ch)
        assert ch.last_beat() > 0.0
        assert seen == [ch]

    def test_without_channel_nothing_is_written(self, monkeypatch):
        from swarmq import supervision as sv

        async def fake_run(ns):
            return None

        monkeypatch.setattr("swarmq.cli.run_worker_async", fake_run)
        shutdown_event = type("Ev", (), {"is_set": staticmethod(lambda: False)})()
        worker_subprocess_entry(0, shutdown_event, _make_test_cfg())
        assert sv.current_channel() is None


class TestRecycleAckTimeoutExitCode:
    def test_missing_ack_exits_with_tempfail_code(self, monkeypatch, capsys):
        from swarmq.supervision import RECYCLE_NOACK_EXIT_CODE, RecycleAckTimeout

        async def boom(ns):
            raise RecycleAckTimeout("no ack")

        monkeypatch.setattr("swarmq.cli.run_worker_async", boom)
        shutdown_event = type("Ev", (), {"is_set": staticmethod(lambda: False)})()
        with pytest.raises(SystemExit) as exc_info:
            worker_subprocess_entry(0, shutdown_event, _make_test_cfg())
        assert exc_info.value.code == RECYCLE_NOACK_EXIT_CODE == 75
        assert "no ack" in capsys.readouterr().err


class TestInheritedSignalMask:
    def test_entry_unblocks_sigterm_inherited_from_supervisor(self, monkeypatch):
        """Der Supervisor blockiert SIGTERM waehrend des Spawns; das Kind muss die
        Maske aufheben, sonst kommt sein eigenes SIGTERM nie an."""
        import signal

        async def fake_run(ns):
            return None

        monkeypatch.setattr("swarmq.cli.run_worker_async", fake_run)
        before = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGTERM})
        try:
            assert signal.SIGTERM in signal.pthread_sigmask(signal.SIG_BLOCK, set())
            shutdown_event = type("Ev", (), {"is_set": staticmethod(lambda: False)})()
            worker_subprocess_entry(0, shutdown_event, _make_test_cfg())
            assert signal.SIGTERM not in signal.pthread_sigmask(signal.SIG_BLOCK, set())
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, before)
