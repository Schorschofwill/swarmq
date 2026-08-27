"""Subprozess-Entry-Tests (Plan U21) — verbatim."""

from __future__ import annotations

import os

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
