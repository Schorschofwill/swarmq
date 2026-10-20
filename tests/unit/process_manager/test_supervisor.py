"""Supervisor-Tests (Plan U21) — verbatim: Restart-Budget, Autoscale,
Reload (execvpe mit eingefrorenem argv/env), Orphan-Guard, FileWatcher."""

# Test list (Plan 2026-10-06-0831, U9, R17):
# 1. ``autoscale_sample_failed`` traegt ``error_type`` und ein bereinigtes
#    ``error`` (keine Zugangsdaten).
# 2. Supervisor-Fehler-Events tragen ``error_type``/``error``.
# 3. KTD11: ``worker_died`` ist bei sauberem Ende (Exit 0) und bei
#    erwartetem Ende INFO (Lebenszyklus), bei einem Absturz WARNING.
# Review A6:
# 4. Ein Supervisor, der mit blockiertem SIGTERM/SIGINT startet (von
#    ``reexec`` ueber ``execve`` geerbt), gibt sie nach dem Einrichten der
#    Handler frei (Charakterisierung: ``run`` tat das schon).
# Restpunkte (Release 0.9.0):
# 5. Ein SIGTERM, das ein anderer Thread annimmt, dessen Handler also erst
#    nach der letzten Pruefung in ``reexec`` laeuft, bleibt ueber ``execve``
#    anhaengig, statt verloren zu gehen.
# Review cc1782e (E-057 unter --reload):
# 6. Der Hinweis zu Exit 78 unter --reload nennt Code oder Konfiguration und
#    SIGHUP als zweiten Ausloeser (ein Umgebungsfehler, E-005/E-016, heilt
#    keine Dateiaenderung).
# 7. SIGTERM waehrend des Wartens nach Exit 78 unter --reload: Ende mit 0,
#    kein Re-Exec.
# Testlücken Runde 4 (Plan 2026-10-20-1500, U6):
# 8. T-F-01: Ein übersprungener Worker (ohne Kanal, tot, schon in Beendigung,
#    Recycler) verdeckt keinen hängenden Worker dahinter.
# 9. T-F-02: Zwei überfällige ersetzte Worker: Jeder überlebende bekommt
#    SIGKILL, auch hinter einem bereits beendeten.
# 10. T-F-08: Eine Runde des Monitor-Loops mit Autoskalierer und Rückstau
#     skaliert hoch.
# 11. T-F-23: Autoskalierung mit min == max ist zulässig; ein verschwundener
#     Worker (ProcessLookupError) hält SIGTERM/SIGKILL an die übrigen nicht
#     auf; Worker starten im ``spawn``-Kontext.

from __future__ import annotations

import multiprocessing
import signal
import sys
import threading
import time
import types
from typing import Any, override
from unittest.mock import MagicMock, patch

import pytest

from swarmq.process_manager import (
    ProcessManager,
    WorkerInfo,
)
from swarmq.process_manager import reload as reload_module
from swarmq.process_manager import supervisor as supervisor_module
from swarmq.process_manager.reload import ReloadController
from swarmq.process_manager.slots import SlotState
from tests.unit.process_manager._helpers import (
    FakeClock,
    FakeProbe,
    FakeProbeFactory,
    _entry_stub,
    _fake_process,
    _instant_crash_entry,
    _make_test_cfg,
    _register,
    _report_pdeathsig_entry,
    as_mock,
    channel_of,
)


class TestProcessManagerCore:
    def test_rejects_invalid_autoscale(self):
        with pytest.raises(ValueError, match="min >= 1"):
            ProcessManager(
                num_workers=1,
                worker_entry=_entry_stub,
                autoscale=(0, 3),
                queue_depth_fn=lambda: 0,
            )
        with pytest.raises(ValueError, match="min >= 1"):
            ProcessManager(
                num_workers=1,
                worker_entry=_entry_stub,
                autoscale=(5, 3),
                queue_depth_fn=lambda: 0,
            )

    def test_autoscale_accepts_equal_bounds(self):
        """T-F-23 (Mutant ``supervisor.py:129`` ``max_w < min_w`` → ``<=``):
        ``--autoscale 2:2`` ist eine feste Zahl, kein Fehler."""
        pm = ProcessManager(
            num_workers=1,
            worker_entry=_entry_stub,
            autoscale=(2, 2),
            queue_depth_fn=lambda: 0,
        )
        assert pm.initial_workers == 2

    def test_workers_start_in_the_spawn_context(self):
        """T-F-23 (Mutant ``supervisor.py:169`` ``get_context("spawn")`` →
        ``get_context("fork")``): Der Supervisor hat Threads (Datei-Wächter,
        Log-Queue); ein ``fork`` daraus kann im Kind an einer geerbten Sperre
        hängen. Der Kontext ist nur über ``pm._ctx`` sichtbar, einen
        öffentlichen Weg ohne echte Kindprozesse gibt es nicht."""
        pm = ProcessManager(num_workers=1, worker_entry=_entry_stub)
        assert pm._ctx.get_start_method() == "spawn"

    def test_autoscale_requires_queue_depth_fn(self):
        with pytest.raises(ValueError, match="queue_depth_fn"):
            ProcessManager(
                num_workers=1,
                worker_entry=_entry_stub,
                autoscale=(1, 3),
            )

    def test_autoscale_clamps_initial_count(self):
        pm = ProcessManager(
            num_workers=10,
            worker_entry=_entry_stub,
            autoscale=(1, 3),
            queue_depth_fn=lambda: 0,
        )
        assert pm.initial_workers == 3

        pm2 = ProcessManager(
            num_workers=0,
            worker_entry=_entry_stub,
            autoscale=(2, 5),
            queue_depth_fn=lambda: 0,
        )
        assert pm2.initial_workers == 2

    def test_start_worker_registers_and_spawns(self):
        pm = ProcessManager(num_workers=1, worker_entry=_entry_stub)
        fake = _fake_process(pid=9001)
        pm._ctx = MagicMock()
        pm._ctx.Process = MagicMock(return_value=fake)
        info = pm._start_worker(0)
        assert info.worker_id == 0
        assert 9001 in pm.workers
        fake.start.assert_called_once()

    def test_start_worker_unblocks_inherited_signals_before_the_entry(self):
        """B15 (Loadsuite-Testlauf v0.6.0): Der Supervisor blockiert
        SIGINT/SIGTERM/SIGHUP, waehrend er Kinder startet; ``spawn`` vererbt
        die Maske. swarmqs eigener Entry hob sie auf, ein fremder Entry
        erbte sie -- sein SIGTERM blieb anhaengig, der Container endete erst
        per SIGKILL nach dem Stop-Timeout. Das Ziel des Kindprozesses hebt
        die Maske darum selbst auf, bevor irgendein Entry laeuft."""
        seen: dict[str, Any] = {}

        def entry(worker_id, shutdown_event, *args):
            seen["mask"] = signal.pthread_sigmask(signal.SIG_BLOCK, set())
            seen["args"] = (worker_id, shutdown_event, *args)

        pm = ProcessManager(num_workers=1, worker_entry=entry, worker_args=("cfg",))
        pm._ctx = MagicMock()
        pm._ctx.Process = MagicMock(return_value=_fake_process(pid=9002))
        pm._start_worker(0)
        kwargs = pm._ctx.Process.call_args.kwargs

        blocked = {signal.SIGINT, signal.SIGTERM, signal.SIGHUP}
        before = signal.pthread_sigmask(signal.SIG_BLOCK, blocked)
        try:
            kwargs["target"](*kwargs["args"])  # so laeuft es im Kind
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, before)

        assert not (blocked & set(seen["mask"]))
        assert seen["args"][0] == 0
        assert seen["args"][1] is pm.shutdown_event
        assert seen["args"][2] == "cfg"

    @pytest.mark.skipif(sys.platform != "linux", reason="PR_SET_PDEATHSIG gibt es nur unter Linux")
    def test_child_target_arms_the_parent_death_signal(self):
        """Ein Worker-Kind stirbt mit seinem Supervisor -- auch bei dessen SIGKILL.

        Bis hierher blieb ein Kind, dessen Supervisor per SIGKILL endete (etwa
        ein Test-Teardown mit ``proc.kill()``), als Waise am Leben, hielt den
        Scheduler-Leader-Lock per Heartbeat und liess spaetere Leader-Tests
        scheitern (Notiz e2e-scheduler-leader-flake). Das Kindziel setzt darum
        ``PR_SET_PDEATHSIG`` auf SIGTERM: der Elterntod loest denselben
        Graceful-Shutdown aus wie ein geordnetes Herunterfahren.
        """
        from swarmq.process_manager.worker_target import run_worker_entry

        ctx = multiprocessing.get_context("spawn")
        parent_conn, child_conn = ctx.Pipe(duplex=False)
        proc = ctx.Process(target=run_worker_entry, args=(_report_pdeathsig_entry, child_conn))
        proc.start()
        try:
            assert parent_conn.poll(30), "das Kind meldete sein Parent-Death-Signal nicht"
            assert parent_conn.recv() == int(signal.SIGTERM)
        finally:
            proc.join(timeout=10)
            if proc.is_alive():
                proc.kill()

    def test_start_all_workers_spawns_n(self):
        pm = ProcessManager(num_workers=3, worker_entry=_entry_stub)
        pm._ctx = MagicMock()
        pm._ctx.Process = MagicMock(
            side_effect=[
                _fake_process(pid=1),
                _fake_process(pid=2),
                _fake_process(pid=3),
            ],
        )
        pm._start_all_workers()
        assert len(pm.workers) == 3

    def test_shutdown_phase1_only_when_workers_exit_cleanly(self):
        pm = ProcessManager(num_workers=2, worker_entry=_entry_stub)
        pm.shutdown_grace_s = 0.5
        pm.SHUTDOWN_TERM_S = 0.5
        p1 = _fake_process(alive=False, pid=1)
        p2 = _fake_process(alive=False, pid=2)
        pm.workers = {
            1: WorkerInfo(process=p1, worker_id=0),
            2: WorkerInfo(process=p2, worker_id=1),
        }
        pm._shutdown_all_workers()
        # No SIGTERM/SIGKILL because workers already dead.
        p1.terminate.assert_not_called()
        p1.kill.assert_not_called()

    def test_shutdown_escalates_to_sigterm_then_sigkill(self):
        pm = ProcessManager(num_workers=1, worker_entry=_entry_stub)
        pm.shutdown_grace_s = 0.01
        pm.SHUTDOWN_TERM_S = 0.01
        p = _fake_process(alive=True, pid=1)
        pm.workers = {1: WorkerInfo(process=p, worker_id=0)}
        pm._shutdown_all_workers()
        p.terminate.assert_called_once()
        p.kill.assert_called_once()

    def test_a_vanished_worker_does_not_stop_the_signals_to_the_others(self, monkeypatch):
        """T-F-23 (Mutant ``supervisor.py:417`` ``continue`` → ``break``):
        Ist ein Worker schon weg (``ProcessLookupError``), bekommen die
        übrigen trotzdem SIGTERM und SIGKILL. Virtuelle Uhr statt 30 s."""
        clock = FakeClock()

        def advance(seconds: float) -> None:
            clock.now += seconds

        monkeypatch.setattr(supervisor_module, "time", types.SimpleNamespace(monotonic=clock, sleep=advance))
        pm = ProcessManager(num_workers=2, worker_entry=_entry_stub)
        vanished = _fake_process(pid=1)
        vanished.terminate.side_effect = ProcessLookupError
        vanished.kill.side_effect = ProcessLookupError
        stuck = _fake_process(pid=2)
        pm.workers = {
            1: WorkerInfo(process=vanished, worker_id=0),
            2: WorkerInfo(process=stuck, worker_id=1),
        }
        pm._shutdown_all_workers()
        stuck.terminate.assert_called_once()
        stuck.kill.assert_called_once()

    def test_setup_signals_does_not_install_sigchld(self):
        """Härtung R2 P1-1: ein eigener SIGCHLD-``waitpid(-1)``-Reaper
        raced gegen multiprocessing's eigenes ``waitpid(pid)`` —
        gewinnt der Handler, liefert ``Popen.poll()`` ECHILD →
        ``is_alive()`` bleibt für tote Worker dauerhaft True.
        Reaping gehört ausschließlich der multiprocessing-API."""
        pm = ProcessManager(num_workers=1, worker_entry=_entry_stub)
        previous = {
            s: signal.getsignal(s)
            for s in (
                signal.SIGINT,
                signal.SIGTERM,
                signal.SIGHUP,
                signal.SIGCHLD,
            )
        }
        try:
            pm._setup_signals()
            assert signal.getsignal(signal.SIGCHLD) is previous[signal.SIGCHLD]
        finally:
            for s, handler in previous.items():
                signal.signal(s, handler)


class TestProcessManagerRestart:
    def _pm(self) -> ProcessManager:
        pm = ProcessManager(num_workers=1, worker_entry=_entry_stub)
        pm._ctx = MagicMock()
        return pm

    def test_maybe_restart_spawns_new_worker(self):
        pm = self._pm()
        as_mock(pm._ctx).Process = MagicMock(return_value=_fake_process(pid=77))
        old = WorkerInfo(
            process=_fake_process(alive=False, pid=1),
            worker_id=0,
        )
        pm._maybe_restart(old)
        assert 77 in pm.workers

    def test_restart_limit_exceeded_stops_restarts(self):
        """ce-review C-01: rolling-window blocks on MAX_RESTARTS
        timestamps inside the window."""
        pm = self._pm()
        as_mock(pm._ctx).Process = MagicMock(return_value=_fake_process(pid=99))
        now = time.monotonic()
        # MAX_RESTARTS recent timestamps already in the window.
        old = WorkerInfo(
            process=_fake_process(alive=False, pid=1),
            worker_id=0,
            restart_timestamps=[now - 1.0] * pm.MAX_RESTARTS,
        )
        pm._maybe_restart(old)
        # No spawn — the history is already at the cap.
        assert len(pm.workers) == 0

    def test_restart_last_slot_at_limit_minus_one_succeeds(self):
        """Exact-boundary: 4 timestamps in window, the 5th should
        still be allowed (count reaches but does not exceed MAX)."""
        pm = self._pm()
        as_mock(pm._ctx).Process = MagicMock(return_value=_fake_process(pid=88))
        now = time.monotonic()
        old = WorkerInfo(
            process=_fake_process(alive=False, pid=1),
            worker_id=0,
            restart_timestamps=[now - 10.0] * (pm.MAX_RESTARTS - 1),
        )
        pm._maybe_restart(old)
        assert 88 in pm.workers
        assert len(pm.workers[88].restart_timestamps) == pm.MAX_RESTARTS

    def test_restart_counter_prunes_stale_entries(self):
        """Entries older than RESTART_WINDOW are dropped, freeing
        restart budget without a full counter reset."""
        pm = self._pm()
        as_mock(pm._ctx).Process = MagicMock(return_value=_fake_process(pid=42))
        now = time.monotonic()
        # All entries older than the window → treated as if fresh.
        old = WorkerInfo(
            process=_fake_process(alive=False, pid=1),
            worker_id=0,
            restart_timestamps=[now - pm.RESTART_WINDOW - 1 for _ in range(10)],
        )
        pm._maybe_restart(old)
        # Pruning dropped all 10; new restart allowed.
        assert 42 in pm.workers
        assert len(pm.workers[42].restart_timestamps) == 1

    def test_rolling_window_rejects_late_crash_after_burst(self):
        """ce-review C-01 regression: 5 crashes inside the window
        then a 6th at t+3601 (outside the old single-gap logic but
        still sliding-window-violating) must still block until
        the oldest entry ages out."""
        pm = self._pm()
        as_mock(pm._ctx).Process = MagicMock(return_value=_fake_process(pid=55))
        now = time.monotonic()
        # Five restarts spread across the window — oldest at (now-100).
        ts = [now - 100.0 + i for i in range(pm.MAX_RESTARTS)]
        old = WorkerInfo(
            process=_fake_process(alive=False, pid=1),
            worker_id=0,
            restart_timestamps=ts,
        )
        pm._maybe_restart(old)
        # All 5 entries still inside the 3600s window → blocked.
        assert len(pm.workers) == 0

    def test_check_dead_workers_skips_expected_shutdown(self):
        pm = self._pm()
        dead = _fake_process(alive=False, exitcode=15, pid=5)
        info = WorkerInfo(process=dead, worker_id=0, expected_shutdown=True)
        pm.workers = {5: info}
        pm._check_dead_workers()
        # No restart because scale-down flagged it.
        assert len(pm.workers) == 0

    def test_check_dead_workers_skips_clean_exit(self):
        pm = self._pm()
        dead = _fake_process(alive=False, exitcode=0, pid=5)
        pm.workers = {5: WorkerInfo(process=dead, worker_id=0)}
        pm._check_dead_workers()
        assert len(pm.workers) == 0  # no restart

    def test_check_dead_workers_restarts_crashed(self):
        pm = self._pm()
        as_mock(pm._ctx).Process = MagicMock(return_value=_fake_process(pid=500))
        dead = _fake_process(alive=False, exitcode=-11, pid=5)  # segfault
        pm.workers = {5: WorkerInfo(process=dead, worker_id=0)}
        pm._check_dead_workers()
        assert 500 in pm.workers

    def test_sighup_only_marks_the_reload_pending(self):
        """U8 (KTD8): Der Handler prueft nichts und stoppt nichts; die Probe
        startet erst der Monitor-Loop."""
        pm = self._pm()
        factory = FakeProbeFactory()
        pm.reloader = ReloadController("/etc/log.json", probe_factory=factory)
        pm._on_reload_signal(signal.SIGHUP, None)
        assert pm.reloader.pending is True
        assert factory.raws == []
        assert pm._reload_requested is False
        assert pm._shutting_down is False
        assert not pm.shutdown_event.is_set()

    def test_sighup_after_sigterm_is_ignored(self):
        """Szenario E (S6): SIGTERM, 50 ms spaeter SIGHUP -- der Supervisor
        endet, statt neu zu starten."""
        pm = self._pm()
        pm._on_shutdown_signal(signal.SIGTERM, None)
        pm._on_reload_signal(signal.SIGHUP, None)
        assert pm.reloader.pending is False
        assert pm.reloader.step() is False

    def test_sigterm_sets_shutdown_flag_no_reload(self):
        pm = self._pm()
        pm._on_shutdown_signal(signal.SIGTERM, None)
        assert pm._shutting_down is True
        assert pm._reload_requested is False

    def test_sigterm_cancels_a_pending_and_an_accepted_reload(self):
        """KTD8 (R10): SIGTERM beendet den Supervisor immer -- ein
        ausstehender oder schon beschlossener Reload faellt weg."""
        pm = self._pm()
        pm.reloader.request()
        pm._reload_requested = True
        pm._on_shutdown_signal(signal.SIGTERM, None)
        assert pm.reloader.pending is False
        assert pm._reload_requested is False
        assert pm._shutting_down is True

    def test_the_shutdown_signal_is_logged_by_the_supervisor_not_the_handler(self, swarmq_logs):
        """KTD8: Loggen aus dem Signal-Handler kann am Queue-Mutex deadlocken;
        der Handler hinterlegt das Event, ``run`` schreibt es."""
        pm = ProcessManager(num_workers=0, worker_entry=_entry_stub)
        pm._ctx = MagicMock()
        pm._on_shutdown_signal(signal.SIGTERM, None)
        assert [e for e in swarmq_logs if e["event"] == "process_manager_shutdown_signal"] == []
        with (
            patch.object(pm, "_start_all_workers"),
            patch.object(pm, "_setup_signals"),
            patch.object(pm, "_shutdown_all_workers"),
            patch.object(pm, "_collect_zombies"),
        ):
            assert pm.run() == 0
        (event,) = [e for e in swarmq_logs if e["event"] == "process_manager_shutdown_signal"]
        assert event["signal"] == "SIGTERM"

    def test_run_triggers_execvp_on_reload(self):
        pm = ProcessManager(num_workers=0, worker_entry=_entry_stub)
        pm._ctx = MagicMock()
        pm._reload_requested = True
        # Skip all worker management; we only verify execvp is called.
        with (
            patch.object(pm, "_start_all_workers"),
            patch.object(pm, "_setup_signals"),
            patch.object(pm, "_monitor_loop", return_value=0),
            patch.object(pm, "_shutdown_all_workers"),
            patch.object(pm, "_collect_zombies"),
            patch("swarmq.process_manager.supervisor.os.execvpe") as execvp,
        ):
            pm.run()
            execvp.assert_called_once()

    def test_a_supervisor_started_with_blocked_shutdown_signals_unblocks_them(self):
        """Review A6: ``reexec`` blockiert SIGTERM/SIGINT ueber ``execve``
        hinweg. Der neue Supervisor muss sie nach dem Einrichten der Handler
        freigeben, sonst bliebe SIGTERM fuer immer anhaengig."""
        seen: list[set[int | signal.Signals]] = []

        def record_mask() -> int:
            seen.append(signal.pthread_sigmask(signal.SIG_BLOCK, set()))
            return 0

        pm = ProcessManager(num_workers=0, worker_entry=_entry_stub)
        pm._ctx = MagicMock()
        before = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGTERM, signal.SIGINT})
        try:
            with (
                patch.object(pm, "_start_all_workers"),
                patch.object(pm, "_setup_signals"),
                patch.object(
                    pm,
                    "_monitor_loop",
                    side_effect=record_mask,
                ),
                patch.object(pm, "_shutdown_all_workers"),
                patch.object(pm, "_collect_zombies"),
            ):
                pm.run()
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, before)

        [mask] = seen
        assert not ({signal.SIGTERM, signal.SIGINT} & set(mask))

    def test_a_sigterm_handled_after_the_last_check_stays_pending_across_execve(self):
        pm = ProcessManager(num_workers=0, worker_entry=_entry_stub)
        pm._ctx = MagicMock()
        pm._reload_requested = True
        pending: list[set[signal.Signals]] = []

        def execvpe(*_args: object) -> None:
            # Ein anderer Thread hat das Signal angenommen; der Handler laeuft
            # im Hauptthread erst nach der Pruefung, direkt vor ``execve``.
            pm._on_shutdown_signal(signal.SIGTERM, None)
            pending.append(signal.sigpending())

        received: list[int] = []
        old_handler = signal.signal(signal.SIGTERM, lambda signum, _frame: received.append(signum))
        old_mask = signal.pthread_sigmask(signal.SIG_BLOCK, [])
        try:
            with (
                patch("swarmq.process_manager.reload.shutdown_logging"),
                patch("swarmq.process_manager.reload.os.execvpe", side_effect=execvpe),
            ):
                reload_module.reexec(pm, 0)
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, old_mask)
            signal.signal(signal.SIGTERM, old_handler)

        [held] = pending
        assert signal.SIGTERM in held

    def test_reload_writes_the_log_queue_before_execvpe(self):
        """U7 (R15): ``process_manager_reload_execvp`` ist geschrieben, bevor
        ``execvpe`` das Prozessabbild ersetzt -- danach schreibt kein
        Listener-Thread mehr."""
        calls: list[str] = []
        pm = ProcessManager(num_workers=0, worker_entry=_entry_stub)
        pm._ctx = MagicMock()
        pm._reload_requested = True
        log = MagicMock()
        log.info.side_effect = lambda event, **kw: calls.append(event)
        with (
            patch.object(pm, "_start_all_workers"),
            patch.object(pm, "_setup_signals"),
            patch.object(pm, "_monitor_loop", return_value=0),
            patch.object(pm, "_shutdown_all_workers"),
            patch.object(pm, "_collect_zombies"),
            patch("swarmq.process_manager.reload.logger", log),
            patch(
                "swarmq.process_manager.reload.shutdown_logging",
                side_effect=lambda: calls.append("shutdown_logging"),
            ),
            patch(
                "swarmq.process_manager.supervisor.os.execvpe",
                side_effect=lambda *a: calls.append("execvpe"),
            ),
        ):
            pm.run()
        assert calls[-3:] == ["process_manager_reload_execvp", "shutdown_logging", "execvpe"]

    def test_failed_execvpe_logs_error_fields_and_exits_one(self, swarmq_logs):
        pm = ProcessManager(num_workers=0, worker_entry=_entry_stub)
        pm._ctx = MagicMock()
        pm._reload_requested = True
        with (
            patch.object(pm, "_start_all_workers"),
            patch.object(pm, "_setup_signals"),
            patch.object(pm, "_monitor_loop", return_value=0),
            patch.object(pm, "_shutdown_all_workers"),
            patch.object(pm, "_collect_zombies"),
            patch("swarmq.process_manager.reload.shutdown_logging"),
            patch(
                "swarmq.process_manager.supervisor.os.execvpe",
                side_effect=PermissionError("python not executable"),
            ),
        ):
            assert pm.run() == 1
        [entry] = [e for e in swarmq_logs if e["event"] == "reload_execvp_failed"]
        assert entry["log_level"] == "error"
        assert entry["error_type"] == "PermissionError"
        assert entry["error"] == "python not executable"

    def test_file_watcher_stop_failure_logs_error_fields(self, swarmq_logs):
        pm = ProcessManager(num_workers=0, worker_entry=_entry_stub)
        pm._ctx = MagicMock()
        pm._file_watcher = MagicMock()
        pm._file_watcher.stop.side_effect = RuntimeError("inotify stuck")
        with (
            patch.object(pm, "_start_all_workers"),
            patch.object(pm, "_setup_signals"),
            patch.object(pm, "_monitor_loop", return_value=0),
            patch.object(pm, "_shutdown_all_workers"),
            patch.object(pm, "_collect_zombies"),
        ):
            assert pm.run() == 0
        [entry] = [e for e in swarmq_logs if e["event"] == "file_watcher_stop_failed"]
        assert entry["log_level"] == "warning"
        assert entry["error_type"] == "RuntimeError"
        assert entry["error"] == "inotify stuck"


class TestCrashLoopExhaustion:
    """P0-1: ``_monitor_loop``-Backfill spawnte nach erschöpftem
    Restart-Limit frische Worker mit leerer ``restart_timestamps``-
    Historie → endloser Spawn-Sturm. Erschöpfte Slots müssen das
    Backfill-Ziel reduzieren; sind alle Slots erschöpft, beendet
    sich der Supervisor mit Exit-Code ≠ 0 (systemd/k8s übernimmt
    mit eigenem Backoff)."""

    def _pm(self, n: int = 1) -> ProcessManager:
        pm = ProcessManager(num_workers=n, worker_entry=_entry_stub)
        pm._ctx = MagicMock()
        return pm

    def test_maybe_restart_limit_marks_slot_exhausted(self):
        pm = self._pm()
        now = time.monotonic()
        old = WorkerInfo(
            process=_fake_process(alive=False, pid=1),
            worker_id=0,
            restart_timestamps=[now - 1.0] * pm.MAX_RESTARTS,
        )
        pm._maybe_restart(old)
        assert pm._exhausted_slots == 1

    def test_current_target_excludes_exhausted_slots(self):
        pm = self._pm(n=3)
        pm._slots.lock(1)
        pm._slots.lock(2)
        assert pm._current_target() == 1

    def test_current_target_never_negative(self):
        pm = self._pm(n=1)
        pm._slots.lock(0)
        pm._clean_exits = 1
        assert pm._current_target() == 0

    def test_autoscale_target_unaffected_by_exhausted_slots(self):
        """Autoscale-Pfad: Ziel bleibt len(workers) — Scale-up ist
        nachfragegetrieben mit Cooldown, kein Backfill-Bypass."""
        pm = ProcessManager(
            num_workers=2,
            worker_entry=_entry_stub,
            autoscale=(1, 3),
            queue_depth_fn=lambda: 0,
        )
        pm._ctx = MagicMock()
        pm._slots.lock(2)
        _register(pm, WorkerInfo(process=_fake_process(pid=1), worker_id=0))
        assert pm._current_target() == 1

    def test_monitor_loop_exits_nonzero_when_all_slots_exhausted(self):
        pm = self._pm(n=2)
        pm.MONITOR_INTERVAL = 0.01
        pm._slots.lock(0)
        pm._slots.lock(1)
        pm.workers = {}
        rc = pm._monitor_loop()
        assert rc != 0

    def test_monitor_loop_keeps_running_degraded(self):
        """Teilweise erschöpft (1 von 2) + 1 lebender Worker →
        Loop läuft weiter (degraded), kein Exit, kein Backfill."""
        pm = self._pm(n=2)
        pm.MONITOR_INTERVAL = 0.01
        pm._slots.lock(1)
        _register(pm, WorkerInfo(process=_fake_process(pid=1), worker_id=0))

        import threading

        result: dict[str, int] = {}
        t = threading.Thread(
            target=lambda: result.update(rc=pm._monitor_loop()),
            daemon=True,
        )
        t.start()
        time.sleep(0.1)
        still_running = t.is_alive()
        pm._shutting_down = True
        t.join(timeout=2)
        assert still_running, "loop must keep supervising the live worker"
        # Kein Backfill über das reduzierte Ziel hinaus gespawnt.
        as_mock(pm._ctx).Process.assert_not_called()

    def test_crash_storm_bounded_and_manager_exits_real_processes(self):
        """Regression mit ECHTEN Subprozessen (kein gemockter
        ``_monitor_loop``): instant-crashender Worker → genau
        1 + MAX_RESTARTS Spawns, dann Exit ≠ 0. Vor dem Fix lief
        das endlos (Backfill mit frischer Historie)."""
        import threading

        pm = ProcessManager(
            num_workers=1,
            worker_entry=_instant_crash_entry,
        )
        pm.MAX_RESTARTS = 2
        pm.MONITOR_INTERVAL = 0.05
        spawned_slots: list[int] = []
        real_start = pm._start_worker

        def counting_start(slot: int) -> WorkerInfo:
            spawned_slots.append(slot)
            return real_start(slot)

        pm._start_worker = counting_start  # type: ignore[method-assign]
        pm._start_all_workers()

        result: dict[str, int] = {}
        t = threading.Thread(
            target=lambda: result.update(rc=pm._monitor_loop()),
            daemon=True,
        )
        t.start()
        t.join(timeout=60)
        if t.is_alive():
            pm._shutting_down = True
            t.join(timeout=5)
            pytest.fail(
                f"monitor loop did not exit after slot exhaustion (spawned {len(spawned_slots)} workers)",
            )
        try:
            assert result["rc"] != 0
            # initial (1) + MAX_RESTARTS Ersatz-Spawns — kein Sturm.
            assert len(spawned_slots) <= 1 + pm.MAX_RESTARTS
            # U1: jeder Ersatz traegt den Slot des Vorgaengers, der am Ende gesperrt ist.
            assert set(spawned_slots) == {0}
            assert pm.slot_state(0) is SlotState.LOCKED
        finally:
            pm._shutdown_all_workers()
            pm._collect_zombies()


class TestProcessManagerAutoscale:
    def _pm(self, depth_values: list[int], min_w: int = 1, max_w: int = 5):
        """Build a PM whose queue_depth_fn returns successive values."""
        it = iter(depth_values)

        def depth_fn() -> int:
            return next(it, 0)

        pm = ProcessManager(
            num_workers=2,
            worker_entry=_entry_stub,
            autoscale=(min_w, max_w),
            queue_depth_fn=depth_fn,
        )
        pm._ctx = MagicMock()
        pm.AUTOSCALE_SAMPLE_INTERVAL = 0.0  # always tick in tests
        pm.SCALE_COOLDOWN_S = 0.0
        pm.SCALE_DOWN_IDLE_S = 0.0
        # ce-review Unit 5: default 30s would defer every scale-down
        # in tests (WorkerInfo.spawned_at ≈ now). Zero it so scale
        # scenarios exercise the decision logic, not the age gate.
        pm.SCALE_MIN_AGE_S = 0.0
        return pm

    def test_sample_failure_logs_sanitized_error_fields(self, swarmq_logs):
        pm = self._pm(depth_values=[])

        def failing_depth() -> int:
            raise ConnectionError("amqp://user:s3cret@rabbit/vh")

        pm._queue_depth_fn = failing_depth
        pm._autoscale_tick()
        [entry] = [e for e in swarmq_logs if e["event"] == "autoscale_sample_failed"]
        assert entry["log_level"] == "warning"
        assert entry["error_type"] == "ConnectionError"
        assert entry["error"] == "amqp://user:***@rabbit/vh"

    def test_scale_up_when_depth_exceeds_threshold(self):
        pm = self._pm(depth_values=[500], min_w=1, max_w=5)
        pm._ctx.Process = MagicMock(return_value=_fake_process(pid=10))
        # Two existing workers; threshold 50/each → 100. Depth 500 > 100.
        _register(
            pm,
            WorkerInfo(process=_fake_process(pid=1), worker_id=0),
            WorkerInfo(process=_fake_process(pid=2), worker_id=1),
        )
        pm._autoscale_tick()
        assert 10 in pm.workers
        assert len(pm.workers) == 3

    def test_scale_up_blocked_at_max(self):
        pm = self._pm(depth_values=[500], min_w=1, max_w=2)
        pm._ctx.Process = MagicMock()
        _register(
            pm,
            WorkerInfo(process=_fake_process(pid=1), worker_id=0),
            WorkerInfo(process=_fake_process(pid=2), worker_id=1),
        )
        pm._autoscale_tick()
        pm._ctx.Process.assert_not_called()

    def test_scale_down_when_idle_above_min(self):
        pm = self._pm(depth_values=[0, 0], min_w=1, max_w=5)
        p1 = _fake_process(pid=1)
        p2 = _fake_process(pid=2)
        _register(
            pm,
            WorkerInfo(process=p1, worker_id=0),
            WorkerInfo(process=p2, worker_id=1),
        )
        # First tick: sets idle_since.
        pm._autoscale_tick()
        # Second tick: idle window elapsed (0.0) → terminate youngest.
        pm._autoscale_tick()
        p2.terminate.assert_called_once()
        # Youngest was flagged expected_shutdown.
        assert pm.workers[2].expected_shutdown is True

    def test_scale_down_blocked_at_min(self):
        pm = self._pm(depth_values=[0, 0], min_w=2, max_w=5)
        p1 = _fake_process(pid=1)
        p2 = _fake_process(pid=2)
        _register(
            pm,
            WorkerInfo(process=p1, worker_id=0),
            WorkerInfo(process=p2, worker_id=1),
        )
        pm._autoscale_tick()
        pm._autoscale_tick()
        p1.terminate.assert_not_called()
        p2.terminate.assert_not_called()

    def test_sample_interval_gates_calls(self):
        calls = [0]

        def depth_fn():
            calls[0] += 1
            return 0

        pm = ProcessManager(
            num_workers=1,
            worker_entry=_entry_stub,
            autoscale=(1, 3),
            queue_depth_fn=depth_fn,
        )
        pm.AUTOSCALE_SAMPLE_INTERVAL = 999.0  # effectively disable re-entry
        pm._autoscale_tick()
        pm._autoscale_tick()
        pm._autoscale_tick()
        assert calls[0] == 1

    def test_cooldown_blocks_consecutive_scales(self):
        pm = self._pm(depth_values=[500, 500], min_w=1, max_w=5)
        pm._ctx.Process = MagicMock(
            side_effect=[_fake_process(pid=10), _fake_process(pid=11)],
        )
        pm.SCALE_COOLDOWN_S = 999.0  # effectively forbid second scale
        _register(
            pm,
            WorkerInfo(process=_fake_process(pid=1), worker_id=0),
        )
        pm._autoscale_tick()  # scales up, records decision time
        assert len(pm.workers) == 2
        pm._autoscale_tick()  # cooldown blocks
        assert len(pm.workers) == 2

    def test_depth_fn_exception_is_swallowed(self):
        def boom():
            raise RuntimeError("broker gone")

        pm = ProcessManager(
            num_workers=1,
            worker_entry=_entry_stub,
            autoscale=(1, 3),
            queue_depth_fn=boom,
        )
        pm.AUTOSCALE_SAMPLE_INTERVAL = 0.0
        pm._ctx = MagicMock()
        _register(
            pm,
            WorkerInfo(process=_fake_process(pid=1), worker_id=0),
        )
        # Should not raise.
        pm._autoscale_tick()

    def test_non_idle_resets_idle_timer(self):
        depths = iter([0, 10, 0])  # idle → load → idle
        pm = ProcessManager(
            num_workers=2,
            worker_entry=_entry_stub,
            autoscale=(1, 5),
            queue_depth_fn=lambda: next(depths, 0),
        )
        pm.AUTOSCALE_SAMPLE_INTERVAL = 0.0
        pm.SCALE_COOLDOWN_S = 0.0
        pm.SCALE_DOWN_IDLE_S = 999.0  # long idle requirement
        pm._ctx = MagicMock()
        p2 = _fake_process(pid=2)
        _register(
            pm,
            WorkerInfo(process=_fake_process(pid=1), worker_id=0),
            WorkerInfo(process=p2, worker_id=1),
        )
        pm._autoscale_tick()  # idle → sets idle_since
        assert pm._idle_since is not None
        pm._autoscale_tick()  # load → resets
        assert pm._idle_since is None


class TestReloadOrphanGuard:
    """ce-review REL-03 / Unit 4: execvp only when no children survive."""

    def _base_pm(self) -> ProcessManager:
        pm = ProcessManager(num_workers=0, worker_entry=_entry_stub)
        pm._ctx = MagicMock()
        pm._reload_requested = True
        return pm

    def test_execvp_called_when_no_alive_children(self):
        """Happy path: workers dict empty → guard passes → execvp fires."""
        pm = self._base_pm()
        with (
            patch.object(pm, "_start_all_workers"),
            patch.object(pm, "_setup_signals"),
            patch.object(pm, "_monitor_loop", return_value=0),
            patch.object(pm, "_shutdown_all_workers"),
            patch.object(pm, "_collect_zombies"),
            patch("swarmq.process_manager.supervisor.os.execvpe") as execvp,
        ):
            pm.run()
            execvp.assert_called_once()

    def test_execvp_blocked_when_alive_child_remains(self):
        """Guard fires: live worker after shutdown → WARNING + return 0."""
        pm = self._base_pm()
        survivor = _fake_process(alive=True, pid=999)
        pm.workers = {999: WorkerInfo(process=survivor, worker_id=0)}
        with (
            patch.object(pm, "_start_all_workers"),
            patch.object(pm, "_setup_signals"),
            patch.object(pm, "_monitor_loop", return_value=0),
            patch.object(pm, "_shutdown_all_workers"),
            patch.object(pm, "_collect_zombies"),
            patch("swarmq.process_manager.supervisor.os.execvpe") as execvp,
        ):
            rc = pm.run()
            execvp.assert_not_called()
            assert rc == 0  # clean exit — systemd must not restart

    def test_execvp_oserror_surfaces_return_1(self):
        """If execvp itself fails (bad sys.executable), return 1 with
        ERROR log — not a raw traceback."""
        pm = self._base_pm()
        with (
            patch.object(pm, "_start_all_workers"),
            patch.object(pm, "_setup_signals"),
            patch.object(pm, "_monitor_loop", return_value=0),
            patch.object(pm, "_shutdown_all_workers"),
            patch.object(pm, "_collect_zombies"),
            patch("swarmq.process_manager.supervisor.os.execvpe", side_effect=OSError("permission denied")),
        ):
            rc = pm.run()
            assert rc == 1


class TestArgvSnapshot:
    """ce-review SEC-01 / Unit 6: argv frozen at __init__."""

    def test_argv_captured_on_init(self, monkeypatch):
        import sys as _sys

        monkeypatch.setattr(
            _sys,
            "orig_argv",
            ["python", "/venv/bin/swarmq", "worker", "--module=x"],
        )
        pm = ProcessManager(num_workers=1, worker_entry=_entry_stub)
        # orig_argv[0] (das Interpreter-Wort) ersetzt der Re-Exec durch sys.executable.
        assert pm._startup_argv == ["/venv/bin/swarmq", "worker", "--module=x"]

    def test_sys_argv_mutation_does_not_affect_snapshot(self, monkeypatch):
        """Post-__init__ mutation of sys.orig_argv must NOT be visible in
        the snapshot used by execvp."""
        import sys as _sys

        monkeypatch.setattr(_sys, "orig_argv", ["python", "swarmq", "worker"])
        pm = ProcessManager(num_workers=1, worker_entry=_entry_stub)
        _sys.orig_argv[1] = "mutated"
        _sys.orig_argv.append("--evil")
        assert pm._startup_argv == ["swarmq", "worker"]

    def test_execvp_uses_snapshot_not_live_argv(self, monkeypatch):
        import sys as _sys

        monkeypatch.setattr(
            _sys,
            "orig_argv",
            ["python", "swarmq", "worker", "--module=safe"],
        )
        pm = ProcessManager(num_workers=0, worker_entry=_entry_stub)
        pm._ctx = MagicMock()
        pm._reload_requested = True
        # Mutate sys.orig_argv between __init__ and run():
        _sys.orig_argv.append("--injected")
        with (
            patch.object(pm, "_start_all_workers"),
            patch.object(pm, "_setup_signals"),
            patch.object(pm, "_monitor_loop", return_value=0),
            patch.object(pm, "_shutdown_all_workers"),
            patch.object(pm, "_collect_zombies"),
            patch("swarmq.process_manager.supervisor.os.execvpe") as execvpe,
        ):
            pm.run()
            execvpe.assert_called_once()
            _, argv_used, _ = execvpe.call_args.args
            # argv is [sys.executable, *snapshot] — not live sys.orig_argv.
            assert "--injected" not in argv_used
            assert "--module=safe" in argv_used

    def test_execvpe_keeps_python_m_and_interpreter_flags(self, monkeypatch):
        """Unter ``python -m swarmq.cli`` steht in sys.argv[0] der Pfad von
        __main__.py -- der Re-Exec muss die Original-Kommandozeile nehmen,
        sonst laeuft das neue Abbild die Datei als Skript (cwd fehlt auf
        sys.path, Spawn-Kinder scheitern im Bootstrapping)."""
        import sys as _sys

        monkeypatch.setattr(
            _sys,
            "orig_argv",
            ["python", "-X", "dev", "-m", "swarmq.cli", "worker", "--module=safe"],
        )
        monkeypatch.setattr(
            _sys,
            "argv",
            ["/venv/swarmq/cli/__main__.py", "worker", "--module=safe"],
        )
        pm = ProcessManager(num_workers=0, worker_entry=_entry_stub)
        pm._ctx = MagicMock()
        pm._reload_requested = True
        with (
            patch.object(pm, "_start_all_workers"),
            patch.object(pm, "_setup_signals"),
            patch.object(pm, "_monitor_loop", return_value=0),
            patch.object(pm, "_shutdown_all_workers"),
            patch.object(pm, "_collect_zombies"),
            patch("swarmq.process_manager.supervisor.os.execvpe") as execvpe,
        ):
            pm.run()
            path_used, argv_used, _ = execvpe.call_args.args
            assert path_used == _sys.executable
            assert argv_used == [
                _sys.executable,
                "-X",
                "dev",
                "-m",
                "swarmq.cli",
                "worker",
                "--module=safe",
            ]

    def test_env_snapshot_captured_on_init(self, monkeypatch):
        """ce-review SEC-ENV-01: os.environ is snapshotted at init."""
        monkeypatch.setenv("SWARMQ_TEST_ENV", "initial")
        pm = ProcessManager(num_workers=1, worker_entry=_entry_stub)
        assert pm._startup_env.get("SWARMQ_TEST_ENV") == "initial"
        # Mutation after __init__ does not bleed into the snapshot.
        monkeypatch.setenv("SWARMQ_TEST_ENV", "mutated")
        assert pm._startup_env.get("SWARMQ_TEST_ENV") == "initial"

    def test_execvpe_uses_env_snapshot(self, monkeypatch):
        """Reload uses captured env, not live os.environ."""
        monkeypatch.setenv("PYTHONPATH", "/safe/path")
        pm = ProcessManager(num_workers=0, worker_entry=_entry_stub)
        pm._ctx = MagicMock()
        pm._reload_requested = True
        # Attacker-like mutation after init.
        monkeypatch.setenv("PYTHONPATH", "/tmp/evil:/safe/path")
        with (
            patch.object(pm, "_start_all_workers"),
            patch.object(pm, "_setup_signals"),
            patch.object(pm, "_monitor_loop", return_value=0),
            patch.object(pm, "_shutdown_all_workers"),
            patch.object(pm, "_collect_zombies"),
            patch("swarmq.process_manager.supervisor.os.execvpe") as execvpe,
        ):
            pm.run()
            _, _, env_used = execvpe.call_args.args
            assert env_used.get("PYTHONPATH") == "/safe/path"
            assert "/tmp/evil" not in env_used.get("PYTHONPATH", "")


class TestScaleDownMinAge:
    """ce-review C-03 / Unit 5: age-gate on scale-down candidates."""

    def _pm_with_age(self, min_age_s: float) -> ProcessManager:
        pm = ProcessManager(
            num_workers=3,
            worker_entry=_entry_stub,
            autoscale=(1, 5),
            queue_depth_fn=lambda: 0,
        )
        pm._ctx = MagicMock()
        pm.SCALE_MIN_AGE_S = min_age_s
        return pm

    def test_young_worker_not_selected(self):
        """All workers < SCALE_MIN_AGE_S → _scale_down_one is no-op."""
        pm = self._pm_with_age(min_age_s=30.0)
        now = time.monotonic()
        # All spawned <1s ago
        p1 = _fake_process(pid=1)
        p2 = _fake_process(pid=2)
        _register(
            pm,
            WorkerInfo(process=p1, worker_id=0, spawned_at=now - 0.5),
            WorkerInfo(process=p2, worker_id=1, spawned_at=now - 0.2),
        )
        pm._scale_down_one()
        p1.terminate.assert_not_called()
        p2.terminate.assert_not_called()

    def test_old_worker_selected(self):
        pm = self._pm_with_age(min_age_s=30.0)
        now = time.monotonic()
        p1 = _fake_process(pid=1)
        _register(
            pm,
            WorkerInfo(process=p1, worker_id=0, spawned_at=now - 120.0),
        )
        pm._scale_down_one()
        p1.terminate.assert_called_once()

    def test_mix_selects_old_youngest_eligible(self):
        """2 old + 1 young; youngest of the eligible set wins (latest
        spawned_at among ages >= threshold, U1)."""
        pm = self._pm_with_age(min_age_s=30.0)
        now = time.monotonic()
        p_old1 = _fake_process(pid=1)  # worker_id=0, age 120s
        p_old2 = _fake_process(pid=2)  # worker_id=1, age 60s
        p_young = _fake_process(pid=3)  # worker_id=2, age 5s
        _register(
            pm,
            WorkerInfo(process=p_old1, worker_id=0, spawned_at=now - 120.0),
            WorkerInfo(process=p_old2, worker_id=1, spawned_at=now - 60.0),
            WorkerInfo(process=p_young, worker_id=2, spawned_at=now - 5.0),
        )
        pm._scale_down_one()
        # Expect worker_id=1 (highest id among eligible) — NOT the young one.
        p_old1.terminate.assert_not_called()
        p_old2.terminate.assert_called_once()
        p_young.terminate.assert_not_called()


class TestReloadReentrancy:
    """R5.3 ce-review #10, seit U8 (KTD8): echte SIGHUP-Zustellung. Der
    Handler merkt den Reload nur vor; das ``busy`` des Reload-Controllers,
    das der FileWatcher liest, kippt sofort."""

    @pytest.mark.usefixtures("_restore_default_signals")
    def test_real_sighup_marks_the_reload_pending_and_busy(
        self,
    ):
        """Main-thread SIGHUP runs the handler synchronously (CPython
        delivers pending signals between bytecodes). An diesen Thread, nicht
        an den Prozess: Unter xdist nimmt sonst womoeglich ein anderer Thread
        das Signal an, und der Handler laeuft erst nach den Assertions (wie
        ``adf31e5``)."""
        pm = ProcessManager(
            num_workers=0,
            worker_entry=_entry_stub,
            reload=True,
            watch_dirs=("/tmp",),
        )
        pm._setup_signals()  # installs the real handler

        busy_before = pm.reloader.busy
        assert busy_before is False
        signal.pthread_kill(threading.get_ident(), signal.SIGHUP)
        assert pm.reloader.pending is True
        assert pm.reloader.busy is True
        assert pm._shutting_down is False

    @pytest.mark.usefixtures("_restore_default_signals")
    def test_second_sighup_while_busy_is_idempotent(
        self,
    ):
        """Double-fire raises no exception and keeps exactly one pending
        reload (KTD8: zusammengefasst). An diesen Thread, nicht an den
        Prozess (siehe oben)."""
        pm = ProcessManager(
            num_workers=0,
            worker_entry=_entry_stub,
            reload=True,
            watch_dirs=("/tmp",),
        )
        pm._setup_signals()

        signal.pthread_kill(threading.get_ident(), signal.SIGHUP)
        signal.pthread_kill(threading.get_ident(), signal.SIGHUP)
        assert pm.reloader.pending is True
        assert pm._reload_requested is False
        assert pm.shutdown_event.is_set() is False


class TestFileWatcherIntegration:
    """R5.3 Unit 6: ProcessManager wiring of the optional FileWatcher."""

    def _pm(self, **kw) -> ProcessManager:
        return ProcessManager(num_workers=0, worker_entry=_entry_stub, **kw)

    def test_reload_disabled_keeps_watcher_none(self):
        pm = self._pm()
        assert pm._reload is False
        assert pm._file_watcher is None

    def test_reload_enabled_stores_watch_dirs(self):
        pm = self._pm(reload=True, watch_dirs=("/a", "/b"))
        assert pm._reload is True
        assert pm._watch_dirs == ("/a", "/b")

    def test_run_starts_and_stops_watcher(self):
        """With --reload active, the supervisor must arm the watcher
        between _setup_signals and _monitor_loop, and stop it in
        finally — regardless of _monitor_loop's return code.

        R5.3 ce-review #8: also verifies that FileWatcher is constructed
        with the correct kwargs — watch_dirs matches pm._watch_dirs, the
        is_busy closure actually reads pm._reload_requested, and the
        trigger callable fires SIGHUP at the current PID. A regression
        wiring a static False is_busy or a wrong signal/PID would be
        invisible to the original call-count-only assertions."""
        import os as _os

        pm = self._pm(reload=True, watch_dirs=("/tmp",))
        fake_watcher = MagicMock()

        # R5.3 ce-review #8: patch os.kill BEFORE pm.run() so the
        # trigger closure (built inside run()) captures the mock.
        # Patching only around trigger() call-time misses the lambda
        # binding and would fire a real SIGHUP at the test process.
        with (
            patch.object(pm, "_start_all_workers"),
            patch.object(pm, "_setup_signals"),
            patch.object(pm, "_monitor_loop", return_value=0),
            patch.object(pm, "_shutdown_all_workers"),
            patch.object(pm, "_collect_zombies"),
            patch(
                "swarmq.hot_reload.FileWatcher",
                return_value=fake_watcher,
            ) as fw_cls,
            patch("swarmq.process_manager.supervisor.os.kill") as mock_kill,
        ):
            pm.run()

            fake_watcher.start.assert_called_once()
            fake_watcher.stop.assert_called_once()

            # Kwarg identity — not just call counts (ce-review #8).
            kwargs = fw_cls.call_args.kwargs
            assert kwargs["watch_dirs"] == ("/tmp",)
            is_busy = kwargs["is_busy"]
            trigger = kwargs["trigger"]
            assert callable(is_busy)
            assert callable(trigger)

            # is_busy reads pm._reload_requested live — static-False
            # closure would fail this.
            pm._reload_requested = False
            assert is_busy() is False
            pm._reload_requested = True
            assert is_busy() is True
            # U8 (KTD8): auch ein ausstehender Reload bzw. eine laufende
            # Probe zaehlt -- sonst loeste jede Dateiaenderung waehrend der
            # Probe ein weiteres SIGHUP aus.
            pm._reload_requested = False
            pm.reloader.request()
            assert is_busy() is True

            # trigger fires SIGHUP at own PID — wrong signal or wrong
            # PID would fail this.
            trigger()
            mock_kill.assert_called_once()
            call_args, _ = mock_kill.call_args
            assert call_args[0] == _os.getpid()
            assert call_args[1] == signal.SIGHUP

    def test_run_without_reload_never_imports_watcher(self):
        """Smoke-test: with reload=False, FileWatcher must never be
        constructed — the optional-dep path stays cold."""
        pm = self._pm(reload=False)
        with (
            patch.object(pm, "_start_all_workers"),
            patch.object(pm, "_setup_signals"),
            patch.object(pm, "_monitor_loop", return_value=0),
            patch.object(pm, "_shutdown_all_workers"),
            patch.object(pm, "_collect_zombies"),
            patch("swarmq.hot_reload.FileWatcher") as FW,
        ):
            pm.run()
        FW.assert_not_called()

    def test_run_stops_watcher_even_when_monitor_raises(self):
        pm = self._pm(reload=True, watch_dirs=("/tmp",))
        fake_watcher = MagicMock()
        with (
            patch.object(pm, "_start_all_workers"),
            patch.object(pm, "_setup_signals"),
            patch.object(
                pm,
                "_monitor_loop",
                side_effect=RuntimeError("monitor bug"),
            ),
            patch.object(pm, "_shutdown_all_workers"),
            patch.object(pm, "_collect_zombies"),
            patch(
                "swarmq.hot_reload.FileWatcher",
                return_value=fake_watcher,
            ),
            pytest.raises(RuntimeError, match="monitor bug"),
        ):
            pm.run()
        fake_watcher.stop.assert_called_once()


class TestConfigErrorNoRestart:
    """R29 (Plan 2026-09-03-001): Exit 78 wird nicht neu gestartet.

    U4 (Plan 2026-09-08-001, R20) zieht den Exit-Code nach: der Supervisor
    endet selbst mit 78 statt mit 1. Eine Orchestrierung unterscheidet damit
    "nicht wieder anfassen" von "gleich nochmal" -- ``RestartPreventExitStatus=78``
    greift jetzt auch fuer den Elternprozess."""

    def _pm(self):
        pm = ProcessManager(num_workers=1, worker_entry=_entry_stub, worker_args=(_make_test_cfg(),))
        pm._ctx = MagicMock()
        return pm

    def test_config_error_exit_is_not_restarted(self):
        from swarmq.process_manager.entry import CONFIG_ERROR_EXIT_CODE

        pm = self._pm()
        pm._ctx.Process = MagicMock(return_value=_fake_process(pid=500))
        dead = _fake_process(alive=False, exitcode=CONFIG_ERROR_EXIT_CODE, pid=5)
        pm.workers = {5: WorkerInfo(process=dead, worker_id=0)}
        pm._check_dead_workers()
        assert pm.workers == {}
        assert pm._config_error is True
        pm._ctx.Process.assert_not_called()

    def test_monitor_loop_exits_seventy_eight_after_config_error(self, monkeypatch):
        """Bewusst geaenderter Test (U4/R20): vorher 1, jetzt 78."""
        from swarmq.supervision import CONFIG_ERROR_EXIT_CODE

        pm = self._pm()
        dead = _fake_process(alive=False, exitcode=CONFIG_ERROR_EXIT_CODE, pid=5)
        pm.workers = {5: WorkerInfo(process=dead, worker_id=0)}
        monkeypatch.setattr(pm, "_shutdown_all_workers", MagicMock())
        assert pm._monitor_loop() == CONFIG_ERROR_EXIT_CODE
        pm._shutdown_all_workers.assert_called_once()

    def test_recycle_noack_exit_is_logged_and_restarted(self, swarmq_logs):
        """R20: Exit 75 bleibt ein Neustart im Budget, bekommt aber einen
        eigenen Logsatz -- vorher war er vom gewoehnlichen Absturz nicht zu
        unterscheiden."""
        from swarmq.supervision import RECYCLE_NOACK_EXIT_CODE

        pm = self._pm()
        pm._ctx.Process = MagicMock(return_value=_fake_process(pid=500))
        dead = _fake_process(alive=False, exitcode=RECYCLE_NOACK_EXIT_CODE, pid=5)
        pm.workers = {5: WorkerInfo(process=dead, worker_id=0)}
        pm._check_dead_workers()
        assert "worker_recycle_noack" in [entry["event"] for entry in swarmq_logs]
        assert 500 in pm.workers  # Neustart im Budget wie bisher
        assert pm._config_error is False

    def test_a_rejected_fork_source_exit_is_logged_and_restarted(self, swarmq_logs):
        """Schluss-Review: Exit 71 (verworfene Kindquelle des CPU-Pools) ist
        ein Neustart im Budget -- mit frischem Forkserver -- und bekommt
        einen eigenen Logsatz."""
        from swarmq.supervision import FORK_SOURCE_REJECTED_EXIT_CODE

        pm = self._pm()
        pm._ctx.Process = MagicMock(return_value=_fake_process(pid=500))
        dead = _fake_process(alive=False, exitcode=FORK_SOURCE_REJECTED_EXIT_CODE, pid=5)
        pm.workers = {5: WorkerInfo(process=dead, worker_id=0)}
        pm._check_dead_workers()
        assert "worker_fork_source_rejected" in [entry["event"] for entry in swarmq_logs]
        assert 500 in pm.workers
        assert pm._config_error is False

    def test_ordinary_crash_still_restarts(self):
        pm = self._pm()
        pm._ctx.Process = MagicMock(return_value=_fake_process(pid=500))
        dead = _fake_process(alive=False, exitcode=1, pid=5)
        pm.workers = {5: WorkerInfo(process=dead, worker_id=0)}
        pm._check_dead_workers()
        assert 500 in pm.workers
        assert pm._config_error is False


class TestConfigErrorUnderReload:
    """E-057, Nachtrag „--reload“ (Georg 2026-10-15): Unter ``swarmq worker
    --reload`` beendet Exit 78 eines Kindes den Supervisor nicht. Er loggt den
    Fehler, startet den Slot nicht neu und wartet auf die naechste
    Dateiaenderung (SIGHUP des FileWatchers), die alle Worker neu startet."""

    @pytest.mark.entscheidung("E-057")
    def test_config_error_waits_for_the_next_file_change(self, monkeypatch, swarmq_logs):
        from types import SimpleNamespace

        from swarmq.supervision import CONFIG_ERROR_EXIT_CODE

        pm = ProcessManager(num_workers=1, worker_entry=_entry_stub, worker_args=(_make_test_cfg(),), reload=True)
        pm._ctx = MagicMock()
        pm._ctx.Process = MagicMock(return_value=_fake_process(pid=500))
        dead = _fake_process(alive=False, exitcode=CONFIG_ERROR_EXIT_CODE, pid=5)
        pm.workers = {5: WorkerInfo(process=dead, worker_id=0)}
        shutdown_all = MagicMock()
        monkeypatch.setattr(pm, "_shutdown_all_workers", shutdown_all)
        ticks: list[float] = []

        def _sleep(seconds: float) -> None:
            ticks.append(seconds)
            if len(ticks) == 3:  # die Dateiaenderung: der FileWatcher schickt SIGHUP
                pm._on_reload_signal(signal.SIGHUP, None)

        monkeypatch.setattr(
            "swarmq.process_manager.supervisor.time",
            SimpleNamespace(monotonic=time.monotonic, sleep=_sleep),
        )

        assert pm._monitor_loop() == 0
        assert pm._reload_requested is True
        assert len(ticks) == 3  # wartete bis zur Dateiaenderung
        pm._ctx.Process.assert_not_called()  # kein Neustart vor der Aenderung
        shutdown_all.assert_not_called()
        entry = next(e for e in swarmq_logs if e["event"] == "worker_config_error")
        assert "next file change" in entry["hint"]

    @pytest.mark.entscheidung("E-057")
    def test_the_file_watcher_trigger_restarts_via_reexec(self, monkeypatch):
        """Ganze Kette in ``run``: Exit 78 -> warten -> FileWatcher-Trigger
        (SIGHUP an die eigene PID) -> Reload per ``reexec``."""
        from types import SimpleNamespace

        from swarmq.supervision import CONFIG_ERROR_EXIT_CODE

        pm = ProcessManager(
            num_workers=1,
            worker_entry=_entry_stub,
            worker_args=(_make_test_cfg(),),
            reload=True,
            watch_dirs=("/tmp",),
        )
        pm._ctx = MagicMock()
        watcher = MagicMock()

        def _start_all() -> None:
            dead = _fake_process(alive=False, exitcode=CONFIG_ERROR_EXIT_CODE, pid=5)
            pm.workers = {5: WorkerInfo(process=dead, worker_id=0)}

        ticks: list[float] = []

        def _sleep(seconds: float) -> None:
            ticks.append(seconds)
            if len(ticks) == 2:
                trigger = fw_cls.call_args.kwargs["trigger"]
                trigger()  # os.kill ist gepatcht und ruft den SIGHUP-Handler

        monkeypatch.setattr(
            "swarmq.process_manager.supervisor.time",
            SimpleNamespace(monotonic=time.monotonic, sleep=_sleep),
        )
        with (
            patch.object(pm, "_start_all_workers", side_effect=_start_all),
            patch.object(pm, "_setup_signals"),
            patch.object(pm, "_shutdown_all_workers"),
            patch.object(pm, "_collect_zombies"),
            patch("swarmq.hot_reload.FileWatcher", return_value=watcher) as fw_cls,
            patch(
                "swarmq.process_manager.supervisor.os.kill",
                side_effect=lambda _pid, signum: pm._on_reload_signal(signum, None),
            ),
            patch("swarmq.process_manager.supervisor.reexec", return_value=0) as reexec,
        ):
            assert pm.run() == 0

        reexec.assert_called_once_with(pm, 0)
        pm._ctx.Process.assert_not_called()

    def test_config_error_hint_under_reload_names_configuration_and_sighup(self, swarmq_logs):
        from swarmq.supervision import CONFIG_ERROR_EXIT_CODE

        pm = ProcessManager(num_workers=1, worker_entry=_entry_stub, worker_args=(_make_test_cfg(),), reload=True)
        pm._ctx = MagicMock()
        dead = _fake_process(alive=False, exitcode=CONFIG_ERROR_EXIT_CODE, pid=5)
        pm.workers = {5: WorkerInfo(process=dead, worker_id=0)}

        pm._check_dead_workers()

        entry = next(e for e in swarmq_logs if e["event"] == "worker_config_error")
        assert entry["hint"] == (
            "fix the code or configuration; workers restart after the next file change or SIGHUP (--reload)"
        )

    @pytest.mark.entscheidung("E-057")
    @pytest.mark.parametrize("signum", [signal.SIGTERM, signal.SIGINT])
    def test_shutdown_signal_while_waiting_ends_with_zero_without_reexec(self, monkeypatch, signum):
        """Waehrend der Supervisor nach Exit 78 auf die Dateiaenderung wartet,
        beendet SIGTERM bzw. Strg-C ihn regulaer: 0 wie jeder Shutdown per
        Signal, kein Re-Exec, kein Neustart."""
        from types import SimpleNamespace

        from swarmq.supervision import CONFIG_ERROR_EXIT_CODE

        pm = ProcessManager(
            num_workers=1,
            worker_entry=_entry_stub,
            worker_args=(_make_test_cfg(),),
            reload=True,
            watch_dirs=("/tmp",),
        )
        pm._ctx = MagicMock()

        def _start_all() -> None:
            dead = _fake_process(alive=False, exitcode=CONFIG_ERROR_EXIT_CODE, pid=5)
            pm.workers = {5: WorkerInfo(process=dead, worker_id=0)}

        ticks: list[float] = []

        def _sleep(seconds: float) -> None:
            ticks.append(seconds)
            if len(ticks) == 2:
                pm._on_shutdown_signal(signum, None)

        monkeypatch.setattr(
            "swarmq.process_manager.supervisor.time",
            SimpleNamespace(monotonic=time.monotonic, sleep=_sleep),
        )
        with (
            patch.object(pm, "_start_all_workers", side_effect=_start_all),
            patch.object(pm, "_setup_signals"),
            patch("swarmq.hot_reload.FileWatcher"),
            patch("swarmq.process_manager.supervisor.reexec") as reexec,
        ):
            assert pm.run() == 0

        assert len(ticks) == 2  # endete beim Signal, nicht vorher
        reexec.assert_not_called()
        pm._ctx.Process.assert_not_called()


class TestCleanExitIsNotRefilled:
    """R20/KTD9 (Plan 2026-09-03-001): Exit 0 ohne expected_shutdown zieht den
    Slot aus dem Ziel; sind alle so beendet, endet der Supervisor mit 0."""

    def _pm(self, n: int = 1) -> ProcessManager:
        pm = ProcessManager(num_workers=n, worker_entry=_entry_stub, worker_args=(_make_test_cfg(),))
        pm._ctx = MagicMock()
        return pm

    def test_clean_exit_reduces_target_and_is_not_respawned(self):
        pm = self._pm(n=2)
        as_mock(pm._ctx).Process = MagicMock(return_value=_fake_process(pid=500))
        dead = _fake_process(alive=False, exitcode=0, pid=5)
        pm.workers = {
            5: WorkerInfo(process=dead, worker_id=0),
            6: WorkerInfo(process=_fake_process(pid=6), worker_id=1),
        }
        pm._check_dead_workers()
        assert pm._clean_exits == 1
        assert pm._current_target() == 1
        assert list(pm.workers) == [6]
        as_mock(pm._ctx).Process.assert_not_called()

    def test_clean_exit_logs_worker_died_as_info(self, swarmq_logs):
        pm = self._pm(n=2)
        pm.workers = {5: WorkerInfo(process=_fake_process(alive=False, exitcode=0, pid=5), worker_id=0)}
        pm._check_dead_workers()
        [entry] = [e for e in swarmq_logs if e["event"] == "worker_died"]
        assert entry["log_level"] == "info"
        assert entry["exitcode"] == 0

    def test_expected_shutdown_logs_worker_died_as_info(self, swarmq_logs):
        pm = self._pm(n=2)
        info = WorkerInfo(process=_fake_process(alive=False, exitcode=-15, pid=5), worker_id=0)
        info.expected_shutdown = True
        pm.workers = {5: info}
        pm._check_dead_workers()
        [entry] = [e for e in swarmq_logs if e["event"] == "worker_died"]
        assert entry["log_level"] == "info"
        assert entry["expected"] is True

    def test_crash_logs_worker_died_as_warning(self, swarmq_logs):
        pm = self._pm(n=2)
        as_mock(pm._ctx).Process = MagicMock(return_value=_fake_process(pid=500))
        pm.workers = {5: WorkerInfo(process=_fake_process(alive=False, exitcode=1, pid=5), worker_id=0)}
        pm._check_dead_workers()
        [entry] = [e for e in swarmq_logs if e["event"] == "worker_died"]
        assert entry["log_level"] == "warning"
        assert entry["exitcode"] == 1

    def test_expected_shutdown_does_not_count_as_clean_exit(self):
        pm = self._pm(n=2)
        dead = _fake_process(alive=False, exitcode=0, pid=5)
        pm.workers = {5: WorkerInfo(process=dead, worker_id=0, expected_shutdown=True)}
        pm._check_dead_workers()
        assert pm._clean_exits == 0
        assert pm._current_target() == 2

    def test_monitor_loop_exits_zero_when_all_workers_ended_clean(self):
        pm = self._pm(n=2)
        pm.MONITOR_INTERVAL = 0.01
        pm.workers = {
            5: WorkerInfo(process=_fake_process(alive=False, exitcode=0, pid=5), worker_id=0),
            6: WorkerInfo(process=_fake_process(alive=False, exitcode=0, pid=6), worker_id=1),
        }
        assert pm._monitor_loop() == 0
        as_mock(pm._ctx).Process.assert_not_called()

    def test_monitor_loop_exits_one_when_mixed_clean_and_exhausted(self):
        pm = self._pm(n=2)
        pm.MONITOR_INTERVAL = 0.01
        pm._slots.lock(0)
        pm._clean_exits = 1
        pm.workers = {}
        assert pm._monitor_loop() == 1


class TestHeartbeatReplacement:
    """R21/R30 (Plan 2026-09-03-001, KTD10): hängende Worker werden ersetzt,
    begrenzt durch ein eigenes Budget; SIGTERM eskaliert zu SIGKILL."""

    def _pm(self, **kw) -> ProcessManager:
        pm = ProcessManager(
            num_workers=kw.pop("num_workers", 1),
            worker_entry=_entry_stub,
            worker_args=(_make_test_cfg(),),
            heartbeat_timeout_s=kw.pop("timeout", 10.0),
            delivery_limit=kw.pop("delivery_limit", 2),
            **kw,
        )
        pm._ctx = MagicMock()
        pm._ctx.Process = MagicMock(return_value=_fake_process(pid=500))
        return pm

    def _channel(self, last_beat: float | None, intent: int = 0):
        import multiprocessing

        from swarmq.supervision import SupervisorChannel

        ch = SupervisorChannel.create(multiprocessing.get_context("spawn"))
        if last_beat is not None:
            ch.heartbeat.value = last_beat
        ch.intent.value = intent
        return ch

    def test_start_worker_attaches_a_channel(self):
        pm = self._pm()
        info = pm._start_worker(0)
        assert info.channel is not None
        args = as_mock(pm._ctx).Process.call_args.kwargs["args"]
        assert args[-1] is info.channel  # als letztes Argument an den Entry

    def test_stale_heartbeat_replaces_worker_and_marks_expected_shutdown(self):
        import time

        pm = self._pm(timeout=10.0)
        hung = _fake_process(pid=5)
        ch = self._channel(time.monotonic() - 61)
        pm.workers = {5: WorkerInfo(process=hung, worker_id=0, channel=ch)}
        pm._check_heartbeats()
        assert pm.workers[5].expected_shutdown is True
        hung.terminate.assert_called_once()
        assert 500 in pm.workers  # Ersatz gestartet
        assert pm._exhausted_slots == 0
        assert len(pm._hang_timestamps) == 1
        # SIGKILL erst nach der Frist
        pm._escalate_terminating()
        hung.kill.assert_not_called()
        pm._terminating = [(info, 0.0) for info, _ in pm._terminating]
        pm._escalate_terminating()
        hung.kill.assert_called_once()

    def test_fresh_heartbeat_and_startup_grace_do_nothing(self):
        import time

        pm = self._pm(timeout=10.0)
        fresh = WorkerInfo(process=_fake_process(pid=5), worker_id=0, channel=self._channel(time.monotonic()))
        just_spawned = WorkerInfo(process=_fake_process(pid=6), worker_id=1, channel=self._channel(None))
        pm.workers = {5: fresh, 6: just_spawned}
        pm._check_heartbeats()
        assert not fresh.expected_shutdown
        assert not just_spawned.expected_shutdown
        as_mock(pm._ctx).Process.assert_not_called()

    def test_draining_worker_gets_the_longer_deadline(self):
        import time

        from swarmq.supervision import INTENT_DRAINING

        pm = self._pm(timeout=1.0, shutdown_grace_s=30.0)
        draining = WorkerInfo(
            process=_fake_process(pid=5), worker_id=0, channel=self._channel(time.monotonic() - 5, INTENT_DRAINING)
        )
        pm.workers = {5: draining}
        pm._check_heartbeats()
        assert draining.expected_shutdown is False  # 5 s < 30 s + 5 s
        channel_of(draining).heartbeat.value = time.monotonic() - 40
        pm._check_heartbeats()
        assert draining.expected_shutdown is True

    def test_hang_budget_is_separate_from_max_restarts(self):
        import time

        pm = self._pm(timeout=1.0, delivery_limit=1)  # Budget = 2 Ersatzungen je Stunde
        for pid in (5, 6, 7):  # drei Generationen desselben Slots 0
            info = WorkerInfo(process=_fake_process(pid=pid), worker_id=0, channel=self._channel(time.monotonic() - 9))
            pm.workers = {pid: info}
            pm._check_heartbeats()
        assert as_mock(pm._ctx).Process.call_count == 2  # dritter Haenger: Budget erschoepft
        assert pm._exhausted_slots == 1

    @pytest.mark.parametrize("skipped", ["no_channel", "dead", "terminating", "recycler"])
    def test_a_skipped_worker_does_not_hide_a_hung_one_behind_it(self, skipped):
        """T-F-01 (Mutanten ``restart_budget.py:76`` und ``:89`` ``continue`` →
        ``break``): Die Prüfung läuft über alle Worker; ein übersprungener
        Worker davor beendet sie nicht."""
        pm = self._pm(timeout=10.0, num_workers=2)
        pids = iter(range(500, 510))
        as_mock(pm._ctx).Process = MagicMock(side_effect=lambda **_kw: _fake_process(pid=next(pids)))
        first = WorkerInfo(process=_fake_process(pid=4), worker_id=0, channel=self._channel(time.monotonic()))
        if skipped == "no_channel":
            first.channel = None
        elif skipped == "dead":
            as_mock(first.process).is_alive.return_value = False
        elif skipped == "terminating":
            pm._terminating = [(first, time.monotonic() + 3600.0)]
        else:
            channel_of(first).request_recycle()
        hung = WorkerInfo(process=_fake_process(pid=5), worker_id=1, channel=self._channel(time.monotonic() - 61))
        pm.workers = {4: first, 5: hung}

        pm._check_heartbeats()

        assert hung.expected_shutdown is True
        as_mock(hung.process).terminate.assert_called_once()
        spawned = as_mock(pm._ctx).Process.call_count
        assert spawned == (2 if skipped == "recycler" else 1)  # Ersatz für den Hänger (und den Recycler)

    def _two_terminated_hung_workers(self, monkeypatch) -> tuple[ProcessManager, FakeClock, MagicMock, MagicMock]:
        """Zwei hängende Worker, beide per ``_check_heartbeats`` ersetzt und mit SIGTERM bedacht."""
        clock = FakeClock()
        monkeypatch.setattr(supervisor_module, "time", types.SimpleNamespace(monotonic=clock, sleep=lambda _s: None))
        pm = self._pm(timeout=10.0, num_workers=2)
        pids = iter(range(500, 510))
        as_mock(pm._ctx).Process = MagicMock(side_effect=lambda **_kw: _fake_process(pid=next(pids)))
        first = _fake_process(pid=4)
        second = _fake_process(pid=5)
        pm.workers = {
            4: WorkerInfo(process=first, worker_id=0, channel=self._channel(clock.now - 61)),
            5: WorkerInfo(process=second, worker_id=1, channel=self._channel(clock.now - 61)),
        }
        pm._check_heartbeats()
        first.terminate.assert_called_once()
        second.terminate.assert_called_once()
        return pm, clock, first, second

    def test_every_overdue_replaced_worker_gets_sigkill(self, monkeypatch):
        """T-F-02 (Mutant ``restart_budget.py:153`` ``continue`` → ``break``):
        Nach der SIGTERM-Frist bekommen beide überlebenden Worker SIGKILL."""
        pm, clock, first, second = self._two_terminated_hung_workers(monkeypatch)
        clock.now += pm.SHUTDOWN_TERM_S
        pm._escalate_terminating()
        first.kill.assert_called_once()
        second.kill.assert_called_once()
        assert pm._terminating == []

    def test_a_worker_that_ended_on_sigterm_does_not_shield_the_next_one(self, monkeypatch):
        """T-F-02 (Mutant ``restart_budget.py:146`` ``continue`` → ``break``):
        Der erste Worker endet auf SIGTERM, der zweite hängt weiter und
        bekommt nach der Frist SIGKILL."""
        pm, clock, first, second = self._two_terminated_hung_workers(monkeypatch)
        first.is_alive.return_value = False
        clock.now += pm.SHUTDOWN_TERM_S
        pm._escalate_terminating()
        first.kill.assert_not_called()
        second.kill.assert_called_once()
        assert pm._terminating == []

    def test_expected_shutdown_is_not_replaced_on_dead(self):
        import time

        pm = self._pm(timeout=1.0)
        hung = _fake_process(pid=5)
        pm.workers = {5: WorkerInfo(process=hung, worker_id=0, channel=self._channel(time.monotonic() - 9))}
        pm._check_heartbeats()
        hung.is_alive.return_value = False
        hung.exitcode = -9
        pm._check_dead_workers()
        assert as_mock(pm._ctx).Process.call_count == 1  # kein zweiter Ersatz durch den SIGKILL-Exit


class TestMonitorLoopAutoscale:
    """T-F-08 (Mutant ``supervisor.py:538`` ``self._autoscale is not None`` →
    ``is None``): Der Monitor-Loop ruft den Autoskalierer. Eine Runde, der
    gefälschte ``time.sleep`` am Rundenende beendet die Schleife (Muster der
    Monitor-Loop-Tests; ``run()`` bräuchte echte Prozesse und Signale)."""

    def test_one_round_with_a_backlog_scales_up(self):
        pm = ProcessManager(
            num_workers=2,
            worker_entry=_entry_stub,
            autoscale=(1, 5),
            queue_depth_fn=lambda: 10_000,
        )
        pm._ctx = MagicMock()
        pm._ctx.Process = MagicMock(return_value=_fake_process(pid=10))
        pm.AUTOSCALE_SAMPLE_INTERVAL = 0.0
        pm.SCALE_COOLDOWN_S = 0.0
        _register(
            pm,
            WorkerInfo(process=_fake_process(pid=1), worker_id=0),
            WorkerInfo(process=_fake_process(pid=2), worker_id=1),
        )
        rounds: list[float] = []

        def end_of_round(seconds: float) -> None:
            rounds.append(seconds)
            pm._shutting_down = True

        with patch("swarmq.process_manager.supervisor.time.sleep", side_effect=end_of_round):
            assert pm._monitor_loop() == 0

        assert len(rounds) == 1
        assert sorted(pm.workers) == [1, 2, 10]
        assert pm.slot_state(2) is SlotState.ACTIVE


class TestRecycleIntent:
    """KTD11: Recycling-Absicht → expected_shutdown, Ersatz, ack; zaehlt nirgends."""

    def test_recycle_intent_spawns_replacement_and_acknowledges(self):
        import multiprocessing

        from swarmq.supervision import SupervisorChannel

        pm = ProcessManager(num_workers=1, worker_entry=_entry_stub, worker_args=(_make_test_cfg(),))
        pm._ctx = MagicMock()
        pm._ctx.Process = MagicMock(return_value=_fake_process(pid=500))
        ch = SupervisorChannel.create(multiprocessing.get_context("spawn"))
        ch.request_recycle()
        old = _fake_process(pid=5)
        pm.workers = {5: WorkerInfo(process=old, worker_id=0, channel=ch)}
        pm._check_heartbeats()
        assert pm.workers[5].expected_shutdown is True
        assert ch.replacement_acknowledged()
        assert 500 in pm.workers
        old.terminate.assert_not_called()  # der Worker drainiert selbst
        # sauberes Ende des alten Workers: weder Neustart noch sauberes-Ende-Zaehler
        old.is_alive.return_value = False
        old.exitcode = 0
        pm._check_dead_workers()
        assert pm._clean_exits == 0
        assert pm._ctx.Process.call_count == 1
        assert pm._current_target() == 1

    def test_acknowledged_recycler_that_freezes_in_drain_is_killed_not_replaced(self):
        """Review #13: nach dem ack war der Recycler von jeder Heartbeat-Pruefung ausgeschlossen
        und hielt sein Prefetch bis zum Supervisor-Ende."""
        import multiprocessing
        import time

        from swarmq.supervision import SupervisorChannel

        pm = ProcessManager(
            num_workers=1,
            worker_entry=_entry_stub,
            worker_args=(_make_test_cfg(),),
            heartbeat_timeout_s=1.0,
        )
        pm._ctx = MagicMock()
        pm._ctx.Process = MagicMock(return_value=_fake_process(pid=500))
        ch = SupervisorChannel.create(multiprocessing.get_context("spawn"))
        ch.request_recycle()
        old = _fake_process(pid=5)
        pm.workers = {5: WorkerInfo(process=old, worker_id=0, channel=ch)}
        pm._check_heartbeats()  # Absicht -> Ersatz + ack
        assert pm._ctx.Process.call_count == 1
        assert old.terminate.call_count == 0
        fresh = SupervisorChannel.create(multiprocessing.get_context("spawn"))  # der Ersatz schlaegt
        fresh.heartbeat.value = time.monotonic()
        pm.workers[500].channel = fresh
        ch.heartbeat.value = time.monotonic() - 120  # der Drain des alten Workers friert ein
        pm._check_heartbeats()
        old.terminate.assert_called_once()
        assert pm._ctx.Process.call_count == 1  # kein zweiter Ersatz
        assert any(info.process is old for info, _ in pm._terminating)
        pm._check_heartbeats()
        old.terminate.assert_called_once()  # in _terminating: nicht noch einmal
        pm._terminating = [(info, 0.0) for info, _ in pm._terminating]
        pm._escalate_terminating()
        old.kill.assert_called_once()


class TestSlotTable:
    """U1/KTD1 (Plan 2026-09-08-001): Slots ``0..N-1`` statt monotoner IDs.

    Test-Liste:
    - [x] Start vergibt 0..N-1; Crash von Slot 1 → Ersatz trägt Slot 1 (AE1)
    - [x] Autoscale-Obergrenze ergibt freie Slots (slot_state)
    - [x] Budget erschöpft → Slot gesperrt, Backfill füllt ihn nicht
    - [x] Recycling: zwei Prozesse in Slot 1 = Übergang; Vorgänger tot → aktiv, kein zweiter Ersatz
    - [x] Hang-Ersatz: Übergang bis der Vorgänger nach SIGKILL tot ist
    - [x] Ersatz stirbt vor dem Vorgänger → bleibt Übergang, weiterer Ersatz
    - [x] Hang-Budget erschöpft → gesperrt trotz lebendem Vorgänger
    - [x] Backfill füllt den frei gewordenen Slot; Scale-Up nimmt den kleinsten freien Slot
    - [x] Scale-Down wählt den jüngsten per spawned_at; Gleichstand → höchster Slot
    - [x] Übergang zählt im Autoscaler einmal; Scale-Down wählt nie den frischen Ersatz
    - [x] Burst-Exit 0 gibt den Slot frei ohne Backfill; restart_timestamps wandern zum Ersatz
    """

    def _pm(self, n: int = 3, **kw) -> ProcessManager:
        pm = ProcessManager(num_workers=n, worker_entry=_entry_stub, **kw)
        pm._ctx = MagicMock()
        # PID = 100 + Slot des ersten Prozesses; Ersatzprozesse bekommen 200 + Slot, 300 + Slot, ...
        generations: dict[int, int] = {}

        def spawn(**kwargs):
            # Ziel ist seit B15 die Huelle ``run_worker_entry``; der Slot steht
            # hinter dem Entry an zweiter Stelle.
            slot = kwargs["args"][1]
            generations[slot] = generations.get(slot, 0) + 1
            return _fake_process(pid=100 * generations[slot] + slot)

        pm._ctx.Process = MagicMock(side_effect=spawn)
        pm._ctx.Value = multiprocessing.get_context("spawn").Value  # echte Kanaele: Lebenszeichen frisch
        return pm

    def _kill(self, pm: ProcessManager, pid: int, exitcode: int = -9) -> None:
        proc = pm.workers[pid].process
        as_mock(proc).is_alive.return_value = False
        as_mock(proc).exitcode = exitcode

    def test_crashed_slot_is_refilled_with_the_same_slot(self):
        """Covers AE1: --processes 3 vergibt 0, 1, 2; der Ersatz für Slot 1 trägt Slot 1."""
        pm = self._pm(n=3)
        pm._start_all_workers()
        assert sorted(w.worker_id for w in pm.workers.values()) == [0, 1, 2]
        self._kill(pm, 101)
        pm._check_dead_workers()
        assert sorted(w.worker_id for w in pm.workers.values()) == [0, 1, 2]
        kwargs = as_mock(pm._ctx).Process.call_args.kwargs
        assert kwargs["args"][1] == 1
        assert kwargs["name"] == "swarmq-worker-1"

    def test_autoscale_upper_bound_defines_the_slot_table(self):
        pm = self._pm(n=2, autoscale=(1, 4), queue_depth_fn=lambda: 0)
        pm._start_all_workers()
        assert [pm.slot_state(s) for s in range(4)] == [
            SlotState.ACTIVE,
            SlotState.ACTIVE,
            SlotState.FREE,
            SlotState.FREE,
        ]

    def test_headroom_defines_the_slot_table_regardless_of_the_start_count(self):
        """Covers AE3 (Kontrollkanal U4, R4): Spielraum 16 -> 16 Slots, 2 davon belegt."""
        pm = self._pm(n=2, max_processes=16)
        pm._start_all_workers()
        states = [pm.slot_state(s) for s in range(16)]
        assert states[:2] == [SlotState.ACTIVE, SlotState.ACTIVE]
        assert states[2:] == [SlotState.FREE] * 14
        with pytest.raises(IndexError):
            pm.slot_state(16)

    def test_without_headroom_the_slot_table_keeps_the_start_count(self):
        pm = self._pm(n=3)
        pm._start_all_workers()
        assert [pm.slot_state(s) for s in range(3)] == [SlotState.ACTIVE] * 3
        with pytest.raises(IndexError):
            pm.slot_state(3)

    def _run_monitor_briefly(self, pm: ProcessManager) -> None:
        import threading

        pm.MONITOR_INTERVAL = 0.01
        t = threading.Thread(target=pm._monitor_loop, daemon=True)
        t.start()
        time.sleep(0.05)
        pm._shutting_down = True
        t.join(timeout=2)
        assert not t.is_alive()

    def test_exhausted_budget_locks_the_slot_and_backfill_skips_it(self):
        pm = self._pm(n=3)
        pm._start_all_workers()
        pm.workers[102].restart_timestamps = [time.monotonic() - 1.0] * pm.MAX_RESTARTS
        self._kill(pm, 102)
        pm._check_dead_workers()
        assert pm.slot_state(2) is SlotState.LOCKED
        assert pm._exhausted_slots == 1
        assert pm._current_target() == 2
        self._run_monitor_briefly(pm)
        assert as_mock(pm._ctx).Process.call_count == 3  # kein Backfill fuer Slot 2
        assert sorted(w.worker_id for w in pm.workers.values()) == [0, 1]

    def test_recycling_replacement_shares_the_slot_until_the_predecessor_exits(self):
        pm = self._pm(n=2)
        pm._start_all_workers()
        channel_of(pm.workers[101]).request_recycle()
        pm._check_heartbeats()
        assert [w.worker_id for w in pm.workers.values()] == [0, 1, 1]
        assert pm.slot_state(1) is SlotState.TRANSITION
        self._kill(pm, 101, exitcode=0)
        pm._check_dead_workers()
        assert pm.slot_state(1) is SlotState.ACTIVE
        assert as_mock(pm._ctx).Process.call_count == 3  # kein zweiter Ersatz
        assert pm._current_target() == 2
        assert pm._clean_exits == 0

    def test_hang_replacement_keeps_the_slot_in_transition_until_the_kill_lands(self):
        pm = self._pm(n=1, heartbeat_timeout_s=10.0)
        pm._start_all_workers()
        hung = pm.workers[100]
        channel_of(hung).heartbeat.value = time.monotonic() - 61
        pm._check_heartbeats()
        assert [w.worker_id for w in pm.workers.values()] == [0, 0]
        assert pm.slot_state(0) is SlotState.TRANSITION
        pm._terminating = [(info, 0.0) for info, _ in pm._terminating]
        pm._escalate_terminating()
        as_mock(hung.process).kill.assert_called_once()
        assert pm.slot_state(0) is SlotState.TRANSITION  # der Vorgaenger lebt noch
        self._kill(pm, 100)
        pm._check_dead_workers()
        assert pm.slot_state(0) is SlotState.ACTIVE
        assert as_mock(pm._ctx).Process.call_count == 2

    def test_replacement_dying_before_the_predecessor_keeps_transition_and_respawns(self):
        pm = self._pm(n=1)
        pm._start_all_workers()
        channel_of(pm.workers[100]).request_recycle()
        pm._check_heartbeats()  # Ersatz pid 200 teilt Slot 0 mit dem drainenden Vorgaenger
        self._kill(pm, 200, exitcode=-11)
        pm._check_dead_workers()
        assert pm.slot_state(0) is SlotState.TRANSITION
        assert sorted(pm.workers) == [100, 300]  # Vorgaenger + weiterer Ersatz
        assert as_mock(pm._ctx).Process.call_count == 3
        assert pm.workers[300].restart_timestamps
        assert pm.workers[300].worker_id == 0

    def test_exhausted_hang_budget_locks_the_slot_while_the_predecessor_lives(self):
        pm = self._pm(n=1, heartbeat_timeout_s=10.0, delivery_limit=1)  # Budget 2
        pm._start_all_workers()
        pm._hang_timestamps = [time.monotonic()] * 2
        channel_of(pm.workers[100]).heartbeat.value = time.monotonic() - 61
        pm._check_heartbeats()
        assert pm.workers[100].process.is_alive()
        assert pm.slot_state(0) is SlotState.LOCKED
        assert pm._current_target() == 0
        assert as_mock(pm._ctx).Process.call_count == 1
        self._kill(pm, 100)
        pm._check_dead_workers()
        assert pm.slot_state(0) is SlotState.LOCKED
        assert not pm.workers

    def test_autoscale_target_counts_occupied_slots_not_processes(self):
        pm = self._pm(n=2, autoscale=(1, 4), queue_depth_fn=lambda: 0)
        pm._start_all_workers()
        channel_of(pm.workers[101]).request_recycle()
        pm._check_heartbeats()
        assert len(pm.workers) == 3
        assert pm._current_target() == 2  # Slot 1 im Uebergang zaehlt einmal

    def test_backfill_refills_the_freed_slot(self):
        pm = self._pm(n=2)
        pm._start_all_workers()
        pm.workers[101].expected_shutdown = True  # Ende ohne Ersatz: Slot 1 wird frei
        self._kill(pm, 101, exitcode=0)
        pm._check_dead_workers()
        assert pm.slot_state(1) is SlotState.FREE
        assert pm._current_target() == 2
        self._run_monitor_briefly(pm)
        assert sorted(w.worker_id for w in pm.workers.values()) == [0, 1]
        assert as_mock(pm._ctx).Process.call_count == 3
        assert pm.slot_state(1) is SlotState.ACTIVE

    def _autoscale_pm(self, depth: int, n: int = 1, bounds: tuple[int, int] = (1, 4)) -> ProcessManager:
        pm = self._pm(n=n, autoscale=bounds, queue_depth_fn=lambda: depth)
        pm.AUTOSCALE_SAMPLE_INTERVAL = 0.0
        pm.SCALE_COOLDOWN_S = 0.0
        pm.SCALE_DOWN_IDLE_S = 0.0
        pm.SCALE_MIN_AGE_S = 0.0
        return pm

    def test_scale_up_takes_the_smallest_free_slot(self):
        pm = self._autoscale_pm(depth=500)
        pm._start_all_workers()
        pm._start_worker(2)
        pm._autoscale_tick()
        assert sorted(w.worker_id for w in pm.workers.values()) == [0, 1, 2]
        assert pm.slot_state(1) is SlotState.ACTIVE

    def test_scale_down_picks_the_youngest_by_spawn_time_not_by_slot(self):
        pm = self._autoscale_pm(depth=0, n=2)
        pm._start_all_workers()
        now = time.monotonic()
        pm.workers[100].spawned_at = now - 10.0  # Slot 0 traegt den juengeren Prozess
        pm.workers[101].spawned_at = now - 100.0
        pm._autoscale_tick()  # idle_since
        pm._autoscale_tick()
        as_mock(pm.workers[100].process).terminate.assert_called_once()
        as_mock(pm.workers[101].process).terminate.assert_not_called()
        assert pm.workers[100].expected_shutdown is True

    def test_scale_down_tie_on_spawn_time_frees_the_highest_slot(self):
        pm = self._autoscale_pm(depth=0, n=3)
        pm._start_all_workers()
        # Relativ zu ``time.monotonic()``, nicht absolut: der Wert ist die
        # Systemlaufzeit. Ein fester 1000.0 liegt auf einer Maschine, die noch
        # keine 17 Minuten laeuft, in der ZUKUNFT — dann gilt kein Worker als
        # alt genug fuer den Scale-Down und der Test scheitert. Genau so in
        # der CI passiert (frischer Container), lokal nie.
        now = time.monotonic()
        for info in pm.workers.values():
            info.spawned_at = now - 100.0
        pm._autoscale_tick()
        pm._autoscale_tick()
        as_mock(pm.workers[102].process).terminate.assert_called_once()
        as_mock(pm.workers[100].process).terminate.assert_not_called()
        as_mock(pm.workers[101].process).terminate.assert_not_called()

    def test_slot_in_transition_counts_once_for_the_autoscaler(self):
        pm = self._autoscale_pm(depth=0, n=2, bounds=(2, 4))
        pm._start_all_workers()
        channel_of(pm.workers[101]).request_recycle()
        pm._check_heartbeats()
        assert len(pm.workers) == 3
        pm._autoscale_tick()
        pm._autoscale_tick()
        for info in pm.workers.values():  # 2 belegte Slots == min: kein Scale-Down
            as_mock(info.process).terminate.assert_not_called()

    def test_scale_down_never_picks_the_fresh_replacement_of_a_transition(self):
        pm = self._autoscale_pm(depth=0, n=2)
        pm._start_all_workers()
        now = time.monotonic()
        pm.workers[100].spawned_at = now - 100.0
        pm.workers[101].spawned_at = now - 50.0
        channel_of(pm.workers[100]).request_recycle()
        pm._check_heartbeats()  # Ersatz pid 200 ist der juengste Prozess, Slot 0 im Uebergang
        pm._autoscale_tick()
        pm._autoscale_tick()
        as_mock(pm.workers[101].process).terminate.assert_called_once()
        as_mock(pm.workers[200].process).terminate.assert_not_called()
        as_mock(pm.workers[100].process).terminate.assert_not_called()

    def test_burst_exit_frees_the_slot_without_backfill(self):
        pm = self._pm(n=2)
        pm._start_all_workers()
        self._kill(pm, 100, exitcode=0)
        pm._check_dead_workers()
        assert pm.slot_state(0) is SlotState.FREE
        assert pm._clean_exits == 1
        assert pm._current_target() == 1
        self._run_monitor_briefly(pm)
        assert as_mock(pm._ctx).Process.call_count == 2
        assert list(pm.workers) == [101]

    def test_restart_timestamps_travel_to_the_replacement_in_the_same_slot(self):
        pm = self._pm(n=2)
        pm._start_all_workers()
        pm.workers[101].restart_timestamps = [time.monotonic() - 5.0]
        self._kill(pm, 101)
        pm._check_dead_workers()
        replacement = pm.workers[201]
        assert replacement.worker_id == 1
        assert len(replacement.restart_timestamps) == 2
        assert pm.slot_state(1) is SlotState.ACTIVE


class TestReloadStateMachine:
    """U8 (KTD8, R9-R11): Probe und Reload laufen im Monitor-Loop, mit
    gefaelschter Probe-Fabrik und Uhr, ohne echte Signale."""

    def _pm(self, tmp_path, *probes: FakeProbe, num_workers: int = 1):
        pm = ProcessManager(num_workers=num_workers, worker_entry=_entry_stub)
        pm._ctx = MagicMock()
        pm._ctx.Process = MagicMock(return_value=_fake_process(pid=500))
        path = tmp_path / "log.json"
        path.write_text('{"version": 1}')
        factory = FakeProbeFactory(*probes)
        pm.reloader = ReloadController(str(path), probe_factory=factory, clock=FakeClock())
        return pm, factory

    @staticmethod
    def _run_loop(pm, on_tick):
        return TestReloadStateMachine._run_loop_via(pm, on_tick, pm._monitor_loop)

    def test_a_tick_starts_the_probe_and_dead_workers_are_still_replaced(self, tmp_path):
        probe = FakeProbe(rc=None)
        pm, factory = self._pm(tmp_path, probe)
        _register(pm, WorkerInfo(process=_fake_process(alive=False, exitcode=-9, pid=5), worker_id=0))
        pm._on_reload_signal(signal.SIGHUP, None)

        def on_tick(n):
            if n == 2:
                pm._on_shutdown_signal(signal.SIGTERM, None)

        assert self._run_loop(pm, on_tick) == 0
        assert len(factory.raws) == 1
        assert 500 in pm.workers  # Ersatz, waehrend die Probe lief
        assert pm._reload_requested is False

    def test_a_valid_probe_ends_the_loop_with_an_accepted_reload(self, tmp_path, swarmq_logs):
        probe = FakeProbe(rc=None)
        pm, _ = self._pm(tmp_path, probe)
        pm._on_reload_signal(signal.SIGHUP, None)

        def on_tick(n):
            probe.rc = 0

        assert self._run_loop(pm, on_tick) == 0
        assert pm._reload_requested is True
        assert pm._shutting_down is True
        assert pm.shutdown_event.is_set()
        assert [e["event"] for e in swarmq_logs].count("process_manager_reload_requested") == 1

    @staticmethod
    def _run(pm, on_tick, shutdown_all=None):
        """``run`` mit echtem Monitor-Loop; Start, Signale und Worker-Stopp gefaelscht."""
        execvpe = MagicMock()
        with (
            patch.object(pm, "_start_all_workers"),
            patch.object(pm, "_setup_signals"),
            patch.object(pm, "_shutdown_all_workers", side_effect=shutdown_all),
            patch.object(pm, "_collect_zombies"),
            patch("swarmq.process_manager.reload.shutdown_logging"),
            patch("swarmq.process_manager.supervisor.os.execvpe", execvpe),
        ):
            rc = TestReloadStateMachine._run_loop_via(pm, on_tick, pm.run)
        return rc, execvpe

    @staticmethod
    def _run_loop_via(pm, on_tick, target):
        """``target`` mit gefaelschtem ``sleep``; ``on_tick(n)`` nach jedem Tick."""
        ticks = []

        def fake_sleep(_seconds):
            ticks.append(1)
            on_tick(len(ticks))
            if len(ticks) > 20:
                raise AssertionError("monitor loop did not end")

        with patch("swarmq.process_manager.supervisor.time.sleep", side_effect=fake_sleep):
            return target()

    def test_sigterm_during_the_probe_kills_it_and_ends_without_reload(self, tmp_path):
        probe = FakeProbe(rc=None)
        pm, _ = self._pm(tmp_path, probe)
        pm._on_reload_signal(signal.SIGHUP, None)

        def on_tick(n):
            pm._on_shutdown_signal(signal.SIGTERM, None)

        rc, execvpe = self._run(pm, on_tick)
        assert rc == 0
        assert probe.killed is True
        execvpe.assert_not_called()

    def test_sigterm_while_the_log_queue_drains_before_execvpe_prevents_it(self, tmp_path):
        """Das Leeren vor ``execvpe`` darf bis ``log_drain_timeout`` dauern;
        ein SIGTERM in dieser Zeit beendet den Supervisor (Pruefung direkt
        vor ``execvpe``)."""
        probe = FakeProbe(rc=0)
        pm, _ = self._pm(tmp_path, probe)
        pm._on_reload_signal(signal.SIGHUP, None)
        execvpe = MagicMock()
        with (
            patch.object(pm, "_start_all_workers"),
            patch.object(pm, "_setup_signals"),
            patch.object(pm, "_shutdown_all_workers"),
            patch.object(pm, "_collect_zombies"),
            patch(
                "swarmq.process_manager.reload.shutdown_logging",
                side_effect=lambda: pm._on_shutdown_signal(signal.SIGTERM, None),
            ),
            patch("swarmq.process_manager.supervisor.os.execvpe", execvpe),
        ):
            rc = self._run_loop_via(pm, lambda n: None, pm.run)
        assert rc == 0
        execvpe.assert_not_called()

    def test_sigterm_while_workers_stop_for_an_accepted_reload_ends_the_supervisor(
        self,
        tmp_path,
        swarmq_logs,
    ):
        """Probe gueltig, Reload beschlossen, SIGTERM waehrend des Worker-Stopps:
        kein ``execvpe``, Ende mit 0, und das Signal steht im Log."""
        pm, _ = self._pm(tmp_path, FakeProbe(rc=0))
        pm._on_reload_signal(signal.SIGHUP, None)
        rc, execvpe = self._run(
            pm,
            lambda n: None,
            shutdown_all=lambda: pm._on_shutdown_signal(signal.SIGTERM, None),
        )
        assert rc == 0
        execvpe.assert_not_called()
        (event,) = [e for e in swarmq_logs if e["event"] == "process_manager_shutdown_signal"]
        assert event["signal"] == "SIGTERM"

    def test_sigterm_then_sighup_ends_without_reload(self, tmp_path):
        """Szenario E (S6) ueber den ganzen Lauf."""
        factory_probe = FakeProbe(rc=0)
        pm, factory = self._pm(tmp_path, factory_probe)

        def on_tick(n):
            pm._on_shutdown_signal(signal.SIGTERM, None)
            pm._on_reload_signal(signal.SIGHUP, None)

        rc, execvpe = self._run(pm, on_tick)
        assert rc == 0
        execvpe.assert_not_called()
        assert factory.raws == []

    def test_two_sighups_during_a_probe_run_exactly_one_probe(self, tmp_path):
        probe = FakeProbe(rc=None)
        pm, factory = self._pm(tmp_path, probe, FakeProbe(rc=1))
        pm._on_reload_signal(signal.SIGHUP, None)

        def on_tick(n):
            if n == 1:
                pm._on_reload_signal(signal.SIGHUP, None)
                pm._on_reload_signal(signal.SIGHUP, None)
            elif n == 2:
                probe.rc = 1
            elif n == 5:
                pm._on_shutdown_signal(signal.SIGTERM, None)

        self._run_loop(pm, on_tick)
        assert len(factory.raws) == 1

    def test_a_probe_past_its_deadline_is_a_timeout_and_workers_keep_running(self, tmp_path, swarmq_logs):
        probe = FakeProbe(rc=None)
        pm, _ = self._pm(tmp_path, probe)
        clock = pm.reloader._clock
        pm._on_reload_signal(signal.SIGHUP, None)

        def on_tick(n):
            if n == 1:
                clock.now += 31.0
            elif n == 3:
                assert not pm.shutdown_event.is_set()
                pm._on_shutdown_signal(signal.SIGTERM, None)

        assert self._run_loop(pm, on_tick) == 0
        assert probe.killed is True
        events = [e["event"] for e in swarmq_logs]
        assert "log_config_reload_probe_timeout" in events
        assert "log_config_reload_failed" not in events
        assert "process_manager_reload_requested" not in events
        assert pm._reload_requested is False

    def test_an_invalid_probe_keeps_the_workers_running(self, tmp_path, swarmq_logs):
        pm, _ = self._pm(tmp_path, FakeProbe(rc=1, error="Unable to configure handler 'err'"))
        pm._on_reload_signal(signal.SIGHUP, None)

        def on_tick(n):
            if n == 3:
                assert not pm.shutdown_event.is_set()
                assert pm._shutting_down is False
                pm._on_shutdown_signal(signal.SIGTERM, None)

        assert self._run_loop(pm, on_tick) == 0
        events = [e["event"] for e in swarmq_logs]
        assert "log_config_reload_failed" in events
        assert "process_manager_reload_requested" not in events
        assert 500 in pm.workers

    def test_a_valid_probe_reloads_with_the_log_queue_drained_before_execvpe(self, tmp_path):
        calls: list[str] = []
        pm, _ = self._pm(tmp_path, FakeProbe(rc=0))
        pm._on_reload_signal(signal.SIGHUP, None)
        with (
            patch.object(pm, "_start_all_workers"),
            patch.object(pm, "_setup_signals"),
            patch.object(pm, "_shutdown_all_workers", side_effect=lambda: calls.append("stop_workers")),
            patch.object(pm, "_collect_zombies"),
            patch(
                "swarmq.process_manager.reload.shutdown_logging",
                side_effect=lambda: calls.append("shutdown_logging"),
            ),
            patch(
                "swarmq.process_manager.supervisor.os.execvpe",
                side_effect=lambda *a: calls.append("execvpe"),
            ),
        ):
            self._run_loop_via(pm, lambda n: None, pm.run)
        assert calls == ["stop_workers", "shutdown_logging", "execvpe"]

    def test_sigterm_while_the_loop_accepts_the_reload_prevents_execvpe(self, tmp_path):
        """Der Handler kann zwischen zwei Bytecodes des Loops laufen -- auch
        nachdem die Probe gueltig war, aber bevor der Loop den Reload
        beschliesst. Auch dann gewinnt SIGTERM."""
        pm, _ = self._pm(tmp_path)

        class _SignallingProbe(FakeProbe):
            @override
            def poll(self) -> int | None:
                pm._on_shutdown_signal(signal.SIGTERM, None)
                return 0

        probe = _SignallingProbe(rc=None)
        pm.reloader = ReloadController(
            pm.reloader.log_config,
            probe_factory=FakeProbeFactory(probe),
            clock=FakeClock(),
        )
        pm._on_reload_signal(signal.SIGHUP, None)
        rc, execvpe = self._run(pm, lambda n: None)
        assert rc == 0
        execvpe.assert_not_called()
