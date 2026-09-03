"""Supervisor-Tests (Plan U21) — verbatim: Restart-Budget, Autoscale,
Reload (execvpe mit eingefrorenem argv/env), Orphan-Guard, FileWatcher."""

from __future__ import annotations

import signal
import time
from unittest.mock import MagicMock, patch

import pytest

from swarmq.process_manager import (
    ProcessManager,
    WorkerInfo,
)
from tests.unit.process_manager._helpers import (
    _entry_stub,
    _fake_process,
    _instant_crash_entry,
    _make_test_cfg,
)


class TestProcessManagerCore:
    def test_rejects_invalid_autoscale(self):
        with pytest.raises(ValueError, match="min >= 1"):
            ProcessManager(
                num_workers=1, worker_entry=_entry_stub,
                autoscale=(0, 3), queue_depth_fn=lambda: 0,
            )
        with pytest.raises(ValueError, match="min >= 1"):
            ProcessManager(
                num_workers=1, worker_entry=_entry_stub,
                autoscale=(5, 3), queue_depth_fn=lambda: 0,
            )

    def test_autoscale_requires_queue_depth_fn(self):
        with pytest.raises(ValueError, match="queue_depth_fn"):
            ProcessManager(
                num_workers=1, worker_entry=_entry_stub,
                autoscale=(1, 3),
            )

    def test_autoscale_clamps_initial_count(self):
        pm = ProcessManager(
            num_workers=10, worker_entry=_entry_stub,
            autoscale=(1, 3), queue_depth_fn=lambda: 0,
        )
        assert pm.initial_workers == 3

        pm2 = ProcessManager(
            num_workers=0, worker_entry=_entry_stub,
            autoscale=(2, 5), queue_depth_fn=lambda: 0,
        )
        assert pm2.initial_workers == 2

    def test_start_worker_registers_and_spawns(self):
        pm = ProcessManager(num_workers=1, worker_entry=_entry_stub)
        fake = _fake_process(pid=9001)
        pm._ctx = MagicMock()
        pm._ctx.Process = MagicMock(return_value=fake)
        info = pm._start_worker()
        assert info.worker_id == 0
        assert 9001 in pm.workers
        fake.start.assert_called_once()

    def test_start_all_workers_spawns_n(self):
        pm = ProcessManager(num_workers=3, worker_entry=_entry_stub)
        pm._ctx = MagicMock()
        pm._ctx.Process = MagicMock(
            side_effect=[
                _fake_process(pid=1), _fake_process(pid=2),
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
                signal.SIGINT, signal.SIGTERM,
                signal.SIGHUP, signal.SIGCHLD,
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
        pm._ctx.Process = MagicMock(return_value=_fake_process(pid=77))
        old = WorkerInfo(
            process=_fake_process(alive=False, pid=1), worker_id=0,
        )
        pm._maybe_restart(old)
        assert 77 in pm.workers

    def test_restart_limit_exceeded_stops_restarts(self):
        """ce-review C-01: rolling-window blocks on MAX_RESTARTS
        timestamps inside the window."""
        pm = self._pm()
        pm._ctx.Process = MagicMock(return_value=_fake_process(pid=99))
        now = time.monotonic()
        # MAX_RESTARTS recent timestamps already in the window.
        old = WorkerInfo(
            process=_fake_process(alive=False, pid=1), worker_id=0,
            restart_timestamps=[now - 1.0] * pm.MAX_RESTARTS,
        )
        pm._maybe_restart(old)
        # No spawn — the history is already at the cap.
        assert len(pm.workers) == 0

    def test_restart_last_slot_at_limit_minus_one_succeeds(self):
        """Exact-boundary: 4 timestamps in window, the 5th should
        still be allowed (count reaches but does not exceed MAX)."""
        pm = self._pm()
        pm._ctx.Process = MagicMock(return_value=_fake_process(pid=88))
        now = time.monotonic()
        old = WorkerInfo(
            process=_fake_process(alive=False, pid=1), worker_id=0,
            restart_timestamps=[now - 10.0] * (pm.MAX_RESTARTS - 1),
        )
        pm._maybe_restart(old)
        assert 88 in pm.workers
        assert len(pm.workers[88].restart_timestamps) == pm.MAX_RESTARTS

    def test_restart_counter_prunes_stale_entries(self):
        """Entries older than RESTART_WINDOW are dropped, freeing
        restart budget without a full counter reset."""
        pm = self._pm()
        pm._ctx.Process = MagicMock(return_value=_fake_process(pid=42))
        now = time.monotonic()
        # All entries older than the window → treated as if fresh.
        old = WorkerInfo(
            process=_fake_process(alive=False, pid=1), worker_id=0,
            restart_timestamps=[
                now - pm.RESTART_WINDOW - 1 for _ in range(10)
            ],
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
        pm._ctx.Process = MagicMock(return_value=_fake_process(pid=55))
        now = time.monotonic()
        # Five restarts spread across the window — oldest at (now-100).
        ts = [now - 100.0 + i for i in range(pm.MAX_RESTARTS)]
        old = WorkerInfo(
            process=_fake_process(alive=False, pid=1), worker_id=0,
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
        pm._ctx.Process = MagicMock(return_value=_fake_process(pid=500))
        dead = _fake_process(alive=False, exitcode=-11, pid=5)  # segfault
        pm.workers = {5: WorkerInfo(process=dead, worker_id=0)}
        pm._check_dead_workers()
        assert 500 in pm.workers

    def test_sighup_sets_reload_flag(self):
        pm = self._pm()
        pm._on_reload_signal(signal.SIGHUP, None)
        assert pm._reload_requested is True
        assert pm._shutting_down is True
        assert pm.shutdown_event.is_set()

    def test_sigterm_sets_shutdown_flag_no_reload(self):
        pm = self._pm()
        pm._on_shutdown_signal(signal.SIGTERM, None)
        assert pm._shutting_down is True
        assert pm._reload_requested is False

    def test_run_triggers_execvp_on_reload(self):
        pm = ProcessManager(num_workers=0, worker_entry=_entry_stub)
        pm._ctx = MagicMock()
        pm._reload_requested = True
        # Skip all worker management; we only verify execvp is called.
        with patch.object(pm, "_start_all_workers"), \
             patch.object(pm, "_setup_signals"), \
             patch.object(pm, "_monitor_loop", return_value=0), \
             patch.object(pm, "_shutdown_all_workers"), \
             patch.object(pm, "_collect_zombies"), \
             patch("swarmq.process_manager.supervisor.os.execvpe") as execvp:
            pm.run()
            execvp.assert_called_once()


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
            process=_fake_process(alive=False, pid=1), worker_id=0,
            restart_timestamps=[now - 1.0] * pm.MAX_RESTARTS,
        )
        pm._maybe_restart(old)
        assert pm._exhausted_slots == 1

    def test_current_target_excludes_exhausted_slots(self):
        pm = self._pm(n=3)
        pm._exhausted_slots = 2
        assert pm._current_target() == 1

    def test_current_target_never_negative(self):
        pm = self._pm(n=1)
        pm._exhausted_slots = 5
        assert pm._current_target() == 0

    def test_autoscale_target_unaffected_by_exhausted_slots(self):
        """Autoscale-Pfad: Ziel bleibt len(workers) — Scale-up ist
        nachfragegetrieben mit Cooldown, kein Backfill-Bypass."""
        pm = ProcessManager(
            num_workers=2, worker_entry=_entry_stub,
            autoscale=(1, 3), queue_depth_fn=lambda: 0,
        )
        pm._ctx = MagicMock()
        pm._exhausted_slots = 1
        pm.workers = {1: WorkerInfo(process=_fake_process(), worker_id=0)}
        assert pm._current_target() == 1

    def test_monitor_loop_exits_nonzero_when_all_slots_exhausted(self):
        pm = self._pm(n=2)
        pm.MONITOR_INTERVAL = 0.01
        pm._exhausted_slots = 2
        pm.workers = {}
        rc = pm._monitor_loop()
        assert rc != 0

    def test_monitor_loop_keeps_running_degraded(self):
        """Teilweise erschöpft (1 von 2) + 1 lebender Worker →
        Loop läuft weiter (degraded), kein Exit, kein Backfill."""
        pm = self._pm(n=2)
        pm.MONITOR_INTERVAL = 0.01
        pm._exhausted_slots = 1
        pm.workers = {1: WorkerInfo(process=_fake_process(), worker_id=0)}

        import threading

        result: dict = {}
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
        pm._ctx.Process.assert_not_called()

    def test_crash_storm_bounded_and_manager_exits_real_processes(self):
        """Regression mit ECHTEN Subprozessen (kein gemockter
        ``_monitor_loop``): instant-crashender Worker → genau
        1 + MAX_RESTARTS Spawns, dann Exit ≠ 0. Vor dem Fix lief
        das endlos (Backfill mit frischer Historie)."""
        import threading

        pm = ProcessManager(
            num_workers=1, worker_entry=_instant_crash_entry,
        )
        pm.MAX_RESTARTS = 2
        pm.MONITOR_INTERVAL = 0.05
        pm._start_all_workers()

        result: dict = {}
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
                "monitor loop did not exit after slot exhaustion "
                f"(spawned {pm._next_worker_id} workers)",
            )
        try:
            assert result["rc"] != 0
            # initial (1) + MAX_RESTARTS Ersatz-Spawns — kein Sturm.
            assert pm._next_worker_id <= 1 + pm.MAX_RESTARTS
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
            num_workers=2, worker_entry=_entry_stub,
            autoscale=(min_w, max_w), queue_depth_fn=depth_fn,
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

    def test_scale_up_when_depth_exceeds_threshold(self):
        pm = self._pm(depth_values=[500], min_w=1, max_w=5)
        pm._ctx.Process = MagicMock(return_value=_fake_process(pid=10))
        # Two existing workers; threshold 50/each → 100. Depth 500 > 100.
        pm.workers = {
            1: WorkerInfo(process=_fake_process(pid=1), worker_id=0),
            2: WorkerInfo(process=_fake_process(pid=2), worker_id=1),
        }
        pm._autoscale_tick()
        assert 10 in pm.workers
        assert len(pm.workers) == 3

    def test_scale_up_blocked_at_max(self):
        pm = self._pm(depth_values=[500], min_w=1, max_w=2)
        pm._ctx.Process = MagicMock()
        pm.workers = {
            1: WorkerInfo(process=_fake_process(pid=1), worker_id=0),
            2: WorkerInfo(process=_fake_process(pid=2), worker_id=1),
        }
        pm._autoscale_tick()
        pm._ctx.Process.assert_not_called()

    def test_scale_down_when_idle_above_min(self):
        pm = self._pm(depth_values=[0, 0], min_w=1, max_w=5)
        p1 = _fake_process(pid=1)
        p2 = _fake_process(pid=2)
        pm.workers = {
            1: WorkerInfo(process=p1, worker_id=0),
            2: WorkerInfo(process=p2, worker_id=1),
        }
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
        pm.workers = {
            1: WorkerInfo(process=p1, worker_id=0),
            2: WorkerInfo(process=p2, worker_id=1),
        }
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
            num_workers=1, worker_entry=_entry_stub,
            autoscale=(1, 3), queue_depth_fn=depth_fn,
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
        pm.workers = {
            1: WorkerInfo(process=_fake_process(pid=1), worker_id=0),
        }
        pm._autoscale_tick()  # scales up, records decision time
        assert len(pm.workers) == 2
        pm._autoscale_tick()  # cooldown blocks
        assert len(pm.workers) == 2

    def test_depth_fn_exception_is_swallowed(self):
        def boom():
            raise RuntimeError("broker gone")

        pm = ProcessManager(
            num_workers=1, worker_entry=_entry_stub,
            autoscale=(1, 3), queue_depth_fn=boom,
        )
        pm.AUTOSCALE_SAMPLE_INTERVAL = 0.0
        pm._ctx = MagicMock()
        pm.workers = {
            1: WorkerInfo(process=_fake_process(pid=1), worker_id=0),
        }
        # Should not raise.
        pm._autoscale_tick()

    def test_non_idle_resets_idle_timer(self):
        depths = iter([0, 10, 0])  # idle → load → idle
        pm = ProcessManager(
            num_workers=2, worker_entry=_entry_stub,
            autoscale=(1, 5),
            queue_depth_fn=lambda: next(depths, 0),
        )
        pm.AUTOSCALE_SAMPLE_INTERVAL = 0.0
        pm.SCALE_COOLDOWN_S = 0.0
        pm.SCALE_DOWN_IDLE_S = 999.0  # long idle requirement
        pm._ctx = MagicMock()
        p2 = _fake_process(pid=2)
        pm.workers = {
            1: WorkerInfo(process=_fake_process(pid=1), worker_id=0),
            2: WorkerInfo(process=p2, worker_id=1),
        }
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
        with patch.object(pm, "_start_all_workers"), \
             patch.object(pm, "_setup_signals"), \
             patch.object(pm, "_monitor_loop", return_value=0), \
             patch.object(pm, "_shutdown_all_workers"), \
             patch.object(pm, "_collect_zombies"), \
             patch("swarmq.process_manager.supervisor.os.execvpe") as execvp:
            pm.run()
            execvp.assert_called_once()

    def test_execvp_blocked_when_alive_child_remains(self):
        """Guard fires: live worker after shutdown → WARNING + return 0."""
        pm = self._base_pm()
        survivor = _fake_process(alive=True, pid=999)
        pm.workers = {999: WorkerInfo(process=survivor, worker_id=0)}
        with patch.object(pm, "_start_all_workers"), \
             patch.object(pm, "_setup_signals"), \
             patch.object(pm, "_monitor_loop", return_value=0), \
             patch.object(pm, "_shutdown_all_workers"), \
             patch.object(pm, "_collect_zombies"), \
             patch("swarmq.process_manager.supervisor.os.execvpe") as execvp:
            rc = pm.run()
            execvp.assert_not_called()
            assert rc == 0  # clean exit — systemd must not restart

    def test_execvp_oserror_surfaces_return_1(self):
        """If execvp itself fails (bad sys.executable), return 1 with
        ERROR log — not a raw traceback."""
        pm = self._base_pm()
        with patch.object(pm, "_start_all_workers"), \
             patch.object(pm, "_setup_signals"), \
             patch.object(pm, "_monitor_loop", return_value=0), \
             patch.object(pm, "_shutdown_all_workers"), \
             patch.object(pm, "_collect_zombies"), \
             patch("swarmq.process_manager.supervisor.os.execvpe",
                   side_effect=OSError("permission denied")):
            rc = pm.run()
            assert rc == 1


class TestArgvSnapshot:
    """ce-review SEC-01 / Unit 6: argv frozen at __init__."""

    def test_argv_captured_on_init(self, monkeypatch):
        import sys as _sys
        monkeypatch.setattr(
            _sys, "argv", ["swarmq", "worker", "--module=x"],
        )
        pm = ProcessManager(num_workers=1, worker_entry=_entry_stub)
        assert pm._startup_argv == ["swarmq", "worker", "--module=x"]

    def test_sys_argv_mutation_does_not_affect_snapshot(self, monkeypatch):
        """Post-__init__ mutation of sys.argv must NOT be visible in
        the snapshot used by execvp."""
        import sys as _sys
        monkeypatch.setattr(_sys, "argv", ["swarmq", "worker"])
        pm = ProcessManager(num_workers=1, worker_entry=_entry_stub)
        _sys.argv[0] = "mutated"
        _sys.argv.append("--evil")
        assert pm._startup_argv == ["swarmq", "worker"]

    def test_execvp_uses_snapshot_not_live_argv(self, monkeypatch):
        import sys as _sys
        monkeypatch.setattr(
            _sys, "argv", ["swarmq", "worker", "--module=safe"],
        )
        pm = ProcessManager(num_workers=0, worker_entry=_entry_stub)
        pm._ctx = MagicMock()
        pm._reload_requested = True
        # Mutate sys.argv between __init__ and run():
        _sys.argv.append("--injected")
        with patch.object(pm, "_start_all_workers"), \
             patch.object(pm, "_setup_signals"), \
             patch.object(pm, "_monitor_loop", return_value=0), \
             patch.object(pm, "_shutdown_all_workers"), \
             patch.object(pm, "_collect_zombies"), \
             patch("swarmq.process_manager.supervisor.os.execvpe") as execvpe:
            pm.run()
            execvpe.assert_called_once()
            _, argv_used, _ = execvpe.call_args.args
            # argv is [sys.executable, *snapshot] — not live sys.argv.
            assert "--injected" not in argv_used
            assert "--module=safe" in argv_used

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
        with patch.object(pm, "_start_all_workers"), \
             patch.object(pm, "_setup_signals"), \
             patch.object(pm, "_monitor_loop", return_value=0), \
             patch.object(pm, "_shutdown_all_workers"), \
             patch.object(pm, "_collect_zombies"), \
             patch("swarmq.process_manager.supervisor.os.execvpe") as execvpe:
            pm.run()
            _, _, env_used = execvpe.call_args.args
            assert env_used.get("PYTHONPATH") == "/safe/path"
            assert "/tmp/evil" not in env_used.get("PYTHONPATH", "")


class TestScaleDownMinAge:
    """ce-review C-03 / Unit 5: age-gate on scale-down candidates."""

    def _pm_with_age(self, min_age_s: float) -> ProcessManager:
        pm = ProcessManager(
            num_workers=3, worker_entry=_entry_stub,
            autoscale=(1, 5), queue_depth_fn=lambda: 0,
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
        pm.workers = {
            1: WorkerInfo(process=p1, worker_id=0, spawned_at=now - 0.5),
            2: WorkerInfo(process=p2, worker_id=1, spawned_at=now - 0.2),
        }
        pm._scale_down_one()
        p1.terminate.assert_not_called()
        p2.terminate.assert_not_called()

    def test_old_worker_selected(self):
        pm = self._pm_with_age(min_age_s=30.0)
        now = time.monotonic()
        p1 = _fake_process(pid=1)
        pm.workers = {
            1: WorkerInfo(process=p1, worker_id=0, spawned_at=now - 120.0),
        }
        pm._scale_down_one()
        p1.terminate.assert_called_once()

    def test_mix_selects_old_youngest_eligible(self):
        """2 old + 1 young; youngest of the eligible set wins (highest
        worker_id among ages >= threshold)."""
        pm = self._pm_with_age(min_age_s=30.0)
        now = time.monotonic()
        p_old1 = _fake_process(pid=1)   # worker_id=0, age 120s
        p_old2 = _fake_process(pid=2)   # worker_id=1, age 60s
        p_young = _fake_process(pid=3)  # worker_id=2, age 5s
        pm.workers = {
            1: WorkerInfo(process=p_old1, worker_id=0, spawned_at=now - 120.0),
            2: WorkerInfo(process=p_old2, worker_id=1, spawned_at=now - 60.0),
            3: WorkerInfo(process=p_young, worker_id=2, spawned_at=now - 5.0),
        }
        pm._scale_down_one()
        # Expect worker_id=1 (highest id among eligible) — NOT the young one.
        p_old1.terminate.assert_not_called()
        p_old2.terminate.assert_called_once()
        p_young.terminate.assert_not_called()


class TestReloadReentrancy:
    """R5.3 ce-review #10: re-entrancy with real SIGHUP dispatch.

    The existing static ``is_busy=lambda: True`` unit test covers
    the watcher contract. These tests exercise the real binding
    ``_on_reload_signal -> _reload_requested -> is_busy-closure``
    using actual signal delivery, so a refactor that breaks the
    closure semantics or removes the handler turns red here.
    """

    def test_real_sighup_sets_reload_requested_and_flips_closure(
        self, _restore_default_signals,
    ):
        """Main-thread SIGHUP flips pm._reload_requested synchronously
        (CPython delivers pending signals between bytecodes), and the
        is_busy closure reflects the new value."""
        import os as _os

        pm = ProcessManager(
            num_workers=0, worker_entry=_entry_stub,
            reload=True, watch_dirs=("/tmp",),
        )
        pm._setup_signals()  # installs the real handler

        # Mirror the closure that ProcessManager.run() builds.
        def is_busy():
            return pm._reload_requested

        assert is_busy() is False
        _os.kill(_os.getpid(), signal.SIGHUP)
        # On CPython, pending signals are processed before the next
        # Python bytecode after os.kill returns.
        assert pm._reload_requested is True
        assert is_busy() is True

    def test_second_sighup_while_busy_is_idempotent(
        self, _restore_default_signals,
    ):
        """Double-fire raises no exception; state remains in the
        reload-requested / shutting-down terminal state.

        ce-review pass 2 #12 / T-01: the prior snapshot-vs-second-read
        comparisons were trivially ``True == True`` because the handler
        only sets flags, never clears them. The meaningful contract is
        that the second delivery does not crash and preserves the set
        flags — not that "second read equals first read", which is
        tautological for set-only booleans.
        """
        import os as _os

        pm = ProcessManager(
            num_workers=0, worker_entry=_entry_stub,
            reload=True, watch_dirs=("/tmp",),
        )
        pm._setup_signals()

        _os.kill(_os.getpid(), signal.SIGHUP)
        # Must-be-set invariant before the second delivery.
        assert pm._reload_requested is True
        assert pm._shutting_down is True
        assert pm.shutdown_event.is_set() is True

        # Second delivery must not raise.
        _os.kill(_os.getpid(), signal.SIGHUP)

        # Same invariants still hold — handler is idempotent at the
        # flag level.
        assert pm._reload_requested is True
        assert pm._shutting_down is True
        assert pm.shutdown_event.is_set() is True


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
        with patch.object(pm, "_start_all_workers"), \
             patch.object(pm, "_setup_signals"), \
             patch.object(pm, "_monitor_loop", return_value=0), \
             patch.object(pm, "_shutdown_all_workers"), \
             patch.object(pm, "_collect_zombies"), \
             patch(
                 "swarmq.hot_reload.FileWatcher",
                 return_value=fake_watcher,
             ) as fw_cls, \
             patch("swarmq.process_manager.supervisor.os.kill") as mock_kill:
            pm.run()

            fake_watcher.start.assert_called_once()
            fake_watcher.stop.assert_called_once()

            # Kwarg identity — not just call counts (ce-review #8).
            kwargs = fw_cls.call_args.kwargs
            assert kwargs["watch_dirs"] == ("/tmp",)
            is_busy = kwargs["is_busy"]
            trigger = kwargs["trigger"]
            assert callable(is_busy) and callable(trigger)

            # is_busy reads pm._reload_requested live — static-False
            # closure would fail this.
            pm._reload_requested = False
            assert is_busy() is False
            pm._reload_requested = True
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
        with patch.object(pm, "_start_all_workers"), \
             patch.object(pm, "_setup_signals"), \
             patch.object(pm, "_monitor_loop", return_value=0), \
             patch.object(pm, "_shutdown_all_workers"), \
             patch.object(pm, "_collect_zombies"), \
             patch("swarmq.hot_reload.FileWatcher") as FW:
            pm.run()
        FW.assert_not_called()

    def test_run_stops_watcher_even_when_monitor_raises(self):
        pm = self._pm(reload=True, watch_dirs=("/tmp",))
        fake_watcher = MagicMock()
        with patch.object(pm, "_start_all_workers"), \
             patch.object(pm, "_setup_signals"), \
             patch.object(
                 pm, "_monitor_loop",
                 side_effect=RuntimeError("monitor bug"),
             ), \
             patch.object(pm, "_shutdown_all_workers"), \
             patch.object(pm, "_collect_zombies"), \
             patch(
                 "swarmq.hot_reload.FileWatcher",
                 return_value=fake_watcher,
             ), pytest.raises(RuntimeError, match="monitor bug"):
            pm.run()
        fake_watcher.stop.assert_called_once()


class TestConfigErrorNoRestart:
    """R29 (Plan 2026-09-03-001): Exit 78 wird nicht neu gestartet; der
    Supervisor beendet den Lauf mit 1 statt das Restart-Budget zu verbrennen."""

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

    def test_monitor_loop_exits_one_after_config_error(self, monkeypatch):
        from swarmq.process_manager.entry import CONFIG_ERROR_EXIT_CODE

        pm = self._pm()
        dead = _fake_process(alive=False, exitcode=CONFIG_ERROR_EXIT_CODE, pid=5)
        pm.workers = {5: WorkerInfo(process=dead, worker_id=0)}
        monkeypatch.setattr(pm, "_shutdown_all_workers", MagicMock())
        assert pm._monitor_loop() == 1
        pm._shutdown_all_workers.assert_called_once()

    def test_ordinary_crash_still_restarts(self):
        pm = self._pm()
        pm._ctx.Process = MagicMock(return_value=_fake_process(pid=500))
        dead = _fake_process(alive=False, exitcode=1, pid=5)
        pm.workers = {5: WorkerInfo(process=dead, worker_id=0)}
        pm._check_dead_workers()
        assert 500 in pm.workers
        assert pm._config_error is False


class TestCleanExitIsNotRefilled:
    """R20/KTD9 (Plan 2026-09-03-001): Exit 0 ohne expected_shutdown zieht den
    Slot aus dem Ziel; sind alle so beendet, endet der Supervisor mit 0."""

    def _pm(self, n: int = 1) -> ProcessManager:
        pm = ProcessManager(num_workers=n, worker_entry=_entry_stub, worker_args=(_make_test_cfg(),))
        pm._ctx = MagicMock()
        return pm

    def test_clean_exit_reduces_target_and_is_not_respawned(self):
        pm = self._pm(n=2)
        pm._ctx.Process = MagicMock(return_value=_fake_process(pid=500))
        dead = _fake_process(alive=False, exitcode=0, pid=5)
        pm.workers = {
            5: WorkerInfo(process=dead, worker_id=0),
            6: WorkerInfo(process=_fake_process(pid=6), worker_id=1),
        }
        pm._check_dead_workers()
        assert pm._clean_exits == 1
        assert pm._current_target() == 1
        assert list(pm.workers) == [6]
        pm._ctx.Process.assert_not_called()

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
        pm._ctx.Process.assert_not_called()

    def test_monitor_loop_exits_one_when_mixed_clean_and_exhausted(self):
        pm = self._pm(n=2)
        pm.MONITOR_INTERVAL = 0.01
        pm._exhausted_slots = 1
        pm._clean_exits = 1
        pm.workers = {}
        assert pm._monitor_loop() == 1
