"""Unit tests for R4.6 ProcessManager + R4.7 Autoscaling.

The supervisor is exercised via ``multiprocessing`` mocks so no real
subprocesses are spawned (important for reliability on CI and for
speed). The few paths that genuinely need a real child process are
out-of-scope for unit tests and are covered by a separate integration
suite if/when needed.
"""

from __future__ import annotations

import signal
import time
from unittest.mock import MagicMock, patch

import pytest

from swarmq.process_manager import (
    ProcessManager,
    WorkerInfo,
    _make_queue_depth_sampler,
    worker_subprocess_entry,
)


def _fake_process(alive: bool = True, exitcode: int | None = None, pid: int = 1234):
    p = MagicMock()
    p.is_alive.return_value = alive
    p.exitcode = exitcode
    p.pid = pid
    return p


def _entry_stub(worker_id, shutdown_event, *args):
    """No-op entry — never actually invoked in these unit tests."""
    return None


# ---------------------------------------------------------------------------
# Core: init + start_worker + shutdown
# ---------------------------------------------------------------------------


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

    def test_sigchld_handler_reaps_zombies(self):
        pm = ProcessManager(num_workers=1, worker_entry=_entry_stub)
        with patch("swarmq.process_manager.os.waitpid") as wp:
            wp.side_effect = [(1234, 0), (0, 0)]  # one zombie, then done
            pm._on_sigchld(signal.SIGCHLD, None)
            assert wp.call_count == 2

    def test_sigchld_handler_stops_on_child_error(self):
        pm = ProcessManager(num_workers=1, worker_entry=_entry_stub)
        with patch(
            "swarmq.process_manager.os.waitpid",
            side_effect=ChildProcessError(),
        ):
            # Should not raise.
            pm._on_sigchld(signal.SIGCHLD, None)


# ---------------------------------------------------------------------------
# Restart: rate limit + SIGHUP reload
# ---------------------------------------------------------------------------


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
             patch("swarmq.process_manager.os.execvp") as execvp:
            pm.run()
            execvp.assert_called_once()


# ---------------------------------------------------------------------------
# Autoscaling
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# ce-review residuals: worker_subprocess_entry + sampler
# ---------------------------------------------------------------------------


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

        async def fake_run_worker_async(ns):
            # Model a non-cooperative worker that does NOT consult
            # shutdown_event. Exit only when SIGTERM is "delivered".
            while not worker_exited.is_set():
                await asyncio.sleep(0.02)

        monkeypatch.setattr("swarmq.process_manager.os.kill", fake_kill)
        monkeypatch.setattr(
            "swarmq.cli._run_worker_async", fake_run_worker_async,
        )

        timer = threading.Timer(0.1, shutdown_event.set)
        timer.start()
        try:
            worker_subprocess_entry(
                worker_id=0,
                shutdown_event=shutdown_event,
                module="dummy.module",
                queues=["default"],
                concurrency=1,
                log_level="ERROR",
                log_format="human",
                burst=False,
                burst_idle_timeout=5.0,
                priorities=False,
                metrics_port=None,
                health_port=None,
            )
        finally:
            timer.cancel()

        import os
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

        monkeypatch.setattr("swarmq.cli._run_worker_async", raise_kbd)

        # Should return None, not raise.
        worker_subprocess_entry(
            worker_id=0,
            shutdown_event=threading.Event(),
            module="dummy.module",
            queues=["default"],
            concurrency=1,
            log_level="ERROR",
            log_format="human",
            burst=False,
            burst_idle_timeout=5.0,
            priorities=False,
            metrics_port=None,
            health_port=None,
        )


class TestQueueDepthSampler:
    """ce-review T-05 + REL-05: sampler success path + timeout bound."""

    def test_sums_multi_queue_depth(self, monkeypatch):
        from unittest.mock import AsyncMock

        class FakeBroker:
            def __init__(self, url):
                self.url = url

            async def connect(self):
                return None

            async def get_queue_depth(self, q):
                return {"a": 3, "b": 7, "c": 0}[q]

            async def close(self):
                return None

        monkeypatch.setattr(
            "swarmq.process_manager.RabbitMQBroker"
            if False else "swarmq.broker.rabbitmq.RabbitMQBroker",
            FakeBroker,
        )
        sampler = _make_queue_depth_sampler("amqp://x", ["a", "b", "c"])
        assert sampler() == 10

    def test_timeout_does_not_hang(self, monkeypatch):
        """A stuck broker.connect() must not block the sampler
        beyond SAMPLE_TIMEOUT_S (ce-review REL-05)."""
        import asyncio

        class HangingBroker:
            def __init__(self, url):
                pass

            async def connect(self):
                # Block effectively forever.
                await asyncio.sleep(3600)

            async def get_queue_depth(self, q):
                return 0

            async def close(self):
                return None

        monkeypatch.setattr(
            "swarmq.broker.rabbitmq.RabbitMQBroker", HangingBroker,
        )
        # Shrink the timeout so the test completes quickly while still
        # exercising the same code path.
        monkeypatch.setattr(
            "swarmq.process_manager.SAMPLE_TIMEOUT_S", 0.2,
        )
        sampler = _make_queue_depth_sampler("amqp://x", ["a"])

        # Sampler raises TimeoutError on the outer asyncio.timeout —
        # the caller (_autoscale_tick) swallows it. Verify the
        # exception type is a TimeoutError-compatible.
        with pytest.raises(TimeoutError):
            sampler()
