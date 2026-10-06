"""Tests fuer swarmq.worker.compute.forkserver — Bootstrap (U3: R5, R14, R15, R16).

Test-Liste Plan 2026-10-06-1811, U6 (R6, R7; KTD6, KTD7):
1. [x] ``start_forkserver`` stellt ``swarmq.worker.compute.preload`` an die
       erste Stelle, auch fuer Aufrufer ohne das Modul, ohne es zu verdoppeln.
2. [x] Thread-Pruefung mit gefaelschtem ``proc_root`` (``Threads: 2``) und
       ``comm`` gleich dem swarmq-Listener: die Meldung nennt die Opt-in-Queue.
3. [x] ``comm`` ``Thread-1 (_moni``: die Meldung nennt einen App-``QueueListener``.
4. [x] Ein anderer Thread: der BLAS-Hinweis, und die Meldung nennt den Thread.
5. [x] ``ForkserverHandle.restarted()`` mit mehr als einem Thread:
       ``forkserver_restarted_multithreaded`` (ERROR) und eine Ausnahme fuer
       den Pool.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from swarmq.exceptions import ConfigurationError
from swarmq.worker.compute import forkserver as fsmod


def _status(tmp_path: Path, pid: int, threads: int) -> Path:
    d = tmp_path / str(pid)
    d.mkdir()
    (d / "status").write_text(f"Name:\tpython\nThreads:\t{threads}\n", encoding="ascii")
    return tmp_path


def _tasks(root: Path, pid: int, *names: str) -> Path:
    """``/proc/<pid>/task/<tid>/comm`` je Thread, dazu ``Threads:`` im Status."""
    _status(root, pid, len(names))
    for tid, name in enumerate(names, start=pid):
        task = root / str(pid) / "task" / str(tid)
        task.mkdir(parents=True)
        (task / "comm").write_text(f"{name}\n", encoding="ascii")
    return root


def _fake_ctx(recorder: list[str]) -> MagicMock:
    ctx = MagicMock(name="forkserver-ctx")
    ctx.set_forkserver_preload.side_effect = lambda mods: recorder.append("preload")
    warm = MagicMock()
    warm.is_alive.return_value = False
    warm.start.side_effect = lambda: recorder.append("warmup")
    ctx.Process.return_value = warm
    return ctx


class TestEnvironment:
    def test_sets_thread_and_malloc_vars_when_absent(self):
        env: dict[str, str] = {}
        result = fsmod.configure_compute_environment(
            thread_limit=1, malloc_trim_threshold=131072, malloc_arena_max=2, environ=env
        )
        for var in fsmod.THREAD_LIMIT_VARS:
            assert env[var] == "1"
        assert env["MALLOC_TRIM_THRESHOLD_"] == "131072"
        assert env["MALLOC_ARENA_MAX"] == "2"
        assert set(result.applied) == set(fsmod.THREAD_LIMIT_VARS) | {
            "MALLOC_TRIM_THRESHOLD_", "MALLOC_ARENA_MAX"
        }
        assert result.kept_from_operator == {}

    def test_operator_values_are_kept(self):
        env = {"OMP_NUM_THREADS": "4", "MALLOC_ARENA_MAX": "8"}
        result = fsmod.configure_compute_environment(
            thread_limit=1, malloc_trim_threshold=None, malloc_arena_max=2, environ=env
        )
        assert env["OMP_NUM_THREADS"] == "4"
        assert env["MALLOC_ARENA_MAX"] == "8"
        assert "MALLOC_TRIM_THRESHOLD_" not in env
        assert result.kept_from_operator == {"OMP_NUM_THREADS": "4", "MALLOC_ARENA_MAX": "8"}


class TestThreadCheck:
    def test_reads_threads_from_proc(self, tmp_path):
        assert fsmod.check_forkserver_threads(42, proc_root=_status(tmp_path, 42, 1)) == 1

    def test_missing_proc_returns_none(self, tmp_path):
        assert fsmod.check_forkserver_threads(42, proc_root=tmp_path) is None

    def test_forkserver_pid_survives_missing_private_api(self, monkeypatch):
        import multiprocessing.forkserver as real

        monkeypatch.delattr(real._forkserver, "_forkserver_pid", raising=False)
        assert fsmod.forkserver_pid() is None


class TestStartForkserver:
    def test_env_is_set_before_preload_and_warmup_runs(self, monkeypatch, tmp_path):
        order: list[str] = []
        ctx = _fake_ctx(order)
        monkeypatch.delenv("OMP_NUM_THREADS", raising=False)
        real_configure = fsmod.configure_compute_environment

        def spy(**kw):
            order.append("env")
            return real_configure(**kw)

        monkeypatch.setattr(fsmod, "configure_compute_environment", spy)
        monkeypatch.setattr(fsmod, "forkserver_pid", lambda: 42)
        handle = fsmod.start_forkserver(
            ["myapp.tasks"],
            context_factory=lambda name: ctx,
            available_methods=["forkserver", "spawn"],
            proc_root=_status(tmp_path, 42, 1),
        )
        assert order == ["env", "preload", "warmup"]
        ctx.set_forkserver_preload.assert_called_once_with([fsmod.PRELOAD_MODULE, "myapp.tasks"])
        assert handle.pid == 42
        assert handle.threads == 1
        assert handle.preload == (fsmod.PRELOAD_MODULE, "myapp.tasks")

    def test_preload_module_comes_first_exactly_once(self, monkeypatch, tmp_path):
        ctx = _fake_ctx([])
        monkeypatch.setattr(fsmod, "forkserver_pid", lambda: 42)
        handle = fsmod.start_forkserver(
            ["a", "swarmq.worker.compute.preload", "b"], context_factory=lambda name: ctx,
            available_methods=["forkserver"], proc_root=_status(tmp_path, 42, 1),
        )
        ctx.set_forkserver_preload.assert_called_once_with(["swarmq.worker.compute.preload", "a", "b"])
        assert handle.preload == ("swarmq.worker.compute.preload", "a", "b")

    def test_multithreaded_forkserver_aborts_start(self, monkeypatch, tmp_path):
        ctx = _fake_ctx([])
        monkeypatch.setattr(fsmod, "forkserver_pid", lambda: 42)
        with pytest.raises(ConfigurationError, match="has 5 threads"):
            fsmod.start_forkserver(
                ["m"], context_factory=lambda name: ctx,
                available_methods=["forkserver"], proc_root=_status(tmp_path, 42, 5),
            )

    def test_swarmq_listener_thread_is_blamed_on_the_opt_in_queue(self, monkeypatch, tmp_path):
        from swarmq.logging import LISTENER_THREAD_NAME

        monkeypatch.setattr(fsmod, "forkserver_pid", lambda: 42)
        with pytest.raises(ConfigurationError) as info:
            fsmod.start_forkserver(
                ["m"], context_factory=lambda name: _fake_ctx([]), available_methods=["forkserver"],
                proc_root=_tasks(tmp_path, 42, "python3", LISTENER_THREAD_NAME),
            )
        message = str(info.value)
        assert "has 2 threads" in message
        assert LISTENER_THREAD_NAME in message
        assert "log_queue=True" in message and "SWARMQ_LOG_QUEUE" in message
        assert "BLAS" not in message

    def test_stdlib_queue_listener_thread_is_blamed_on_the_app(self, monkeypatch, tmp_path):
        monkeypatch.setattr(fsmod, "forkserver_pid", lambda: 42)
        with pytest.raises(ConfigurationError) as info:
            fsmod.start_forkserver(
                ["m"], context_factory=lambda name: _fake_ctx([]), available_methods=["forkserver"],
                proc_root=_tasks(tmp_path, 42, "python3", "Thread-1 (_moni"),
            )
        message = str(info.value)
        assert "QueueListener" in message and "--configure" in message
        assert "BLAS" not in message and "log_queue=True" not in message

    def test_other_threads_get_the_blas_hint_and_are_named(self, monkeypatch, tmp_path):
        monkeypatch.setattr(fsmod, "forkserver_pid", lambda: 42)
        with pytest.raises(ConfigurationError) as info:
            fsmod.start_forkserver(
                ["m"], context_factory=lambda name: _fake_ctx([]), available_methods=["forkserver"],
                proc_root=_tasks(tmp_path, 42, "python3", "OpenBLAS-1"),
            )
        message = str(info.value)
        assert "OpenBLAS-1" in message and "BLAS/OpenMP" in message
        assert "QueueListener" not in message and "log_queue=True" not in message

    def test_missing_proc_is_a_warning_not_an_error(self, monkeypatch, tmp_path):
        ctx = _fake_ctx([])
        monkeypatch.setattr(fsmod, "forkserver_pid", lambda: 42)
        handle = fsmod.start_forkserver(
            ["m"], context_factory=lambda name: ctx,
            available_methods=["forkserver"], proc_root=tmp_path,
        )
        assert handle.threads is None

    def test_platform_without_forkserver_is_rejected(self):
        with pytest.raises(ConfigurationError, match="forkserver"):
            fsmod.start_forkserver(["m"], available_methods=["spawn"])

    def test_restart_detection_repeats_thread_check(self, monkeypatch, tmp_path):
        ctx = _fake_ctx([])
        pids = iter([42, 42, 77])
        monkeypatch.setattr(fsmod, "forkserver_pid", lambda: next(pids))
        root = _status(tmp_path, 42, 1)
        _status(root, 77, 1)
        handle = fsmod.start_forkserver(
            ["m"], context_factory=lambda name: ctx,
            available_methods=["forkserver"], proc_root=root,
        )
        assert handle.restarted() is False
        assert handle.restarted() is True
        assert handle.pid == 77
        assert handle.threads == 1

    def test_multithreaded_restart_is_rejected(self, monkeypatch, tmp_path, swarmq_logs):
        from swarmq.logging import LISTENER_THREAD_NAME

        pids = iter([42, 77])
        monkeypatch.setattr(fsmod, "forkserver_pid", lambda: next(pids))
        root = _status(tmp_path, 42, 1)
        _tasks(root, 77, "python3", LISTENER_THREAD_NAME)
        handle = fsmod.start_forkserver(
            ["m"], context_factory=lambda name: _fake_ctx([]),
            available_methods=["forkserver"], proc_root=root,
        )

        with pytest.raises(fsmod.ForkSourceUnsafe, match="has 2 threads"):
            handle.restarted()

        [entry] = [e for e in swarmq_logs if e["event"] == "forkserver_restarted_multithreaded"]
        assert entry["log_level"] == "error"
        assert entry["threads"] == 2
        assert entry["thread_names"] == ["python3", LISTENER_THREAD_NAME]
