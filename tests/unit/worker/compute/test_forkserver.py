"""Tests fuer swarmq.worker.compute.forkserver — Bootstrap (U3: R5, R14, R15, R16)."""

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
        ctx.set_forkserver_preload.assert_called_once_with(["myapp.tasks"])
        assert handle.pid == 42
        assert handle.threads == 1
        assert handle.preload == ("myapp.tasks",)

    def test_multithreaded_forkserver_aborts_start(self, monkeypatch, tmp_path):
        ctx = _fake_ctx([])
        monkeypatch.setattr(fsmod, "forkserver_pid", lambda: 42)
        with pytest.raises(ConfigurationError, match="has 5 threads"):
            fsmod.start_forkserver(
                ["m"], context_factory=lambda name: ctx,
                available_methods=["forkserver"], proc_root=_status(tmp_path, 42, 5),
            )

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
        _status(root, 77, 3)
        handle = fsmod.start_forkserver(
            ["m"], context_factory=lambda name: ctx,
            available_methods=["forkserver"], proc_root=root,
        )
        assert handle.restarted() is False
        assert handle.restarted() is True
        assert handle.pid == 77
        assert handle.threads == 3
