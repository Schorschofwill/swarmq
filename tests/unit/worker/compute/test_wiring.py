"""Tests fuer swarmq.worker.compute.wiring (U7).

Plan 2026-10-06-1811, U9 (KTD9):
- [x] Vor dem Forkserver-Start steht die aufgeloeste ``configure_logging``-
  Einstellung als ``SWARMQ_FORKSERVER_LOG_SENTINEL`` (``"1"``/``"0"``) in der
  Umgebung -- der Preload liest sie dort.

Plan 2026-10-06-1811, U10 (KTD7):
- [x] Der ``/ready``-Grund eines ungesunden Pools nennt auch eine verworfene
  Kindquelle (``forkserver_restarted_multithreaded``).

Schluss-Review (Abweichung vom Plan, freigegeben):
- [x] Verwirft der Pool die Kindquelle, hoert der Worker auf zu konsumieren:
  ERROR-Event ``compute_fork_source_rejected`` und geordneter Shutdown;
  ``Worker.start`` wirft danach ``ForkSourceRejected`` (tests/unit/worker/
  test_worker.py), der Worker-Subprozess endet mit 71.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.signals import Signal
from swarmq.task import TaskInfo
from swarmq.worker.compute import wiring
from swarmq.worker.compute.executor import ComputeExecutor
from swarmq.worker.compute.forkserver import MAIN_MODULE_ENV, SENTINEL_ENV, ForkserverHandle
from swarmq.worker.compute.pool import SlotPool


class _T:
    task_info = TaskInfo(name="t", queues=["q"], hard_timeout=3600)


class _Registry:
    def __init__(self, classes: dict[str, type]) -> None:
        self._c = classes

    def get_all(self) -> dict[str, type]:
        return self._c


def _worker(pool: str = "cpu", **cfg):
    config = SwarmQConfig(broker_url="amqp://x", backend_url="redis://x", pool=pool, concurrency=2, **cfg)
    dispatcher = MagicMock()
    dispatcher.dispatch = AsyncMock()
    return SimpleNamespace(
        _config=config,
        _registry=_Registry({"t": _T}),
        _gate_deps=MagicMock(config=config),
        _signal_dispatcher=dispatcher,
        _executor="original",
    )


def test_preload_modules_are_the_task_modules():
    # Unter pytest ist __main__ ein Konsolenskript (per Pfad geladen) -> "__main__".
    # U8 (KTD10): das Preload-Modul kommt zuerst, das Hauptmodul bleibt enthalten.
    assert wiring.preload_modules(_Registry({"t": _T})) == [
        "swarmq.worker.compute.preload",
        "__main__",
        "swarmq.logging",
        "swarmq.worker.compute.child",
        __name__,
    ]


@pytest.mark.timeout(5)
async def test_io_pool_leaves_executor_untouched():
    w = _worker(pool="io")
    assert await wiring.start_compute_executor(w) is None
    assert w._executor == "original"


@pytest.mark.timeout(5)
async def test_cpu_pool_installs_compute_executor_and_recycle_signal(monkeypatch):
    w = _worker()
    handle = ForkserverHandle(context=MagicMock(), preload=(__name__,), pid=42, threads=1)
    seen: dict[str, object] = {}

    def fake_start(preload, **kw):
        seen["preload"], seen["kw"] = preload, kw
        return handle

    monkeypatch.setattr(wiring, "start_forkserver", fake_start)
    aclose = AsyncMock()
    monkeypatch.setattr(SlotPool, "start", AsyncMock())
    monkeypatch.setattr(SlotPool, "aclose", aclose)
    executor = await wiring.start_compute_executor(w)
    assert isinstance(executor, ComputeExecutor)
    assert w._executor is executor
    assert seen["preload"] == [
        "swarmq.worker.compute.preload",
        "__main__",
        "swarmq.logging",
        "swarmq.worker.compute.child",
        __name__,
    ]
    assert seen["kw"] == {"thread_limit": 1, "malloc_trim_threshold": 131072, "malloc_arena_max": 2}
    assert w._compute_handle is handle
    pool = executor._pool
    assert pool.recycle_rss_mb == 250
    assert pool.recycle_max_tasks == 1000
    await pool._on_recycle(1, "rss", 320.0)
    w._signal_dispatcher.dispatch.assert_awaited_once()
    args, kwargs = w._signal_dispatcher.dispatch.await_args
    assert args[0] is Signal.WORKER_RECYCLING
    assert kwargs["kind"] == "compute_child"
    assert kwargs["slot"] == 1
    assert kwargs["reason"] == "rss"
    await wiring.close_compute_executor(w)
    aclose.assert_awaited_once()


def test_hard_timeout_over_consumer_timeout_warns(swarmq_logs):
    wiring.warn_hard_timeout_over_consumer_timeout(_Registry({"t": _T}), consumer_timeout_s=1800)
    wiring.warn_hard_timeout_over_consumer_timeout(_Registry({"t": _T}), consumer_timeout_s=7200)
    logs = [entry for entry in swarmq_logs if entry["logger"] == "swarmq.worker.compute.wiring"]
    events = [entry["event"] for entry in logs]
    assert events == ["hard_timeout_exceeds_consumer_timeout"]
    assert logs[0]["tasks"] == ["t(3600s)"]


@pytest.mark.timeout(5)
async def test_forkserver_ready_reports_running_and_dead(monkeypatch, tmp_path):
    w = SimpleNamespace()
    assert await wiring.forkserver_ready(w) == (False, "compute pool not started")
    w._compute_handle = ForkserverHandle(context=None, preload=(), pid=42, threads=1)
    monkeypatch.setattr(wiring, "forkserver_pid", lambda: 42)
    (tmp_path / "42").mkdir()
    assert await wiring.forkserver_ready(w, proc_root=tmp_path) == (True, None)
    monkeypatch.setattr(wiring, "forkserver_pid", lambda: 43)
    assert await wiring.forkserver_ready(w, proc_root=tmp_path) == (False, "forkserver not running")
    monkeypatch.setattr(wiring, "forkserver_pid", lambda: None)
    assert await wiring.forkserver_ready(w, proc_root=tmp_path) == (True, None)


@pytest.mark.timeout(5)
async def test_forkserver_ready_reports_an_unhealthy_pool(monkeypatch, tmp_path):
    from types import SimpleNamespace

    from swarmq.worker.compute import wiring

    monkeypatch.setattr(wiring, "forkserver_pid", lambda: None)
    worker = SimpleNamespace(_compute_handle=object(), _executor=SimpleNamespace(pool_healthy=False))
    ok, reason = await wiring.forkserver_ready(worker, proc_root=tmp_path)
    assert ok is False
    assert "unhealthy" in (reason or "")
    worker._executor.pool_healthy = True
    assert (await wiring.forkserver_ready(worker, proc_root=tmp_path)) == (True, None)


@pytest.mark.timeout(5)
async def test_forkserver_ready_names_a_rejected_fork_source_as_a_cause(monkeypatch, tmp_path):
    """Plan 2026-10-06-1811, KTD7: Ein still neu gestarteter, mehrfaediger
    Forkserver macht den Pool dauerhaft ungesund; der ``/ready``-Grund nennt
    diese Ursache neben Handshake-Fehlern und blockierten Slots."""
    monkeypatch.setattr(wiring, "forkserver_pid", lambda: None)
    worker = SimpleNamespace(_compute_handle=object(), _executor=SimpleNamespace(pool_healthy=False))
    _, reason = await wiring.forkserver_ready(worker, proc_root=tmp_path)
    assert "fork source rejected" in (reason or "")


def test_preload_includes_the_child_runtime_modules():
    """Messnachweis M0: ohne Preload importiert jedes frische Kind child.py und swarmq.logging
    selbst — Spawn->Ready 33 ms statt 8 ms. Der Forkserver laedt sie deshalb vorab (R17)."""
    from types import SimpleNamespace

    from swarmq.worker.compute.wiring import preload_modules

    class Demo:
        __module__ = "tests.demo_tasks"

    registry = SimpleNamespace(get_all=lambda: {"demo": Demo})
    preload = preload_modules(registry)
    assert preload[:4] == [
        "swarmq.worker.compute.preload",
        "__main__",
        "swarmq.logging",
        "swarmq.worker.compute.child",
    ]
    assert preload.count("__main__") == 1
    assert "tests.demo_tasks" in preload


@pytest.mark.timeout(5)
async def test_cpu_pool_sets_main_module_env_before_forkserver_start(monkeypatch):
    """U8 (KTD10): der Forkserver-Preload liest den Hauptmodulnamen aus ``SWARMQ_MAIN_MODULE``."""
    import importlib.machinery
    import os
    import sys
    from types import ModuleType

    fake_main = ModuleType("pkg.mod")
    fake_main.__spec__ = importlib.machinery.ModuleSpec("pkg.mod", None)
    monkeypatch.setitem(sys.modules, "__main__", fake_main)  # wie bei ``python -m pkg.mod``
    monkeypatch.delenv(MAIN_MODULE_ENV, raising=False)
    seen: dict[str, Any] = {}

    def fake_start(preload, **kw):
        seen["env"], seen["preload"] = os.environ.get(MAIN_MODULE_ENV), preload
        return ForkserverHandle(context=MagicMock(), preload=tuple(preload), pid=42, threads=1)

    monkeypatch.setattr(wiring, "start_forkserver", fake_start)
    monkeypatch.setattr(SlotPool, "start", AsyncMock())
    await wiring.start_compute_executor(_worker())
    assert seen["env"] == "pkg.mod"
    assert seen["preload"][:2] == ["swarmq.worker.compute.preload", "pkg.mod"]


def test_preload_list_never_contains_the_configure_hook():
    """U5/KTD5: Der ``--configure``-Hook gehoert NICHT in die Preload-Liste.

    Der Forkserver laedt seine Preload-Module einmal und gibt sie an jedes
    Rechenkind weiter. Anwendungscode aus dem Hook liefe damit im
    Forkserver-Prozess -- vor jeder Task-Ausfuehrung, ohne Broker und ohne
    Backend. Der Hook laeuft ausschliesslich im Worker (``run_worker_async``).
    """
    w = _worker()
    w._config.configure = "tests.integration._cli_hook_module:setup"
    preload = wiring.preload_modules(w._registry)
    assert not any("hook" in name for name in preload), preload


@pytest.mark.timeout(5)
@pytest.mark.parametrize(("configure", "expected"), [(True, "1"), (False, "0")])
async def test_cpu_pool_passes_the_configure_setting_to_the_forkserver(monkeypatch, configure, expected):
    import os

    monkeypatch.delenv(SENTINEL_ENV, raising=False)
    seen: dict[str, object] = {}

    def fake_start(preload, **kw):
        seen["env"] = os.environ.get(SENTINEL_ENV)
        return ForkserverHandle(context=MagicMock(), preload=tuple(preload), pid=42, threads=1)

    monkeypatch.setattr(wiring, "start_forkserver", fake_start)
    monkeypatch.setattr(SlotPool, "start", AsyncMock())
    await wiring.start_compute_executor(_worker(configure_logging=configure))
    assert seen["env"] == expected


@pytest.mark.timeout(5)
async def test_a_rejected_fork_source_stops_the_worker(monkeypatch, swarmq_logs):
    from swarmq.worker.compute.envelope import ComputeJob
    from swarmq.worker.compute.forkserver import ForkSourceUnsafe
    from swarmq.worker.compute.pool import SlotUnavailable

    def restarted() -> bool:
        raise ForkSourceUnsafe("restarted forkserver has 2 threads")

    handle = SimpleNamespace(context=MagicMock(), pid=42, restarted=restarted)
    monkeypatch.setattr(wiring, "start_forkserver", lambda preload, **kw: handle)
    w = _worker()
    w._request_shutdown = MagicMock()
    executor = await wiring.start_compute_executor(w)
    assert executor is not None
    job = ComputeJob(task_id="t1", task_name="t", queue="q", args=(), kwargs={})

    with pytest.raises(SlotUnavailable):
        await executor._pool.run(job, use_di=False)

    w._request_shutdown.assert_called_once_with()
    [entry] = [e for e in swarmq_logs if e["event"] == "compute_fork_source_rejected"]
    assert entry["log_level"] == "error"
    assert "2 threads" in entry["reason"]
    await wiring.close_compute_executor(w)
