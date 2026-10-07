"""Tests fuer swarmq.worker.compute.executor gegen einen gefaelschten Pool (U6)."""

from __future__ import annotations

import asyncio
from typing import Any, ClassVar
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.exceptions import NoRetry, SoftTimeoutError
from swarmq.middleware import MiddlewareChain
from swarmq.task import DeliveryMode, Task, TaskInfo
from swarmq.worker.compute.envelope import error_envelope, success_envelope
from swarmq.worker.compute.executor import ComputeExecutor
from swarmq.worker.compute.pool import ChildDied, SlotUnavailable
from swarmq.worker.gates import GateContext, GateDeps


class FakePool:
    def __init__(self) -> None:
        self.behaviour: Any = success_envelope("t1", 42, slot=0)
        self.cancelled = 0  # SlotPool.run toetet das Kind selbst im Cancel-Pfad
        self.started = self.closed = False

    async def start(self) -> None:
        self.started = True

    async def aclose(self) -> None:
        self.closed = True

    async def run(self, job, *, use_di):
        self.job, self.use_di = job, use_di
        b = self.behaviour
        if b == "hang":
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                self.cancelled += 1
                raise
        if isinstance(b, BaseException):
            raise b
        return b


class Plain(Task):
    task_info = TaskInfo(name="plain", queues=["q"])

    async def run(self) -> int:
        return 1


class WithDi(Task):
    task_info = TaskInfo(name="with_di", queues=["q"])
    _dishka_injected_keys: ClassVar[list[str]] = ["x"]

    async def run(self) -> int:
        return 1


def _deps(**cfg: Any) -> GateDeps:
    config = SwarmQConfig(broker_url="amqp://x", backend_url="redis://x", cancel_poll_interval=0.02, **cfg)
    backend = MagicMock()
    backend.is_cancelled = AsyncMock(return_value=False)
    backend.set_progress = AsyncMock()
    backend.publish_progress = AsyncMock()
    signals = MagicMock()
    signals.dispatch = AsyncMock()
    return GateDeps(
        broker=MagicMock(), backend=backend, config=config, signals=signals, registry=MagicMock(),
        rate_limiter=lambda: None, lock_manager=lambda: None, finalize_schedfire=AsyncMock(),
        maybe_release_unique=AsyncMock(), extract_valid_workflow_id=lambda _m: None,
        claim_schedfire=lambda _m, _k, _o: None, register_lock=lambda _h, _e: None,
    )


def _ctx(task_class: type = Plain, **info: Any) -> GateContext:
    task_info = TaskInfo(name=task_class.task_info.name, queues=["q"], **info)
    return GateContext(task_id="t1", task_name=task_class.task_info.name, queue="q", task_info=task_info,
                       task_class=task_class, args=[1, 2], kwargs={"k": "v"})


@pytest.fixture
def setup():
    deps = _deps()
    pool = FakePool()
    ex = ComputeExecutor(deps, pool, worker_id="w1")  # type: ignore[arg-type]
    return deps, pool, ex


@pytest.mark.timeout(5)
async def test_success_envelope_becomes_result_and_job_is_minimal(setup):
    deps, pool, ex = setup
    res = await ex.execute(_ctx(), {"retry_count": 3}, MiddlewareChain([]))
    assert res.result_value == 42 and not res.is_error and res.di_scope is None
    assert pool.job.args == (1, 2) and pool.job.kwargs == {"k": "v"} and pool.job.retry_count == 3
    assert pool.job.worker_id == "w1" and pool.use_di is False
    dispatched = [c.args[0].name for c in deps.signals.dispatch.await_args_list]
    assert dispatched == ["TASK_STARTED", "TASK_COMPLETED"]


@pytest.mark.timeout(5)
async def test_di_task_selects_reused_child(setup):
    _, pool, ex = setup
    await ex.execute(_ctx(WithDi), {"retry_count": 0}, MiddlewareChain([]))
    assert pool.use_di is True


@pytest.mark.timeout(5)
async def test_error_envelope_keeps_exception_type_for_retry_path(setup):
    deps, pool, ex = setup
    pool.behaviour = error_envelope("t1", NoRetry("nope"), slot=0)
    res = await ex.execute(_ctx(), {"retry_count": 0}, MiddlewareChain([]))
    assert res.is_error and isinstance(res.task_exception, NoRetry)
    assert [c.args[0].name for c in deps.signals.dispatch.await_args_list][-1] == "TASK_FAILED"


@pytest.mark.timeout(5)
async def test_soft_timeout_message_mirrors_today(setup):
    _, pool, ex = setup
    pool.behaviour = error_envelope("t1", SoftTimeoutError("soft timeout after 1s"), slot=0)
    res = await ex.execute(_ctx(), {"retry_count": 0}, MiddlewareChain([]))
    assert res.error_message.startswith("SoftTimeoutError:")


@pytest.mark.timeout(5)
async def test_hard_timeout_kills_child_and_fails_like_watchdog(setup):
    _, pool, ex = setup
    pool.behaviour = "hang"
    res = await ex.execute(_ctx(hard_timeout=0.05), {"retry_count": 0}, MiddlewareChain([]))
    assert res.is_error and not res.is_cancelled
    assert isinstance(res.task_exception, RuntimeError) and "hard timeout after 0.05s" in res.error_message
    assert pool.cancelled == 1  # der Pool toetet im Cancel-Pfad, nicht der Executor


@pytest.mark.timeout(5)
async def test_cancel_flag_kills_child_and_takes_cancel_path(setup):
    deps, pool, ex = setup
    pool.behaviour = "hang"
    deps.backend.is_cancelled = AsyncMock(return_value=True)
    res = await ex.execute(_ctx(), {"retry_count": 0}, MiddlewareChain([]))
    assert res.is_cancelled and not res.is_error
    assert pool.cancelled == 1
    assert [c.args[0].name for c in deps.signals.dispatch.await_args_list][-1] == "TASK_CANCELLED"


@pytest.mark.timeout(5)
async def test_outer_cancel_during_drain_kills_child_and_returns_error_result(setup):
    _, pool, ex = setup
    pool.behaviour = "hang"
    handler = asyncio.create_task(ex.execute(_ctx(), {"retry_count": 0}, MiddlewareChain([])))
    await asyncio.sleep(0.05)
    handler.cancel()
    res = await handler  # KTD2: kein propagierter CancelledError
    assert res.is_error and "drain" in res.error_message
    assert pool.cancelled == 1


@pytest.mark.timeout(5)
async def test_child_death_is_an_error_result(setup):
    _, pool, ex = setup
    pool.behaviour = ChildDied("pid 7 exited without result")
    res = await ex.execute(_ctx(), {"retry_count": 0}, MiddlewareChain([]))
    assert res.is_error and isinstance(res.task_exception, ChildDied) and not res.slot_unavailable


@pytest.mark.timeout(5)
async def test_slot_unavailable_is_flagged(setup):
    _, pool, ex = setup
    pool.behaviour = SlotUnavailable("no ready")
    res = await ex.execute(_ctx(), {"retry_count": 0}, MiddlewareChain([]))
    assert res.is_error and res.slot_unavailable


@pytest.mark.timeout(5)
async def test_progress_sink_forwards_to_backend(setup):
    deps, _, ex = setup
    await ex.progress_sink(("progress", "t1", {"current": "1"}, 5000))
    await ex.progress_sink(("publish", "t1", b"{}"))
    deps.backend.set_progress.assert_awaited_once_with("t1", {"current": "1"}, ttl_ms=5000)
    deps.backend.publish_progress.assert_awaited_once_with("t1", b"{}")


@pytest.mark.timeout(5)
async def test_progress_sink_failure_logs_warning_with_error_type_and_error(setup, swarmq_logs):
    deps, _, ex = setup
    deps.backend.set_progress = AsyncMock(side_effect=ConnectionError("valkey down"))
    await ex.progress_sink(("progress", "t1", {"current": "1"}, 5000))
    [entry] = [
        e for e in swarmq_logs
        if e["logger"] == "swarmq.worker.compute.executor" and e["event"] == "compute_progress_forward_failed"
    ]
    assert entry["log_level"] == "warning"
    assert entry["error_type"] == "ConnectionError" and entry["error"] == "valkey down"


@pytest.mark.timeout(5)
async def test_start_and_aclose_delegate_to_pool(setup):
    _, pool, ex = setup
    await ex.start()
    await ex.aclose()
    assert pool.started and pool.closed


class ComputeFireAndForget(Task):
    task_info = TaskInfo(name="compute_faf", queues=["q"], delivery=DeliveryMode.FIRE_AND_FORGET)

    async def run(self) -> int:
        return 1


@pytest.mark.timeout(5)
async def test_slot_unavailable_on_fire_and_forget_logs_the_dedicated_loss_message(setup):
    """Zweit-Review #4 / Erst-Review #14: die Nachricht ist bereits bestaetigt und verloren —
    eigene Meldung statt eines generischen Task-Fehlers."""
    from swarmq.worker.compute import executor as executor_module

    _, pool, ex = setup
    pool.behaviour = SlotUnavailable("compute child for slot 0 sent no ready signal")
    fake_logger = MagicMock()
    monkeypatch_target = executor_module.logger
    executor_module.logger = fake_logger
    try:
        res = await ex.execute(
            _ctx(ComputeFireAndForget, delivery=DeliveryMode.FIRE_AND_FORGET), {"retry_count": 0}, MiddlewareChain([])
        )
    finally:
        executor_module.logger = monkeypatch_target
    assert res.is_error and res.slot_unavailable
    events = [call.args[0] for call in fake_logger.error.call_args_list]
    assert "compute_slot_unavailable_fire_and_forget_dropped" in events
    assert "fire_and_forget_error" not in events


def _executor_logs(entries: list[dict[str, Any]], event: str) -> list[dict[str, Any]]:
    return [e for e in entries if e["logger"] == "swarmq.worker.compute.executor" and e["event"] == event]


@pytest.mark.timeout(5)
async def test_fire_and_forget_error_logs_error_type_and_error(setup, swarmq_logs):
    """Plan 2026-10-06-0831, U9 (R17): einheitliches Fehlerschema."""
    _, pool, ex = setup
    pool.behaviour = ChildDied("pid 7 exited without result")
    await ex.execute(
        _ctx(ComputeFireAndForget, delivery=DeliveryMode.FIRE_AND_FORGET), {"retry_count": 0}, MiddlewareChain([])
    )
    [entry] = _executor_logs(swarmq_logs, "fire_and_forget_error")
    assert entry["log_level"] == "error"
    assert entry["error_type"] == "ChildDied"
    assert entry["error"] == "pid 7 exited without result"


# --------------------------------------------------------------------------- Logging der Kinder
# Plan 2026-10-06-0831, U7 Teil b (KTD7, KTD9, KTD14). Test-Liste:
# 1. [x] Unter ``swarmq worker`` traegt der Job die im Supervisor aufgeloesten
#        Einstellungen aus ``SwarmQConfig.logging_settings``.
# 2. [x] Eingebettet (ohne Settings-Objekt) stammen sie aus den Logging-Feldern
#        von ``SwarmQConfig``.
# 4. [x] Plan 2026-10-06-1811, U4 (KTD11): ``log_drain_timeout`` reist in
#        beiden Faellen mit dem Job ins Kind.
# 3. [ ] Eingebettet mit ``log_config`` laedt der Executor die Datei einmal und
#        gibt sie als JSON mit; die Kinder lesen sie nie.


@pytest.mark.timeout(5)
async def test_job_carries_the_supervisor_resolved_logging_settings():
    from swarmq.logging_settings import LoggingSettings

    settings = LoggingSettings(
        level="DEBUG", level_explicit=True, log_config="/abs/logging.json",
        log_config_json='{"version": 1}', logger_levels=(("aio_pika", "WARNING"),), queue_size=500,
        drain_timeout=0.75,
    )
    pool = FakePool()
    ex = ComputeExecutor(_deps(logging_settings=settings), pool, worker_id="w1")  # type: ignore[arg-type]
    await ex.execute(_ctx(), {"retry_count": 0}, MiddlewareChain([]))
    job = pool.job
    assert (job.log_level, job.log_level_explicit, job.log_config, job.log_config_json) == (
        "DEBUG", True, "/abs/logging.json", '{"version": 1}',
    )
    assert (job.configure_logging, job.logger_levels, job.log_queue_size) == (
        True, (("aio_pika", "WARNING"),), 500,
    )
    assert job.log_drain_timeout == 0.75


@pytest.mark.timeout(5)
async def test_embedded_job_takes_the_logging_fields_of_swarmq_config():
    pool = FakePool()
    deps = _deps(
        log_level="WARNING", log_json=False, configure_logging=False,
        logger_levels=(("aio_pika", "ERROR"),), log_queue_size=200, log_drain_timeout=1.5,
    )
    ex = ComputeExecutor(deps, pool, worker_id="w1")  # type: ignore[arg-type]
    await ex.execute(_ctx(), {"retry_count": 0}, MiddlewareChain([]))
    job = pool.job
    assert (job.log_level, job.log_json, job.log_config, job.log_config_json) == ("WARNING", False, None, None)
    assert (job.configure_logging, job.logger_levels, job.log_queue_size) == (
        False, (("aio_pika", "ERROR"),), 200,
    )
    assert job.log_drain_timeout == 1.5


@pytest.mark.timeout(5)
async def test_embedded_log_config_is_loaded_once_and_travels_as_json(tmp_path):
    import json

    path = tmp_path / "logging.json"
    path.write_text(json.dumps({"version": 1, "root": {"level": "INFO"}}))
    pool = FakePool()
    ex = ComputeExecutor(_deps(log_config=str(path)), pool, worker_id="w1")  # type: ignore[arg-type]
    path.unlink()  # danach liest niemand mehr die Datei

    await ex.execute(_ctx(), {"retry_count": 0}, MiddlewareChain([]))
    job = pool.job
    assert job.log_config == str(path)
    assert json.loads(job.log_config_json) == {
        "version": 1, "root": {"level": "INFO"}, "disable_existing_loggers": False,
    }


@pytest.mark.timeout(5)
async def test_slot_unavailable_on_fire_and_forget_logs_error_type_and_error(setup, swarmq_logs):
    _, pool, ex = setup
    pool.behaviour = SlotUnavailable("compute child for slot 0 sent no ready signal")
    await ex.execute(
        _ctx(ComputeFireAndForget, delivery=DeliveryMode.FIRE_AND_FORGET), {"retry_count": 0}, MiddlewareChain([])
    )
    [entry] = _executor_logs(swarmq_logs, "compute_slot_unavailable_fire_and_forget_dropped")
    assert entry["error_type"] == "SlotUnavailable"
    assert entry["error"] == "compute child for slot 0 sent no ready signal"
    assert entry["task_id"] and "hint" in entry


@pytest.mark.timeout(5)
async def test_job_carries_the_workflow_id_of_the_delivery(setup):
    """Plan 2026-10-06-0831, U10 (KTD13): das Kind bindet ``workflow_id``
    in seinen Logkontext und braucht sie deshalb im Job."""
    _, pool, ex = setup
    ctx = _ctx()
    ctx.workflow_id = "0190a3c4-5d6e-7f80-9a1b-2c3d4e5f6a7b"

    await ex.execute(ctx, {"retry_count": 0}, MiddlewareChain([]))

    assert pool.job.workflow_id == "0190a3c4-5d6e-7f80-9a1b-2c3d4e5f6a7b"


# Lebenszyklus-Events im Compute-Pool (Plan 2026-10-06-0831, U11, KTD12):
# - task_started (Lebenszyklus-Level) vor task_succeeded (INFO) von swarmq.task.
# - task_succeeded/task_failed tragen compute_child, die PID aus dem Envelope.
def _lifecycle(logs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [e for e in logs if e["logger"] == "swarmq.task"]


@pytest.mark.timeout(5)
async def test_compute_pool_reports_started_then_succeeded(setup, swarmq_logs):
    _, _, ex = setup
    await ex.execute(_ctx(), {"retry_count": 0}, MiddlewareChain([]))

    started, succeeded = _lifecycle(swarmq_logs)
    assert (started["event"], started["log_level"]) == ("task_started", "debug")
    assert (succeeded["event"], succeeded["log_level"]) == ("task_succeeded", "info")
    assert succeeded["duration_ms"] >= 0


@pytest.mark.timeout(5)
async def test_compute_pool_succeeded_carries_the_pid_of_the_child(setup, swarmq_logs):
    _, pool, ex = setup
    pool.behaviour = success_envelope("t1", 42, slot=0, pid=4242)

    await ex.execute(_ctx(), {"retry_count": 0}, MiddlewareChain([]))

    [succeeded] = [e for e in _lifecycle(swarmq_logs) if e["event"] == "task_succeeded"]
    assert succeeded["compute_child"] == 4242


@pytest.mark.timeout(5)
async def test_compute_pool_failed_carries_error_fields_and_the_pid_of_the_child(setup, swarmq_logs):
    _, pool, ex = setup
    pool.behaviour = error_envelope("t1", ValueError("boom"), slot=0, pid=4242)

    await ex.execute(_ctx(), {"retry_count": 0}, MiddlewareChain([]))

    [failed] = [e for e in _lifecycle(swarmq_logs) if e["event"] == "task_failed"]
    assert (failed["log_level"], failed["error_type"], failed["error"]) == ("info", "ValueError", "boom")
    assert failed["compute_child"] == 4242
