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
        claim_schedfire=lambda _m, _t: None, register_lock=lambda _h, _e: None,
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
