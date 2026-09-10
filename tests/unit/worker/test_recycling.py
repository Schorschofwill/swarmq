"""Tests fuer swarmq.worker.recycling — Selbst-Recycling des IO-Workers (U10)."""

from __future__ import annotations

import asyncio
import multiprocessing

import pytest
import structlog

from swarmq import supervision as sv
from swarmq.signals import Signal
from swarmq.worker import recycling
from swarmq.worker.lifecycle import WorkerState


@pytest.fixture
def channel():
    ch = sv.SupervisorChannel.create(multiprocessing.get_context("spawn"))
    sv.set_current_channel(ch)
    yield ch
    sv.set_current_channel(None)


async def _wait_for(pred, timeout=3.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while not pred():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.02)


def _arm(worker, **cfg):
    for k, v in cfg.items():
        setattr(worker._config, k, v)
    worker._state = WorkerState.RUNNING
    events: list[dict] = []

    async def _rec(**kw):
        events.append(kw)

    worker._signal_dispatcher.on_signal(Signal.WORKER_RECYCLING)(_rec)
    return events


@pytest.mark.timeout(5)
async def test_thresholds_off_means_no_monitor(worker):
    _arm(worker)
    worker._start_recycle_monitor()
    assert worker._recycle_monitor_task is None
    assert not recycling.recycling_enabled(worker._config)


@pytest.mark.timeout(5)
async def test_cpu_pool_does_not_run_the_io_recycler(worker):
    _arm(worker, pool="cpu", recycle_max_tasks=1)
    worker._start_recycle_monitor()
    assert worker._recycle_monitor_task is None


@pytest.mark.timeout(10)
async def test_task_count_threshold_recycles_with_ack(worker, channel, monkeypatch):
    monkeypatch.setattr(recycling, "RECYCLE_ACK_TIMEOUT_S", 2.0)
    events = _arm(worker, recycle_max_tasks=2)
    worker._start_recycle_monitor()
    for _ in range(2):
        await worker._signal_dispatcher.dispatch(Signal.TASK_COMPLETED, task_id="t", task_name="n", queue="q")
    await _wait_for(lambda: channel.intent_value() == sv.INTENT_RECYCLE)
    assert worker._state is WorkerState.RUNNING  # wartet auf die Bestaetigung
    channel.acknowledge_replacement()
    await _wait_for(lambda: worker._state is WorkerState.DRAINING)
    assert worker._recycle_ack_missing is False
    assert events and events[0]["kind"] == "worker" and events[0]["reason"] == "tasks" and events[0]["value"] == 2.0
    assert worker._drain_timeout_override == float(worker._config.shutdown_timeout)


@pytest.mark.timeout(10)
async def test_missing_ack_marks_failure_and_still_drains(worker, channel, monkeypatch):
    monkeypatch.setattr(recycling, "RECYCLE_ACK_TIMEOUT_S", 0.3)
    _arm(worker, recycle_max_tasks=1)
    worker._start_recycle_monitor()
    await worker._signal_dispatcher.dispatch(Signal.TASK_FAILED, task_id="t", task_name="n", queue="q")
    await _wait_for(lambda: worker._state is WorkerState.DRAINING, timeout=4.0)
    assert worker._recycle_ack_missing is True


@pytest.mark.timeout(10)
async def test_without_supervisor_recycling_is_a_graceful_exit_with_warning(worker):
    _arm(worker, recycle_max_tasks=1)
    with structlog.testing.capture_logs() as logs:
        worker._start_recycle_monitor()
        await worker._signal_dispatcher.dispatch(Signal.TASK_CANCELLED, task_id="t", task_name="n", queue="q")
        await _wait_for(lambda: worker._state is WorkerState.DRAINING)
    events = [entry["event"] for entry in logs]
    assert "recycling_without_supervisor" in events
    assert "recycle_without_supervisor_exit0" in events
    assert worker._recycle_ack_missing is False


@pytest.mark.timeout(10)
async def test_rss_threshold_wins_over_task_count(worker, channel, monkeypatch):
    monkeypatch.setattr(recycling, "rss_mb", lambda *_a, **_k: 320.0)
    events = _arm(worker, recycle_rss_mb=300, recycle_max_tasks=1000)
    worker._start_recycle_monitor()
    channel.acknowledge_replacement()
    await worker._signal_dispatcher.dispatch(Signal.TASK_COMPLETED, task_id="t", task_name="n", queue="q")
    await _wait_for(lambda: worker._state is WorkerState.DRAINING)
    assert events[0]["reason"] == "rss" and events[0]["value"] == 320.0


def test_drain_timeout_uses_largest_hard_timeout_plus_buffer(worker):
    from swarmq.task import Task, TaskInfo

    class Long(Task):
        task_info = TaskInfo(name="long_hard", queues=["q"], hard_timeout=120)

    worker._registry.register(Long)
    assert recycling.recycle_drain_timeout(worker) == 120 + recycling.RECYCLE_DRAIN_BUFFER_S


@pytest.mark.timeout(5)
async def test_bind_retry_retries_bind_failures_only():
    attempts = {"n": 0}

    async def flaky():
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise RuntimeError("metrics bind failed on 127.0.0.1:9100: Address already in use")

    await recycling.start_with_bind_retry(flaky, budget_s=5.0)
    assert attempts["n"] == 3

    async def other():
        raise RuntimeError("something else")

    with pytest.raises(RuntimeError, match="something else"):
        await recycling.start_with_bind_retry(other)


@pytest.mark.timeout(10)
async def test_recycling_signal_carries_the_slot_identity(worker, channel, monkeypatch):
    """U2 (R3): ``WORKER_RECYCLING`` traegt dieselbe Kennung wie die uebrigen
    Worker-Signale — gerade beim Recycling zaehlt, dass der Slot gleich
    bleibt, denn genau hier wechselt der Prozess."""
    monkeypatch.setattr(recycling, "RECYCLE_ACK_TIMEOUT_S", 2.0)
    worker._config.worker_slot = 1
    worker._config.worker_id = "phx"
    events = _arm(worker, recycle_max_tasks=1)
    worker._start_recycle_monitor()
    await worker._signal_dispatcher.dispatch(Signal.TASK_COMPLETED, task_id="t", task_name="n", queue="q")
    await _wait_for(lambda: channel.intent_value() == sv.INTENT_RECYCLE)
    channel.acknowledge_replacement()
    await _wait_for(lambda: worker._state is WorkerState.DRAINING)
    assert events[0]["worker_id"] == "phx:1"
    assert events[0]["kind"] == "worker"
