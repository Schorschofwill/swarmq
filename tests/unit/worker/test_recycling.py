"""Tests fuer swarmq.worker.recycling — Selbst-Recycling des IO-Workers (U10)."""

from __future__ import annotations

import asyncio
import multiprocessing

import pytest

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
async def test_without_supervisor_recycling_is_a_graceful_exit_with_warning(worker, swarmq_logs):
    _arm(worker, recycle_max_tasks=1)
    worker._start_recycle_monitor()
    await worker._signal_dispatcher.dispatch(Signal.TASK_CANCELLED, task_id="t", task_name="n", queue="q")
    await _wait_for(lambda: worker._state is WorkerState.DRAINING)
    events = [entry["event"] for entry in swarmq_logs if entry["logger"] == "swarmq.worker.recycling"]
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


class TestBindRetryBudget:
    """U3/KTD4 (R8): Das Bind-Budget des Ersatzes deckt beide Freigabefristen.

    Ein haengender Vorgaenger haelt den Port bis zum SIGKILL nach
    ``SHUTDOWN_TERM_S``; ein recycelnder gibt ihn erst nach dem
    ``WORKER_SHUTDOWN``-Dispatch frei, der mit 10 s gedeckelt ist. Ein festes
    5-s-Budget liess den Ersatz in einen Bind-Fehler und damit ins
    Neustart-Budget laufen — genau bei einem langsamen Handler, den
    ``--configure`` wahrscheinlich macht.
    """

    def test_entry_derives_a_budget_covering_both_release_deadlines(self, monkeypatch):
        import threading

        from swarmq.process_manager.entry import worker_subprocess_entry
        from swarmq.supervision import SHUTDOWN_TERM_S
        from swarmq.worker.lifecycle import _LIFECYCLE_DISPATCH_TIMEOUT_S
        from tests.unit.process_manager.test_entry import _make_test_cfg

        captured: dict = {}

        async def capture_ns(ns):
            captured["ns"] = ns

        monkeypatch.setattr("swarmq.cli.run_worker_async", capture_ns)
        worker_subprocess_entry(
            worker_id=0, shutdown_event=threading.Event(), cfg=_make_test_cfg(),
        )
        assert captured["ns"].bind_retry_budget_s > max(
            SHUTDOWN_TERM_S, _LIFECYCLE_DISPATCH_TIMEOUT_S
        )

    def test_embedded_default_stays_short(self):
        """Ohne Supervisor haelt kein Vorgaenger den Port — ein langes Budget
        wuerde einen echten Portkonflikt nur spaeter melden."""
        from swarmq.config import SwarmQConfig

        cfg = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x")
        assert cfg.bind_retry_budget_s == 5.0

    def test_supervisor_constant_is_the_shared_one(self):
        from swarmq.process_manager.supervisor import ProcessManager
        from swarmq.supervision import SHUTDOWN_TERM_S

        assert ProcessManager.SHUTDOWN_TERM_S == SHUTDOWN_TERM_S

    async def test_lifecycle_passes_the_configured_budget(self, worker, monkeypatch):
        seen: dict[str, float] = {}

        async def fake_start_with_bind_retry(start, *, budget_s):
            seen["budget"] = budget_s
            await start()

        monkeypatch.setattr(recycling, "start_with_bind_retry", fake_start_with_bind_retry)
        worker._config.bind_retry_budget_s = 12.0
        started = []
        await worker._start_with_bind_retry(lambda: _noop(started))
        assert seen["budget"] == 12.0
        assert started == ["started"]


async def _noop(sink: list[str]) -> None:
    sink.append("started")
