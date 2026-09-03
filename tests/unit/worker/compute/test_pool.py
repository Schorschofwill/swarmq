"""Tests fuer swarmq.worker.compute.pool — Slot-Pool gegen gefaelschte Prozesse (U5)."""

from __future__ import annotations

import asyncio
import os
import queue as _queue
import signal
from typing import Any, ClassVar

import pytest

from swarmq.worker.compute import pool as poolmod
from swarmq.worker.compute.envelope import ComputeJob, ResultEnvelope
from swarmq.worker.compute.pool import ChildDied, SlotPool, SlotUnavailable

_PIDS = iter(range(1000, 9999))


class FakeProcess:
    """Kind-Attrappe: Sentinel ist das Leseende einer Pipe, ``die()`` schliesst das Schreibende."""

    def __init__(self, target: Any, args: tuple[Any, ...], name: str = "") -> None:
        self.target, self.args, self.name = target, args, name
        self.pid: int | None = None
        self._r, self._w = os.pipe()
        self.sentinel = self._r
        self._alive = False
        self.behaviour = "cooperative"  # cooperative | stubborn | immortal
        self.started = 0

    def start(self) -> None:
        self.pid = next(_PIDS)
        self._alive = True
        self.started += 1
        FakeCtx.started.append(self)

    def is_alive(self) -> bool:
        return self._alive

    def join(self, timeout: float | None = None) -> None:
        return None

    def die(self) -> None:
        if self._alive:
            self._alive = False
            os.close(self._w)


class FakeCtx:
    started: ClassVar[list[FakeProcess]] = []

    def Queue(self) -> _queue.Queue:
        return _queue.Queue()

    def Process(self, target: Any, args: tuple[Any, ...], name: str = "") -> FakeProcess:
        return FakeProcess(target, args, name)


@pytest.fixture
def ctx() -> FakeCtx:
    FakeCtx.started = []
    return FakeCtx()


@pytest.fixture
def signals(monkeypatch) -> list[tuple[str, int, int]]:
    sent: list[tuple[str, int, int]] = []

    def _find(pid: int) -> FakeProcess | None:
        return next((p for p in FakeCtx.started if p.pid == pid), None)

    def _deliver(kind: str, pid: int, sig: int) -> None:
        sent.append((kind, pid, sig))
        proc = _find(pid)
        if proc is None:
            return
        if proc.behaviour == "cooperative" or (proc.behaviour == "stubborn" and sig == signal.SIGKILL):
            proc.die()

    monkeypatch.setattr(poolmod.os, "killpg", lambda pid, sig: _deliver("killpg", pid, sig))
    monkeypatch.setattr(poolmod.os, "kill", lambda pid, sig: _deliver("kill", pid, sig))
    return sent


def _job(tid: str = "t1") -> ComputeJob:
    return ComputeJob(task_id=tid, task_name="demo", queue="q", args=(), kwargs={})


async def _started(n: int) -> FakeProcess:
    for _ in range(200):
        if len(FakeCtx.started) >= n:
            return FakeCtx.started[n - 1]
        await asyncio.sleep(0.005)
    raise AssertionError("child was not started")


async def _ready(p: SlotPool, proc: FakeProcess, slot: int, pdeath: bool = True) -> None:
    p._result_queue.put(("ready", slot, proc.pid, pdeath))
    await asyncio.sleep(0.02)


def _result(tid: str, slot: int, rss: float | None = 100.0, value: Any = "ok") -> ResultEnvelope:
    return ResultEnvelope(task_id=tid, ok=True, value=value, rss_mb=rss, slot=slot)


async def _make_pool(ctx: FakeCtx, slots: int = 2, **kw: Any) -> SlotPool:
    kw.setdefault("recycle_rss_mb", None)
    kw.setdefault("recycle_max_tasks", None)
    kw.setdefault("handshake_timeout_s", 0.3)
    kw.setdefault("end_grace_s", 0.2)
    kw.setdefault("kill_grace_s", 0.05)
    kw.setdefault("reap_timeout_s", 0.05)
    p = SlotPool(ctx, slots, **kw)
    await p.start()
    return p


@pytest.mark.timeout(5)
async def test_fresh_child_runs_one_job_and_frees_the_slot(ctx, signals):
    p = await _make_pool(ctx)
    slots_seen: list[int] = []
    task = asyncio.create_task(p.run(_job(), use_di=False, on_slot=slots_seen.append))
    proc = await _started(1)
    assert proc.target is poolmod.child_once
    await _ready(p, proc, slots_seen[0])
    p._result_queue.put(("result", _result("t1", slots_seen[0])))
    env = await task
    assert env.value == "ok"
    assert p._free.qsize() == 2
    proc.die()
    await p.aclose()


@pytest.mark.timeout(5)
async def test_reuse_child_serves_consecutive_di_jobs(ctx, signals):
    p = await _make_pool(ctx, slots=1)
    t1 = asyncio.create_task(p.run(_job("a"), use_di=True))
    proc = await _started(1)
    assert proc.target is poolmod.child_loop
    await _ready(p, proc, 0)
    job_q = proc.args[1]
    assert job_q.get(timeout=1).task_id == "a"
    p._result_queue.put(("result", _result("a", 0)))
    await t1
    t2 = asyncio.create_task(p.run(_job("b"), use_di=True))
    await asyncio.sleep(0.02)
    assert job_q.get(timeout=1).task_id == "b"
    p._result_queue.put(("result", _result("b", 0)))
    await t2
    assert len(FakeCtx.started) == 1
    await p.aclose()


@pytest.mark.timeout(5)
async def test_rss_threshold_recycles_rolling_with_signal(ctx, signals):
    recycled: list[tuple[int, str, float]] = []

    async def hook(slot: int, reason: str, value: float) -> None:
        recycled.append((slot, reason, value))

    p = await _make_pool(ctx, slots=1, recycle_rss_mb=300, on_recycle=hook)
    t1 = asyncio.create_task(p.run(_job("a"), use_di=True))
    old = await _started(1)
    await _ready(p, old, 0)
    old.args[1].get(timeout=1)

    async def replacement_ready() -> None:
        new = await _started(2)
        await _ready(p, new, 0)

    bg = asyncio.create_task(replacement_ready())
    p._result_queue.put(("result", _result("a", 0, rss=320.0)))
    await t1
    await bg
    assert recycled == [(0, "rss", 320.0)]
    assert old.args[1].get(timeout=1) is None  # Ende-Signal an das alte Kind
    assert p.stats.recycled == 1 and len(FakeCtx.started) == 2
    old.die()
    await p.aclose()


@pytest.mark.timeout(5)
async def test_task_count_threshold_recycles(ctx, signals):
    p = await _make_pool(ctx, slots=1, recycle_max_tasks=1)
    t1 = asyncio.create_task(p.run(_job("a"), use_di=True))
    old = await _started(1)
    await _ready(p, old, 0)

    async def replacement_ready() -> None:
        new = await _started(2)
        await _ready(p, new, 0)

    bg = asyncio.create_task(replacement_ready())
    p._result_queue.put(("result", _result("a", 0, rss=10.0)))
    await t1
    await bg
    assert p.stats.recycled == 1
    old.die()
    await p.aclose()


@pytest.mark.timeout(5)
async def test_child_death_without_result_raises_child_died_and_slot_stays_usable(ctx, signals):
    p = await _make_pool(ctx, slots=1)
    t1 = asyncio.create_task(p.run(_job("a"), use_di=True))
    proc = await _started(1)
    await _ready(p, proc, 0)
    proc.die()
    with pytest.raises(ChildDied):
        await t1
    assert p.stats.died == 1
    t2 = asyncio.create_task(p.run(_job("b"), use_di=True))
    new = await _started(2)  # lazy Ersatz beim naechsten Dispatch
    await _ready(p, new, 0)
    p._result_queue.put(("result", _result("b", 0)))
    assert (await t2).value == "ok"
    await p.aclose()


@pytest.mark.timeout(5)
async def test_handshake_timeout_is_slot_unavailable_not_child_death(ctx, signals):
    p = await _make_pool(ctx, slots=1, handshake_timeout_s=0.05)
    with pytest.raises(SlotUnavailable):
        await p.run(_job("a"), use_di=False)
    assert p.stats.unavailable == 1 and p.stats.died == 0
    assert signals and signals[0][0] == "kill"  # vor Ready: Kill per PID, nicht killpg
    await p.aclose()


@pytest.mark.timeout(5)
async def test_kill_slot_is_two_stage_and_waits_for_death(ctx, signals):
    p = await _make_pool(ctx, slots=1)
    t1 = asyncio.create_task(p.run(_job("a"), use_di=False))
    proc = await _started(1)
    proc.behaviour = "stubborn"
    await _ready(p, proc, 0)
    await p.kill_slot(0, reason="hard_timeout")
    assert [s[2] for s in signals] == [signal.SIGTERM, signal.SIGKILL]
    assert all(s[0] == "killpg" for s in signals)
    assert not proc.is_alive()
    with pytest.raises(ChildDied):
        await t1
    await p.aclose()


@pytest.mark.timeout(5)
async def test_cooperative_child_needs_only_sigterm(ctx, signals):
    p = await _make_pool(ctx, slots=1)
    t1 = asyncio.create_task(p.run(_job("a"), use_di=False))
    proc = await _started(1)
    await _ready(p, proc, 0)
    await p.kill_slot(0, reason="cancel")
    assert [s[2] for s in signals] == [signal.SIGTERM]
    with pytest.raises(ChildDied):
        await t1
    await p.aclose()


@pytest.mark.timeout(5)
async def test_immortal_child_blocks_the_slot(ctx, signals):
    p = await _make_pool(ctx, slots=1)
    t1 = asyncio.create_task(p.run(_job("a"), use_di=False))
    proc = await _started(1)
    proc.behaviour = "immortal"
    await _ready(p, proc, 0)
    await p.kill_slot(0, reason="hard_timeout")
    assert p.stats.blocked_slots == {0}
    t1.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t1
    assert p._free.qsize() == 0  # blockierter Slot kommt nicht zurueck
    proc.die()
    await p.aclose()


@pytest.mark.timeout(5)
async def test_progress_messages_reach_the_sink(ctx, signals):
    seen: list[tuple[Any, ...]] = []

    async def sink(msg: tuple[Any, ...]) -> None:
        seen.append(msg)

    p = await _make_pool(ctx, slots=1, progress_sink=sink)
    p._progress_queue.put(("progress", "t1", {"current": "1"}, 1000))
    for _ in range(50):
        if seen:
            break
        await asyncio.sleep(0.01)
    assert seen == [("progress", "t1", {"current": "1"}, 1000)]
    await p.aclose()
