"""Tests fuer swarmq.worker.compute.pool mit gefaelschtem Prozess-Kontext (U5).

Die Fakes bilden die Kanalsemantik nach: je Kind eine Pipe (Kindseite ``send``,
Elternseite ``recv_bytes`` mit Pickle-Rahmen und EOF beim Schliessen), ein
Sentinel als Pipe-Leseende, Signale per gepatchtem ``os.kill``/``os.killpg``.
"""

from __future__ import annotations

import asyncio
import itertools
import os
import pickle
import queue as _queue
import signal
import threading
from typing import Any, ClassVar

import pytest

from swarmq.worker.compute import pool as poolmod
from swarmq.worker.compute.envelope import ComputeJob, ResultEnvelope
from swarmq.worker.compute.pool import ChildDied, SlotPool, SlotUnavailable

_PIDS = itertools.count(40000)
_EOF = object()


class _FakePipeEnd:
    """Beide Enden einer Pipe-Attrappe teilen sich eine Thread-Queue."""

    def __init__(self, q: _queue.Queue, *, readable: bool) -> None:
        self._q = q
        self._readable = readable
        self.closed = False
        self.child_side = False  # vom FakeProcess markiert: Eltern-``close`` ist Dup-Semantik

    # Kindseite
    def send(self, obj: Any) -> None:
        if self.closed:
            raise OSError("closed")
        self._q.put(pickle.dumps(obj))

    def send_raw(self, raw: bytes) -> None:
        self._q.put(raw)

    # Elternseite
    def recv_bytes(self) -> bytes:
        while True:
            item = self._q.get()
            if item is _EOF or self.closed:
                raise EOFError
            return item

    def recv(self, timeout: float | None = None) -> Any:
        item = self._q.get(timeout=timeout)
        if item is _EOF:
            raise EOFError
        return pickle.loads(item)

    def fileno(self) -> int:
        return -1

    def eof(self) -> None:
        """Kindseite: das Kind ist weg, kein Schreibende mehr offen."""
        self._q.put(_EOF)

    def close(self) -> None:
        # Elternseite: Ende schliessen (EOF fuer die Gegenseite). Ein Ende, das dem
        # Kind gehoert, schliesst der Elternprozess nur als eigene Kopie (Dup-Semantik).
        if self.child_side or self.closed:
            return
        self.closed = True
        self._q.put(_EOF)


class FakeProcess:
    """Kind-Attrappe: Sentinel ist das Leseende einer Pipe, ``die()`` schliesst
    Sentinel und (standardmaessig) den Rueckkanal."""

    def __init__(self, target: Any, args: tuple[Any, ...], name: str = "") -> None:
        self.target, self.args, self.name = target, args, name
        for end in args:
            if isinstance(end, _FakePipeEnd):
                end.child_side = True
        self.pid: int | None = None
        self._r, self._w = os.pipe()
        self.sentinel = self._r
        self._alive = False
        self.behaviour = "cooperative"  # cooperative | stubborn | immortal
        self.started = 0

    @property
    def channel(self) -> _FakePipeEnd:
        """Kindseite des Rueckkanals (child_once: args[1], child_loop: args[2])."""
        end: _FakePipeEnd = self.args[1] if self.target is poolmod.child_once else self.args[2]
        return end

    @property
    def job_conn(self) -> _FakePipeEnd:
        assert self.target is poolmod.child_loop
        end: _FakePipeEnd = self.args[1]
        return end

    def start(self) -> None:
        if FakeCtx.fail_next_start:
            FakeCtx.fail_next_start = False
            raise OSError("forkserver unreachable")
        self.pid = next(_PIDS)
        self._alive = True
        self.started += 1
        FakeCtx.started.append(self)

    def is_alive(self) -> bool:
        return self._alive

    def join(self, timeout: float | None = None) -> None:
        return None

    def die(self, *, eof: bool = True) -> None:
        if self._alive:
            self._alive = False
            os.close(self._w)
            if eof:
                self.channel.eof()


class FakeCtx:
    started: ClassVar[list[FakeProcess]] = []
    fail_next_start: ClassVar[bool] = False

    def Pipe(self, duplex: bool = True) -> tuple[_FakePipeEnd, _FakePipeEnd]:
        q: _queue.Queue = _queue.Queue()
        return _FakePipeEnd(q, readable=True), _FakePipeEnd(q, readable=False)

    def Process(self, target: Any, args: tuple[Any, ...], name: str = "") -> FakeProcess:
        return FakeProcess(target, args, name)


@pytest.fixture
def ctx() -> FakeCtx:
    FakeCtx.started = []
    FakeCtx.fail_next_start = False
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
    proc.channel.send(("ready", slot, proc.pid, pdeath))
    await asyncio.sleep(0.02)


async def _next_job(proc: FakeProcess, timeout: float = 2.0) -> Any:
    """Job von der Kindseite lesen, ohne den Loop zu blockieren (die Uebergabe laeuft im Executor)."""
    for _ in range(int(timeout / 0.01)):
        try:
            return proc.job_conn.recv(timeout=0.0)
        except _queue.Empty:
            await asyncio.sleep(0.01)
    raise AssertionError("no job arrived")


async def _settle(pred, timeout: float = 2.0) -> None:
    for _ in range(int(timeout / 0.01)):
        if pred():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not reached")


def _result(tid: str, slot: int, rss: float | None = 100.0, value: Any = "ok") -> ResultEnvelope:
    return ResultEnvelope(task_id=tid, ok=True, value=value, rss_mb=rss, slot=slot)


async def _make_pool(ctx: FakeCtx, slots: int = 2, **kw: Any) -> SlotPool:
    kw.setdefault("recycle_rss_mb", None)
    kw.setdefault("recycle_max_tasks", None)
    kw.setdefault("handshake_timeout_s", 0.3)
    kw.setdefault("end_grace_s", 0.2)
    kw.setdefault("kill_grace_s", 0.05)
    kw.setdefault("reap_timeout_s", 0.05)
    kw.setdefault("eof_grace_s", 0.2)
    p = SlotPool(ctx, slots, **kw)
    await p.start()
    return p


@pytest.mark.timeout(5)
async def test_fresh_child_runs_one_job_and_frees_the_slot(ctx, signals):
    p = await _make_pool(ctx)
    task = asyncio.create_task(p.run(_job(), use_di=False))
    proc = await _started(1)
    assert proc.target is poolmod.child_once
    assert proc.args[0].task_id == "t1"
    slot = proc.args[2]
    await _ready(p, proc, slot)
    proc.channel.send(("result", _result("t1", slot)))
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
    assert (await _next_job(proc)).task_id == "a"
    proc.channel.send(("result", _result("a", 0)))
    await t1
    t2 = asyncio.create_task(p.run(_job("b"), use_di=True))
    assert (await _next_job(proc)).task_id == "b"
    proc.channel.send(("result", _result("b", 0)))
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
    (await _next_job(old))

    async def replacement_ready() -> None:
        new = await _started(2)
        await _ready(p, new, 0)

    bg = asyncio.create_task(replacement_ready())
    old.channel.send(("result", _result("a", 0, rss=320.0)))
    env = await t1  # das Ergebnis ist da, BEVOR das Recycling laeuft
    assert env.value == "ok"
    await bg
    await _settle(lambda: p.stats.recycled == 1)
    assert recycled == [(0, "rss", 320.0)]
    assert (await _next_job(old)) is None  # Ende-Signal an das alte Kind
    assert len(FakeCtx.started) == 2
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
    old.channel.send(("result", _result("a", 0, rss=10.0)))
    await t1
    await bg
    await _settle(lambda: p.stats.recycled == 1)
    old.die()
    await p.aclose()


@pytest.mark.timeout(5)
async def test_next_dispatch_waits_for_a_running_replacement(ctx, signals):
    """Waehrend das Recycling im Hintergrund laeuft, spawnt der naechste Job kein zweites Ersatzkind."""
    p = await _make_pool(ctx, slots=1, recycle_max_tasks=1)
    t1 = asyncio.create_task(p.run(_job("a"), use_di=True))
    old = await _started(1)
    await _ready(p, old, 0)
    old.channel.send(("result", _result("a", 0)))
    await t1
    t2 = asyncio.create_task(p.run(_job("b"), use_di=True))
    new = await _started(2)
    await asyncio.sleep(0.05)
    assert len(FakeCtx.started) == 2  # kein drittes Kind
    await _ready(p, new, 0)
    assert (await _next_job(new)).task_id == "b"
    new.channel.send(("result", _result("b", 0)))
    assert (await t2).value == "ok"
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
    new.channel.send(("result", _result("b", 0)))
    assert (await t2).value == "ok"
    await p.aclose()


@pytest.mark.timeout(5)
async def test_result_before_eof_wins_even_if_the_sentinel_fires_first(ctx, signals):
    """P0 #3: der Sentinel darf ein Ergebnis nicht ueberholen — das EOF des Kanals
    steht hinter dem letzten Ergebnis, der Sentinel gewaehrt nur eine Gnadenfrist."""
    p = await _make_pool(ctx, slots=1, eof_grace_s=1.0)
    t1 = asyncio.create_task(p.run(_job("a"), use_di=False))
    proc = await _started(1)
    await _ready(p, proc, 0)
    proc.die(eof=False)  # Prozess tot laut Sentinel, Kanal noch offen (z. B. Enkel haelt das fd)
    await asyncio.sleep(0.05)
    assert not t1.done()
    proc.channel.send(("result", _result("a", 0, value=42)))
    proc.channel.eof()
    assert (await t1).value == 42
    assert p.stats.died == 0
    await p.aclose()


@pytest.mark.timeout(5)
async def test_sentinel_without_eof_ends_in_child_died_after_the_grace(ctx, signals):
    p = await _make_pool(ctx, slots=1, eof_grace_s=0.05)
    t1 = asyncio.create_task(p.run(_job("a"), use_di=False))
    proc = await _started(1)
    await _ready(p, proc, 0)
    proc.die(eof=False)
    with pytest.raises(ChildDied):
        await t1
    assert p.stats.died == 1
    proc.channel.eof()
    await p.aclose()


@pytest.mark.timeout(5)
async def test_undecodable_result_fails_only_this_task_and_the_pool_stays_alive(ctx, signals):
    """P0 #1: ein im Elternprozess nicht entpickelbares Ergebnis darf den Pool nicht taub machen."""
    p = await _make_pool(ctx, slots=1)
    t1 = asyncio.create_task(p.run(_job("a"), use_di=True))
    proc = await _started(1)
    await _ready(p, proc, 0)
    (await _next_job(proc))
    proc.channel.send_raw(b"\x80\x05not a pickle at all")
    with pytest.raises(ChildDied, match="undecodable"):
        await t1
    assert p.stats.undecodable == 1
    t2 = asyncio.create_task(p.run(_job("b"), use_di=True))  # dasselbe Kind lebt noch
    assert (await _next_job(proc)).task_id == "b"
    proc.channel.send(("result", _result("b", 0)))
    assert (await t2).value == "ok"
    assert len(FakeCtx.started) == 1
    await p.aclose()


@pytest.mark.timeout(5)
async def test_result_with_foreign_task_id_is_dropped(ctx, signals):
    """P0 #2 (Routing): ein Envelope fuer einen anderen Task resolvt den laufenden Job nicht."""
    p = await _make_pool(ctx, slots=1)
    t1 = asyncio.create_task(p.run(_job("b"), use_di=False))
    proc = await _started(1)
    await _ready(p, proc, 0)
    proc.channel.send(("result", _result("a", 0, value="stale")))
    await asyncio.sleep(0.05)
    assert not t1.done()
    proc.channel.send(("result", _result("b", 0, value="mine")))
    assert (await t1).value == "mine"
    await p.aclose()


@pytest.mark.timeout(5)
async def test_cancel_during_handshake_kills_the_started_child(ctx, signals):
    """P0 #2 (Cancel-Fenster): ein Abbruch vor dem Ready-Handshake laesst kein Kind verwaist rechnen."""
    p = await _make_pool(ctx, slots=1, handshake_timeout_s=5.0)
    t1 = asyncio.create_task(p.run(_job("a"), use_di=False))
    proc = await _started(1)
    t1.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t1
    assert signals and signals[0][0] == "kill"  # vor Ready: per PID
    assert not proc.is_alive()
    assert p._free.qsize() == 1
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
    await p._kill_child(p._slots[0].active, reason="hard_timeout")
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
    await p._kill_child(p._slots[0].active, reason="cancel")
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
    await p._kill_child(p._slots[0].active, reason="hard_timeout")
    assert p.stats.blocked_slots == {0}
    t1.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t1
    assert not p.healthy and all(i < 0 for i in p._free._queue)  # blockierter Slot kommt nicht zurueck, nur Poison
    proc.die()
    await p.aclose()


@pytest.mark.timeout(5)
async def test_progress_messages_reach_the_sink(ctx, signals):
    seen: list[tuple[Any, ...]] = []

    async def sink(msg: tuple[Any, ...]) -> None:
        seen.append(msg)

    p = await _make_pool(ctx, slots=1, progress_sink=sink)
    t1 = asyncio.create_task(p.run(_job("a"), use_di=False))
    proc = await _started(1)
    await _ready(p, proc, 0)
    proc.channel.send(("progress", "a", {"current": "1"}, 1000))
    await _settle(lambda: bool(seen))
    assert seen == [("progress", "a", {"current": "1"}, 1000)]
    proc.channel.send(("result", _result("a", 0)))
    await t1
    await p.aclose()


@pytest.mark.timeout(5)
async def test_cancelled_run_kills_the_child_before_the_slot_is_freed(ctx, signals):
    """KTD3/KTD4: Watchdog/Cancel brechen ``run`` ab — das Kind (samt Prozessgruppe)
    stirbt synchron, ehe der Slot wieder frei wird; der Enkel ueberlebt nicht."""
    p = await _make_pool(ctx, slots=1)
    t1 = asyncio.create_task(p.run(_job("a"), use_di=False))
    proc = await _started(1)
    proc.behaviour = "stubborn"
    await _ready(p, proc, 0)
    assert p._free.qsize() == 0
    t1.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t1
    assert [s[2] for s in signals] == [signal.SIGTERM, signal.SIGKILL]
    assert all(s[0] == "killpg" for s in signals)
    assert not proc.is_alive()
    assert p._free.qsize() == 1
    await p.aclose()


@pytest.mark.timeout(5)
async def test_reader_threads_end_with_their_children(ctx, signals):
    before = {t.name for t in threading.enumerate()}
    p = await _make_pool(ctx, slots=1)
    t1 = asyncio.create_task(p.run(_job("a"), use_di=False))
    proc = await _started(1)
    await _ready(p, proc, 0)
    new_threads = {t.name for t in threading.enumerate()} - before
    assert any(name.startswith("swarmq-compute-reader") for name in new_threads)
    proc.channel.send(("result", _result("a", 0)))
    await t1
    proc.die()
    await p.aclose()
    await _settle(lambda: not (new_threads & {t.name for t in threading.enumerate()}))


@pytest.mark.timeout(5)
async def test_repeated_handshake_failures_mark_the_pool_unhealthy_until_a_ready(ctx, signals):
    """Review #17: dauerhafte SlotUnavailable-Fehler duerfen nicht still jede Nachricht bis
    zum Delivery-Limit requeuen — der Pool meldet sich ueber /ready als ungesund."""
    p = await _make_pool(ctx, slots=1, handshake_timeout_s=0.05)
    assert p.healthy and p.unhealthy_after == 3
    for _ in range(3):
        with pytest.raises(SlotUnavailable):
            await p.run(_job("a"), use_di=False)
    assert not p.healthy and p.consecutive_unavailable == 3
    t = asyncio.create_task(p.run(_job("b"), use_di=False))
    proc = await _started(4)
    await _ready(p, proc, 0)
    assert p.healthy  # ein erfolgreicher Handshake setzt zurueck
    proc.channel.send(("result", _result("b", 0)))
    await t
    await p.aclose()


@pytest.mark.timeout(5)
async def test_fresh_child_result_right_after_ready_is_honored(ctx, signals):
    """Zweit-Review #2: ``ready`` und ``result`` stehen hintereinander in der Loop-Queue,
    bevor run() nach dem Handshake weiterlaeuft — ``pending`` muss vor dem Start stehen."""
    p = await _make_pool(ctx, slots=1)
    t = asyncio.create_task(p.run(_job("fast"), use_di=False))
    proc = await _started(1)
    proc.channel.send(("ready", 0, proc.pid, True))
    proc.channel.send(("result", _result("fast", 0, value="quick")))
    proc.die()
    assert (await t).value == "quick"
    assert p.stats.died == 0
    await p.aclose()


@pytest.mark.timeout(5)
async def test_next_dispatch_waits_only_for_the_replacement_not_the_old_childs_exit(ctx, signals):
    """Zweit-Review #12: das alte Kind darf bis end_grace_s weiterleben, der naechste Job
    geht trotzdem sofort an das bereite Ersatzkind (rollierend, R7)."""
    p = await _make_pool(ctx, slots=1, recycle_max_tasks=1, end_grace_s=0.3)
    t1 = asyncio.create_task(p.run(_job("a"), use_di=True))
    old = await _started(1)
    old.behaviour = "stubborn"  # ignoriert Ende-Signal und SIGTERM: lebt noch die ganze Gnadenfrist
    await _ready(p, old, 0)
    old.channel.send(("result", _result("a", 0)))
    await t1
    t2 = asyncio.create_task(p.run(_job("b"), use_di=True))
    new = await _started(2)
    await _ready(p, new, 0)
    assert (await _next_job(new, timeout=1.0)).task_id == "b"  # ohne auf das alte Kind zu warten
    new.channel.send(("result", _result("b", 0)))
    assert (await t2).value == "ok"
    assert old.is_alive()
    old.die()
    await p.aclose()


@pytest.mark.timeout(5)
async def test_recycle_spawn_error_still_ends_the_old_child_and_spares_the_next_dispatch(ctx, signals):
    """Zweit-Review #15: ein OSError beim Spawn des Ersatzkinds darf weder das alte Kind
    verwaisen noch in den naechsten Dispatch durchschlagen."""
    p = await _make_pool(ctx, slots=1, recycle_max_tasks=1)
    t1 = asyncio.create_task(p.run(_job("a"), use_di=True))
    old = await _started(1)
    await _ready(p, old, 0)
    assert (await _next_job(old)).task_id == "a"
    FakeCtx.fail_next_start = True
    old.channel.send(("result", _result("a", 0)))
    await t1
    assert (await _next_job(old)) is None  # Ende-Signal kam trotz Spawn-Fehler
    t2 = asyncio.create_task(p.run(_job("b"), use_di=True))
    new = await _started(2)  # dritter Startversuch gelingt
    await _ready(p, new, 0)
    assert (await _next_job(new)).task_id == "b"
    new.channel.send(("result", _result("b", 0)))
    assert (await t2).value == "ok"
    old.die()
    await p.aclose()


@pytest.mark.timeout(5)
async def test_process_start_failure_is_slot_unavailable_and_counts_against_health(ctx, signals):
    """Zweit-Review #13: ein Fehler aus process.start() ist ein SlotUnavailable (Redelivery),
    schliesst die Pipe-Enden und zaehlt gegen die Pool-Gesundheit."""
    p = await _make_pool(ctx, slots=1)
    FakeCtx.fail_next_start = True
    with pytest.raises(SlotUnavailable, match="could not start"):
        await p.run(_job("a"), use_di=False)
    assert p.stats.unavailable == 1 and p.consecutive_unavailable == 1
    assert FakeCtx.started == []
    assert p._free.qsize() == 1
    await p.aclose()


@pytest.mark.timeout(5)
async def test_all_slots_blocked_fails_fast_and_marks_the_pool_unhealthy(ctx, signals):
    """Zweit-Review #10: ohne nutzbaren Slot darf run() nicht ewig warten, /ready wird rot."""
    p = await _make_pool(ctx, slots=1)
    t1 = asyncio.create_task(p.run(_job("a"), use_di=False))
    proc = await _started(1)
    proc.behaviour = "immortal"
    await _ready(p, proc, 0)
    waiter = asyncio.create_task(p.run(_job("b"), use_di=False))  # wartet auf den einzigen Slot
    await asyncio.sleep(0.02)
    t1.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t1
    assert p.stats.blocked_slots == {0} and not p.healthy
    with pytest.raises(SlotUnavailable):
        await waiter
    with pytest.raises(SlotUnavailable):
        await p.run(_job("c"), use_di=False)
    proc.die()
    await p.aclose()


@pytest.mark.timeout(5)
async def test_aclose_is_not_held_hostage_by_a_hanging_recycle_hook(ctx, signals):
    """Zweit-Review #16: ein blockierender WORKER_RECYCLING-Handler darf aclose nur end_grace_s kosten."""

    async def hook(slot: int, reason: str, value: float) -> None:
        await asyncio.sleep(60)

    p = await _make_pool(ctx, slots=1, recycle_max_tasks=1, on_recycle=hook, end_grace_s=0.1)
    t1 = asyncio.create_task(p.run(_job("a"), use_di=True))
    old = await _started(1)
    await _ready(p, old, 0)
    old.channel.send(("result", _result("a", 0)))
    await t1
    new = await _started(2)
    await _ready(p, new, 0)
    await asyncio.wait_for(p.aclose(), timeout=2.0)
    assert not old.is_alive() and not new.is_alive()


@pytest.mark.timeout(5)
async def test_spawn_timings_are_recorded_for_diagnostics(ctx, signals):
    p = await _make_pool(ctx, slots=1)
    t = asyncio.create_task(p.run(_job("a"), use_di=False))
    proc = await _started(1)
    await _ready(p, proc, 0)
    proc.channel.send(("result", _result("a", 0)))
    await t
    assert len(p.recent_spawn_ms) == 1
    spawn_ms, ready_ms = p.recent_spawn_ms[0]
    assert 0 <= spawn_ms <= ready_ms
    await p.aclose()


# Plan 2026-10-06-0831, U7 Teil b (KTD14). Test-Liste:
# 1. [x] Melden mehrere Kinder, dass die App das Logging hat, erscheint
#        ``logging_reconfigured_by_app`` einmal je Pool, mit den Feldern aus
#        der ersten Meldung.


@pytest.mark.timeout(5)
async def test_app_logging_takeover_is_reported_once_per_pool(ctx, signals):
    import structlog

    p = await _make_pool(ctx, slots=1)
    with structlog.testing.capture_logs() as logs:
        for n, tid in enumerate(("a", "b"), start=1):
            task = asyncio.create_task(p.run(_job(tid), use_di=False))
            proc = await _started(n)
            await _ready(p, proc, 0)
            proc.channel.send(("logging_reconfigured_by_app", {"reenabled_loggers": 2}))
            proc.channel.send(("result", _result(tid, 0)))
            await task
            proc.die()
    await p.aclose()
    reports = [e for e in logs if e["event"] == "logging_reconfigured_by_app"]
    assert len(reports) == 1
    assert reports[0]["reenabled_loggers"] == 2
