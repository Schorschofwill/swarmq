"""Tests fuer swarmq.worker.compute.pool mit gefaelschtem Prozess-Kontext (U5).

Die Fakes bilden die Kanalsemantik nach: je Kind eine Pipe (Kindseite ``send``,
Elternseite ``recv_bytes`` mit Pickle-Rahmen und EOF beim Schliessen), ein
Sentinel als Pipe-Leseende, Signale per gepatchtem ``os.kill``/``os.killpg``.
"""

from __future__ import annotations

import asyncio
import gc
import itertools
import os
import pickle
import queue as _queue
import signal
import threading
import weakref
from typing import Any, ClassVar

import pytest

from swarmq.worker.compute.child import child_loop, child_once
from swarmq.worker.compute.envelope import ComputeJob, ResultEnvelope
from swarmq.worker.compute.pool import ChildDied, SlotPool, SlotUnavailable, _Child

_PIDS = itertools.count(40000)
_EOF = object()


class _FakePipeEnd:
    """Beide Enden einer Pipe-Attrappe teilen sich eine Thread-Queue."""

    def __init__(self, q: _queue.Queue[Any], *, readable: bool) -> None:
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
            raw: bytes = item
            return raw

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
        if not FakeCtx.grandchild_holds_channel:
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
        end: _FakePipeEnd = self.args[1] if self.target is child_once else self.args[2]
        return end

    @property
    def job_conn(self) -> _FakePipeEnd:
        assert self.target is child_loop
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
            if eof and not FakeCtx.grandchild_holds_channel:
                self.channel.eof()


class FakeCtx:
    started: ClassVar[list[FakeProcess]] = []
    fail_next_start: ClassVar[bool] = False
    # Ein Enkel haelt das Schreibende des Rueckkanals offen: Weder der Tod des
    # Kindes noch das Schliessen der Elternseite bringt dem Leser ein EOF.
    grandchild_holds_channel: ClassVar[bool] = False

    def Pipe(self, duplex: bool = True) -> tuple[_FakePipeEnd, _FakePipeEnd]:
        q: _queue.Queue[Any] = _queue.Queue()
        return _FakePipeEnd(q, readable=True), _FakePipeEnd(q, readable=False)

    def Process(self, target: Any, args: tuple[Any, ...], name: str = "") -> FakeProcess:
        return FakeProcess(target, args, name)


def fresh_fake_ctx() -> FakeCtx:
    """Neuer Fake-Kontext mit geleertem Startprotokoll (auch fuer die Nachweisdateien)."""
    FakeCtx.started = []
    FakeCtx.fail_next_start = False
    FakeCtx.grandchild_holds_channel = False
    return FakeCtx()


def install_fake_signals(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, int, int]]:
    """``os.kill``/``os.killpg`` durch Zustellung an die Fake-Kinder ersetzen; liefert das Signalprotokoll."""
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

    monkeypatch.setattr(os, "killpg", lambda pid, sig: _deliver("killpg", pid, sig))
    monkeypatch.setattr(os, "kill", lambda pid, sig: _deliver("kill", pid, sig))
    return sent


@pytest.fixture
def ctx() -> FakeCtx:
    return fresh_fake_ctx()


@pytest.fixture
def signals(monkeypatch) -> list[tuple[str, int, int]]:
    return install_fake_signals(monkeypatch)


def _free(p: SlotPool) -> asyncio.Queue[int]:
    assert p._free is not None
    return p._free


def _active(p: SlotPool, slot: int = 0) -> _Child:
    child = p._slots[slot].active
    assert child is not None
    return child


def _healthy(p: SlotPool) -> bool:
    # Frisch gelesen: ein ``assert p.healthy`` verengte mypy die Property
    # sonst fuer den Rest des Tests auf einen festen Wert.
    return p.healthy


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
    assert proc.target is child_once
    assert proc.args[0].task_id == "t1"
    slot = proc.args[2]
    await _ready(p, proc, slot)
    proc.channel.send(("result", _result("t1", slot)))
    env = await task
    assert env.value == "ok"
    assert _free(p).qsize() == 2
    proc.die()
    await p.aclose()


@pytest.mark.timeout(5)
async def test_reuse_child_serves_consecutive_di_jobs(ctx, signals):
    p = await _make_pool(ctx, slots=1)
    t1 = asyncio.create_task(p.run(_job("a"), use_di=True))
    proc = await _started(1)
    assert proc.target is child_loop
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
async def test_undecodable_result_logs_error_type_and_error(ctx, signals, swarmq_logs):
    """Plan 2026-10-06-0831, U9 (R17): einheitliches Fehlerschema."""
    p = await _make_pool(ctx, slots=1)
    t1 = asyncio.create_task(p.run(_job("a"), use_di=True))
    proc = await _started(1)
    await _ready(p, proc, 0)
    (await _next_job(proc))
    proc.channel.send_raw(b"\x80\x05not a pickle at all")
    with pytest.raises(ChildDied, match="undecodable"):
        await t1
    await p.aclose()
    [entry] = [e for e in swarmq_logs if e["event"] == "compute_result_undecodable"]
    assert entry["log_level"] == "error"
    assert entry["error_type"] == "UnpicklingError"
    assert entry["error"]
    assert "UnpicklingError" not in entry["error"]


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
    # Ohne Ready keine bestaetigte eigene Gruppe: kein Sweep-SIGKILL an die (evtl.
    # schon fremde) PID des per SIGTERM beendeten Kindes.
    assert signals == [("kill", proc.pid, signal.SIGTERM)]
    assert not proc.is_alive()
    assert _free(p).qsize() == 1
    await p.aclose()
    assert signals == [("kill", proc.pid, signal.SIGTERM)]


@pytest.mark.timeout(5)
async def test_handshake_timeout_is_slot_unavailable_not_child_death(ctx, signals):
    p = await _make_pool(ctx, slots=1, handshake_timeout_s=0.05)
    with pytest.raises(SlotUnavailable):
        await p.run(_job("a"), use_di=False)
    await _settle(lambda: not p._children)  # FS-24: erst nach _finalize zaehlt "kein Kindtod"
    assert p.stats.unavailable == 1
    assert p.stats.died == 0
    assert signals == [("kill", FakeCtx.started[0].pid, signal.SIGTERM)]
    await p.aclose()


@pytest.mark.timeout(5)
@pytest.mark.entscheidung("E-036")
async def test_kill_slot_is_two_stage_and_waits_for_death(ctx, signals):
    p = await _make_pool(ctx, slots=1)
    t1 = asyncio.create_task(p.run(_job("a"), use_di=False))
    proc = await _started(1)
    proc.behaviour = "stubborn"
    await _ready(p, proc, 0)
    await p._kill_child(_active(p), reason="hard_timeout")
    assert [s[2] for s in signals] == [signal.SIGTERM, signal.SIGKILL]
    assert all(s[0] == "killpg" for s in signals)
    assert not proc.is_alive()
    with pytest.raises(ChildDied):
        await t1
    await p.aclose()


@pytest.mark.timeout(5)
async def test_cooperative_child_skips_the_kill_grace_and_its_group_is_swept_after_death(ctx, signals):
    """Ein kooperatives Kind endet auf SIGTERM ohne ``kill_grace_s`` abzuwarten; das
    SIGKILL danach trifft nur noch die Gruppe (Enkel, FS-16), genau einmal."""
    p = await _make_pool(ctx, slots=1, kill_grace_s=1.0)
    t1 = asyncio.create_task(p.run(_job("a"), use_di=False))
    proc = await _started(1)
    await _ready(p, proc, 0)
    loop = asyncio.get_running_loop()
    t0 = loop.time()
    await p._kill_child(_active(p), reason="cancel")
    assert loop.time() - t0 < 0.5  # keine Gnadenfrist abgewartet
    with pytest.raises(ChildDied):
        await t1
    await _settle(lambda: not p._children)
    assert signals == [("killpg", proc.pid, signal.SIGTERM), ("killpg", proc.pid, signal.SIGKILL)]
    await p.aclose()


@pytest.mark.timeout(5)
async def test_child_death_without_result_sweeps_its_group_exactly_once(ctx, signals):
    """FS-16 (R9): stirbt das Kind von selbst (OOM, Segfault), bekommt seine Gruppe ein SIGKILL."""
    p = await _make_pool(ctx, slots=1)
    t1 = asyncio.create_task(p.run(_job("a"), use_di=True))
    proc = await _started(1)
    await _ready(p, proc, 0)
    proc.die()
    with pytest.raises(ChildDied):
        await t1
    await _settle(lambda: not p._children)
    assert signals == [("killpg", proc.pid, signal.SIGKILL)]
    await p.aclose()


@pytest.mark.timeout(5)
async def test_a_denied_group_sweep_still_ends_run_with_child_died(ctx, signals, monkeypatch, swarmq_logs):
    """Review U19: lebt in der Gruppe nur noch ein fremder (setuid-)Prozess, liefert
    ``killpg`` EPERM. Der Sweep darf ``run`` dann nicht haengen lassen."""

    def denied(pid: int, sig: int) -> None:
        signals.append(("killpg", pid, sig))
        raise PermissionError(1, "Operation not permitted")

    monkeypatch.setattr(os, "killpg", denied)
    p = await _make_pool(ctx, slots=1)
    t1 = asyncio.create_task(p.run(_job("a"), use_di=True))
    proc = await _started(1)
    await _ready(p, proc, 0)
    proc.die()
    with pytest.raises(ChildDied):
        await asyncio.wait_for(t1, timeout=1.0)
    assert signals == [("killpg", proc.pid, signal.SIGKILL)]
    [entry] = [e for e in swarmq_logs if e["event"] == "compute_signal_denied"]
    assert entry["log_level"] == "warning"
    assert entry["pid"] == proc.pid
    assert entry["group"] is True
    await p.aclose()


@pytest.mark.timeout(5)
async def test_cancel_right_after_the_child_died_still_sweeps_its_group_once(ctx, signals):
    """E-036-Nachtrag: trifft der Abbruch ein Kind, das gerade von selbst starb (EOF steht
    noch aus), bekommt seine Gruppe trotzdem genau ein SIGKILL -- und kein SIGTERM."""
    p = await _make_pool(ctx, slots=1, eof_grace_s=1.0)
    t1 = asyncio.create_task(p.run(_job("a"), use_di=False))
    proc = await _started(1)
    await _ready(p, proc, 0)
    proc.die(eof=False)
    t1.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t1
    assert signals == [("killpg", proc.pid, signal.SIGKILL)]
    await p.aclose()
    assert signals == [("killpg", proc.pid, signal.SIGKILL)]


@pytest.mark.timeout(5)
async def test_a_by_pid_only_kill_of_a_dead_child_leaves_its_group_alone(ctx, signals):
    """``by_pid_only`` (Handshake, verworfene Kindquelle) signalisiert nie die Gruppe,
    auch nicht als Sweep nach dem Tod."""
    p = await _make_pool(ctx, slots=1, eof_grace_s=0.05)
    t1 = asyncio.create_task(p.run(_job("a"), use_di=False))
    proc = await _started(1)
    await _ready(p, proc, 0)
    proc.die(eof=False)
    await p._kill_child(_active(p), reason="handshake", by_pid_only=True)
    assert signals == []
    with pytest.raises(ChildDied):
        await t1
    await p.aclose()


@pytest.mark.timeout(5)
async def test_failed_handshake_without_own_group_sends_no_killpg(ctx, signals):
    """Ohne Ready ist die eigene Gruppe nicht bestaetigt: nur die PID wird signalisiert."""
    p = await _make_pool(ctx, slots=1, handshake_timeout_s=0.05)
    with pytest.raises(SlotUnavailable):
        await p.run(_job("a"), use_di=False)
    await _settle(lambda: not p._children)
    assert signals == [("kill", FakeCtx.started[0].pid, signal.SIGTERM)]
    await p.aclose()


@pytest.mark.timeout(5)
async def test_finalize_sends_no_second_sigkill_after_the_second_kill_stage(ctx, signals):
    """Regressionsschutz fuer E-036: das SIGKILL der zweiten Stufe ist zugleich der Sweep."""
    p = await _make_pool(ctx, slots=1)
    t1 = asyncio.create_task(p.run(_job("a"), use_di=False))
    proc = await _started(1)
    proc.behaviour = "stubborn"
    await _ready(p, proc, 0)
    await p._kill_child(_active(p), reason="hard_timeout")
    with pytest.raises(ChildDied):
        await t1
    await _settle(lambda: not p._children)
    assert signals == [("killpg", proc.pid, signal.SIGTERM), ("killpg", proc.pid, signal.SIGKILL)]
    await p.aclose()


@pytest.mark.timeout(5)
async def test_idle_death_of_the_warm_di_child_sweeps_its_group(ctx, signals):
    p = await _make_pool(ctx, slots=1)
    t1 = asyncio.create_task(p.run(_job("a"), use_di=True))
    proc = await _started(1)
    await _ready(p, proc, 0)
    proc.channel.send(("result", _result("a", 0)))
    await t1
    proc.die()  # zwischen zwei Jobs, ohne Ende-Signal
    await _settle(lambda: not p._children)
    assert signals == [("killpg", proc.pid, signal.SIGKILL)]
    await p.aclose()


@pytest.mark.timeout(5)
async def test_recycled_child_ending_on_its_end_signal_is_not_swept(ctx, signals):
    """Regulaeres Ende (Recycling mit Ende-Signal): E-036 verlangt keinen Sweep."""
    p = await _make_pool(ctx, slots=1, recycle_max_tasks=1, end_grace_s=1.0)
    t1 = asyncio.create_task(p.run(_job("a"), use_di=True))
    old = await _started(1)
    await _ready(p, old, 0)
    assert (await _next_job(old)).task_id == "a"
    old.channel.send(("result", _result("a", 0)))
    await t1
    new = await _started(2)
    await _ready(p, new, 0)
    assert (await _next_job(old)) is None  # Ende-Signal
    old.die()
    await _settle(lambda: old.pid not in p._children and not p._background)
    assert signals == []
    new.die()
    await p.aclose()


@pytest.mark.timeout(5)
async def test_warm_child_ending_on_its_close_signal_is_not_swept(ctx, signals):
    """Regulaeres Ende beim Shutdown: das warme DI-Kind ist noch ``reuse_child`` seines
    Slots, endet aber auf das Ende-Signal -- das ist kein Leerlauf-Tod, kein Sweep."""
    p = await _make_pool(ctx, slots=1, end_grace_s=1.0)
    t1 = asyncio.create_task(p.run(_job("a"), use_di=True))
    proc = await _started(1)
    await _ready(p, proc, 0)
    assert (await _next_job(proc)).task_id == "a"
    proc.channel.send(("result", _result("a", 0)))
    await t1
    closing = asyncio.create_task(p.aclose())
    assert (await _next_job(proc)) is None  # Ende-Signal
    proc.die()
    await closing
    assert signals == []


@pytest.mark.timeout(5)
@pytest.mark.entscheidung("E-035")
async def test_immortal_child_blocks_the_slot(ctx, signals):
    p = await _make_pool(ctx, slots=1)
    t1 = asyncio.create_task(p.run(_job("a"), use_di=False))
    proc = await _started(1)
    proc.behaviour = "immortal"
    await _ready(p, proc, 0)
    await p._kill_child(_active(p), reason="hard_timeout")
    assert p.stats.blocked_slots == {0}
    t1.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t1
    assert not p.healthy
    assert all(i < 0 for i in _free(p)._queue)  # type: ignore[attr-defined]  # privater Deque der asyncio.Queue
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
async def test_progress_forward_survives_gc_while_the_sink_waits(ctx, signals):
    """Ein Weiterleitungs-Task, dessen Sink auf ein sonst unreferenziertes Future
    wartet, haengt nur an der schwachen Taskliste der Loop -- ohne starke
    Referenz raeumt ``gc`` ihn ab, und der Fortschritt kommt nie an."""
    waiting: list[weakref.ref[asyncio.Future[None]]] = []
    delivered: list[tuple[Any, ...]] = []

    async def sink(msg: tuple[Any, ...]) -> None:
        fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        waiting.append(weakref.ref(fut))
        await fut
        delivered.append(msg)

    p = await _make_pool(ctx, slots=1, progress_sink=sink)
    t1 = asyncio.create_task(p.run(_job("a"), use_di=False))
    proc = await _started(1)
    await _ready(p, proc, 0)
    proc.channel.send(("progress", "a", {"current": "1"}, 1000))
    await _settle(lambda: bool(waiting))
    gc.collect()
    fut = waiting[0]()
    assert fut is not None, "der wartende Weiterleitungs-Task wurde eingesammelt"
    fut.set_result(None)
    await _settle(lambda: bool(delivered))
    assert delivered == [("progress", "a", {"current": "1"}, 1000)]
    proc.channel.send(("result", _result("a", 0)))
    await t1
    await p.aclose()


@pytest.mark.timeout(5)
async def test_a_finished_progress_forward_is_released(ctx, signals):
    """Ein zugestellter Fortschritt haelt seinen Weiterleitungs-Task nicht fest:
    sonst waechst die Taskmenge mit jeder Fortschrittsmeldung."""
    forwards: list[weakref.ref[asyncio.Task[Any]]] = []

    async def sink(msg: tuple[Any, ...]) -> None:
        task = asyncio.current_task()
        assert task is not None
        forwards.append(weakref.ref(task))

    p = await _make_pool(ctx, slots=1, progress_sink=sink)
    t1 = asyncio.create_task(p.run(_job("a"), use_di=False))
    proc = await _started(1)
    await _ready(p, proc, 0)
    proc.channel.send(("progress", "a", {"current": "1"}, 1000))

    def _finished() -> bool:
        task = forwards[0]() if forwards else None
        return bool(forwards) and (task is None or task.done())

    def _released() -> bool:
        gc.collect()
        return forwards[0]() is None

    await _settle(_finished)
    # Pollend statt einmalig: der Done-Callback laeuft erst eine Loop-Runde nach dem Task-Ende.
    await _settle(_released, timeout=0.5)
    proc.channel.send(("result", _result("a", 0)))
    await t1
    await p.aclose()


@pytest.mark.timeout(5)
async def test_aclose_cancels_a_hanging_progress_forward_after_the_grace(ctx, signals):
    """Ein haengender Fortschritts-Sink darf aclose nur end_grace_s kosten und
    wird danach abgebrochen, statt als verwaister Task weiterzulaufen."""
    entered = asyncio.Event()
    never = asyncio.Event()
    cancelled = asyncio.Event()

    async def sink(msg: tuple[Any, ...]) -> None:
        entered.set()
        try:
            await never.wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    p = await _make_pool(ctx, slots=1, progress_sink=sink, end_grace_s=0.1)
    t1 = asyncio.create_task(p.run(_job("a"), use_di=False))
    proc = await _started(1)
    await _ready(p, proc, 0)
    proc.channel.send(("progress", "a", {"current": "1"}, 1000))
    await asyncio.wait_for(entered.wait(), timeout=2.0)
    proc.channel.send(("result", _result("a", 0)))
    await t1
    await asyncio.wait_for(p.aclose(), timeout=2.0)
    assert cancelled.is_set(), "der haengende Weiterleitungs-Task laeuft nach aclose weiter"


@pytest.mark.timeout(5)
async def test_cancelled_run_kills_the_child_before_the_slot_is_freed(ctx, signals):
    """KTD3/KTD4: Watchdog/Cancel brechen ``run`` ab — das Kind (samt Prozessgruppe)
    stirbt synchron, ehe der Slot wieder frei wird; der Enkel ueberlebt nicht."""
    p = await _make_pool(ctx, slots=1)
    t1 = asyncio.create_task(p.run(_job("a"), use_di=False))
    proc = await _started(1)
    proc.behaviour = "stubborn"
    await _ready(p, proc, 0)
    assert _free(p).qsize() == 0
    t1.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t1
    assert [s[2] for s in signals] == [signal.SIGTERM, signal.SIGKILL]
    assert all(s[0] == "killpg" for s in signals)
    assert not proc.is_alive()
    assert _free(p).qsize() == 1
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
    assert _healthy(p)
    assert p.unhealthy_after == 3
    for _ in range(3):
        with pytest.raises(SlotUnavailable):
            await p.run(_job("a"), use_di=False)
    assert not _healthy(p)
    assert p.consecutive_unavailable == 3
    t = asyncio.create_task(p.run(_job("b"), use_di=False))
    proc = await _started(4)
    await _ready(p, proc, 0)
    assert _healthy(p)  # ein erfolgreicher Handshake setzt zurueck
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
    assert p.stats.unavailable == 1
    assert p.consecutive_unavailable == 1
    assert FakeCtx.started == []
    assert _free(p).qsize() == 1
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
    assert p.stats.blocked_slots == {0}
    assert not p.healthy
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
    assert not old.is_alive()
    assert not new.is_alive()


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
# Plan 2026-10-06-1811, U9 (KTD9):
# 2. [x] Melden mehrere Kinder, dass die App neben swarmq Root-Handler
#        angehaengt hat, erscheint ``logging_handlers_added_by_app`` (WARNING)
#        einmal je Pool mit den Handler-Typen -- unabhaengig von einer
#        Uebernahme-Meldung.


@pytest.mark.timeout(5)
async def test_app_logging_takeover_is_reported_once_per_pool(ctx, signals, swarmq_logs):
    p = await _make_pool(ctx, slots=1)
    for n, tid in enumerate(("a", "b"), start=1):
        task = asyncio.create_task(p.run(_job(tid), use_di=False))
        proc = await _started(n)
        await _ready(p, proc, 0)
        proc.channel.send(("logging_reconfigured_by_app", {"reenabled_loggers": 2}))
        proc.channel.send(("result", _result(tid, 0)))
        await task
        proc.die()
    await p.aclose()
    reports = [
        e
        for e in swarmq_logs
        if e["event"] == "logging_reconfigured_by_app" and e["logger"] == "swarmq.worker.compute.pool"
    ]
    assert len(reports) == 1
    assert reports[0]["reenabled_loggers"] == 2


@pytest.mark.timeout(5)
async def test_app_handlers_added_next_to_swarmq_are_reported_once_per_pool(ctx, signals, swarmq_logs):
    p = await _make_pool(ctx, slots=1)
    for n, tid in enumerate(("a", "b"), start=1):
        task = asyncio.create_task(p.run(_job(tid), use_di=False))
        proc = await _started(n)
        await _ready(p, proc, 0)
        proc.channel.send(("logging_reconfigured_by_app", {"reenabled_loggers": 1}))
        proc.channel.send(("logging_handlers_added_by_app", {"handler_types": ["StreamHandler"]}))
        proc.channel.send(("result", _result(tid, 0)))
        await task
        proc.die()
    await p.aclose()
    pool_events = [e for e in swarmq_logs if e["logger"] == "swarmq.worker.compute.pool"]
    added = [e for e in pool_events if e["event"] == "logging_handlers_added_by_app"]
    assert [(e["log_level"], e["handler_types"]) for e in added] == [("warning", ["StreamHandler"])]
    assert len([e for e in pool_events if e["event"] == "logging_reconfigured_by_app"]) == 1


# Plan 2026-10-06-1811, U6 (R7, KTD7) -- Test-Liste:
# 1. Meldet die Neustart-Pruefung eine unsichere Kindquelle
#    (``ForkSourceUnsafe``), verwirft der Pool sie: ``SlotUnavailable``, kein
#    Kind, ``healthy`` ist falsch -- auch beim naechsten Job, obwohl die
#    Pruefung dann nichts Neues mehr meldet.
@pytest.mark.timeout(5)
async def test_unsafe_restarted_fork_source_is_rejected_for_good(ctx, signals):
    from swarmq.worker.compute.forkserver import ForkSourceUnsafe

    checks = iter([ForkSourceUnsafe("restarted forkserver has 2 threads")])

    def restart_check() -> bool:
        exc = next(checks, None)
        if exc is not None:
            raise exc
        return False

    p = await _make_pool(ctx, slots=1, restart_check=restart_check)
    for tid in ("a", "b"):
        with pytest.raises(SlotUnavailable, match="2 threads"):
            await p.run(_job(tid), use_di=False)
    assert FakeCtx.started == []
    assert not p.healthy
    await p.aclose()


# Schluss-Review -- Test-Liste:
# 1. [x] Verwirft der Pool die Kindquelle, meldet er das genau einmal an
#    ``on_source_rejected`` (mit dem Grund): Der Worker hoert dann auf zu
#    konsumieren, statt jede Nachricht neu zustellen zu lassen.
# 2. [x] Startet der Forkserver zwischen Pruefung und Fork still neu (die
#    Pruefung direkt nach ``process.start()`` meldet ``ForkSourceUnsafe``),
#    bekommt das eben gestartete Kind keine Arbeit: Es wird getoetet, der Job
#    endet in ``SlotUnavailable``. Gilt auch fuer das Ersatzkind beim
#    Recycling (``_spawn_reuse``).
@pytest.mark.timeout(5)
async def test_a_child_forked_from_an_unsafe_restarted_source_gets_no_work(ctx, signals):
    from swarmq.worker.compute.forkserver import ForkSourceUnsafe

    def restart_check() -> bool:
        if FakeCtx.started:  # der stille Neustart geschah in ``process.start()``
            raise ForkSourceUnsafe("restarted forkserver has 2 threads")
        return False

    p = await _make_pool(ctx, slots=1, restart_check=restart_check)
    with pytest.raises(SlotUnavailable, match="2 threads"):
        await p.run(_job("a"), use_di=True)
    (proc,) = FakeCtx.started
    assert not proc.is_alive()
    assert not p.healthy
    await p.aclose()


@pytest.mark.timeout(5)
async def test_a_recycle_replacement_from_an_unsafe_restarted_source_is_killed(ctx, signals):
    """Schutztest zu Punkt 2 der Liste (Recycling-Pfad, ohne eigenes Rot)."""
    from swarmq.worker.compute.forkserver import ForkSourceUnsafe

    def restart_check() -> bool:
        if len(FakeCtx.started) > 1:  # Neustart beim Fork des Ersatzkindes
            raise ForkSourceUnsafe("restarted forkserver has 2 threads")
        return False

    p = await _make_pool(ctx, slots=1, recycle_max_tasks=1, restart_check=restart_check)
    t1 = asyncio.create_task(p.run(_job("a"), use_di=True))
    old = await _started(1)
    await _ready(p, old, 0)
    old.channel.send(("result", _result("a", 0, rss=10.0)))
    await t1
    replacement = await _started(2)
    await _settle(lambda: p.stats.recycled == 1)
    assert not replacement.is_alive()
    assert not p.healthy
    old.die()
    await p.aclose()


# B23 (0.9.1) -- Test-Liste:
# 1. [x] ``compute_recycle_replacement_failed`` traegt ``error_type``/``error``:
#    ohne sie blieb offen, warum das Ersatzkind nicht bereit wurde.
@pytest.mark.timeout(5)
async def test_a_failed_recycle_replacement_logs_error_fields(ctx, signals, swarmq_logs):
    from swarmq.worker.compute.forkserver import ForkSourceUnsafe

    def restart_check() -> bool:
        if len(FakeCtx.started) > 1:
            raise ForkSourceUnsafe("restarted forkserver has 2 threads")
        return False

    p = await _make_pool(ctx, slots=1, recycle_max_tasks=1, restart_check=restart_check)
    t1 = asyncio.create_task(p.run(_job("a"), use_di=True))
    old = await _started(1)
    await _ready(p, old, 0)
    old.channel.send(("result", _result("a", 0, rss=10.0)))
    await t1
    await _settle(lambda: p.stats.recycled == 1)

    [entry] = [e for e in swarmq_logs if e["event"] == "compute_recycle_replacement_failed"]
    assert entry["log_level"] == "warning"
    assert entry["slot"] == 0
    assert entry["error_type"] == "SlotUnavailable"
    assert "2 threads" in entry["error"]
    old.die()
    await p.aclose()


@pytest.mark.timeout(5)
async def test_a_rejected_fork_source_is_reported_once(ctx, signals):
    from swarmq.worker.compute.forkserver import ForkSourceUnsafe

    def restart_check() -> bool:
        raise ForkSourceUnsafe("restarted forkserver has 2 threads")

    reported: list[str] = []
    p = await _make_pool(ctx, slots=1, restart_check=restart_check, on_source_rejected=reported.append)
    for tid in ("a", "b"):
        with pytest.raises(SlotUnavailable):
            await p.run(_job(tid), use_di=False)
    assert reported == ["restarted forkserver has 2 threads"]
    await p.aclose()


@pytest.mark.timeout(5)
def test_a_channel_eof_after_the_event_loop_ended_stays_silent(ctx, signals):
    """Ein Enkel haelt das Kanal-fd offen; das EOF erreicht den Leser-Thread
    erst nach dem Ende des Event-Loops. Der Thread darf daran nicht mit
    ``RuntimeError: Event loop is closed`` scheitern (CI-Pipeline 2947339525).

    Bewusst synchron mit ``asyncio.run``: Der Loop muss vor dem EOF
    geschlossen sein, was pytest-asyncio innerhalb des Tests nicht zulaesst.
    """
    FakeCtx.grandchild_holds_channel = True

    async def main() -> FakeProcess:
        p = await _make_pool(ctx, slots=1)
        task = asyncio.create_task(p.run(_job("a"), use_di=False))
        proc = await _started(1)
        await _ready(p, proc, 0)
        proc.channel.send(("result", _result("a", 0)))
        await task
        proc.die()
        await _settle(lambda: proc.pid not in p._children)
        await p.aclose()
        return proc

    proc = asyncio.run(main())
    [reader] = [t for t in threading.enumerate() if t.name == f"swarmq-compute-reader-{proc.pid}"]
    proc.channel.eof()  # der Enkel endet
    reader.join(timeout=2)
    assert not reader.is_alive()


def test_pool_without_slots_is_rejected(ctx):
    with pytest.raises(ValueError, match="slots must be >= 1"):
        SlotPool(ctx, 0, recycle_rss_mb=None, recycle_max_tasks=None)
