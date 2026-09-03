"""Slot-Pool des Loop-Prozesses (U5: R6, R7, R9, R11, R23, R27, R28; KTD3, KTD4).

Der Pool verwaltet N Rechenslots. Ein Task ohne DI bekommt ein frisches
Kind (``child_once``), ein Task mit DI das wiederverwendete Kind seines
Slots (``child_loop``) mit eigener Job-Queue. Kindtod meldet der
``Process.sentinel`` ueber ``add_reader``; Ergebnisse und Fortschritt kommen
ueber zwei multiprocessing-Queues, die je ein Leser-Thread in den Event-Loop
reicht. Recycling ist rollierend: erst das Ersatzkind, dann das Ende-Signal,
nach Frist der Kill. Der Kill ist zweistufig (SIGTERM, Gnade, SIGKILL) und
kehrt erst zurueck, wenn der Sentinel den Tod gemeldet hat.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import threading
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

import structlog

from swarmq.worker.compute.child import child_loop, child_once
from swarmq.worker.compute.envelope import ComputeJob, ResultEnvelope

logger = structlog.get_logger(__name__)

RecycleHook = Callable[[int, str, float], Awaitable[None]]
ProgressSink = Callable[[tuple[Any, ...]], Awaitable[None]]


class SlotUnavailable(RuntimeError):
    """Der Ready-Handshake blieb vor der Job-Uebergabe aus (KTD4): kein Kindtod."""


class ChildDied(RuntimeError):
    """Das Rechenkind endete ohne Envelope (R9)."""


class ChildKilled(RuntimeError):
    """Der Pool hat das Kind auf Anweisung getoetet (Timeout/Cancel/Drain)."""


@dataclass(slots=True)
class _Child:
    process: Any
    slot: int
    pid: int | None = None
    pgid_ready: bool = False
    ready: asyncio.Future[None] | None = None
    job_queue: Any | None = None  # nur reuse-Modus
    tasks_done: int = 0
    last_rss_mb: float | None = None
    pending: asyncio.Future[ResultEnvelope] | None = None
    ending: bool = False  # Ende-Signal gesendet (Recycling/Close)


@dataclass(slots=True)
class _Slot:
    index: int
    reuse_child: _Child | None = None  # warmes DI-Kind
    active: _Child | None = None  # rechnet gerade (frisch oder = reuse_child)
    blocked: bool = False  # unreapbares Kind (D-State)
    blocked_reason: str = ""


@dataclass(slots=True)
class PoolStats:
    started: int = 0
    recycled: int = 0
    killed: int = 0
    died: int = 0
    forced: int = 0
    unavailable: int = 0
    blocked_slots: set[int] = field(default_factory=set)


class SlotPool:
    """N Rechenslots ueber einem laufenden Forkserver-Kontext."""

    def __init__(
        self,
        context: Any,
        slots: int,
        *,
        recycle_rss_mb: int | None,
        recycle_max_tasks: int | None,
        handshake_timeout_s: float = 5.0,
        end_grace_s: float = 30.0,
        kill_grace_s: float = 1.0,
        reap_timeout_s: float = 1.0,
        on_recycle: RecycleHook | None = None,
        progress_sink: ProgressSink | None = None,
        restart_check: Callable[[], bool] | None = None,
    ) -> None:
        if slots < 1:
            raise ValueError("slots must be >= 1")
        self._ctx = context
        self.size = slots
        self.recycle_rss_mb = recycle_rss_mb
        self.recycle_max_tasks = recycle_max_tasks
        self.handshake_timeout_s = handshake_timeout_s
        self.end_grace_s = end_grace_s
        self.kill_grace_s = kill_grace_s
        self.reap_timeout_s = reap_timeout_s
        self._on_recycle = on_recycle
        self._progress_sink = progress_sink
        self._restart_check = restart_check
        self._slots = [_Slot(index=i) for i in range(slots)]
        self._free: asyncio.Queue[int] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._result_queue: Any = None
        self._progress_queue: Any = None
        self._threads: list[threading.Thread] = []
        self._children: dict[int, _Child] = {}  # pid -> child (auch sterbende)
        self._closing = False
        self._background: set[asyncio.Task[None]] = set()
        self.stats = PoolStats()

    # ------------------------------------------------------------------ Start/Ende

    async def start(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._free = asyncio.Queue()
        for slot in self._slots:
            self._free.put_nowait(slot.index)
        self._result_queue = self._ctx.Queue()
        self._progress_queue = self._ctx.Queue()
        for q, handler in ((self._result_queue, self._on_message), (self._progress_queue, self._on_progress)):
            t = threading.Thread(target=self._pump, args=(q, handler), name="swarmq-compute-pump", daemon=True)
            t.start()
            self._threads.append(t)

    def _pump(self, queue: Any, handler: Callable[[Any], None]) -> None:
        """Leser-Thread: blockierendes ``get`` in den Event-Loop reichen."""
        assert self._loop is not None
        while True:
            try:
                msg = queue.get()
            except (EOFError, OSError):
                return
            if msg is None:
                return
            self._loop.call_soon_threadsafe(handler, msg)

    async def aclose(self) -> None:
        """Alle Kinder beenden: Ende-Signal an warme Kinder, Kill fuer den Rest."""
        self._closing = True
        for slot in self._slots:
            if slot.active is not None:
                await self._kill_child(slot.active, reason="close")
            if slot.reuse_child is not None and slot.reuse_child is not slot.active:
                await self._end_child(slot.reuse_child, reason="close")
        if self._background:
            await asyncio.gather(*self._background, return_exceptions=True)
        for q in (self._result_queue, self._progress_queue):
            with contextlib.suppress(Exception):
                q.put(None)
        for t in self._threads:
            t.join(timeout=2)

    # ------------------------------------------------------------------ Dispatch

    async def run(
        self,
        job: ComputeJob,
        *,
        use_di: bool,
        on_slot: Callable[[int], None] | None = None,
    ) -> ResultEnvelope:
        """Einen Job ausfuehren; liefert den Envelope oder wirft ``SlotUnavailable``/``ChildDied``.

        ``on_slot`` meldet dem Aufrufer den belegten Slot, damit Watchdog und
        Cancel-Monitor ``kill_slot`` gezielt rufen koennen (KTD4).
        """
        assert self._free is not None
        index = await self._free.get()
        slot = self._slots[index]
        if on_slot is not None:
            on_slot(index)
        try:
            if self._restart_check is not None and self._restart_check():
                logger.warning("forkserver_restarted_before_dispatch", slot=index)
            child = await (self._ensure_reuse_child(slot) if use_di else self._spawn_fresh(slot, job))
            slot.active = child
            fut: asyncio.Future[ResultEnvelope] = asyncio.get_running_loop().create_future()
            child.pending = fut
            if use_di:
                assert child.job_queue is not None
                child.job_queue.put(job)
            try:
                envelope = await fut
            finally:
                child.pending = None
            child.tasks_done += 1
            child.last_rss_mb = envelope.rss_mb
            if use_di:
                await self._maybe_recycle(slot, child)
            return envelope
        finally:
            slot.active = None
            if not slot.blocked and not self._closing:
                self._free.put_nowait(index)

    async def kill_slot(self, slot_index: int, *, reason: str) -> None:
        """Zweistufiger, synchroner Kill des aktiven Kindes eines Slots (KTD3)."""
        slot = self._slots[slot_index]
        if slot.active is not None:
            await self._kill_child(slot.active, reason=reason)

    # ------------------------------------------------------------------ Kinder

    async def _spawn_fresh(self, slot: _Slot, job: ComputeJob) -> _Child:
        process = self._ctx.Process(
            target=child_once, args=(job, self._result_queue, self._progress_queue, slot.index),
            name=f"swarmq-compute-{slot.index}",
        )
        return await self._start_child(process, slot, job_queue=None)

    async def _ensure_reuse_child(self, slot: _Slot) -> _Child:
        child = slot.reuse_child
        if child is not None and child.process.is_alive() and not child.ending:
            return child
        job_queue = self._ctx.Queue()
        process = self._ctx.Process(
            target=child_loop, args=(slot.index, job_queue, self._result_queue, self._progress_queue),
            name=f"swarmq-compute-slot-{slot.index}",
        )
        child = await self._start_child(process, slot, job_queue=job_queue)
        slot.reuse_child = child
        return child

    async def _start_child(self, process: Any, slot: _Slot, *, job_queue: Any) -> _Child:
        loop = asyncio.get_running_loop()
        child = _Child(process=process, slot=slot.index, job_queue=job_queue, ready=loop.create_future())
        process.start()
        child.pid = process.pid
        self._children[process.pid] = child
        self.stats.started += 1
        loop.add_reader(process.sentinel, self._on_sentinel, child)
        try:
            assert child.ready is not None
            await asyncio.wait_for(asyncio.shield(child.ready), timeout=self.handshake_timeout_s)
        except (TimeoutError, ChildDied) as exc:
            self.stats.unavailable += 1
            await self._kill_child(child, reason="handshake", by_pid_only=True)
            raise SlotUnavailable(f"compute child for slot {slot.index} sent no ready signal") from exc
        return child

    def _on_sentinel(self, child: _Child) -> None:
        """Kindtod (Sentinel lesbar): Futures aufloesen, Slot freigeben."""
        assert self._loop is not None
        with contextlib.suppress(Exception):
            self._loop.remove_reader(child.process.sentinel)
        with contextlib.suppress(Exception):
            child.process.join(timeout=0)
        self._children.pop(child.pid or -1, None)
        slot = self._slots[child.slot]
        if slot.reuse_child is child:
            slot.reuse_child = None  # lazy Ersatz beim naechsten Dispatch
        if child.ready is not None and not child.ready.done():
            child.ready.set_exception(ChildDied("child exited before ready"))
        if child.pending is not None and not child.pending.done():
            self.stats.died += 1
            child.pending.set_exception(ChildDied(f"compute child pid={child.pid} exited without result"))

    def _on_message(self, msg: Any) -> None:
        kind = msg[0]
        if kind == "ready":
            _, slot_index, pid, pdeath = msg
            child = self._children.get(pid)
            if child is None:
                return
            child.pgid_ready = True
            if child.ready is not None and not child.ready.done():
                child.ready.set_result(None)
            if not pdeath:
                logger.debug("compute_child_without_pdeathsig", slot=slot_index, pid=pid)
        elif kind == "result":
            envelope: ResultEnvelope = msg[1]
            child = self._active_child_for_slot(envelope.slot)
            if child is None or child.pending is None or child.pending.done():
                logger.debug("compute_result_for_finished_task", task_id=envelope.task_id)
                return
            child.pending.set_result(envelope)

    def _active_child_for_slot(self, slot_index: int | None) -> _Child | None:
        if slot_index is None or not 0 <= slot_index < self.size:
            return None
        return self._slots[slot_index].active

    def _on_progress(self, msg: Any) -> None:
        if self._progress_sink is None or self._loop is None:
            return
        sink = self._progress_sink

        async def _forward() -> None:
            await sink(msg)

        self._loop.create_task(_forward())

    # ------------------------------------------------------------------ Recycling und Kill

    async def _maybe_recycle(self, slot: _Slot, child: _Child) -> None:
        reason: str | None = None
        value = 0.0
        rss = child.last_rss_mb
        if self.recycle_rss_mb is not None and rss is not None and rss > self.recycle_rss_mb:
            reason, value = "rss", rss
        elif self.recycle_max_tasks is not None and child.tasks_done >= self.recycle_max_tasks:
            reason, value = "tasks", float(child.tasks_done)
        if reason is None:
            return
        # Rollierend (R7): das Ersatzkind ist bereit, bevor das alte endet.
        slot.reuse_child = None
        try:
            await self._ensure_reuse_child(slot)
        except SlotUnavailable:
            logger.warning("compute_recycle_replacement_failed", slot=slot.index)
        self.stats.recycled += 1
        logger.info("compute_child_recycled", slot=slot.index, reason=reason, value=value, pid=child.pid)
        if self._on_recycle is not None:
            await self._on_recycle(slot.index, reason, value)
        ender = asyncio.get_running_loop().create_task(self._end_child(child, reason=reason))
        self._background.add(ender)
        ender.add_done_callback(self._background.discard)

    async def _end_child(self, child: _Child, *, reason: str) -> None:
        """Ende-Signal senden; nach ``end_grace_s`` Kill nach KTD3 (Grund ``forced``)."""
        child.ending = True
        if child.job_queue is not None:
            with contextlib.suppress(Exception):
                child.job_queue.put(None)
        if await self._wait_dead(child, self.end_grace_s):
            return
        self.stats.forced += 1
        logger.warning("compute_child_end_forced", slot=child.slot, pid=child.pid, reason=reason)
        await self._kill_child(child, reason="forced")

    async def _kill_child(self, child: _Child, *, reason: str, by_pid_only: bool = False) -> None:
        """SIGTERM → Gnade → SIGKILL → auf den Sentinel warten (synchron, KTD3)."""
        if child.pid is None or not child.process.is_alive():
            return
        self.stats.killed += 1
        self._signal(child, signal.SIGTERM, by_pid_only)
        if await self._wait_dead(child, self.kill_grace_s):
            return
        self._signal(child, signal.SIGKILL, by_pid_only)
        if await self._wait_dead(child, self.reap_timeout_s):
            return
        slot = self._slots[child.slot]
        slot.blocked = True
        slot.blocked_reason = reason
        self.stats.blocked_slots.add(slot.index)
        logger.error(
            "compute_child_unreaped", slot=child.slot, pid=child.pid, reason=reason,
            hint="child survived SIGKILL (D-state?); slot stays blocked",
        )

    def _signal(self, child: _Child, sig: signal.Signals, by_pid_only: bool) -> None:
        assert child.pid is not None
        try:
            if child.pgid_ready and not by_pid_only:
                os.killpg(child.pid, sig)
            else:
                os.kill(child.pid, sig)
        except ProcessLookupError:
            pass

    async def _wait_dead(self, child: _Child, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        while child.process.is_alive():
            if time.monotonic() >= deadline:
                return False
            await asyncio.sleep(0.01)
        return True


__all__ = ["ChildDied", "ChildKilled", "PoolStats", "SlotPool", "SlotUnavailable"]
