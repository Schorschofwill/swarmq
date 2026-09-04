"""Slot-Pool des Loop-Prozesses (U5: R6, R7, R9, R11, R23, R27, R28; KTD3, KTD4).

Der Pool verwaltet N Rechenslots. Ein Task ohne DI bekommt ein frisches
Kind (``child_once``), ein Task mit DI das wiederverwendete Kind seines
Slots (``child_loop``) mit eigener Job-Pipe.

Jedes Kind hat seinen eigenen Rueckkanal (``Pipe``): Ready-Handshake,
Ergebnis und Fortschritt laufen darueber, ein Leser-Thread je Kind reicht
die Nachrichten in den Event-Loop. Das EOF des Kanals ist die Todesmeldung —
es steht in derselben FIFO hinter dem letzten Ergebnis, ein Ergebnis kann
also nie hinter den Tod fallen. Der ``Process.sentinel`` bleibt Rueckfall-
ebene fuer ein Kanal-fd, das ein ueberlebender Enkel offen haelt (Gnadenfrist,
dann ``ChildDied``). Ohne gemeinsame Queue gibt es kein prozessuebergreifendes
Schreib-Lock, das ein SIGKILL mitten im Schreiben belegt hinterlassen koennte;
ein Dekodierfehler trifft nur das eine Kind, nie den Pool.

Recycling ist rollierend und laeuft nach dem Ergebnis im Hintergrund: das
Ergebnis ist festgeschrieben, sobald es da ist. Der Kill ist zweistufig
(SIGTERM, Gnade, SIGKILL) und kehrt erst zurueck, wenn das Kind tot ist; jeder
Abbruch von ``run`` — auch waehrend Spawn und Handshake — toetet das Kind,
bevor der Slot frei wird.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import pickle
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
    """Das Rechenkind endete ohne verwertbares Envelope (R9)."""


@dataclass(slots=True)
class _Child:
    process: Any
    slot: int
    conn: Any  # Elternseite des Rueckkanals (recv)
    job_conn: Any | None = None  # Elternseite der Job-Pipe (send), nur reuse-Modus
    pid: int | None = None
    pgid_ready: bool = False
    ready: asyncio.Future[None] | None = None
    pending: asyncio.Future[ResultEnvelope] | None = None
    current_task_id: str | None = None
    tasks_done: int = 0
    last_rss_mb: float | None = None
    ending: bool = False  # Ende-Signal gesendet (Recycling/Close)
    finalized: bool = False  # Tod verarbeitet (EOF oder Sentinel-Gnade)


@dataclass(slots=True)
class _Slot:
    index: int
    reuse_child: _Child | None = None  # warmes DI-Kind
    active: _Child | None = None  # rechnet gerade (frisch oder = reuse_child)
    replacement: asyncio.Task[None] | None = None  # laufendes Recycling
    blocked: bool = False  # unreapbares Kind (D-State)
    blocked_reason: str = ""


@dataclass(slots=True)
class PoolStats:
    started: int = 0
    recycled: int = 0
    killed: int = 0
    died: int = 0
    undecodable: int = 0
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
        eof_grace_s: float = 2.0,
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
        self.eof_grace_s = eof_grace_s
        self._on_recycle = on_recycle
        self._progress_sink = progress_sink
        self._restart_check = restart_check
        self._slots = [_Slot(index=i) for i in range(slots)]
        self._free: asyncio.Queue[int] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._children: dict[int, _Child] = {}  # pid -> child (auch sterbende)
        self._threads: dict[int, threading.Thread] = {}
        self._closing = False
        self._background: set[asyncio.Task[None]] = set()
        self.stats = PoolStats()

    def set_progress_sink(self, sink: ProgressSink) -> None:
        self._progress_sink = sink

    # ------------------------------------------------------------------ Start/Ende

    async def start(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._free = asyncio.Queue()
        for slot in self._slots:
            self._free.put_nowait(slot.index)

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
        readers = list(self._threads.values())
        for child in list(self._children.values()):
            self._finalize(child)
        for t in readers:
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

        Jeder Abbruch (Watchdog, Cancel-Monitor, Drain) toetet das Kind, bevor der
        Slot frei wird (KTD3/KTD4) — auch waehrend Spawn und Handshake.
        """
        assert self._free is not None
        index = await self._free.get()
        slot = self._slots[index]
        if on_slot is not None:
            on_slot(index)
        child: _Child | None = None
        try:
            if self._restart_check is not None and self._restart_check():
                logger.warning("forkserver_restarted_before_dispatch", slot=index)
            child = await (self._ensure_reuse_child(slot) if use_di else self._spawn_fresh(slot, job))
            slot.active = child
            child.current_task_id = job.task_id
            fut: asyncio.Future[ResultEnvelope] = asyncio.get_running_loop().create_future()
            child.pending = fut
            if use_di:
                await self._send_job(child, job)
            try:
                envelope = await fut
            finally:
                child.pending = None
                child.current_task_id = None
            child.tasks_done += 1
            child.last_rss_mb = envelope.rss_mb
            if use_di:
                # Das Ergebnis ist festgeschrieben; das Recycling laeuft im Hintergrund,
                # damit ein spaeter Cancel es nicht mehr verwerfen kann.
                self._schedule_recycle(slot, child)
            return envelope
        except asyncio.CancelledError:
            if child is not None:
                await self._kill_child_uninterruptible(child, reason="cancelled")
            raise
        finally:
            slot.active = None
            if not slot.blocked and not self._closing:
                self._free.put_nowait(index)

    async def _send_job(self, child: _Child, job: ComputeJob) -> None:
        assert child.job_conn is not None
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, child.job_conn.send, job)

    async def _kill_child_uninterruptible(self, child: _Child, *, reason: str) -> None:
        """``_kill_child`` zu Ende fuehren, auch wenn weitere Cancels eintreffen.

        Der Kill ist durch ``kill_grace_s`` + ``reap_timeout_s`` begrenzt; ein
        waehrenddessen eintreffender Cancel wird erst danach weitergereicht.
        """
        kill = asyncio.ensure_future(self._kill_child(child, reason=reason))
        while True:
            try:
                await asyncio.shield(kill)
                return
            except asyncio.CancelledError:
                if kill.done():
                    raise

    async def kill_slot(self, slot_index: int, *, reason: str) -> None:
        """Zweistufiger, synchroner Kill des aktiven Kindes eines Slots (KTD3)."""
        slot = self._slots[slot_index]
        if slot.active is not None:
            await self._kill_child(slot.active, reason=reason)

    # ------------------------------------------------------------------ Kinder

    async def _spawn_fresh(self, slot: _Slot, job: ComputeJob) -> _Child:
        recv_conn, send_conn = self._ctx.Pipe(duplex=False)
        process = self._ctx.Process(
            target=child_once, args=(job, send_conn, slot.index), name=f"swarmq-compute-{slot.index}",
        )
        return await self._start_child(process, slot, conn=recv_conn, child_side=(send_conn,), job_conn=None)

    async def _ensure_reuse_child(self, slot: _Slot) -> _Child:
        if slot.replacement is not None and not slot.replacement.done():
            await asyncio.shield(slot.replacement)
        child = slot.reuse_child
        if child is not None and child.process.is_alive() and not child.ending:
            return child
        return await self._spawn_reuse(slot)

    async def _spawn_reuse(self, slot: _Slot) -> _Child:
        recv_conn, send_conn = self._ctx.Pipe(duplex=False)
        job_r, job_w = self._ctx.Pipe(duplex=False)
        process = self._ctx.Process(
            target=child_loop, args=(slot.index, job_r, send_conn), name=f"swarmq-compute-slot-{slot.index}",
        )
        child = await self._start_child(process, slot, conn=recv_conn, child_side=(send_conn, job_r), job_conn=job_w)
        slot.reuse_child = child
        return child

    async def _start_child(
        self, process: Any, slot: _Slot, *, conn: Any, child_side: tuple[Any, ...], job_conn: Any
    ) -> _Child:
        loop = asyncio.get_running_loop()
        child = _Child(process=process, slot=slot.index, conn=conn, job_conn=job_conn, ready=loop.create_future())
        process.start()
        for c in child_side:  # Kindseiten schliessen, sonst kommt nie ein EOF
            with contextlib.suppress(Exception):
                c.close()
        child.pid = process.pid
        self._children[process.pid] = child
        self.stats.started += 1
        reader = threading.Thread(
            target=self._reader, args=(child,), name=f"swarmq-compute-reader-{process.pid}", daemon=True,
        )
        self._threads[process.pid] = reader
        reader.start()
        loop.add_reader(process.sentinel, self._on_sentinel, child)
        try:
            assert child.ready is not None
            await asyncio.wait_for(asyncio.shield(child.ready), timeout=self.handshake_timeout_s)
        except (TimeoutError, ChildDied) as exc:
            self.stats.unavailable += 1
            await self._kill_child(child, reason="handshake", by_pid_only=True)
            raise SlotUnavailable(f"compute child for slot {slot.index} sent no ready signal") from exc
        except asyncio.CancelledError:
            # Abbruch im Handshake: das gestartete Kind darf nicht verwaist weiterrechnen.
            await self._kill_child_uninterruptible(child, reason="cancelled")
            raise
        return child

    # ------------------------------------------------------------------ Rueckkanal

    def _reader(self, child: _Child) -> None:
        """Leser-Thread je Kind: Nachrichten und danach das EOF in den Loop reichen."""
        assert self._loop is not None
        conn = child.conn
        while True:
            try:
                raw = conn.recv_bytes()
            except (EOFError, OSError):
                break
            try:
                msg = pickle.loads(raw)
            except Exception as exc:  # Dekodierfehler trifft nur dieses Kind
                self._loop.call_soon_threadsafe(self._on_decode_error, child, repr(exc))
                continue
            self._loop.call_soon_threadsafe(self._on_message, child, msg)
        self._loop.call_soon_threadsafe(self._on_eof, child)

    def _on_message(self, child: _Child, msg: Any) -> None:
        kind = msg[0]
        if kind == "ready":
            _, _slot_index, pid, pdeath = msg
            child.pgid_ready = True
            if child.ready is not None and not child.ready.done():
                child.ready.set_result(None)
            if not pdeath:
                logger.debug("compute_child_without_pdeathsig", slot=child.slot, pid=pid)
        elif kind == "result":
            envelope: ResultEnvelope = msg[1]
            if child.pending is None or child.pending.done():
                logger.debug("compute_result_for_finished_task", task_id=envelope.task_id, pid=child.pid)
                return
            if envelope.task_id != child.current_task_id:
                logger.warning(
                    "compute_result_task_mismatch", expected=child.current_task_id,
                    received=envelope.task_id, slot=child.slot, pid=child.pid,
                )
                return
            child.pending.set_result(envelope)
        elif kind in ("progress", "publish"):
            self._forward_progress(msg)

    def _on_decode_error(self, child: _Child, error: str) -> None:
        self.stats.undecodable += 1
        logger.error("compute_result_undecodable", slot=child.slot, pid=child.pid, error=error)
        if child.pending is not None and not child.pending.done():
            child.pending.set_exception(
                ChildDied(f"compute child pid={child.pid} sent an undecodable result: {error}")
            )

    def _on_eof(self, child: _Child) -> None:
        """EOF des Rueckkanals: alle Ergebnisse sind verarbeitet, das Kind ist weg."""
        self._finalize(child)

    def _on_sentinel(self, child: _Child) -> None:
        """Prozess-Tod laut Sentinel: Rueckfallebene, falls das Kanal-EOF ausbleibt."""
        assert self._loop is not None
        with contextlib.suppress(Exception):
            self._loop.remove_reader(child.process.sentinel)
        with contextlib.suppress(Exception):
            child.process.join(timeout=0)
        if not child.finalized:
            # Ein Enkel kann das Kanal-fd offen halten; das letzte Ergebnis darf
            # trotzdem noch eintreffen — erst nach der Gnadenfrist gilt es als tot.
            self._loop.call_later(self.eof_grace_s, self._finalize, child)

    def _finalize(self, child: _Child) -> None:
        if child.finalized:
            return
        child.finalized = True
        self._children.pop(child.pid or -1, None)
        self._threads.pop(child.pid or -1, None)
        slot = self._slots[child.slot]
        if slot.reuse_child is child:
            slot.reuse_child = None  # lazy Ersatz beim naechsten Dispatch
        if child.ready is not None and not child.ready.done():
            child.ready.set_exception(ChildDied("child exited before ready"))
        if child.pending is not None and not child.pending.done():
            self.stats.died += 1
            child.pending.set_exception(ChildDied(f"compute child pid={child.pid} exited without result"))
        for c in (child.conn, child.job_conn):
            if c is not None:
                with contextlib.suppress(Exception):
                    c.close()

    def _forward_progress(self, msg: tuple[Any, ...]) -> None:
        if self._progress_sink is None or self._loop is None:
            return
        sink = self._progress_sink

        async def _forward() -> None:
            await sink(msg)

        self._loop.create_task(_forward())

    # ------------------------------------------------------------------ Recycling und Kill

    def _recycle_reason(self, child: _Child) -> tuple[str, float] | None:
        rss = child.last_rss_mb
        if self.recycle_rss_mb is not None and rss is not None and rss > self.recycle_rss_mb:
            return "rss", rss
        if self.recycle_max_tasks is not None and child.tasks_done >= self.recycle_max_tasks:
            return "tasks", float(child.tasks_done)
        return None

    def _schedule_recycle(self, slot: _Slot, child: _Child) -> None:
        decision = self._recycle_reason(child)
        if decision is None or self._closing:
            return
        reason, value = decision
        slot.reuse_child = None
        task = asyncio.get_running_loop().create_task(self._recycle(slot, child, reason, value))
        slot.replacement = task
        self._background.add(task)
        task.add_done_callback(self._background.discard)

    async def _recycle(self, slot: _Slot, old: _Child, reason: str, value: float) -> None:
        """Rollierend (R7): das Ersatzkind ist bereit, bevor das alte endet."""
        try:
            await self._spawn_reuse(slot)
        except SlotUnavailable:
            logger.warning("compute_recycle_replacement_failed", slot=slot.index)
        finally:
            slot.replacement = None
        self.stats.recycled += 1
        logger.info("compute_child_recycled", slot=slot.index, reason=reason, value=value, pid=old.pid)
        if self._on_recycle is not None:
            await self._on_recycle(slot.index, reason, value)
        await self._end_child(old, reason=reason)

    async def _end_child(self, child: _Child, *, reason: str) -> None:
        """Ende-Signal senden; nach ``end_grace_s`` Kill nach KTD3 (Grund ``forced``)."""
        child.ending = True
        if child.job_conn is not None:
            with contextlib.suppress(Exception):
                child.job_conn.send(None)
        if await self._wait_dead(child, self.end_grace_s):
            return
        self.stats.forced += 1
        logger.warning("compute_child_end_forced", slot=child.slot, pid=child.pid, reason=reason)
        await self._kill_child(child, reason="forced")

    async def _kill_child(self, child: _Child, *, reason: str, by_pid_only: bool = False) -> None:
        """SIGTERM → Gnade → SIGKILL → auf den Tod warten (synchron, KTD3)."""
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


__all__ = ["ChildDied", "PoolStats", "SlotPool", "SlotUnavailable"]
