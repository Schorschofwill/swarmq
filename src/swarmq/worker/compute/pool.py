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
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from swarmq.logging import APP_LOGGING_REPORTS, _error_fields, get_logger, report_app_logging
from swarmq.worker.compute.child import child_loop, child_once
from swarmq.worker.compute.envelope import ComputeJob, ResultEnvelope
from swarmq.worker.compute.forkserver import ForkSourceUnsafe

logger = get_logger(__name__)

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
    group_killed: bool = False  # SIGKILL an die Prozessgruppe gesendet (FS-16)


@dataclass(slots=True)
class _Slot:
    index: int
    reuse_child: _Child | None = None  # warmes DI-Kind
    active: _Child | None = None  # rechnet gerade (frisch oder = reuse_child)
    replacement: asyncio.Future[None] | None = None  # Ersatzkind wird gerade gespawnt
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
        on_source_rejected: Callable[[str], None] | None = None,
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
        self._on_source_rejected = on_source_rejected
        self._rejected_source: str | None = None  # KTD7: unsichere Kindquelle, endgueltig
        self._slots = [_Slot(index=i) for i in range(slots)]
        self._free: asyncio.Queue[int] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._children: dict[int, _Child] = {}  # pid -> child (auch sterbende)
        self._threads: dict[int, threading.Thread] = {}
        self._closing = False
        self._background: set[asyncio.Task[None]] = set()
        # Weiterleitungen von Fortschritt: starke Referenz, sonst sammelt gc einen
        # Task ein, dessen Sink auf ein sonst unreferenziertes Future wartet.
        self._progress_forwards: set[asyncio.Task[None]] = set()
        self.stats = PoolStats()
        self.consecutive_unavailable = 0  # Handshake-Fehler in Folge; 0 nach jedem Ready
        # Diagnose (Messnachweis M0): (Dauer process.start(), Zeit bis Ready) je Spawn in ms.
        self.recent_spawn_ms: deque[tuple[float, float]] = deque(maxlen=256)
        self.unhealthy_after = 3 * slots
        self._app_logging_reported: set[str] = set()

    @property
    def healthy(self) -> bool:
        """``False``, wenn der Handshake dauerhaft ausbleibt (toter Forkserver o. ae.)
        oder jeder Slot blockiert ist: ``/ready`` meldet den Pool dann als Ausfall,
        statt dass jede Nachricht bis zum Delivery-Limit requeued wird."""
        return (
            self.consecutive_unavailable < self.unhealthy_after
            and self._usable_slots() > 0
            and self._rejected_source is None
        )

    def _usable_slots(self) -> int:
        return self.size - len(self.stats.blocked_slots)

    def _note_unavailable(self) -> None:
        self.stats.unavailable += 1
        self.consecutive_unavailable += 1
        if self.consecutive_unavailable == self.unhealthy_after:
            logger.error(
                "compute_pool_unhealthy",
                failures=self.consecutive_unavailable,
                hint="no compute child completed the ready handshake; /ready reports the pool down",
            )

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
        lingering = self._background | self._progress_forwards
        if lingering:
            # Begrenzt: ein haengender Recycling-Hook oder Fortschritts-Sink darf den
            # Shutdown nicht aufhalten.
            _done, pending = await asyncio.wait(lingering, timeout=self.end_grace_s)
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
        readers = list(self._threads.values())
        for child in list(self._children.values()):
            self._finalize(child)
        for t in readers:
            t.join(timeout=2)

    # ------------------------------------------------------------------ Dispatch

    async def run(self, job: ComputeJob, *, use_di: bool) -> ResultEnvelope:
        """Einen Job ausfuehren; liefert den Envelope oder wirft ``SlotUnavailable``/``ChildDied``.

        Jeder Abbruch (Watchdog, Cancel-Monitor, Drain) toetet das Kind, bevor der
        Slot frei wird (KTD3/KTD4) — auch waehrend Spawn und Handshake.
        """
        assert self._free is not None
        while True:
            if self._usable_slots() == 0:
                raise SlotUnavailable("every compute slot is blocked by an unreapable child")
            index = await self._free.get()
            if index < 0:  # Poison: der letzte Slot wurde blockiert, waehrend wir warteten
                self._free.put_nowait(index)
                raise SlotUnavailable("every compute slot is blocked by an unreapable child")
            if not self._slots[index].blocked:
                break
            # FS-23: im Hintergrund (Recycling) gesperrt, nachdem der Index schon frei war:
            # verwerfen, nicht zuruecklegen (E-035, die Sperre bleibt).
        slot = self._slots[index]
        child: _Child | None = None
        fut: asyncio.Future[ResultEnvelope] = asyncio.get_running_loop().create_future()
        try:
            self._check_fork_source(index)
            if use_di:
                child = await self._ensure_reuse_child(slot)
                child.current_task_id = job.task_id
                child.pending = fut
            else:
                # Das frische Kind rechnet sofort nach ``ready``; das Ergebnis darf nicht
                # eintreffen, bevor ``pending`` steht — also VOR dem Start setzen.
                child = await self._spawn_fresh(slot, job, pending=fut)
            slot.active = child
            try:
                if use_di:
                    await self._send_job(child, job)
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
        except asyncio.CancelledError:
            if child is not None:
                await self._kill_child_uninterruptible(child, reason="cancelled")
            raise
        else:
            return envelope
        finally:
            slot.active = None
            if not slot.blocked and not self._closing:
                self._free.put_nowait(index)

    def _check_fork_source(self, index: int) -> None:
        """KTD4: still neu gestarteter Forkserver? Ist er mehrfaedig
        (Plan 2026-10-06-1811, KTD7), verwirft der Pool die Kindquelle
        endgueltig: ``SlotUnavailable``, ``/ready`` meldet den Ausfall, und
        ``on_source_rejected`` erfaehrt es einmal -- der Worker hoert dann
        auf zu konsumieren (Schluss-Review)."""
        if self._rejected_source is None and self._restart_check is not None:
            try:
                if self._restart_check():
                    logger.warning("forkserver_restarted_before_dispatch", slot=index)
            except ForkSourceUnsafe as exc:
                self._rejected_source = str(exc)
                if self._on_source_rejected is not None:
                    self._on_source_rejected(self._rejected_source)
        if self._rejected_source is not None:
            raise SlotUnavailable(f"compute fork source rejected: {self._rejected_source}")

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
            except asyncio.CancelledError:
                if kill.done():
                    raise
            else:
                return

    # ------------------------------------------------------------------ Kinder

    async def _spawn_fresh(self, slot: _Slot, job: ComputeJob, *, pending: asyncio.Future[ResultEnvelope]) -> _Child:
        recv_conn, send_conn = self._ctx.Pipe(duplex=False)
        process = self._ctx.Process(
            target=child_once,
            args=(job, send_conn, slot.index),
            name=f"swarmq-compute-{slot.index}",
        )
        return await self._start_child(
            process,
            slot,
            conn=recv_conn,
            child_side=(send_conn,),
            job_conn=None,
            pending=pending,
            task_id=job.task_id,
        )

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
            target=child_loop,
            args=(slot.index, job_r, send_conn),
            name=f"swarmq-compute-slot-{slot.index}",
        )
        child = await self._start_child(process, slot, conn=recv_conn, child_side=(send_conn, job_r), job_conn=job_w)
        slot.reuse_child = child
        return child

    async def _start_child(
        self,
        process: Any,
        slot: _Slot,
        *,
        conn: Any,
        child_side: tuple[Any, ...],
        job_conn: Any,
        pending: asyncio.Future[ResultEnvelope] | None = None,
        task_id: str | None = None,
    ) -> _Child:
        loop = asyncio.get_running_loop()
        child = _Child(
            process=process,
            slot=slot.index,
            conn=conn,
            job_conn=job_conn,
            ready=loop.create_future(),
            pending=pending,
            current_task_id=task_id,
        )
        spawn_t0 = time.monotonic()
        try:
            process.start()
        except Exception as exc:
            # Forkserver nicht erreichbar o. ae.: kein Kind, kein fd-Leck, kein stilles Weiter.
            for c in (conn, job_conn, *child_side):
                if c is not None:
                    with contextlib.suppress(Exception):
                        c.close()
            self._note_unavailable()
            raise SlotUnavailable(f"compute child for slot {slot.index} could not start: {exc}") from exc
        spawn_ms = (time.monotonic() - spawn_t0) * 1000
        for c in child_side:  # Kindseiten schliessen, sonst kommt nie ein EOF
            with contextlib.suppress(Exception):
                c.close()
        child.pid = process.pid
        try:
            # Schluss-Review: ``process.start()`` startet einen toten
            # Forkserver still neu. Ist der neue unsicher, bekommt das eben
            # geforkte Kind keine Arbeit.
            self._check_fork_source(slot.index)
        except SlotUnavailable:
            await self._kill_child(child, reason="fork_source_rejected", by_pid_only=True)
            for c in (conn, job_conn):
                if c is not None:
                    with contextlib.suppress(Exception):
                        c.close()
            raise
        self._children[process.pid] = child
        self.stats.started += 1
        reader = threading.Thread(
            target=self._reader,
            args=(child,),
            name=f"swarmq-compute-reader-{process.pid}",
            daemon=True,
        )
        self._threads[process.pid] = reader
        reader.start()
        loop.add_reader(process.sentinel, self._on_sentinel, child)
        try:
            assert child.ready is not None
            await asyncio.wait_for(asyncio.shield(child.ready), timeout=self.handshake_timeout_s)
            self.recent_spawn_ms.append((spawn_ms, (time.monotonic() - spawn_t0) * 1000))
        except (TimeoutError, ChildDied) as exc:
            self._note_unavailable()
            self._abandon_handshake(child)
            await self._kill_child(child, reason="handshake", by_pid_only=True)
            raise SlotUnavailable(f"compute child for slot {slot.index} sent no ready signal") from exc
        except asyncio.CancelledError:
            # Abbruch im Handshake: das gestartete Kind darf nicht verwaist weiterrechnen.
            self._abandon_handshake(child)
            await self._kill_child_uninterruptible(child, reason="cancelled")
            raise
        return child

    @staticmethod
    def _abandon_handshake(child: _Child) -> None:
        """FS-24: den Handshake vor dem Kill aufgeben. Ein abgebrochenes ``ready``
        meldet der Shield nicht als unabgeholte Ausnahme, und ohne ``pending``
        zaehlt ``_finalize`` keinen Kindtod: der Fehler ist schon ``unavailable``."""
        if child.ready is not None and not child.ready.done():
            child.ready.cancel()
        child.pending = None

    # ------------------------------------------------------------------ Rueckkanal

    def _reader(self, child: _Child) -> None:
        """Leser-Thread je Kind: Nachrichten und danach das EOF in den Loop reichen."""
        assert self._loop is not None
        conn = child.conn
        while True:
            try:
                raw = conn.recv_bytes()
            except Exception:  # noqa: BLE001 - EOF, geschlossenes Handle, abgerissener Rahmen: das Kind ist weg
                break
            try:
                msg = pickle.loads(raw)  # noqa: S301 - Daten aus dem eigenen Kindprozess
            except Exception as exc:  # noqa: BLE001 - Dekodierfehler trifft nur dieses Kind
                self._loop.call_soon_threadsafe(self._on_decode_error, child, exc)
                continue
            self._loop.call_soon_threadsafe(self._on_message, child, msg)
        self._loop.call_soon_threadsafe(self._on_eof, child)

    def _on_message(self, child: _Child, msg: Any) -> None:
        kind = msg[0]
        if kind == "ready":
            _, _slot_index, pid, pdeath = msg
            child.pgid_ready = True
            self.consecutive_unavailable = 0
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
                    "compute_result_task_mismatch",
                    expected=child.current_task_id,
                    received=envelope.task_id,
                    slot=child.slot,
                    pid=child.pid,
                )
                return
            child.pending.set_result(envelope)
        elif kind in ("progress", "publish"):
            self._forward_progress(msg)
        elif kind in APP_LOGGING_REPORTS:
            self._report_app_logging(child, kind, msg[1])

    def _report_app_logging(self, child: _Child, event: str, fields: dict[str, Any]) -> None:
        """Was die App am Logging der Kinder geaendert hat (KTD14; Plan
        2026-10-06-1811, KTD9): je Event einmal je Pool melden, nicht je Kind
        -- jedes Kind erbt dieselbe Einrichtung vom Forkserver."""
        if event in self._app_logging_reported:
            return
        self._app_logging_reported.add(event)
        report_app_logging(logger, event, compute_slot=child.slot, pid=child.pid, **fields)

    def _on_decode_error(self, child: _Child, exc: Exception) -> None:
        self.stats.undecodable += 1
        logger.error("compute_result_undecodable", slot=child.slot, pid=child.pid, **_error_fields(exc))
        if child.pending is not None and not child.pending.done():
            child.pending.set_exception(ChildDied(f"compute child pid={child.pid} sent an undecodable result: {exc!r}"))

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
        idle_death = slot.reuse_child is child and child.pending is None and not child.ending
        if slot.reuse_child is child:
            slot.reuse_child = None  # lazy Ersatz beim naechsten Dispatch
        if child.ready is not None and not child.ready.done():
            child.ready.set_exception(ChildDied("child exited before ready"))
            child.pending = None  # FS-24: der Handshake-Pfad meldet SlotUnavailable, kein Kindtod
        # R9: Nachkommen eines ohne Ergebnis oder ohne Ende-Signal (warmes DI-Kind)
        # gestorbenen Kindes beenden -- erst zuletzt, damit ein Signalfehler weder
        # ``run`` warten laesst noch die Pipes offen haelt.
        sweep = idle_death
        if child.pending is not None and not child.pending.done():
            self.stats.died += 1
            sweep = True
            child.pending.set_exception(ChildDied(f"compute child pid={child.pid} exited without result"))
        for c in (child.conn, child.job_conn):
            if c is not None:
                with contextlib.suppress(Exception):
                    c.close()
        if sweep:
            self._sweep_group(child)

    def _forward_progress(self, msg: tuple[Any, ...]) -> None:
        if self._progress_sink is None or self._loop is None:
            return
        sink = self._progress_sink

        async def _forward() -> None:
            await sink(msg)

        task = self._loop.create_task(_forward())
        self._progress_forwards.add(task)
        task.add_done_callback(self._progress_forwards.discard)

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
        loop = asyncio.get_running_loop()
        spawned: asyncio.Future[None] = loop.create_future()
        slot.replacement = spawned  # der naechste Dispatch wartet nur auf den Spawn, nicht aufs Ende des alten Kindes
        task = loop.create_task(self._recycle(slot, child, reason, value, spawned))
        self._background.add(task)
        task.add_done_callback(self._background.discard)

    async def _recycle(
        self, slot: _Slot, old: _Child, reason: str, value: float, spawned: asyncio.Future[None]
    ) -> None:
        """Rollierend (R7): das Ersatzkind ist bereit, bevor das alte endet."""
        try:
            try:
                await self._spawn_reuse(slot)
            except SlotUnavailable as exc:
                logger.warning("compute_recycle_replacement_failed", slot=slot.index, **_error_fields(exc))
            except Exception:
                logger.exception("compute_recycle_replacement_error", slot=slot.index)
            finally:
                slot.replacement = None
                if not spawned.done():
                    spawned.set_result(None)
            self.stats.recycled += 1
            logger.info("compute_child_recycled", slot=slot.index, reason=reason, value=value, pid=old.pid)
            if self._on_recycle is not None:
                try:
                    await self._on_recycle(slot.index, reason, value)
                except Exception:
                    logger.exception("compute_recycle_hook_failed", slot=slot.index)
        finally:
            await self._end_child(old, reason=reason)  # das alte Kind endet in jedem Fall

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
        """SIGTERM → Gnade → SIGKILL → auf den Tod warten (synchron, KTD3).

        Nur fuer gestartete Kinder (``pid`` gesetzt)."""
        if not child.process.is_alive():
            if not by_pid_only:
                self._sweep_group(child)
            return
        self.stats.killed += 1
        self._signal(child, signal.SIGTERM, by_pid_only)
        if await self._wait_dead(child, self.kill_grace_s):
            if not by_pid_only:
                self._sweep_group(child)  # Enkel, die SIGTERM ignorieren, ueberleben sonst
            return
        self._signal(child, signal.SIGKILL, by_pid_only)
        if await self._wait_dead(child, self.reap_timeout_s):
            return
        slot = self._slots[child.slot]
        slot.blocked = True
        slot.blocked_reason = reason
        self.stats.blocked_slots.add(slot.index)
        logger.error(
            "compute_child_unreaped",
            slot=child.slot,
            pid=child.pid,
            reason=reason,
            hint="child survived SIGKILL (D-state?); slot stays blocked",
        )
        if self._usable_slots() == 0 and self._free is not None:
            logger.error("compute_pool_unhealthy", failures=self.consecutive_unavailable, hint="every slot is blocked")
            for _ in range(self.size):  # Wartende in run() aufwecken: SlotUnavailable statt ewig warten
                self._free.put_nowait(-1)

    def _sweep_group(self, child: _Child) -> None:
        """FS-16 (E-036): nach dem Kindtod genau ein SIGKILL an seine Prozessgruppe.

        Nur mit eigener Gruppe (``pgid_ready``) und nur, wenn die Gruppe noch
        kein SIGKILL bekam. Synchron, also ohne Cancel-Fenster; die PGID bleibt
        reserviert, solange ein Enkel in ihr lebt."""
        if child.pid is None or not child.pgid_ready or child.group_killed:
            return
        self._signal(child, signal.SIGKILL, by_pid_only=False)

    def _signal(self, child: _Child, sig: signal.Signals, by_pid_only: bool) -> None:
        assert child.pid is not None
        group = child.pgid_ready and not by_pid_only
        try:
            if group:
                if sig == signal.SIGKILL:
                    child.group_killed = True
                os.killpg(child.pid, sig)
            else:
                os.kill(child.pid, sig)
        except ProcessLookupError:
            pass
        except PermissionError as exc:
            # EPERM: in der Gruppe lebt nur noch ein fremder (setuid-)Prozess oder die
            # PGID wurde wiederverwendet. Nicht signalisierbar darf nicht blockieren.
            logger.warning(
                "compute_signal_denied",
                slot=child.slot,
                pid=child.pid,
                signal=sig.name,
                group=group,
                **_error_fields(exc),
            )

    async def _wait_dead(self, child: _Child, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        while child.process.is_alive():
            if time.monotonic() >= deadline:
                return False
            await asyncio.sleep(0.01)
        return True


__all__ = ["ChildDied", "PoolStats", "SlotPool", "SlotUnavailable"]
