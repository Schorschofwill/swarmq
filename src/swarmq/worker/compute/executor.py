"""``ComputeExecutor`` — Task-Koerper im Rechenkind, alles andere im Loop-Prozess (U6).

Gleiche Schnittstelle wie :class:`TaskExecutor` (KTD1): ``execute`` liefert ein
``ExecutionResult``, spricht kein Broker-Verb und laesst nie eine Exception
entkommen (KTD2). ``hard_timeout`` und Cancel toeten das Kind synchron
(KTD3) und werden als heutiges Fehler- bzw. Cancel-Ergebnis abgebildet;
Kindtod ohne Envelope ist ein Fehlerergebnis; ein ausgebliebener
Ready-Handshake ist ``slot_unavailable`` (KTD4). Der REQUEST-Scope gehoert
dem Kind, ``di_scope`` bleibt ``None`` (R26).
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import Awaitable, Callable
from typing import Any

import structlog

from swarmq.cancellation import CANCEL_REASON_CANCEL, CANCEL_REASON_TIMEOUT, CancelReason
from swarmq.exceptions import SoftTimeoutError
from swarmq.signals import Signal
from swarmq.task import DeliveryMode
from swarmq.worker.compute.envelope import ComputeJob, ResultEnvelope
from swarmq.worker.compute.pool import ChildDied, SlotPool, SlotUnavailable
from swarmq.worker.execution import ExecutionResult, TaskExecutor
from swarmq.worker.gates import GateContext, GateDeps

logger = structlog.get_logger(__name__)

RecycleHook = Callable[[int, str, float], Awaitable[None]]


class ComputeExecutor(TaskExecutor):
    """Fuehrt Task-Koerper ueber den :class:`SlotPool` aus."""

    def __init__(self, deps: GateDeps, pool: SlotPool, *, worker_id: str = "") -> None:
        super().__init__(deps)
        self._pool = pool
        self._worker_id = worker_id or str(id(self))

    async def start(self) -> None:
        await self._pool.start()

    async def aclose(self) -> None:
        await self._pool.aclose()

    # ------------------------------------------------------------------ Rueckkanal

    async def progress_sink(self, msg: tuple[Any, ...]) -> None:
        """Fortschritt aus dem Kind nach Valkey schreiben (best effort, R12)."""
        deps = self._deps
        try:
            if msg[0] == "progress":
                _, task_id, fields, ttl_ms = msg
                await deps.backend.set_progress(task_id, fields, ttl_ms=ttl_ms)
            elif msg[0] == "publish":
                _, task_id, payload = msg
                await deps.backend.publish_progress(task_id, payload)
        except Exception as exc:
            logger.warning("compute_progress_forward_failed", error=str(exc))

    # ------------------------------------------------------------------ execute

    def _build_job(self, ctx: GateContext, message_dict: Any) -> ComputeJob:
        deps = self._deps
        task_info = ctx.task_info
        assert task_info is not None
        ttl = task_info.result_ttl
        if ttl is None:
            ttl = 0 if task_info.delivery is DeliveryMode.FIRE_AND_FORGET else deps.config.result_ttl
        if not ttl:
            ttl = 86400
        return ComputeJob(
            task_id=ctx.task_id,
            task_name=ctx.task_name,
            queue=ctx.queue,
            args=tuple(ctx.args),
            kwargs=dict(ctx.kwargs),
            retry_count=int(message_dict.get("retry_count", 0)),
            result_ttl_ms=int(ttl) * 1000,
            log_level=deps.config.log_level,
            log_json=deps.config.log_json,
            worker_id=self._worker_id,
        )

    async def execute(self, ctx: GateContext, message_dict: Any, chain: Any) -> ExecutionResult:
        deps = self._deps
        task_info = ctx.task_info
        assert task_info is not None
        await chain.run_pre_execute(message_dict)
        await deps.signals.dispatch(
            Signal.TASK_STARTED, task_id=ctx.task_id, task_name=ctx.task_name, queue=ctx.queue
        )
        start_time = time.monotonic()
        result = ExecutionResult(
            result_value=None, is_error=False, is_cancelled=False, error_message="", task_exception=None
        )

        def _fail(exc: BaseException, message: str) -> None:
            result.is_error = True
            result.error_message = message
            result.task_exception = exc
            if task_info.delivery is DeliveryMode.FIRE_AND_FORGET:
                logger.error("fire_and_forget_error", error=message)

        job = self._build_job(ctx, message_dict)
        use_di = bool(getattr(ctx.task_class, "_dishka_injected_keys", None))
        slot_box: list[int | None] = [None]
        cancel_reason: list[CancelReason | None] = [None]

        def set_reason_if_unset(reason: CancelReason) -> None:
            if cancel_reason[0] is None:
                cancel_reason[0] = reason

        run_task: asyncio.Task[ResultEnvelope] = asyncio.create_task(
            self._pool.run(job, use_di=use_di, on_slot=lambda index: slot_box.__setitem__(0, index))
        )
        watchdog: asyncio.Task[Any] | None = None
        if task_info.hard_timeout is not None:
            watchdog = asyncio.create_task(
                self._hard_timeout_watchdog(run_task, task_info.hard_timeout, set_reason_if_unset)
            )
        monitor = asyncio.create_task(self._cancel_monitor(ctx.task_id, run_task, set_reason_if_unset))
        try:
            await self._await_envelope(run_task, task_info, result, _fail, slot_box, cancel_reason)
        finally:
            await self._cancel_guard(watchdog)
            await self._cancel_guard(monitor)
        duration_ms = (time.monotonic() - start_time) * 1000.0
        await self._emit_terminal_hooks(ctx, message_dict, chain, result, duration_ms)
        return result

    async def _await_envelope(
        self,
        run_task: asyncio.Task[ResultEnvelope],
        task_info: Any,
        result: ExecutionResult,
        fail: Callable[[BaseException, str], None],
        slot_box: list[int | None],
        cancel_reason: list[CancelReason | None],
    ) -> None:
        """Envelope abwarten und jeden Ausgang als ``ExecutionResult`` abbilden (KTD2)."""
        try:
            envelope = await run_task
        except asyncio.CancelledError:
            # Watchdog, Cancel-Monitor oder Drain-Ende (Outer-Cancel): Kind toeten,
            # Tod bestaetigen, dann abbilden — nie propagieren.
            if not run_task.done():
                run_task.cancel()
                with contextlib.suppress(BaseException):
                    await run_task
            await self._kill_current(slot_box, cancel_reason[0])
            if cancel_reason[0] == CANCEL_REASON_CANCEL:
                result.is_cancelled = True
            else:
                msg = (
                    f"hard timeout after {task_info.hard_timeout}s"
                    if cancel_reason[0] == CANCEL_REASON_TIMEOUT
                    else "compute child cancelled during worker drain"
                )
                fail(RuntimeError(msg), msg)
            return
        except SlotUnavailable as exc:
            result.slot_unavailable = True
            fail(exc, str(exc))
            return
        except ChildDied as exc:
            fail(exc, str(exc))
            return
        except Exception as exc:  # Pool-Fehler duerfen die Pipeline nicht verlassen
            fail(exc, f"compute pool failure: {exc}")
            return
        self._apply_envelope(envelope, result, fail)

    async def _kill_current(self, slot_box: list[int | None], reason: CancelReason | None) -> None:
        if slot_box[0] is None:
            return
        await self._pool.kill_slot(slot_box[0], reason=str(reason or "drain"))

    @staticmethod
    def _apply_envelope(
        envelope: ResultEnvelope, result: ExecutionResult, fail: Callable[[BaseException, str], None]
    ) -> None:
        if envelope.ok:
            result.result_value = envelope.value
            return
        exc = envelope.error() or RuntimeError("compute child reported an unspecified error")
        message = f"SoftTimeoutError: {exc}" if isinstance(exc, SoftTimeoutError) else str(exc)
        fail(exc, message)


__all__ = ["ComputeExecutor", "RecycleHook"]
