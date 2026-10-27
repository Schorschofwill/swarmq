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
from typing import Any, override

from swarmq.cancellation import CANCEL_REASON_CANCEL, CANCEL_REASON_TIMEOUT, CancelReason
from swarmq.config import DEFAULT_RESULT_TTL_S, resolve_result_ttl
from swarmq.exceptions import SoftTimeoutError
from swarmq.logging import _error_fields, get_logger, load_log_config
from swarmq.logging_settings import LoggingSettings
from swarmq.task import DeliveryMode
from swarmq.worker.compute.envelope import ComputeJob, ResultEnvelope
from swarmq.worker.compute.pool import ChildDied, SlotPool, SlotUnavailable
from swarmq.worker.error_info import mark_worker_error
from swarmq.worker.execution import ExecutionResult, TaskExecutor
from swarmq.worker.gates import GateContext, GateDeps

logger = get_logger(__name__)

RecycleHook = Callable[[int, str, float], Awaitable[None]]


def _child_logging_fields(config: Any) -> dict[str, Any]:
    """Die Logging-Felder des ``ComputeJob`` aus den wirksamen Einstellungen
    (Plan 2026-10-06-0831, U7, KTD7)."""
    settings: LoggingSettings | None = config.logging_settings
    if settings is None:
        # Eingebettet: die Logging-Felder von ``SwarmQConfig`` sind die Quelle
        # fuer die Kinder des eingebetteten CPU-Pools (KTD7). Eine
        # ``log_config``-Datei laedt der Loop-Prozess hier genau einmal; die
        # Kinder lesen sie nie (KTD9).
        log_config: str | None = config.log_config
        settings = LoggingSettings(
            level=config.log_level,
            format="json" if config.log_json else "human",
            log_config=log_config,
            log_config_json=None if log_config is None else load_log_config(log_config),
            configure=config.configure_logging,
            logger_levels=config.logger_levels,
            queue_size=config.log_queue_size,
            drain_timeout=config.log_drain_timeout,
            batch_delay=config.log_batch_delay,
        )
    return {
        "log_level": settings.level,
        "log_level_explicit": settings.level_explicit,
        "log_json": settings.format == "json",
        "log_config": settings.log_config,
        "log_config_json": settings.log_config_json,
        "configure_logging": settings.configure,
        "logger_levels": settings.logger_levels,
        "log_queue_size": settings.queue_size,
        "log_drain_timeout": settings.drain_timeout,
        "log_batch_delay": settings.batch_delay,
    }


class ComputeExecutor(TaskExecutor):
    """Fuehrt Task-Koerper ueber den :class:`SlotPool` aus."""

    def __init__(self, deps: GateDeps, pool: SlotPool, *, worker_id: str = "") -> None:
        super().__init__(deps)
        self._pool = pool
        self._worker_id = worker_id or deps.config.worker_identity
        self._log_fields = _child_logging_fields(deps.config)

    async def start(self) -> None:
        await self._pool.start()

    async def aclose(self) -> None:
        await self._pool.aclose()

    @property
    def pool_healthy(self) -> bool:
        """``/ready``: der Pool nimmt noch Kinder an (Handshakes gelingen, Slots frei)."""
        return self._pool.healthy

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
        except Exception as exc:  # noqa: BLE001 - Fehler wird geloggt, Weiterleitung laeuft weiter
            logger.warning("compute_progress_forward_failed", **_error_fields(exc))

    # ------------------------------------------------------------------ execute

    def _build_job(self, ctx: GateContext, message_dict: Any) -> ComputeJob:
        deps = self._deps
        task_info = ctx.task_info
        assert task_info is not None
        # Progress-TTL wie im Async-Pfad: 0 ("nicht speichern") faellt auf
        # DEFAULT_RESULT_TTL_S, damit der Fortschritt lesbar bleibt.
        ttl = (
            resolve_result_ttl(
                task_info.result_ttl,
                deps.config.result_ttl,
                fire_and_forget=task_info.delivery is DeliveryMode.FIRE_AND_FORGET,
            )
            or DEFAULT_RESULT_TTL_S
        )
        return ComputeJob(
            task_id=ctx.task_id,
            task_name=ctx.task_name,
            queue=ctx.queue,
            args=tuple(ctx.args),
            kwargs=dict(ctx.kwargs),
            retry_count=int(message_dict.get("retry_count", 0)),
            result_ttl_ms=int(ttl) * 1000,
            worker_id=self._worker_id,
            workflow_id=ctx.workflow_id,
            **self._log_fields,
        )

    @override
    async def execute(self, ctx: GateContext, message_dict: Any, chain: Any) -> ExecutionResult:
        task_info = ctx.task_info
        assert task_info is not None
        await chain.run_pre_execute(message_dict)
        await self._emit_started(ctx)
        start_time = time.monotonic()
        result = ExecutionResult(
            result_value=None, is_error=False, is_cancelled=False, error_message="", task_exception=None
        )

        def _fail(exc: BaseException, message: str) -> None:
            result.is_error = True
            result.error_message = message
            result.task_exception = exc
            if task_info.delivery is not DeliveryMode.FIRE_AND_FORGET:
                return
            if result.slot_unavailable:
                # Bereits bestaetigt, kein Redelivery moeglich: Infrastrukturfehler,
                # kein Task-Fehler — eigene Meldung, damit er nicht als solcher untergeht.
                logger.error(
                    "compute_slot_unavailable_fire_and_forget_dropped",
                    task_id=ctx.task_id,
                    **_error_fields(exc),
                    hint="fire-and-forget messages are acked before dispatch; "
                    "the message is lost — check the forkserver and /ready",
                )
            else:
                logger.error("fire_and_forget_error", **_error_fields(exc))

        job = self._build_job(ctx, message_dict)
        use_di = bool(getattr(ctx.task_class, "_dishka_injected_keys", None))
        cancel_reason: list[CancelReason | None] = [None]

        def set_reason_if_unset(reason: CancelReason) -> None:
            if cancel_reason[0] is None:
                cancel_reason[0] = reason

        run_task: asyncio.Task[ResultEnvelope] = asyncio.create_task(self._pool.run(job, use_di=use_di))
        watchdog: asyncio.Task[Any] | None = None
        if task_info.hard_timeout is not None:
            watchdog = asyncio.create_task(
                self._hard_timeout_watchdog(run_task, task_info.hard_timeout, set_reason_if_unset)
            )
        monitor = asyncio.create_task(self._cancel_monitor(ctx.task_id, run_task, set_reason_if_unset))
        try:
            await self._await_envelope(run_task, task_info, result, _fail, cancel_reason)
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
        cancel_reason: list[CancelReason | None],
    ) -> None:
        """Envelope abwarten und jeden Ausgang als ``ExecutionResult`` abbilden (KTD2)."""
        try:
            envelope = await run_task
        except asyncio.CancelledError:
            # Watchdog, Cancel-Monitor oder Drain-Ende (Outer-Cancel): ``SlotPool.run``
            # toetet das Kind selbst im Cancel-Pfad und kehrt erst nach dessen Tod
            # zurueck — hier nur abwarten und abbilden, nie propagieren.
            if not run_task.done():
                run_task.cancel()
                with contextlib.suppress(BaseException):
                    await run_task
            if cancel_reason[0] == CANCEL_REASON_CANCEL:
                result.is_cancelled = True
            else:
                msg = (
                    f"hard timeout after {task_info.hard_timeout}s"
                    if cancel_reason[0] == CANCEL_REASON_TIMEOUT
                    else "compute child cancelled during worker drain"
                )
                # Vom Worker erzeugt: keine ``error_class`` (T12).
                fail(mark_worker_error(RuntimeError(msg)), msg)
            return
        except SlotUnavailable as exc:
            result.slot_unavailable = True
            fail(mark_worker_error(exc), str(exc))
            return
        except ChildDied as exc:
            fail(mark_worker_error(exc), str(exc))
            return
        except Exception as exc:  # noqa: BLE001 - Pool-Fehler duerfen die Pipeline nicht verlassen
            fail(mark_worker_error(exc), f"compute pool failure: {exc}")
            return
        self._apply_envelope(envelope, result, fail)

    @staticmethod
    def _apply_envelope(
        envelope: ResultEnvelope, result: ExecutionResult, fail: Callable[[BaseException, str], None]
    ) -> None:
        result.compute_child = envelope.pid
        if envelope.ok:
            result.result_value = envelope.value
            return
        exc = envelope.error() or RuntimeError("compute child reported an unspecified error")
        message = f"SoftTimeoutError: {exc}" if isinstance(exc, SoftTimeoutError) else str(exc)
        fail(exc, message)


__all__ = ["ComputeExecutor", "RecycleHook"]
