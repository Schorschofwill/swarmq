"""Task-Ausführung (Plan U8): Lifecycle ``pre_execution → run →
post_execution`` mit Soft-/Hard-Timeout, Cancel-Monitor, Cancel-Reason-Box,
Dishka-REQUEST-Scope, Middleware-Hooks und Signalen — verbatim aus
``pipeline.py``. Gibt ein ``ExecutionResult`` zurück; der Runner entscheidet
danach Retry/Dead-Letter bzw. den Ergebnis-Tail.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import structlog

from swarmq.cancellation import CANCEL_REASON_CANCEL, CANCEL_REASON_TIMEOUT, CancelReason
from swarmq.dishka import _MISSING, TaskContext, get_registered_container, request_container
from swarmq.exceptions import SoftTimeoutError
from swarmq.signals import Signal
from swarmq.task import DeliveryMode
from swarmq.worker.gates import GateContext, GateDeps

if TYPE_CHECKING:
    from swarmq.middleware import MessageContext, MiddlewareChain

logger = structlog.get_logger()


@dataclass
class ExecutionResult:
    result_value: Any
    is_error: bool
    is_cancelled: bool
    error_message: str
    task_exception: BaseException | None
    # Dishka-REQUEST-Scope: der Runner schließt ihn im finally (Detached-Shield).
    di_scope: Any = None
    di_token: Any = None


class TaskExecutor:
    def __init__(self, deps: GateDeps) -> None:
        self._deps = deps

    def _prepare_task_instance(self, ctx: GateContext) -> Any:
        """Task-Instanz bauen und das R3.7-Progress-Wiring setzen.

        ``Task.update_progress`` liest die vier Instanzattribute; ruft der
        Task nie update_progress, kosten sie nur ein paar Zyklen. Die
        ``result_ttl``-Kaskade spiegelt den Result-Storage-Pfad
        (task_info → config → 86400-Fallback).
        """
        deps = self._deps
        task_info = ctx.task_info
        task_class = ctx.task_class
        assert task_info is not None and task_class is not None
        task_instance = task_class()
        progress_result_ttl = task_info.result_ttl
        if progress_result_ttl is None:
            progress_result_ttl = 0 if task_info.delivery is DeliveryMode.FIRE_AND_FORGET else deps.config.result_ttl
        if progress_result_ttl in (None, 0):
            # result_ttl=0 means "don't store results" — progress gets the
            # default 24h fallback so it's still inspectable during the run.
            progress_result_ttl = 86400
        task_instance._task_id = ctx.task_id
        task_instance._backend = deps.backend
        task_instance._task_start_time = time.monotonic()
        task_instance._result_ttl_ms = progress_result_ttl * 1000
        return task_instance

    @staticmethod
    def _open_di_scope(result: ExecutionResult, ctx: GateContext, message_dict: MessageContext) -> Any:
        """Optional Dishka DI: REQUEST-Scope über den Task-Lebenszyklus öffnen.

        Der ContextVar wird HIER gesetzt — vor dem ``asyncio.create_task``
        des Run-Coros —, damit der Run-Task ihn in seinen Kontext kopiert.
        Teardown übernimmt der Runner-finally (Detached-Shield) mit der
        eingefangenen Exception (yield-Finalizer committen/rollbacken).
        Liefert das awaitbare ``__aenter__`` oder ``None`` ohne DI.
        """
        di_container: Any = get_registered_container()
        if di_container is _MISSING:
            return None
        result.di_scope = di_container(
            context={
                TaskContext: TaskContext(
                    task_id=ctx.task_id, task_name=ctx.task_name, queue=ctx.queue,
                    retry_count=message_dict["retry_count"],
                )
            }
        )
        return result.di_scope

    async def _run_with_guards(
        self,
        task_instance: Any,
        ctx: GateContext,
        result: ExecutionResult,
        fail: Any,
    ) -> None:
        """``run()`` unter Soft-/Hard-Timeout und Cancel-Monitor ausführen."""
        task_info = ctx.task_info
        assert task_info is not None
        run_coro = task_instance.run(*ctx.args, **ctx.kwargs)
        if task_info.soft_timeout is not None:
            run_coro = asyncio.wait_for(run_coro, timeout=task_info.soft_timeout)

        run_task = asyncio.create_task(run_coro)

        # Per-handler cancel-reason box (Phase 3b.3). Both the hard-timeout
        # watchdog and the cancel monitor write via set_reason_if_unset —
        # first writer wins.
        cancel_reason: list[CancelReason | None] = [None]

        def set_reason_if_unset(reason: CancelReason) -> None:
            if cancel_reason[0] is None:
                cancel_reason[0] = reason

        watchdog = None
        monitor = asyncio.create_task(self._cancel_monitor(ctx.task_id, run_task, set_reason_if_unset))
        try:
            if task_info.hard_timeout is not None:
                watchdog = asyncio.create_task(
                    self._hard_timeout_watchdog(run_task, task_info.hard_timeout, set_reason_if_unset)
                )

            await self._await_run_result(
                run_task, task_instance, ctx, result, fail, cancel_reason
            )
        finally:
            # Outer finally: guarantees watchdog + monitor cleanup
            # regardless of which path was taken.
            await self._cancel_guard(watchdog)
            await self._cancel_guard(monitor)

    async def _await_run_result(
        self,
        run_task: asyncio.Task[Any],
        task_instance: Any,
        ctx: GateContext,
        result: ExecutionResult,
        fail: Any,
        cancel_reason: list[CancelReason | None],
    ) -> None:
        task_info = ctx.task_info
        assert task_info is not None
        try:
            result.result_value = await run_task
        except TimeoutError:
            soft_err = SoftTimeoutError(f"soft timeout after {task_info.soft_timeout}s")
            fail(soft_err, f"SoftTimeoutError: {soft_err}")
        except asyncio.CancelledError:
            # Discriminate user-cancel vs hard-timeout via the
            # cancel_reason box (Phase 3b.3) — written BEFORE
            # run_task.cancel() by whichever source fires first.
            if cancel_reason[0] == CANCEL_REASON_CANCEL:
                result.is_cancelled = True
                # No task_exception / error_message: cancel bypasses
                # the error pipeline entirely.
            else:
                # "timeout" or None (bare CancelledError): hard-timeout path.
                hard_err_msg = f"hard timeout after {task_info.hard_timeout}s"
                fail(RuntimeError(hard_err_msg), hard_err_msg)
        except Exception as exc:
            fail(exc, str(exc))
        else:
            try:
                await task_instance.post_execution()
            except Exception as exc:
                fail(exc, str(exc))
                result.result_value = None

    @staticmethod
    async def _cancel_guard(guard: asyncio.Task[Any] | None) -> None:
        if guard is not None and not guard.done():
            guard.cancel()
            try:
                await guard
            except asyncio.CancelledError:
                pass

    async def _emit_terminal_hooks(
        self,
        ctx: GateContext,
        message_dict: MessageContext,
        chain: MiddlewareChain,
        result: ExecutionResult,
        duration_ms: float,
    ) -> None:
        """Post-Execute-/On-Error-/Cancel-Hooks nach aufgelöstem Lifecycle."""
        deps = self._deps
        if result.is_cancelled:
            # Cancel bypasses the error/success pipeline: no on_error, no
            # post_execute hooks — only the TASK_CANCELLED signal fires.
            await deps.signals.dispatch(
                Signal.TASK_CANCELLED, task_id=ctx.task_id, task_name=ctx.task_name, queue=ctx.queue
            )
        elif result.is_error and result.task_exception is not None:
            await chain.run_on_error(message_dict, exception=result.task_exception)
            await deps.signals.dispatch(
                Signal.TASK_FAILED,
                task_id=ctx.task_id,
                task_name=ctx.task_name,
                queue=ctx.queue,
                exception=str(result.task_exception),
            )
        else:
            await chain.run_post_execute(message_dict, result=result.result_value)
            await deps.signals.dispatch(
                Signal.TASK_COMPLETED,
                task_id=ctx.task_id, task_name=ctx.task_name, queue=ctx.queue, duration_ms=duration_ms,
            )

    async def execute(
        self, ctx: GateContext, message_dict: MessageContext, chain: MiddlewareChain
    ) -> ExecutionResult:
        deps = self._deps
        task_info = ctx.task_info
        assert task_info is not None
        # Pre-execute hooks
        await chain.run_pre_execute(message_dict)
        await deps.signals.dispatch(
            Signal.TASK_STARTED, task_id=ctx.task_id, task_name=ctx.task_name, queue=ctx.queue
        )
        start_time = time.monotonic()

        task_instance = self._prepare_task_instance(ctx)
        result = ExecutionResult(
            result_value=None, is_error=False, is_cancelled=False, error_message="", task_exception=None
        )
        di_scope = self._open_di_scope(result, ctx, message_dict)
        if di_scope is not None:
            di_request = await di_scope.__aenter__()
            result.di_token = request_container.set(di_request)

        def _fail(exc: BaseException, message: str) -> None:
            result.is_error = True
            result.error_message = message
            result.task_exception = exc
            if task_info.delivery is DeliveryMode.FIRE_AND_FORGET:
                logger.error("fire_and_forget_error", error=message)

        try:
            await task_instance.pre_execution()
        except Exception as exc:
            _fail(exc, str(exc))
        else:
            await self._run_with_guards(task_instance, ctx, result, _fail)

        duration_ms = (time.monotonic() - start_time) * 1000.0
        await self._emit_terminal_hooks(ctx, message_dict, chain, result, duration_ms)
        return result

    async def _hard_timeout_watchdog(
        self,
        run_task: asyncio.Task[Any],
        timeout: float,
        set_reason_if_unset: Callable[[CancelReason], None] | None = None,
    ) -> None:
        """Sleep for *timeout* seconds, then cancel *run_task*.

        The optional *set_reason_if_unset* callback writes ``"timeout"``
        into the per-handler cancel-reason box (Phase 3b.3) so the
        CancelledError branch can distinguish a hard-timeout from a user
        cancel. Omitted arg keeps the old behaviour for tests that
        instantiate the watchdog directly.
        """
        await asyncio.sleep(timeout)
        if set_reason_if_unset is not None:
            set_reason_if_unset(CANCEL_REASON_TIMEOUT)
        run_task.cancel()

    async def _cancel_monitor(
        self,
        task_id: str,
        run_task: asyncio.Task[Any],
        set_reason_if_unset: Callable[[CancelReason], None],
    ) -> None:
        """Poll Valkey for a cancel flag and cancel *run_task* when set.

        Polling interval: ``config.cancel_poll_interval``. Uses
        ``asyncio.wait(timeout=…)`` to short-circuit the sleep as soon as
        *run_task* completes. Transient errors from ``backend.is_cancelled``
        are logged as a warning and do not break the loop — the next poll
        re-tries. The caller cancels and awaits this coroutine in its finally.
        """
        deps = self._deps
        poll_interval = deps.config.cancel_poll_interval
        while not run_task.done():
            try:
                done, _ = await asyncio.wait({run_task}, timeout=poll_interval)
                if run_task in done:
                    # Task completed on its own — stop polling.
                    return
                if await deps.backend.is_cancelled(task_id, op_timeout=deps.config.backend_op_timeout):
                    set_reason_if_unset(CANCEL_REASON_CANCEL)
                    run_task.cancel()
                    return
            except asyncio.CancelledError:
                # Handler finally cancels us explicitly — propagate.
                raise
            except Exception as exc:
                logger.warning("cancel_monitor_error", task_id=task_id, error=str(exc))
                # Fall through — the next iteration's asyncio.wait already
                # waits poll_interval, so no extra sleep is needed.
