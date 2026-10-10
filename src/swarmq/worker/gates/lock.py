"""Lock-Gate (Mutex/Semaphor), Plan U7 — verbatim aus ``pipeline.py``.

Vier Regeln aus ``docs/solutions/logic-errors/worker-pipeline-stage-ack-ordering-2026-04-16.md``:
vor dem FIRE_AND_FORGET-Ack; Entscheidung + Publish + Ack in einem
try/except; Zähler-Header ``x-swarmq-lock-conflict-count`` (+
``x-swarmq-lock-name``); ein Helfer mit eindeutigem Ergebnis.

Fallbacks (KTD3, unverändert): Render-Fehler, leerer Render und Konflikt am
Cap gehen an das Dead-Letter-Ziel und geben das Dedup-Flag inline frei; schlägt
der Publish fehl, ``nack(requeue=False)`` (DLX ohne Diagnose-Header). Konflikt
unter dem Cap → exponentielle Rückstellung, bei Publish-Fehler
``nack(requeue=True)``. Backend-Ausfall beim Acquire → Zurueckstellung mit
Backoff (``backend_defer``), ohne den Konfliktzaehler zu erhoehen; eine
Fehlerantwort (``BackendResponseError``) oder ein anderer Fehler →
``lock_acquire_failed`` und Requeue. Jedes Requeue gibt einen gehaltenen
Schedfire-Claim vorher frei (``redeliver_in_place``).

Outcome bis U8: jeder behandelte Sub-Pfad meldet ``Redelivered`` — für den
``finally`` des Runners verhaltensgleich mit dem heutigen Stand (keine
erneute Freigabe, In-Flight-Claim wird gelöscht; auf den terminalen DLQ-
Zweigen folgenlos). Siehe Plan-Fortschrittsprotokoll U19.
"""

from __future__ import annotations

import asyncio
import inspect
import uuid
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from swarmq.header_text import bounded_header_text
from swarmq.locking import compute_lock_ttl_ms, lock_key
from swarmq.logging import _error_fields, get_logger
from swarmq.signals import Signal
from swarmq.worker.gates import (
    GateContext,
    GateDeps,
    redeliver_in_place,
    republish_headers,
    route_via_dlx,
)
from swarmq.worker.gates.backend_defer import bounded_backend_call, defer_backend_unavailable, is_backend_outage
from swarmq.worker.outcome import Continue, DeliveryOutcome, Redelivered

if TYPE_CHECKING:
    from swarmq.contracts.coordination import LockStore, LockToken

logger = get_logger(__name__)

# Lock-conflict requeue cap: after this many conflicts for the same
# message, route to DLQ instead of requeuing (visibility-first, auto-DLQ
# is the safety net).
LOCK_CONFLICT_DLQ_CAP = 10

# ``error_type`` der DLQ-Kopie am Konflikt-Deckel (``task_sent_to_dlq``); es
# gibt keine ausloesende Ausnahme.
LOCK_CONFLICT_LIMIT_ERROR_TYPE = "LockConflictLimitError"

# Exponential backoff for lock-conflict requeue: base * 2^(n-1), capped.
LOCK_CONFLICT_BACKOFF_BASE_MS = 100
LOCK_CONFLICT_BACKOFF_CAP_MS = 60_000


def render_lock_name(lock_template: str, task_class: type[Any], args: list[Any], kwargs: dict[str, Any]) -> str:
    """Resolve format placeholders in ``lock_template`` against the
    task's run-method signature so positional and keyword args can
    both fill ``"user-{user_id}"``-style templates.

    Raises ``ValueError`` (with a wrapped TypeError / KeyError as
    cause) on render failure — caller routes the message to DLQ
    with an explanatory header. Plain non-format strings short-
    circuit before any inspect work, keeping the common case cheap.
    """
    if "{" not in lock_template:
        return lock_template
    try:
        bound = inspect.signature(task_class.run).bind(None, *args, **kwargs).arguments
        # Drop the first parameter ('self') so {self} placeholders
        # don't accidentally pull in the bound None.
        bound.pop("self", None)
        return lock_template.format_map(bound)
    except (TypeError, KeyError) as exc:
        raise ValueError(f"failed to render lock name {lock_template!r}: {exc}") from exc


# (delivery, ctx, error_message, error_type, extra_headers, on_publish_failure)
# -- der terminale Dead-Letter des Workers (``RetryHandler.dead_letter``):
# Fehlerergebnis, Workflow-Fehlmarkierung, DLQ-Kopie, Unique-Freigabe und
# ``TASK_DLQ`` (Plan 0.12.0, MR 3, BZ-13/BZ-05). Scheitert der DLQ-Publish,
# laeuft statt des ``redeliver`` der DLX-Fallback des Gates.
PublishFailure = Callable[[Exception], Awaitable[None]]
TerminalDeadLetter = Callable[[Any, GateContext, str, str, dict[str, Any], PublishFailure], Awaitable[None]]


class LockGate:
    def __init__(self, deps: GateDeps, *, dead_letter: TerminalDeadLetter) -> None:
        self._deps = deps
        self._dead_letter = dead_letter

    async def apply(self, delivery: Any, ctx: GateContext) -> DeliveryOutcome:
        task_info = ctx.task_info
        assert task_info is not None
        deps = self._deps
        lock_manager = deps.lock_manager()
        if task_info.lock is None or lock_manager is None:
            return Continue
        assert ctx.task_class is not None and ctx.handler_id is not None

        # Render the lock name once. A render failure is a user
        # configuration bug — route to DLQ with a clear header, do NOT
        # requeue (it would just bounce until the cap and then DLQ
        # anyway, but with a less informative diagnostic).
        try:
            lock_name = render_lock_name(task_info.lock, ctx.task_class, ctx.args, ctx.kwargs)
        except ValueError as exc:
            logger.error(
                "lock_name_render_failed",
                task_id=ctx.task_id,
                task_name=ctx.task_name,
                lock_template=task_info.lock,
                **_error_fields(exc),
            )
            await self._dead_letter(
                delivery, ctx, str(exc), type(exc).__name__,
                {"x-swarmq-lock-render-error": bounded_header_text(str(exc))},
                lambda publish_exc: self._render_dlx_fallback(delivery, ctx, publish_exc, empty=False),
            )
            return Redelivered

        # Empty-rendered lock guard. TaskInfo rejects literal lock="" and
        # whitespace-only at construction, but a format-string template
        # like lock="{var}" with var="" renders to "" at runtime. Without
        # this check, lock_key("") returns "sq:lock:" — the bare namespace
        # prefix — which silently collides with every other empty-rendered
        # lock in the system. Same DLX-via-nack pattern as the render-fail
        # branch above.
        if not lock_name.strip():
            logger.error(
                "lock_name_rendered_empty", task_id=ctx.task_id, task_name=ctx.task_name, lock_template=task_info.lock
            )
            error_message = f"lock template {task_info.lock!r} rendered to empty string"
            await self._dead_letter(
                delivery, ctx, error_message, "ValueError", {"x-swarmq-lock-render-error": error_message},
                lambda publish_exc: self._render_dlx_fallback(delivery, ctx, publish_exc, empty=True),
            )
            return Redelivered

        owner_token = uuid.uuid4().hex
        ttl_ms = compute_lock_ttl_ms(task_info.hard_timeout)

        # Acquire + register run inside asyncio.shield so a cancellation
        # between successful Lua-acquire and the in-memory registration
        # cannot leave the lock held with no Python record. The shield
        # itself cannot block KeyboardInterrupt, but SIGTERM goes through
        # the graceful-shutdown path which drains active tasks first.
        try:
            acquired = await asyncio.shield(
                self._acquire_and_register(lock_manager, ctx, lock_name, owner_token, ttl_ms, task_info.max_concurrency)
            )
        except Exception as exc:
            # Same shape as rate-limit: an outage defers; any other
            # failure means we cannot decide safely -> nack(requeue) so the
            # broker hands the message to the next worker (the delivery
            # limit ends a persistent failure in the DLQ). Runs BEFORE
            # FIRE_AND_FORGET ack — no data loss.
            if is_backend_outage(exc):
                # Backend weg: zurueckstellen statt per Requeue das
                # Zustellbudget zu verbrauchen (kein Lock-Konflikt); die
                # Zurueckstellung loggt die eine WARNING-Zeile. Eine
                # Fehlerantwort (WRONGTYPE ...) nimmt den alten Weg unten.
                return await defer_backend_unavailable(
                    deps, delivery, ctx, exc, gate="lock", stage="acquire", lock_name=lock_name,
                )
            logger.error(
                "lock_acquire_failed", task_id=ctx.task_id, task_name=ctx.task_name, lock_name=lock_name,
                **_error_fields(exc),
            )
            await redeliver_in_place(deps, delivery, ctx)
            return Redelivered

        if acquired:
            return Continue

        # Conflict: lock held by someone else (mutex) or semaphore full.
        # Track how often this exact message has bounced so operators can
        # see a wedged lock. Plan 005 set the precedent — visibility-first,
        # auto-DLQ at the cap is the safety net.
        original_headers = republish_headers(delivery, ctx)
        prior_count = int(original_headers.get("x-swarmq-lock-conflict-count", 0))
        new_count = prior_count + 1
        original_headers["x-swarmq-lock-conflict-count"] = new_count
        # Wedged-lock recovery: operator reads this header from the DLQ
        # message and runs `valkey-cli DEL sq:lock:<lock_name>`.
        original_headers["x-swarmq-lock-name"] = lock_name

        logger.info(
            "task_lock_conflict",
            task_id=ctx.task_id,
            task_name=ctx.task_name,
            lock_name=lock_name,
            lock_conflict_count=new_count,
        )
        await deps.signals.dispatch(
            Signal.TASK_LOCKED,
            task_id=ctx.task_id,
            task_name=ctx.task_name,
            lock_name=lock_name,
            lock_conflict_count=new_count,
        )

        # At the cap, route to DLQ instead of requeuing — operator must
        # intervene (typically: DEL the wedged Valkey key, then republish
        # the DLQ message). Comparison uses >= so the cap is inclusive:
        # cap=10 means the 10th conflict routes to DLQ.
        if new_count >= LOCK_CONFLICT_DLQ_CAP:
            # Ueber den terminalen Dead-Letter des Workers (MR 3, BZ-05/BZ-13):
            # Fehlerergebnis, Workflow-Fehlmarkierung, ``TASK_DLQ`` und
            # Unique-Freigabe. Der Deckel bleibt vorerst bei 10 (MR 13).
            error_message = (
                f"lock conflict limit reached: {new_count} conflicts on lock {lock_name!r}; "
                f"recover a wedged lock via valkey-cli DEL {lock_key(lock_name)}"
            )
            await self._dead_letter(
                delivery, ctx, error_message, LOCK_CONFLICT_LIMIT_ERROR_TYPE,
                {"x-swarmq-lock-conflict-count": new_count, "x-swarmq-lock-name": lock_name},
                lambda publish_exc: self._conflict_dlx_fallback(delivery, ctx, publish_exc, lock_name, new_count),
            )
            return Redelivered

        delay_ms = min(LOCK_CONFLICT_BACKOFF_BASE_MS * (2 ** (new_count - 1)), LOCK_CONFLICT_BACKOFF_CAP_MS)

        try:
            await deps.broker.retry_later(delivery, delay_ms=delay_ms, headers=original_headers)
        except Exception as exc:
            # Same try/except shape as retry / rate-limit: broker failure
            # between publish and ack means the message might double-
            # deliver. nack(requeue=True) keeps the original in flight;
            # the next acquire will naturally serialize.
            logger.error(
                "lock_conflict_requeue_failed",
                task_id=ctx.task_id,
                task_name=ctx.task_name,
                lock_name=lock_name,
                **_error_fields(exc),
            )
            await redeliver_in_place(deps, delivery, ctx)

        return Redelivered

    async def _conflict_dlx_fallback(
        self, delivery: Any, ctx: GateContext, exc: Exception, lock_name: str, conflict_count: int
    ) -> None:
        """DLQ-Publish am Konflikt-Deckel gescheitert: ein Requeue kaeme mit
        Zaehler >= Deckel zurueck, scheiterte wieder und liefe ohne Backoff
        im Kreis. ``nack(requeue=False)`` routet ueber die Broker-DLX nach
        ``{queue}.XQ`` -- nicht verloren, aber ohne Diagnose-Header."""
        logger.error(
            "lock_conflict_dlq_failed",
            task_id=ctx.task_id,
            task_name=ctx.task_name,
            lock_name=lock_name,
            lock_conflict_count=conflict_count,
            **_error_fields(exc),
            note=f"message routed via broker DLX to {ctx.queue}.XQ "
            "without diagnostic headers; recover wedged lock "
            f"via valkey-cli DEL {lock_key(lock_name)}",
        )
        # Terminal DLX fallback — route first, then release.
        await route_via_dlx(
            self._deps, delivery, reason="dlq_publish_failed",
            task_id=ctx.task_id, task_name=ctx.task_name, error=str(exc),
        )
        await self._deps.maybe_release_unique(delivery, ctx.task_id, ctx.task_info)

    async def _render_dlx_fallback(self, delivery: Any, ctx: GateContext, exc: Exception, *, empty: bool) -> None:
        """Render-Fehler/leerer Render, DLQ-Publish gescheitert: der terminale
        DLX-Fallback (``nack(requeue=False)``) und danach die Freigabe. Der
        Fehler ist dauerhaft -- ein Requeue liefe nur erneut hinein."""
        deps = self._deps
        fields = dict(
            task_id=ctx.task_id,
            task_name=ctx.task_name,
            lock_template=ctx.task_info.lock if ctx.task_info else None,
            **_error_fields(exc),
            note=f"message routed via broker DLX to {ctx.queue}.XQ without diagnostic headers",
        )
        # R18: feste Event-Namen -- je Render-Fehlerart ein Literal.
        if empty:
            logger.error("lock_empty_render_dlq_failed", **fields)
        else:
            logger.error("lock_render_dlq_failed", **fields)
        await route_via_dlx(
            deps, delivery, reason="dlq_publish_failed",
            task_id=ctx.task_id, task_name=ctx.task_name, error=str(exc),
        )
        await deps.maybe_release_unique(delivery, ctx.task_id, ctx.task_info)

    async def _acquire_and_register(
        self, lock_manager: Any, ctx: GateContext, lock_name: str, owner_token: str, ttl_ms: int, max_concurrency: int
    ) -> bool:
        """Atomic-ish acquire + dual-register helper for asyncio.shield.

        On success appends to both the per-handler list and (via
        ``deps.register_lock``) the worker-wide registry. The two writes
        are sync so they cannot raise between them — the only failure
        mode is the acquire itself.
        """
        # LockStore-Vertrag (U15): acquire liefert den Token, der später
        # renew/release adressiert — die Registry hält Tokens.
        # Das Zeitlimit gilt INNERHALB des Shields: laeuft es ab, wird nichts
        # registriert (ein Blackhole zaehlt als Backend-Ausfall).
        token = await bounded_backend_call(
            self._deps,
            lock_manager.acquire(lock_name, owner=owner_token, ttl_ms=ttl_ms, max_concurrency=max_concurrency),
            what=f"lock acquire for {lock_name!r}",
        )
        if token is not None:
            ctx.acquired_locks.append(token)
            assert ctx.handler_id is not None
            self._deps.register_lock(ctx.handler_id, token)
        return token is not None


async def release_acquired_locks(
    lock_manager: LockStore,
    active_handler_locks: dict[uuid.UUID, list[LockToken]],
    handler_id: uuid.UUID,
    acquired_locks: list[LockToken],
    *,
    log: Any,
) -> None:
    """Release every lock acquired by this handler. Called in the
    ``_handle_message`` finally block. Failures are logged and
    swallowed — the task already produced its result; release
    problems must not break the worker pipeline. The TTL-recovery
    path is the safety net for any lock that ends up not released.
    """
    for token in acquired_locks:
        try:
            await lock_manager.release(token)
        except Exception as exc:
            # KTD11: die Lock-TTL heilt den Fall selbst -> WARNING.
            log.warning(
                "lock_release_failed",
                lock_name=token.name,
                **_error_fields(exc),
            )
    # Drop the worker-wide registry entry whether release succeeded
    # or not — anything still held will time out via TTL, and we
    # don't want shutdown-cleanup to double-release.
    active_handler_locks.pop(handler_id, None)
