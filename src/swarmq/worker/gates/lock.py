"""Lock-Gate (Mutex/Semaphor), Plan U7 — verbatim aus ``pipeline.py``.

Vier Regeln aus ``docs/solutions/logic-errors/worker-pipeline-stage-ack-ordering-2026-04-16.md``:
vor dem FIRE_AND_FORGET-Ack; Entscheidung + Publish + Ack in einem
try/except; Zähler-Header ``x-swarmq-lock-conflict-count`` (+
``x-swarmq-lock-name``); ein Helfer mit eindeutigem Ergebnis.

Fallbacks (KTD3, unverändert): Render-Fehler, leerer Render und Konflikt am
Cap gehen an das Dead-Letter-Ziel und geben das Dedup-Flag inline frei; schlägt
der Publish fehl, ``nack(requeue=False)`` (DLX ohne Diagnose-Header). Konflikt
unter dem Cap → exponentielle Rückstellung, bei Publish-Fehler
``nack(requeue=True)``. Backend-Fehler beim Acquire → Zurueckstellung mit
Backoff (``backend_defer``), ohne den Konfliktzaehler zu erhoehen.

Outcome bis U8: jeder behandelte Sub-Pfad meldet ``Redelivered`` — für den
``finally`` des Runners verhaltensgleich mit dem heutigen Stand (keine
erneute Freigabe, In-Flight-Claim wird gelöscht; auf den terminalen DLQ-
Zweigen folgenlos). Siehe Plan-Fortschrittsprotokoll U19.
"""

from __future__ import annotations

import asyncio
import inspect
import uuid
from typing import Any, Literal

from swarmq.exceptions import BackendConnectionError
from swarmq.locking import compute_lock_ttl_ms
from swarmq.logging import _error_fields, get_logger
from swarmq.signals import Signal
from swarmq.worker.gates import GateContext, GateDeps, republish_headers, route_via_dlx
from swarmq.worker.gates.backend_defer import bounded_backend_call, defer_backend_unavailable
from swarmq.worker.outcome import Continue, DeliveryOutcome, Redelivered

logger = get_logger(__name__)

_DlqFailureEvent = Literal["lock_render_dlq_failed", "lock_empty_render_dlq_failed"]

# Lock-conflict requeue cap: after this many conflicts for the same
# message, route to DLQ instead of requeuing (visibility-first, auto-DLQ
# is the safety net).
LOCK_CONFLICT_DLQ_CAP = 10

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


class LockGate:
    def __init__(self, deps: GateDeps) -> None:
        self._deps = deps

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
            await self._dead_letter_terminal(
                delivery, ctx, {"x-swarmq-lock-render-error": str(exc)}, "lock_render_dlq_failed"
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
            await self._dead_letter_terminal(
                delivery,
                ctx,
                {"x-swarmq-lock-render-error": f"lock template {task_info.lock!r} rendered to empty string"},
                "lock_empty_render_dlq_failed",
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
            # Same shape as rate-limit: BackendConnectionError or any
            # other failure means we cannot decide safely. nack(requeue)
            # so the broker hands the message to the next worker. Runs
            # BEFORE FIRE_AND_FORGET ack — no data loss.
            if isinstance(exc, BackendConnectionError):
                # Backend weg: zurueckstellen statt per Requeue das
                # Zustellbudget zu verbrauchen (kein Lock-Konflikt); die
                # Zurueckstellung loggt die eine WARNING-Zeile.
                return await defer_backend_unavailable(
                    deps, delivery, ctx, exc, gate="lock", stage="acquire", lock_name=lock_name,
                )
            logger.error(
                "lock_acquire_failed", task_id=ctx.task_id, task_name=ctx.task_name, lock_name=lock_name,
                **_error_fields(exc),
            )
            await deps.broker.redeliver(delivery)
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
            try:
                await deps.broker.dead_letter(delivery, headers=original_headers)
                await deps.maybe_release_unique(delivery, ctx.task_id, task_info)
            except Exception as exc:
                # DLQ-publish failure at the cap creates a hazard: nacking
                # with requeue lets the message bounce back at count >= cap
                # again, hit the same DLQ failure, and loop without backoff.
                # nack(requeue=False) routes the message via the broker-side
                # x-dead-letter-exchange config to {queue}.XQ — the message
                # is NOT lost, but the diagnostic headers will be missing.
                logger.error(
                    "lock_conflict_dlq_failed",
                    task_id=ctx.task_id,
                    task_name=ctx.task_name,
                    lock_name=lock_name,
                    lock_conflict_count=new_count,
                    **_error_fields(exc),
                    note=f"message routed via broker DLX to {ctx.queue}.XQ "
                    "without diagnostic headers; recover wedged lock "
                    f"via valkey-cli DEL sq:lock:{lock_name}",
                )
                # Terminal DLX fallback — route first, then release.
                await route_via_dlx(
                    deps, delivery, reason="dlq_publish_failed",
                    task_id=ctx.task_id, task_name=ctx.task_name, error=str(exc),
                )
                await deps.maybe_release_unique(delivery, ctx.task_id, task_info)
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
            await deps.broker.redeliver(delivery)

        return Redelivered

    async def _dead_letter_terminal(
        self, delivery: Any, ctx: GateContext, extra_headers: dict[str, Any], failure_event: _DlqFailureEvent
    ) -> None:
        """Render-Fehler/leerer Render: Dead-Letter mit Diagnose-Header + Ack +
        Freigabe; bei Publish-Fehler der terminale DLX-Fallback
        (``nack(requeue=False)``) und danach die Freigabe — Ack-Ordering
        bleibt einheitlich."""
        deps = self._deps
        original_headers = dict(delivery.headers) if delivery.headers else {}
        original_headers.update(extra_headers)
        try:
            await deps.broker.dead_letter(delivery, headers=original_headers)
            # Release AFTER successful ack.
            await deps.maybe_release_unique(delivery, ctx.task_id, ctx.task_info)
        except Exception as broker_exc:
            fields = dict(
                task_id=ctx.task_id,
                task_name=ctx.task_name,
                lock_template=ctx.task_info.lock if ctx.task_info else None,
                **_error_fields(broker_exc),
                note=f"message routed via broker DLX to {ctx.queue}.XQ without diagnostic headers",
            )
            # R18: feste Event-Namen -- je Render-Fehlerart ein Literal.
            if failure_event == "lock_render_dlq_failed":
                logger.error("lock_render_dlq_failed", **fields)
            else:
                logger.error("lock_empty_render_dlq_failed", **fields)
            await route_via_dlx(
                deps, delivery, reason="dlq_publish_failed",
                task_id=ctx.task_id, task_name=ctx.task_name, error=str(broker_exc),
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
