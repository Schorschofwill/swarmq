"""Lock-Gate (Mutex/Semaphor), Plan U7 — verbatim aus ``pipeline.py``.

Vier Regeln aus ``docs/solutions/logic-errors/worker-pipeline-stage-ack-ordering-2026-04-16.md``:
vor dem FIRE_AND_FORGET-Ack; Entscheidung + Publish + Ack in einem
try/except; Zustands-Header der Wartezeit (``x-swarmq-lock-conflicts``,
``x-swarmq-lock-wait-since``, ``x-swarmq-lock-holder*`` +
``x-swarmq-lock-name``); ein Helfer mit eindeutigem Ergebnis.

Wartezeit (Plan 0.12.0, MR 13, BZ-05): Ein Konflikt stellt die Nachricht mit
Backoff zurueck, ohne Deckel. Gemessen wird ab dem ersten Konflikt und ab
dem letzten Halterwechsel (Fingerprint der Owner, ohne Score und PTTL). In
die DLQ (``LockWaitExceeded``) geht die Nachricht erst, wenn derselbe Halter
laenger haelt als die beim Wechsel beobachtete PTTL + 60 s (Stillstand) oder
``lock_wait_max_s`` ab dem ersten Konflikt erreicht ist; ``max_age`` bleibt
die harte Obergrenze (Expiry-Gate davor). Ist der Fingerprint nicht messbar
(Q15), gilt nur die Obergrenze. Eine Neuzustellung durch den Broker und die
Kopie einer spaeteren Ausfuehrung (``x-swarmq-lock-wait-gen``) beginnen eine
neue Wartezeit (Review MR 13, P2-4). Der Legacy-Zaehler
``x-swarmq-lock-conflict-count`` wird nicht mehr erhoeht (0.11-Worker
derselben App schickten wartende Kopien sonst ab 10 in die DLQ).

Fallbacks (KTD3, unverändert): Render-Fehler, leerer Render und das Ende der
Wartezeit gehen an das Dead-Letter-Ziel und geben das Dedup-Flag inline frei;
schlägt der Publish fehl, ``nack(requeue=False)`` (DLX ohne Diagnose-Header).
Konflikt → exponentielle Rückstellung, bei Publish-Fehler
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
import hashlib
import inspect
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from swarmq.contracts.coordination import LockObservation
from swarmq.exceptions import LockWaitExceeded
from swarmq.header_text import bounded_header_text
from swarmq.headers import (
    HEADER_LOCK_CONFLICTS,
    HEADER_LOCK_HOLDER,
    HEADER_LOCK_HOLDER_SINCE,
    HEADER_LOCK_HOLDER_TTL_MS,
    HEADER_LOCK_WAIT_GEN,
    HEADER_LOCK_WAIT_SINCE,
    HEADER_REPLAY_ID,
    HEADER_RETRY_COUNT,
    LOCK_WAIT_STATE_HEADERS,
)
from swarmq.keyspace import KeySpace
from swarmq.locking import compute_lock_ttl_ms
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
from swarmq.worker.lock_wait_tracker import LockWaitTracker
from swarmq.worker.outcome import Continue, DeliveryOutcome, Redelivered

if TYPE_CHECKING:
    from swarmq.contracts.coordination import LockStore, LockToken

logger = get_logger(__name__)

# ``error_type`` der DLQ-Kopie, wenn die Wartezeit endet (MR 13, BZ-05):
# Stillstand des Halters oder Obergrenze ``SWARMQ_LOCK_WAIT_MAX_S``; es gibt
# keine ausloesende Ausnahme.
LOCK_WAIT_EXCEEDED_ERROR_TYPE = LockWaitExceeded.__name__

# Stillstand: derselbe Halter haelt laenger als die beim Halterwechsel
# beobachtete PTTL des Keys plus diese Toleranz.
LOCK_STALL_GRACE_MS = 60_000

# Exponential backoff for lock-conflict requeue: base * 2^(n-1), capped. Der
# Deckel liegt bei 1 min, damit Lock-Rueckstellungen in Stufen <= 1 min
# bleiben (Autoscaling zaehlt nur die Haupt-Queue).
LOCK_CONFLICT_BACKOFF_BASE_MS = 100
LOCK_CONFLICT_BACKOFF_CAP_MS = 60_000

# Halter-Header der Wartezeit (entfallen, sobald kein Halter mehr da ist).
LOCK_HOLDER_HEADERS = (HEADER_LOCK_HOLDER, HEADER_LOCK_HOLDER_SINCE, HEADER_LOCK_HOLDER_TTL_MS)


def lock_conflict_delay_ms(conflicts: int) -> int:
    """Wartezeit vor der naechsten Zustellung nach ``conflicts`` Konflikten."""
    exponent = min(max(conflicts, 1) - 1, 20)  # ab 2^10 liegt die Wartezeit ohnehin am Deckel
    delay: int = LOCK_CONFLICT_BACKOFF_BASE_MS << exponent
    return min(delay, LOCK_CONFLICT_BACKOFF_CAP_MS)


def holder_fingerprint(owners: frozenset[str]) -> str:
    """Fingerprint der Halter: nur die Menge der Owner, ohne Score und PTTL
    (sonst saehe jede Erneuerung wie ein Halterwechsel aus)."""
    return hashlib.sha1(",".join(sorted(owners)).encode()).hexdigest()[:16]


def _as_int(raw: Any, default: int) -> int:
    """Nicht-negativer Header-Wert; ein kaputter Header zaehlt wie keiner."""
    try:
        value = int(raw)
    except TypeError, ValueError:
        return default
    return value if value >= 0 else default


def wait_generation(headers: Any) -> str:
    """Generation einer Zustellung fuer die Lock-Wartezeit (Review MR 13,
    P2-4): Retry-Zaehler und Replay-ID -- beide bleiben auf den Kopien der
    Gates gleich und aendern sich mit jeder neuen Ausfuehrung."""
    replay = headers.get(HEADER_REPLAY_ID)
    return f"{_as_int(headers.get(HEADER_RETRY_COUNT), 0)}:{'' if replay is None else replay}"


def _wait_state_is_current(headers: Any) -> bool:
    """Gehoeren die Zustands-Header zur Wartezeit dieser Ausfuehrung? Nicht
    auf der Kopie einer spaeteren (0.11-Retry-Kopie, ``dlq retry`` von 0.11).
    Nach einer Neuzustellung durch den Broker entfernt ``republish_headers``
    sie bereits."""
    return str(headers.get(HEADER_LOCK_WAIT_GEN, "0:")) == wait_generation(headers)


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
TerminalDeadLetter = Callable[[Any, GateContext, str, str, dict[str, Any], PublishFailure], Awaitable[object]]


class LockGate:
    def _lock_key(self, lock_name: str) -> str:
        """Valkey-Key des Locks fuer Hinweise, mit Namensraum (BZ-10). Es gilt
        der Namensraum des Backends (Review MR 15, E1/P1-1); ein Backend ohne
        lesbaren Namensraum (Q15) faellt auf die Konfiguration zurueck."""
        configured = self._deps.config.namespace
        namespace = getattr(self._deps.backend, "namespace", configured)
        if namespace is not None and not isinstance(namespace, str):
            namespace = configured
        return KeySpace(namespace).lock(lock_name)

    def __init__(self, deps: GateDeps, *, dead_letter: TerminalDeadLetter) -> None:
        self._deps = deps
        self._dead_letter = dead_letter
        # Q15: ein Store ohne ``observe`` wird einmal gemeldet.
        self._observe_unsupported_logged = False
        # Gauges (R11): wartende Konflikte und aeltester Wartender je Worker.
        self.waits = LockWaitTracker()

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
                delivery,
                ctx,
                str(exc),
                type(exc).__name__,
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
                delivery,
                ctx,
                error_message,
                "ValueError",
                {"x-swarmq-lock-render-error": error_message},
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
                    deps,
                    delivery,
                    ctx,
                    exc,
                    gate="lock",
                    stage="acquire",
                    lock_name=lock_name,
                )
            logger.error(
                "lock_acquire_failed",
                task_id=ctx.task_id,
                task_name=ctx.task_name,
                lock_name=lock_name,
                **_error_fields(exc),
            )
            await redeliver_in_place(deps, delivery, ctx)
            return Redelivered

        if acquired:
            self.waits.forget(ctx.task_id)
            return Continue

        # Conflict: lock held by someone else (mutex) or semaphore full.
        return await self._conflict(delivery, ctx, lock_manager, lock_name, ttl_ms, task_info.max_concurrency)

    async def _conflict(
        self, delivery: Any, ctx: GateContext, lock_manager: Any, lock_name: str, ttl_ms: int, max_concurrency: int
    ) -> DeliveryOutcome:
        """Zurueckstellen mit Backoff; DLQ erst am Ende der Wartezeit
        (Stillstand des Halters oder ``lock_wait_max_s``)."""
        deps = self._deps
        headers = republish_headers(delivery, ctx)
        if not _wait_state_is_current(headers):
            # Review MR 13, P2-4: die Zustands-Header stammen aus einer
            # frueheren Wartezeit -- eine neue beginnt.
            for header in LOCK_WAIT_STATE_HEADERS:
                headers.pop(header, None)
        headers[HEADER_LOCK_WAIT_GEN] = wait_generation(headers)
        now_ms = int(time.time() * 1000)
        conflicts = _as_int(headers.get(HEADER_LOCK_CONFLICTS), 0) + 1
        headers[HEADER_LOCK_CONFLICTS] = conflicts
        # Wedged-lock recovery: operator reads this header from the DLQ
        # message and runs `valkey-cli DEL sq:lock:<lock_name>`.
        headers["x-swarmq-lock-name"] = lock_name
        wait_since = _as_int(headers.get(HEADER_LOCK_WAIT_SINCE), 0) or now_ms
        headers[HEADER_LOCK_WAIT_SINCE] = wait_since
        waited_ms = max(now_ms - wait_since, 0)

        # Log nur beim ersten Konflikt und danach einmal je Minute (sobald die
        # Rueckstellung am 60-s-Deckel liegt); das Signal bleibt je Konflikt (API).
        if conflicts == 1 or lock_conflict_delay_ms(conflicts - 1) >= LOCK_CONFLICT_BACKOFF_CAP_MS:
            logger.info(
                "task_lock_conflict",
                task_id=ctx.task_id,
                task_name=ctx.task_name,
                lock_name=lock_name,
                lock_conflict_count=conflicts,
                waited_ms=waited_ms,
            )
        await deps.signals.dispatch(
            Signal.TASK_LOCKED,
            task_id=ctx.task_id,
            task_name=ctx.task_name,
            lock_name=lock_name,
            lock_conflict_count=conflicts,
        )

        limit_ms = int(deps.config.lock_wait_max_s * 1000)
        if waited_ms >= limit_ms:
            message = (
                f"{LOCK_WAIT_EXCEEDED_ERROR_TYPE}: lock wait limit reached: waited {waited_ms / 1000:.0f} s "
                f"({conflicts} conflicts) on lock "
                f"{lock_name!r}, SWARMQ_LOCK_WAIT_MAX_S={deps.config.lock_wait_max_s:g}; "
                f"recover a wedged lock via valkey-cli DEL {self._lock_key(lock_name)}"
            )
            return await self._wait_over(delivery, ctx, lock_name, conflicts, waited_ms, "max_wait", message)

        held_ms = await self._track_holder(lock_manager, ctx, lock_name, max_concurrency, ttl_ms, headers, now_ms)
        if held_ms is not None:
            message = (
                f"{LOCK_WAIT_EXCEEDED_ERROR_TYPE}: lock holder stalled: the same holder has held lock "
                f"{lock_name!r} for {held_ms / 1000:.0f} s, "
                f"longer than its TTL + {LOCK_STALL_GRACE_MS // 1000} s ({conflicts} conflicts, waited "
                f"{waited_ms / 1000:.0f} s); recover a wedged lock via valkey-cli DEL {self._lock_key(lock_name)}"
            )
            return await self._wait_over(delivery, ctx, lock_name, conflicts, waited_ms, "stalled", message)

        self.waits.record(ctx.task_id, wait_since_ms=wait_since, now_ms=now_ms)
        try:
            await deps.broker.retry_later(delivery, delay_ms=lock_conflict_delay_ms(conflicts), headers=headers)
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

    async def _track_holder(
        self,
        lock_manager: Any,
        ctx: GateContext,
        lock_name: str,
        max_concurrency: int,
        ttl_ms: int,
        headers: dict[str, Any],
        now_ms: int,
    ) -> int | None:
        """Halter-Header der Kopie fortschreiben; liefert die Haltedauer in
        ms, wenn derselbe Halter laenger haelt als die beim Wechsel
        beobachtete PTTL + 60 s (Stillstand), sonst ``None``."""
        observation = await self._observe(lock_manager, ctx, lock_name, max_concurrency)
        if observation is None:
            return None  # nicht messbar (Q15): nur die Obergrenze gilt
        if observation.pttl_ms == -2 or not observation.owners:
            # Kein Halter (der Konflikt war ein Rennen): kein Stillstand.
            for header in LOCK_HOLDER_HEADERS:
                headers.pop(header, None)
            return None
        fingerprint = holder_fingerprint(observation.owners)
        if headers.get(HEADER_LOCK_HOLDER) != fingerprint:
            headers[HEADER_LOCK_HOLDER] = fingerprint
            headers[HEADER_LOCK_HOLDER_SINCE] = now_ms
            # PTTL -1 (ohne Frist, Fremdzustand): die eigene Lock-TTL ist die Basis.
            headers[HEADER_LOCK_HOLDER_TTL_MS] = observation.pttl_ms if observation.pttl_ms >= 0 else ttl_ms
            return None
        holder_since = _as_int(headers.get(HEADER_LOCK_HOLDER_SINCE), 0) or now_ms
        base_ms = _as_int(headers.get(HEADER_LOCK_HOLDER_TTL_MS), 0) or ttl_ms
        held_ms = max(now_ms - holder_since, 0)
        return held_ms if held_ms > base_ms + LOCK_STALL_GRACE_MS else None

    async def _observe(
        self, lock_manager: Any, ctx: GateContext, lock_name: str, max_concurrency: int
    ) -> LockObservation | None:
        """Halter und PTTL lesen; ``None``, wenn nicht messbar (Store ohne
        ``observe`` -- Q15 --, ein Ergebnis, das keine ``LockObservation``
        ist -- ``AsyncMock``, Protocol-Subklasse; Review MR 13, P2-5 -- oder
        Backend-Fehler)."""
        try:
            observation = await bounded_backend_call(
                self._deps,
                lock_manager.observe(lock_name, max_concurrency=max_concurrency),
                what=f"lock observe for {lock_name!r}",
            )
        except (AttributeError, NotImplementedError, TypeError) as exc:
            self._warn_observe_unsupported(lock_name, **_error_fields(exc))
            return None
        except Exception as exc:
            logger.warning(
                "lock_observe_failed",
                task_id=ctx.task_id,
                task_name=ctx.task_name,
                lock_name=lock_name,
                **_error_fields(exc),
            )
            return None
        if not isinstance(observation, LockObservation):
            self._warn_observe_unsupported(lock_name, result_type=type(observation).__name__)
            return None
        return observation

    def _warn_observe_unsupported(self, lock_name: str, **fields: Any) -> None:
        """Q15: einmal je Gate melden, dass der Store keine Halter liefert."""
        if self._observe_unsupported_logged:
            return
        self._observe_unsupported_logged = True
        logger.warning(
            "lock_observe_unsupported",
            lock_name=lock_name,
            **fields,
            hint="the lock store cannot report holders; stalled holders are not detected, "
            "only SWARMQ_LOCK_WAIT_MAX_S ends the wait",
        )

    async def _wait_over(
        self, delivery: Any, ctx: GateContext, lock_name: str, conflicts: int, waited_ms: int, reason: str, message: str
    ) -> DeliveryOutcome:
        """Ende der Wartezeit: ueber den terminalen Dead-Letter des Workers
        (MR 3, BZ-05/BZ-13) -- Fehlerergebnis, Workflow-Fehlmarkierung,
        ``TASK_DLQ`` und Unique-Freigabe."""
        self.waits.forget(ctx.task_id)
        logger.error(
            "lock_wait_exceeded",
            task_id=ctx.task_id,
            task_name=ctx.task_name,
            lock_name=lock_name,
            reason=reason,
            lock_conflicts=conflicts,
            waited_ms=waited_ms,
        )
        await self._dead_letter(
            delivery,
            ctx,
            message,
            LOCK_WAIT_EXCEEDED_ERROR_TYPE,
            {"x-swarmq-lock-name": lock_name},
            lambda publish_exc: self._conflict_dlx_fallback(delivery, ctx, publish_exc, lock_name, conflicts),
        )
        return Redelivered

    async def _conflict_dlx_fallback(
        self, delivery: Any, ctx: GateContext, exc: Exception, lock_name: str, conflicts: int
    ) -> None:
        """DLQ-Publish am Ende der Wartezeit gescheitert: ein Requeue kaeme
        mit abgelaufener Frist zurueck, scheiterte wieder und liefe ohne
        Backoff im Kreis. ``nack(requeue=False)`` routet ueber die Broker-DLX
        nach ``{queue}.XQ`` -- nicht verloren, aber ohne Diagnose-Header."""
        logger.error(
            "lock_conflict_dlq_failed",
            task_id=ctx.task_id,
            task_name=ctx.task_name,
            lock_name=lock_name,
            lock_conflicts=conflicts,
            **_error_fields(exc),
            note=f"message routed via broker DLX to {ctx.queue}.XQ "
            "without diagnostic headers; recover wedged lock "
            f"via valkey-cli DEL {self._lock_key(lock_name)}",
        )
        # Terminal DLX fallback — route first, then release.
        await route_via_dlx(
            self._deps,
            delivery,
            reason="dlq_publish_failed",
            task_id=ctx.task_id,
            task_name=ctx.task_name,
            error=str(exc),
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
            deps,
            delivery,
            reason="dlq_publish_failed",
            task_id=ctx.task_id,
            task_name=ctx.task_name,
            error=str(exc),
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
            if self._deps.keep_lock is not None:
                # BZ-25: der Keeper verlaengert den Lock, solange der Handler lebt.
                self._deps.keep_lock(token, ttl_ms, ctx)
        return token is not None


async def release_acquired_locks(
    lock_manager: LockStore,
    active_handler_locks: dict[uuid.UUID, list[LockToken]],
    handler_id: uuid.UUID,
    acquired_locks: list[LockToken],
    *,
    log: Any,
    drop: Callable[[LockToken], object] | None = None,
) -> None:
    """Release every lock acquired by this handler. Called in the
    ``_handle_message`` finally block. Failures are logged and
    swallowed — the task already produced its result; release
    problems must not break the worker pipeline. The TTL-recovery
    path is the safety net for any lock that ends up not released.

    ``drop`` (BZ-25): nimmt den Lock VOR dem Freigabeversuch aus dem Keeper,
    auch wenn der Versuch scheitert.
    """
    for token in acquired_locks:
        kept = drop(token) if drop is not None else None
        # Task-Felder aus der Lease (Review MR 13, Hinweis); im Handler setzt
        # sie zusaetzlich der Log-Kontext.
        task_fields = {field: getattr(kept, field) for field in ("task_id", "task_name") if hasattr(kept, field)}
        try:
            released = await lock_manager.release(token)
        except Exception as exc:
            # KTD11: die Lock-TTL heilt den Fall selbst -> WARNING.
            log.warning(
                "lock_release_failed",
                lock_name=token.name,
                **_error_fields(exc),
            )
            continue
        if released is False:
            # BZ-04 (Plan 0.12.0, MR 13): Key weg oder fremder Halter --
            # ein zweiter Halter konnte parallel laufen (Eviction, Datenverlust,
            # abgelaufene TTL). Vorher still uebergangen.
            log.warning(
                "task_lock_lost",
                lock_name=token.name,
                **task_fields,
                stage="release",
                hint="the lock was gone or held by another owner at release; a second holder may have run in parallel",
            )
    # Drop the worker-wide registry entry whether release succeeded
    # or not — anything still held will time out via TTL, and we
    # don't want shutdown-cleanup to double-release.
    active_handler_locks.pop(handler_id, None)
