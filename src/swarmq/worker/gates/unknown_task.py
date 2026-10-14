"""Lookup unbekannter Tasks (Plan 0.12.0, MR 0; Gnadenfrist seit MR 11, BZ-06).

Ein nicht registrierter Task geht nicht mehr beim ersten Kontakt in die DLQ:
beim Rolling Deploy (neuer Producer, alter Worker) kennt ein neuer Worker ihn
Sekunden spaeter. Der Worker stellt ihn deshalb innerhalb der Gnadenfrist
``unknown_task_grace_s`` mit Backoff zurueck (Kopie ueber ``retry_later``,
Zustands-Header ``x-swarmq-unknown-since`` ab dem ersten Kontakt, Q6). Erst
nach Ablauf der Frist folgt der terminale Pfad. Der Logger kommt vom Aufrufer
(``Worker._lookup_task_or_route`` reicht den Logger von
``swarmq.worker.pipeline`` durch, Q17), damit Logger-Name und Patch-Pfad
unveraendert bleiben.
"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from swarmq.exceptions import TaskNotFoundError
from swarmq.headers import HEADER_UNKNOWN_SINCE
from swarmq.logging import _error_fields
from swarmq.signals import Signal
from swarmq.worker.gates import GateContext, GateDeps, redeliver_in_place, republish_headers, route_via_dlx
from swarmq.worker.outcome import DeliveryOutcome, Redelivered

if TYPE_CHECKING:
    from swarmq.registry import TaskRegistry
    from swarmq.task import Task

# Argumente delivery, ctx, error_message, error_type, extra_headers und
# on_publish_failure -- der terminale Dead-Letter des Workers (``RetryHandler.dead_letter``), wie
# ihn auch das Lock-Gate nutzt.
PublishFailure = Callable[[Exception], Awaitable[None]]
TerminalDeadLetter = Callable[[Any, GateContext, str, str, dict[str, Any], PublishFailure], Awaitable[bool]]

#: Wartezeit der ersten Rueckstellung; danach so lang wie bisher gewartet
#: (verdoppelt sich je Runde), gedeckelt auf eine Delay-Stufe <= 1 min, damit
#: der Autoscaler die Kopien nicht stundenlang als Rueckstand zaehlt.
_DEFER_BASE_MS = 1_000
_DEFER_CAP_MS = 60_000


def unknown_defer_delay_ms(*, elapsed_ms: int, remaining_ms: int) -> int:
    """Wartezeit (ms) der naechsten Rueckstellung: ``max(elapsed, 1 s)``,
    hoechstens 60 s und hoechstens die Restfrist (mindestens die Basis)."""
    return max(min(max(elapsed_ms, _DEFER_BASE_MS), _DEFER_CAP_MS, remaining_ms), _DEFER_BASE_MS)


async def lookup_task_or_route(
    registry: TaskRegistry,
    deps: GateDeps,
    delivery: Any,
    ctx: GateContext,
    *,
    log: Any,
    dead_letter: TerminalDeadLetter,
) -> type[Task] | DeliveryOutcome:
    """Registry-Lookup; ein unbekannter Task wird innerhalb der Gnadenfrist
    zurueckgestellt, danach terminal behandelt (beides ``Redelivered``: die
    Kopie ist eingereiht bzw. der Dead-Letter gibt inline frei). Der Body ist
    hier dekodiert, also traegt das Signal die Kennung -- ohne sie waere die
    Nachricht im ``.XQ`` nicht wiederzufinden (B14 des Loadsuite-Backlogs)."""
    try:
        return registry.get(ctx.task_name)
    except TaskNotFoundError:
        pass
    now_ms = int(time.time() * 1000)
    since_ms = _unknown_since_ms(delivery.headers or {}, now_ms)
    grace_ms = int(deps.config.unknown_task_grace_s * 1000)
    remaining_ms = since_ms + grace_ms - now_ms
    if grace_ms > 0 and remaining_ms > 0:
        return await _defer(
            deps, delivery, ctx, since_ms=since_ms, elapsed_ms=now_ms - since_ms, remaining_ms=remaining_ms, log=log
        )
    return await _give_up(deps, delivery, ctx, since_ms=since_ms, log=log, dead_letter=dead_letter)


async def _give_up(
    deps: GateDeps,
    delivery: Any,
    ctx: GateContext,
    *,
    since_ms: int,
    log: Any,
    dead_letter: TerminalDeadLetter,
) -> DeliveryOutcome:
    """Frist abgelaufen (oder 0): terminaler Dead-Letter des Workers mit
    Fehlerergebnis, Workflow-Fehlmarkierung, Unique-Freigabe, ``TASK_DLQ``
    und DLQ-Frist; dazu ``TASK_REJECTED`` mit dem bisherigen Grund."""
    error = f"task {ctx.task_name!r} is not registered"
    log.error("task_not_found", unknown_since=since_ms, grace_s=deps.config.unknown_task_grace_s)

    async def _fallback(exc: Exception) -> None:
        # DLQ-Publish gescheitert: die Ursache (kein Task) ist dauerhaft, ein
        # Requeue liefe nur erneut hinein -- terminaler DLX-Fallback wie bei
        # den Lock-Pfaden (``TASK_REJECTED`` mit ``dlq_publish_failed``),
        # danach die Freigabe.
        log.error("unknown_task_dlq_failed", **_error_fields(exc))
        await route_via_dlx(
            deps,
            delivery,
            reason="dlq_publish_failed",
            task_id=ctx.task_id,
            task_name=ctx.task_name,
            error=str(exc),
        )
        await deps.maybe_release_unique(delivery, ctx, None)

    if await dead_letter(delivery, ctx, error, TaskNotFoundError.__name__, {}, _fallback):
        # Das Signal folgt der liegenden DLQ-Kopie (wie am DLX-Fallback).
        await deps.signals.dispatch(
            Signal.TASK_REJECTED,
            queue=ctx.queue,
            reason="unregistered_task",
            task_id=ctx.task_id,
            task_name=ctx.task_name,
            error=error,
        )
    return Redelivered


async def _defer(
    deps: GateDeps,
    delivery: Any,
    ctx: GateContext,
    *,
    since_ms: int,
    elapsed_ms: int,
    remaining_ms: int,
    log: Any,
) -> DeliveryOutcome:
    """Kopie ueber die Delay-Queue; der erste Kontakt reist als Header mit."""
    headers = republish_headers(delivery, ctx)
    headers[HEADER_UNKNOWN_SINCE] = since_ms
    delay_ms = unknown_defer_delay_ms(elapsed_ms=elapsed_ms, remaining_ms=remaining_ms)
    try:
        await deps.broker.retry_later(delivery, delay_ms=delay_ms, headers=headers)
    except Exception as exc:  # noqa: BLE001 - Broker gestoert, Original bleibt in der Queue
        # Wie Retry und Rate-Limit: das Original bleibt in der Queue, die
        # naechste Zustellung misst weiter ab ``since_ms`` (Header bleibt).
        log.error("unknown_task_defer_failed", **_error_fields(exc))
        try:
            await redeliver_in_place(deps, delivery, ctx)
        except Exception:  # noqa: BLE001, S110 - Redeliver best effort, Fehler schon geloggt
            pass
        return Redelivered
    await deps.signals.dispatch(
        Signal.TASK_UNKNOWN_DEFERRED,
        task_id=ctx.task_id,
        task_name=ctx.task_name,
        queue=ctx.queue,
        delay=delay_ms / 1000,
        unknown_since=since_ms,
    )
    log.warning(
        "task_unknown_deferred",
        delay_ms=delay_ms,
        unknown_since=since_ms,
        grace_s=deps.config.unknown_task_grace_s,
        hint=(
            "this worker does not register the task; the message is retried with backoff until "
            "unknown_task_grace_s runs out (rolling deploy: start workers before producers)"
        ),
    )
    return Redelivered


def _unknown_since_ms(headers: Any, now_ms: int) -> int:
    """Erster Kontakt laut Header, sonst jetzt; ein kaputter Header zaehlt
    wie keiner."""
    try:
        return int(headers.get(HEADER_UNKNOWN_SINCE, now_ms))
    except TypeError, ValueError:
        return now_ms
