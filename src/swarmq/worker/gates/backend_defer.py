"""Zurueckstellung bei Backend-Ausfall (Gate-Backoff).

Scheitert ein Gate vor der Ausfuehrung am Backend (Verbindungs-/Timeout-
Fehler), geht die Nachricht nicht per ``nack(requeue=True)`` an derselben
Stelle zurueck, sondern ueber ``retry_later`` an die Delay-Queue der
Zustellungs-Queue. Ein Requeue kostete auf Brokern, die ihn aufs
``x-delivery-limit`` anrechnen (RabbitMQ 3.13), je Runde eine Zustellung --
nach ``delivery_limit`` Runden laege die Nachricht still im ``.XQ``. Die
Kopie ist eine neue Nachricht mit eigenem Zustellbudget.

Einbezogen: Schedfire-, Cancel-, Rate-Limit- und Lock-Gate. Expiry und
Delivery-Count entscheiden ohne Backend; die Unique-Freigabe beim Start
schluckt Backend-Fehler selbst.

Jeder Backend-Aufruf dieser Gates ist durch ``backend_op_timeout`` begrenzt
(``bounded_backend_call``): ein Blackhole zaehlt nach dieser Frist als
Backend-Ausfall, statt das Gate bis zum Socket-Timeout zu blockieren.

Nur ein Ausfall wird zurueckgestellt (``is_backend_outage``). Eine
Fehlerantwort des Backends (``BackendResponseError``: WRONGTYPE,
Skriptfehler) heilt keine Wartezeit -- sie nimmt den alten Weg (Requeue an
derselben Stelle, das Zustelllimit fuehrt sie ins Dead-Letter-Ziel).
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from typing import Any

from swarmq.exceptions import BackendConnectionError, is_backend_outage
from swarmq.logging import _error_fields, get_logger
from swarmq.worker.gates import GateContext, GateDeps, deferral_copy, publish_deferral_copy, redeliver_in_place
from swarmq.worker.outcome import DeliveryOutcome, Redelivered

logger = get_logger(__name__)


async def bounded_backend_call[T](deps: GateDeps, call: Awaitable[T], *, what: str) -> T:
    """Begrenzt einen Backend-Aufruf eines Gates auf ``backend_op_timeout``.

    Die Koordinations-Stores (Rate-Limit, Lock, Schedfire) kennen kein
    eigenes Zeitlimit; ohne diese Schranke hinge ein Gate bei einem
    Blackhole bis zum Socket-Timeout des Clients (samt dessen Retries).
    Eine Zeitueberschreitung gilt als Backend-Ausfall
    (``BackendConnectionError``) -- das Gate stellt dann zurueck.
    """
    timeout = deps.config.backend_op_timeout
    try:
        return await asyncio.wait_for(call, timeout=timeout)
    except TimeoutError as exc:
        raise BackendConnectionError(f"{what} timed out after {timeout}s") from exc


async def defer_backend_unavailable(
    deps: GateDeps,
    delivery: Any,
    ctx: GateContext,
    exc: BaseException,
    *,
    gate: str,
    stage: str | None = None,
    **context: Any,
) -> DeliveryOutcome:
    """Stellt *delivery* mit wachsendem Backoff zurueck; Outcome ``Redelivered``.

    Die Kopie behaelt alle Header (Retry-Zaehler, Workflow-Kontext,
    Unique-Hash, Enqueue-Zeit ...) ausser den Zustellzaehlern des Brokers und
    zaehlt ``x-swarmq-backend-defer-count`` hoch. Kein Retry: kein Retry-
    Budget, kein ``TASK_RETRYING``. Nur Gates vor dem FIRE_AND_FORGET-Ack
    rufen das auf -- das Original ist hier immer noch unbestaetigt.

    Das Gate loggt selbst nichts: ``task_deferred_backend_unavailable`` ist
    die eine Zeile je Zurueckstellung; ``stage`` und ``context`` (z. B.
    ``schedfire_key``, ``workflow_id``, ``lock_name``) tragen, was frueher
    in der zusaetzlichen Gate-Warnung stand.
    """
    if not is_backend_outage(exc):
        # Das Backend hat geantwortet, die Operation scheitert trotzdem
        # (WRONGTYPE, Skriptfehler): das heilt keine Wartezeit. Alter Weg --
        # Requeue an derselben Stelle; das Zustelllimit fuehrt eine dauerhaft
        # scheiternde Nachricht ins Dead-Letter-Ziel.
        logger.error(
            "task_gate_backend_error",
            task_id=ctx.task_id,
            task_name=ctx.task_name,
            gate=gate,
            stage=stage,
            **context,
            **_error_fields(exc),
        )
        await redeliver_in_place(deps, delivery, ctx)
        return Redelivered
    headers, attempt, delay_ms = deferral_copy(deps, delivery, ctx)
    try:
        # Publish-then-ack: ``retry_later`` bestaetigt das Original erst, wenn
        # der Broker die Kopie bestaetigt hat (Publisher-Confirm).
        await publish_deferral_copy(
            deps,
            delivery,
            ctx,
            delay_ms=delay_ms,
            headers=headers,
            deferral="backend_defer",
            gate=gate,
            stage=stage,
            **context,
        )
    except Exception as publish_exc:  # noqa: BLE001 - Broker gestoert, Rueckfall auf alten Weg
        # Broker ebenfalls gestoert: der alte Weg. Das Original ist nicht
        # bestaetigt; ``PublishIndeterminate`` kann eine Kopie hinterlassen
        # (Duplikat statt Verlust, at-least-once).
        logger.warning(
            "task_defer_failed_redelivered",
            task_id=ctx.task_id,
            task_name=ctx.task_name,
            gate=gate,
            stage=stage,
            **context,
            **{f"backend_{key}": value for key, value in _error_fields(exc).items()},
            **_error_fields(publish_exc),
        )
        await redeliver_in_place(deps, delivery, ctx)
        return Redelivered
    logger.warning(
        "task_deferred_backend_unavailable",
        task_id=ctx.task_id,
        task_name=ctx.task_name,
        gate=gate,
        defer_count=attempt,
        delay_ms=delay_ms,
        stage=stage,
        **context,
        **_error_fields(exc),
    )
    return Redelivered
