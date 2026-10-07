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
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Mapping
from typing import Any

from swarmq.backoff import backend_defer_delay
from swarmq.exceptions import BackendConnectionError
from swarmq.headers import HEADER_BACKEND_DEFER_COUNT
from swarmq.logging import _error_fields, get_logger
from swarmq.worker.gates import BROKER_DELIVERY_COUNTERS, GateContext, GateDeps, republish_headers
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


def _as_count(raw: Any) -> int:
    """Nicht-negativer Zaehler; ein kaputter Header zaehlt wie keiner."""
    try:
        count = int(raw)
    except (TypeError, ValueError):
        return 0
    return max(count, 0)


def _prior_deferrals(headers: Mapping[str, Any]) -> int:
    """Bisherige Zurueckstellungen in Folge.

    Eine Zurueckstellungs-Kopie traegt keine Broker-Zaehler (neue Nachricht).
    Traegt die Zustellung welche, hat der Broker sie nach einer Zustellung
    zurueckgegeben, die nicht zurueckgestellt hat (Requeue nach bestandenen
    Gates, Absturz, Fallback): die Folge ist unterbrochen, die Zaehlung
    beginnt neu. Klassische Queues fuehren keine solchen Zaehler.
    """
    if any(_as_count(headers.get(counter)) > 0 for counter in BROKER_DELIVERY_COUNTERS):
        return 0
    return _as_count(headers.get(HEADER_BACKEND_DEFER_COUNT))


async def defer_backend_unavailable(
    deps: GateDeps, delivery: Any, ctx: GateContext, exc: BaseException, *, gate: str,
    stage: str | None = None, **context: Any,
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
    headers = republish_headers(delivery, ctx)
    attempt = _prior_deferrals(delivery.headers or {}) + 1
    headers[HEADER_BACKEND_DEFER_COUNT] = attempt
    delay_s = backend_defer_delay(
        attempt, deps.config.backend_defer_base_delay, deps.config.backend_defer_max_delay,
    )
    delay_ms = int(delay_s * 1000)
    try:
        # Publish-then-ack: ``retry_later`` bestaetigt das Original erst, wenn
        # der Broker die Kopie bestaetigt hat (Publisher-Confirm).
        await deps.broker.retry_later(delivery, delay_ms=delay_ms, headers=headers)
    except Exception as publish_exc:
        # Broker ebenfalls gestoert: der alte Weg. Das Original ist nicht
        # bestaetigt; ``PublishIndeterminate`` kann eine Kopie hinterlassen
        # (Duplikat statt Verlust, at-least-once).
        logger.warning(
            "task_defer_failed_redelivered",
            task_id=ctx.task_id, task_name=ctx.task_name, gate=gate, stage=stage, **context,
            **{f"backend_{key}": value for key, value in _error_fields(exc).items()},
            **_error_fields(publish_exc),
        )
        await deps.broker.redeliver(delivery)
        return Redelivered
    logger.warning(
        "task_deferred_backend_unavailable",
        task_id=ctx.task_id, task_name=ctx.task_name, gate=gate,
        defer_count=attempt, delay_ms=delay_ms, stage=stage, **context, **_error_fields(exc),
    )
    return Redelivered
