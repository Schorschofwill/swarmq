"""Delivery-Count-Gate (Plan 2026-09-03-001, R21/R30, KTD10).

Wird ein haengender Worker ersetzt, stellt der Broker seine unbestaetigten
Nachrichten erneut zu und zaehlt ``x-delivery-count`` hoch (Quorum-Queues).
Ohne dieses Gate terminiert die Broker-DLX nach ``x-delivery-limit`` stumm:
kein Fehlerergebnis, kein Workflow-Fail, keine Unique-Freigabe, kein
``TASK_DLQ``. Ab dem vorletzten Zustellversuch laeuft die Nachricht deshalb
ueber den Anwendungs-Dead-Letter des Workers. Liegt vor dem
fire-and-forget-Ack (Ack-Ordering-Invariante).

BZ-29 (Plan 0.12.0, MR 3): Auf RabbitMQ 4.3 zaehlt ``nack(requeue=True)``
den ``x-delivery-count`` nicht, nur ``x-acquired-count`` (ab der zweiten
Zustellung 1, 2, ...; Spike S4). Das Gate zaehlt deshalb das Maximum beider.
Erreicht schon ``x-delivery-count`` die Grenze (Channel-Abbruch, Haenger),
heisst der Fehler wie bisher ``WorkerHungError``; sonst ``RedeliveryLimitError``
(die Zustellung wurde immer wieder per ``redeliver`` zurueckgegeben). Auf 4.2
fehlt ``x-acquired-count``; dort bleibt alles wie bisher.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from swarmq.exceptions import RedeliveryLimitError
from swarmq.logging import get_logger
from swarmq.worker.gates import GateContext, GateDeps
from swarmq.worker.outcome import Continue, DeliveryOutcome, Terminal

logger = get_logger(__name__)

DELIVERY_COUNT_HEADER = "x-delivery-count"
ACQUIRED_COUNT_HEADER = "x-acquired-count"

# (delivery, ctx, error_message, error_type) — siehe ``RetryHandler.dead_letter``.
DeadLetter = Callable[[Any, GateContext, str, str], Awaitable[None]]

# Fester ``error_type`` fuer ``task_sent_to_dlq``: auf diesem Pfad gibt es
# keine ausloesende Ausnahme, nur den Zustellzaehler des Brokers.
WORKER_HUNG_ERROR_TYPE = "WorkerHungError"
# BZ-29: die Grenze erreichte nur ``x-acquired-count`` (``redeliver``-Schleife).
REDELIVERY_LIMIT_ERROR_TYPE = RedeliveryLimitError.__name__


def _count(headers: Mapping[str, Any], name: str) -> int | None:
    """Zaehler-Header als Zahl; fehlend oder unlesbar -> ``None``."""
    raw = headers.get(name)
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


class DeliveryCountGate:
    def __init__(self, deps: GateDeps, dead_letter: DeadLetter) -> None:
        self._deps = deps
        self._dead_letter = dead_letter

    async def apply(self, delivery: Any, ctx: GateContext) -> DeliveryOutcome:
        headers = delivery.headers or {}
        delivered = _count(headers, DELIVERY_COUNT_HEADER)
        acquired = _count(headers, ACQUIRED_COUNT_HEADER)
        limit = self._deps.config.delivery_limit
        if delivered is not None and delivered >= limit - 1:
            count, error_type = delivered, WORKER_HUNG_ERROR_TYPE
            message = (
                f"{WORKER_HUNG_ERROR_TYPE}: delivered {count} times without acknowledgement "
                f"(delivery limit {limit}); no consumer completed this message "
                "(hung worker, killed compute child or unavailable compute slot)"
            )
        elif acquired is not None and acquired >= limit - 1:
            count, error_type = acquired, REDELIVERY_LIMIT_ERROR_TYPE
            message = (
                f"{REDELIVERY_LIMIT_ERROR_TYPE}: handed back {count} times without completion "
                f"(delivery limit {limit}); every delivery of this message was returned to the queue"
            )
        else:
            return Continue
        logger.error(
            "delivery_limit_reached_dead_letter",
            task_id=ctx.task_id, task_name=ctx.task_name, queue=ctx.queue,
            delivery_count=count, delivery_limit=limit,
        )
        await self._dead_letter(delivery, ctx, message, error_type)
        return Terminal.already_released()


__all__ = ["DELIVERY_COUNT_HEADER", "DeliveryCountGate"]
