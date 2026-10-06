"""Delivery-Count-Gate (Plan 2026-09-03-001, R21/R30, KTD10).

Wird ein haengender Worker ersetzt, stellt der Broker seine unbestaetigten
Nachrichten erneut zu und zaehlt ``x-delivery-count`` hoch (Quorum-Queues).
Ohne dieses Gate terminiert die Broker-DLX nach ``x-delivery-limit`` stumm:
kein Fehlerergebnis, kein Workflow-Fail, keine Unique-Freigabe, kein
``TASK_DLQ``. Ab dem vorletzten Zustellversuch laeuft die Nachricht deshalb
ueber den Anwendungs-Dead-Letter des Workers. Liegt vor dem
fire-and-forget-Ack (Ack-Ordering-Invariante).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from swarmq.logging import get_logger
from swarmq.worker.gates import GateContext, GateDeps
from swarmq.worker.outcome import Continue, DeliveryOutcome, Terminal

logger = get_logger(__name__)

DELIVERY_COUNT_HEADER = "x-delivery-count"

# (delivery, ctx, error_message, error_type) — siehe ``RetryHandler.dead_letter``.
DeadLetter = Callable[[Any, GateContext, str, str], Awaitable[None]]

# Fester ``error_type`` fuer ``task_sent_to_dlq``: auf diesem Pfad gibt es
# keine ausloesende Ausnahme, nur den Zustellzaehler des Brokers.
WORKER_HUNG_ERROR_TYPE = "WorkerHungError"


class DeliveryCountGate:
    def __init__(self, deps: GateDeps, dead_letter: DeadLetter) -> None:
        self._deps = deps
        self._dead_letter = dead_letter

    async def apply(self, delivery: Any, ctx: GateContext) -> DeliveryOutcome:
        raw = (delivery.headers or {}).get(DELIVERY_COUNT_HEADER)
        if raw is None:
            return Continue
        try:
            count = int(raw)
        except (TypeError, ValueError):
            return Continue
        limit = self._deps.config.delivery_limit
        if count < limit - 1:
            return Continue
        message = (
            f"{WORKER_HUNG_ERROR_TYPE}: delivered {count} times without acknowledgement "
            f"(delivery limit {limit}); no consumer completed this message "
            "(hung worker, killed compute child or unavailable compute slot)"
        )
        logger.error(
            "delivery_limit_reached_dead_letter",
            task_id=ctx.task_id, task_name=ctx.task_name, queue=ctx.queue,
            delivery_count=count, delivery_limit=limit,
        )
        await self._dead_letter(delivery, ctx, message, WORKER_HUNG_ERROR_TYPE)
        return Terminal.already_released()


__all__ = ["DELIVERY_COUNT_HEADER", "DeliveryCountGate"]
