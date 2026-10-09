"""RabbitMQ-Topologie einer Queue-Familie (Plan 0.12.0, MR 0).

Re-exportiert die reinen Namensfunktionen aus ``swarmq.queue_names`` und haelt
die Deklarationsargumente der drei Queues. Das Wire-Format steht in
``tests/integration/backend/golden/queue_args.json``.
"""

from __future__ import annotations

from typing import Any

from swarmq.queue_names import delay_queue_for, is_delay_queue, queue_family


def main_queue_arguments(queue: str, queue_type: str, delivery_limit: int) -> dict[str, Any]:
    """Haupt-Queue: die Broker-DLX zeigt auf ``{queue}.XQ``."""
    arguments: dict[str, Any] = {
        "x-queue-type": queue_type,
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": f"{queue}.XQ",
    }
    # Quorum-queue safety net (U2.1 skip): without x-delivery-limit a
    # deterministically-crashing task (SIGSEGV/OOM) is redelivered
    # forever by the broker, never reaching max_retries. The budget is
    # configurable since B5 (``SWARMQ_DELIVERY_LIMIT``), default 20 —
    # ~3-4 task-level retries per delivery plus broker-hiccup headroom,
    # while still providing a terminal DLQ path. That headroom is
    # narrower since the publish deadline: a publish that breaks its
    # deadline inside the worker redelivers the in-flight message
    # instead of acking it, and each round costs one delivery.
    if queue_type == "quorum":
        arguments["x-delivery-limit"] = delivery_limit
    return arguments


def delay_queue_arguments(queue: str, queue_type: str) -> dict[str, Any]:
    """Delay-Queue: die Broker-DLX zeigt zurueck auf die Haupt-Queue."""
    return {
        "x-queue-type": queue_type,
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": queue,
    }


def dead_letter_queue_arguments(queue_type: str) -> dict[str, Any]:
    return {"x-queue-type": queue_type}


__all__ = [
    "dead_letter_queue_arguments",
    "delay_queue_arguments",
    "delay_queue_for",
    "is_delay_queue",
    "main_queue_arguments",
    "queue_family",
]
