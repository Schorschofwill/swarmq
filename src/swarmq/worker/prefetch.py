"""Prefetch je Consumer gegen ``consumer_timeout`` (Plan 0.12.0, MR 18, BZ-20).

Der Worker meldet je konsumierter Queue einen Consumer mit
``prefetch=concurrency`` an; bei K Queues haelt er bis zu K x concurrency
unbestaetigte Nachrichten. Die letzte davon wartet hinter K - 1 Runden bis
zu K x ``hard_timeout`` auf ihr Ack; RabbitMQ schliesst den Channel nach
``consumer_timeout``. Globales QoS (ein Limit fuer alle Consumer eines
Channels) geht mit Quorum-Queues nicht (Spike S3: RabbitMQ 4.2 schliesst
die Verbindung, 4.3 ignoriert es). Darum eine dokumentierte Grenze mit
Startwarnung und Gauge.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from swarmq.logging import get_logger

logger = get_logger(__name__)

#: Ab so vielen Queues warnt diese Pruefung; mit einer Queue warnt
#: ``hard_timeout_exceeds_consumer_timeout``.
_MIN_QUEUES_FOR_WARNING = 2


def prefetch_limit(consumed_queues: Sequence[str], concurrency: int) -> int:
    """Hoechstens gehaltene unbestaetigte Nachrichten eines Workers."""
    return len(consumed_queues) * concurrency


def warn_prefetch_over_consumer_timeout(
    registry: Any, consumed_queues: Sequence[str], concurrency: int, consumer_timeout_s: float
) -> None:
    """Startwarnung, wenn ``hard_timeout`` x K ueber ``consumer_timeout_s``
    liegt (K >= 2; mit einer Queue warnt ``hard_timeout_exceeds_consumer_timeout``)."""
    count = len(consumed_queues)
    if count < _MIN_QUEUES_FOR_WARNING:
        return
    offenders = [
        f"{name}({ti.hard_timeout}s)"
        for name, cls in registry.get_all().items()
        if (ti := getattr(cls, "task_info", None)) is not None
        and ti.hard_timeout is not None
        and ti.hard_timeout * count > consumer_timeout_s
    ]
    if not offenders:
        return
    logger.warning(
        "prefetch_exceeds_consumer_timeout",
        tasks=offenders[:20],
        consumed_queues=count,
        concurrency=concurrency,
        max_unacked=prefetch_limit(consumed_queues, concurrency),
        consumer_timeout_s=consumer_timeout_s,
        hint=(
            "prefetch applies per consumer: with several queues this worker holds up to "
            "queues x concurrency unacked messages, and the last one waits up to queues x "
            "hard_timeout for its ack; RabbitMQ closes the channel after consumer_timeout. "
            "Consume fewer queues per worker, lower hard_timeout, or raise the broker's "
            "consumer_timeout (and SWARMQ_CONSUMER_TIMEOUT_S)."
        ),
    )
