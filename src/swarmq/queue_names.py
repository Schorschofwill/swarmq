"""Namen der Queues einer Queue-Familie (Plan 0.12.0, MR 0, Q16).

Neutral und rein: ``worker``, ``scheduler`` und ``client`` duerfen es nutzen,
ohne den RabbitMQ-Broker zu importieren. ``broker/rabbitmq/topology.py``
re-exportiert die Funktionen.

Zu jeder Haupt-Queue ``q`` gehoeren ``{q}.DQ`` (Delay-Queue, deren
Dead-Letter-Ziel wieder ``q`` ist) und ``{q}.XQ`` (Dead-Letter-Queue).
"""

from __future__ import annotations

_DELAY_SUFFIX = ".DQ"
_DEAD_LETTER_SUFFIX = ".XQ"


def queue_family(queue: str) -> tuple[str, ...]:
    """Alle Queues der Familie von ``queue``: Haupt-, Delay- und Dead-Letter-Queue."""
    return (queue, f"{queue}{_DELAY_SUFFIX}", f"{queue}{_DEAD_LETTER_SUFFIX}")


def is_delay_queue(name: str) -> bool:
    """``True`` fuer eine Delay-Queue einer Familie."""
    return name.endswith(_DELAY_SUFFIX) and len(name) > len(_DELAY_SUFFIX)


def delay_queue_for(queue: str, delay_ms: int) -> str:
    """Die Delay-Queue fuer eine Wartezeit von ``delay_ms``. Heute gibt es eine
    Delay-Queue je Familie, unabhaengig von der Wartezeit."""
    return f"{queue}{_DELAY_SUFFIX}"
