"""Namen der Queues einer Queue-Familie (Plan 0.12.0, MR 0, Q16).

Neutral und rein: ``worker``, ``scheduler`` und ``client`` duerfen es nutzen,
ohne den RabbitMQ-Broker zu importieren. ``broker/rabbitmq/topology.py``
re-exportiert die Funktionen.

Zu jeder Haupt-Queue ``q`` gehoeren ``{q}.DQ`` (Delay-Queue, deren
Dead-Letter-Ziel wieder ``q`` ist) und ``{q}.XQ`` (Dead-Letter-Queue). Seit
0.12 (BZ-01, MR 10) kommen die Delay-Stufen ``{q}.DQ.1s`` bis ``{q}.DQ.1h``
dazu: RabbitMQ laesst Nachrichten mit eigener TTL nur am Kopf einer Queue
verfallen; getrennte Stufen halten kurze Rueckstellungen nicht hinter langen
fest. Wartezeiten ueber 1 h bleiben in ``{q}.DQ``.
"""

from __future__ import annotations

_DELAY_SUFFIX = ".DQ"
_DEAD_LETTER_SUFFIX = ".XQ"

#: Delay-Stufen (BZ-01, MR 10): Kennung und Obergrenze der Wartezeit in ms.
#: Benachbarte Stufen liegen hoechstens Faktor 4 auseinander; innerhalb einer
#: Stufe wartet eine Nachricht so hoechstens das Vierfache (Spike S9),
#: mindestens aber bis zu 1 s.
DELAY_TIERS: tuple[tuple[str, int], ...] = (
    ("1s", 1_000),
    ("4s", 4_000),
    ("15s", 15_000),
    ("1m", 60_000),
    ("4m", 240_000),
    ("15m", 900_000),
    ("1h", 3_600_000),
)
_TIER_LABELS = frozenset(label for label, _ in DELAY_TIERS)


def queue_family(queue: str) -> tuple[str, ...]:
    """Alle Queues der Familie von ``queue``: Haupt-, Delay- und Dead-Letter-Queue."""
    return (queue, f"{queue}{_DELAY_SUFFIX}", f"{queue}{_DEAD_LETTER_SUFFIX}")


def is_delay_queue(name: str) -> bool:
    """``True`` fuer eine Delay-Queue einer Familie: ``{q}.DQ`` oder eine
    ihrer Delay-Stufen ``{q}.DQ.<stufe>`` (BZ-01, MR 10)."""
    base, dot, label = name.rpartition(".")
    if dot and label in _TIER_LABELS:
        name = base
    return name.endswith(_DELAY_SUFFIX) and len(name) > len(_DELAY_SUFFIX)


def main_queue_of(name: str) -> str:
    """Die Haupt-Queue der Familie von ``name``: Haupt-, Delay-, Stufen- oder
    Dead-Letter-Queue (BZ-01, MR 10)."""
    base, dot, label = name.rpartition(".")
    if dot and label in _TIER_LABELS and base.endswith(_DELAY_SUFFIX):
        name = base
    for suffix in (_DELAY_SUFFIX, _DEAD_LETTER_SUFFIX):
        if name.endswith(suffix) and len(name) > len(suffix):
            return name.removesuffix(suffix)
    return name


def delay_queue_for(queue: str, delay_ms: int) -> str:
    """Die Delay-Queue fuer eine Wartezeit von ``delay_ms``. Heute gibt es eine
    Delay-Queue je Familie, unabhaengig von der Wartezeit."""
    return f"{queue}{_DELAY_SUFFIX}"


def delay_tiers(queue: str) -> tuple[str, ...]:
    """Die Delay-Stufen von ``queue``, kuerzeste zuerst (BZ-01, MR 10)."""
    return tuple(f"{queue}{_DELAY_SUFFIX}.{label}" for label, _ in DELAY_TIERS)


def delay_tier_for(queue: str, delay_ms: int) -> str:
    """Die kleinste Delay-Stufe, deren Obergrenze ``delay_ms`` erreicht;
    ueber 1 h ``{queue}.DQ`` (BZ-01, MR 10)."""
    for label, limit_ms in DELAY_TIERS:
        if delay_ms <= limit_ms:
            return f"{queue}{_DELAY_SUFFIX}.{label}"
    return f"{queue}{_DELAY_SUFFIX}"
