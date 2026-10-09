"""Spike S9 (MR 10): Delay-Stufe mit Per-Message-TTL bei gemischten Wartezeiten.

Beobachtet auf 4.3.6 und 4.2.9 (identisch): In einer Quorum-Delay-Queue
verfällt nur der Kopf. Eine Nachricht mit 0,2 s hinter einer mit 0,8 s
erreicht das Ziel erst mit dieser, nach etwa 0,8 s -- also beim Vierfachen
ihrer Wartezeit, nicht später. Das bestätigt den Deckel "innerhalb einer
Stufe bis zum Vierfachen", wenn die Stufen höchstens Faktor 4 auseinander liegen.

Test-Liste:
- [x] kurze TTL hinter langer TTL: Reihenfolge und Ankunft beim Ablauf des Kopfs
"""

from __future__ import annotations

import time
from collections.abc import Callable

import aio_pika
import pytest
from aio_pika.abc import AbstractConnection

from tests.integration.spikes._broker import QUORUM, declare, get_message, publish

pytestmark = [pytest.mark.integration, pytest.mark.spike]

_LONG_S = 0.8
_SHORT_S = 0.2


async def test_short_ttl_behind_long_ttl_is_released_with_the_head(
    amqp: AbstractConnection, queue_name: Callable[[str], str]
) -> None:
    target, tier = queue_name("s9-target"), queue_name("s9-tier")
    assert await declare(amqp, target, QUORUM) is None
    tier_args = {**QUORUM, "x-dead-letter-exchange": "", "x-dead-letter-routing-key": target}
    assert await declare(amqp, tier, tier_args) is None

    start = time.monotonic()
    await publish(amqp, tier, aio_pika.Message(b"long", expiration=_LONG_S))
    await publish(amqp, tier, aio_pika.Message(b"short", expiration=_SHORT_S))
    arrivals = []
    for _ in range(2):
        message = await get_message(amqp, target)
        arrivals.append((message.body, time.monotonic() - start))

    assert [body for body, _ in arrivals] == [b"long", b"short"]
    short_arrival = arrivals[1][1]
    assert _LONG_S - 0.1 <= short_arrival <= _LONG_S + 1.0, f"short message arrived after {short_arrival:.2f} s"
