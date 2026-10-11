"""Spike S2 (MR 8, 10): at-least-once mit Per-Message-TTL; reject-publish mit max-length-Policy.

Beobachtet auf 4.3.6 und 4.2.9 (identisch):
- Eine Quorum-Delay-Queue mit ``x-dead-letter-strategy=at-least-once`` und
  ``x-overflow=reject-publish`` dead-lettert eine am Kopf verfallene Nachricht
  über den Default-Exchange in die Ziel-Queue, mit ``x-death`` (``expired``).
- reject-publish mit ``max-length``-Policy 1: zwei Publishes werden bestätigt
  (Quorum-Grenze ist weich, Tiefe 2), jeder weitere bekommt ``basic.nack``
  (aiormq ``DeliveryError``); der Channel bleibt offen, nichts wird verworfen.

Test-Liste:
- [x] Verfall am Kopf einer at-least-once-Queue erreicht das Ziel über den Default-Exchange
- [x] Publish auf eine Queue mit reject-publish und max-length-Policy
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

import aio_pika
import pytest
from aio_pika.abc import AbstractConnection

from tests.integration.spikes._broker import QUORUM, declare, depth, get_message, publish, queue_policy
from tests.support.services import RabbitMQNode

pytestmark = [pytest.mark.integration, pytest.mark.spike]

ALO = {"x-dead-letter-strategy": "at-least-once", "x-overflow": "reject-publish"}


async def test_expiry_at_head_dead_letters_at_least_once_via_default_exchange(
    amqp: AbstractConnection, queue_name: Callable[[str], str]
) -> None:
    target, delay = queue_name("s2-target"), queue_name("s2-delay")
    assert await declare(amqp, target, QUORUM) is None
    delay_args = {**QUORUM, **ALO, "x-dead-letter-exchange": "", "x-dead-letter-routing-key": target}
    assert await declare(amqp, delay, delay_args) is None

    start = time.monotonic()
    await publish(amqp, delay, aio_pika.Message(b"head", expiration=0.2))
    message = await get_message(amqp, target)

    assert message.body == b"head"
    assert time.monotonic() - start < 5.0
    headers: dict[str, Any] = dict(message.headers)
    (death,) = headers["x-death"]
    assert {k: death[k] for k in ("count", "reason", "queue", "exchange", "routing-keys", "original-expiration")} == {
        "count": 1,
        "reason": "expired",
        "queue": delay,
        "exchange": "",
        "routing-keys": [delay],
        "original-expiration": "200",
    }
    assert headers["x-first-death-reason"] == "expired"


async def test_reject_publish_with_max_length_policy(
    node: RabbitMQNode, amqp: AbstractConnection, queue_name: Callable[[str], str]
) -> None:
    queue = queue_name("s2-maxlen")
    outcomes: list[str] = []
    async with queue_policy(node, queue, {"max-length": 1}):
        assert await declare(amqp, queue, {**QUORUM, "x-overflow": "reject-publish"}) is None
        channel = await amqp.channel(publisher_confirms=True)
        try:
            for index in range(4):
                try:
                    confirmation = await channel.default_exchange.publish(
                        aio_pika.Message(f"m{index}".encode()), routing_key=queue
                    )
                    outcomes.append(type(confirmation).__name__)
                except Exception as exc:
                    outcomes.append(type(exc).__name__)
            channel_open = not channel.is_closed
        finally:
            if not channel.is_closed:
                await channel.close()
        queue_depth = await depth(amqp, queue)

    # Die Grenze ist weich: die Queue nimmt eine Nachricht über max-length an,
    # danach antwortet der Broker mit basic.nack (aiormq ``DeliveryError``),
    # nicht mit drop-head; der Channel bleibt offen.
    assert outcomes == ["Ack", "Ack", "DeliveryError", "DeliveryError"]
    assert channel_open
    assert queue_depth == 2
