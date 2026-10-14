"""Nachweis BZ-19: Quorum-Dead-Lettering ist at-most-once.

Register: docs/befunde/betrieb-ueber-zeit-2026-10.md (BZ-19). Jede
Rückstellung (``{queue}.DQ`` → Haupt-Queue, TTL-Ablauf) und jedes
Delivery-Limit (Haupt-Queue → ``{queue}.XQ``) läuft über Dead-Lettering des
Brokers. Für Quorum-Queues ist dessen Standard ``at-most-once``: bei
Leader-Wechsel oder nicht verfügbarem Ziel gehen Nachrichten verloren. Die
Doku verspricht at-least-once.

Der Verlust selbst ist nur im Cluster mit Leader-Wechsel beobachtbar
(lokal Single-Node). Prüfbar ist, ob swarmq die Garantie überhaupt anfordert:
``x-dead-letter-strategy=at-least-once`` braucht laut RabbitMQ zusätzlich
``x-overflow=reject-publish``. Eine Operator-Policy könnte es ebenfalls
setzen; swarmq dokumentiert keine.

Test-Liste:
- [x] Quorum-Haupt-Queue und DQ fordern at-least-once-Dead-Lettering an (BZ-19);
      seit MR 8 für neu angelegte Queues, auf eigenen Wegwerf-Channels
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest
from aio_pika.exceptions import ChannelNotFoundEntity

from swarmq.broker.rabbitmq import RabbitMQBroker

pytestmark = [pytest.mark.timeout(5)]


@pytest.mark.entscheidung("E-017")
@pytest.mark.befund("BZ-19")
async def test_quorum_dead_lettering_queues_request_at_least_once() -> None:
    # Plan 0.12.0, MR 8 (Abschnitt 6): Deklarationen laufen auf eigenen
    # Wegwerf-Channels; die Queues existieren noch nicht (passives Declare
    # NOT_FOUND), werden also mit den Zielargumenten angelegt.
    broker = RabbitMQBroker("amqp://guest:guest@localhost/", queue_type="quorum")
    broker._publish_channel = AsyncMock(is_closed=False)
    declared: dict[str, dict[str, Any]] = {}

    async def open_channel(**_: Any) -> AsyncMock:
        channel = AsyncMock(is_closed=False)

        async def declare_queue(name: str, *, passive: bool = False, **kwargs: Any) -> None:
            if passive:
                raise ChannelNotFoundEntity(f"NOT_FOUND - no queue '{name}' in vhost '/'")
            declared[name] = kwargs["arguments"]

        channel.declare_queue = AsyncMock(side_effect=declare_queue)
        return channel

    broker._connection = AsyncMock(is_closed=False)
    broker._connection.channel = AsyncMock(side_effect=open_channel)

    await broker.declare_queue("tasks")

    # Beide Queues mit DLX: Haupt-Queue (→ XQ) und Delay-Queue (→ Haupt-Queue).
    for name in ("tasks", "tasks.DQ"):
        args = declared[name]
        assert args.get("x-dead-letter-strategy") == "at-least-once", (
            f"{name!r} dead-letters via DLX without x-dead-letter-strategy (broker default at-most-once): {args}"
        )
        assert args.get("x-overflow") == "reject-publish", (
            f"{name!r}: at-least-once dead-lettering requires x-overflow=reject-publish: {args}"
        )
