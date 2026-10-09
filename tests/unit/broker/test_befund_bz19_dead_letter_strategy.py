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
- [x] Quorum-Haupt-Queue und DQ fordern at-least-once-Dead-Lettering an (BZ-19)
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker

pytestmark = [pytest.mark.timeout(5)]


@pytest.mark.befund("BZ-19")
@pytest.mark.xfail(
    strict=True,
    reason=(
        "BZ-19: Quorum-Queues mit DLX werden ohne x-dead-letter-strategy=at-least-once "
        "deklariert; der Broker-Standard at-most-once verliert DQ/XQ-Übergänge bei Leader-Wechsel"
    ),
)
async def test_quorum_dead_lettering_queues_request_at_least_once() -> None:
    broker = RabbitMQBroker("amqp://guest:guest@localhost/", queue_type="quorum")
    channel = AsyncMock(is_closed=False)
    broker._publish_channel = channel
    broker._connection = AsyncMock()

    await broker.declare_queue("tasks")

    declared = {call.args[0]: call.kwargs["arguments"] for call in channel.declare_queue.call_args_list}
    # Beide Queues mit DLX: Haupt-Queue (→ XQ) und Delay-Queue (→ Haupt-Queue).
    for name in ("tasks", "tasks.DQ"):
        args = declared[name]
        assert args.get("x-dead-letter-strategy") == "at-least-once", (
            f"{name!r} dead-letters via DLX without x-dead-letter-strategy (broker default at-most-once): {args}"
        )
        assert args.get("x-overflow") == "reject-publish", (
            f"{name!r}: at-least-once dead-lettering requires x-overflow=reject-publish: {args}"
        )
