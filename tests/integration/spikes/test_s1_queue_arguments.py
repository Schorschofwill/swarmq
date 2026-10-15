"""Spike S1 (MR 8): 406-Texte bei Argumentkonflikten, Delivery-Limit-Default, -1/0, Policy.

Beobachtet auf 4.3.6 und 4.2.9 (identisch):
- 406 kommt als ``ChannelPreconditionFailed``; der Text nennt das erste
  abweichende Argument. Drei Formen: ``received the value '<v>' of type
  '<t>' but current is none``, ``received none but current is the value
  '<v>' of type '<t>'`` und ``received '<v>' but current is '<v>'``. aio-pika
  kodiert kleine Ganzzahlen als ``byte`` (20, 40), größere als ``short``.
- Ohne ``x-delivery-limit`` gilt 20: nach 20 gezählten Rückgaben ist die
  Nachricht weg (21 Zustellungen). ``-1`` heißt unbegrenzt, ``0`` heißt
  "keine Wiederzustellung" (weg nach der ersten Rückgabe), nicht unbegrenzt.
- Eine ``delivery-limit``-Policy und das Argument: es gilt das kleinere;
  ``-1`` im Argument zählt nicht als kleiner (die Policy gilt).
- Gezählt wird hier mit ``basic.reject(requeue)``; auf 4.3 zählt
  ``basic.nack(requeue)`` nicht (siehe S4).

Test-Liste:
- [x] Texte der 406 für delivery-limit, queue-type, dead-letter-strategy, overflow, beide Richtungen
- [x] wirksames Delivery-Limit: ohne Argument, -1, 0, 3
- [x] Verhältnis Policy zu Argument
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import aio_pika
import pytest
from aio_pika.abc import AbstractConnection

from tests.integration.spikes._broker import QUORUM, declare, delivery_counts, publish, queue_policy, reject_requeue
from tests.support.services import RabbitMQNode

pytestmark = [pytest.mark.integration, pytest.mark.spike]

ALO = {"x-dead-letter-strategy": "at-least-once", "x-overflow": "reject-publish"}
_PREFIX = "ChannelPreconditionFailed: PRECONDITION_FAILED - inequivalent arg"


@pytest.mark.parametrize(
    ("first", "second", "expected"),
    [
        pytest.param(
            QUORUM,
            {**QUORUM, "x-delivery-limit": 20},
            "'x-delivery-limit' {where}received the value '20' of type 'byte' but current is none",
            id="delivery-limit-none-to-20",
        ),
        pytest.param(
            QUORUM,
            {**QUORUM, "x-delivery-limit": 1000},
            "'x-delivery-limit' {where}received the value '1000' of type 'short' but current is none",
            id="delivery-limit-none-to-1000",
        ),
        pytest.param(
            {**QUORUM, "x-delivery-limit": 20},
            QUORUM,
            "'x-delivery-limit' {where}received none but current is the value '20' of type 'byte'",
            id="delivery-limit-20-to-none",
        ),
        pytest.param(
            {**QUORUM, "x-delivery-limit": 20},
            {**QUORUM, "x-delivery-limit": 40},
            "'x-delivery-limit' {where}received '40' but current is '20'",
            id="delivery-limit-20-to-40",
        ),
        pytest.param(
            {},
            QUORUM,
            "'x-queue-type' {where}received 'quorum' but current is 'classic'",
            id="queue-type-classic-to-quorum",
        ),
        pytest.param(
            QUORUM,
            {},
            "'x-queue-type' {where}received 'classic' but current is 'quorum'",
            id="queue-type-quorum-to-none",
        ),
        pytest.param(
            QUORUM,
            {**QUORUM, **ALO},
            "'x-dead-letter-strategy' {where}received the value 'at-least-once' of type 'longstr' but current is none",
            id="strategy-none-to-at-least-once",
        ),
        pytest.param(
            {**QUORUM, **ALO},
            QUORUM,
            "'x-dead-letter-strategy' {where}received none but current is the value 'at-least-once' of type 'longstr'",
            id="strategy-at-least-once-to-none",
        ),
        pytest.param(
            {**QUORUM, **ALO},
            {**QUORUM, "x-overflow": "reject-publish"},
            "'x-dead-letter-strategy' {where}received none but current is the value 'at-least-once' of type 'longstr'",
            id="strategy-at-least-once-to-overflow-only",
        ),
        pytest.param(
            {**QUORUM, **ALO},
            {**QUORUM, **ALO, "x-dead-letter-strategy": "at-most-once"},
            "'x-dead-letter-strategy' {where}received 'at-most-once' but current is 'at-least-once'",
            id="strategy-at-least-once-to-at-most-once",
        ),
        pytest.param(
            QUORUM,
            {**QUORUM, "x-overflow": "reject-publish"},
            "'x-overflow' {where}received the value 'reject-publish' of type 'longstr' but current is none",
            id="overflow-none-to-reject-publish",
        ),
        pytest.param(
            {**QUORUM, "x-overflow": "reject-publish"},
            QUORUM,
            "'x-overflow' {where}received none but current is the value 'reject-publish' of type 'longstr'",
            id="overflow-reject-publish-to-none",
        ),
    ],
)
async def test_inequivalent_arg_text(
    node: RabbitMQNode,
    amqp: AbstractConnection,
    queue_name: Callable[[str], str],
    first: dict[str, Any],
    second: dict[str, Any],
    expected: str,
) -> None:
    queue = queue_name("s1-args")
    assert await declare(amqp, queue, first) is None

    assert (
        await declare(amqp, queue, second)
        == f"{_PREFIX} {expected.format(where=f"for queue '{queue}' in vhost '{node.vhost}': ")}"
    )


@pytest.mark.parametrize(
    ("arguments", "deliveries"),
    [
        pytest.param({}, 21, id="none-means-20"),
        pytest.param({"x-delivery-limit": -1}, 30, id="minus-one-unlimited"),
        pytest.param({"x-delivery-limit": 0}, 1, id="zero-no-redelivery"),
        pytest.param({"x-delivery-limit": 3}, 4, id="three"),
    ],
)
async def test_effective_delivery_limit(
    amqp: AbstractConnection, queue_name: Callable[[str], str], arguments: dict[str, Any], deliveries: int
) -> None:
    queue = queue_name("s1-limit")
    assert await declare(amqp, queue, {**QUORUM, **arguments}) is None
    await publish(amqp, queue, aio_pika.Message(b"s1"))

    counts = await delivery_counts(amqp, queue, reject_requeue, cap=30)

    assert len(counts) == deliveries, counts
    assert counts[:4] == [None, 1, 2, 3][: min(4, deliveries)]


@pytest.mark.parametrize(
    ("policy", "argument", "deliveries"),
    [
        pytest.param(3, 10, 4, id="policy-lower"),
        pytest.param(10, 3, 4, id="argument-lower"),
        pytest.param(3, None, 4, id="policy-only"),
        pytest.param(3, -1, 4, id="argument-unlimited"),
        pytest.param(3, 0, 1, id="argument-zero"),
    ],
)
async def test_delivery_limit_policy_and_argument(
    node: RabbitMQNode,
    amqp: AbstractConnection,
    queue_name: Callable[[str], str],
    policy: int,
    argument: int | None,
    deliveries: int,
) -> None:
    queue = queue_name("s1-policy")
    arguments = dict(QUORUM) if argument is None else {**QUORUM, "x-delivery-limit": argument}
    async with queue_policy(node, queue, {"delivery-limit": policy}):
        assert await declare(amqp, queue, arguments) is None
        await publish(amqp, queue, aio_pika.Message(b"s1"))

        counts = await delivery_counts(amqp, queue, reject_requeue, cap=30)

    assert len(counts) == deliveries, counts
