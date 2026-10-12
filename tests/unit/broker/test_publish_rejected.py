"""Queue-spezifische Ablehnung eines Publish (Review MR 15, P2-2).

Ein negatives Publisher-Confirm (``basic.nack``: volle Queue mit
``reject-publish``, Quorum-Queue ohne Quorum) ueber alle Versuche endet als
``PublishRejected`` (Unterklasse von ``BrokerConnectionError``, Q14); der
Scheduler zaehlt es nicht fuer den Ruecktritt. Kanal- und Verbindungsfehler
bleiben ``BrokerConnectionError``.

Test-Liste:
- [x] negatives Confirm in jedem Versuch -> ``PublishRejected`` mit bisherigem Text
- [x] Kanal-/Verbindungsfehler in jedem Versuch -> ``BrokerConnectionError``, nicht ``PublishRejected``
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest
from aio_pika.exceptions import DeliveryError
from pamqp.commands import Basic

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import BrokerConnectionError, PublishRejected

pytestmark = [pytest.mark.timeout(5)]


@pytest.fixture(autouse=True)
def _no_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    # Die Wiederholungen warten 0.05 * 2**n s; im Unit-Test ohne Wartezeit.
    original = asyncio.sleep

    async def _instant(_delay: float, *args: object) -> None:
        await original(0)

    monkeypatch.setattr(asyncio, "sleep", _instant)


def _broker(error: BaseException) -> RabbitMQBroker:
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    broker._connection = Mock(is_closed=False, transport=None)
    broker._connection.ready = AsyncMock()
    channel = AsyncMock(is_closed=False)
    channel.default_exchange.publish = AsyncMock(side_effect=error)
    broker._publish_channel = channel
    broker._publish_channel_factory = AsyncMock(return_value=channel)
    return broker


async def test_negative_confirms_in_every_attempt_raise_publish_rejected() -> None:
    broker = _broker(DeliveryError(None, Basic.Nack()))

    with pytest.raises(PublishRejected, match="failed after 5 attempts"):
        await broker.publish("tasks", b"{}")


async def test_channel_errors_in_every_attempt_stay_a_broker_connection_error() -> None:
    # Q14-Regression: Kanal- oder Verbindungsverlust zaehlt fuer den Ruecktritt.
    from aiormq.exceptions import ChannelInvalidStateError

    broker = _broker(ChannelInvalidStateError("channel closed"))

    with pytest.raises(BrokerConnectionError, match="failed after 5 attempts") as caught:
        await broker.publish("tasks", b"{}")

    assert not isinstance(caught.value, PublishRejected)
