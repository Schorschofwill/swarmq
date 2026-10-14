"""Publish nach verstrichener Publish-Endfrist (FS-02 Teil a, Plan
2026-10-14-0748, U3, KTD3; E-042).

Ist die Endfrist beim Eintritt schon verstrichen, geht nichts auf die
Leitung: kein Kanalzugriff, kein Verwurf, kein ``publish_deadline_exceeded``.
Der Aufrufer bekommt ein eindeutiges "nicht gesendet"
(``BrokerConnectionError``), nie ``PublishIndeterminate``.

Testliste:
- [x] publish nach Fristende: BrokerConnectionError, nichts gesendet, kein Kanal geholt
- [x] retry_later nach Fristende: nichts gesendet, Original nicht bestaetigt
- [x] dead_letter nach Fristende: ebenso
- [x] Grenzwert: Frist genau jetzt gilt als verstrichen
- [x] kein Verwurf des Publish-Kanals, keine WARNING publish_deadline_exceeded
- [x] Frist in der Zukunft: Publish geht normal raus
"""

from __future__ import annotations

import asyncio
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.contracts import Delivery
from swarmq.exceptions import BrokerConnectionError, PublishIndeterminate

pytestmark = pytest.mark.timeout(5)


def _broker() -> RabbitMQBroker:
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    broker.publish_timeout_s = 60.0
    broker._connection = Mock(transport=None, is_closed=False)
    broker._connection.ready = AsyncMock()
    broker._publish_channel = AsyncMock(is_closed=False)
    return broker


def _sent(broker: RabbitMQBroker) -> list[str]:
    publish = cast("AsyncMock", broker._publish_channel).default_exchange.publish
    return [call.kwargs["routing_key"] for call in publish.await_args_list]


def _delivery() -> Delivery:
    return Delivery(body=b"{}", headers={}, queue="tasks", handle=AsyncMock(consumer_tag=None))


def _elapse(broker: RabbitMQBroker, by: float = 1.0) -> None:
    broker.set_publish_deadline(asyncio.get_running_loop().time() - by)


async def test_publish_after_the_deadline_is_not_sent_and_not_indeterminate() -> None:
    broker = _broker()
    _elapse(broker)

    with pytest.raises(BrokerConnectionError) as info:
        await broker.publish("tasks", b"{}")

    assert not isinstance(info.value, PublishIndeterminate)
    assert "not sent" in str(info.value)
    assert _sent(broker) == []


async def test_publish_after_the_deadline_takes_no_channel_and_discards_nothing(
    swarmq_logs: list[dict[str, Any]],
) -> None:
    broker = _broker()
    broker._ensure_publish_channel = AsyncMock()  # type: ignore[method-assign]  # Grenze: Kanalbezug beobachten
    broker._discard_publish_channel = AsyncMock()  # type: ignore[method-assign]  # Grenze: Verwurf beobachten
    _elapse(broker)

    with pytest.raises(BrokerConnectionError):
        await broker.publish("tasks", b"{}")

    broker._ensure_publish_channel.assert_not_awaited()
    broker._discard_publish_channel.assert_not_awaited()
    assert [e["event"] for e in swarmq_logs if e["event"] == "publish_deadline_exceeded"] == []


async def test_deadline_exactly_now_counts_as_passed() -> None:
    broker = _broker()
    _elapse(broker, by=0.0)

    with pytest.raises(BrokerConnectionError) as info:
        await broker.publish("tasks", b"{}")

    assert not isinstance(info.value, PublishIndeterminate)
    assert _sent(broker) == []


async def test_deadline_in_the_future_still_publishes() -> None:
    broker = _broker()
    broker.set_publish_deadline(asyncio.get_running_loop().time() + 5.0)

    await broker.publish("tasks", b"{}")

    assert _sent(broker) == ["tasks"]


async def test_retry_later_after_the_deadline_sends_no_copy_and_keeps_the_original() -> None:
    broker = _broker()
    delivery = _delivery()
    _elapse(broker)

    with pytest.raises(BrokerConnectionError) as info:
        await broker.retry_later(delivery, delay_ms=1000, headers={})

    assert not isinstance(info.value, PublishIndeterminate)
    assert _sent(broker) == []
    cast("AsyncMock", delivery.handle).ack.assert_not_called()


async def test_dead_letter_after_the_deadline_sends_no_copy_and_keeps_the_original() -> None:
    broker = _broker()
    delivery = _delivery()
    _elapse(broker)

    with pytest.raises(BrokerConnectionError) as info:
        await broker.dead_letter(delivery, headers={})

    assert not isinstance(info.value, PublishIndeterminate)
    assert _sent(broker) == []
    cast("AsyncMock", delivery.handle).ack.assert_not_called()
