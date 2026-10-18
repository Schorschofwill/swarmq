"""Nachweis F4-04 (behoben in 0.13.0): ``purge_dead_letters`` schloss seinen DLQ-Channel nicht.

Register: docs/befunde/fehlersuche-runde-4-2026-10.md (F4-04; Verdacht
R4M-E-02; Gegenprüfung ``6-gegenpruefung-M.md``).

Seit 47ecffb (MR 16, BZ-21, 0.12.0) öffnet ``_dlq_queue`` je Aufruf einen
eigenen Channel; der Docstring sagt „Der Aufrufer schliesst ihn
(``queue.channel``)“. ``_drain_dlq_with_filter`` schließt ihn im ``finally``,
``purge_dead_letters`` nicht, auch nicht, wenn ``purge()`` wirft. In einem
langlebigen Prozess (Dashboard ab swarmq 0.12, eingebettete Nutzung) bleibt
je Purge ein AMQP-Channel bis zum Verbindungsende offen (aiormq hält ihn).

Der Fake bildet die aio-pika-API nach: ``connection.channel()`` →
``Channel.get_queue`` → ``Queue.purge()`` mit ``message_count``.

Testliste:
# [x] Purge mit drei Nachrichten: Zahl 3, Queue ``tasks.XQ``, Channel geschlossen
# [x] ``purge()`` wirft: ``BrokerConnectionError``, Channel trotzdem geschlossen
"""

from __future__ import annotations

from unittest.mock import AsyncMock, Mock

import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import BrokerConnectionError

pytestmark = pytest.mark.timeout(5)


def _broker(purge: AsyncMock) -> tuple[RabbitMQBroker, Mock]:
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    channel = Mock()
    channel.is_closed = False
    channel.close = AsyncMock()
    queue = Mock()
    queue.channel = channel
    queue.purge = purge
    channel.get_queue = AsyncMock(return_value=queue)
    connection = Mock()
    connection.is_closed = False
    connection.channel = AsyncMock(return_value=channel)
    broker._connection = connection
    return broker, channel


@pytest.mark.befund("F4-04")
async def test_purge_closes_its_own_dlq_channel() -> None:
    broker, channel = _broker(AsyncMock(return_value=Mock(message_count=3)))

    assert await broker.purge_dead_letters("tasks") == 3

    channel.get_queue.assert_awaited_once_with("tasks.XQ")
    assert channel.close.await_count == 1, "DLQ-Channel nach dem Purge offen (Docstring _dlq_queue)"


@pytest.mark.befund("F4-04")
async def test_failed_purge_still_closes_its_own_dlq_channel() -> None:
    broker, channel = _broker(AsyncMock(side_effect=RuntimeError("channel error")))

    error: BaseException | None = None
    try:
        await broker.purge_dead_letters("tasks")
    except BrokerConnectionError as exc:
        error = exc

    assert isinstance(error, BrokerConnectionError)
    assert channel.close.await_count == 1, "DLQ-Channel nach gescheitertem Purge offen (Docstring _dlq_queue)"
