"""Nachweis F4-23 (behoben in 0.14.1): ``consume()`` lässt einen Verbindungsabriss roh als aiormq-Fehler durch.

Register: docs/befunde/fehlersuche-runde-4-2026-10.md (F4-23; Gegenprüfung 0.14.1).

Reißt die Verbindung während ``set_qos``, des passiven ``get_queue`` oder des
``basic.consume``, kam der Fehler als ``ConnectionClosed``,
``AMQPConnectionError``, ``ChannelInvalidStateError`` oder ``RuntimeError``
beim Aufrufer an. Verletzt: ``website/reference/errors.md`` (Recovery-Beispiel
mit ``except BrokerConnectionError`` um ``start_worker()``) und dieselbe Regel
in ``declare_queue`` (Review MR 8, H5a: „wie jeder Verbindungsverlust ein
``BrokerConnectionError``“). ``ChannelNotFoundEntity`` (Queue fehlt) bleibt
roh: Darauf baut die Behandlung von PF-20 / E-083 auf.

Grenze: der Consume-Kanal als ``AsyncMock`` wie in ``test_rabbitmq.py``.

Test-Liste:
# [x] Abriss in set_qos, get_queue oder basic.consume -> BrokerConnectionError mit Ursache
# [x] ChannelNotFoundEntity aus get_queue bleibt ChannelNotFoundEntity (Deklaration verworfen)
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import aiormq.exceptions
import pytest
from aio_pika.exceptions import ChannelNotFoundEntity

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import BrokerConnectionError

pytestmark = pytest.mark.timeout(5)


def _broker_with_consume_channel() -> tuple[RabbitMQBroker, AsyncMock, AsyncMock]:
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    channel = AsyncMock(is_closed=False)
    broker._consume_channel = channel
    broker._connection = AsyncMock()
    queue = AsyncMock()
    channel.get_queue.return_value = queue
    return broker, channel, queue


_ERRORS = [
    pytest.param(aiormq.exceptions.ConnectionClosed(320, "CONNECTION_FORCED"), id="connection-closed"),
    pytest.param(aiormq.exceptions.AMQPConnectionError("connection lost"), id="amqp-connection-error"),
    pytest.param(aiormq.exceptions.ChannelInvalidStateError("channel closed"), id="channel-invalid-state"),
    pytest.param(RuntimeError("connection closed"), id="runtime-error"),
]


@pytest.mark.befund("F4-23")
@pytest.mark.parametrize("step", ["set_qos", "get_queue", "basic_consume"])
@pytest.mark.parametrize("error", _ERRORS)
async def test_connection_loss_during_consume_becomes_broker_connection_error(step: str, error: Exception) -> None:
    broker, channel, queue = _broker_with_consume_channel()
    target = {"set_qos": channel.set_qos, "get_queue": channel.get_queue, "basic_consume": queue.consume}[step]
    target.side_effect = error

    raised: BaseException | None = None
    try:
        await broker.consume("tasks", AsyncMock(), concurrency=3)
    except Exception as exc:
        raised = exc

    assert isinstance(raised, BrokerConnectionError), (
        f"a connection loss in {step} must arrive as BrokerConnectionError, got {type(raised).__name__}"
    )
    assert raised.__cause__ is error


@pytest.mark.befund("F4-23")
async def test_a_missing_queue_on_consume_stays_channel_not_found_entity() -> None:
    broker, channel, _queue = _broker_with_consume_channel()
    channel.get_queue.side_effect = ChannelNotFoundEntity("NOT_FOUND - no queue 'tasks' in vhost '/'")

    with pytest.raises(ChannelNotFoundEntity):
        await broker.consume("tasks", AsyncMock(), concurrency=3)
