"""Kopplung der Publish-Deadline an die Shutdown-Gnadenfrist.

Plan 2026-08-28-001, U3 (R12 / AE7).

Ohne die Kopplung bringt der Deckel im Herunterfahren nichts: Der
Supervisor eskaliert nach seiner Gnadenfrist auf SIGKILL, waehrend ein
Publish noch gegen seine deutlich laengere eigene Frist laeuft.
"""

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import BrokerConnectionError
from tests.support.rabbitmq_faults import hanging_publish


def _broker(timeout: float = 5.0) -> RabbitMQBroker:
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    broker._connection = AsyncMock()
    broker.publish_timeout_s = timeout
    return broker


class TestEffectiveDeadline:
    async def test_without_shutdown_deadline_the_config_value_applies(self):
        """Covers AE7. Ein Producer ohne Worker setzt nie eine Endfrist."""
        broker = _broker(timeout=3.0)
        assert broker._effective_publish_timeout() == 3.0

    async def test_nearer_shutdown_deadline_wins(self):
        broker = _broker(timeout=120.0)
        loop = asyncio.get_running_loop()
        broker.set_publish_deadline(loop.time() + 0.5)

        effective = broker._effective_publish_timeout()

        assert 0.0 < effective <= 0.5

    async def test_farther_shutdown_deadline_does_not_extend_the_budget(self):
        broker = _broker(timeout=1.0)
        loop = asyncio.get_running_loop()
        broker.set_publish_deadline(loop.time() + 90.0)

        assert broker._effective_publish_timeout() == 1.0

    async def test_elapsed_shutdown_deadline_fails_immediately(self):
        broker = _broker(timeout=120.0)
        loop = asyncio.get_running_loop()
        broker.set_publish_deadline(loop.time() - 1.0)

        assert broker._effective_publish_timeout() == 0.0

        with hanging_publish(broker), pytest.raises(BrokerConnectionError):
            await asyncio.wait_for(broker.publish("tasks", b"{}"), timeout=2.0)

    async def test_clearing_the_deadline_restores_the_config_value(self):
        """Ein eingebetteter Nutzer reiht nach dem Worker-Ende weiter Tasks
        ein — eine liegengebliebene Endfrist wuerde jeden Publish sofort
        scheitern lassen."""
        broker = _broker(timeout=7.0)
        loop = asyncio.get_running_loop()
        broker.set_publish_deadline(loop.time() - 1.0)

        broker.set_publish_deadline(None)

        assert broker._effective_publish_timeout() == 7.0


class TestReconnectClearsAStaleDeadline:
    async def test_fresh_connect_resets_the_deadline(self):
        """Ein frischer Verbindungsaufbau darf keine Endfrist aus einem
        abgeschlossenen Herunterfahren mitschleppen — sonst scheitert jeder
        Publish auf der neuen Verbindung sofort."""
        broker = _broker(timeout=9.0)
        broker._connection = None
        loop = asyncio.get_running_loop()
        broker._publish_deadline_at = loop.time() - 100.0

        connection = AsyncMock()
        connection.channel = AsyncMock(return_value=AsyncMock(is_closed=False))
        with patch("aio_pika.connect_robust", AsyncMock(return_value=connection)):
            await broker.connect()

        assert broker._publish_deadline_at is None
        assert broker._effective_publish_timeout() == 9.0


class TestReschedulingRunningPublishes:
    async def test_publish_started_before_the_deadline_is_pulled_in(self):
        """Covers AE7. Der Kern des Falls: Der Publish laeuft bereits, DANN
        beginnt das Herunterfahren. Ein einmal gesetzter Zeitdeckel
        verkuerzt sich nicht von selbst — er muss nachgezogen werden."""
        broker = _broker(timeout=60.0)
        loop = asyncio.get_running_loop()

        with hanging_publish(broker) as hanging:
            publish_task = asyncio.create_task(broker.publish("tasks", b"{}"))
            await hanging.default_exchange.publish_started.wait()

            broker.set_publish_deadline(loop.time() + 0.05)

            with pytest.raises(BrokerConnectionError):
                await asyncio.wait_for(publish_task, timeout=2.0)

    async def test_clearing_does_not_extend_a_running_publish(self):
        broker = _broker(timeout=0.05)
        loop = asyncio.get_running_loop()

        with hanging_publish(broker) as hanging:
            publish_task = asyncio.create_task(broker.publish("tasks", b"{}"))
            await hanging.default_exchange.publish_started.wait()

            broker.set_publish_deadline(loop.time() + 60.0)

            with pytest.raises(BrokerConnectionError):
                await asyncio.wait_for(publish_task, timeout=2.0)
