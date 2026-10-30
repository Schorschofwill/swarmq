"""Nachweis B24: Verbindungsabriss, waehrend ein Publish auf sein Confirm wartet.

Loadsuite v0.12.0rc1 (``swarmq-loadsuite/docs/testlauf-v0.12.0rc1.md``): In
der Broker-Trennung endeten Dispatches mit einem rohen
``aiormq.exceptions.AMQPConnectionError``, obwohl die Nachricht angekommen
war und lief. Ein Produzent, der wie fuer ``BrokerConnectionError``
dokumentiert wiederholt, fuehrt den Task doppelt aus.

Deterministisch ueber Toxiproxy (``SWARMQ_TEST_TOXIPROXY_*``): ein
``timeout``-Toxic auf der Gegenrichtung (Broker -> Client) haelt das Confirm
zurueck, waehrend der Publish den Broker erreicht. Liegt die Nachricht in der
Queue (direkt auf Port ``SWARMQ_TEST_RABBITMQ_PORT`` gelesen), trennt das
Abschalten des Proxys die Verbindung; aiormq meldet "Server connection
unexpectedly closed".

Test-Liste:
- [x] Abriss beim Warten auf das Confirm -> PublishIndeterminate; die Nachricht
      liegt genau einmal in der Queue
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from collections.abc import AsyncIterator

import aio_pika
import pytest
from aiormq.exceptions import AMQPConnectionError

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import PublishIndeterminate
from tests.e2e.combinations._toxiproxy import ToxiproxyClient
from tests.integration.broker.conftest import PROXY as _PROXY
from tests.integration.broker.conftest import PROXY_PORT as _PROXY_PORT
from tests.support.queues import delete_queue_family
from tests.support.services import rabbitmq_amqp_url
from tests.support.wait import wait_until

pytestmark = [pytest.mark.integration, pytest.mark.timeout(60)]

_TOXIC = "b24-hold-confirms"


@pytest.fixture
async def hold_confirms(toxiproxy: ToxiproxyClient) -> AsyncIterator[ToxiproxyClient]:
    """Proxy aus ``conftest.toxiproxy``; entfernt am Ende den Toxic dieses Tests."""
    try:
        yield toxiproxy
    finally:
        await toxiproxy.remove_toxic(_PROXY, _TOXIC)


async def _depth(queue: str) -> int:
    connection = await aio_pika.connect(rabbitmq_amqp_url())
    try:
        channel = await connection.channel()
        declared = await channel.declare_queue(queue, passive=True)
        assert declared.declaration_result.message_count is not None
        return declared.declaration_result.message_count
    finally:
        await connection.close()


async def test_connection_lost_while_waiting_for_the_confirm_is_indeterminate(
    hold_confirms: ToxiproxyClient,
) -> None:
    toxiproxy = hold_confirms
    queue = f"b24_{uuid.uuid4().hex[:8]}"
    broker = RabbitMQBroker(rabbitmq_amqp_url("127.0.0.1", _PROXY_PORT))
    await broker.connect()
    try:
        await broker.declare_queue(queue)
        await toxiproxy.add_toxic(_PROXY, "timeout", attributes={"timeout": 0}, stream="downstream", name=_TOXIC)
        publish = asyncio.create_task(broker.publish(queue, b'{"task": "b24"}'))

        async def in_queue() -> bool:
            return await _depth(queue) == 1

        arrived = await wait_until(in_queue, timeout=10)
        assert arrived, "the publish never reached the broker through the proxy"
        assert not publish.done(), "the confirm came through despite the toxic"

        await toxiproxy.set_proxy_enabled(_PROXY, False)
        with pytest.raises(PublishIndeterminate) as caught:
            await publish

        assert isinstance(caught.value.__cause__, AMQPConnectionError)
        assert await _depth(queue) == 1  # angekommen, und swarmq hat nicht neu gesendet
    finally:
        with contextlib.suppress(Exception):
            await broker.close()
        await delete_queue_family(rabbitmq_amqp_url(), queue)
