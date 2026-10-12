"""Spike S6 (MR 6, 16): PublishError/DeliveryError, Zugriff auf die aiormq-Verbindung, Blocked-Zustand.

Belegt mit aio-pika 10.0.1 / aiormq 7.0.0:
- ``aiormq.exceptions.PublishError`` erbt von ``DeliveryError``; aio-pika
  re-exportiert beide unverändert (statisch, ohne Netz).
- Der Blocked-Zustand ist **synchron** lesbar, aber nur über ein privates
  Attribut: ``Connection.__connection_unblocked`` (``asyncio.Event``,
  name-mangled ``_Connection__connection_unblocked``). ``connection.blocked``
  löscht es, ``connection.unblocked`` setzt es; ein öffentliches
  ``is_blocked`` gibt es nicht (ohne Netz, über die Frame-Handler belegt).
- Zugriffspfad: ``aio_pika``-Verbindung ``.transport`` (``UnderlayConnection``)
  ``.connection`` (``aiormq.Connection``). Nach einem robusten Reconnect ist
  das ein **neues** Objekt; der Pfad muss also bei jedem Lesen neu gegangen
  werden (gegen beide Broker belegt; der Server schließt die Verbindung über
  die Management-API, kein Alarm).

Test-Liste:
- [x] PublishError ist eine DeliveryError (statisch)
- [x] Blocked-Zustand synchron lesbar (ohne Netz)
- [x] Zugriffspfad vor und nach robustem Reconnect (beide Broker)
"""

from __future__ import annotations

import asyncio
import uuid
from urllib.parse import quote

import aio_pika
import aiormq
import aiormq.exceptions
import httpx
import pytest
from pamqp import commands as spec

from tests.integration.spikes._broker import AUTH
from tests.support.services import RabbitMQNode
from tests.support.wait import wait_until

pytestmark = [pytest.mark.integration, pytest.mark.spike]

_UNBLOCKED = "_Connection__connection_unblocked"


def test_publish_error_is_a_delivery_error() -> None:
    assert issubclass(aiormq.exceptions.PublishError, aiormq.exceptions.DeliveryError)
    assert aio_pika.exceptions.PublishError is aiormq.exceptions.PublishError
    assert aio_pika.exceptions.DeliveryError is aiormq.exceptions.DeliveryError


async def test_blocked_state_is_readable_synchronously_without_network() -> None:
    connection = aiormq.Connection("amqp://guest:guest@127.0.0.1:1/")
    unblocked: asyncio.Event = getattr(connection, _UNBLOCKED)
    assert not hasattr(connection, "is_blocked")
    unblocked.set()  # wie der Reader nach dem Verbindungsaufbau

    await connection._Connection__handle_connection_blocked(spec.Connection.Blocked(reason="low on memory"))  # type: ignore[attr-defined]
    assert unblocked.is_set() is False

    await connection._Connection__handle_connection_unblocked(spec.Connection.Unblocked())  # type: ignore[attr-defined]
    assert unblocked.is_set() is True


@pytest.mark.timeout(60)
async def test_access_path_after_robust_reconnect(node: RabbitMQNode) -> None:
    name = f"swarmq-spike-s6-{uuid.uuid4().hex[:8]}"
    connection = await aio_pika.connect_robust(f"{node.amqp_url}?name={name}&reconnect_interval=0.5")
    try:
        assert connection.transport is not None
        before = connection.transport.connection
        assert type(connection.transport).__name__ == "UnderlayConnection"
        assert isinstance(before, aiormq.Connection)
        assert getattr(before, _UNBLOCKED).is_set()

        reconnected = asyncio.Event()
        connection.reconnect_callbacks.add(lambda *_: reconnected.set())
        server_names: list[str] = []

        async def listed() -> bool:
            # Die Management-API listet neue Verbindungen erst nach dem Statistik-Takt.
            response = await asyncio.to_thread(httpx.get, f"{node.management_url}/connections", auth=AUTH)
            server_names[:] = [c["name"] for c in response.json() if c.get("user_provided_name") == name]
            return bool(server_names)

        assert await wait_until(listed, timeout=20.0, interval=0.5), "connection never listed by the management API"
        response = await asyncio.to_thread(
            httpx.delete, f"{node.management_url}/connections/{quote(server_names[0], safe='')}", auth=AUTH
        )
        assert response.status_code == 204
        await asyncio.wait_for(reconnected.wait(), timeout=20.0)

        assert connection.transport is not None
        after = connection.transport.connection
        assert after is not before
        assert before.is_closed
        assert getattr(after, _UNBLOCKED).is_set()
    finally:
        await connection.close()
