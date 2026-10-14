"""Spike S8 (MR 8, 10): 406/403/405-Texte, die keine Argumentkonflikte sind.

Beobachtet auf 4.3.6 und 4.2.9 (identisch):
- Exklusive Queue einer anderen Verbindung: 405 ``ChannelLockedResource``
  ``RESOURCE_LOCKED - cannot obtain exclusive access to locked queue ...``,
  auch beim passiven Declare.
- ``max-queues`` erreicht: 406 ``ChannelPreconditionFailed``
  ``PRECONDITION_FAILED - cannot declare queue '<q>': queue limit in vhost
  '<v>' (<n>) is reached`` -- ein 406 ohne "inequivalent arg".
- Fehlendes configure-Recht: 403 ``ChannelAccessRefused`` ``ACCESS_REFUSED -
  configure access to queue '<q>' in vhost '<v>' refused for user 'guest'``.

Limit und Rechte entstehen in einem eigenen Wegwerf-vhost, den der Test im
``finally`` löscht; der vhost ``/`` bleibt unberührt.

Test-Liste:
- [x] exklusive Queue (aktiv und passiv)
- [x] max-queues
- [x] ACCESS_REFUSED
"""

from __future__ import annotations

import dataclasses
import uuid
from collections.abc import AsyncIterator, Callable

import aio_pika
import httpx
import pytest
from aio_pika.abc import AbstractConnection

from tests.integration.spikes._broker import AUTH, QUORUM, declare, reply_text
from tests.support.services import RabbitMQNode

pytestmark = [pytest.mark.integration, pytest.mark.spike]


async def test_exclusive_queue_of_another_connection(
    node: RabbitMQNode, amqp: AbstractConnection, queue_name: Callable[[str], str]
) -> None:
    queue = queue_name("s8-excl")
    owner = await amqp.channel()
    await owner.declare_queue(queue, exclusive=True)
    other = await aio_pika.connect(node.amqp_url)
    try:
        active = await declare(other, queue, {})
        channel = await other.channel()
        try:
            await channel.declare_queue(queue, passive=True)
            passive = None
        except Exception as exc:
            passive = f"{type(exc).__name__}: {exc.args[0]}"
    finally:
        await other.close()

    expected = "ChannelLockedResource: " + reply_text(
        f"RESOURCE_LOCKED - cannot obtain exclusive access to locked queue '{queue}' "
        f"in vhost '{node.vhost}'. It could be originally declared on another connection or the exclusive property value "
        "does not match that of the original declaration."
    )
    assert (active, passive) == (expected, expected)


@pytest.fixture
async def limited_vhost(node: RabbitMQNode) -> AsyncIterator[str]:
    """Wegwerf-vhost mit ``max-queues`` 1; configure nur für ``allowed*``."""
    vhost = f"swarmq-spike-{uuid.uuid4().hex[:8]}"
    async with httpx.AsyncClient(base_url=node.management_url, auth=AUTH, timeout=10.0) as client:
        (await client.put(f"/vhosts/{vhost}")).raise_for_status()
        try:
            permissions = {"configure": "^allowed", "write": ".*", "read": ".*"}
            (await client.put(f"/permissions/{vhost}/guest", json=permissions)).raise_for_status()
            (await client.put(f"/vhost-limits/{vhost}/max-queues", json={"value": 1})).raise_for_status()
            yield vhost
        finally:
            await client.delete(f"/vhosts/{vhost}")


async def test_queue_limit_and_access_refused(node: RabbitMQNode, limited_vhost: str) -> None:
    connection = await aio_pika.connect(dataclasses.replace(node, vhost=limited_vhost).amqp_url)
    try:
        first = await declare(connection, "allowed-1", QUORUM)
        over_limit = await declare(connection, "allowed-2", QUORUM)
        refused = await declare(connection, "denied-1", QUORUM)
    finally:
        await connection.close()

    assert first is None
    assert over_limit == (
        "ChannelPreconditionFailed: PRECONDITION_FAILED - cannot declare queue 'allowed-2': "
        f"queue limit in vhost '{limited_vhost}' (1) is reached"
    )
    assert refused == (
        "ChannelAccessRefused: ACCESS_REFUSED - configure access to queue 'denied-1' "
        f"in vhost '{limited_vhost}' refused for user 'guest'"
    )
