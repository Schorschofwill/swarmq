"""Nachweis TS-06: Die Vorbedingung „aio-pika stellt den Kanal wieder her“ in
``test_channel_callbacks_flat.py`` gilt, bevor die Wiederherstellung fertig ist.

Register: docs/befunde/teststrenge-2026-10.md (TS-06; Gegenprüfung
2026-11-01).

aio-pikas ``_open`` (in swarmq ``underlay.py``) setzt ``channel._channel`` auf
den neuen Unterbau, bevor ``_on_open`` mit ``basic_qos`` fertig ist und das
Ereignis ``__restored`` setzt. ``restored_by_aio_pika`` ist in diesem Fenster
schon wahr; die nächste Runde des Tests schließt dann den neuen Kanal während
des offenen Qos-RPC, aio-pika gibt die Wiederherstellung auf
(``in_restore_state``), und die Vorbedingung scheitert nach 5 s. In der CI 7
rote Integrationsjobs am 2026-10-31/11-01, darunter die Tags v0.16.1rc1 und
v0.16.1.

Erwartet: AGENTS.md, Qualitäts-Gate „Tests sind deterministisch“.

Das verzögerte ``basic_qos`` stellt die Last der CI nach.

Testliste:
# [x] Qos-RPC verzögert -> die Vorbedingung gilt erst, wenn aio-pika fertig ist
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from typing import Any

import aio_pika
import aiormq
import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from tests.integration.broker.test_channel_callbacks_flat import restored_by_aio_pika
from tests.support.queues import delete_queue_family
from tests.support.wait import wait_until

pytestmark = [pytest.mark.integration, pytest.mark.timeout(30)]

_QOS_DELAY_S = 0.05


@pytest.fixture
async def broker(rabbitmq_url: str) -> AsyncIterator[tuple[RabbitMQBroker, str]]:
    queue = f"ts06-{uuid.uuid4().hex[:8]}"
    impl = RabbitMQBroker(rabbitmq_url, queue_type="classic")
    await impl.connect()
    try:
        await impl.declare_queue(queue)
        yield impl, queue
    finally:
        await impl.close()
        await delete_queue_family(rabbitmq_url, queue)


@pytest.mark.befund("TS-06")
@pytest.mark.xfail(strict=True, reason="TS-06: die Vorbedingung gilt vor dem Ende der Wiederherstellung")
async def test_the_restore_precondition_holds_only_once_aio_pika_finished_restoring(
    broker: tuple[RabbitMQBroker, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    impl, queue = broker
    await impl.publish(queue, b"warm-up")
    channel = impl._publish_channel
    assert isinstance(channel, aio_pika.RobustChannel)

    original_qos = aiormq.Channel.basic_qos

    async def delayed_qos(self: aiormq.Channel, **kwargs: Any) -> Any:
        await asyncio.sleep(_QOS_DELAY_S)
        return await original_qos(self, **kwargs)

    monkeypatch.setattr(aiormq.Channel, "basic_qos", delayed_qos)

    underlay = await channel.get_underlay_channel()
    await underlay.basic_ack(delivery_tag=2**31)  # Broker schließt den Kanal
    await asyncio.wait({underlay.closing}, timeout=5)
    assert await wait_until(lambda: restored_by_aio_pika(channel, underlay), timeout=5, interval=0.001)

    restored: asyncio.Event = channel._RobustChannel__restored  # type: ignore[attr-defined]  # aio-pika bietet nur ready() zum Warten
    assert restored.is_set(), "precondition met while aio-pika is still restoring the channel"
