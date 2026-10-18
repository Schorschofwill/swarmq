"""Kanalverwurf nach einem Fristriss nimmt die übrigen offenen Confirms mit (E-078).

Reißt die Publish-Frist eines schon gesendeten Publish, verwirft swarmq den
Publish-Kanal (R10, ``_discard_publish_channel``). Das Schließen des Kanals
lehnt in aiormq alle offenen Confirm-Futures ab; jeder andere Publish, der
auf diesem Kanal gesendet hat und noch auf sein Confirm wartet, endet als
``PublishIndeterminate`` (E-041, E-042), auch wenn seine eigene Frist noch
läuft. Der nächste Publish öffnet einen neuen Kanal.

Beleg: Loadsuite-Abnahme 0.13.0rc1, ``broker_alarm``: eine Retry-Kopie riss
bei der Aufhebung des Alarms ihre Frist, mit ihr endeten 12 weitere Kopien
auf demselben Kanal als unbestimmt; kein Duplikat ausgeführt.

Charakterisierung: Der Test schreibt bestehendes Verhalten fest
(``delivery.py`` seit 0.12.4 unverändert) und war darum beim ersten Lauf
grün; ein roter Schritt entfällt. Der Publish läuft durch den echten
aio-pika/aiormq-Code (``tests/support/aiormq_transport.py``).

Testliste:
# [x] Frist des ersten Publish reißt nach dem Senden: der zweite, gesendete
#     Publish auf demselben Kanal endet als PublishIndeterminate, der dritte
#     läuft auf einem neuen Kanal
"""

from __future__ import annotations

import asyncio
from typing import override
from unittest.mock import AsyncMock, Mock

import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import PublishIndeterminate
from tests.support.aiormq_transport import FakePublishChannel, FakeTransport

pytestmark = [pytest.mark.timeout(5)]


class _ClosingPublishChannel(FakePublishChannel):
    """Wie aio-pika/aiormq: ``close()`` lehnt die offenen Futures des Kanals
    mit ``CancelledError`` ab und schließt ``closing``."""

    @override
    async def close(self) -> None:
        await super().close()
        self.transport.lose_connection(asyncio.CancelledError())


def _broker(*transports: FakeTransport) -> RabbitMQBroker:
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    broker._connection = Mock(is_closed=False, transport=None)
    broker._connection.ready = AsyncMock()
    channels = [_ClosingPublishChannel(transport) for transport in transports]
    broker._publish_channel_factory = AsyncMock(side_effect=channels)
    return broker


@pytest.mark.entscheidung("E-078")
async def test_a_deadline_breach_discards_the_channel_and_the_other_open_confirms_end_indeterminate() -> None:
    first, second = FakeTransport(closing=True), FakeTransport(closing=True)
    broker = _broker(first, second)
    broker.publish_timeout_s = 60.0

    breaching = asyncio.create_task(broker.publish("retry-a", b"{}"))
    (await first.next_frame()).drain()
    [breaching_deadline] = broker._active_publish_timeouts
    bystander = asyncio.create_task(broker.publish("retry-b", b"{}"))
    (await first.next_frame()).drain()
    # Beide sind gesendet und warten auf ihr Confirm; nur die erste Frist reißt.
    breaching_deadline.reschedule(asyncio.get_running_loop().time())

    with pytest.raises(PublishIndeterminate, match="exceeded the publish deadline"):
        await breaching
    with pytest.raises(PublishIndeterminate, match="lost the connection while waiting for the broker's confirm"):
        await bystander

    following = asyncio.create_task(broker.publish("retry-c", b"{}"))
    (await second.next_frame()).drain()
    second.confirm_all()
    receipt = await following

    assert receipt.queue == "retry-c"
    assert broker._publish_channel_factory.await_count == 2  # type: ignore[union-attr]  # AsyncMock der Test-Naht
