"""Nachweis F4-30 (behoben, ``channel_close.close_without_abort``): Reißt die Frist das Schließen eines verworfenen Kanals ab, nachdem der Broker es schon beantwortet hat, beendet RabbitMQ die ganze Verbindung.

Register: docs/befunde/fehlersuche-runde-4-2026-10.md (F4-30), Signal S-010
im Sättigungsbuch; Plan docs/plans/2026-10-29-1945-chore-aiormq-7-2-anheben-plan.md
(U1, KTD1).

``_close_channel_detached`` schließt den verworfenen Publish-Kanal unter
``asyncio.timeout(_CHANNEL_CLOSE_TIMEOUT_S)``. Läuft die Frist ab, während
aiormqs ``Channel.Close``-RPC noch wartet, schickt aiormq im Abbruchzweig von
``Channel.rpc`` ein zweites ``Channel.Close(504)`` auf dieselbe Kanalnummer.
Hat der Broker das erste Close schon beantwortet, ist die Nummer bei ihm frei;
der zweite Frame gilt als neuer Kanal ohne ``channel.open``, und RabbitMQ
schließt die Verbindung mit ``CHANNEL_ERROR - expected 'channel.open'``. Das
trifft alle Publishes auf dieser Verbindung und, auf dem injizierten Pfad,
die Verbindung des Aufrufers. Verletzt: E-078 (der Verwurf bereinigt nur den
einen Kanal, „kein Leck“, keine Folgen für die Verbindung).

Deterministisch über die Client-Seite: Der Test hält aiormqs Verarbeitung
des eingehenden ``Channel.CloseOk`` für diesen Kanal an, bis die verkürzte
Frist das Close abgebrochen hat, und gibt sie dann frei. Ein Speicheralarm
taugt dafür nicht: Dort liegen beide Closes im Puffer, und es entsteht nur
die harmlose Zeile „Got frame for closed channel“ (Nachtrag E-025).

Test-Liste:
# [x] Verwurf, Frist reißt nach dem CloseOk des Brokers: die Publish-Verbindung bleibt offen
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

import aiormq
import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.broker.rabbitmq import connection as connection_module
from tests.support.known_leaks import aiormq_streamwriter_leak
from tests.support.queues import delete_queue_family
from tests.support.wait import wait_until

# exclusive: Die Close-Frist ist auf 0,5 s verkürzt.
pytestmark = [pytest.mark.integration, pytest.mark.timeout(60), pytest.mark.exclusive]

_CLOSE_DEADLINE_S = 0.5


@aiormq_streamwriter_leak  # TS-02: schließt der Broker die Verbindung, lässt aiormq den Writer offen
@pytest.mark.befund("F4-30")
async def test_a_discard_whose_close_misses_the_deadline_after_the_broker_answered_keeps_the_connection(
    rabbitmq_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    queue = f"f4-30-{uuid.uuid4().hex[:10]}"
    # aiormq bindet seine Frame-Handler beim Start des Kanal-Readers; der
    # Patch muss also vor dem Öffnen des Publish-Kanals stehen.
    original = aiormq.Channel._on_close_ok_frame
    target: list[object] = []
    close_ok_arrived = asyncio.Event()
    release = asyncio.Event()

    async def held_close_ok(self: Any, frame: Any) -> None:
        if target and self is target[0]:
            close_ok_arrived.set()
            await release.wait()
        await original(self, frame)

    monkeypatch.setattr(aiormq.Channel, "_on_close_ok_frame", held_close_ok)
    broker = RabbitMQBroker(rabbitmq_url, queue_type="classic")
    await broker.connect()
    try:
        await broker.declare_queue(queue)
        await broker.publish(queue, b"m0")
        channel = broker._publish_channel
        assert channel is not None
        target.append(await channel.get_underlay_channel())
        underlay = broker._connection.transport.connection  # type: ignore[union-attr]
        monkeypatch.setattr(connection_module, "_CHANNEL_CLOSE_TIMEOUT_S", _CLOSE_DEADLINE_S)

        await broker._discard_publish_channel(channel)
        assert await wait_until(close_ok_arrived.is_set, timeout=10, interval=0.01), (
            "precondition: the broker answered the first Channel.Close"
        )
        assert await wait_until(lambda: not broker._pending_channel_closes, timeout=10, interval=0.01), (
            "precondition: the deadline ended the detached close"
        )
        release.set()
        await asyncio.sleep(1.0)

        assert not underlay.is_closed, (
            f"the publish connection survives the aborted close, closed with {underlay.closing.exception()!r}"
            if underlay.closing.done()
            else "the publish connection survives the aborted close"
        )
    finally:
        await broker.close()
        await delete_queue_family(rabbitmq_url, queue)
