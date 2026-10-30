"""Nachweis FS-08 gegen das echte aiormq 7.2: Publishes in der Warteschlange der Kanal-Sperre enden beim Abriss als "nicht gesendet".

Register: docs/befunde/fehlersuche-runde-2-2026-10.md (FS-08, Quelle N-1).

Befund (aiormq 7.0): ``aiormq.Channel.basic_publish`` serialisiert über die
Kanal-Sperre, die ``is_closed`` nur beim Zugriff prüfte. Wer beim
Verbindungsabriss in ihrer Warteschlange stand, bekam die Sperre danach auf
dem toten Kanal, reihte seine Frames in die ``write_queue`` der alten
Verbindung und wartete auf ein ``drain_future``, das nie endete. swarmq meldete
nach ``publish_timeout_s`` ``PublishIndeterminate``, obwohl nichts gesendet war.

Seit aiormq 7.2 (#240) prüft die Sperre ``is_closed`` auch nach dem Erwerb
(Charakterisierung ohne swarmq:
``tests/integration/broker/test_channel_callbacks_flat.py``,
``test_a_publish_waiting_on_the_channel_lock_ends_at_once_when_the_channel_closes``).
Dieser Test ersetzt den Nachbau des alten Fehlverhaltens
(``tests/unit/broker/test_befund_fs08_publish_lock_waiter.py``, Plan
2026-10-29-1945, KTD5) und zeigt über swarmqs Publish-Pfad gegen echtes
RabbitMQ, dass E-042 gilt: Der Test hält die Sperre des Publish-Kanals wie ein
Publish, der gerade schreibt; zwei Publishes warten in ihrer Warteschlange.
Toxiproxy trennt die Verbindung und bleibt aus, ein Reconnect gelingt nicht.
Beide verlassen den toten Kanal, sobald die Sperre frei ist, und enden als
``BrokerConnectionError``, nicht als ``PublishIndeterminate``; in der Queue
liegt nur die Nachricht vor dem Abriss.

Testliste:
# [x] Abriss, während zwei Publishes auf die Kanal-Sperre warten -> beide
#     verlassen den toten Kanal sofort, beide BrokerConnectionError ohne
#     PublishIndeterminate, nichts gesendet
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid

import aio_pika
import aiormq
import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import BrokerConnectionError, PublishIndeterminate
from tests.e2e.combinations._toxiproxy import ToxiproxyClient
from tests.integration.broker.conftest import PROXY, PROXY_PORT
from tests.support.queues import delete_queue_family
from tests.support.services import rabbitmq_amqp_url
from tests.support.wait import wait_until

# exclusive: Der Test schaltet den gemeinsamen Proxy ``rabbitmq`` ab. Die
# xdist-Gruppe ``toxiproxy`` (tests/support/xdist_isolation.py) trennt das unter
# pytest 9.2 und pytest-xdist 3.8 nicht: xdist hängt die Gruppe über
# ``item._nodeid`` an, pytest liest die Node-ID inzwischen aus ``_id``; B24 lief
# parallel auf einem anderen Prozess und verlor seine Verbindung.
pytestmark = [pytest.mark.integration, pytest.mark.timeout(60), pytest.mark.exclusive]

# Ohne erreichbaren Broker endet jeder Publish an dieser Frist; vorher, auf
# aiormq 7.0, hing der Wartende bis hierhin auf dem toten Kanal.
_PUBLISH_TIMEOUT_S = 3.0


async def _depth(queue: str) -> int:
    """Nachrichten in *queue*, direkt am Test-Broker gelesen (am Proxy vorbei)."""
    connection = await aio_pika.connect(rabbitmq_amqp_url())
    try:
        channel = await connection.channel()
        declared = await channel.declare_queue(queue, passive=True)
        assert declared.declaration_result.message_count is not None
        return declared.declaration_result.message_count
    finally:
        await connection.close()


@pytest.mark.befund("FS-08")
async def test_fs08_publishes_waiting_on_the_channel_lock_end_unsent_when_the_connection_is_cut(
    toxiproxy: ToxiproxyClient,
) -> None:
    queue = f"fs08_{uuid.uuid4().hex[:8]}"
    broker = RabbitMQBroker(rabbitmq_amqp_url("127.0.0.1", PROXY_PORT), queue_type="classic")
    broker.publish_timeout_s = _PUBLISH_TIMEOUT_S
    await broker.connect()
    try:
        await broker.declare_queue(queue)
        await broker.publish(queue, b'{"fs08": "before"}')  # öffnet den Publish-Kanal
        assert broker._publish_channel is not None
        underlay = await broker._publish_channel.get_underlay_channel()
        assert isinstance(underlay, aiormq.Channel)
        lock: asyncio.Lock = underlay._Channel__lock  # type: ignore[attr-defined]  # die Sperre hinter der Property ``lock``

        async with underlay.lock:  # wie ein Publish, der gerade schreibt
            waiting = [asyncio.create_task(broker.publish(queue, f'{{"fs08": {n}}}'.encode())) for n in range(2)]
            # asyncio.Lock bietet keine API für seine Warteschlange.
            assert await wait_until(lambda: len(lock._waiters or ()) == 2, timeout=5, interval=0.01), (
                "precondition: both publishes wait on the channel lock"
            )
            await toxiproxy.set_proxy_enabled(PROXY, False)
            assert await wait_until(lambda: underlay.is_closed, timeout=10, interval=0.01), (
                "precondition: the cut closes the publish channel"
            )

        assert await wait_until(lambda: not lock._waiters, timeout=1, interval=0.01), (
            "the waiting publishes stay in the lock queue of the dead channel"
        )
        outcomes = await asyncio.gather(*waiting, return_exceptions=True)
        for outcome in outcomes:
            assert isinstance(outcome, BrokerConnectionError), f"{type(outcome).__name__}: {outcome}"
            assert not isinstance(outcome, PublishIndeterminate), f"nothing was sent: {outcome}"

        await toxiproxy.set_proxy_enabled(PROXY, True)
        assert await _depth(queue) == 1, "only the publish before the cut reached the broker"
    finally:
        await toxiproxy.set_proxy_enabled(PROXY, True)
        with contextlib.suppress(Exception):
            await broker.close()
        await delete_queue_family(rabbitmq_amqp_url(), queue)
