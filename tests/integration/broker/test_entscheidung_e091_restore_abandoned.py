"""E-091 (PF-22): Gibt aio-pika die Wiederherstellung des Publish-Kanals auf,
heilt swarmq erst nach der Bereitschaftsfrist, ohne Fehler und ohne Verlust.

Register: compound-packs/swarmq-entscheidungen/e091-aufgegebene-kanal-wiederherstellung-heilt-nach-frist.md;
docs/befunde/fehlersuche-runde-4-2026-10.md (PF-22, Signal S-012).

Schließt der Broker den Publish-Kanal ein zweites Mal, während aio-pika ihn
noch wiederherstellt (offenes ``basic_qos`` in ``_on_open``), gibt aio-pika auf
(``in_restore_state``). Der nächste Publish wartet in ``_restored_in_time`` die
Frist ``_CHANNEL_READY_TIMEOUT_S`` ab und ersetzt dann den Kanal. Die Frist ist
hier auf 1 s gekürzt; das verzögerte ``basic_qos`` öffnet das Fenster.

Testliste:
# [x] zweites Schließen im Qos-Fenster -> der nächste Publish gelingt nach der
#     Frist auf neuem Kanal, die Nachricht kommt an
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import AsyncIterator
from typing import Any

import aio_pika
import aiormq
import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.broker.rabbitmq import connection as connection_module
from tests.support.queues import delete_queue_family
from tests.support.wait import wait_until

pytestmark = [pytest.mark.integration, pytest.mark.timeout(30)]

_QOS_DELAY_S = 0.2
_READY_TIMEOUT_S = 1.0


@pytest.fixture
async def broker(rabbitmq_url: str) -> AsyncIterator[tuple[RabbitMQBroker, str]]:
    queue = f"e091-{uuid.uuid4().hex[:8]}"
    impl = RabbitMQBroker(rabbitmq_url, queue_type="classic")
    await impl.connect()
    try:
        await impl.declare_queue(queue)
        yield impl, queue
    finally:
        await impl.close()
        await delete_queue_family(rabbitmq_url, queue)


async def _close_by_broker(underlay: aiormq.abc.AbstractChannel) -> None:
    await underlay.basic_ack(delivery_tag=2**31)  # Ack auf ein nie vergebenes Tag
    done, _ = await asyncio.wait({underlay.closing}, timeout=5)
    assert done, "precondition: the broker closed the channel"


@pytest.mark.entscheidung("E-091")
async def test_an_abandoned_restore_heals_after_the_ready_timeout_without_loss(
    broker: tuple[RabbitMQBroker, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    impl, queue = broker
    await impl.publish(queue, b"warm-up")
    channel = impl._publish_channel
    assert isinstance(channel, aio_pika.RobustChannel)

    monkeypatch.setattr(connection_module, "_CHANNEL_READY_TIMEOUT_S", _READY_TIMEOUT_S)
    original_qos = aiormq.Channel.basic_qos

    async def delayed_qos(self: aiormq.Channel, **kwargs: Any) -> Any:
        await asyncio.sleep(_QOS_DELAY_S)
        return await original_qos(self, **kwargs)

    monkeypatch.setattr(aiormq.Channel, "basic_qos", delayed_qos)

    first = await channel.get_underlay_channel()
    await _close_by_broker(first)
    # aio-pika hat den neuen Unterbau eingesetzt, wartet aber noch auf Qos.
    assert await wait_until(
        lambda: channel._channel is not None and channel._channel.channel is not first, timeout=5, interval=0.001
    ), "precondition: aio-pika opened a new underlay"
    second = channel._channel.channel  # type: ignore[union-attr]  # oben geprüft
    await _close_by_broker(second)
    # aio-pika gibt auf: kein Unterbau mehr, Wiederherstellung nicht fertig. Dass
    # keine mehr nachkommt, belegen die Assertionen nach dem Publish.
    restored: asyncio.Event = channel._RobustChannel__restored  # type: ignore[attr-defined]  # aio-pika bietet nur ready() zum Warten
    assert await wait_until(lambda: channel._channel is None and not restored.is_set(), timeout=5, interval=0.001), (
        "precondition: aio-pika dropped the half-restored underlay"
    )

    started = time.monotonic()
    await impl.publish(queue, b"after")
    waited = time.monotonic() - started

    assert waited >= _READY_TIMEOUT_S, f"healed after {waited:.2f} s, before the ready timeout"
    assert impl._publish_channel is not channel, "swarmq replaced the abandoned channel"
    assert await impl.queue_depth(queue) == 2
