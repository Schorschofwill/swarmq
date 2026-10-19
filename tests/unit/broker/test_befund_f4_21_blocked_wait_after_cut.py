"""Nachweis F4-21 (behoben in 0.14.0): ``_wait_while_blocked`` wartet nach einem Abriss im Alarm bis zur Frist.

Register: docs/befunde/fehlersuche-runde-4-2026-10.md (F4-21; Nebenbeobachtung 2
im Spike ``docs/solutions/spikes/2026-10-spike-kanal-close-nach-unblocked.md``,
Gegenprüfung 0.14, Probe B).

Der Publish wartet im Ressourcenalarm nur auf das Unblocked-Event der
aiormq-Verbindung, die beim Lesen aktuell war; deren ``closing`` beachtet er
nicht, und aiormq setzt das Event beim Schließen nicht. Reißt die Verbindung im
Alarm, wartet er bis zur Publish-Frist und endet als ``BrokerPublishBlocked``,
obwohl nach dem Reconnect gesendet werden könnte. Verletzt: Docstring von
``BrokerPublishBlocked`` und ``errors.md`` („does not unblock it within …“),
E-042 („Riss die Verbindung, bevor der Frame geschrieben war, wiederholt swarmq
auf einem neuen Kanal“).

Fake-Transport wie in ``test_publish_blocked.py``: ``connection.transport.connection``
trägt das Unblocked-Event und ``closing``; ein Reconnect tauscht das Objekt.

Test-Liste:
# [x] Abriss im Warten, danach neue, unblockierte Verbindung: der Publish geht sofort durch
# [x] Abriss im Warten, der Transport zeigt noch auf die tote Verbindung: gilt wie
#     "nicht verbunden", weiter in den Publish-Pfad (der auf den Reconnect wartet)
# [x] Abriss im Warten, die neue Verbindung ist wieder blockiert: weiter warten, bis
#     zur Frist ``BrokerPublishBlocked``, eine WARNING
# [x] eigener Abbruch im Warten mit lesbarem ``closing``: ``PublishCancelledUnsent``,
#     nichts gesendet, kein Hilfs-Task bleibt zurück
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import cast, override
from unittest.mock import AsyncMock, Mock

import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import BrokerPublishBlocked, PublishCancelledUnsent

pytestmark = [pytest.mark.timeout(5)]

_ATTR = "_Connection__connection_unblocked"
# Weit über dem, was der Test abwartet: Ohne den Fix endet der Publish erst hier.
_PUBLISH_TIMEOUT_S = 3.0
_SENT_WITHIN_S = 1.0


class _ObservedEvent(asyncio.Event):
    """Ein Unblocked-Event, das meldet, sobald jemand darauf wartet."""

    def __init__(self) -> None:
        super().__init__()
        self.waiting = asyncio.Event()

    @override
    async def wait(self) -> bool:  # type: ignore[override]
        self.waiting.set()
        return await super().wait()


def _aiormq(*, blocked: bool) -> SimpleNamespace:
    event = _ObservedEvent()
    if not blocked:
        event.set()
    closing: asyncio.Future[object] = asyncio.get_running_loop().create_future()
    return SimpleNamespace(**{_ATTR: event}, closing=closing)


def _broker(aiormq_connection: SimpleNamespace) -> RabbitMQBroker:
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    broker.publish_timeout_s = _PUBLISH_TIMEOUT_S
    broker._connection = Mock(transport=SimpleNamespace(connection=aiormq_connection), is_closed=False)
    broker._connection.ready = AsyncMock()
    broker._publish_channel = AsyncMock(is_closed=False)
    return broker


def _sent(broker: RabbitMQBroker) -> list[str]:
    publish = cast("AsyncMock", broker._publish_channel).default_exchange.publish
    return [call.kwargs["routing_key"] for call in publish.await_args_list]


@pytest.mark.befund("F4-21")
@pytest.mark.entscheidung("E-025")
async def test_a_cut_while_blocked_publishes_on_the_new_unblocked_connection() -> None:
    dropped = _aiormq(blocked=True)
    broker = _broker(dropped)
    publish = asyncio.create_task(broker.publish("tasks", b"{}"))
    try:
        await getattr(dropped, _ATTR).waiting.wait()

        # Abriss im Alarm: aiormq erledigt closing, das Event bleibt ungesetzt;
        # aio-pika verbindet neu, die neue Verbindung ist nicht blockiert.
        dropped.closing.set_result(None)
        broker._connection.transport = SimpleNamespace(connection=_aiormq(blocked=False))  # type: ignore[union-attr]

        done, _ = await asyncio.wait({publish}, timeout=_SENT_WITHIN_S)
        assert publish in done, f"the publish is sent after the reconnect, not at the deadline of {_PUBLISH_TIMEOUT_S}s"
        await publish
        assert _sent(broker) == ["tasks"]
    finally:
        publish.cancel()
        await asyncio.gather(publish, return_exceptions=True)


async def test_a_cut_while_blocked_with_the_dead_connection_still_current_goes_on_to_publish() -> None:
    dropped = _aiormq(blocked=True)
    broker = _broker(dropped)
    publish = asyncio.create_task(broker.publish("tasks", b"{}"))
    await getattr(dropped, _ATTR).waiting.wait()

    # Bis aio-pika neu verbindet, bleibt die tote aiormq-Verbindung der Transport.
    dropped.closing.set_result(None)
    await publish

    assert _sent(broker) == ["tasks"]


@pytest.mark.entscheidung("E-025")
async def test_a_cut_while_blocked_onto_a_blocked_connection_waits_until_the_deadline(swarmq_logs) -> None:
    dropped = _aiormq(blocked=True)
    broker = _broker(dropped)
    # Lange Frist, die der Test erst reißt, wenn der Publish auf der neuen
    # Verbindung wartet: kein Rennen gegen die Uhr.
    broker.publish_timeout_s = 60.0
    publish = asyncio.create_task(broker.publish("tasks", b"{}"))
    await getattr(dropped, _ATTR).waiting.wait()

    fresh = _aiormq(blocked=True)
    broker._connection.transport = SimpleNamespace(connection=fresh)  # type: ignore[union-attr]
    dropped.closing.set_result(None)
    waiting = asyncio.create_task(getattr(fresh, _ATTR).waiting.wait())
    done, _ = await asyncio.wait({publish, waiting}, timeout=_SENT_WITHIN_S, return_when=asyncio.FIRST_COMPLETED)
    waiting.cancel()
    assert publish not in done, "the publish keeps waiting on the new blocked connection"
    assert waiting in done, "after the cut the publish waits on the new connection, not on the dead one"
    [deadline] = broker._active_publish_timeouts
    deadline.reschedule(asyncio.get_running_loop().time())

    with pytest.raises(BrokerPublishBlocked):
        await publish
    assert _sent(broker) == []
    assert [e["event"] for e in swarmq_logs if e["log_level"] == "warning"] == ["publish_connection_blocked"]


async def test_own_cancel_while_blocked_with_a_readable_closing_is_unsent_and_leaves_no_task() -> None:
    blocked = _aiormq(blocked=True)
    broker = _broker(blocked)
    publish = asyncio.create_task(broker.publish("tasks", b"{}"))
    await getattr(blocked, _ATTR).waiting.wait()

    publish.cancel()
    with pytest.raises(asyncio.CancelledError) as caught:
        await publish

    assert isinstance(caught.value, PublishCancelledUnsent)
    assert _sent(broker) == []
    leftovers = asyncio.all_tasks() - {asyncio.current_task()}
    if leftovers:
        await asyncio.wait(leftovers, timeout=1.0)
    assert all(task.done() for task in leftovers), "the helper task waiting for unblocked ends with the publish"
