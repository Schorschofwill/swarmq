"""Blockierte Publish-Verbindung (Plan 0.12.0, MR 16, BZ-16; Spike S6).

Den Blocked-Zustand liest ein Adapter synchron ueber das private
``aiormq.Connection.__connection_unblocked`` (``asyncio.Event``); der Pfad
``connection.transport.connection`` wird bei jedem Lesen neu gegangen, weil
ein robuster Reconnect ein neues Objekt bringt.

Test-Liste:
- [x] unblockiert: nie ``BrokerPublishBlocked``, kein Warten, kein Roundtrip (Pflichttest)
- [x] blockiert bis zur Publish-Frist: ``BrokerPublishBlocked`` (eine ``BrokerConnectionError``),
      nichts gesendet, Publish-Kanal bleibt, WARNING
- [x] blockiert, dann vor der Frist frei: der Publish geht durch
- [x] Shutdown-Endfrist zieht das Warten nach (``set_publish_deadline``)
- [x] Zustand nicht lesbar (Attribut fehlt): fail-open, eine WARNING je Broker
- [x] keine Transport-Verbindung (Reconnect laeuft): kein Warten, keine WARNING
- [x] WARNING zum Blockieren einmal je Episode
- [x] eigener Abbruch im Blocked-Warten: ``PublishCancelledUnsent`` (ein
      ``CancelledError``), ``task.cancelled()``, nichts gesendet (U4, E-041)
- [x] aiormq 7.2: der beim Warten gelesene ``closing``-Beobachter bleibt nicht
      an ``_closing`` der Verbindung haengen, weder nach ``unblocked`` noch
      nach der Frist (KTD2, Plan 2026-10-29-1945)
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import cast, override
from unittest.mock import AsyncMock, Mock

import aiormq.base
import pytest
from aiormq.abc import ExceptionType

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import BrokerConnectionError, BrokerPublishBlocked, PublishCancelledUnsent

pytestmark = [pytest.mark.timeout(5)]

_ATTR = "_Connection__connection_unblocked"


def _broker(underlying: object, *, timeout: float = 5.0) -> RabbitMQBroker:
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    broker.publish_timeout_s = timeout
    broker._connection = Mock(transport=SimpleNamespace(connection=underlying), is_closed=False)
    broker._connection.ready = AsyncMock()
    broker._publish_channel = AsyncMock(is_closed=False)
    return broker


class _ObservedEvent(asyncio.Event):
    """Ein Unblocked-Event, das meldet, sobald jemand darauf wartet."""

    def __init__(self) -> None:
        super().__init__()
        self.waiting = asyncio.Event()

    @override
    async def wait(self) -> bool:  # type: ignore[override]
        self.waiting.set()
        return await super().wait()


def _underlying(blocked: bool) -> SimpleNamespace:
    event = _ObservedEvent()
    if not blocked:
        event.set()
    return SimpleNamespace(**{_ATTR: event})


def _sent(broker: RabbitMQBroker) -> list[str]:
    publish = cast("AsyncMock", broker._publish_channel).default_exchange.publish
    return [call.kwargs["routing_key"] for call in publish.await_args_list]


@pytest.mark.entscheidung("E-025")
async def test_unblocked_connection_publishes_without_waiting_or_roundtrip() -> None:
    event = Mock(spec=asyncio.Event)
    event.is_set.return_value = True
    event.wait = AsyncMock()
    broker = _broker(SimpleNamespace(**{_ATTR: event}))

    await broker.publish("tasks", b"{}")

    assert _sent(broker) == ["tasks"]
    event.wait.assert_not_awaited()
    broker._connection.ready.assert_not_awaited()  # type: ignore[union-attr]


@pytest.mark.entscheidung("E-025")
async def test_blocked_until_the_deadline_raises_publish_blocked(swarmq_logs) -> None:
    broker = _broker(_underlying(blocked=True), timeout=0.05)
    channel = broker._publish_channel

    with pytest.raises(BrokerPublishBlocked) as info:
        await broker.publish("tasks", b"{}")

    assert isinstance(info.value, BrokerConnectionError)
    assert _sent(broker) == []
    assert broker._publish_channel is channel
    assert [e["event"] for e in swarmq_logs if e["log_level"] == "warning"] == ["publish_connection_blocked"]


async def test_blocked_then_unblocked_before_the_deadline_publishes() -> None:
    underlying = _underlying(blocked=True)
    broker = _broker(underlying)

    event = getattr(underlying, _ATTR)
    publish = asyncio.create_task(broker.publish("tasks", b"{}"))
    await event.waiting.wait()
    assert _sent(broker) == []
    event.set()
    await publish

    assert _sent(broker) == ["tasks"]


async def test_shutdown_deadline_cuts_the_wait_short() -> None:
    underlying = _underlying(blocked=True)
    broker = _broker(underlying, timeout=60.0)

    publish = asyncio.create_task(broker.publish("tasks", b"{}"))
    await getattr(underlying, _ATTR).waiting.wait()
    broker.set_publish_deadline(asyncio.get_running_loop().time())

    with pytest.raises(BrokerPublishBlocked):
        await publish


async def test_no_transport_connection_publishes_without_warning(swarmq_logs) -> None:
    broker = _broker(None)

    await broker.publish("tasks", b"{}")

    assert _sent(broker) == ["tasks"]
    assert [e for e in swarmq_logs if e["log_level"] == "warning"] == []


@pytest.mark.entscheidung("E-025")
async def test_unreadable_state_fails_open_with_one_warning_per_broker(swarmq_logs) -> None:
    # aiormq ohne das private Event: der Publish laeuft wie vor 0.12.
    broker = _broker(SimpleNamespace())

    await broker.publish("tasks", b"{}")
    await broker.publish("tasks", b"{}")

    assert _sent(broker) == ["tasks", "tasks"]
    warnings = [e["event"] for e in swarmq_logs if e["log_level"] == "warning"]
    assert warnings == ["publish_blocked_state_unreadable"]


@pytest.mark.entscheidung("E-041")
async def test_own_cancel_while_blocked_is_marked_as_unsent() -> None:
    """Plan 0.14, U4: Bricht der Aufrufer im Blocked-Warten ab, ist
    nachweislich nichts gesendet. Der Abbruch geht als
    ``PublishCancelledUnsent`` weiter, bleibt aber ein Abbruch."""
    underlying = _underlying(blocked=True)
    broker = _broker(underlying)
    publish = asyncio.create_task(broker.publish("tasks", b"{}"))
    await getattr(underlying, _ATTR).waiting.wait()

    publish.cancel()
    with pytest.raises(asyncio.CancelledError) as caught:
        await publish

    assert isinstance(caught.value, PublishCancelledUnsent)
    assert publish.cancelled()
    assert _sent(broker) == []


class _AiormqConnection(aiormq.base.Base):
    """Blockierte aiormq-Verbindung mit dem echten ``closing`` aus aiormq 7.2:
    je Zugriff ein neuer Beobachter mit einem Callback an ``_closing``."""

    def __init__(self) -> None:
        super().__init__(loop=asyncio.get_running_loop())
        setattr(self, _ATTR, _ObservedEvent())

    @override
    async def _on_close(self, exc: ExceptionType | None = None) -> None:
        return None

    @override
    def __str__(self) -> str:
        return "connection"


def _callbacks(future: asyncio.Future[object]) -> int:
    return len(future._callbacks or ())  # privat: asyncio bietet keine öffentliche Zählung


async def test_the_closing_observer_of_the_wait_is_released_once_unblocked() -> None:
    underlying = _AiormqConnection()
    before = _callbacks(underlying._closing)
    broker = _broker(underlying)
    event = getattr(underlying, _ATTR)

    publish = asyncio.create_task(broker.publish("tasks", b"{}"))
    await event.waiting.wait()
    event.set()
    await publish
    await asyncio.sleep(0)  # Done-Callbacks laufen erst im nächsten Schritt der Loop

    assert _sent(broker) == ["tasks"]
    assert _callbacks(underlying._closing) == before


async def test_the_closing_observer_of_the_wait_is_released_at_the_deadline() -> None:
    underlying = _AiormqConnection()
    before = _callbacks(underlying._closing)
    broker = _broker(underlying, timeout=0.05)

    with pytest.raises(BrokerPublishBlocked):
        await broker.publish("tasks", b"{}")
    await asyncio.sleep(0)

    assert _callbacks(underlying._closing) == before
