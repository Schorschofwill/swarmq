"""Publish in einer vom Drain abgebrochenen Handler-Task (Review U3, KTD9
Teil Publish; fix-r-flow.md 2.2).

Nach E-001 laeuft ein vom Drain abgebrochener Handler weiter, sein
``cancelling()`` bleibt dabei > 0. Wie in ``settle()`` heisst "eigener
Abbruch" im Publish-Pfad nur ein Anstieg gegenueber dem Stand beim Eintritt.
Eine aio-pika-interne ``CancelledError`` (Kanalverlust) beim Retry-, DLQ- oder
Advance-Publish eines solchen Handlers ist "nicht gesendet"
(``BrokerConnectionError``) -- sonst saehe das ``finally`` der Pipeline einen
terminalen Ausgang und schriebe den Completed-Marker auf eine unbestaetigte
Nachricht.

Testliste:
- [x] cancelling() == 1 ohne Endfrist, jede Publish-Runde wirft interne
      CancelledError -> BrokerConnectionError statt CancelledError
- [x] ebenso dead_letter: BrokerConnectionError, Original unbestaetigt
- [x] einmalige interne CancelledError: der naechste Versuch geht raus
- [x] zweiter, echter Abbruch waehrend des Publish -> CancelledError geht durch
- [x] haengender Publish ueber die Publish-Frist -> PublishIndeterminate
      (die TimeoutError-Umwandlung bleibt trotz cancelling() == 1 erhalten)
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.contracts import Delivery
from swarmq.exceptions import BrokerConnectionError, PublishIndeterminate

pytestmark = pytest.mark.timeout(5)


@pytest.fixture(autouse=True)
def _no_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    # Die Wiederholungen warten 0.05 * 2**n s; im Unit-Test ohne Wartezeit.
    original = asyncio.sleep

    async def _instant(_delay: float, *args: object) -> None:
        await original(0)

    monkeypatch.setattr(asyncio, "sleep", _instant)


def _broker(publish: AsyncMock) -> RabbitMQBroker:
    """Broker ohne Publish-Endfrist (etwa Crash-Teardown), dessen Kanal
    *publish* als ``default_exchange.publish`` hat."""
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    broker.publish_timeout_s = 60.0
    broker._connection = Mock(transport=None, is_closed=False)
    broker._connection.ready = AsyncMock()
    channel = AsyncMock(is_closed=False)
    channel.default_exchange.publish = publish
    # Grenzen: Kanalbezug und -verwurf sind Broker-I/O.
    broker._ensure_publish_channel = AsyncMock(return_value=channel)  # type: ignore[method-assign]  # Grenze: Kanal-I/O
    broker._discard_publish_channel = AsyncMock()  # type: ignore[method-assign]  # Grenze: Kanal-I/O
    return broker


async def _after_drain_cancel(work: Callable[[], Awaitable[None]], running: asyncio.Event) -> BaseException | None:
    """Ein vom Drain abgebrochener Handler, der nach E-001 weiterlaeuft und
    dann *work* (Retry-/DLQ-/Advance-Publish) ausfuehrt."""
    try:
        running.set()
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass  # E-001: Abbruch wird zum Fehlerergebnis, der Handler laeuft weiter
    current = asyncio.current_task()
    assert current is not None
    assert current.cancelling() == 1, "Voraussetzung: der Drain-Abbruch bleibt sichtbar"
    try:
        await work()
    except BaseException as exc:
        return exc
    return None


async def _run_drain_cancelled(work: Callable[[], Awaitable[None]]) -> BaseException | None:
    running = asyncio.Event()
    handler = asyncio.create_task(_after_drain_cancel(work, running))
    await running.wait()
    handler.cancel()
    return await handler


async def test_internal_cancel_during_publish_after_a_drain_cancel_is_not_sent() -> None:
    publish = AsyncMock(side_effect=asyncio.CancelledError())
    broker = _broker(publish)

    async def _work() -> None:
        await broker.publish("tasks", b"{}")

    outcome = await _run_drain_cancelled(_work)

    assert isinstance(outcome, BrokerConnectionError)
    assert not isinstance(outcome, PublishIndeterminate)


async def test_internal_cancel_during_dead_letter_after_a_drain_cancel_keeps_the_original() -> None:
    publish = AsyncMock(side_effect=asyncio.CancelledError())
    broker = _broker(publish)
    handle = AsyncMock(consumer_tag=None)
    delivery = Delivery(body=b"{}", headers={}, queue="tasks", handle=handle)

    async def _work() -> None:
        await broker.dead_letter(delivery, headers={})

    outcome = await _run_drain_cancelled(_work)

    assert isinstance(outcome, BrokerConnectionError)
    handle.ack.assert_not_awaited()


async def test_a_single_internal_cancel_after_a_drain_cancel_is_retried() -> None:
    publish = AsyncMock(side_effect=[asyncio.CancelledError(), None])
    broker = _broker(publish)

    async def _work() -> None:
        await broker.publish("tasks", b"{}")

    outcome = await _run_drain_cancelled(_work)

    assert outcome is None
    assert publish.await_count == 2


async def test_a_second_real_cancel_during_publish_propagates() -> None:
    publishing = asyncio.Event()

    async def _hanging(*_args: Any, **_kwargs: Any) -> None:
        publishing.set()
        await asyncio.Event().wait()

    broker = _broker(AsyncMock(side_effect=_hanging))
    running = asyncio.Event()

    async def _work() -> None:
        await broker.publish("tasks", b"{}")

    handler = asyncio.create_task(_after_drain_cancel(_work, running))
    await running.wait()
    handler.cancel()  # Drain-Abbruch, cancelling() bleibt 1
    await publishing.wait()
    handler.cancel()  # zweiter, echter Abbruch waehrend des Publish

    outcome = await handler

    assert isinstance(outcome, asyncio.CancelledError)


async def test_a_hanging_publish_after_a_drain_cancel_ends_indeterminate_at_the_deadline() -> None:
    # Der Zeitdeckel bricht per cancel() ab und hebt cancelling() auf 2; der
    # Publish-Pfad muss das als eigenen Abbruch durchreichen, damit
    # asyncio.timeout ihn in TimeoutError -> PublishIndeterminate umwandelt.
    async def _hanging(*_args: Any, **_kwargs: Any) -> None:
        await asyncio.Event().wait()

    broker = _broker(AsyncMock(side_effect=_hanging))
    broker.publish_timeout_s = 0.01

    async def _work() -> None:
        await broker.publish("tasks", b"{}")

    outcome = await _run_drain_cancelled(_work)

    assert isinstance(outcome, PublishIndeterminate)
