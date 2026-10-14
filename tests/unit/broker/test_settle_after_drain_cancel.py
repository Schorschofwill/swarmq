"""``settle()`` in einer vom Drain abgebrochenen Handler-Task (Plan
2026-10-14-0748, U3, KTD9 Teil ``settle``; fix-r-flow.md 2.2, Kritisch 1).

Nach E-001 laeuft ein vom Drain abgebrochener Handler weiter, sein
``cancelling()`` bleibt dabei > 0. "Eigener Abbruch" heisst deshalb nur ein
Anstieg gegenueber dem Stand beim Eintritt in ``settle()``. Eine aio-pika-
interne ``CancelledError`` (Kanalverlust) ist dann ``BrokerConnectionError``
-- sonst sieht das ``finally`` der Pipeline einen terminalen Ausgang und
schreibt den Completed-Marker auf eine unbestaetigte Nachricht.

Testliste:
- [x] Task mit cancelling() > 0 beim Eintritt, interne CancelledError -> BrokerConnectionError
- [x] Abbruch der Task waehrend settle() geht weiter als CancelledError durch
- [x] ohne vorherigen Abbruch: interne CancelledError -> BrokerConnectionError (Bestand)
- [x] vom Drain abgebrochener Handler (cancelling() == 1) wird waehrend settle()
      erneut abgebrochen -> CancelledError geht durch (Review U3, testing)
"""

from __future__ import annotations

import asyncio

import pytest

from swarmq.broker.rabbitmq.delivery import settle
from swarmq.exceptions import BrokerConnectionError

pytestmark = pytest.mark.timeout(5)


async def _internal_cancel() -> None:
    """aio-pika bricht bei Kanalverlust das ``drain_future`` ab."""
    raise asyncio.CancelledError


async def _drain_cancelled_handler(running: asyncio.Event) -> BaseException | None:
    """Ein vom Drain abgebrochener Handler, der nach E-001 weiterlaeuft und
    dann seine Nachricht zurueckstellt."""
    try:
        running.set()
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass  # E-001: Abbruch wird zum Fehlerergebnis, der Handler laeuft weiter
    current = asyncio.current_task()
    assert current is not None
    assert current.cancelling() > 0, "Voraussetzung: der Drain-Abbruch bleibt sichtbar"
    try:
        await settle(_internal_cancel(), "redeliver")
    except BaseException as exc:
        return exc
    return None


async def test_internal_cancel_after_a_drain_cancel_is_a_lost_channel() -> None:
    running = asyncio.Event()
    handler = asyncio.create_task(_drain_cancelled_handler(running))
    await running.wait()
    handler.cancel()

    outcome = await handler

    assert isinstance(outcome, BrokerConnectionError)


async def test_cancel_during_settle_still_propagates() -> None:
    entered = asyncio.Event()

    async def _hanging_action() -> None:
        entered.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(settle(_hanging_action(), "ack"))
    await entered.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task


async def test_internal_cancel_without_a_prior_cancel_is_a_lost_channel() -> None:
    with pytest.raises(BrokerConnectionError):
        await settle(_internal_cancel(), "ack")


async def test_second_cancel_of_a_drain_cancelled_handler_during_settle_propagates() -> None:
    running = asyncio.Event()
    settling = asyncio.Event()

    async def _hanging_action() -> None:
        settling.set()
        await asyncio.Event().wait()

    async def _handler() -> None:
        try:
            running.set()
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            pass  # E-001: der Handler laeuft nach dem Drain-Abbruch weiter
        await settle(_hanging_action(), "redeliver")

    handler = asyncio.create_task(_handler())
    await running.wait()
    handler.cancel()  # Drain-Abbruch, cancelling() bleibt 1
    await settling.wait()
    handler.cancel()  # zweiter, echter Abbruch waehrend settle() (etwa _abort_teardown)

    with pytest.raises(asyncio.CancelledError):
        await handler
