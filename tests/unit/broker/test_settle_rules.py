"""Settle-Regeln nach Broker-Verhalten (Plan 2026-10-08-1541, U7, KTD4).

Ab RabbitMQ 4.3 beendet der Broker einen Consumer nach ``consumer-timeout``
per ``basic.cancel`` und lässt den Channel offen. Ein spätes Settle auf den
toten Tag weckte den Zombie-Consumer; es darf erst raus, nachdem der
Consumer-Wächter den Tag selbst abgemeldet hat. Bis 4.2 ist der Channel zu,
dort greift B22 (``BrokerConnectionError``).

Test-Liste:
1. Settle auf vom Broker gecancelten Tag → wartet auf den eigenen Cancel des
   Wächters, geht dann raus; der Aufrufer sieht normalen Erfolg.
2. Der Wächter meldet nicht rechtzeitig ab → Settle wird nicht gesendet,
   kehrt nach der Wartefrist normal zurück, WARNING ``delivery_settle_withheld``.
3. Ein zurückgehaltenes Settle wird als ``delivery_settle_deferred`` geloggt.
4. ``redeliver`` folgt derselben Regel.
5. ``route_via_dlx`` folgt derselben Regel.
6. Channel geschlossen (bis 4.2) → weiter ``BrokerConnectionError`` (B22).
7. Geordneter Shutdown: nach ``cancel_consume`` wird eine laufende Zustellung
   sofort bestätigt.
8. Gültige Zustellung → Ack sofort wie bisher, kein Anstoß des Wächters.
9. ``retry_later`` nach erfolgreichem Publish mit zurückgehaltenem Ack →
   normaler Erfolg.

Ausführungsschutz (U8, KTD5): scheitert nach erfolgreichem Publish nur das
Ack des Originals, ist die Kopie unterwegs -- der Aufrufer muss das vom
gescheiterten Publish unterscheiden können.
10. ``retry_later``: Publish gelingt, Ack auf geschlossenem Channel →
    ``SettleFailedAfterPublish`` (ein ``BrokerConnectionError``).
11. ``dead_letter``: ebenso.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from unittest.mock import AsyncMock

import pytest
from aio_pika.exceptions import ChannelInvalidStateError

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.contracts import Delivery
from swarmq.exceptions import BrokerConnectionError, SettleFailedAfterPublish
from tests.support.consumer_fakes import FakeChannel, FakeQueue, FakeUnderlay

pytestmark = pytest.mark.timeout(5)


class FakeMessage:
    """aio-pika-``IncomingMessage``: Tag, Channel der Zustellung, Verben.

    Jedes Verb hält fest, welche Tags die Queue zu diesem Zeitpunkt schon
    selbst abgemeldet hatte."""

    def __init__(self, tag: str, underlay: FakeUnderlay, queue: FakeQueue) -> None:
        self.consumer_tag = tag
        self._underlay = underlay
        self._queue = queue
        self.settled: list[tuple[str, list[str]]] = []

    @property
    def channel(self) -> FakeUnderlay:
        if self._underlay.is_closed:
            raise ChannelInvalidStateError
        return self._underlay

    async def ack(self) -> None:
        _ = self.channel
        self.settled.append(("ack", list(self._queue.cancelled)))

    async def nack(self, requeue: bool) -> None:
        _ = self.channel
        self.settled.append((f"nack(requeue={requeue})", list(self._queue.cancelled)))


class Setup:
    def __init__(self) -> None:
        self.broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        self.underlay = FakeUnderlay("tag-1")
        self.queue = FakeQueue("q1", FakeChannel(self.underlay))
        self.broker.consumer_watch.add("q1", self.queue, object(), "tag-1")
        self.message = FakeMessage("tag-1", self.underlay, self.queue)
        self.delivery = Delivery(body=b"x", headers={}, queue="q1", handle=self.message)

    async def broker_cancels(self) -> None:
        """Der Wächter hat den Tag gesehen; dann ``basic.cancel`` vom Broker."""
        await self.broker.consumer_watch.tick()
        self.underlay.consumers.pop("tag-1")


@pytest.fixture
async def setup() -> AsyncIterator[Setup]:
    s = Setup()
    yield s
    await s.broker.consumer_watch.stop()


class TestBrokerCancelledTag:
    async def test_ack_waits_for_the_own_cancel_and_then_goes_out(self, setup: Setup) -> None:
        await setup.broker_cancels()
        setup.broker.start_consumer_watch(interval_s=3600.0)

        await setup.broker.ack(setup.delivery)

        assert setup.message.settled == [("ack", ["tag-1"])]


    async def test_ack_is_withheld_when_the_own_cancel_does_not_come_in_time(
        self, setup: Setup, swarmq_logs: list[dict]
    ) -> None:
        await setup.broker_cancels()  # Wächter läuft nicht: niemand meldet ab
        setup.broker.settle_cancel_wait_s = 0.05

        await setup.broker.ack(setup.delivery)

        assert setup.message.settled == []
        [withheld] = [e for e in swarmq_logs if e["event"] == "delivery_settle_withheld"]
        assert withheld["log_level"] == "warning"
        assert withheld["verb"] == "ack"
        assert withheld["consumer_tag"] == "tag-1"
        assert withheld["wait_s"] == 0.05
        assert "redelivers" in withheld["hint"]

    async def test_deferred_ack_is_logged(self, setup: Setup, swarmq_logs: list[dict]) -> None:
        await setup.broker_cancels()
        setup.broker.start_consumer_watch(interval_s=3600.0)

        await setup.broker.ack(setup.delivery)

        [deferred] = [e for e in swarmq_logs if e["event"] == "delivery_settle_deferred"]
        assert deferred["log_level"] == "info"
        assert deferred["verb"] == "ack"
        assert deferred["consumer_tag"] == "tag-1"

    async def test_redeliver_waits_for_the_own_cancel_too(self, setup: Setup) -> None:
        await setup.broker_cancels()
        setup.broker.start_consumer_watch(interval_s=3600.0)

        await setup.broker.redeliver(setup.delivery)

        assert setup.message.settled == [("nack(requeue=True)", ["tag-1"])]

    async def test_route_via_dlx_waits_for_the_own_cancel_too(self, setup: Setup) -> None:
        await setup.broker_cancels()
        setup.broker.start_consumer_watch(interval_s=3600.0)

        await setup.broker.route_via_dlx(setup.delivery)

        assert setup.message.settled == [("nack(requeue=False)", ["tag-1"])]

    async def test_retry_later_with_withheld_ack_returns_normally(self, setup: Setup) -> None:
        await setup.broker_cancels()
        setup.broker.settle_cancel_wait_s = 0.05
        setup.broker._publish_with_retry = AsyncMock()  # type: ignore[method-assign]

        await setup.broker.retry_later(setup.delivery, delay_ms=1000, headers={})

        setup.broker._publish_with_retry.assert_awaited_once()
        assert setup.message.settled == []


class TestClosedChannel:
    async def test_ack_on_closed_channel_still_raises_broker_connection_error(self, setup: Setup) -> None:
        await setup.broker.consumer_watch.tick()
        setup.underlay.is_closed = True  # bis 4.2: Consumer-Timeout schließt den Channel

        with pytest.raises(BrokerConnectionError):
            await setup.broker.ack(setup.delivery)


    async def test_retry_later_reports_a_failed_ack_after_publish(self, setup: Setup) -> None:
        await setup.broker.consumer_watch.tick()
        setup.underlay.is_closed = True
        setup.broker._publish_with_retry = AsyncMock()  # type: ignore[method-assign]

        with pytest.raises(SettleFailedAfterPublish):
            await setup.broker.retry_later(setup.delivery, delay_ms=1000, headers={})

        setup.broker._publish_with_retry.assert_awaited_once()


    async def test_dead_letter_reports_a_failed_ack_after_publish(self, setup: Setup) -> None:
        await setup.broker.consumer_watch.tick()
        setup.underlay.is_closed = True
        setup.broker._publish_with_retry = AsyncMock()  # type: ignore[method-assign]

        with pytest.raises(SettleFailedAfterPublish):
            await setup.broker.dead_letter(setup.delivery, headers={})


class TestOwnTags:
    """Eigene und gültige Tags bleiben ohne Wartezeit bestätigbar. Die
    Standard-Wartefrist (10 s) überstiege den Test-Timeout."""

    async def test_running_delivery_is_acked_at_once_after_cancel_consume(self, setup: Setup) -> None:
        await setup.broker.cancel_consume()

        await setup.broker.ack(setup.delivery)

        assert setup.message.settled == [("ack", ["tag-1"])]

    async def test_valid_delivery_is_acked_at_once(self, setup: Setup) -> None:
        await setup.broker.consumer_watch.tick()

        await setup.broker.ack(setup.delivery)

        assert setup.message.settled == [("ack", [])]
        assert setup.queue.cancelled == []
