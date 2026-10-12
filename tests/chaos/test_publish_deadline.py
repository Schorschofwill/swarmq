"""Reproduktion des Referenzlauf-Ausfalls gegen einen echten Broker.

Plan 2026-08-28-001, U6.

Am 2026-08-28 blockierte ein fuenfsekuendiger Toxiproxy-Ausfall auf
RabbitMQ eine Handvoll ``schedule``-Aufrufe dauerhaft. aio-pika verband neu
und meldete das auch, die haengenden Aufrufe blieben offen. Danach stand
der Lauf 62 Minuten ohne eine einzige weitere Logzeile.

Entscheidend ist die Art der Stoerung: Ein abgeschalteter Proxy schliesst
alle Verbindungen, das sieht die Gegenseite und der bestehende
Wiederholpfad faengt es ab. Erst ein Blackhole — Pakete verschwinden, die
Verbindung bleibt offen — erzeugt den Haenger, gegen den dieser Test
gerichtet ist. Ohne den Deckel laeuft er in den pytest-Timeout.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import BrokerConnectionError

_QUEUE = "chaos_publish_deadline"
# Kurz genug fuer einen zuegigen Test, lang genug, dass ein gesunder
# Publish gegen den echten Broker sie nie ausschoepft.
_DEADLINE_S = 3.0


class TestPublishDeadlineAgainstRealBroker:
    @pytest.fixture
    async def broker(self, chaos_stack):
        broker = RabbitMQBroker(chaos_stack.rabbitmq_url)
        broker.publish_timeout_s = _DEADLINE_S
        await broker.connect()
        await broker.declare_queue(_QUEUE)
        try:
            yield broker
        finally:
            await broker.close()

    async def test_publish_returns_during_a_silent_stall(self, broker, toxiproxy):
        """Der Kern: Waehrend der stillen Stoerung kehrt der Publish
        innerhalb der Frist mit Fehler zurueck, statt unbegrenzt zu warten.

        Ohne den Deckel haengt dieser Aufruf bis zum pytest-Timeout — das
        ist der Beweis, dass der Test ohne den Fix nicht gruen wird.
        """
        await toxiproxy.add_blackhole("rmq")

        started = time.monotonic()
        with pytest.raises(BrokerConnectionError):
            await asyncio.wait_for(
                broker.publish(_QUEUE, b'{"probe": "stall"}'),
                timeout=_DEADLINE_S * 4,
            )
        elapsed = time.monotonic() - started

        assert elapsed < _DEADLINE_S * 3, (
            f"Publish brauchte {elapsed:.1f}s statt der zugesagten {_DEADLINE_S}s-Obergrenze"
        )

    async def test_publish_recovers_after_the_stall_clears(self, broker, toxiproxy):
        """Der Beleg fuer den Kanalverwurf: Nach Wiederherstellung gelingt
        der naechste Publish in normaler Zeit. Bliebe der tote Kanal
        stehen, verbrennte auch er erneut die volle Frist.
        """
        await toxiproxy.add_blackhole("rmq")

        with pytest.raises(BrokerConnectionError):
            await asyncio.wait_for(
                broker.publish(_QUEUE, b'{"probe": "stall"}'),
                timeout=_DEADLINE_S * 4,
            )

        await toxiproxy.reset()

        started = time.monotonic()
        receipt = await asyncio.wait_for(
            broker.publish(_QUEUE, b'{"probe": "recovered"}'),
            timeout=_DEADLINE_S * 4,
        )
        elapsed = time.monotonic() - started

        assert receipt.queue == _QUEUE
        assert elapsed < _DEADLINE_S, (
            f"Publish nach der Stoerung brauchte {elapsed:.1f}s — der tote Kanal wurde offenbar nicht verworfen"
        )
