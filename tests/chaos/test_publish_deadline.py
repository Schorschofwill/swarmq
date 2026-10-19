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

PF-19 (a), Plan 0.14 U6 (Nachtrag (3) zu E-025): Im Speicheralarm wartet das
Close des verworfenen Publish-Kanals auf ``unblocked`` oder das Ende der
Verbindung. ``TestDeferredCloseUnderAlarm`` reisst die Verbindung waehrend
dieses Wartens per Toxiproxy ab: Das Close endet sofort, nach Aufhebung und
Reconnect laeuft der naechste Publish, aiormq meldet keine ERROR zum Kanal.
"""

from __future__ import annotations

import asyncio
import logging
import re
import subprocess
import time
from collections.abc import Iterator
from typing import Any, override

import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import BrokerConnectionError, PublishIndeterminate
from tests.chaos.conftest import ChaosStack, ToxiproxyClient
from tests.support.known_leaks import aiormq_streamwriter_leak
from tests.support.wait import wait_until

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


_ALARM_WATERMARK = "0.0000001"
_ALARM_PUBLISH_TIMEOUT_S = 3.0
# Die Wartezeit des Close nach dem Verwurf: lang, damit nur das Verbindungsende
# sie vorher beendet.
_CLOSE_WAIT_S = 30.0
_CLOSE_ENDS_WITHIN_S = 5.0


def _rabbitmqctl(container: str, *args: str) -> str:
    result = subprocess.run(
        ["docker", "exec", container, "rabbitmqctl", *args], capture_output=True, text=True, timeout=30, check=True
    )
    return result.stdout.strip()


class _ChaosAlarm:
    """Speicheralarm auf dem Chaos-Broker; ``release()`` stellt die vorher
    gelesene Wasserlinie exakt wieder her und wartet, bis der Alarm weg ist."""

    def __init__(self, container: str) -> None:
        self._container = container
        original = _rabbitmqctl(container, "eval", "vm_memory_monitor:get_vm_memory_high_watermark().")
        absolute = re.fullmatch(r"\{absolute,(\d+)\}", original)
        self._restore = ["absolute", absolute.group(1)] if absolute else [str(float(original))]

    def _alarms(self) -> str:
        return _rabbitmqctl(self._container, "eval", "rabbit_alarm:get_alarms().")

    async def trigger(self) -> None:
        _rabbitmqctl(self._container, "set_vm_memory_high_watermark", _ALARM_WATERMARK)
        assert await wait_until(lambda: self._alarms() != "[]", timeout=15, interval=0.2), "alarm active"

    def release(self) -> None:
        _rabbitmqctl(self._container, "set_vm_memory_high_watermark", *self._restore)
        deadline = time.monotonic() + 15
        while self._alarms() != "[]":
            assert time.monotonic() < deadline, "chaos broker alarm still active after restoring the watermark"
            time.sleep(0.1)


@pytest.fixture
def chaos_memory_alarm(chaos_stack: ChaosStack) -> Iterator[_ChaosAlarm]:
    """Der Alarm wird am Testende immer aufgehoben, auch nach einem ``release()``."""
    alarm = _ChaosAlarm(f"{chaos_stack.project}-rabbitmq-1")
    try:
        yield alarm
    finally:
        alarm.release()


class _Records(logging.Handler):
    def __init__(self) -> None:
        super().__init__(logging.WARNING)
        self.records: list[logging.LogRecord] = []

    @override
    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


def _aiormq_connection(broker: RabbitMQBroker) -> Any:
    transport = getattr(broker._connection, "transport", None)
    return None if transport is None else transport.connection


class TestDeferredCloseUnderAlarm:
    @aiormq_streamwriter_leak  # TS-02: der abgeschaltete Proxy schliesst die Verbindung
    @pytest.mark.entscheidung("E-025")
    async def test_a_proxy_cut_during_the_alarm_ends_the_deferred_close(
        self, chaos_stack: ChaosStack, toxiproxy: ToxiproxyClient, chaos_memory_alarm: _ChaosAlarm
    ) -> None:
        records = _Records()
        aiormq_logger = logging.getLogger("aiormq")
        aiormq_logger.addHandler(records)
        broker = RabbitMQBroker(chaos_stack.rabbitmq_url, queue_type="classic")
        broker.publish_timeout_s = _ALARM_PUBLISH_TIMEOUT_S
        await broker.connect()
        try:
            await broker.declare_queue(_QUEUE)
            await broker.publish(_QUEUE, b'{"probe": "m0"}')

            await chaos_memory_alarm.trigger()
            # Der auslösende Publish liegt auf dem Socket: Frist reißt, Kanal verworfen.
            with pytest.raises(PublishIndeterminate):
                await broker.publish(_QUEUE, b'{"probe": "m1"}')
            # Das Close liest die Frist erst in seinem ersten Schritt (s. o.).
            broker.publish_timeout_s = _CLOSE_WAIT_S
            [closer] = broker._pending_channel_closes
            dropped = _aiormq_connection(broker)

            await toxiproxy.down("rmq")
            done, _ = await asyncio.wait({closer}, timeout=_CLOSE_ENDS_WITHIN_S)
            assert done, "the cut connection releases the deferred close"
            assert closer.exception() is None

            await asyncio.to_thread(chaos_memory_alarm.release)
            await toxiproxy.up("rmq")

            async def reconnected() -> bool:
                return _aiormq_connection(broker) not in (None, dropped)

            assert await wait_until(reconnected, timeout=30, interval=0.1), "aio-pika reconnects through the proxy"
            receipt = await asyncio.wait_for(broker.publish(_QUEUE, b'{"probe": "m2"}'), timeout=_CLOSE_WAIT_S)
            assert receipt.queue == _QUEUE
            channel_errors = [
                r.getMessage() for r in records.records if r.levelno >= logging.ERROR and "channel" in r.getMessage()
            ]
            assert channel_errors == [], f"no aiormq ERROR about the discarded channel: {channel_errors}"
        finally:
            aiormq_logger.removeHandler(records)
            await broker.close()
