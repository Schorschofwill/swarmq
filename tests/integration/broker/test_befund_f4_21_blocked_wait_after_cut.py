"""Nachweis F4-21 (behoben in 0.14.0): Ein Publish, der im Alarm wartet, wartet nach einem Abriss bis zur Frist.

Register: docs/befunde/fehlersuche-runde-4-2026-10.md (F4-21; Nebenbeobachtung 2
im Spike ``docs/solutions/spikes/2026-10-spike-kanal-close-nach-unblocked.md``,
Gegenprüfung 0.14, Probe B).

Probe B: m2 wartete im Alarm, Abriss bei 1 s, Aufhebung und Reconnect bei 2,4 s,
ein frisches m3 war sofort bestätigt; m2 endete trotzdem erst nach 20,01 s, der
vollen Frist, als ``BrokerPublishBlocked``. ``_wait_while_blocked`` wartet nur
auf das Unblocked-Event der alten aiormq-Verbindung; aiormq setzt es beim
Schließen nicht. Verletzt: Docstring von ``BrokerPublishBlocked``,
``errors.md`` („does not unblock it within …“), E-042.

Ablauf: m0, Alarm, m1 (``PublishIndeterminate``, Verbindung blockiert), m2
wartet, der Broker trennt die Verbindung, Aufhebung. m2 muss nach dem Reconnect
gesendet und bestätigt sein, lange vor seiner Frist. Ob m2 auf der neuen
Verbindung vor oder nach der Aufhebung sendet, ist gleich: Ein Frame, der im
Alarm auf dem Socket liegt, bestätigt der Broker nach der Aufhebung (D-1, E-025).

Test-Liste:
# [x] Abriss im Alarm, Aufhebung: m2 bestätigt, lange vor seiner Frist
"""

from __future__ import annotations

import asyncio
import time
import uuid

import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import PublishIndeterminate
from tests.integration.broker.conftest import MemoryAlarm, wait_for_alarm
from tests.integration.broker.test_pf19_close_after_unblocked import (
    _close_connection_at_broker,
    _wait_until_blocked,
)
from tests.support.queues import delete_queue_family

# exclusive: der Speicheralarm blockiert jeden Verleger des Brokers.
pytestmark = [pytest.mark.integration, pytest.mark.timeout(90), pytest.mark.exclusive]

_TRIGGER_TIMEOUT_S = 3.0
_PUBLISH_TIMEOUT_S = 20.0
# Reconnect (bis 5 s) plus Aufhebung, mit Luft; die Frist liegt deutlich darüber.
_CONFIRMED_WITHIN_S = 12.0


@pytest.mark.befund("F4-21")
@pytest.mark.entscheidung("E-025")
async def test_a_publish_waiting_in_the_alarm_is_sent_after_the_reconnect(
    rabbitmq_url: str, memory_alarm: MemoryAlarm
) -> None:
    queue = f"f4-21-{uuid.uuid4().hex[:10]}"
    name = f"f4-21-{uuid.uuid4().hex[:10]}"
    broker = RabbitMQBroker(f"{rabbitmq_url}?name={name}", queue_type="classic")
    broker.publish_timeout_s = _TRIGGER_TIMEOUT_S
    await broker.connect()
    m2: asyncio.Task[object] | None = None
    try:
        await broker.declare_queue(queue)
        await broker.publish(queue, b"m0")

        memory_alarm()
        await wait_for_alarm(active=True)
        with pytest.raises(PublishIndeterminate):
            await broker.publish(queue, b"m1")
        await _wait_until_blocked(broker)
        broker.publish_timeout_s = _PUBLISH_TIMEOUT_S
        m2 = asyncio.create_task(broker.publish(queue, b"m2"))
        done, _ = await asyncio.wait({m2}, timeout=1.0)
        assert not done, "precondition: m2 waits while the broker blocks"

        await _close_connection_at_broker(name)
        cut_at = time.monotonic()
        await asyncio.to_thread(memory_alarm.release)

        outcome = (await asyncio.gather(asyncio.wait_for(m2, timeout=_PUBLISH_TIMEOUT_S + 5), return_exceptions=True))[
            0
        ]
        elapsed = time.monotonic() - cut_at
        assert not isinstance(outcome, BaseException), f"m2 is confirmed after the reconnect, got {outcome!r}"
        assert elapsed < _CONFIRMED_WITHIN_S, f"m2 is confirmed soon after the reconnect, took {elapsed:.1f}s"
    finally:
        if m2 is not None and not m2.done():
            m2.cancel()
        if m2 is not None:
            await asyncio.gather(m2, return_exceptions=True)
        await broker.close()
        await delete_queue_family(rabbitmq_url, queue)
