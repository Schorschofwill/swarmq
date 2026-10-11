"""Nachweis BZ-16: Publish und Konsum teilen eine Verbindung.

Register: docs/befunde/betrieb-ueber-zeit-2026-10.md (BZ-16). Bei einem
Ressourcenalarm (Speicher/Platte) hört RabbitMQ auf, vom Socket einer
Verbindung zu lesen, sobald darüber publiziert wird. Der Worker publiziert
(Retry, DLQ, Defer) und bestätigt über dieselbe Verbindung; RabbitMQ empfiehlt
getrennte Verbindungen. ``connection.blocked`` wird nicht behandelt.

Erwartet (Nutzersicht): während ein Publish im Alarm hängt, kommen Acks
anderer Nachrichten desselben Workers beim Broker an.

Der Test löst auf dem Test-Broker (Port 5699) per ``rabbitmqctl`` einen
Speicher-Alarm aus und setzt die Wasserlinie im ``finally`` exakt auf den
vorher gelesenen Wert zurück. Er läuft nur, wenn der Docker-Container des
Test-Brokers eindeutig zu ``SWARMQ_TEST_RABBITMQ_PORT`` gehört; Läufe über
``itest.sh`` sind per ``flock`` serialisiert, andere Läufe sehen den Alarm
daher nicht.

Test-Liste:
- [x] Ack kommt an, während ein Publish desselben Workers im Alarm hängt (BZ-16)
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import subprocess
import time
import uuid

import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.contracts import Delivery
from tests.support.queues import delete_queue_family
from tests.support.services import RABBITMQ_PORT
from tests.support.wait import wait_until

pytestmark = [pytest.mark.integration, pytest.mark.timeout(30)]

_CONTAINER = os.environ.get("SWARMQ_TEST_RABBITMQ_CONTAINER", "swarmq-test-rabbit")
_ALARM_WATERMARK = "0.0000001"
_ACK_BUDGET_S = 3.0


def _rabbitmqctl(*args: str) -> str:
    result = subprocess.run(
        ["docker", "exec", _CONTAINER, "rabbitmqctl", *args],
        capture_output=True,
        text=True,
        timeout=20,
        check=True,
    )
    return result.stdout.strip()


def _eval(expr: str) -> str:
    return _rabbitmqctl("eval", expr)


def _container_serves_test_port() -> bool:
    try:
        mapping = subprocess.run(
            ["docker", "port", _CONTAINER, "5672/tcp"], capture_output=True, text=True, timeout=10, check=True
        ).stdout
    except OSError, subprocess.SubprocessError:
        return False
    return any(line.rsplit(":", 1)[-1].strip() == str(RABBITMQ_PORT) for line in mapping.splitlines())


def _unacked(queue: str) -> int:
    out = _eval(
        f'{{ok,Q}}=rabbit_amqqueue:lookup(rabbit_misc:r(<<"/">>,queue,<<"{queue}">>)),'
        "proplists:get_value(messages_unacknowledged, rabbit_amqqueue:info(Q,[messages_unacknowledged]))."
    )
    return int(out)


async def _wait_for_alarm(active: bool, timeout: float = 10.0) -> None:
    async def reached() -> bool:
        alarms = await asyncio.to_thread(_eval, "rabbit_alarm:get_alarms().")
        return (alarms != "[]") == active

    # rabbitmqctl kennt kein Warten auf Alarme.
    if not await wait_until(reached, timeout=timeout, interval=0.1):
        raise AssertionError(f"broker alarm state did not become active={active} within {timeout}s")


@pytest.fixture
def memory_alarm():
    """Löst einen Speicher-Alarm aus und setzt die Wasserlinie sicher zurück."""
    if not _container_serves_test_port():
        pytest.skip(f"BZ-16: Docker-Container {_CONTAINER!r} bedient nicht Port {RABBITMQ_PORT}")
    original = _eval("vm_memory_monitor:get_vm_memory_high_watermark().")
    try:
        float(original)
    except ValueError:
        pytest.skip(f"BZ-16: unerwartete Wasserlinie {original!r}, Broker bleibt unangetastet")

    def trigger() -> None:
        _rabbitmqctl("set_vm_memory_high_watermark", _ALARM_WATERMARK)

    try:
        yield trigger
    finally:
        _rabbitmqctl("set_vm_memory_high_watermark", original)
        restored = _eval("vm_memory_monitor:get_vm_memory_high_watermark().")
        assert float(restored) == float(original), f"watermark not restored: {restored} != {original}"
        # Erst zurückgeben, wenn der Alarm wirklich weg ist.
        deadline = time.monotonic() + 15
        while _eval("rabbit_alarm:get_alarms().") != "[]":
            assert time.monotonic() < deadline, "broker alarm still active after restoring the watermark"
            time.sleep(0.1)


async def test_ack_reaches_broker_while_publish_is_blocked_by_alarm(rabbitmq_url: str, memory_alarm) -> None:
    queue = f"bz16-{uuid.uuid4().hex[:10]}"
    # Classic statt Quorum: der Effekt liegt an der Verbindung, nicht am
    # Queue-Typ, und nur Classic-Queues melden unbestätigte Nachrichten live.
    worker = RabbitMQBroker(rabbitmq_url, queue_type="classic")
    worker.publish_timeout_s = 20.0
    await worker.connect()
    deliveries: asyncio.Queue[Delivery] = asyncio.Queue()
    blocked_publish: asyncio.Task[None] | None = None
    try:
        await worker.declare_queue(queue)
        await worker.publish(queue, b"A")
        await worker.publish(queue, b"B")

        async def on_delivery(delivery: Delivery) -> None:
            await deliveries.put(delivery)

        await worker.consume(queue, on_delivery, concurrency=10)
        got = [await asyncio.wait_for(deliveries.get(), timeout=5) for _ in range(2)]
        by_body = {d.body: d for d in got}
        assert await asyncio.to_thread(_unacked, queue) == 2, "precondition: A and B are in flight"

        memory_alarm()
        await _wait_for_alarm(active=True)

        # A wird zurückgestellt: der Publish hängt im Alarm.
        blocked_publish = asyncio.create_task(worker.retry_later(by_body[b"A"], delay_ms=60_000, headers={}))
        done, _ = await asyncio.wait({blocked_publish}, timeout=1.0)
        assert not done, "precondition: the retry publish should block during the alarm"

        # B ist fertig und wird bestätigt.
        await worker.ack(by_body[b"B"])

        unacked = 2

        async def ack_arrived() -> bool:
            nonlocal unacked
            unacked = await asyncio.to_thread(_unacked, queue)
            return unacked <= 1

        # Kein Broker-Ereignis meldet ein angekommenes Ack; Pollen.
        await wait_until(ack_arrived, timeout=_ACK_BUDGET_S, interval=0.1)

        assert unacked == 1, (
            f"{unacked} unacked messages in {queue!r} {_ACK_BUDGET_S}s after acking B — "
            "the ack is stuck behind the alarm-blocked publish on the shared connection"
        )
    finally:
        if blocked_publish is not None:
            blocked_publish.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await blocked_publish
        with contextlib.suppress(Exception):
            await asyncio.wait_for(worker.close(), timeout=5)
        # Das Löschen ist kein Publish und läuft auch im Alarm; die Fixture setzt
        # den Alarm erst danach zurück.
        await delete_queue_family(rabbitmq_url, queue)
