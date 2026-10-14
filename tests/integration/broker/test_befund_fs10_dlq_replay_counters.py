"""Nachweis FS-10: Ein Replay per "Move messages"/Shovel einer am Zustelllimit
dead-gelettereten Nachricht landet sofort wieder in der DLQ.

Register: docs/befunde/fehlersuche-runde-2-2026-10.md (FS-10, Quelle R-1).

Die DLQ-Kopie des Workers behaelt ``x-delivery-count``/``x-acquired-count``
des Originals (Unit-Nachweis: ``tests/unit/worker/test_befund_fs10_dlq_copy_counters.py``).
Ein Fremdwerkzeug legt die Nachricht mit allen Headern zurueck. Offen war,
ob RabbitMQ 4 einen vom Publisher gesetzten Zaehler ueberhaupt behaelt; auf
4.3.6 (Testbroker) gilt: Quorum- und Classic-Queues liefern bei der ersten
Zustellung ``x-delivery-count: 19`` und ``x-acquired-count: 19`` unveraendert
aus (Spike im Bericht). Dann sieht das Delivery-Count-Gate ``19 >= 19`` und
dead-lettert die Nachricht vor jeder Ausfuehrung erneut als
``WorkerHungError``.

Abkuerzung fuer die Vorbedingung: Statt 19 echter Abstuerze legt der Test die
eingereihte Nachricht mit ``x-delivery-count: 19`` (dem Stand nach dem 19.
Absturz) in die Haupt-Queue; das Gate schickt sie ohne Ausfuehrung in die DLQ.

Gegen echtes RabbitMQ und Valkey (Testports 5699/6399 via itest.sh).

Behoben (KTD13): ``identity.copy_headers`` entfernt die Zaehler aus jeder
Kopie, die swarmq publiziert, auch aus dem DLQ-Stempel.

Grenze: Eine Nachricht, die der Broker selbst ueber den DLX ins ``.XQ``
legt (``route_via_dlx``, ``nack(requeue=False)``), kann swarmq nicht
filtern. Die zweite Variante haelt fest, was RabbitMQ dabei mit einem
mitgebrachten Zaehler macht.

Testliste:
# [x] am Zustelllimit dead-gelettert, unveraendert aus dem .XQ in die
#     Haupt-Queue zurueckgelegt -> der Task laeuft
# [x] Broker-DLX (``route_via_dlx``) einer Nachricht mit Zaehlern: Replay
#     per Shovel -> Verhalten des Brokers festgehalten (RabbitMQ 4.3.6: der
#     Zaehler bleibt, das Replay geht wieder in die DLQ; xfail als Grenze)
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from collections.abc import AsyncGenerator, Callable
from typing import Any, override

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.contracts import Delivery
from swarmq.task import Task, TaskInfo
from tests.support.wait import wait_until
from tests.unit._log_capture import capture_swarmq_logs

pytestmark = [pytest.mark.integration, pytest.mark.timeout(60)]

_FS10_COUNT_AT_LIMIT = 19  # delivery_limit 20 (Default) -> Gate ab 19


@pytest.fixture
def fs10_queue() -> str:
    return f"fs10-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def fs10_runs() -> list[str]:
    return []


@pytest.fixture
def _fs10_tasks(fs10_queue: str, fs10_runs: list[str]) -> Any:
    class Fs10Crashed(Task):
        task_info = TaskInfo(name="fs10_crashed_before_fix", queues=[fs10_queue])

        @override
        async def run(self) -> str:
            fs10_runs.append("run")
            return "fixed"

    yield Fs10Crashed
    Task._clear_registered_tasks()


@contextlib.asynccontextmanager
async def _fs10_worker(rabbitmq_url: str, valkey_url: str, queue: str) -> AsyncGenerator[SwarmQ]:
    app = SwarmQ(RabbitMQBroker(rabbitmq_url), ValkeyBackend(valkey_url), queues=[queue], concurrency=2)
    async with app:
        await app.broker.declare_queue(queue)
        worker = asyncio.create_task(app.start_worker())
        try:
            yield app
        finally:
            worker.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker


async def _fs10_wait_for(predicate: Callable[[], bool], timeout: float = 15.0) -> None:
    if not await wait_until(predicate, timeout=timeout):
        raise TimeoutError(f"condition not met within {timeout} s")


async def _fs10_move(rabbitmq_url: str, source: str, target: str, extra_headers: dict[str, Any] | None = None) -> None:
    """Eine Nachricht aus *source* holen und mit allen Headern nach *target*
    legen -- wie "Move messages" in der RabbitMQ-UI bzw. ein Shovel."""
    broker = RabbitMQBroker(rabbitmq_url)
    await broker.connect()
    try:
        assert broker._connection is not None
        channel = await broker._connection.channel()
        message = await (await channel.get_queue(source)).get(no_ack=True)
        await channel.close()
        assert message is not None, f"Voraussetzung: Nachricht liegt in {source}"
        headers = {**dict(message.headers or {}), **(extra_headers or {})}
        await broker.publish(target, message.body, headers=headers)
    finally:
        await broker.close()


@pytest.mark.befund("FS-10")
@pytest.mark.usefixtures("_fs10_tasks")
async def test_fs10_shovel_replay_of_a_delivery_limit_dead_letter_runs_the_task(
    rabbitmq_url: str, valkey_url: str, fs10_queue: str, fs10_runs: list[str]
) -> None:
    def _dlq_events() -> list[dict[str, Any]]:
        return [e for e in logs if e["event"] == "task_sent_to_dlq"]

    with capture_swarmq_logs() as logs:
        # Eingereiht, solange noch kein Worker laeuft; dann mit dem Zaehlerstand
        # nach dem 19. Absturz zurueck in die Haupt-Queue.
        producer = SwarmQ(RabbitMQBroker(rabbitmq_url), ValkeyBackend(valkey_url), queues=[fs10_queue])
        async with producer:
            await producer.broker.declare_queue(fs10_queue)
            await producer.schedule("fs10_crashed_before_fix")
        await _fs10_move(
            rabbitmq_url,
            fs10_queue,
            fs10_queue,
            extra_headers={"x-delivery-count": _FS10_COUNT_AT_LIMIT, "x-acquired-count": _FS10_COUNT_AT_LIMIT},
        )

        async with _fs10_worker(rabbitmq_url, valkey_url, fs10_queue):
            await _fs10_wait_for(lambda: len(_dlq_events()) >= 1)
        assert fs10_runs == [], "Voraussetzung: am Zustelllimit vor der Ausfuehrung dead-gelettert"
        assert _dlq_events()[0].get("error_type") == "WorkerHungError", f"Voraussetzung: {_dlq_events()[0]}"

        # Bug behoben, Operator spielt die DLQ per Shovel/"Move messages" zurueck.
        await _fs10_move(rabbitmq_url, f"{fs10_queue}.XQ", fs10_queue)

        async with _fs10_worker(rabbitmq_url, valkey_url, fs10_queue):
            await _fs10_wait_for(lambda: len(fs10_runs) >= 1 or len(_dlq_events()) >= 2)

    assert fs10_runs == ["run"], (
        f"Replay nicht ausgefuehrt, sofort wieder dead-gelettert: {[(e.get('error_type'), e.get('error')) for e in _dlq_events()[1:]]}"
    )


async def _fs10_route_via_dlx(rabbitmq_url: str, queue: str) -> None:
    """Eine Nachricht aus *queue* holen und per ``route_via_dlx``
    (``nack(requeue=False)``) vom Broker ins ``.XQ`` legen lassen."""
    broker = RabbitMQBroker(rabbitmq_url)
    await broker.connect()
    try:
        assert broker._connection is not None
        channel = await broker._connection.channel()
        message = await (await channel.get_queue(queue)).get(no_ack=False)
        assert message is not None, f"Voraussetzung: Nachricht liegt in {queue}"
        delivery = Delivery(body=message.body, headers=dict(message.headers or {}), queue=queue, handle=message)
        await broker.route_via_dlx(delivery)
        await channel.close()
    finally:
        await broker.close()


@pytest.mark.xfail(
    strict=True,
    reason=(
        "Grenze FS-10 (kein Befund): RabbitMQ 4.3 nimmt einen mitgebrachten x-delivery-count in die "
        "DLX-Kopie mit; solche Kopien und der 0.12.0-Bestand nur per 'swarmq dlq retry' zurueckspielen"
    ),
)
@pytest.mark.usefixtures("_fs10_tasks")
async def test_fs10_shovel_replay_of_a_broker_dlx_copy_with_counters(
    rabbitmq_url: str, valkey_url: str, fs10_queue: str, fs10_runs: list[str]
) -> None:
    """Grenze von FS-10: Die DLX-Kopie entsteht im Broker, swarmq filtert
    sie nicht. Festgehalten wird, ob RabbitMQ den vom Publisher gesetzten
    Zaehler dabei mitnimmt -- dann gilt fuer solche Kopien weiter
    ``swarmq dlq retry`` (Doku ``delivery-semantics``)."""
    producer = SwarmQ(RabbitMQBroker(rabbitmq_url), ValkeyBackend(valkey_url), queues=[fs10_queue])
    async with producer:
        await producer.broker.declare_queue(fs10_queue)
        await producer.schedule("fs10_crashed_before_fix")
    await _fs10_move(
        rabbitmq_url,
        fs10_queue,
        fs10_queue,
        extra_headers={"x-delivery-count": _FS10_COUNT_AT_LIMIT, "x-acquired-count": _FS10_COUNT_AT_LIMIT},
    )
    await _fs10_route_via_dlx(rabbitmq_url, fs10_queue)
    await _fs10_move(rabbitmq_url, f"{fs10_queue}.XQ", fs10_queue)

    with capture_swarmq_logs() as logs:
        async with _fs10_worker(rabbitmq_url, valkey_url, fs10_queue):
            await _fs10_wait_for(lambda: len(fs10_runs) >= 1 or any(e["event"] == "task_sent_to_dlq" for e in logs))

    dlq = [(e.get("error_type"), e.get("error")) for e in logs if e["event"] == "task_sent_to_dlq"]
    assert fs10_runs == ["run"], f"DLX-Kopie traegt den Zaehler, Replay wieder dead-gelettert: {dlq}"
