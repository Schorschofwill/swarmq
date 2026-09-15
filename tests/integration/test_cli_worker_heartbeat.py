"""R21/R30 (Plan 2026-09-03-001, AE11): haengender Worker wird ersetzt, die
Giftnachricht landet nach dem Delivery-Limit im Anwendungs-DLQ mit Fehlerergebnis."""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import time
import uuid

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import TaskError
from tests.support.cli_process import terminate_worker_process
from tests.support.fixture_modules import ensure_fixture_registered
from tests.support.services import missing_services, rabbitmq_amqp_url, valkey_db_url

pytestmark = [
    pytest.mark.timeout(180),
    pytest.mark.skipif(bool(missing_services()), reason="RabbitMQ/Valkey nicht erreichbar"),
]


async def _enqueue(queue: str) -> str:
    os.environ["HANG_TEST_QUEUE"] = queue
    ensure_fixture_registered("tests.integration._hang_fixture_tasks", "loop_blocker")

    app = SwarmQ(RabbitMQBroker(rabbitmq_amqp_url()), ValkeyBackend(valkey_db_url()), queues=[queue], delivery_limit=2)
    async with app:
        await app.broker.declare_queue(queue)
        return await app.schedule("loop_blocker", 120.0, queue=queue)


async def _fetch_error(queue: str, tid: str) -> str:
    app = SwarmQ(RabbitMQBroker(rabbitmq_amqp_url()), ValkeyBackend(valkey_db_url()), queues=[queue], delivery_limit=2)
    async with app:
        with pytest.raises(TaskError) as excinfo:
            await app.get_result(tid, timeout=10.0)
    return str(excinfo.value)


def test_hung_worker_is_replaced_and_poison_message_reaches_dlq():
    queue = f"cli-hang-{uuid.uuid4().hex[:8]}"
    tid = asyncio.run(_enqueue(queue))
    env = dict(
        os.environ, SWARMQ_BROKER_URL=rabbitmq_amqp_url(), SWARMQ_BACKEND_URL=valkey_db_url(),
        HANG_TEST_QUEUE=queue, PYTHONUNBUFFERED="1",
        SWARMQ_HEARTBEAT_INTERVAL_S="1", SWARMQ_HEARTBEAT_TIMEOUT_S="6", SWARMQ_DELIVERY_LIMIT="2",
        SWARMQ_SHUTDOWN_TIMEOUT="2",
    )
    proc = subprocess.Popen(
        [sys.executable, "-m", "swarmq.cli", "worker", "--module", "tests.integration._hang_fixture_tasks",
         "--queues", queue, "--burst", "--burst-idle-timeout", "12", "--log-format", "json"],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        start_new_session=True,
    )
    deadline = time.monotonic() + 150
    while proc.poll() is None and time.monotonic() < deadline:
        time.sleep(0.5)
    output = proc.stdout.read() if proc.stdout else ""
    if proc.poll() is None:
        terminate_worker_process(proc)
        pytest.fail(f"supervisor did not finish:\n{output[-4000:]}")
    assert "worker_hung" in output, output[-4000:]
    assert "delivery_limit_reached_dead_letter" in output, output[-4000:]
    assert proc.returncode == 0, output[-4000:]
    # Idle-Timeout 12 s: der Ersatz muss den Kill des haengenden Workers
    # (6 s Frist + 2 s Gnade) ueberdauern, um die Wiederzustellung zu sehen.
    assert "WorkerHungError" in asyncio.run(_fetch_error(queue, tid))
