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
from tests.support.services import missing_services, rabbitmq_url, valkey_url

pytestmark = [
    pytest.mark.timeout(180),
    pytest.mark.skipif(bool(missing_services()), reason="RabbitMQ/Valkey nicht erreichbar"),
]


async def _enqueue(queue: str) -> str:
    os.environ["HANG_TEST_QUEUE"] = queue
    from tests.integration import _hang_fixture_tasks  # noqa: F401

    app = SwarmQ(RabbitMQBroker(rabbitmq_url()), ValkeyBackend(valkey_url()), queues=[queue], delivery_limit=2)
    async with app:
        await app.broker.declare_queue(queue)
        return await app.schedule("loop_blocker", 120.0)


async def _fetch(queue: str, tid: str) -> dict:
    app = SwarmQ(RabbitMQBroker(rabbitmq_url()), ValkeyBackend(valkey_url()), queues=[queue], delivery_limit=2)
    async with app:
        return await app.get_result(tid, timeout=10.0)


def test_hung_worker_is_replaced_and_poison_message_reaches_dlq():
    queue = f"cli-hang-{uuid.uuid4().hex[:8]}"
    tid = asyncio.run(_enqueue(queue))
    env = dict(
        os.environ, SWARMQ_BROKER_URL=rabbitmq_url(), SWARMQ_BACKEND_URL=valkey_url(),
        HANG_TEST_QUEUE=queue, PYTHONUNBUFFERED="1",
        SWARMQ_HEARTBEAT_INTERVAL_S="1", SWARMQ_HEARTBEAT_TIMEOUT_S="6", SWARMQ_DELIVERY_LIMIT="2",
        SWARMQ_SHUTDOWN_TIMEOUT="2",
    )
    proc = subprocess.Popen(
        [sys.executable, "-m", "swarmq.cli", "worker", "--module", "tests.integration._hang_fixture_tasks",
         "--queues", queue, "--burst", "--burst-idle-timeout", "3", "--log-format", "json"],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    deadline = time.monotonic() + 150
    while proc.poll() is None and time.monotonic() < deadline:
        time.sleep(0.5)
    output = proc.stdout.read() if proc.stdout else ""
    if proc.poll() is None:
        proc.kill()
        pytest.fail(f"supervisor did not finish:\n{output[-4000:]}")
    assert "worker_hung" in output, output[-4000:]
    assert "delivery_limit_reached_dead_letter" in output, output[-4000:]
    assert proc.returncode == 0, output[-4000:]
    result = asyncio.run(_fetch(queue, tid))
    assert result["is_error"] is True
    assert "WorkerHungError" in result["error_message"]
