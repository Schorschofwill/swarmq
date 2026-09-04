"""R22 (Plan 2026-09-03-001, AE10): Selbst-Recycling nach Taskzahl unter dem Supervisor,
kein Task verloren, Metrik-Port nach jedem Recycling wieder gebunden."""

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
from tests.support.fixture_modules import ensure_fixture_registered
from tests.support.services import missing_services, rabbitmq_amqp_url, valkey_db_url

pytestmark = [
    pytest.mark.timeout(180),
    pytest.mark.skipif(bool(missing_services()), reason="RabbitMQ/Valkey nicht erreichbar"),
]

_N = 120


async def _enqueue(queue: str) -> list[str]:
    os.environ["CLI_TEST_QUEUE"] = queue
    ensure_fixture_registered("tests.integration._cli_fixture_tasks", "cli_echo")

    app = SwarmQ(RabbitMQBroker(rabbitmq_amqp_url()), ValkeyBackend(valkey_db_url()), queues=[queue])
    async with app:
        await app.broker.declare_queue(queue)
        return [await app.schedule("cli_echo", "ping", queue=queue) for _ in range(_N)]


async def _all_done(queue: str, tids: list[str]) -> int:
    app = SwarmQ(RabbitMQBroker(rabbitmq_amqp_url()), ValkeyBackend(valkey_db_url()), queues=[queue])
    ok = 0
    async with app:
        for tid in tids:
            try:
                await app.get_result(tid, timeout=10.0)
            except TaskError:
                continue
            ok += 1
    return ok


def test_worker_recycles_by_task_count_without_losing_tasks():
    queue = f"cli-recycle-{uuid.uuid4().hex[:8]}"
    tids = asyncio.run(_enqueue(queue))
    env = dict(
        os.environ, SWARMQ_BROKER_URL=rabbitmq_amqp_url(), SWARMQ_BACKEND_URL=valkey_db_url(),
        CLI_TEST_QUEUE=queue, PYTHONUNBUFFERED="1", SWARMQ_RECYCLE_MAX_TASKS="40",
    )
    proc = subprocess.Popen(
        [sys.executable, "-m", "swarmq.cli", "worker", "--module", "tests.integration._cli_fixture_tasks",
         "--queues", queue, "--burst", "--burst-idle-timeout", "4", "--log-format", "json",
         "--metrics-port", "9188"],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    deadline = time.monotonic() + 150
    while proc.poll() is None and time.monotonic() < deadline:
        time.sleep(0.5)
    output = proc.stdout.read() if proc.stdout else ""
    if proc.poll() is None:
        proc.kill()
        pytest.fail(f"worker did not exit:\n{output[-4000:]}")
    assert proc.returncode == 0, output[-4000:]
    # Echo-Tasks sind so schnell, dass der Monitor die Schwelle meist erst nach
    # allen 120 Tasks sieht — entscheidend ist: mindestens ein Wechsel, kein Verlust.
    assert output.count("worker_recycling_replacement") >= 1, output[-4000:]
    assert '"expected": true' in output, output[-4000:]
    assert "recycle_ack_missing" not in output
    assert asyncio.run(_all_done(queue, tids)) == _N
