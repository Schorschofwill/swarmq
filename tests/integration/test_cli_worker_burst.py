"""R20 (Plan 2026-09-03-001): ``swarmq worker --burst`` unter dem Supervisor endet mit 0."""

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
    pytest.mark.timeout(120),
    pytest.mark.skipif(bool(missing_services()), reason="RabbitMQ/Valkey nicht erreichbar"),
]


async def _enqueue(queue: str, n: int) -> list[str]:
    os.environ["CLI_TEST_QUEUE"] = queue
    from tests.integration import _cli_fixture_tasks

    app = SwarmQ(RabbitMQBroker(rabbitmq_url()), ValkeyBackend(valkey_url()), queues=[queue])
    async with app:
        await app.broker.declare_queue(queue)
        names = [c.task_info.name for c in _cli_fixture_tasks.__dict__.values() if hasattr(c, "task_info")]
        return [await app.schedule(names[0]) for _ in range(n)]


def test_burst_worker_under_default_supervisor_drains_and_exits_zero():
    queue = f"cli-burst-{uuid.uuid4().hex[:8]}"
    asyncio.run(_enqueue(queue, 5))
    env = dict(os.environ, SWARMQ_BROKER_URL=rabbitmq_url(), SWARMQ_BACKEND_URL=valkey_url(),
               CLI_TEST_QUEUE=queue, PYTHONUNBUFFERED="1")
    proc = subprocess.Popen(
        [sys.executable, "-m", "swarmq.cli", "worker", "--module", "tests.integration._cli_fixture_tasks",
         "--queues", queue, "--burst", "--burst-idle-timeout", "2", "--log-format", "json"],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    deadline = time.monotonic() + 60
    while proc.poll() is None and time.monotonic() < deadline:
        time.sleep(0.5)
    output = proc.stdout.read() if proc.stdout else ""
    if proc.poll() is None:
        proc.kill()
        pytest.fail(f"burst worker did not exit under the supervisor:\n{output[-3000:]}")
    assert proc.returncode == 0, output[-3000:]
    assert "all_workers_exited_clean" in output
