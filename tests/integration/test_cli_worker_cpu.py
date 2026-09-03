"""CLI-Integration des CPU-Modus (Plan 2026-09-03-001, U7): ``swarmq worker --pool cpu``.

Startet den Worker als Subprozess (der Forkserver ist ein Singleton je
Prozess, KTD16), reiht einen Task ein und prueft Startlog und Ergebnis.
"""

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


def _spawn_cpu_worker(queue: str) -> subprocess.Popen[str]:
    env = dict(os.environ)
    env["SWARMQ_BROKER_URL"] = rabbitmq_url()
    env["SWARMQ_BACKEND_URL"] = valkey_url()
    env["CPU_FIXTURE_QUEUE"] = queue
    env["PYTHONUNBUFFERED"] = "1"
    return subprocess.Popen(
        [
            sys.executable, "-m", "swarmq.cli", "worker",
            "--module", "tests.e2e._cpu_fixture_tasks",
            "--queues", queue, "--pool", "cpu", "--concurrency", "2",
            "--burst", "--burst-idle-timeout", "3", "--log-format", "json",
        ],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )


async def _enqueue(queue: str) -> str:
    os.environ["CPU_FIXTURE_QUEUE"] = queue
    from tests.e2e import _cpu_fixture_tasks  # noqa: F401  registriert die Tasks fuer den Client

    app = SwarmQ(RabbitMQBroker(rabbitmq_url()), ValkeyBackend(valkey_url()), queues=[queue])
    async with app:
        await app.broker.declare_queue(queue)
        return await app.schedule("cpu_echo", 21)


async def _fetch(queue: str, tid: str) -> dict:
    app = SwarmQ(RabbitMQBroker(rabbitmq_url()), ValkeyBackend(valkey_url()), queues=[queue])
    async with app:
        return await app.get_result(tid, timeout=10.0)


def test_cpu_pool_worker_processes_a_task_and_exits_clean():
    queue = f"cli-cpu-{uuid.uuid4().hex[:8]}"
    tid = asyncio.run(_enqueue(queue))
    proc = _spawn_cpu_worker(queue)
    deadline = time.monotonic() + 90
    while proc.poll() is None and time.monotonic() < deadline:
        time.sleep(0.5)
    output = proc.stdout.read() if proc.stdout else ""
    if proc.poll() is None:
        proc.kill()
        pytest.fail(f"cpu worker did not exit:\n{output[-4000:]}")
    assert proc.returncode == 0, output[-4000:]
    assert "compute_pool_started" in output
    assert '"slots": 2' in output or "slots=2" in output
    result = asyncio.run(_fetch(queue, tid))
    assert result["result"]["value"] == 42
