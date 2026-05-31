"""End-to-end test for ``swarmq worker`` with the optional Dishka DI
integration (plan U6).

Spawns the CLI as a real subprocess against running RabbitMQ + Valkey using a
task module that builds + registers a Dishka container at import. Schedules a
task whose result proves both APP-scoped and REQUEST-scoped (TaskContext-
derived) dependencies were injected, then sends SIGTERM and asserts a clean
exit — which implies the APP container was closed during shutdown.
"""

from __future__ import annotations

import asyncio
import importlib.util
import os
import signal
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker

REPO_ROOT = Path(__file__).resolve().parents[2]
RABBITMQ_HOST = "localhost"
RABBITMQ_PORT = 5672
VALKEY_HOST = "localhost"
VALKEY_PORT = 6379


def _port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


pytestmark = [
    pytest.mark.integration,
    pytest.mark.timeout(120),
    pytest.mark.skipif(
        not (
            _port_open(RABBITMQ_HOST, RABBITMQ_PORT)
            and _port_open(VALKEY_HOST, VALKEY_PORT)
        ),
        reason="Docker services (RabbitMQ + Valkey) not reachable on default ports",
    ),
    pytest.mark.skipif(
        importlib.util.find_spec("dishka") is None,
        reason="dishka extra not installed",
    ),
]


def _spawn_worker(queue_name: str):
    env = dict(os.environ)
    env["SWARMQ_BROKER_URL"] = "amqp://guest:guest@localhost:5672/"
    env["SWARMQ_BACKEND_URL"] = "valkey://localhost:6379/0"
    env["CLI_TEST_QUEUE"] = queue_name
    env["PYTHONUNBUFFERED"] = "1"
    return subprocess.Popen(
        [
            sys.executable,
            "-m", "swarmq.cli",
            "worker",
            "--module", "tests.integration._dishka_fixture_tasks",
            "--queues", queue_name,
            "--concurrency", "2",
            "--log-format", "json",
        ],
        cwd=str(REPO_ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def _wait_for_worker_started(proc: subprocess.Popen, deadline: float) -> None:
    while time.monotonic() < deadline:
        line = proc.stdout.readline() if proc.stdout else ""
        if not line:
            if proc.poll() is not None:
                raise RuntimeError(
                    f"worker exited before starting; rc={proc.returncode}"
                )
            continue
        if "worker_started" in line:
            return
    raise TimeoutError("worker did not emit worker_started in time")


async def _schedule_and_get_result(queue: str, value: str) -> str:
    os.environ["CLI_TEST_QUEUE"] = queue
    import tests.integration._dishka_fixture_tasks  # noqa: F401 (side-effect)

    broker = RabbitMQBroker("amqp://guest:guest@localhost:5672/")
    backend = ValkeyBackend("valkey://localhost:6379/0")
    app = SwarmQ(broker, backend, queues=[queue])
    async with app:
        task_id = await app.schedule("cli_di_echo", value, queue=queue)
        return await app.get_result(task_id, timeout=15)


def test_cli_worker_dishka_injection_round_trip_then_clean_sigterm() -> None:
    queue = f"cli_di_{uuid.uuid4().hex[:10]}"
    proc = _spawn_worker(queue)
    try:
        _wait_for_worker_started(proc, time.monotonic() + 30)

        result = asyncio.run(_schedule_and_get_result(queue, "ping"))
        # Proves APP-scoped Greeter ("di:ping") AND REQUEST-scoped
        # TaskContext.queue ("@<queue>") were both injected.
        assert result == f"di:ping@{queue}"

        proc.send_signal(signal.SIGTERM)
        rc = proc.wait(timeout=30)
        assert rc == 0, f"expected clean exit (implies container.close), got rc={rc}"
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)
