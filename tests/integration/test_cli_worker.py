"""End-to-end test for ``swarmq worker``.

Spawns the CLI as a real subprocess against running RabbitMQ +
Valkey, schedules a task via the in-process client, verifies the
worker executed it, then sends SIGTERM and asserts a clean exit.

This is the only integration that proves CLI -> Worker.start() ->
broker -> backend round-trips actually wire up correctly when the
CLI provides the configuration.
"""

from __future__ import annotations

import asyncio
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
from tests.support.services import (  # noqa: E402  # nach den Konstanten-Docs
    RABBITMQ_HOST,
    RABBITMQ_PORT,
    VALKEY_HOST,
    VALKEY_PORT,
    rabbitmq_amqp_url,
    valkey_db_url,
)


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
        not (_port_open(RABBITMQ_HOST, RABBITMQ_PORT) and _port_open(VALKEY_HOST, VALKEY_PORT)),
        reason="Docker services (RabbitMQ + Valkey) not reachable on default ports",
    ),
]


def _spawn_worker(queue_name: str, env_extra: dict[str, str] | None = None):
    env = dict(os.environ)
    env["SWARMQ_BROKER_URL"] = rabbitmq_amqp_url()
    env["SWARMQ_BACKEND_URL"] = valkey_db_url()
    env["CLI_TEST_QUEUE"] = queue_name
    env["PYTHONUNBUFFERED"] = "1"
    if env_extra:
        env.update(env_extra)
    return subprocess.Popen(
        [
            sys.executable,
            "-m", "swarmq.cli",
            "worker",
            "--module", "tests.integration._cli_fixture_tasks",
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
    """Block until the worker prints worker_started, raises on timeout."""
    while time.monotonic() < deadline:
        # readline blocks; rely on the outer pytest.mark.timeout for hard cap.
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
    # The test process needs the task class registered too so the
    # client's registry validation lets schedule() through. The fixture
    # module reads CLI_TEST_QUEUE at import time, so set it BEFORE the
    # import. The CLI subprocess imports it independently in its own
    # interpreter (with its own CLI_TEST_QUEUE pointing at the same
    # queue name).
    os.environ["CLI_TEST_QUEUE"] = queue
    import tests.integration._cli_fixture_tasks  # noqa: F401

    broker = RabbitMQBroker(rabbitmq_amqp_url())
    backend = ValkeyBackend(valkey_db_url())
    app = SwarmQ(broker, backend, queues=[queue])
    async with app:
        task_id = await app.schedule("cli_echo", value, queue=queue)
        return await app.get_result(task_id, timeout=15)


def test_cli_worker_round_trip_then_clean_sigterm() -> None:
    queue = f"cli_e2e_{uuid.uuid4().hex[:10]}"
    proc = _spawn_worker(queue)
    try:
        _wait_for_worker_started(proc, time.monotonic() + 30)

        result = asyncio.run(_schedule_and_get_result(queue, "ping"))
        assert result == "echoed:ping"

        proc.send_signal(signal.SIGTERM)
        rc = proc.wait(timeout=30)
        assert rc == 0, f"expected clean exit, got rc={rc}"
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)


def test_cli_worker_unknown_module_exits_non_zero_quickly() -> None:
    """Sanity: bad --module should fail fast with exit 1, not hang."""
    env = dict(os.environ)
    env["SWARMQ_BROKER_URL"] = rabbitmq_amqp_url()
    env["SWARMQ_BACKEND_URL"] = valkey_db_url()
    proc = subprocess.run(
        [
            sys.executable, "-m", "swarmq.cli",
            "worker",
            "--module", "definitely_no_module_with_this_name_xyz",
        ],
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert proc.returncode == 1, f"expected rc=1, got {proc.returncode}"
    assert "swarmq worker:" in proc.stderr
    assert "definitely_no_module_with_this_name_xyz" in proc.stderr
    assert "Traceback" not in proc.stderr  # default INFO level
