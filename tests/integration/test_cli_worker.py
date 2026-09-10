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
from tests.support.fixture_modules import ensure_fixture_registered  # noqa: E402
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
    ensure_fixture_registered("tests.integration._cli_fixture_tasks", "cli_echo")

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


def test_cli_worker_unknown_module_exits_with_config_code_quickly() -> None:
    """Bewusst geaenderter Test (U4/R19): ein unbekanntes Modul ist ein
    Konfigurationsfehler und endet mit 78 statt 1 -- eine Orchestrierung
    startet einen kaputt konfigurierten Worker sonst endlos neu."""
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
    from swarmq.supervision import CONFIG_ERROR_EXIT_CODE

    assert proc.returncode == CONFIG_ERROR_EXIT_CODE, proc.stderr[-2000:]
    assert "worker_config_error" in proc.stderr
    assert "definitely_no_module_with_this_name_xyz" in proc.stderr
    assert "Traceback" not in proc.stderr  # default INFO level


def _run_worker_cli(*args: str, env_extra: dict[str, str] | None = None, timeout: float = 60.0):
    env = dict(os.environ)
    env["SWARMQ_BROKER_URL"] = rabbitmq_amqp_url()
    env["SWARMQ_BACKEND_URL"] = valkey_db_url()
    env["PYTHONUNBUFFERED"] = "1"
    env.update(env_extra or {})
    return subprocess.run(
        [sys.executable, "-m", "swarmq.cli", "worker", *args],
        cwd=str(REPO_ROOT), env=env, capture_output=True, text=True, timeout=timeout,
    )


def test_configure_hook_middleware_runs_for_every_task() -> None:
    """AE4 (U5, R9/R10): Die Middleware aus ``--configure`` sieht jeden Task.

    Vorher brauchte man dafuer ein eigenes Worker-Entry -- genau das hat die
    Lasttest-Suite geschrieben, nur um eine Middleware anzuhaengen."""
    import json

    queue = f"cli-hook-{uuid.uuid4().hex[:8]}"
    tids = asyncio.run(_enqueue_hook_tasks(queue, 3))
    proc = _run_worker_cli(
        "--module", "tests.integration._cli_fixture_tasks",
        "--configure", "tests.integration._cli_hook_module:setup",
        "--queues", queue, "--burst", "--burst-idle-timeout", "4",
        "--log-format", "json",
        env_extra={"CLI_TEST_QUEUE": queue},
        timeout=90,
    )
    assert proc.returncode == 0, proc.stdout[-3000:] + proc.stderr[-3000:]
    lines = [
        json.loads(line)
        for line in (proc.stdout + proc.stderr).splitlines()
        if line.startswith("{")
    ]
    events = [entry.get("event") for entry in lines]
    assert "hook_called" in events, events[-20:]
    assert events.count("hook_middleware_pre_execute") == 3, events[-20:]
    assert asyncio.run(_results_ok(queue, tids)) == 3


async def _enqueue_hook_tasks(queue: str, n: int) -> list[str]:
    os.environ["CLI_TEST_QUEUE"] = queue
    ensure_fixture_registered("tests.integration._cli_fixture_tasks", "cli_echo")
    app = SwarmQ(RabbitMQBroker(rabbitmq_amqp_url()), ValkeyBackend(valkey_db_url()), queues=[queue])
    async with app:
        await app.broker.declare_queue(queue)
        return [await app.schedule("cli_echo", "ping", queue=queue) for _ in range(n)]


async def _results_ok(queue: str, tids: list[str]) -> int:
    app = SwarmQ(RabbitMQBroker(rabbitmq_amqp_url()), ValkeyBackend(valkey_db_url()), queues=[queue])
    ok = 0
    async with app:
        for tid in tids:
            if await app.get_result(tid, timeout=10.0) is not None:
                ok += 1
    return ok
def test_config_error_in_the_parent_is_one_structured_line_and_exit_78() -> None:
    """AE8 (U4, R19): ``SWARMQ_QUEUE_TYPE=bogus`` scheitert im Elternprozess
    beim Bau der Konfiguration -- vor jedem Kind. Genau eine JSON-Zeile,
    Exit 78. Vorher: eine rohe stderr-Zeile und Exit 1, in dem eine
    Orchestrierung "gleich nochmal" las."""
    import json

    from swarmq.supervision import CONFIG_ERROR_EXIT_CODE

    proc = _run_worker_cli(
        "--module", "tests.integration._cli_fixture_tasks", "--log-format", "json",
        env_extra={"SWARMQ_QUEUE_TYPE": "bogus", "CLI_TEST_QUEUE": "cli-cfg-err"},
        timeout=30,
    )
    assert proc.returncode == CONFIG_ERROR_EXIT_CODE, proc.stderr[-2000:]
    lines = [json.loads(line) for line in proc.stderr.splitlines() if line.startswith("{")]
    config_errors = [entry for entry in lines if entry.get("event") == "worker_config_error"]
    assert len(config_errors) == 1, proc.stderr[-2000:]
    assert config_errors[0]["source"] == "supervisor"
    assert "bogus" in config_errors[0]["error"]


def test_child_config_error_under_two_processes_ends_the_supervisor_with_78() -> None:
    """AE9 (U4, R19-R21): Ein Task-Modul ohne Tasks -- beide Kinder enden mit
    78 und einem strukturierten Logsatz, der Supervisor endet mit 78 ohne
    Neustart. Vorher lief der Supervisor in fuenf Neustarts je Kind und
    endete mit 1."""
    import json

    from swarmq.supervision import CONFIG_ERROR_EXIT_CODE

    proc = _run_worker_cli(
        "--module", "json",  # stdlib, registriert keinen Task
        "--processes", "2", "--log-format", "json",
        timeout=60,
    )
    assert proc.returncode == CONFIG_ERROR_EXIT_CODE, proc.stderr[-3000:]
    lines = [json.loads(line) for line in proc.stderr.splitlines() if line.startswith("{")]
    events = [entry.get("event") for entry in lines]
    # Je Kind ein Logsatz aus dem Kind (source="worker") und der Supervisor
    # meldet den Exit-Code; kein Neustart-Sturm.
    from_worker = [e for e in lines if e.get("event") == "worker_config_error" and e.get("source") == "worker"]
    assert len(from_worker) >= 1, proc.stderr[-3000:]
    assert "no tasks registered" in from_worker[0]["error"]
    assert events.count("worker_spawned") <= 2, proc.stderr[-3000:]
