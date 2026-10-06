"""E2E: CLI > Env > Default fuer die Optionen von ``swarmq worker`` in echten
Prozessen.

``swarmq worker`` laeuft als echter Subprozess (Supervisor -> Worker-
Subprozess) gegen echtes RabbitMQ und Valkey. Gemessen wird im Worker: die
Felder von ``worker_started`` (Queues, Nebenlaeufigkeit) und die PID, unter
der ein Task laeuft (gleich der Worker-PID heisst IO-Pool, ein Rechenkind
hiesse ``--pool cpu``). Vorher schlug im Worker ``SWARMQ_*`` die Option.

Test-Liste:
1. [x] Widersprechende Flags schlagen ``SWARMQ_QUEUES``/``SWARMQ_CONCURRENCY``/
       ``SWARMQ_POOL``/``SWARMQ_BURST_IDLE_TIMEOUT`` im Worker.
2. [x] Ohne Flags gilt das Env (Queues, Nebenlaeufigkeit, Burst).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from tests.support.cli_process import terminate_worker_process
from tests.support.fixture_modules import ensure_fixture_registered

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(120)]

_REPO_ROOT = Path(__file__).resolve().parents[2]
# Das Logging-Fixture-Modul liest seine Queue aus ``LOG_E2E_QUEUE`` und gibt
# die PID des ausfuehrenden Prozesses zurueck -- genau, was hier gebraucht wird.
_TASKS_MODULE = "tests.e2e._logging_fixture_tasks"
_RUN_TIMEOUT_S = 60.0


def _new_queue(prefix: str) -> str:
    return f"cli-prec-{prefix}-{uuid.uuid4().hex[:8]}"


async def _enqueue(rabbitmq_url: str, valkey_url: str, queue: str) -> str:
    previous = os.environ.get("LOG_E2E_QUEUE")
    os.environ["LOG_E2E_QUEUE"] = queue
    try:
        ensure_fixture_registered(_TASKS_MODULE, "log_async_echo")
    finally:
        if previous is None:
            os.environ.pop("LOG_E2E_QUEUE", None)
        else:
            os.environ["LOG_E2E_QUEUE"] = previous
    app = SwarmQ(RabbitMQBroker(rabbitmq_url), ValkeyBackend(valkey_url), queues=[queue])
    async with app:
        await app.broker.declare_queue(queue)
        return await app.schedule("log_async_echo", "prec", queue=queue)


async def _result(rabbitmq_url: str, valkey_url: str, queue: str, tid: str) -> Any:
    app = SwarmQ(RabbitMQBroker(rabbitmq_url), ValkeyBackend(valkey_url), queues=[queue])
    async with app:
        return await app.get_result(tid, timeout=15.0)


def _env(rabbitmq_url: str, valkey_url: str, task_queue: str, **swarmq_env: str) -> dict[str, str]:
    """Env ohne geerbte ``SWARMQ_*`` -- sonst entscheidet die Shell des
    Entwicklers mit."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("SWARMQ_")}
    env.update({
        "SWARMQ_BROKER_URL": rabbitmq_url,
        "SWARMQ_BACKEND_URL": valkey_url,
        "LOG_E2E_QUEUE": task_queue,
        "PYTHONUNBUFFERED": "1",
        "PYTHONPATH": os.pathsep.join(filter(None, [str(_REPO_ROOT), os.environ.get("PYTHONPATH")])),
    })
    env.update(swarmq_env)
    return env


def _run_worker(*flags: str, env: dict[str, str]) -> tuple[int, str]:
    """``swarmq worker`` bis zum Burst-Ende; eigene Sitzung fuer den Teardown."""
    proc = subprocess.Popen(
        [
            sys.executable, "-m", "swarmq.cli", "worker", "--module", _TASKS_MODULE,
            "--log-format", "json", *flags,
        ],
        cwd=str(_REPO_ROOT), env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, start_new_session=True,
    )
    try:
        stdout, stderr = proc.communicate(timeout=_RUN_TIMEOUT_S)
    except subprocess.TimeoutExpired:
        terminate_worker_process(proc, grace_s=10.0)
        stdout, stderr = proc.communicate()
        pytest.fail(f"worker did not finish in {_RUN_TIMEOUT_S}s:\n{stderr[-4000:]}")
    finally:
        terminate_worker_process(proc, grace_s=10.0)
    return proc.returncode, stdout + stderr


def _worker_started(output: str) -> dict[str, Any]:
    for line in output.splitlines():
        try:
            record = json.loads(line)
        except ValueError:
            continue
        if isinstance(record, dict) and record.get("event") == "worker_started":
            return record
    pytest.fail(f"no worker_started line:\n{output[-4000:]}")


@pytest.mark.asyncio
async def test_cli_flags_win_over_environment_in_the_worker(rabbitmq_url, valkey_url):
    cli_queue, env_queue = _new_queue("cli"), _new_queue("env")
    tid = await _enqueue(rabbitmq_url, valkey_url, cli_queue)
    env = _env(
        rabbitmq_url, valkey_url, cli_queue,
        SWARMQ_QUEUES=env_queue, SWARMQ_CONCURRENCY="7", SWARMQ_POOL="cpu",
        # Gewaenne das Env, liefe der Worker in den Test-Timeout statt nach 3 s
        # Leerlauf zu enden.
        SWARMQ_BURST_IDLE_TIMEOUT="600",
    )

    rc, output = _run_worker(
        "--queues", cli_queue, "--concurrency", "3", "--pool", "io",
        "--burst", "--burst-idle-timeout", "3", env=env,
    )

    assert rc == 0, output[-4000:]
    started = _worker_started(output)
    assert (started["queues"], started["concurrency"]) == ([cli_queue], 3)
    result = await _result(rabbitmq_url, valkey_url, cli_queue, tid)
    # IO-Pool: der Task laeuft im Worker selbst, nicht in einem Rechenkind.
    assert result["pid"] == started["pid"]


@pytest.mark.asyncio
async def test_environment_applies_without_flags(rabbitmq_url, valkey_url):
    env_queue = _new_queue("env")
    tid = await _enqueue(rabbitmq_url, valkey_url, env_queue)
    env = _env(
        rabbitmq_url, valkey_url, env_queue,
        SWARMQ_QUEUES=env_queue, SWARMQ_CONCURRENCY="4",
        SWARMQ_BURST_MODE="true", SWARMQ_BURST_IDLE_TIMEOUT="3",
    )

    rc, output = _run_worker(env=env)

    assert rc == 0, output[-4000:]
    started = _worker_started(output)
    assert (started["queues"], started["concurrency"]) == ([env_queue], 4)
    result = await _result(rabbitmq_url, valkey_url, env_queue, tid)
    assert result["tag"] == "prec"
