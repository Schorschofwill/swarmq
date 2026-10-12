"""Console-Script findet das Task-Modul im Arbeitsverzeichnis (Plan
2026-10-12-1105, MR 6, P6, E1 = C).

``swarmq`` als Console-Script hat in ``sys.path[0]`` das ``bin`` des venv.
Bis 0.12 fand ``swarmq worker --module appmod`` ein Modul im
Arbeitsverzeichnis daher nur mit ``PYTHONPATH``. Der Test startet das echte
Script ohne ``PYTHONPATH`` aus einem Temp-Verzeichnis im CPU-Modus: das
Modul muss im Supervisor, im Worker-Kind und im Forkserver-Preload gefunden
werden.

Test-Liste:
13. ``swarmq worker --module appmod --pool cpu`` aus einem Temp-Arbeitsverzeichnis
    fuehrt einen Task aus.
"""

from __future__ import annotations

import asyncio
import importlib
import os
import shutil
import signal
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from tests.support.services import missing_services, rabbitmq_amqp_url, valkey_db_url

pytestmark = [
    pytest.mark.timeout(120),
    pytest.mark.skipif(bool(missing_services()), reason="RabbitMQ/Valkey nicht erreichbar"),
]

_TASK_MODULE = """
from __future__ import annotations

import os

from swarmq.task import Task, TaskInfo


class AppDirEcho(Task):
    task_info = TaskInfo(name="{name}", queues=["{queue}"], max_retries=0)

    async def run(self, x: int) -> dict[str, object]:
        return {{"value": x * 2, "pid": os.getpid()}}
"""


def _console_script() -> str | None:
    return shutil.which("swarmq", path=str(Path(sys.executable).parent))


async def _enqueue(queue: str, task_name: str) -> str:
    app = SwarmQ(RabbitMQBroker(rabbitmq_amqp_url()), ValkeyBackend(valkey_db_url()), queues=[queue])
    async with app:
        await app.broker.declare_queue(queue)
        return await app.schedule(task_name, 21, queue=queue)


async def _fetch(queue: str, tid: str) -> Any:
    app = SwarmQ(RabbitMQBroker(rabbitmq_amqp_url()), ValkeyBackend(valkey_db_url()), queues=[queue])
    async with app:
        return await app.get_result(tid, timeout=10.0)


def test_console_script_runs_cpu_task_from_working_directory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    script = _console_script()
    if script is None:
        pytest.skip("Konsolenskript 'swarmq' liegt nicht neben dem Interpreter")
    suffix = uuid.uuid4().hex[:8]
    queue = f"app-dir-{suffix}"
    module = f"appmod_e2e_{suffix}"
    task_name = f"app_dir_echo_{suffix}"
    (tmp_path / f"{module}.py").write_text(_TASK_MODULE.format(name=task_name, queue=queue))
    # Der Client braucht die Task-Klasse in seiner Registry.
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.import_module(module)
    tid = asyncio.run(_enqueue(queue, task_name))

    env = {k: v for k, v in os.environ.items() if k not in {"PYTHONPATH", "PYTHONSAFEPATH"}}
    env.update(
        SWARMQ_BROKER_URL=rabbitmq_amqp_url(),
        SWARMQ_BACKEND_URL=valkey_db_url(),
        PYTHONUNBUFFERED="1",
    )
    proc = subprocess.Popen(
        [
            script,
            "worker",
            "--module",
            module,
            "--queues",
            queue,
            "--pool",
            "cpu",
            "--concurrency",
            "1",
            "--burst",
            "--burst-idle-timeout",
            "3",
            "--log-format",
            "json",
        ],
        cwd=tmp_path,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )
    try:
        output, _ = proc.communicate(timeout=90)
    except subprocess.TimeoutExpired:
        os.killpg(proc.pid, signal.SIGKILL)
        output, _ = proc.communicate(timeout=10)
        pytest.fail(f"worker did not exit:\n{output[-4000:]}")

    assert proc.returncode == 0, output[-4000:]
    assert "compute_pool_started" in output, output[-4000:]
    result = asyncio.run(_fetch(queue, tid))
    assert result["value"] == 42
    assert result["pid"] != proc.pid
