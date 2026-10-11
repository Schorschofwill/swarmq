"""Chaos-Test (Plan 2026-09-03-001, Verification Contract): der Loop-Prozess
eines CPU-Workers stirbt hart unter dem Supervisor.

Erwartung: das Rechenkind stirbt mit (PDEATHSIG-Kette ueber den Forkserver,
KTD3), der Supervisor startet den Worker neu, die unbestaetigte Nachricht
wird wiederzugestellt und vom Ersatz zu Ende gebracht.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import subprocess
import sys
import threading
import time
import uuid

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from tests.support.cli_process import terminate_worker_process
from tests.support.fixture_modules import ensure_fixture_registered
from tests.support.services import missing_services, rabbitmq_amqp_url, valkey_db_url

pytestmark = [
    pytest.mark.timeout(180),
    pytest.mark.skipif(bool(missing_services()), reason="RabbitMQ/Valkey nicht erreichbar"),
]


async def _enqueue(queue: str, pid_file: str) -> str:
    os.environ["CPU_FIXTURE_QUEUE"] = queue
    ensure_fixture_registered("tests.e2e._cpu_fixture_tasks", "cpu_slow_once")
    app = SwarmQ(RabbitMQBroker(rabbitmq_amqp_url()), ValkeyBackend(valkey_db_url()), queues=[queue])
    async with app:
        await app.broker.declare_queue(queue)
        return await app.schedule("cpu_slow_once", pid_file, 120.0, queue=queue)


async def _fetch(queue: str, tid: str) -> object:
    app = SwarmQ(RabbitMQBroker(rabbitmq_amqp_url()), ValkeyBackend(valkey_db_url()), queues=[queue])
    async with app:
        return await app.get_result(tid, timeout=10.0)


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def _wait_until(pred, timeout: float, what: str) -> None:
    deadline = time.monotonic() + timeout
    while not pred():
        if time.monotonic() > deadline:
            pytest.fail(f"timed out waiting for {what}")
        time.sleep(0.1)


def test_loop_process_sigkill_kills_the_child_and_the_replacement_finishes_the_task(tmp_path):
    queue = f"cli-chaos-{uuid.uuid4().hex[:8]}"
    pid_file = tmp_path / "child.pid"
    tid = asyncio.run(_enqueue(queue, str(pid_file)))

    env = dict(
        os.environ,
        SWARMQ_BROKER_URL=rabbitmq_amqp_url(),
        SWARMQ_BACKEND_URL=valkey_db_url(),
        CPU_FIXTURE_QUEUE=queue,
        PYTHONUNBUFFERED="1",
        # Ausführungsschutz (KTD5, R5): der Ersatz führt die Neuzustellung erst
        # aus, wenn der Running-Marker des getöteten Workers abgelaufen ist;
        # bis dahin stellt er sie zurück (innerhalb des Burst-Fensters).
        SWARMQ_EXECUTION_GUARD_TTL_S="2",
    )
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "swarmq.cli",
            "worker",
            "--module",
            "tests.e2e._cpu_fixture_tasks",
            "--queues",
            queue,
            "--pool",
            "cpu",
            "--concurrency",
            "1",
            "--burst",
            "--burst-idle-timeout",
            "5",
            "--log-format",
            "json",
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )
    lines: list[str] = []

    def _pump() -> None:
        assert proc.stdout is not None
        for line in proc.stdout:
            lines.append(line)

    threading.Thread(target=_pump, daemon=True).start()

    def _spawned_pids() -> list[int]:
        pids = []
        for line in lines:
            if '"worker_spawned"' in line:
                try:
                    pids.append(int(json.loads(line[line.index("{") :])["pid"]))
                except ValueError, KeyError:
                    continue
        return pids

    try:
        _wait_until(pid_file.exists, 60.0, "the compute child to write its pid")
        child_pid = int(pid_file.read_text(encoding="ascii"))
        loop_pids = _spawned_pids()
        assert loop_pids, "".join(lines)[-4000:]
        os.kill(loop_pids[0], signal.SIGKILL)

        _wait_until(lambda: not _alive(child_pid), 10.0, "the compute child to die with its parent")
        _wait_until(lambda: len(_spawned_pids()) >= 2, 30.0, "the supervisor to replace the worker")
        _wait_until(lambda: proc.poll() is not None, 90.0, "the burst run to finish")
    finally:
        terminate_worker_process(proc)
    output = "".join(lines)
    assert proc.returncode == 0, output[-4000:]
    assert '"exitcode": -9' in output, output[-4000:]
    assert asyncio.run(_fetch(queue, tid)) == "redelivered"
