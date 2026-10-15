"""R22 (Plan 2026-09-03-001, AE10): Selbst-Recycling nach Taskzahl unter dem Supervisor,
kein Task verloren, Metrik-Port nach jedem Recycling wieder gebunden."""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import threading
import time
import uuid

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import TaskError
from tests.support.cli_process import terminate_worker_process
from tests.support.fixture_modules import ensure_fixture_registered
from tests.support.services import missing_services, rabbitmq_amqp_url, valkey_db_url

pytestmark = [
    pytest.mark.timeout(180),
    # Feste Metrik-Ports 9188 und 9190/9191: beide Tests auf einem xdist-Prozess.
    pytest.mark.xdist_group("metrics-ports"),
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
        os.environ,
        SWARMQ_BROKER_URL=rabbitmq_amqp_url(),
        SWARMQ_BACKEND_URL=valkey_db_url(),
        CLI_TEST_QUEUE=queue,
        PYTHONUNBUFFERED="1",
        SWARMQ_RECYCLE_MAX_TASKS="40",
    )
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "swarmq.cli",
            "worker",
            "--module",
            "tests.integration._cli_fixture_tasks",
            "--queues",
            queue,
            "--burst",
            "--burst-idle-timeout",
            "4",
            "--log-format",
            "json",
            "--metrics-port",
            "9188",
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )
    deadline = time.monotonic() + 150
    while proc.poll() is None and time.monotonic() < deadline:
        time.sleep(0.5)
    output = proc.stdout.read() if proc.stdout else ""
    if proc.poll() is None:
        terminate_worker_process(proc)
        pytest.fail(f"worker did not exit:\n{output[-4000:]}")
    assert proc.returncode == 0, output[-4000:]
    # Echo-Tasks sind so schnell, dass der Monitor die Schwelle meist erst nach
    # allen 120 Tasks sieht — entscheidend ist: mindestens ein Wechsel, kein Verlust.
    assert output.count("worker_recycling_replacement") >= 1, output[-4000:]
    assert '"expected": true' in output, output[-4000:]
    assert "recycle_ack_missing" not in output
    assert asyncio.run(_all_done(queue, tids)) == _N


def _drain(proc: subprocess.Popen[str], sink: list[str]) -> None:
    """Liest den Worker-Ausgabestrom nebenlaeufig mit, damit der Test auf eine
    Logzeile warten kann, ohne den Prozess vorher zu beenden."""
    assert proc.stdout is not None
    for line in proc.stdout:
        sink.append(line)  # noqa: PERF402 - jede Zeile muss sofort sichtbar sein, waehrend der Test parallel auf sie wartet


def _wait_for_line(sink: list[str], needle: str, timeout: float = 60.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if any(needle in line for line in sink):
            return True
        time.sleep(0.2)
    return False


def _metrics_ok(port: int, timeout: float = 20.0) -> bool:
    """Wartet, bis ``/metrics`` auf *port* antwortet."""
    import urllib.error
    import urllib.request

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=1.0) as resp:
                if resp.status == 200:
                    return True
        except urllib.error.URLError, OSError:
            time.sleep(0.2)
    return False


def test_two_processes_bind_their_own_metrics_port_across_a_recycle():
    """AE3 (U3, R5/R8): Basisport 9190 → Slot 0 bindet 9190, Slot 1 bindet
    9191, und nach einem Recycling bindet der Ersatz denselben Port wieder.

    Vor U3 lehnte der Startguard diese Kombination rundheraus ab: ein fester
    Port bei mehreren Prozessen war ein Konfigurationsfehler.
    """
    queue = f"cli-ports-{uuid.uuid4().hex[:8]}"
    tids = asyncio.run(_enqueue(queue))
    env = dict(
        os.environ,
        SWARMQ_BROKER_URL=rabbitmq_amqp_url(),
        SWARMQ_BACKEND_URL=valkey_db_url(),
        CLI_TEST_QUEUE=queue,
        PYTHONUNBUFFERED="1",
        SWARMQ_RECYCLE_MAX_TASKS="20",
    )
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "swarmq.cli",
            "worker",
            "--module",
            "tests.integration._cli_fixture_tasks",
            "--queues",
            queue,
            "--processes",
            "2",
            "--log-format",
            "json",
            "--metrics-port",
            "9190",
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )
    lines: list[str] = []
    reader = threading.Thread(target=_drain, args=(proc, lines), daemon=True)
    reader.start()
    try:
        assert _metrics_ok(9190), "Slot 0 hat den Basisport nicht gebunden"
        assert _metrics_ok(9191), "Slot 1 hat basis+1 nicht gebunden"
        # Die 120 eingereihten Tasks loesen bei Schwelle 20 Ersetzungen aus.
        assert asyncio.run(_all_done(queue, tids)) == _N
        # Erst wenn ein Ersatz tatsaechlich gelaufen ist, sagt die erneute
        # Portpruefung etwas ueber R8 aus.
        assert _wait_for_line(lines, "worker_recycling_replacement"), "kein Ersatz beobachtet"
        assert _metrics_ok(9190, timeout=30.0), "Slot 0 hat den Port nach dem Ersatz nicht wieder gebunden"
        assert _metrics_ok(9191, timeout=30.0), "Slot 1 hat den Port nach dem Ersatz nicht wieder gebunden"
    finally:
        terminate_worker_process(proc)
        reader.join(timeout=5)
    output = "".join(lines)
    assert "bind failed" not in output, output[-4000:]
