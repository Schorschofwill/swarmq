"""Worker-Kinder sterben mit ihrem Supervisor -- auch bei dessen SIGKILL.

Hintergrund (Notiz e2e-scheduler-leader-flake): Ein per ``proc.kill()``
beendeter Supervisor liess seine Loop-Prozesse als Waisen zurueck. Die liefen
stundenlang weiter, erneuerten den Scheduler-Leader-Lock per Heartbeat und
liessen spaetere Leader-Tests in derselben Datenbank scheitern. Seit dem
Parent-Death-Signal im Kindziel (``_run_worker_entry``) bekommt jedes Kind
SIGTERM, sobald der Supervisor stirbt, und drainiert.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
import uuid

import pytest

from tests.support.cli_process import terminate_worker_process
from tests.support.services import rabbitmq_amqp_url, valkey_db_url

pytestmark = [pytest.mark.timeout(120)]


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    # Zombie zaehlt als tot: der Supervisor ist weg, niemand sammelt ihn ein.
    try:
        with open(f"/proc/{pid}/status", encoding="ascii") as fh:
            return "State:\tZ" not in fh.read()
    except OSError:
        return False


def _wait_until(predicate, timeout_s: float, what: str) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.2)
    pytest.fail(f"timed out waiting for {what}")


@pytest.mark.skipif(sys.platform != "linux", reason="PR_SET_PDEATHSIG gibt es nur unter Linux")
def test_workers_die_with_a_sigkilled_supervisor():
    queue = f"cli-orphan-{uuid.uuid4().hex[:8]}"
    env = dict(
        os.environ, SWARMQ_BROKER_URL=rabbitmq_amqp_url(), SWARMQ_BACKEND_URL=valkey_db_url(),
        CLI_TEST_QUEUE=queue, PYTHONUNBUFFERED="1",
    )
    proc = subprocess.Popen(
        [sys.executable, "-m", "swarmq.cli", "worker", "--module", "tests.integration._cli_fixture_tasks",
         "--queues", queue, "--processes", "2", "--log-format", "json"],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, start_new_session=True,
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
                    pids.append(int(json.loads(line[line.index("{"):])["pid"]))
                except (ValueError, KeyError):
                    continue
        return pids

    try:
        _wait_until(lambda: len(_spawned_pids()) >= 2, 60.0, "two worker subprocesses to spawn")
        _wait_until(
            lambda: sum(1 for line in lines if '"worker_started"' in line) >= 2,
            60.0, "both workers to start",
        )
        loop_pids = _spawned_pids()[:2]
        assert all(_alive(pid) for pid in loop_pids)

        proc.kill()  # SIGKILL: kein Shutdown-Event, kein SIGTERM vom Supervisor
        proc.wait(timeout=10)

        _wait_until(
            lambda: not any(_alive(pid) for pid in loop_pids),
            20.0, f"the loop processes {loop_pids} to die with their supervisor",
        )
    finally:
        terminate_worker_process(proc)
        # Sicherheitsnetz gegen genau die Waisen, die dieser Test ausschliessen soll.
        for pid in _spawned_pids():
            if _alive(pid):
                os.kill(pid, 9)
