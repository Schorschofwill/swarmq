"""CLI-Integration des CPU-Modus (Plan 2026-09-03-001, U7): ``swarmq worker --pool cpu``.

Startet den Worker als Subprozess (der Forkserver ist ein Singleton je
Prozess, KTD16), reiht einen Task ein und prueft Startlog und Ergebnis.

Plan 2026-09-08-001, U8 (R28, AE13): jeder Start laeuft mit ``RuntimeWarning``
als Fehler — ein Rechenkind, das das Hauptmodul per ``runpy`` erneut
ausfuehrt (B13), stirbt dann statt nur zu warnen.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import signal
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from tests.support.fixture_modules import ensure_fixture_registered
from tests.support.services import missing_services, rabbitmq_amqp_url, valkey_db_url

pytestmark = [
    pytest.mark.timeout(120),
    pytest.mark.skipif(bool(missing_services()), reason="RabbitMQ/Valkey nicht erreichbar"),
]

_REPO_ROOT = Path(__file__).resolve().parents[2]

#: Startwege des Workers: Paket-``__main__``, Konsolenskript, Anwender-Hauptmodul per ``python -m``.
_PYTHON_M_CLI = [sys.executable, "-W", "error::RuntimeWarning", "-m", "swarmq.cli"]
_PYTHON_M_USER_MAIN = [sys.executable, "-W", "error::RuntimeWarning", "-m", "tests.integration._cli_cpu_main"]


def _console_script() -> list[str] | None:
    exe = shutil.which("swarmq", path=str(Path(sys.executable).parent))
    return [exe] if exe else None


def _spawn_cpu_worker(queue: str, module: str, launcher: list[str]) -> subprocess.Popen[str]:
    env = dict(os.environ)
    env["SWARMQ_BROKER_URL"] = rabbitmq_amqp_url()
    env["SWARMQ_BACKEND_URL"] = valkey_db_url()
    env["CPU_FIXTURE_QUEUE"] = queue
    env["PYTHONUNBUFFERED"] = "1"
    env["PYTHONWARNINGS"] = "error::RuntimeWarning"  # auch fuer das Konsolenskript ohne ``-W``
    env["PYTHONPATH"] = str(_REPO_ROOT)  # das Konsolenskript hat das Repo nicht im sys.path
    # Eigene Sitzung: bei einem Haenger muss die ganze Gruppe (Supervisor, Worker,
    # Forkserver, Rechenkinder) sterben, sonst halten Enkel die stdout-Pipe offen.
    return subprocess.Popen(
        [
            *launcher, "worker",
            "--module", module,
            "--queues", queue, "--pool", "cpu", "--concurrency", "2",
            "--burst", "--burst-idle-timeout", "3", "--log-format", "json",
        ],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, start_new_session=True,
    )


def _wait_for_exit(proc: subprocess.Popen[str]) -> str:
    try:
        output, _ = proc.communicate(timeout=90)
    except subprocess.TimeoutExpired:
        os.killpg(proc.pid, signal.SIGKILL)
        output, _ = proc.communicate(timeout=10)
        pytest.fail(f"cpu worker did not exit:\n{output[-4000:]}")
    return output


async def _enqueue(queue: str, module: str, task_name: str, *args: object, count: int = 1) -> list[str]:
    os.environ["CPU_FIXTURE_QUEUE"] = queue
    ensure_fixture_registered(module, task_name)

    app = SwarmQ(RabbitMQBroker(rabbitmq_amqp_url()), ValkeyBackend(valkey_db_url()), queues=[queue])
    async with app:
        await app.broker.declare_queue(queue)
        return [await app.schedule(task_name, *args, queue=queue) for _ in range(count)]


async def _fetch(queue: str, tids: list[str]) -> list[dict]:
    app = SwarmQ(RabbitMQBroker(rabbitmq_amqp_url()), ValkeyBackend(valkey_db_url()), queues=[queue])
    async with app:
        return [await app.get_result(tid, timeout=10.0) for tid in tids]


@pytest.mark.parametrize(
    "launcher",
    [
        pytest.param(_PYTHON_M_CLI, id="python-m-swarmq.cli"),
        pytest.param(_console_script(), id="console-script"),
    ],
)
def test_cpu_pool_worker_processes_a_task_and_exits_clean(launcher):
    """U7-Abnahme; U8 (R28): unter ``RuntimeWarning``-als-Fehler weiterhin gruen."""
    if launcher is None:
        pytest.skip("Konsolenskript 'swarmq' liegt nicht neben dem Interpreter")
    queue = f"cli-cpu-{uuid.uuid4().hex[:8]}"
    module = "tests.e2e._cpu_fixture_tasks"
    (tid,) = asyncio.run(_enqueue(queue, module, "cpu_echo", 21))
    proc = _spawn_cpu_worker(queue, module, launcher)
    output = _wait_for_exit(proc)
    assert proc.returncode == 0, output[-4000:]
    assert "RuntimeWarning" not in output, output[-4000:]
    assert "compute_pool_started" in output
    assert '"slots": 2' in output or "slots=2" in output
    (result,) = asyncio.run(_fetch(queue, [tid]))
    assert result["value"] == 42


def test_python_m_user_main_runs_without_runtime_warning_and_without_second_main_import():
    """Covers AE13 (R28, KTD10): ``python -m paket.modul`` als Worker-Start, zehn Tasks in
    frischen Rechenkindern, keine ``RuntimeWarning``, Hauptmodul im Kind nicht erneut ausgefuehrt."""
    queue = f"cli-cpu-{uuid.uuid4().hex[:8]}"
    module = "tests.integration._cli_cpu_fixture_tasks"
    tids = asyncio.run(_enqueue(queue, module, "cpu_main_probe", count=10))
    proc = _spawn_cpu_worker(queue, module, _PYTHON_M_USER_MAIN)
    output = _wait_for_exit(proc)
    assert proc.returncode == 0, output[-4000:]
    assert "RuntimeWarning" not in output, output[-4000:]
    assert "found in sys.modules after import of package" not in output, output[-4000:]
    results = asyncio.run(_fetch(queue, tids))
    assert len(results) == 10
    assert len({r["pid"] for r in results}) == 10, "jeder Task laeuft in einem frischen Rechenkind"
    for probe in results:
        assert probe["main_spec_name"] == "tests.integration._cli_cpu_main", probe
        assert probe["main_is_module"] is True, probe
        assert probe["executions"] == 1, f"Hauptmodul im Rechenkind erneut ausgefuehrt: {probe}"
