"""Hot-Reload mit echtem Re-Exec, gestartet als ``python -m swarmq.cli``.

``test_hot_reload_e2e.py`` mockt ``os.execvpe`` -- das Ziel des Re-Exec
prueft dort niemand. Der Supervisor startete sich frueher mit
``[sys.executable, *sys.argv]`` neu. Unter ``-m`` steht in ``sys.argv[0]``
aber der Pfad von ``swarmq/cli/__main__.py``; das neue Abbild lief diese
Datei als Skript: ohne PYTHONPATH fehlte das Arbeitsverzeichnis auf
``sys.path`` (``No module named 'tests'`` -> ``worker_config_error``), mit
PYTHONPATH scheiterten die gespawnten Worker im Bootstrapping, weil
``__main__.py`` keinen ``__name__``-Guard hatte. Der Konsolen-Einstieg
``swarmq`` war nie betroffen.

Dieser Test laesst den echten Re-Exec laufen: Start, SIGHUP, und danach
muessen wieder Worker starten und ein Task durchlaufen. Die Variante mit
dem Konsolen-Skript belegt, dass dieser Weg mit ``sys.orig_argv`` weiter
funktioniert (PYTHONPATH, damit das Fixture-Modul importierbar ist).
"""

from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from tests.support.cli_process import terminate_worker_process
from tests.support.fixture_modules import ensure_fixture_registered
from tests.support.services import rabbitmq_amqp_url, valkey_db_url

REPO_ROOT = Path(__file__).resolve().parents[2]

pytestmark = [pytest.mark.integration, pytest.mark.timeout(120)]


def _wait_until(predicate, timeout_s: float, what: str, lines: list[str]) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.1)
    pytest.fail(f"timed out waiting for {what}; output tail:\n{''.join(lines[-40:])}")


def _count(lines: list[str], event: str, start: int = 0) -> int:
    return sum(1 for line in lines[start:] if f'"{event}"' in line)


async def _schedule_and_get_result(queue: str, value: str) -> str:
    os.environ["CLI_TEST_QUEUE"] = queue
    ensure_fixture_registered("tests.integration._cli_fixture_tasks", "cli_echo")
    app = SwarmQ(RabbitMQBroker(rabbitmq_amqp_url()), ValkeyBackend(valkey_db_url()), queues=[queue])
    async with app:
        task_id = await app.schedule("cli_echo", value, queue=queue)
        return await app.get_result(task_id, timeout=15)


_PYTHON_M = [sys.executable, "-m", "swarmq.cli"]
# Konsolen-Einstieg: sys.orig_argv ist dort [python3, .../bin/swarmq, ...] --
# der Re-Exec-Vektor bleibt derselbe wie frueher mit sys.argv.
_CONSOLE_SCRIPT = [str(Path(sys.executable).parent / "swarmq")]


@pytest.mark.parametrize(
    ("launcher", "pythonpath"),
    [(_PYTHON_M, None), (_PYTHON_M, str(REPO_ROOT)), (_CONSOLE_SCRIPT, str(REPO_ROOT))],
    ids=["python-m-ohne-pythonpath", "python-m-mit-pythonpath", "konsolen-skript"],
)
def test_sighup_reexec_starts_workers_again(
    tmp_path: Path,
    launcher: list[str],
    pythonpath: str | None,
) -> None:
    pytest.importorskip("watchfiles")
    queue = f"reload-m-{uuid.uuid4().hex[:8]}"
    env = dict(
        os.environ,
        SWARMQ_BROKER_URL=rabbitmq_amqp_url(),
        SWARMQ_BACKEND_URL=valkey_db_url(),
        CLI_TEST_QUEUE=queue,
        PYTHONUNBUFFERED="1",
    )
    env.pop("PYTHONPATH", None)
    if pythonpath is not None:
        env["PYTHONPATH"] = pythonpath
    proc = subprocess.Popen(
        [
            *launcher,
            "worker",
            "--module",
            "tests.integration._cli_fixture_tasks",
            "--queues",
            queue,
            "--processes",
            "1",
            "--reload",
            "--watch-dir",
            str(tmp_path),
            "--log-format",
            "json",
        ],
        cwd=str(REPO_ROOT),
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
            lines.append(line)  # noqa: PERF402 - jede Zeile muss sofort sichtbar sein, waehrend der Test parallel auf sie wartet

    threading.Thread(target=_pump, daemon=True).start()
    try:
        # hot_reload_enabled faellt erst nach _setup_signals -- ab dann ist
        # der SIGHUP-Handler sicher installiert.
        _wait_until(
            lambda: _count(lines, "worker_started") >= 1 and _count(lines, "hot_reload_enabled") >= 1,
            60.0,
            "first worker start with armed reload",
            lines,
        )
        proc.send_signal(signal.SIGHUP)
        # Marke ist der zweite process_manager_start, nicht
        # process_manager_reload_execvp: die Logs laufen ueber eine
        # QueueListener-Queue, und execvpe ersetzt das Abbild, bevor der
        # Listener diese letzte Zeile schreibt.
        _wait_until(
            lambda: _count(lines, "process_manager_start") >= 2 or proc.poll() is not None,
            30.0,
            "process_manager_start of the re-exec'd supervisor",
            lines,
        )
        starts = [i for i, line in enumerate(lines) if '"process_manager_start"' in line]
        assert len(starts) >= 2, f"no re-exec; rc={proc.returncode}:\n{''.join(lines[-40:])}"
        mark = starts[1]
        _wait_until(
            lambda: _count(lines, "worker_started", mark) >= 1 or proc.poll() is not None,
            60.0,
            "worker_started after re-exec",
            lines,
        )
        tail = "".join(lines[mark:])
        assert proc.poll() is None, f"supervisor exited after re-exec rc={proc.returncode}:\n{tail}"
        assert _count(lines, "worker_started", mark) >= 1, tail
        assert "worker_config_error" not in tail
        assert "worker_restart_limit_exceeded" not in tail
        assert "bootstrapping phase" not in tail

        assert asyncio.run(_schedule_and_get_result(queue, "after-reload")) == "echoed:after-reload"
    finally:
        terminate_worker_process(proc)
