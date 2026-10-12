"""E2E: Logging-Einrichtung ueber echte Prozessbaeume (Plan 2026-10-06-0831, R9-R14,
KTD7-KTD9, KTD14).

Jeder Test startet ``swarmq worker`` als echten Subprozess (Supervisor mit
``--processes 2`` -> Worker-Subprozesse -> bei ``--pool cpu`` Forkserver und
Rechenkinder) gegen echtes RabbitMQ und Valkey. Die Tasks werden vorher
eingereiht, der Worker laeuft im Burst-Modus und endet von selbst; der
Teardown beendet die ganze Prozessgruppe (``terminate_worker_process``).

Test-Liste:
1. [x] ``--log-config`` (relativer Pfad, cwd=tmp_path) mit ``build_formatter``
       als JSON und QueueHandler: Supervisor, beide Worker und ein Rechenkind
       schreiben ueber den Formatter und Filter der Datei, jede Zeile genau
       einmal, ``task_succeeded`` fuer beide Tasks (R11, KTD9, KTD14).
2. [x] ``--no-configure-logging``: das App-Modul richtet beim Import sein
       Format ein -- swarmq-Events erscheinen darin mit ``%(task_id)s`` als
       Attribut, keine swarmq-JSON-Zeilen, kein ``logging_reconfigured_by_app``,
       der Task laeuft (R12, R2).
3. [x] CLI schlaegt Env in allen Prozessen: Env JSON+DEBUG, CLI human+INFO ->
       nur Textzeilen, keine DEBUG-Lebenszyklus-Events (R13, KTD7, AE5).
4. [x] Ohne CLI-Flags gilt das Env: alle Zeilen JSON, DEBUG-Events da (AE5).
5. [x] Ungueltiges Env (``SWARMQ_LOG_FORMAT=jsonl``), das die CLI
       ueberschreibt, bricht die Worker nicht mit 78 ab (R14, KTD7).
"""

from __future__ import annotations

import asyncio
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
_TASKS_MODULE = "tests.e2e._logging_fixture_tasks"
_APP_MODULE = "tests.e2e._logging_app_configures"
_RUN_TIMEOUT_S = 90.0


# --- Helfer ---------------------------------------------------------------


def _new_queue(prefix: str) -> str:
    return f"log-e2e-{prefix}-{uuid.uuid4().hex[:8]}"


async def _enqueue(
    rabbitmq_url: str,
    valkey_url: str,
    queue: str,
    calls: list[tuple[str, tuple[Any, ...]]],
) -> list[str]:
    # Das Fixture-Modul liest die Queue beim Import; die Registry wird nach
    # jedem Test geleert, ``ensure_fixture_registered`` laedt dann neu.
    previous = os.environ.get("LOG_E2E_QUEUE")
    os.environ["LOG_E2E_QUEUE"] = queue
    try:
        for name, _ in calls:
            ensure_fixture_registered(_TASKS_MODULE, name)
    finally:
        if previous is None:
            os.environ.pop("LOG_E2E_QUEUE", None)
        else:
            os.environ["LOG_E2E_QUEUE"] = previous
    app = SwarmQ(RabbitMQBroker(rabbitmq_url), ValkeyBackend(valkey_url), queues=[queue])
    async with app:
        await app.broker.declare_queue(queue)
        return [await app.schedule(name, *args, queue=queue) for name, args in calls]


async def _results(rabbitmq_url: str, valkey_url: str, queue: str, tids: list[str]) -> list[Any]:
    app = SwarmQ(RabbitMQBroker(rabbitmq_url), ValkeyBackend(valkey_url), queues=[queue])
    async with app:
        return [await app.get_result(tid, timeout=15.0) for tid in tids]


def _base_env(rabbitmq_url: str, valkey_url: str, queue: str) -> dict[str, str]:
    """Env ohne geerbte Logging-Einstellungen -- sonst entscheidet die Shell
    des Entwicklers ueber das Ergebnis."""
    env = {k: v for k, v in os.environ.items() if not k.startswith(("SWARMQ_LOG", "SWARMQ_CONFIGURE_LOGGING"))}
    env.update(
        {
            "SWARMQ_BROKER_URL": rabbitmq_url,
            "SWARMQ_BACKEND_URL": valkey_url,
            "LOG_E2E_QUEUE": queue,
            "PYTHONUNBUFFERED": "1",
            "PYTHONPATH": os.pathsep.join(filter(None, [str(_REPO_ROOT), os.environ.get("PYTHONPATH")])),
        }
    )
    return env


def _run_burst_worker(
    queue: str,
    module: str,
    *extra: str,
    env: dict[str, str],
    cwd: Path = _REPO_ROOT,
) -> tuple[int, str, int]:
    """``swarmq worker --burst`` bis zum Ende; liefert (rc, stderr, supervisor_pid).

    Eigene Sitzung, damit der Teardown die ganze Prozessgruppe trifft --
    verwaiste Worker mit Heartbeat haben frueher andere e2e-Tests gestoert.
    """
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "swarmq.cli",
            "worker",
            "--module",
            module,
            "--queues",
            queue,
            "--burst",
            "--burst-idle-timeout",
            "3",
            *extra,
        ],
        cwd=str(cwd),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        stdout, stderr = proc.communicate(timeout=_RUN_TIMEOUT_S)
    except subprocess.TimeoutExpired:
        terminate_worker_process(proc, grace_s=10.0)
        stdout, stderr = proc.communicate()
        pytest.fail(f"worker did not finish in {_RUN_TIMEOUT_S}s:\n{stderr[-4000:]}")
    finally:
        terminate_worker_process(proc, grace_s=10.0)
    return proc.returncode, stdout + stderr, proc.pid


def _lines(output: str) -> list[str]:
    return [line for line in output.splitlines() if line.strip()]


# --- 1. --log-config ------------------------------------------------------


def _write_log_config(path: Path) -> None:
    """Das Beispiel aus website/reference/logging.md plus Markierungsfilter."""
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "filters": {"mark": {"()": f"{_TASKS_MODULE}.MarkFilter"}},
                "formatters": {
                    "json": {"()": "swarmq.logging.build_formatter", "json": True, "colors": False},
                },
                "handlers": {
                    "stderr": {
                        "class": "logging.StreamHandler",
                        "stream": "ext://sys.stderr",
                        "formatter": "json",
                        "filters": ["mark"],
                    },
                    "queue": {"class": "logging.handlers.QueueHandler", "handlers": ["stderr"]},
                },
                "root": {"level": "INFO", "handlers": ["queue"]},
                "loggers": {"aio_pika": {"level": "WARNING"}, "aiormq": {"level": "WARNING"}},
            }
        )
    )


def test_relative_log_config_applies_in_supervisor_workers_and_compute_children(
    rabbitmq_url: str,
    valkey_url: str,
    tmp_path: Path,
) -> None:
    queue = _new_queue("cfg")
    tids = asyncio.run(
        _enqueue(
            rabbitmq_url,
            valkey_url,
            queue,
            [
                ("log_async_echo", ("a1",)),
                ("log_cpu_sum", (200_000,)),
            ],
        )
    )
    (tmp_path / "conf").mkdir()
    _write_log_config(tmp_path / "conf" / "logging.json")

    rc, output, supervisor_pid = _run_burst_worker(
        queue,
        _TASKS_MODULE,
        "--processes",
        "2",
        "--pool",
        "cpu",
        "--concurrency",
        "1",
        "--log-config",
        "conf/logging.json",
        env=_base_env(rabbitmq_url, valkey_url, queue),
        cwd=tmp_path,
    )
    assert rc == 0, output[-4000:]

    lines = _lines(output)
    # Jede Zeile kommt ueber Formatter und Filter der Datei -- keine
    # Zeile aus der eingebauten Einrichtung, kein Rohtext.
    entries = [json.loads(line) for line in lines]
    assert all(e.get("e2e_marker") == "from-log-config-file" for e in entries), output[-4000:]
    # Genau einmal: keine Zeile doppelt (je Prozess genau ein Handlerpfad).
    assert len(lines) == len(set(lines)), [line for line in lines if lines.count(line) > 1][:5]

    by_pid: dict[int, list[dict[str, Any]]] = {}
    for e in entries:
        by_pid.setdefault(e["e2e_pid"], []).append(e)

    # Supervisor
    assert supervisor_pid in by_pid, sorted(by_pid)
    assert any(e["logger"].startswith("swarmq.process_manager") for e in by_pid[supervisor_pid])

    # Zwei Worker, je genau ein worker_started aus dem eigenen Prozess
    started = [e for e in entries if e["event"] == "worker_started"]
    assert len(started) == 2, output[-4000:]
    worker_pids = {e["e2e_pid"] for e in started}
    assert len(worker_pids) == 2
    assert supervisor_pid not in worker_pids

    # Rechenkind: App-Zeilen aus dem Task, im Kind erzeugt, mit Kind-Kontext
    app_lines = [e for e in entries if e["event"] == "app_task_line"]
    assert sorted(e["tag"] for e in app_lines) == ["a1", "cpu-200000"], output[-4000:]
    for e in app_lines:
        assert e["e2e_pid"] not in worker_pids | {supervisor_pid}
        assert e["compute_child"] == e["e2e_pid"]
        assert e["task_id"] in tids

    # Lebenszyklus: task_succeeded fuer beide Tasks, je genau einmal
    succeeded = [e["task_id"] for e in entries if e["event"] == "task_succeeded"]
    assert sorted(succeeded) == sorted(tids), output[-4000:]

    results = asyncio.run(_results(rabbitmq_url, valkey_url, queue, tids))
    assert results[0]["tag"] == "a1"
    assert results[1]["total"] == sum(i * i for i in range(200_000))
    assert {r["pid"] for r in results} == {e["e2e_pid"] for e in app_lines}


# --- 2. --no-configure-logging --------------------------------------------


def test_no_configure_logging_leaves_the_app_format_in_every_worker(
    rabbitmq_url: str,
    valkey_url: str,
) -> None:
    queue = _new_queue("nocfg")
    [tid] = asyncio.run(_enqueue(rabbitmq_url, valkey_url, queue, [("log_async_echo", ("n1",))]))

    rc, output, _ = _run_burst_worker(
        queue,
        _APP_MODULE,
        "--processes",
        "2",
        "--no-configure-logging",
        env=_base_env(rabbitmq_url, valkey_url, queue),
    )
    assert rc == 0, output[-4000:]

    lines = _lines(output)
    assert not [line for line in lines if line.startswith("{")], output[-4000:]
    assert "logging_reconfigured_by_app" not in output
    app_lines = [line for line in lines if line.startswith("APPFMT|")]
    # swarmq-Events im Format der App, in beiden Workern
    started = [line for line in app_lines if "|worker_started|" in line]
    assert len(started) == 2, output[-4000:]
    assert len({line.split("|")[1] for line in started}) == 2
    # Felder als LogRecord-Attribute: task_id im Format der App
    assert f"|swarmq.task|INFO|task_succeeded|task_id={tid}" in output, output[-4000:]
    assert f"|e2e.app|WARNING|app_task_line|task_id={tid}" in output

    [result] = asyncio.run(_results(rabbitmq_url, valkey_url, queue, [tid]))
    assert result["tag"] == "n1"


# --- 3.-5. Prioritaet CLI > Env -------------------------------------------


def test_cli_format_and_level_beat_env_in_supervisor_and_workers(
    rabbitmq_url: str,
    valkey_url: str,
) -> None:
    queue = _new_queue("prio-cli")
    [tid] = asyncio.run(_enqueue(rabbitmq_url, valkey_url, queue, [("log_async_echo", ("c1",))]))
    env = _base_env(rabbitmq_url, valkey_url, queue)
    env.update({"SWARMQ_LOG_FORMAT": "json", "SWARMQ_LOG_LEVEL": "DEBUG"})

    rc, output, _ = _run_burst_worker(
        queue,
        _TASKS_MODULE,
        "--processes",
        "2",
        "--log-format",
        "human",
        "--log-level",
        "INFO",
        env=env,
    )
    assert rc == 0, output[-4000:]

    lines = _lines(output)
    assert not [line for line in lines if line.lstrip().startswith("{")], output[-4000:]
    assert sum("worker_started" in line for line in lines) == 2, output[-4000:]
    assert any("worker_spawned" in line or "supervisor" in line for line in lines), output[-4000:]
    assert any("task_succeeded" in line and tid in line for line in lines), output[-4000:]
    assert "task_received" not in output
    assert "task_started" not in output
    assert "[debug" not in output
    assert "\x1b[" not in output
    [result] = asyncio.run(_results(rabbitmq_url, valkey_url, queue, [tid]))
    assert result["tag"] == "c1"


def test_env_format_and_level_apply_in_all_processes_without_cli_flags(
    rabbitmq_url: str,
    valkey_url: str,
) -> None:
    queue = _new_queue("prio-env")
    [tid] = asyncio.run(_enqueue(rabbitmq_url, valkey_url, queue, [("log_async_echo", ("e1",))]))
    env = _base_env(rabbitmq_url, valkey_url, queue)
    env.update({"SWARMQ_LOG_FORMAT": "json", "SWARMQ_LOG_LEVEL": "DEBUG"})

    rc, output, _ = _run_burst_worker(queue, _TASKS_MODULE, "--processes", "2", env=env)
    assert rc == 0, output[-4000:]

    lines = _lines(output)
    entries = [json.loads(line) for line in lines]  # jede Zeile ist JSON
    started = [e for e in entries if e["event"] == "worker_started"]
    assert len(started) == 2, output[-4000:]
    assert any(e["logger"].startswith("swarmq.process_manager") for e in entries)
    lifecycle = [
        (e["event"], e["level"]) for e in entries if e.get("logger") == "swarmq.task" and e.get("task_id") == tid
    ]
    assert lifecycle == [
        ("task_received", "debug"),
        ("task_started", "debug"),
        ("task_succeeded", "info"),
    ], output[-4000:]
    [result] = asyncio.run(_results(rabbitmq_url, valkey_url, queue, [tid]))
    assert result["tag"] == "e1"


def test_invalid_env_format_overridden_by_cli_does_not_abort_workers(
    rabbitmq_url: str,
    valkey_url: str,
) -> None:
    queue = _new_queue("prio-bad")
    [tid] = asyncio.run(_enqueue(rabbitmq_url, valkey_url, queue, [("log_async_echo", ("b1",))]))
    env = _base_env(rabbitmq_url, valkey_url, queue)
    env["SWARMQ_LOG_FORMAT"] = "jsonl"

    rc, output, _ = _run_burst_worker(
        queue,
        _TASKS_MODULE,
        "--processes",
        "2",
        "--log-format",
        "json",
        env=env,
    )
    assert rc == 0, output[-4000:]

    entries = [json.loads(line) for line in _lines(output)]
    events = [e["event"] for e in entries]
    assert "worker_config_error" not in events, output[-4000:]
    assert events.count("worker_started") == 2, output[-4000:]
    assert any(e["event"] == "task_succeeded" and e.get("task_id") == tid for e in entries)
    [result] = asyncio.run(_results(rabbitmq_url, valkey_url, queue, [tid]))
    assert result["tag"] == "b1"
