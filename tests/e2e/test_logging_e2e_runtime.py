"""E2E: Logging-Laufzeitpfade mit echten Prozessen, RabbitMQ und Valkey
(Plan 2026-10-06-0831, R5, R6, R14, KTD9, KTD10).

Bisher nur per Unit-Test abgedeckt; hier laufen ``swarmq worker`` und eine
eingebettete App als echte Subprozesse.

Test-Liste:
- [x] Hot-Reload mit inzwischen ungueltiger ``--log-config``:
      ``log_config_reload_failed``, kein Exit 78, Worker laufen weiter und
      arbeiten einen neuen Task ab, die alte Logging-Konfiguration bleibt
      aktiv; nach dem Reparieren laeuft der Reload (``execvpe``) durch und die
      neue Konfiguration gilt (KTD9)
- [x] Opt-in ``log_queue=True`` in einer eingebetteten App: swarmq-Zeilen
      erreichen die Handler der App aus dem Thread ``swarmq-log-lsnr``,
      je genau einmal, Root-Handler/-Level unveraendert, nichts geht beim
      Verlassen von ``async with`` verloren, ``swarmq`` wird wiederhergestellt
      (R5, R6, KTD10)
- [x] Ohne Opt-in: Zeilen kommen aus dem Aufrufer-Thread, ``SwarmQ()``
      beruehrt Root-Handler und -Level nicht (R1, KTD10)
- [x] ``swarmq worker --check`` mit ungueltigen Logging-Einstellungen:
      ok:false mit ``error_type``, Exit 1; gueltige Datei ok:true; ohne
      erreichbaren Broker (R14)
- [x] ``--check`` mit einer Datei, an der ``dictConfig`` scheitert (Tippfehler
      in der Handler-Klasse), meldet ok:false wie der Start Exit 78 (R14, AE4)
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
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from tests.support.cli_process import terminate_worker_process
from tests.support.fixture_modules import ensure_fixture_registered

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_MODULE = "tests.integration._cli_fixture_tasks"
APP_MODULE = "tests.e2e._logging_runtime_app"
# Der Reload-Test startet das Konsolen-Skript wie im Betrieb. Der Reload ist
# ein ``execvpe`` mit ``sys.argv``: nach ``python -m swarmq.cli`` stuende dort
# ``swarmq/cli/__main__.py`` -- ohne Arbeitsverzeichnis in ``sys.path`` und
# ohne ``__main__``-Guard fuer die spawn-Kinder.
SWARMQ_SCRIPT = Path(sys.executable).parent / "swarmq"
CHECK_VALIDATION_EXIT_CODE = 1  # wie alle --check-Validierungsfehler

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(120)]


# ---------------------------------------------------------------------------
# Helfer
# ---------------------------------------------------------------------------


class _LineCollector:
    """Liest stdout eines Subprozesses in einem Thread mit; ``wait_for``
    wartet begrenzt auf eine passende Zeile, ohne den Test zu blockieren."""

    def __init__(self, proc: subprocess.Popen[str]) -> None:
        self.lines: list[str] = []
        self._cond = threading.Condition()
        self._eof = False
        self._thread = threading.Thread(target=self._pump, args=(proc,), name="e2e-log-pump", daemon=True)
        self._thread.start()

    def _pump(self, proc: subprocess.Popen[str]) -> None:
        assert proc.stdout is not None
        for line in proc.stdout:
            with self._cond:
                self.lines.append(line.rstrip("\n"))
                self._cond.notify_all()
        with self._cond:
            self._eof = True
            self._cond.notify_all()

    def wait_for(self, predicate: Callable[[str], bool], *, start: int = 0, timeout: float = 30.0) -> int:
        """Index der ersten passenden Zeile ab ``start``; AssertionError nach
        ``timeout`` oder wenn der Prozess vorher endet."""
        deadline = time.monotonic() + timeout
        with self._cond:
            while True:
                for index in range(start, len(self.lines)):
                    if predicate(self.lines[index]):
                        return index
                remaining = deadline - time.monotonic()
                if self._eof or remaining <= 0:
                    raise AssertionError(
                        "expected log line not seen "
                        f"({'process ended' if self._eof else 'timeout'}); tail:\n" + "\n".join(self.lines[-40:])
                    )
                self._cond.wait(timeout=remaining)

    def snapshot(self) -> list[str]:
        with self._cond:
            return list(self.lines)

    def join(self, timeout: float = 10.0) -> None:
        self._thread.join(timeout=timeout)


def _log_config(tag: str, *, handler_class: str = "logging.StreamHandler") -> str:
    """dictConfig mit erkennbarem Praefix je Konfigurationsstand."""
    return json.dumps(
        {
            "version": 1,
            "formatters": {"plain": {"format": f"{tag}|%(name)s|%(message)s"}},
            "handlers": {
                "err": {"class": handler_class, "stream": "ext://sys.stderr", "formatter": "plain"},
            },
            "root": {"level": "INFO", "handlers": ["err"]},
        }
    )


def _worker_env(queue: str) -> dict[str, str]:
    from tests.support.services import rabbitmq_amqp_url, valkey_db_url

    env = dict(os.environ)
    for key in ("SWARMQ_LOG_FORMAT", "SWARMQ_LOG_JSON", "SWARMQ_LOG_CONFIG", "SWARMQ_CONFIGURE_LOGGING"):
        env.pop(key, None)
    env["SWARMQ_BROKER_URL"] = rabbitmq_amqp_url()
    env["SWARMQ_BACKEND_URL"] = valkey_db_url()
    env["CLI_TEST_QUEUE"] = queue
    env["PYTHONUNBUFFERED"] = "1"
    # Wie in einer Installation: das Task-Modul ist ueber PYTHONPATH
    # importierbar, nicht ueber das Arbeitsverzeichnis (siehe SWARMQ_SCRIPT).
    env["PYTHONPATH"] = os.pathsep.join(filter(None, [str(REPO_ROOT), env.get("PYTHONPATH")]))
    return env


async def _echo_roundtrip(queue: str, value: str) -> str:
    from tests.support.services import rabbitmq_amqp_url, valkey_db_url

    os.environ["CLI_TEST_QUEUE"] = queue
    ensure_fixture_registered(FIXTURE_MODULE, "cli_echo")
    app = SwarmQ(RabbitMQBroker(rabbitmq_amqp_url()), ValkeyBackend(valkey_db_url()), queues=[queue])
    async with app:
        task_id = await app.schedule("cli_echo", value, queue=queue)
        return await app.get_result(task_id, timeout=20)


_ReloadWorker = tuple[subprocess.Popen[str], _LineCollector, Path, str]


@pytest.fixture
def reload_worker(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[_ReloadWorker]:
    """``swarmq worker --reload --log-config cfg.json`` mit eigenem, leerem
    Watch-Verzeichnis; Teardown ueber die Prozessgruppe (keine Waisen)."""
    monkeypatch.setenv("CLI_TEST_QUEUE", "unused")  # _echo_roundtrip setzt es; nach dem Test zurueck
    queue = f"logrt-reload-{uuid.uuid4().hex[:10]}"
    cfg = tmp_path / "cfg.json"
    cfg.write_text(_log_config("CFGV1"))
    watch_dir = tmp_path / "watch"
    watch_dir.mkdir()
    proc = subprocess.Popen(
        [
            str(SWARMQ_SCRIPT),
            "worker",
            "--module",
            FIXTURE_MODULE,
            "--queues",
            queue,
            "--concurrency",
            "2",
            "--reload",
            "--watch-dir",
            str(watch_dir),
            "--log-config",
            str(cfg),
        ],
        cwd=str(REPO_ROOT),
        env=_worker_env(queue),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )
    collector = _LineCollector(proc)
    try:
        yield proc, collector, cfg, queue
    finally:
        terminate_worker_process(proc)
        collector.join()


# ---------------------------------------------------------------------------
# 1. Hot-Reload mit ungueltig gewordener --log-config (KTD9)
# ---------------------------------------------------------------------------


def test_reload_with_invalid_log_config_keeps_running_then_reloads_after_fix(reload_worker) -> None:
    proc, out, cfg, queue = reload_worker

    # Der SIGHUP-Handler steht erst nach dem Start der Kinder (_setup_signals
    # nach _start_all_workers); ``hot_reload_enabled`` kommt danach.
    out.wait_for(lambda line: line == "CFGV1|swarmq.worker.lifecycle|worker_started")
    armed = out.wait_for(lambda line: line.endswith("|hot_reload_enabled"))
    assert out.lines[armed].startswith("CFGV1|")

    # Ungueltig: Tippfehler in der Handler-Klasse; ein neuer Praefix verriete
    # einen faelschlich angewendeten Stand.
    cfg.write_text(_log_config("CFGV2", handler_class="logging.StreamHandlr"))
    proc.send_signal(signal.SIGHUP)
    failed = out.wait_for(lambda line: "log_config_reload_failed" in line, start=armed)
    assert out.lines[failed] == "CFGV1|swarmq.process_manager.reload|log_config_reload_failed"

    # Worker laufen weiter und arbeiten einen neuen Task ab -- mit der alten
    # Logging-Konfiguration.
    assert asyncio.run(_echo_roundtrip(queue, "after-failed-reload")) == "echoed:after-failed-reload"
    out.wait_for(lambda line: line == "CFGV1|swarmq.task|task_succeeded", start=failed)
    assert proc.poll() is None, f"supervisor exited with {proc.returncode}"
    seen = out.snapshot()
    assert not [line for line in seen if line.startswith("CFGV2|")], seen[-40:]
    for forbidden in ("process_manager_reload_requested", "process_manager_reload_execvp", "worker_config_error"):
        assert not [line for line in seen if forbidden in line], (forbidden, seen[-40:])
    assert sum(line.endswith("|worker_started") for line in seen) == 1, seen[-40:]

    # Repariert: der Reload laeuft durch (execvpe), danach gilt die neue Datei.
    cfg.write_text(_log_config("CFGV2"))
    proc.send_signal(signal.SIGHUP)
    execvp = out.wait_for(lambda line: line.endswith("|process_manager_reload_execvp"), start=failed)
    assert out.lines[execvp] == "CFGV1|swarmq.process_manager.reload|process_manager_reload_execvp"
    restarted = out.wait_for(lambda line: line == "CFGV2|swarmq.worker.lifecycle|worker_started", start=execvp)
    out.wait_for(lambda line: line == "CFGV2|swarmq.process_manager.supervisor|hot_reload_enabled", start=execvp)
    assert asyncio.run(_echo_roundtrip(queue, "after-reload")) == "echoed:after-reload"
    out.wait_for(lambda line: line == "CFGV2|swarmq.task|task_succeeded", start=restarted)
    assert proc.poll() is None

    proc.send_signal(signal.SIGTERM)
    assert proc.wait(timeout=30) == 0, out.snapshot()[-40:]


# ---------------------------------------------------------------------------
# 2. Eingebettete App: Opt-in-Queue und Standard (R5, R6, KTD10)
# ---------------------------------------------------------------------------


def _run_embedded_app(mode: str) -> dict[str, Any]:
    from tests.support.services import rabbitmq_amqp_url, valkey_db_url

    queue = f"logrt-app-{mode}-{uuid.uuid4().hex[:10]}"
    env = dict(os.environ)
    env.pop("SWARMQ_LOG_QUEUE", None)
    proc = subprocess.run(
        [sys.executable, "-m", APP_MODULE, mode, queue, rabbitmq_amqp_url(), valkey_db_url()],
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert proc.returncode == 0, proc.stdout[-3000:] + proc.stderr[-3000:]
    # Die App schreibt alles in ihren eigenen Handler; auf stderr landet
    # nichts (kein lastResort, kein "--- Logging error ---").
    assert proc.stderr == "", proc.stderr[-3000:]
    [summary_line] = proc.stdout.splitlines()
    summary: dict[str, Any] = json.loads(summary_line)
    return summary


def _parse(lines: list[str]) -> list[dict[str, str]]:
    keys = ("handler_thread", "record_thread", "logger", "event", "task_id")
    return [dict(zip(keys, line.split("|"), strict=True)) for line in lines]


def _assert_root_untouched(summary: dict[str, Any]) -> None:
    for stage in ("after_init", "after_exit"):
        for key in ("root_handlers", "root_handler_types", "root_level"):
            assert summary[stage][key] == summary["before"][key], (stage, key, summary)


def _assert_swarmq_restored(summary: dict[str, Any]) -> None:
    after = summary["after_exit"]
    assert after["swarmq_handlers"] == []
    assert after["swarmq_propagate"] is True
    assert after["swarmq_level"] == summary["before"]["swarmq_level"]


def _assert_tasks_and_probes(records: list[dict[str, str]], summary: dict[str, Any]) -> None:
    from tests.e2e._logging_runtime_app import PROBE_COUNT

    assert summary["results"] == [1, 2, 3]
    succeeded = [r["task_id"] for r in records if r["logger"] == "swarmq.task" and r["event"] == "task_succeeded"]
    assert sorted(succeeded) == sorted(summary["task_ids"]), succeeded  # je genau einmal
    probes = [r["event"] for r in records if r["logger"] == "swarmq.e2e_probe"]
    # Nichts geht beim Verlassen verloren, nichts doppelt, Reihenfolge bleibt.
    assert probes == [f"exit_probe_{seq}" for seq in range(PROBE_COUNT)]
    lines = ["|".join(r.values()) for r in records]
    assert len(lines) == len(set(lines)), "a record reached the app's handler twice"


def test_embedded_app_with_log_queue_hands_swarmq_records_to_app_handlers_off_thread() -> None:
    summary = _run_embedded_app("queue")
    records = _parse(summary["lines"])

    swarmq_records = [r for r in records if r["logger"].startswith("swarmq")]
    assert swarmq_records == records, "only swarmq.* records expected in the app's handler"
    # Der Handler der App laeuft im Listener-Thread; erzeugt wurde der Record
    # im Aufrufer (``%(threadName)s`` = Erzeuger, stdlib-Semantik).
    assert {r["handler_thread"] for r in records} == {"swarmq-log-lsnr"}, records[:5]
    assert {r["record_thread"] for r in records} == {"MainThread"}
    # ``log_level="INFO"`` gilt am Logger ``swarmq``, obwohl Root auf WARNING steht.
    assert any(r["event"] == "worker_started" for r in records)
    _assert_tasks_and_probes(records, summary)

    _assert_root_untouched(summary)
    # Lazy: kein Listener-Thread nach ``SwarmQ()`` (AE6), keiner mehr nach dem Verlassen.
    assert "swarmq-log-lsnr" not in summary["after_init"]["threads"]
    assert "swarmq-log-lsnr" not in summary["after_exit"]["threads"]
    assert summary["after_init"]["swarmq_propagate"] is False
    _assert_swarmq_restored(summary)


def test_embedded_app_without_log_queue_logs_in_caller_thread_and_leaves_logging_alone() -> None:
    summary = _run_embedded_app("default")
    records = _parse(summary["lines"])

    assert {r["handler_thread"] for r in records} == {"MainThread"}, records[:5]
    assert {r["record_thread"] for r in records} == {"MainThread"}
    assert any(r["event"] == "worker_started" for r in records)
    _assert_tasks_and_probes(records, summary)

    _assert_root_untouched(summary)
    for stage in ("after_init", "after_exit"):
        assert summary[stage]["swarmq_handlers"] == [], stage
        assert summary[stage]["swarmq_propagate"] is True, stage
        assert summary[stage]["swarmq_level"] == summary["before"]["swarmq_level"], stage
        assert "swarmq-log-lsnr" not in summary[stage]["threads"], stage


# ---------------------------------------------------------------------------
# 3. swarmq worker --check mit Logging-Einstellungen (R14)
# ---------------------------------------------------------------------------


def _run_check(*args: str) -> tuple[int, dict[str, Any]]:
    env = dict(os.environ)
    for key in (
        "SWARMQ_LOG_FORMAT",
        "SWARMQ_LOG_JSON",
        "SWARMQ_LOG_CONFIG",
        "SWARMQ_CONFIGURE_LOGGING",
        "SWARMQ_LOG_QUEUE_SIZE",
        "SWARMQ_LOGGER_LEVELS",
    ):
        env.pop(key, None)
    # Kein Broker noetig: unerreichbare Adressen, --check fasst sie nicht an.
    env["SWARMQ_BROKER_URL"] = "amqp://guest:guest@127.0.0.1:1/"
    env["SWARMQ_BACKEND_URL"] = "valkey://127.0.0.1:1/0"
    env["CLI_TEST_QUEUE"] = "logrt-check"
    proc = subprocess.run(
        [sys.executable, "-m", "swarmq.cli", "worker", "--module", FIXTURE_MODULE, "--check", *args],
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    [line] = proc.stdout.splitlines()
    return proc.returncode, json.loads(line)


@pytest.fixture
def valid_cfg(tmp_path: Path) -> Path:
    cfg = tmp_path / "valid.json"
    cfg.write_text(_log_config("CHECK"))
    return cfg


@pytest.mark.parametrize(
    ("args", "error_fragment"),
    [
        pytest.param(lambda d, v: ["--log-config", str(d / "missing.json")], "FileNotFoundError", id="missing-file"),
        pytest.param(lambda d, v: ["--log-config", str(v), "--log-format", "json"], "--log-format", id="config+format"),
        pytest.param(lambda d, v: ["--log-queue-size", "0"], "--log-queue-size", id="queue-size-0"),
        pytest.param(lambda d, v: ["--logger-level", "aio_pika"], "NAME=LEVEL", id="logger-level-without-eq"),
    ],
)
def test_check_reports_invalid_logging_settings(tmp_path: Path, valid_cfg: Path, args, error_fragment: str) -> None:
    rc, payload = _run_check(*args(tmp_path, valid_cfg))
    assert rc == CHECK_VALIDATION_EXIT_CODE, payload
    assert payload["ok"] is False
    assert payload["error_type"] == "ConfigurationError"
    assert error_fragment in payload["error"]


def test_check_accepts_a_valid_log_config(valid_cfg: Path) -> None:
    rc, payload = _run_check("--log-config", str(valid_cfg))
    assert rc == 0, payload
    assert payload["ok"] is True
    assert payload["error"] is None and payload["error_type"] is None


def test_check_reports_a_log_config_that_dictconfig_rejects(tmp_path: Path) -> None:
    """R14/AE4: Dieselbe Datei beendet den Start mit Exit 78
    (``test_invalid_log_config_is_one_structured_line_and_exit_78``); ``--check``
    muss sie ebenfalls melden, sonst verspricht ok:true einen Start, der
    scheitert."""
    bad = tmp_path / "bad.json"
    bad.write_text(_log_config("CHECK", handler_class="logging.StreamHandlr"))
    rc, payload = _run_check("--log-config", str(bad))
    assert rc == CHECK_VALIDATION_EXIT_CODE, payload
    assert payload["ok"] is False
    assert payload["error_type"] is not None
    assert "StreamHandlr" in payload["error"] or "Unable to configure handler" in payload["error"]
