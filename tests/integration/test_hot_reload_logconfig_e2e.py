"""Hot-Reload mit ``--log-config`` ueber echte Prozesse (Plan 2026-10-06-1811, U8).

Die Pruefung der Logging-Datei lief frueher synchron im SIGHUP-Handler
(Analyse S6): Der Supervisor fror waehrend der Probe ein, ein SIGTERM ging
verloren, und ein SIGHUP kurz nach einem SIGTERM startete ihn neu statt ihn zu
beenden. Vorlage ist ``drive_reload.py`` der Analyse, Szenarien A-E. Eine
langsame Probe kommt aus ``_slow_log_handler.SlowHandler``: Die Flag-Datei
legt der Test erst unmittelbar vor dem SIGHUP an, so schlaeft nur die Probe.
"""

# Test list (U8, echte Prozesse):
# - [x] Langsame Probe, waehrenddessen getoeteter Worker: der Ersatz kommt,
#       bevor die Probe endet (Szenario A/D)
# - [x] SIGTERM waehrend der Probe: Rueckgabecode 0, kein zweiter
#       ``process_manager_start``, die Probe ist beendet (Szenario B)
# - [x] SIGTERM, dann SIGHUP: Ende ohne Neustart (Szenario E)
# - [x] SIGTERM, waehrend die Worker fuer einen beschlossenen Reload stoppen:
#       Ende ohne Neustart
# Echte Probe (aus ``tests/unit/process_manager/test_reload.py`` verschoben,
# Review B3: sie starten einen Interpreter):
# - [x] Die echte Probe meldet einen Handler-Tippfehler als Fehlertext
# - [x] Ohne Fabrik prueft die echte Probe die Datei

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path

import psutil
import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from tests.support.cli_process import terminate_worker_process
from tests.support.fixture_modules import ensure_fixture_registered
from tests.support.services import rabbitmq_amqp_url, valkey_db_url

REPO_ROOT = Path(__file__).resolve().parents[2]
_TASKS = "tests.integration._reload_fixture_tasks"

pytestmark = [pytest.mark.integration, pytest.mark.timeout(120)]


@dataclass
class _Supervisor:
    proc: subprocess.Popen[str]
    flag: Path
    queue: str
    lines: list[str] = field(default_factory=list)

    def count(self, event: str, start: int = 0) -> int:
        return sum(1 for line in self.lines[start:] if line.rstrip("\n").endswith(f"|{event}"))

    def wait_for(self, predicate: Callable[[], bool], timeout_s: float, what: str) -> None:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.05)
        pytest.fail(f"timed out waiting for {what}; output tail:\n{''.join(self.lines[-40:])}")

    def slow_probe(self, seconds: float) -> Path:
        """Die naechste Probe schlaeft ``seconds``; liefert die Datei, die
        nach dem Verbrauch existiert."""
        target = self.flag.with_name(self.flag.name + ".1")
        target.write_text(str(seconds))
        return target.with_name(target.name + ".used")

    def children(self, marker: str) -> list[int]:
        # psutil statt pgrep: das CI-Image hat kein procps.
        return [child.pid for child in _live_children(self.proc.pid) if marker in _cmdline(child)]


def _log_config(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "formatters": {"plain": {"format": "RLC|%(name)s|%(message)s"}},
                "handlers": {
                    "out": {
                        "class": "tests.integration._slow_log_handler.SlowHandler",
                        "stream": "ext://sys.stdout",
                        "formatter": "plain",
                    }
                },
                "root": {"level": "INFO", "handlers": ["out"]},
            }
        )
    )


@pytest.fixture
def supervisor(tmp_path: Path) -> Iterator[_Supervisor]:
    pytest.importorskip("watchfiles")
    queue = f"reload-lc-{uuid.uuid4().hex[:8]}"
    watch = tmp_path / "watch"
    watch.mkdir()
    cfg = tmp_path / "log.json"
    _log_config(cfg)
    flag = tmp_path / "slow"
    env = dict(
        os.environ,
        SWARMQ_BROKER_URL=rabbitmq_amqp_url(),
        SWARMQ_BACKEND_URL=valkey_db_url(),
        CLI_TEST_QUEUE=queue,
        PYTHONUNBUFFERED="1",
        PYTHONPATH=str(REPO_ROOT),
        SLOW_LOG_FLAG=str(flag),
        SWARMQ_LOG_LIFECYCLE_LEVEL="INFO",  # ``task_started`` als Marke
    )
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "swarmq.cli",
            "worker",
            "--module",
            _TASKS,
            "--queues",
            queue,
            "--processes",
            "1",
            "--reload",
            "--watch-dir",
            str(watch),
            "--log-config",
            str(cfg),
        ],
        cwd=str(REPO_ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )
    sup = _Supervisor(proc=proc, flag=flag, queue=queue)

    def _pump() -> None:
        assert proc.stdout is not None
        for line in proc.stdout:
            sup.lines.append(line)

    pump = threading.Thread(target=_pump, daemon=True)
    pump.start()
    try:
        # hot_reload_enabled faellt erst nach _setup_signals: ab dann ist der
        # SIGHUP-Handler installiert.
        sup.wait_for(
            lambda: sup.count("worker_started") >= 1 and sup.count("hot_reload_enabled") >= 1,
            60.0,
            "first worker start with armed reload",
        )
        yield sup
    finally:
        terminate_worker_process(proc, grace_s=20)
        pump.join(timeout=5)
        _assert_no_leftovers(proc.pid)


def _assert_no_leftovers(supervisor_pid: int) -> None:
    """Weder Worker (Sitzung des Supervisors) noch eine Probe (eigene Sitzung)
    ueberleben den Supervisor."""
    # Die Probe ist ``python -c "\nimport json, logging.config, sys ..."``.
    probe = re.compile(r"^\S+ -c .?import json, logging\.config")
    leftovers = []
    for proc in psutil.process_iter():
        try:
            in_session = os.getsid(proc.pid) == supervisor_pid
        except OSError:
            in_session = False
        cmdline = _cmdline(proc)
        if (in_session or probe.match(cmdline)) and _alive(proc):
            leftovers.append((proc.pid, cmdline))
    assert not leftovers, leftovers


def _alive(proc: psutil.Process) -> bool:
    """Ein beendeter Prozess, den init noch nicht abgeholt hat (Zombie), oder
    einer, der beim Lesen schon weg war, ueberlebt nichts (V0160-K4: in CI
    ``(924, '')``, tini raeumt ihn kurz darauf ab). Ein lebender Prozess mit
    unlesbarer Kommandozeile zaehlt weiter."""
    try:
        return proc.status() != psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        return False


def _live_children(pid: int) -> list[psutil.Process]:
    try:
        return psutil.Process(pid).children()
    except psutil.NoSuchProcess:
        return []


def _cmdline(proc: psutil.Process) -> str:
    try:
        return " ".join(proc.cmdline())
    except psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess:
        return ""


def _probe_pids(sup: _Supervisor) -> list[int]:
    return sup.children("dictConfig")


def test_worker_killed_during_a_slow_probe_is_replaced_before_the_probe_ends(supervisor) -> None:
    sup = supervisor
    used = sup.slow_probe(8.0)
    mark = len(sup.lines)
    sighup_at = time.monotonic()
    sup.proc.send_signal(signal.SIGHUP)
    sup.wait_for(used.exists, 10.0, "probe consumed the slow flag")
    workers = sup.children("spawn_main")
    assert workers, "no worker child found"
    for pid in workers:
        with contextlib.suppress(ProcessLookupError):
            os.kill(pid, signal.SIGKILL)

    sup.wait_for(lambda: sup.count("worker_started", mark) >= 1, 7.0, "replacement worker during the probe")
    assert time.monotonic() - sighup_at < 8.0
    assert _probe_pids(sup), "probe should still be running"
    assert sup.count("worker_died", mark) >= 1
    assert sup.count("process_manager_reload_requested", mark) == 0


def test_sigterm_during_the_probe_ends_the_supervisor_without_restart(supervisor) -> None:
    sup = supervisor
    used = sup.slow_probe(15.0)
    sup.proc.send_signal(signal.SIGHUP)
    sup.wait_for(used.exists, 10.0, "probe consumed the slow flag")
    sup.wait_for(lambda: bool(_probe_pids(sup)), 5.0, "running probe")
    probes = _probe_pids(sup)

    sent_at = time.monotonic()
    sup.proc.send_signal(signal.SIGTERM)
    assert sup.proc.wait(timeout=12) == 0, "".join(sup.lines[-40:])
    assert time.monotonic() - sent_at < 12.0
    time.sleep(0.2)  # Pump liest die letzten Zeilen
    assert sup.count("process_manager_start") == 1, "".join(sup.lines[-40:])
    assert sup.count("process_manager_reload_execvp") == 0
    assert sup.count("process_manager_shutdown_signal") == 1
    for pid in probes:
        assert not Path(f"/proc/{pid}").exists() or "dictConfig" not in Path(f"/proc/{pid}/cmdline").read_text()


def test_sigterm_then_sighup_ends_without_restart(supervisor) -> None:
    sup = supervisor
    sup.proc.send_signal(signal.SIGTERM)
    time.sleep(0.05)  # Szenario E: SIGHUP 50 ms nach SIGTERM
    sup.proc.send_signal(signal.SIGHUP)
    assert sup.proc.wait(timeout=30) == 0, "".join(sup.lines[-40:])
    time.sleep(0.2)
    assert sup.count("process_manager_start") == 1, "".join(sup.lines[-40:])
    assert sup.count("process_manager_reload_requested") == 0


async def _schedule_sleep(queue: str, seconds: float) -> None:
    os.environ["CLI_TEST_QUEUE"] = queue
    ensure_fixture_registered(_TASKS, "reload_sleep")
    app = SwarmQ(RabbitMQBroker(rabbitmq_amqp_url()), ValkeyBackend(valkey_db_url()), queues=[queue])
    async with app:
        await app.schedule("reload_sleep", seconds, queue=queue)


def test_sigterm_while_workers_stop_for_an_accepted_reload_ends_without_restart(supervisor) -> None:
    sup = supervisor
    mark = len(sup.lines)
    asyncio.run(_schedule_sleep(sup.queue, 6.0))
    sup.wait_for(lambda: sup.count("task_started", mark) >= 1, 15.0, "sleep task running")

    sup.proc.send_signal(signal.SIGHUP)  # schnelle, gueltige Probe
    sup.wait_for(lambda: sup.count("process_manager_reload_requested", mark) >= 1, 10.0, "accepted reload")
    sup.proc.send_signal(signal.SIGTERM)  # Worker drainieren noch den Task

    assert sup.proc.wait(timeout=40) == 0, "".join(sup.lines[-40:])
    time.sleep(0.2)
    tail = "".join(sup.lines[mark:])
    assert sup.count("process_manager_start") == 1, tail
    assert sup.count("process_manager_reload_execvp") == 0, tail


def test_the_real_probe_reports_a_handler_typo() -> None:
    """AE4 beim Reload: Der Tippfehler faellt erst in ``dictConfig`` auf."""
    from swarmq.logging_file import DictConfigProbe

    probe = DictConfigProbe(
        json.dumps(
            {
                "version": 1,
                "handlers": {"err": {"class": "logging.StreamHandlr"}},
                "root": {"handlers": ["err"]},
            }
        )
    )
    assert probe.wait(timeout=5) == 1
    assert probe.poll() == 1
    assert "StreamHandlr" in probe.error()


def test_by_default_the_real_probe_checks_the_file(tmp_path: Path) -> None:
    from swarmq.process_manager.reload import ReloadController

    log_config = tmp_path / "log.json"
    log_config.write_text('{"version": 1}')
    ctl = ReloadController(str(log_config))
    ctl.request()
    deadline = time.monotonic() + 5
    while not ctl.step():
        assert ctl.busy
        assert time.monotonic() < deadline
        time.sleep(0.01)
