"""Fork aus Task-Code im Datei-Modus, ueber echte Prozesse (Plan
2026-10-06-1811, U7; R8, KTD10).

Ein Prozess mit ``--log-config`` und ``QueueHandler`` vor einem
``FileHandler`` ruft selbst ``os.fork()`` auf, wie Task-Code im IO-Worker
oder Compute-Kind. Vorlagen sind ``repro5`` und ``repro5_proto`` der
Analyse (S5, R3). Vorher gingen die Zeilen des Kindes verloren: Sein
Listener schrieb zuerst die geerbte Queue-Kopie des Elternprozesses, und
jeder Schreibversuch scheiterte mit ``RuntimeError: reentrant call``, weil
der Elternprozess beim Fork mitten im Schreiben war. Ein ``faulthandler``
im Elternprozess und ein Alarm im Kind beenden haengende Prozesse mit Stack.
"""

# Test list (U7, echte Prozesse):
# - [x] Ein Fork: Die Datei enthaelt alle Zeilen des Kindes (mit seiner PID),
#       die Zeile des Elternprozesses genau einmal.
# - [x] Begrenzte Queue: kein ``Logging error`` auf stderr, die Zeilen des
#       Kindes kommen an.
# - [x] Fork unter Schreiblast, 20 Runden: kein ``RuntimeError: reentrant
#       call``, alle Zeilen aller Kinder in der Datei.

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.timeout(60)

_REPO = Path(__file__).resolve().parents[2]

_HANG_DEADLINE_S = 20
"""Frist des ``faulthandler`` im Elternprozess; danach Stack und Exit 1."""

_CHILD_DEADLINE_S = 5
"""Frist des Alarms im Kind; danach Stack und Exit 3."""

_SCRIPT = f"""
import faulthandler, json, logging, os, signal, sys, warnings
faulthandler.dump_traceback_later({_HANG_DEADLINE_S}, exit=True)
warnings.simplefilter("ignore", DeprecationWarning)  # fork mit Threads (3.14)

from swarmq.logging import configure_logging, shutdown_logging
from swarmq.logging_settings import LoggingSettings

log_file, maxsize, burst, rounds = sys.argv[1], *map(int, sys.argv[2:5])
queue_handler = {{"class": "logging.handlers.QueueHandler", "handlers": ["file"]}}
if maxsize:
    queue_handler["queue"] = {{"()": "queue.Queue", "maxsize": maxsize}}
config = {{
    "version": 1,
    "formatters": {{"p": {{"format": "%(process)d %(message)s"}}}},
    "handlers": {{
        "file": {{"class": "logging.FileHandler", "filename": log_file, "formatter": "p"}},
        "q": queue_handler,
    }},
    "root": {{"level": "INFO", "handlers": ["q"]}},
}}
configure_logging(LoggingSettings(log_config="/never/read.json", log_config_json=json.dumps(config)))
log = logging.getLogger("app")
log.info("parent_before_fork")


def on_alarm(*_):
    faulthandler.dump_traceback(all_threads=True)
    os._exit(3)


children = []
for _ in range(rounds):
    for i in range(burst):
        log.info("parent_burst_%d %s", i, "x" * 200)
    pid = os.fork()
    if pid == 0:
        signal.signal(signal.SIGALRM, on_alarm)
        signal.alarm({_CHILD_DEADLINE_S})
        for i in range(3):
            log.info("child_line_%d", i)
        shutdown_logging()
        os._exit(0)
    _, status = os.waitpid(pid, 0)
    children.append([pid, os.waitstatus_to_exitcode(status)])
shutdown_logging()
print(json.dumps(children))
"""


def _run(tmp_path: Path, *, maxsize: int = 0, burst: int = 0, rounds: int = 1) -> tuple[
    list[list[int]], list[str], str,
]:
    """Fuehrt das Skript aus: Kinder mit Exit-Code, Zeilen der Datei, stderr."""
    log_file = tmp_path / "out.log"
    proc = subprocess.run(
        [sys.executable, "-c", _SCRIPT, str(log_file), str(maxsize), str(burst), str(rounds)],
        cwd=_REPO, capture_output=True, text=True, timeout=_HANG_DEADLINE_S + 10,
    )
    assert proc.returncode == 0, proc.stderr
    children = json.loads(proc.stdout)
    return children, log_file.read_text().splitlines(), proc.stderr


def _child_lines(lines: list[str], pid: int) -> list[str]:
    return [line for line in lines if line.startswith(f"{pid} ")]


def test_the_child_writes_its_lines_and_no_inherited_ones(tmp_path: Path) -> None:
    children, lines, _ = _run(tmp_path)

    ((pid, code),) = children
    assert code == 0
    assert _child_lines(lines, pid) == [f"{pid} child_line_{i}" for i in range(3)]
    assert sum(line.endswith(" parent_before_fork") for line in lines) == 1


def test_a_bounded_queue_logs_no_error_in_the_child(tmp_path: Path) -> None:
    children, lines, stderr = _run(tmp_path, maxsize=5, burst=5)

    ((pid, code),) = children
    assert code == 0
    assert "Logging error" not in stderr
    assert _child_lines(lines, pid) == [f"{pid} child_line_{i}" for i in range(3)]


def test_forking_under_write_load_leaves_no_half_written_record(tmp_path: Path) -> None:
    children, lines, stderr = _run(tmp_path, burst=1000, rounds=20)

    assert "reentrant call" not in stderr
    assert [code for _, code in children] == [0] * 20
    for pid, _ in children:
        assert _child_lines(lines, pid) == [f"{pid} child_line_{i}" for i in range(3)]
