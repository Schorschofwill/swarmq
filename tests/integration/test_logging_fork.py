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
# Schluss-Review (P1, Regression gegenueber main), ``multiprocessing.Queue``
# als Queue des ``QueueHandler``:
# - [x] Kind per ``multiprocessing.Process`` (fork-Kontext) und per rohem
#       ``os.fork``: Die Zeilen des Kindes kommen ueber den Listener des
#       Elternprozesses an, der Elternprozess schreibt danach alle weiteren
#       Zeilen, und sein Ende wartet nicht bis zur Frist.
# Restpunkte (Release 0.9.0), Proxy aus ``multiprocessing.Manager`` als Queue:
# - [x] Rohes ``os.fork``, Eltern- und Kindprozess loggen gleichzeitig: Das
#       Kind baut eine eigene Verbindung zum Manager auf, statt die des
#       Elternprozesses mitzubenutzen -- kein ``Logging error``, alle Zeilen
#       beider Prozesse in der Datei.
# Restpunkte (Release 0.9.0), ``multiprocessing.Queue`` am Prozessende ohne
# eigenes ``shutdown_logging`` (nur atexit; die atexit-Funktion von
# ``multiprocessing`` laeuft vor der von swarmq und schliesst die Pipe):
# - [x] Kein Traceback auf stderr, alle Zeilen in der Datei.
# - [x] Ein Record aus einer spaeteren atexit-Funktion der App: kein
#       Traceback, er zaehlt als verworfen, alle frueheren Zeilen in der Datei.

from __future__ import annotations

import contextlib
import json
import os
import signal
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

import swarmq.logging_fork
from swarmq.logging import configure_logging, shutdown_logging
from swarmq.logging_settings import LoggingSettings

log_file, maxsize, burst, rounds = sys.argv[1], *map(int, sys.argv[2:5])
if len(sys.argv) > 5:
    swarmq.logging_fork.LOCK_BUDGET_S = float(sys.argv[5])
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


def _run(
    tmp_path: Path, *, maxsize: int = 0, burst: int = 0, rounds: int = 1, lock_budget_s: float | None = None
) -> tuple[
    list[list[int]],
    list[str],
    str,
]:
    """Fuehrt das Skript aus: Kinder mit Exit-Code, Zeilen der Datei, stderr.

    ``lock_budget_s`` ersetzt ``LOCK_BUDGET_S`` des ``before``-Hooks im Skript."""
    log_file = tmp_path / "out.log"
    budget = [] if lock_budget_s is None else [str(lock_budget_s)]
    proc = subprocess.run(
        [sys.executable, "-c", _SCRIPT, str(log_file), str(maxsize), str(burst), str(rounds), *budget],
        cwd=_REPO,
        capture_output=True,
        text=True,
        timeout=_HANG_DEADLINE_S + 10,
        check=False,
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
    """Geprueft wird, dass der Hook die Locks ueber den Fork haelt. Mit den
    50 ms aus ``LOCK_BUDGET_S`` gibt er unter Last auf (entschiedenes
    Restrisiko, KTD10; CI-Lauf 41231517138, V0161-K3); im Skript gilt
    deshalb die Haengefrist als Budget."""
    children, lines, stderr = _run(tmp_path, burst=1000, rounds=20, lock_budget_s=_HANG_DEADLINE_S)

    assert "reentrant call" not in stderr
    assert [code for _, code in children] == [0] * 20
    for pid, _ in children:
        assert _child_lines(lines, pid) == [f"{pid} child_line_{i}" for i in range(3)]


_MP_DRAIN_TIMEOUT_S = 4.0
"""Frist des Leerens im Skript; das Ende des Elternprozesses bleibt weit
darunter."""

# Kein ``faulthandler.dump_traceback_later`` wie oben: Das per ``os.fork``
# geteilte Kind endet hier ueber den Interpreter, und dessen Ende wartete auf
# den Watchdog-Thread des Elternprozesses, den es im Kind nicht gibt. Den
# Stack liefert stattdessen ein Alarm (im Kind geerbt, mit eigener Frist).
_MP_SCRIPT = f"""
import faulthandler, json, logging, multiprocessing, os, signal, sys, threading, time, warnings
warnings.simplefilter("ignore", DeprecationWarning)  # fork mit Threads (3.14)


def on_alarm(*_):
    faulthandler.dump_traceback(all_threads=True)
    os._exit(3)


signal.signal(signal.SIGALRM, on_alarm)
signal.alarm({_HANG_DEADLINE_S})

from swarmq.logging import configure_logging, shutdown_logging
from swarmq.logging_settings import LoggingSettings

log_file, mode = sys.argv[1], sys.argv[2]
config = {{
    "version": 1,
    "formatters": {{"p": {{"format": "%(process)d %(message)s"}}}},
    "handlers": {{
        "file": {{"class": "logging.FileHandler", "filename": log_file, "formatter": "p"}},
        "q": {{
            "class": "logging.handlers.QueueHandler", "handlers": ["file"],
            "queue": {{"()": "multiprocessing.Queue"}},
        }},
    }},
    "root": {{"level": "INFO", "handlers": ["q"]}},
}}
configure_logging(LoggingSettings(
    log_config="/never/read.json", log_config_json=json.dumps(config),
    drain_timeout={_MP_DRAIN_TIMEOUT_S},
))
log = logging.getLogger("app")
log.info("parent_before_fork")


def child_body():
    signal.alarm({_CHILD_DEADLINE_S})
    for i in range(3):
        log.info("child_line_%d", i)
    # Ausser dem Feeder der Queue laeuft kein Thread: Ein Listener im Kind
    # konkurrierte mit dem des Elternprozesses.
    others = [t.name for t in threading.enumerate()
              if t is not threading.main_thread() and t.name != "QueueFeederThread"]
    if others:
        print("listener in the child:", others, file=sys.stderr)
        os._exit(4)


if mode == "process":
    child = multiprocessing.get_context("fork").Process(target=child_body)
    child.start()
    child.join()
    pid, code = child.pid, child.exitcode
else:
    pid = os.fork()
    if pid == 0:
        child_body()
        # Ende ueber den Interpreter: atexit leert den Feeder der Queue.
        sys.exit(0)
    _, status = os.waitpid(pid, 0)
    code = os.waitstatus_to_exitcode(status)
for i in range(5):
    log.info("parent_after_child_%d", i)
started = time.monotonic()
shutdown_logging()
print(json.dumps({{"child": [pid, code], "shutdown_s": time.monotonic() - started}}))
"""


@pytest.mark.parametrize("mode", ["process", "raw_fork"])
def test_a_multiprocessing_queue_keeps_the_parent_listener_as_its_only_consumer(
    tmp_path: Path,
    mode: str,
) -> None:
    log_file = tmp_path / "out.log"
    proc = subprocess.run(
        [sys.executable, "-c", _MP_SCRIPT, str(log_file), mode],
        cwd=_REPO,
        capture_output=True,
        text=True,
        timeout=_HANG_DEADLINE_S + 10,
        check=False,
    )

    assert proc.returncode == 0, proc.stderr
    result = json.loads(proc.stdout)
    pid, code = result["child"]
    assert code == 0
    assert "log_drain_timeout_exceeded" not in proc.stderr
    assert result["shutdown_s"] < _MP_DRAIN_TIMEOUT_S / 2
    lines = log_file.read_text().splitlines()
    assert _child_lines(lines, pid) == [f"{pid} child_line_{i}" for i in range(3)]
    parent_lines = [line.split(" ", 1)[1] for line in lines if not line.startswith(f"{pid} ")]
    assert parent_lines == ["parent_before_fork"] + [f"parent_after_child_{i}" for i in range(5)]


_MANAGER_SCRIPT = f"""
import faulthandler, json, logging, multiprocessing, os, signal, sys, warnings
warnings.simplefilter("ignore", DeprecationWarning)  # fork mit Threads (3.14)


def on_alarm(*_):
    faulthandler.dump_traceback(all_threads=True)
    os._exit(3)


signal.signal(signal.SIGALRM, on_alarm)
signal.alarm({_HANG_DEADLINE_S})

from swarmq.logging import configure_logging, shutdown_logging
from swarmq.logging_settings import LoggingSettings

MANAGER = None


def manager_queue():
    return MANAGER.Queue()


if __name__ == "__main__":
    MANAGER = multiprocessing.Manager()
    log_file, lines = sys.argv[1], int(sys.argv[2])
    config = {{
        "version": 1,
        "formatters": {{"p": {{"format": "%(process)d %(message)s"}}}},
        "handlers": {{
            "file": {{"class": "logging.FileHandler", "filename": log_file, "formatter": "p"}},
            "q": {{
                "class": "logging.handlers.QueueHandler", "handlers": ["file"],
                "queue": {{"()": "__main__.manager_queue"}},
            }},
        }},
        "root": {{"level": "INFO", "handlers": ["q"]}},
    }}
    configure_logging(LoggingSettings(
        log_config="/never/read.json", log_config_json=json.dumps(config),
        drain_timeout={_MP_DRAIN_TIMEOUT_S},
    ))
    log = logging.getLogger("app")
    log.info("parent_before_fork")  # baut die Verbindung des Hauptthreads auf
    pid = os.fork()
    if pid == 0:
        signal.alarm({_CHILD_DEADLINE_S})
        for i in range(lines):
            log.info("child_line_%d", i)
        # Ohne Interpreter-Ende: Dessen ``multiprocessing``-atexit wollte den
        # Manager des Elternprozesses einsammeln. Ein Proxy hat keinen Puffer.
        os._exit(0)
    for i in range(lines):
        log.info("parent_line_%d", i)
    _, status = os.waitpid(pid, 0)
    shutdown_logging()
    print(json.dumps({{"child": [pid, os.waitstatus_to_exitcode(status)]}}))
    MANAGER.shutdown()
"""


def test_a_manager_queue_gets_its_own_connection_in_a_raw_forked_child(tmp_path: Path) -> None:
    log_file = tmp_path / "out.log"
    lines = 300
    # Eigene Sitzung: Haengt der Lauf, beendet ``killpg`` auch den Manager
    # und den Forkserver, die sonst die Pipes offen und verwaist zurueckblieben.
    proc = subprocess.Popen(
        [sys.executable, "-c", _MANAGER_SCRIPT, str(log_file), str(lines)],
        cwd=_REPO,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        stdout, stderr = proc.communicate(timeout=_HANG_DEADLINE_S + 10)
    finally:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(proc.pid, signal.SIGKILL)
        proc.wait()

    assert proc.returncode == 0, stderr[-3000:]
    assert "Logging error" not in stderr, stderr[-3000:]
    pid, code = json.loads(stdout)["child"]
    assert code == 0, stderr[-3000:]
    written = log_file.read_text().splitlines()
    assert _child_lines(written, pid) == [f"{pid} child_line_{i}" for i in range(lines)]
    parent = [line.split(" ", 1)[1] for line in written if not line.startswith(f"{pid} ")]
    assert parent == ["parent_before_fork"] + [f"parent_line_{i}" for i in range(lines)]


_EXIT_SCRIPT = f"""
import atexit, faulthandler, json, logging, sys
faulthandler.dump_traceback_later({_HANG_DEADLINE_S}, exit=True)

from swarmq.logging import configure_logging
from swarmq.logging_settings import LoggingSettings

log_file, lines, late = sys.argv[1], int(sys.argv[2]), sys.argv[3] == "late"
if late:
    # Vor ``multiprocessing`` registriert: laeuft nach dessen atexit-Funktion.
    atexit.register(lambda: logging.getLogger("app").info("late_line"))
config = {{
    "version": 1,
    "formatters": {{"p": {{"format": "%(message)s"}}}},
    "handlers": {{
        "file": {{"class": "logging.FileHandler", "filename": log_file, "formatter": "p"}},
        "q": {{
            "class": "logging.handlers.QueueHandler", "handlers": ["file"],
            "queue": {{"()": "multiprocessing.Queue"}},
        }},
    }},
    "root": {{"level": "INFO", "handlers": ["q"]}},
}}
configure_logging(LoggingSettings(log_config="/never/read.json", log_config_json=json.dumps(config)))
log = logging.getLogger("app")
for i in range(lines):
    log.info("line_%d", i)
"""


@pytest.mark.parametrize("late", ["no_late", "late"])
def test_a_multiprocessing_queue_ends_without_traceback_at_interpreter_exit(
    tmp_path: Path,
    late: str,
) -> None:
    log_file = tmp_path / "out.log"
    lines = 2000
    proc = subprocess.run(
        [sys.executable, "-c", _EXIT_SCRIPT, str(log_file), str(lines), late],
        cwd=_REPO,
        capture_output=True,
        text=True,
        timeout=_HANG_DEADLINE_S + 10,
        check=False,
    )

    assert proc.returncode == 0, proc.stderr[-3000:]
    assert "Traceback" not in proc.stderr, proc.stderr[-3000:]
    assert log_file.read_text().splitlines() == [f"line_{i}" for i in range(lines)]
    if late == "late":
        assert "1 log record(s)" in proc.stderr, proc.stderr
    else:
        assert proc.stderr == ""
