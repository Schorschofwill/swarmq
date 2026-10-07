"""Robustheit der Log-Listener ueber echte Prozesse (Plan 2026-10-06-1811, U2).

Ein werfender App-Handler beendete den Opt-in-Listener (Analyse S2, 2.1):
Danach lief die Queue voll, und das ``shutdown_logging`` aus ``atexit``
haengte auf dem blockierenden Sentinel -- der Prozess endete nie (Exit 124).
Vorlage ist ``s2_dead_listener.py`` der Analyse. Der Listener ueberlebt
jetzt; das Leeren beim Prozessende kommt durch. Ein ``faulthandler`` im Kind
beendet es mit Exit 1 und Stack, falls es doch haengt.
"""

# Test list (U2, echte Prozesse):
# - [x] Opt-in, werfender App-Handler, volle Queue: Exit 0 innerhalb der
#       Frist, ``log_handler_errors`` auf stderr, keine Exception im
#       Listener-Thread.
# Test list (U3, echte Prozesse; Vorlage ``s1_drop_notice.py``):
# - [x] fd 2 auf einer vollen, nie geleerten Pipe: Jeder Logaufruf dauert im
#       Kind unter 100 ms (ueber stdout gemeldet); nach dem Leeren der Pipe
#       erscheint ``log_records_dropped`` als JSON-Zeile.
# Test list (U4, echte Prozesse; Vorlagen ``s2c_blocked_stderr_shutdown``,
# ``s2b_filemode``, ``repro_final2``):
# - [x] ``swarmq``-Befehl, fd 2 auf einer nie geleerten Pipe: Der Prozess
#       endet innerhalb von Frist plus Puffer mit seinem Exit-Code (78) --
#       ueber ``os._exit``, weil der Listener im Schreiben haengt.
# - [x] fd 2 auf einer Pipe, die erst nach Ablauf der Frist geleert wird:
#       kein Exit 134, kein "Fatal Python error".
# - [x] Eingebettet (Opt-in, blockierter App-Handler): kein ``os._exit`` --
#       spaetere atexit-Funktionen laufen, die App endet mit ihrem Code.
# - [x] Datei-Modus mit begrenzter Queue und werfendem Handler: Ende
#       innerhalb der Frist mit Exit 0.
# - [x] Record aus ``__del__`` waehrend der Finalisierung: kein
#       ``--- Logging error ---``, die Zeile erscheint.
# - [x] (Review A4) stdout und stderr auf derselben vollen Pipe, stdout mit
#       gepufferten Daten, Listener haengt an stderr: Das harte Ende kommt
#       trotzdem (Exit-Code 7), ``flush`` von stdout blockiert es nicht.
# Restpunkte (Release 0.9.0):
# - [x] stdout auf einer fast vollen Pipe (eine Seite frei, ``poll`` meldet
#       beschreibbar), mehr als ``PIPE_BUF`` Bytes gepuffert, ein Log-Thread
#       haengt anderswo: Das harte Ende kommt trotzdem mit seinem Code (3);
#       der ``flush`` von stdout blockiert es nicht.
# Test list (U5, echter Prozess; Vorlage ``s3_aexit_stall``):
# - [x] Opt-in, langsamer App-Handler, Records in der Queue: Der maximale
#       Loop-Lag waehrend ``__aexit__`` liegt unter 100 ms (im Kind
#       gemessen), alle Records kommen an.

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import select
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.timeout(60)

_REPO = Path(__file__).resolve().parents[2]

_HANG_DEADLINE_S = 10
"""Frist des ``faulthandler`` im Kind; danach Stack und Exit 1."""

_CHILD = f"""
import faulthandler, logging, sys, threading
faulthandler.dump_traceback_later({_HANG_DEADLINE_S}, exit=True)

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker

entered = threading.Event()
release = threading.Event()


class ShipperDown(logging.Handler):
    # Wartet beim ersten Record, bis der Aufrufer die Queue gefuellt hat,
    # und wirft dann -- ohne ``handleError``.
    def emit(self, record):
        entered.set()
        release.wait()
        raise ConnectionError("log shipper down")


logging.getLogger().handlers[:] = [ShipperDown()]
# Nie verbunden; die Ports sind die der Test-Dienste.
SwarmQ(
    RabbitMQBroker("amqp://guest:guest@localhost:5699/"),
    ValkeyBackend("valkey://localhost:6399/0"),
    log_queue=True,
    log_queue_size=20,
)
log = logging.getLogger("swarmq.tests.robustness")
log.warning("poison")
entered.wait()
for i in range(40):
    log.warning("after %d", i)
release.set()
# Ende: ``atexit`` leert die volle Queue.
"""


def test_a_raising_app_handler_with_a_full_opt_in_queue_still_exits_cleanly() -> None:
    proc = subprocess.run(
        [sys.executable, "-c", _CHILD],
        cwd=_REPO,
        capture_output=True,
        text=True,
        timeout=_HANG_DEADLINE_S + 20,
    )

    assert proc.returncode == 0, proc.stderr
    assert "log_handler_errors" in proc.stderr
    assert "Exception in thread" not in proc.stderr


_LOG_CALL_LIMIT_S = 0.1
"""Obergrenze je Logaufruf im Kind (Plan 2026-10-06-1811, U3)."""

_DROP_CHILD = f"""
import faulthandler, logging, sys, time
faulthandler.dump_traceback_later({_HANG_DEADLINE_S}, exit=True)

from swarmq.logging import configure_logging, dropped_log_records
from swarmq.logging_settings import LoggingSettings

configure_logging(LoggingSettings(format="json", queue_size=10))
log = logging.getLogger("swarmq.tests.robustness")
slowest = 0.0
for i in range(30):
    t0 = time.perf_counter()
    log.warning("record %d", i)
    slowest = max(slowest, time.perf_counter() - t0)
print(slowest, dropped_log_records(), flush=True)
# Ende: ``atexit`` leert die Queue, sobald der Test die Pipe liest.
"""


def _full_pipe() -> tuple[int, int]:
    """Pipe, deren Puffer voll ist; das Schreibende blockiert wieder.
    Gefuellt mit Zeilenenden, damit die spaeteren Logzeilen parsebar sind."""
    r, w = os.pipe()
    flags = fcntl.fcntl(w, fcntl.F_GETFL)
    fcntl.fcntl(w, fcntl.F_SETFL, flags | os.O_NONBLOCK)
    for chunk in (b"\n" * 4096, b"\n"):
        with contextlib.suppress(BlockingIOError):
            while True:
                os.write(w, chunk)
    fcntl.fcntl(w, fcntl.F_SETFL, flags)
    return r, w


def test_log_calls_return_at_once_while_stderr_is_a_full_pipe() -> None:
    r, w = _full_pipe()
    with os.fdopen(r, "rb") as stderr:
        try:
            proc = subprocess.Popen(
                [sys.executable, "-c", _DROP_CHILD],
                cwd=_REPO,
                stdout=subprocess.PIPE,
                stderr=w,
                text=True,
            )
        finally:
            os.close(w)
        try:
            assert proc.stdout is not None
            ready, _, _ = select.select([proc.stdout], [], [], _HANG_DEADLINE_S + 5)
            assert ready, "Kind meldet nichts: ein Logaufruf haengt"
            slowest, dropped = proc.stdout.readline().split()

            assert float(slowest) < _LOG_CALL_LIMIT_S
            assert int(dropped) > 0

            # Pipe leeren: Der Listener schreibt weiter, ``atexit`` leert.
            output = stderr.read().decode()
            assert proc.wait(timeout=_HANG_DEADLINE_S + 5) == 0
        finally:
            proc.kill()
            proc.wait()

    reports = [json.loads(line) for line in output.splitlines() if "log_records_dropped" in line]
    assert [report["count"] for report in reports] == [int(dropped)]
    assert reports[0]["logger"] == "swarmq.logging"


_DRAIN_S = 0.5
"""``--log-drain-timeout`` der U4-Kinder; der Puffer steckt im Prozess-Timeout."""


def test_the_swarmq_command_ends_with_its_exit_code_while_stderr_never_drains(
    tmp_path: Path,
) -> None:
    dump = tmp_path / "hang.txt"
    child = (
        "import faulthandler, sys\n"
        f"faulthandler.dump_traceback_later({_HANG_DEADLINE_S}, exit=True,"
        f" file=open({str(dump)!r}, 'w'))\n"
        "from swarmq.cli import console_main\n"
        "raise SystemExit(console_main(sys.argv[1:]))\n"
    )
    env = dict(
        os.environ,
        SWARMQ_BROKER_URL="amqp://guest:guest@127.0.0.1:1/",
        SWARMQ_BACKEND_URL="valkey://127.0.0.1:1/0",
    )
    r, w = _full_pipe()
    try:
        proc = subprocess.Popen(
            [sys.executable, "-c", child, "worker", "--module", "no_such_module_u4",
             "--log-drain-timeout", str(_DRAIN_S)],
            cwd=_REPO, env=env, stdout=subprocess.DEVNULL, stderr=w,
        )
        os.close(w)
        try:
            returncode = proc.wait(timeout=_DRAIN_S + 15)
        finally:
            proc.kill()
            proc.wait()
    finally:
        os.close(r)  # erst jetzt: Die Pipe wird nie geleert

    hang = dump.read_text() if dump.exists() else ""
    assert returncode == 78, hang


_LATE_DRAIN_CHILD = f"""
import faulthandler, logging, sys
faulthandler.dump_traceback_later({_HANG_DEADLINE_S}, exit=True, file=open(sys.argv[1], "w"))

from swarmq.logging import configure_logging, shutdown_logging
from swarmq.logging_drain import exit_if_log_thread_stuck
from swarmq.logging_settings import LoggingSettings

configure_logging(LoggingSettings(format="json", drain_timeout={_DRAIN_S}))
log = logging.getLogger("swarmq.tests.robustness")
for i in range(20):
    log.warning("pending %d", i)
shutdown_logging()  # laeuft in die Frist: die Pipe ist voll
print("deadline_passed", flush=True)
sys.stdin.readline()  # der Test leert die Pipe jetzt -- mitten ins Ende
exit_if_log_thread_stuck(3)
raise SystemExit(3)
"""


def test_a_pipe_drained_only_after_the_deadline_ends_without_a_fatal_error(
    tmp_path: Path,
) -> None:
    dump = tmp_path / "hang.txt"
    r, w = _full_pipe()
    with os.fdopen(r, "rb") as stderr:
        proc = subprocess.Popen(
            [sys.executable, "-c", _LATE_DRAIN_CHILD, str(dump)],
            cwd=_REPO, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=w,
        )
        os.close(w)
        try:
            assert proc.stdout is not None and proc.stdin is not None
            ready, _, _ = select.select([proc.stdout], [], [], _HANG_DEADLINE_S + 5)
            assert ready, "Kind meldet nichts: das Leeren haengt"
            assert proc.stdout.readline().strip() == b"deadline_passed"
            proc.stdin.write(b"\n")
            proc.stdin.flush()
            output = stderr.read().decode(errors="replace")
            returncode = proc.wait(timeout=_HANG_DEADLINE_S + 5)
        finally:
            proc.kill()
            proc.wait()

    assert returncode == 3, dump.read_text() if dump.exists() else output[-2000:]
    assert "Fatal Python error" not in output


_SHARED_PIPE_CHILD = f"""
import faulthandler, logging, sys
faulthandler.dump_traceback_later({_HANG_DEADLINE_S}, exit=True, file=open(sys.argv[1], "w"))

from swarmq.logging import configure_logging, shutdown_logging
from swarmq.logging_drain import exit_if_log_thread_stuck
from swarmq.logging_settings import LoggingSettings

configure_logging(LoggingSettings(drain_timeout={_DRAIN_S}))
logging.getLogger("swarmq.tests.robustness").warning("stuck on the full pipe")
sys.stdout.write("buffered, never flushed\\n" * 10)  # Pipe: blockgepuffert
shutdown_logging()  # gibt den Listener auf
exit_if_log_thread_stuck(7)
raise SystemExit(3)
"""


def test_the_hard_exit_is_not_blocked_by_stdout_on_the_same_full_pipe(
    tmp_path: Path,
) -> None:
    dump = tmp_path / "hang.txt"
    r, w = _full_pipe()
    try:
        proc = subprocess.Popen(
            [sys.executable, "-c", _SHARED_PIPE_CHILD, str(dump)],
            cwd=_REPO, stdout=w, stderr=w,
        )
        os.close(w)
        try:
            returncode = proc.wait(timeout=_HANG_DEADLINE_S + 5)
        finally:
            proc.kill()
            proc.wait()
    finally:
        os.close(r)  # erst jetzt: Die Pipe wird nie geleert

    assert returncode == 7, dump.read_text() if dump.exists() else ""


_NEARLY_FULL_CHILD = f"""
import faulthandler, logging, sys, threading
faulthandler.dump_traceback_later({_HANG_DEADLINE_S}, exit=True, file=open(sys.argv[1], "w"))

from swarmq.logging_drain import abandon, exit_if_log_thread_stuck

stuck = threading.Thread(target=threading.Event().wait, daemon=True)
stuck.start()
abandon(stuck, [logging.NullHandler()])  # haengt nicht an stdout
sys.stdout.write("x" * 8000)  # Pipe: blockgepuffert, mehr als PIPE_BUF
exit_if_log_thread_stuck(3)
raise SystemExit(4)
"""


def _nearly_full_pipe() -> tuple[int, int]:
    """Pipe mit genau einer freien Seite: ``poll`` meldet sie beschreibbar,
    ein Schreibvorgang ueber ``PIPE_BUF`` Bytes blockiert aber."""
    r, w = os.pipe()
    flags = fcntl.fcntl(w, fcntl.F_GETFL)
    fcntl.fcntl(w, fcntl.F_SETFL, flags | os.O_NONBLOCK)
    with contextlib.suppress(BlockingIOError):
        while True:
            os.write(w, b"\n" * select.PIPE_BUF)
    fcntl.fcntl(w, fcntl.F_SETFL, flags)
    os.read(r, select.PIPE_BUF)
    return r, w


def test_the_hard_exit_is_not_blocked_by_flushing_stdout_into_a_nearly_full_pipe(
    tmp_path: Path,
) -> None:
    dump = tmp_path / "hang.txt"
    r, w = _nearly_full_pipe()
    try:
        proc = subprocess.Popen(
            [sys.executable, "-c", _NEARLY_FULL_CHILD, str(dump)],
            cwd=_REPO, stdout=w, stderr=subprocess.DEVNULL,
        )
        os.close(w)
        try:
            returncode = proc.wait(timeout=_HANG_DEADLINE_S + 5)
        finally:
            proc.kill()
            proc.wait()
    finally:
        os.close(r)  # erst jetzt: Die Pipe wird nicht weiter geleert

    assert returncode == 3, dump.read_text() if dump.exists() else ""


_EMBEDDED_CHILD = f"""
import atexit, faulthandler, logging, sys, threading
faulthandler.dump_traceback_later({_HANG_DEADLINE_S}, exit=True)
# Vor swarmq registriert, laeuft also nach dessen ``shutdown_logging``: Ein
# hartes Ende dort liesse diese Zeile aus.
atexit.register(lambda: print("late_atexit_ran", flush=True))

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker

entered = threading.Event()


class Stuck(logging.Handler):
    def emit(self, record):
        entered.set()
        threading.Event().wait()  # Log-Versand antwortet nie


logging.getLogger().handlers[:] = [Stuck()]
SwarmQ(
    RabbitMQBroker("amqp://guest:guest@localhost:5699/"),
    ValkeyBackend("valkey://localhost:6399/0"),
    log_queue=True,
    log_drain_timeout={_DRAIN_S},  # gilt auch fuer ``atexit`` (U5)
)
log = logging.getLogger("swarmq.tests.robustness")
log.warning("first")
entered.wait()
log.warning("pending")
sys.exit(7)
"""


def test_an_embedded_app_is_never_ended_hard_and_keeps_its_exit_code() -> None:
    proc = subprocess.run(
        [sys.executable, "-c", _EMBEDDED_CHILD],
        cwd=_REPO, capture_output=True, text=True, timeout=_HANG_DEADLINE_S + 20,
    )

    assert proc.returncode == 7, proc.stderr[-2000:]
    assert "late_atexit_ran" in proc.stdout


_FILE_MODE_CHILD = f"""
import faulthandler, json, logging
faulthandler.dump_traceback_later({_HANG_DEADLINE_S}, exit=True)

from swarmq.logging import configure_logging
from swarmq.logging_settings import LoggingSettings

config = {{
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {{
        "bad": {{"class": "tests.integration._blocking_log_handler.AlwaysRaises"}},
        "q": {{
            "class": "logging.handlers.QueueHandler",
            "handlers": ["bad"],
            "queue": {{"()": "queue.Queue", "maxsize": 20}},
        }},
    }},
    "root": {{"handlers": ["q"], "level": "INFO"}},
}}
configure_logging(LoggingSettings(
    log_config="logging.json", log_config_json=json.dumps(config), drain_timeout={_DRAIN_S},
))
log = logging.getLogger("swarmq.tests.robustness")
for i in range(40):
    log.warning("record %d", i)
# Ende: ``atexit`` leert innerhalb der Frist.
"""


def test_file_mode_with_a_bounded_queue_and_a_raising_handler_ends_in_time() -> None:
    proc = subprocess.run(
        [sys.executable, "-c", _FILE_MODE_CHILD],
        cwd=_REPO, capture_output=True, text=True, timeout=_DRAIN_S + 15,
    )

    assert proc.returncode == 0, proc.stderr[-2000:]
    assert "Logging error" not in proc.stderr


_FINALIZATION_CHILD = f"""
import faulthandler, gc, logging
faulthandler.dump_traceback_later({_HANG_DEADLINE_S}, exit=True)

from swarmq.logging import configure_logging
from swarmq.logging_settings import LoggingSettings

configure_logging(LoggingSettings(format="json"))
_log = logging.getLogger("app.final")


class Cycle:
    # Nur ueber einen Zyklus erreichbar: Der Finalizer laeuft erst in der
    # Garbage Collection der Finalisierung, nach ``atexit``.
    def __init__(self):
        self.me = self

    def __del__(self, _log=_log):
        _log.warning("cycle_finalized_at_shutdown")


gc.disable()
Cycle()
_log.warning("normal_line")
"""


def test_a_record_from_a_finalizer_at_shutdown_is_written_without_a_logging_error() -> None:
    proc = subprocess.run(
        [sys.executable, "-c", _FINALIZATION_CHILD],
        cwd=_REPO, capture_output=True, text=True, timeout=_HANG_DEADLINE_S + 20,
    )

    assert proc.returncode == 0, proc.stderr[-2000:]
    assert "--- Logging error ---" not in proc.stderr
    assert "normal_line" in proc.stderr
    assert "cycle_finalized_at_shutdown" in proc.stderr


_LOOP_LAG_LIMIT_S = 0.1
"""Obergrenze des Loop-Lags waehrend ``__aexit__`` (Plan 2026-10-06-1811, U5)."""

_AEXIT_CHILD = f"""
import asyncio, faulthandler, logging, time
faulthandler.dump_traceback_later({_HANG_DEADLINE_S}, exit=True)

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker

written = []


class SlowShipper(logging.Handler):
    def emit(self, record):
        time.sleep(0.1)
        written.append(record.getMessage())


logging.getLogger().handlers[:] = [SlowShipper()]


async def _no_connection():
    pass


async def main():
    # Nie verbunden; die Ports sind die der Test-Dienste.
    broker = RabbitMQBroker("amqp://guest:guest@localhost:5699/")
    backend = ValkeyBackend("valkey://localhost:6399/0")
    broker.connect = broker.close = backend.connect = backend.close = _no_connection
    loop = asyncio.get_running_loop()
    lag = 0.0
    ticking = asyncio.Event()
    leaving = True

    async def heartbeat():
        nonlocal lag
        while leaving:
            ticking.set()
            t0 = loop.time()
            await asyncio.sleep(0.01)
            lag = max(lag, loop.time() - t0 - 0.01)

    async with SwarmQ(broker, backend, log_queue=True):
        log = logging.getLogger("swarmq.tests.robustness")
        for i in range(5):
            log.warning("queued %d", i)
        beat = asyncio.create_task(heartbeat())
        await ticking.wait()
    leaving = False
    await beat
    print(lag, len(written), flush=True)


asyncio.run(main())
"""


def test_leaving_an_embedded_app_keeps_the_event_loop_responsive() -> None:
    proc = subprocess.run(
        [sys.executable, "-c", _AEXIT_CHILD],
        cwd=_REPO, capture_output=True, text=True, timeout=_HANG_DEADLINE_S + 20,
    )

    assert proc.returncode == 0, proc.stderr[-2000:]
    lag, written = proc.stdout.split()
    assert float(lag) < _LOOP_LAG_LIMIT_S
    assert int(written) == 5
