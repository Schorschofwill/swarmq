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
