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

from __future__ import annotations

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
