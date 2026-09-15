"""Teardown fuer ``swarmq worker``-Subprozesse in Tests.

Ein ``proc.kill()`` auf den Supervisor liess seine Worker-Kinder als Waisen
zurueck; die hielten den Scheduler-Leader-Lock per Heartbeat und liessen
spaetere Leader-Tests scheitern (Notiz e2e-scheduler-leader-flake). Seit dem
Parent-Death-Signal im Kindziel sterben die Kinder zwar mit -- der Teardown
hier geht trotzdem den geordneten Weg: SIGTERM an den Supervisor, Frist fuer
den Drain, und erst dann SIGKILL fuer die ganze Prozessgruppe. Dafuer muss der
Subprozess mit ``start_new_session=True`` gestartet sein; ohne eigene Sitzung
trifft der Kill nur den Supervisor selbst.
"""

from __future__ import annotations

import contextlib
import os
import signal
import subprocess
from typing import Any


def terminate_worker_process(proc: subprocess.Popen[Any], *, grace_s: float = 30.0) -> None:
    """Supervisor geordnet beenden; nach ``grace_s`` die Prozessgruppe hart toeten."""
    if proc.poll() is not None:
        return
    with contextlib.suppress(ProcessLookupError):
        proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=grace_s)
        return
    except subprocess.TimeoutExpired:
        pass
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(proc.pid, signal.SIGKILL)
    with contextlib.suppress(ProcessLookupError):
        proc.kill()
    proc.wait(timeout=10)
