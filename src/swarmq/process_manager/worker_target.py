"""Kindziel der Worker-Prozesse und Signalblockade des Supervisors (aus
``supervisor`` ausgelagert): ``run_worker_entry`` hebt die geerbte
Signalmaske auf und bindet das Kind per Parent-Death-Signal an den
Supervisor."""

from __future__ import annotations

import multiprocessing
import os
import signal
from collections.abc import Callable
from typing import Any

_HANDLED_SIGNALS = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)


def block_signals() -> None:
    """Block ``_HANDLED_SIGNALS`` on the current thread."""
    try:
        signal.pthread_sigmask(signal.SIG_BLOCK, _HANDLED_SIGNALS)
    except (AttributeError, OSError):
        # Non-POSIX platforms: best effort; supervisor still works.
        pass


def unblock_signals() -> None:
    """Unblock ``_HANDLED_SIGNALS`` on the current thread."""
    try:
        signal.pthread_sigmask(signal.SIG_UNBLOCK, _HANDLED_SIGNALS)
    except (AttributeError, OSError):
        pass


#: ``prctl``-Option, mit der ein Kind sich beim Tod seines Elternprozesses ein
#: Signal bestellt (Linux). Dieselbe Mechanik nutzt das Rechenkind (KTD3 des
#: CPU-Plans), dort mit SIGKILL; ein Worker bekommt SIGTERM und drainiert.
_PR_SET_PDEATHSIG = 1


def _bind_to_supervisor() -> bool:
    """Das Kind stirbt mit dem Supervisor -- auch wenn dieser per SIGKILL endet.

    Ohne dieses Band lebte ein Worker weiter, dessen Supervisor hart beendet
    wurde (Test-Teardown, OOM-Killer, ``docker kill``): reparented, unbeachtet,
    und mit einem Scheduler-Heartbeat, der den Leader-Lock am Leben hielt und
    jeden spaeteren Leader in derselben Datenbank blockierte (Notiz
    e2e-scheduler-leader-flake). Liefert ``True``, wenn ``prctl`` das Signal
    gesetzt hat; ohne ``prctl`` (Nicht-Linux) bleibt es beim bisherigen
    Verhalten.

    Stirbt der Supervisor zwischen ``fork`` und ``prctl``, kommt kein Signal
    mehr: dann ist der Elternprozess des Kindes nicht mehr der, der es
    gestartet hat, und das Kind beendet sich, statt als Waise anzulaufen.
    """
    try:
        import ctypes

        libc = ctypes.CDLL(None, use_errno=True)
        armed = int(libc.prctl(_PR_SET_PDEATHSIG, int(signal.SIGTERM), 0, 0, 0)) == 0
    except (AttributeError, OSError):
        return False
    parent = multiprocessing.parent_process()
    if parent is not None and os.getppid() != parent.pid:
        raise SystemExit(0)
    return armed


def run_worker_entry(entry: Callable[..., Any], *args: Any) -> Any:
    """Ziel des Kindprozesses: geerbte Signalblockade aufheben, dann den Entry rufen.

    Der Supervisor blockiert ``_HANDLED_SIGNALS``, waehrend er Kinder startet;
    ``spawn`` vererbt die Maske. swarmqs eigener Entry hob sie selbst auf, ein
    fremder Entry (der Betreiber gibt dem ``ProcessManager`` seinen eigenen)
    erbte sie -- das SIGTERM, das sein Shutdown-Watcher auf die eigene PID
    schickt, blieb anhaengig, und der Container endete erst per SIGKILL nach
    dem Stop-Timeout (B15 des Loadsuite-Backlogs). Danach bindet sich das Kind
    per Parent-Death-Signal an den Supervisor (:func:`_bind_to_supervisor`).
    Modulfunktion, damit ``spawn`` sie picklen kann.
    """
    unblock_signals()
    _bind_to_supervisor()
    return entry(*args)
