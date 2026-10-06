"""Pruefungen in einem echten Kind per ``os.fork``.

Damit laufen die registrierten Fork-Hooks (``os.register_at_fork``) wie im
Betrieb, statt dass ein Test den privaten Hook direkt aufruft (AGENTS.md:
keine Tests privater Methoden). Ein Fork dauert wenige Millisekunden.
"""

from __future__ import annotations

import os
import signal
import threading
import traceback
import warnings
from collections.abc import Callable

CHILD_TIMED_OUT = -1
"""Exit-Code von :func:`run_in_forked_child`, wenn das Kind haengt."""


def run_in_forked_child(check: Callable[[], object], timeout: float = 2.0) -> int:
    """Fuehrt ``check`` im Kind aus und liefert dessen Exit-Code.

    0 ohne Ausnahme, 1 mit (Traceback auf stderr), :data:`CHILD_TIMED_OUT`,
    wenn das Kind nach ``timeout`` Sekunden noch laeuft -- es wird dann
    getoetet. Das Kind endet immer per ``os._exit``: kein pytest-Teardown,
    keine atexit-Hooks.
    """
    with warnings.catch_warnings():
        # 3.14 warnt vor ``fork`` in einem Prozess mit Threads.
        warnings.simplefilter("ignore", DeprecationWarning)
        pid = os.fork()
    if pid == 0:  # pragma: no cover - laeuft im Kind
        code = 1
        try:
            check()
            code = 0
        except BaseException:
            traceback.print_exc()
        finally:
            os._exit(code)
    status: list[int] = []
    # Ein wartender Thread statt Polling: ``join`` traegt die Zeitgrenze.
    reaper = threading.Thread(target=lambda: status.append(os.waitpid(pid, 0)[1]))
    reaper.start()
    reaper.join(timeout)
    if reaper.is_alive():
        os.kill(pid, signal.SIGKILL)
        reaper.join()
        return CHILD_TIMED_OUT
    return os.waitstatus_to_exitcode(status[0])
