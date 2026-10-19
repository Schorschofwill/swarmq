"""Pruefungen in einem echten Kind per ``os.fork``.

Damit laufen die registrierten Fork-Hooks (``os.register_at_fork``) wie im
Betrieb, statt dass ein Test den privaten Hook direkt aufruft (AGENTS.md:
keine Tests privater Methoden). Ein Fork dauert wenige Millisekunden.

Der Deckel ist nur eine Meldezeit (Plan 0.14, KTD8): Jede Pruefung im Kind
endet bei einem Fehler mit eigener Assertion oder als Haenger, nie als
spaetes Gelingen -- wer im Kind wartet, wartet ohne eigene Frist oder mit
einer, deren Ablauf eine Assertion verletzt. Unter CI-Last darf das Kind
deshalb lange brauchen; der Deckel entscheidet nur, wann ein Haenger
gemeldet wird. Haengt es, nennt die Meldung, wann das Kind aus den
Fork-Hooks kam, und das Kind schreibt seinen Stack: So zeigt ein Fehler, ob
es Last war oder ein Deadlock an einem geerbten Lock.

Den Stack loest ``SIGABRT`` aus (``faulthandler.enable`` im Kind), nicht
``faulthandler.dump_traceback_later``: Dessen Watchdog-Thread hat pytest im
Elternprozess gestartet (``faulthandler_timeout``), im Kind fehlt er, und
der neue Aufruf wartete dort fuer immer auf sein Ende.
"""

from __future__ import annotations

import contextlib
import faulthandler
import os
import signal
import sys
import threading
import time
import traceback
import warnings
from collections.abc import Callable

CHILD_TIMED_OUT = -1
"""Exit-Code von :func:`run_in_forked_child`, wenn das Kind haengt."""

CHILD_TIMEOUT_S = 10.0
"""Vorgabe-Deckel von :func:`run_in_forked_child`: nur die Meldezeit eines
Haengers, im Erfolgsfall laeuft nichts so lange."""

FORK_TEST_TIMEOUT_S = 30
"""``pytest.mark.timeout`` fuer Tests mit Fork-Kind: ueber dem Deckel, damit
:func:`run_in_forked_child` den Haenger meldet und das Kind toetet, bevor
pytest-timeout den Test abbricht."""

UNBOUNDED_DRAIN_S = 3600.0
"""Frist fuer ``shutdown_logging`` im Kind: Wartet das Leeren auf einen
geerbten Listener, haengt das Kind, statt nach der Vorgabe von 5 s
(``drain_timeout``) mit 0 zu enden -- ein spaetes Gelingen, das ein Deckel
ueber 5 s nicht mehr meldete."""

_STACK_DUMP_GRACE_S = 5.0
"""So lange darf das Kind nach ``SIGABRT`` seinen Stack schreiben, bevor es
per ``SIGKILL`` endet."""


def run_in_forked_child(check: Callable[[], object], timeout: float = CHILD_TIMEOUT_S) -> int:
    """Fuehrt ``check`` im Kind aus und liefert dessen Exit-Code.

    0 ohne Ausnahme, 1 mit (Traceback auf stderr), :data:`CHILD_TIMED_OUT`,
    wenn das Kind nach ``timeout`` Sekunden noch laeuft -- es schreibt dann
    seinen Stack auf stderr und wird beendet. Das Kind
    endet immer per ``os._exit``: kein pytest-Teardown, keine atexit-Hooks.
    """
    forked_at = time.monotonic()
    deadline = forked_at + timeout
    started_r, started_w = os.pipe()
    with warnings.catch_warnings():
        # 3.14 warnt vor ``fork`` in einem Prozess mit Threads.
        warnings.simplefilter("ignore", DeprecationWarning)
        pid = os.fork()
    if pid == 0:  # pragma: no cover - laeuft im Kind
        _run_child(check, started_r, started_w)
    os.close(started_w)
    started: list[float] = []
    status: list[int] = []

    def wait_for_start() -> None:
        # Ein Byte, sobald das Kind aus den Fork-Hooks kommt; EOF, wenn es
        # vorher endet.
        if os.read(started_r, 1):
            started.append(time.monotonic())

    # Wartende Threads statt Polling: ``join`` traegt die Zeitgrenze.
    starter = threading.Thread(target=wait_for_start)
    reaper = threading.Thread(target=lambda: status.append(os.waitpid(pid, 0)[1]))
    starter.start()
    reaper.start()
    timed_out = True
    try:
        reaper.join(max(0.0, deadline - time.monotonic()))
        timed_out = reaper.is_alive()
        if timed_out:
            _signal_child(pid, signal.SIGABRT)  # Stack des Kinds auf stderr
            reaper.join(_STACK_DUMP_GRACE_S)
    finally:
        # Auch wenn pytest-timeout den Test abbricht: kein verwaistes Kind.
        if reaper.is_alive():
            _signal_child(pid, signal.SIGKILL)
        reaper.join()
        starter.join()
        os.close(started_r)
    if timed_out:
        since_fork = "nie" if not started else f"nach {started[0] - forked_at:.3f} s"
        print(
            f"run_in_forked_child: Kind nach {timeout:.1f} s nicht fertig, per SIGABRT beendet"
            f" (Stack darueber); aus den Fork-Hooks: {since_fork}",
            file=sys.stderr,
        )
        return CHILD_TIMED_OUT
    return os.waitstatus_to_exitcode(status[0])


def _signal_child(pid: int, signum: signal.Signals) -> None:
    """``os.kill``, ausser der Reaper hat das Kind eben eingesammelt."""
    with contextlib.suppress(ProcessLookupError):
        os.kill(pid, signum)


def _run_child(
    check: Callable[[], object], started_r: int, started_w: int
) -> None:  # pragma: no cover - laeuft im Kind
    code = 1
    try:
        os.close(started_r)
        faulthandler.enable(file=2, all_threads=True, c_stack=False)  # Stack bei SIGABRT
        os.write(started_w, b"s")
        os.close(started_w)
        check()
        code = 0
    except BaseException:
        traceback.print_exc()
    finally:
        sys.stderr.flush()
        os._exit(code)
