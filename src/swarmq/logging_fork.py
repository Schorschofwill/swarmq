"""Fork-Sicherheit der Log-Listener (Plan 2026-10-06-1811, U7, KTD10).

Ein ``os.fork()`` aus Task-Code, waehrend ein Listener-Thread gerade in einen
Ziel-Handler schreibt, vererbte dem Kind einen halb geschriebenen Datensatz:
Den Lock des ``BufferedWriter`` haelt im Kind niemand mehr, jeder weitere
Schreibversuch scheitert mit ``RuntimeError: reentrant call`` (Analyse R3).
Der ``before``-Hook erwirbt deshalb die Locks der Ziel-Handler laufender
Listener; laufende ``emit``-Aufrufe kommen so zu Ende, neue beginnen erst
nach dem Fork.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import Any

from swarmq.logging_drain import Deadline, is_stuck

LOCK_BUDGET_S = 0.05
"""So lange wartet der ``before``-Hook hoechstens auf die Locks: Ein Fork
haengt nie an einem langsamen oder blockierten Ziel."""

_HELD: list[Any] = []
"""Die vom ``before``-Hook erworbenen Locks, in Erwerbsreihenfolge."""


def hold_handler_locks(handlers: Iterable[logging.Handler]) -> None:
    """``before``-Hook: erwirbt die Locks von ``handlers``. Das Budget
    ``LOCK_BUDGET_S`` gilt fuer alle zusammen; ist es aufgebraucht, werden
    die uebrigen nur noch ohne Warten versucht, und der Fork laeuft
    trotzdem. Ziel-Handler aufgegebener Threads (KTD3) bleiben aussen vor:
    Ihr Lock haelt womoeglich der haengende Thread."""
    locks = [h.lock for h in handlers if h.lock is not None and not is_stuck(h)]
    if not locks:
        return
    deadline = Deadline(LOCK_BUDGET_S)
    # Erst ``logging._lock``, dann die Handler-Locks -- die Reihenfolge von
    # ``dictConfig``. Andersherum wartete der stdlib-``before``-Hook (er
    # laeuft nach diesem) auf einen Thread, der ``logging._lock`` haelt und
    # auf ein hier gehaltenes Handler-Lock wartet: ein Deadlock im Fork.
    module_lock = logging._lock  # type: ignore[attr-defined]
    if not module_lock.acquire(timeout=deadline.remaining()):
        return
    _HELD.append(module_lock)
    for lock in locks:
        if lock.acquire(timeout=deadline.remaining()):
            _HELD.append(lock)


def release_handler_locks() -> None:
    """``after_in_parent``: gibt die erworbenen Locks wieder frei."""
    while _HELD:
        _HELD.pop().release()


def forget_handler_locks() -> None:
    """``after_in_child``: Die stdlib setzt im Kind ``logging._lock`` und die
    Locks aller Handler selbst zurueck (``_after_at_fork_child_reinit_locks``,
    vor diesem Hook registriert und daher zuerst ausgefuehrt). Freizugeben
    bleibt nichts; ein ``release`` wuerfe ``RuntimeError``. Nur die Liste
    wird geleert, sonst gaebe ein weiterer Fork aus dem Kind sie frei."""
    _HELD.clear()
