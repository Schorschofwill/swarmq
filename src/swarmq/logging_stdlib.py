"""Zugriff auf private Interna des stdlib-Moduls ``logging``.

``logging._lock`` und ``logging._handlerList`` sind CPython-Implementierungs-
details: nicht dokumentiert, nicht in typeshed deklariert und ohne
Stabilitaetszusage. swarmq braucht sie trotzdem -- fuer die Lock-Reihenfolge
im Fork-Hook (``swarmq.logging_fork``), zum Vergessen aufgegebener Handler
(``swarmq.logging_drain``) und um Handler vor ``dictConfig`` zu verbergen
(``swarmq.logging``). Dieses Modul ist die einzige Stelle, die sie anfasst;
aendert CPython sie, ist nur hier anzupassen.

Importiert nur die stdlib: Es liegt auf dem Fork-Pfad.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    import threading
    import weakref

_LOGGING: Any = cast("Any", logging)
"""Das Modul ``logging`` ohne Typpruefung: Die privaten Attribute fehlen in
typeshed. Der einzige ``cast`` auf diese Interna."""


def module_lock() -> threading.RLock:
    """``logging._lock``: das Modul-Lock, das ``dictConfig``,
    ``addHandler``-Registrierung und ``logging.shutdown`` serialisiert."""
    lock: threading.RLock = _LOGGING._lock
    return lock


def handler_refs() -> list[weakref.ref[logging.Handler]]:
    """``logging._handlerList``: die Schwachreferenzen aller bekannten
    Handler, die ``logging.shutdown`` und ``dictConfig`` schliessen. Die
    Liste selbst, nicht eine Kopie -- Aenderungen nur unter
    :func:`module_lock`."""
    refs: list[weakref.ref[logging.Handler]] = _LOGGING._handlerList
    return refs
