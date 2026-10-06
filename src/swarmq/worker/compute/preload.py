"""Erster Forkserver-Preload: das Hauptmodul einmal als ``__main__`` registrieren (U8: R28, KTD10).

Laeuft ausschliesslich im Forkserver-Prozess, als erstes Element der
Preload-Liste (``wiring.preload_modules``). Bei ``python -m paket.modul``
meldet der Loop-Prozess jedem Rechenkind ``init_main_from_name=paket.modul``;
``multiprocessing.spawn._fixup_main_from_name`` fuehrt das Modul dann per
``runpy.run_module`` erneut aus — obwohl der Preload es schon importiert hat —
und runpy warnt je Kind ("found in sys.modules after import of package", B13).

Der Ausweg ist oeffentliches Verhalten von ``_fixup_main_from_name``: findet es
``sys.modules["__main__"]`` mit ``__spec__.name == mod_name``, kehrt es sofort
zurueck ("this process was forked, __main__ may already be populated"). Genau
diesen Zustand stellt dieses Modul im Forkserver her; jedes Kind erbt ihn per
fork. Ein per Pfad gestartetes Programm (Konsolenskript) kommt als
``"__main__"`` und braucht nichts davon.

Vor jedem App-Import markiert es den Prozess als Fork-Quelle (Plan
2026-10-06-1811, U6, KTD6): Loggt die App beim Import ueber eine Queue von
swarmq (etwa nach ``SwarmQ(log_queue=True)``), geht der Record synchron
hinaus, statt im Forkserver einen Listener-Thread zu starten.
"""

from __future__ import annotations

import importlib
import os
import sys

from swarmq.logging_queue import mark_fork_source

#: Gesetzt von ``wiring.start_compute_executor`` vor dem Forkserver-Start.
MAIN_MODULE_ENV = "SWARMQ_MAIN_MODULE"


def register_main_module(name: str) -> None:
    """``name`` unter seinem Namen importieren und als ``__main__`` eintragen.

    ``__main__`` (Pfad-Start) und ``paket.__main__`` bleiben aussen vor — dieselbe
    Weiche wie in ``_fixup_main_from_name``; ein Paket-``__main__`` liefe beim
    Import ungeschuetzt los.
    """
    if name == "__main__" or name.endswith(".__main__"):
        return
    sys.modules["__main__"] = importlib.import_module(name)


mark_fork_source()
register_main_module(os.environ.get(MAIN_MODULE_ENV, "__main__"))
