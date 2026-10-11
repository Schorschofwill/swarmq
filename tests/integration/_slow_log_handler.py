"""Log-Handler mit einstellbar langsamem Aufbau (Plan 2026-10-06-1811, U8).

Vorlage ist ``slowmod.py`` aus der Analyse S6: Der Aufbau schlaeft, wenn eine
Flag-Datei ``$SLOW_LOG_FLAG.<n>`` existiert, und verbraucht sie per Umbenennen
(``.used``). So schlaeft genau ein Prozess -- die Probe des Supervisors, die
die Datei nach dem SIGHUP als erste anlegt --, nicht Supervisor oder Worker.
Bewusst ohne swarmq-Importe, damit die Probe nur diese Datei laedt.
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path


class SlowHandler(logging.StreamHandler):  # type: ignore[type-arg]
    def __init__(self, *args, **kwargs) -> None:
        base = os.environ.get("SLOW_LOG_FLAG", "")
        flags = sorted(Path(base).parent.glob(Path(base).name + ".*")) if base else []
        for flag in flags:
            if flag.name.endswith(".used"):
                continue
            used = flag.with_name(flag.name + ".used")
            try:
                flag.rename(used)
            except OSError:
                continue  # ein anderer Prozess war schneller
            with used.open() as fh:
                time.sleep(float(fh.read() or "5"))
            break
        super().__init__(*args, **kwargs)
