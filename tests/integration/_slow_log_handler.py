"""Log-Handler mit einstellbar langsamem Aufbau (Plan 2026-10-06-1811, U8).

Vorlage ist ``slowmod.py`` aus der Analyse S6: Der Aufbau schlaeft, wenn eine
Flag-Datei ``$SLOW_LOG_FLAG.<n>`` existiert, und verbraucht sie per Umbenennen
(``.used``). So schlaeft genau ein Prozess -- die Probe des Supervisors, die
die Datei nach dem SIGHUP als erste anlegt --, nicht Supervisor oder Worker.
Bewusst ohne swarmq-Importe, damit die Probe nur diese Datei laedt.
"""

from __future__ import annotations

import glob
import logging
import os
import time


class SlowHandler(logging.StreamHandler):  # type: ignore[type-arg]
    def __init__(self, *args, **kwargs) -> None:
        base = os.environ.get("SLOW_LOG_FLAG", "")
        for flag in sorted(glob.glob(base + ".*")) if base else []:
            if flag.endswith(".used"):
                continue
            try:
                os.rename(flag, flag + ".used")
            except OSError:
                continue  # ein anderer Prozess war schneller
            with open(flag + ".used") as fh:
                time.sleep(float(fh.read() or "5"))
            break
        super().__init__(*args, **kwargs)
