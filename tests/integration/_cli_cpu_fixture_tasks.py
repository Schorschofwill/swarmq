"""Task-Modul fuer den ``python -m``-Start des CPU-Workers (Plan 2026-09-08-001, U8, AE13).

Das Hauptmodul ``tests.integration._cli_cpu_main`` importiert dieses Modul
und zaehlt hier jede seiner Ausfuehrungen mit. Der Zaehler lebt absichtlich
*nicht* im Hauptmodul: ``runpy.run_module`` fuehrt das Hauptmodul in einem
frischen Namensraum aus, ein dortiger Zaehler stuende immer bei 1. Hier
dagegen sieht ein Rechenkind, ob das vom Forkserver geerbte Hauptmodul im
Kind ein zweites Mal ausgefuehrt wurde (B13).
"""

from __future__ import annotations

import os
import sys
from typing import override

from swarmq.task import Task, TaskInfo

QUEUE = os.environ.get("CPU_FIXTURE_QUEUE", "cpu-fixture")
MAIN_MODULE = "tests.integration._cli_cpu_main"

#: Vom Hauptmodul bei jeder Ausfuehrung um eins erhoeht (Doppel-Import-Nachweis).
MAIN_MODULE_EXECUTIONS = 0


class CpuMainProbe(Task):
    """Meldet aus dem Rechenkind, wie das Hauptmodul dort registriert ist."""

    task_info = TaskInfo(name="cpu_main_probe", queues=[QUEUE], max_retries=0)

    @override
    async def run(self) -> dict[str, object]:
        main = sys.modules.get("__main__")
        return {
            "pid": os.getpid(),
            "main_is_module": main is not None and main is sys.modules.get(MAIN_MODULE),
            "main_spec_name": getattr(getattr(main, "__spec__", None), "name", None),
            "executions": MAIN_MODULE_EXECUTIONS,
        }
