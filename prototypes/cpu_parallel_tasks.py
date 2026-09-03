"""Task-Modul fuer den Parallel- und Dauerlauftest.

Wird vom ``forkserver`` vorgeladen und baut auf
``cpu_integration_tasks`` auf — dessen Import registriert die dortigen
Tasks und richtet den Dishka-Container ein. Neu sind hier nur die Tasks,
die der Parallelbetrieb braucht: messbare Rechenlast, echter
Speicherdruck und gemischte Laufzeiten.

Wichtig fuer die Fork-Frage: auch dieses Modul rechnet beim Import
nichts. Der Forkserver darf importieren, aber niemals rechnen — sonst
startet OpenBLAS seine Threads schon im Forkserver und die Fork-Quelle
ist nicht mehr threadfrei (Befund B1).
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import Any

import numpy as np

# Registriert die Tasks der Integrationsserie und setzt Dishka auf.
from prototypes import cpu_integration_tasks as _base  # noqa: F401
from swarmq.task import Task, TaskInfo


def own_threads() -> int:
    """Threadzahl des eigenen Prozesses.

    Nach dem ersten BLAS-Aufruf steht hier, wie viele Threads OpenBLAS
    aufgemacht hat — die Zahl, die bei N gleichzeitigen Rechenkindern
    ueber Auslastung oder Ueberbuchung entscheidet.
    """
    with open("/proc/self/status") as fh:
        for line in fh:
            if line.startswith("Threads:"):
                return int(line.split()[1])
    return -1


def rss_mb() -> float:
    with open("/proc/self/status") as fh:
        for line in fh:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024
    return -1.0


class ParallelBlasTask(Task):
    """Rechenlast mit analytisch bekanntem Ergebnis und messbarer Dauer.

    ``a @ b`` mit skalierten Einheitsmatrizen rechnet BLAS vollstaendig
    durch (es kennt die Nullen nicht), die Spur ist aber vorhersagbar.
    Damit faellt eine durch fork korrumpierte BLAS-Bibliothek auf, statt
    nur langsamer zu sein.
    """

    task_info = TaskInfo(name="proto.par_blas", queues=["cpu"])

    async def run(self, n: int = 600, reps: int = 6) -> dict[str, Any]:
        a = np.eye(n, dtype=np.float64) * 2.0
        b = np.eye(n, dtype=np.float64) * 3.0
        t0 = time.perf_counter()
        c = a
        for _ in range(reps):
            c = a @ b
        cpu_s = time.perf_counter() - t0
        trace = float(np.trace(c))
        expected = 6.0 * n
        return {
            "correct": abs(trace - expected) < 1e-6,
            "cpu_s": cpu_s,
            "threads": own_threads(),
            "rss": rss_mb(),
            "pid": os.getpid(),
        }


class MemoryPressureTask(Task):
    """Belegt echten Speicher und haelt ihn, bis die Geschwister da sind.

    ``np.ones`` fasst jede Seite an, der Speicher ist also wirklich
    resident und nicht nur reserviert. Das ``sleep`` sorgt dafuer, dass
    alle Slots gleichzeitig unter Druck stehen — genau der Fall, den der
    Integrationstest offengelassen hat.
    """

    task_info = TaskInfo(name="proto.mem_pressure", queues=["cpu"])

    async def run(self, mb: int = 400, hold_s: float = 1.5) -> dict[str, Any]:
        buf = np.ones(mb * 1024 * 1024 // 8, dtype=np.float64)
        resident = rss_mb()
        await asyncio.sleep(hold_s)
        checksum = float(buf[0] + buf[-1])
        return {
            "correct": abs(checksum - 2.0) < 1e-9 and resident > mb * 0.8,
            "resident_mb": resident,
            "pid": os.getpid(),
        }


class MixedDurationTask(Task):
    """Ungleiche Laufzeiten — der Fall, in dem ein Slot brachliegen kann."""

    task_info = TaskInfo(name="proto.mixed", queues=["cpu"])

    async def run(self, reps: int) -> dict[str, Any]:
        n = 400
        a = np.eye(n, dtype=np.float64) * 2.0
        b = np.eye(n, dtype=np.float64) * 3.0
        t0 = time.perf_counter()
        c = a
        for _ in range(reps):
            c = a @ b
        return {
            "correct": abs(float(np.trace(c)) - 6.0 * n) < 1e-6,
            "cpu_s": time.perf_counter() - t0,
            "pid": os.getpid(),
        }
