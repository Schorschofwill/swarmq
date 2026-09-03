"""Task-Modul fuer den CPU-Worker-Integrationstest.

Wird vom ``forkserver`` vorgeladen. Enthaelt bewusst echte C-Extensions
(numpy/BLAS, Pillow), weil genau die die fork-Frage aufwerfen: laden sie
beim Import Threads, waere der Forkserver keine sichere Fork-Quelle mehr.
"""

from __future__ import annotations

import os
from typing import Any, ClassVar

import numpy as np
from PIL import Image, ImageFilter

from swarmq.task import Task, TaskInfo

try:
    from dishka import FromDishka, Provider, Scope, make_async_container, provide

    from swarmq.dishka import SwarmQProvider, setup_dishka

    _HAVE_DISHKA = True
except ImportError:  # pragma: no cover
    _HAVE_DISHKA = False


class Database:
    """APP-scoped Ressource — belegt, in welchem Prozess sie entstand."""

    instances = 0

    def __init__(self) -> None:
        Database.instances += 1
        self.built_in_pid = os.getpid()
        self.instance_no = Database.instances

    def query(self) -> str:
        return f"db(pid={self.built_in_pid},no={self.instance_no})"


if _HAVE_DISHKA:

    class AppProvider(Provider):
        @provide(scope=Scope.APP)
        def database(self) -> Database:
            return Database()

    setup_dishka(make_async_container(AppProvider(), SwarmQProvider()))


class NumpyTask(Task):
    """BLAS-Matrixmultiplikation mit verifizierbarem Ergebnis.

    Der Vergleich gegen die analytisch bekannte Spur ist der eigentliche
    Test: eine durch fork korrumpierte BLAS-Bibliothek wuerde entweder
    haengen oder falsch rechnen — beides faellt hier auf.
    """

    task_info = TaskInfo(name="proto.numpy", queues=["cpu"])

    async def run(self, n: int, db: FromDishka[Database] | None = None) -> dict[str, Any]:  # type: ignore[valid-type]
        a = np.eye(n, dtype=np.float64) * 2.0
        b = np.eye(n, dtype=np.float64) * 3.0
        c = a @ b  # echter BLAS-Aufruf nach dem Fork
        trace = float(np.trace(c))
        expected = 6.0 * n
        return {
            "correct": abs(trace - expected) < 1e-6,
            "detail": f"{n}x{n} @ -> Spur {trace:.1f} (erwartet {expected:.1f})",
            "db": db.query() if db is not None else "kein DI",
            "pid": os.getpid(),
        }


class NumpyProgressTask(Task):
    """Wie NumpyTask, meldet aber Fortschritt — fuer den Valkey-Rueckkanal."""

    task_info = TaskInfo(name="proto.numpy_progress", queues=["cpu"])

    async def run(self, steps: int) -> dict[str, Any]:
        total = 0.0
        for step in range(steps):
            m = np.random.default_rng(step).random((500, 500))
            total += float((m @ m.T).trace())
            await self.update_progress(step + 1, steps, f"Block {step + 1}")
        return {"correct": total > 0, "detail": f"{steps} Bloecke", "pid": os.getpid()}


class PillowTask(Task):
    """Bildverarbeitung mit pruefbarem Ergebnis."""

    task_info = TaskInfo(name="proto.pillow", queues=["cpu"])

    async def run(self, size: int) -> dict[str, Any]:
        img = Image.new("RGB", (size, size), (10, 120, 240))
        img = img.filter(ImageFilter.GaussianBlur(radius=4)).resize((size // 2, size // 2))
        px = img.getpixel((size // 4, size // 4))
        # Eine Flaeche einer Farbe bleibt nach Blur+Resize dieselbe Farbe.
        return {
            "correct": abs(px[0] - 10) <= 2 and abs(px[1] - 120) <= 2 and abs(px[2] - 240) <= 2,
            "detail": f"{size}px -> {img.size[0]}px, Mittelpixel {px}",
            "pid": os.getpid(),
        }


class BlasHangTask(Task):
    """Verbringt Sekunden IN C-Code (SVD), nicht in Python-Bytecode.

    Der harte Fall fuer den Hard-Timeout: hier laeuft kein Interpreter, der
    ein Signal pruefen koennte — nur ein SIGKILL von aussen wirkt.
    """

    task_info = TaskInfo(name="proto.blas_hang", queues=["cpu"])

    async def run(self, n: int) -> dict[str, Any]:
        rng = np.random.default_rng(0)
        for _ in range(50):
            np.linalg.svd(rng.random((n, n)), compute_uv=False)
        return {"correct": True, "detail": "unerwartet fertig", "pid": os.getpid()}


class NumpyLeakTask(Task):
    """Behaelt NumPy-Puffer ueber das Taskende hinaus."""

    task_info = TaskInfo(name="proto.numpy_leak", queues=["cpu"])

    _leaked: ClassVar[list[Any]] = []

    async def run(self, mb: int = 40) -> int:
        NumpyLeakTask._leaked.append(np.ones(mb * 1024 * 1024 // 8, dtype=np.float64))
        return sum(a.nbytes for a in NumpyLeakTask._leaked)


def rss_mb() -> float:
    with open("/proc/self/status") as fh:
        for line in fh:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024
    return -1.0
