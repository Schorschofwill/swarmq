"""Task-Modul fuer den CPU-Worker-Machbarkeitstest.

Liegt bewusst in einem eigenen Modul: der ``forkserver`` laedt es EINMAL
vor (``set_forkserver_preload``), danach erben alle Rechenkinder die
Importe per Copy-on-Write. Das entspricht der Realitaet — beim Betreiber
ist das ``myapp.tasks``.

Enthaelt echte ``swarmq.Task``-Subklassen mit echter Dishka-Injektion,
damit der Test die tatsaechliche Maschinerie prueft und keine Nachbildung.
"""

from __future__ import annotations

import os
import time
from typing import Any, ClassVar

from swarmq.task import Task, TaskInfo

# ---------------------------------------------------------------------------
# Dishka-Provider: eine APP-scoped "Verbindung" und eine REQUEST-scoped
# "Session". Beide merken sich, in welchem Prozess sie gebaut wurden — genau
# das ist die Frage, die der Test beantworten muss.
# ---------------------------------------------------------------------------

try:
    from dishka import Provider, Scope, make_async_container, provide

    from swarmq.dishka import SwarmQProvider, TaskContext, setup_dishka

    _HAVE_DISHKA = True
except ImportError:  # pragma: no cover — Test meldet das selbst
    _HAVE_DISHKA = False


class Database:
    """Steht fuer eine APP-scoped Ressource (Connection-Pool).

    ``built_in_pid`` ist der Beweis, in welchem Prozess der APP-Scope
    materialisiert wurde. ``instances`` zaehlt, wie oft im aktuellen
    Prozess gebaut wurde — belegt die Lebensdauer des APP-Singletons.
    """

    instances = 0

    def __init__(self) -> None:
        Database.instances += 1
        self.built_in_pid = os.getpid()
        self.instance_no = Database.instances

    def query(self) -> str:
        return f"db(pid={self.built_in_pid},no={self.instance_no})"


class Session:
    """REQUEST-scoped Ressource, haengt an der konkreten Task-Ausfuehrung."""

    def __init__(self, db: Database, ctx: Any) -> None:
        self.db = db
        self.task_id = getattr(ctx, "task_id", "<kein TaskContext>")


if _HAVE_DISHKA:

    class AppProvider(Provider):
        @provide(scope=Scope.APP)
        def database(self) -> Database:
            return Database()

        @provide(scope=Scope.REQUEST)
        def session(self, db: Database, ctx: TaskContext) -> Session:
            return Session(db, ctx)

    # Auf Modulebene registriert — exakt wie die swarmq-Doku es vorschreibt,
    # damit jeder neue Prozess den Container beim Re-Import frisch aufbaut.
    setup_dishka(make_async_container(AppProvider(), SwarmQProvider()))


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

if _HAVE_DISHKA:
    from dishka import FromDishka

    class ImageTask(Task):
        """Der Kernfall: DI + Fortschritt + echte CPU-Arbeit.

        Die Signatur ist unveraendert gegenueber einem normalen swarmq-Task —
        genau das ist die Zusage, die dieser Test belegen soll.
        """

        task_info = TaskInfo(name="feasibility.image", queues=["cpu"])

        async def run(self, steps: int, db: FromDishka[Database], sess: FromDishka[Session]) -> dict[str, Any]:
            checksum = 0
            for step in range(steps):
                # Echte CPU-Arbeit, ohne await-Punkt innerhalb des Schritts.
                for i in range(8_000_000):
                    checksum += i * i
                await self.update_progress(step + 1, steps, f"Schritt {step + 1}")
            return {
                "pid": os.getpid(),
                "db": db.query(),
                "db_instance_no": db.instance_no,
                "session_task_id": sess.task_id,
                "checksum": checksum,
            }


class LeakTask(Task):
    """Behaelt Speicher ueber das Taskende hinaus — simuliert den bewiesenen
    Pillow-/ImageMagick-Leak."""

    task_info = TaskInfo(name="feasibility.leak", queues=["cpu"])

    _leaked: ClassVar[list[bytes]] = []

    async def run(self, mb: int = 40) -> int:
        LeakTask._leaked.append(b"x" * (mb * 1024 * 1024))
        return sum(len(b) for b in LeakTask._leaked)


class HangTask(Task):
    """Blockiert ohne einen einzigen await-Punkt — der Fall, den swarmq
    heute nicht abbrechen kann."""

    task_info = TaskInfo(name="feasibility.hang", queues=["cpu"])

    async def run(self) -> None:
        while True:
            pass


class FailTask(Task):
    """Wirft — der Retry-/DLQ-Pfad muss die Exception ueber die
    Prozessgrenze unveraendert erhalten."""

    task_info = TaskInfo(name="feasibility.fail", queues=["cpu"])

    async def run(self) -> None:
        raise ValueError("absichtlicher Fehler aus dem Rechenkind")


def rss_mb() -> float:
    """RSS des aktuellen Prozesses in MB (Linux)."""
    with open("/proc/self/status") as fh:
        for line in fh:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024
    return -1.0


def preload_marker() -> str:
    """Beweist, dass dieses Modul zum Fork-Zeitpunkt bereits geladen war."""
    return f"preloaded_at={_IMPORTED_AT:.3f} pid={os.getpid()}"


_IMPORTED_AT = time.time()
