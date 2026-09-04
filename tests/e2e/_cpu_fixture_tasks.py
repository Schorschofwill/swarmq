"""Task-Modul fuer die CPU-Modus-Abnahme (Plan 2026-09-03-001, U7).

Muss ein importierbares Modul sein: der Forkserver laedt es per Preload,
und das Rechenkind sucht die Task-Klassen in ``Task._registered_tasks`` —
in Testfunktionen definierte Klassen existieren im Kind nicht.
"""

from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path

from swarmq.exceptions import NoRetry
from swarmq.task import Task, TaskInfo

QUEUE = os.environ.get("CPU_FIXTURE_QUEUE", "cpu-fixture")


class CpuEcho(Task):
    task_info = TaskInfo(name="cpu_echo", queues=[QUEUE], max_retries=0)

    async def run(self, x: int) -> dict[str, int]:
        return {"value": x * 2, "pid": os.getpid()}


class CpuFail(Task):
    task_info = TaskInfo(name="cpu_fail", queues=[QUEUE], max_retries=0)

    async def run(self) -> None:
        raise NoRetry("deterministic failure in compute child")


class CpuHang(Task):
    """Blockiert den Kind-Loop synchron — nur ein Kill beendet das (R8)."""

    task_info = TaskInfo(name="cpu_hang", queues=[QUEUE], hard_timeout=1.0, max_retries=0)

    async def run(self, seconds: float = 30.0) -> None:
        time.sleep(seconds)


class CpuSubprocess(Task):
    """Startet einen Enkel und blockiert; der Kill muss den Enkel mitnehmen (AE13)."""

    task_info = TaskInfo(name="cpu_subprocess", queues=[QUEUE], hard_timeout=1.0, max_retries=0)

    async def run(self, pid_file: str) -> None:
        proc = subprocess.Popen(["sleep", "60"])
        Path(pid_file).write_text(str(proc.pid), encoding="ascii")
        time.sleep(30)


class CpuSlowOnce(Task):
    """Chaos-Fixture: die erste Zustellung schreibt die Kind-PID und rechnet lange;
    eine Wiederzustellung (PID-Datei vorhanden) endet sofort."""

    task_info = TaskInfo(name="cpu_slow_once", queues=[QUEUE], max_retries=0)

    async def run(self, pid_file: str, seconds: float = 30.0) -> str:
        path = Path(pid_file)
        if path.exists():
            return "redelivered"
        path.write_text(str(os.getpid()), encoding="ascii")
        time.sleep(seconds)
        return "first"


try:  # DI-Tasks nur mit installiertem dishka (test-Extra)
    from dishka import FromDishka, Provider, Scope, make_async_container, provide

    from swarmq.dishka import SwarmQProvider, setup_dishka

    class PidHolder:
        def __init__(self) -> None:
            self.pid = os.getpid()

    class _Prov(Provider):
        @provide(scope=Scope.APP)
        def holder(self) -> PidHolder:
            return PidHolder()

    setup_dishka(make_async_container(_Prov(), SwarmQProvider()))

    class CpuDi(Task):
        task_info = TaskInfo(name="cpu_di", queues=[QUEUE], max_retries=0)

        async def run(self, holder: FromDishka[PidHolder]) -> dict[str, int]:  # type: ignore[valid-type]
            return {"app_pid": holder.pid, "pid": os.getpid()}

except ImportError:  # pragma: no cover - ohne dishka keine DI-Tasks
    pass
