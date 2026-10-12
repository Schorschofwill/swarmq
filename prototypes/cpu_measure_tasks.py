"""Task-Modul fuer den Messnachweis der I-Serie gegen die Implementierung (0.5.0).

Beim Import wird nichts gerechnet (Auflage B1); der Forkserver laedt das
Modul per Preload, die Rechenkinder finden die Klassen in der Registry.
Queue-Name per ``CPU_MEASURE_QUEUE``, damit jeder Lauf frisch startet.
"""

from __future__ import annotations

import os
import time
from pathlib import Path

from swarmq.task import Task, TaskInfo

QUEUE = os.environ.get("CPU_MEASURE_QUEUE", "cpu-measure")


class MeasureEcho(Task):
    """M0: Startlatenz — Zeitstempel beim Eintritt in ``run`` im Kind."""

    task_info = TaskInfo(name="measure_echo", queues=[QUEUE], max_retries=0)

    async def run(self, x: int) -> dict[str, float | int]:
        return {"x": x, "run_at": time.time(), "pid": os.getpid()}


class MeasureSleep(Task):
    """I1: rechnender Task waehrend des Shutdowns."""

    task_info = TaskInfo(name="measure_sleep", queues=[QUEUE], max_retries=0)

    async def run(self, seconds: float) -> float:
        time.sleep(seconds)
        return time.time()


class MeasureKillMe(Task):
    """I2: der erste Versuch schreibt seine PID und rechnet; er wird von aussen
    per SIGKILL getoetet. Der zweite Versuch (Retry) liefert das Ergebnis."""

    task_info = TaskInfo(
        name="measure_kill_me",
        queues=[QUEUE],
        max_retries=1,
        retry_backoff="fixed",
        retry_base_delay=0.5,
    )

    async def run(self, pid_file: str, marker_dir: str) -> str:
        attempts = len(list(Path(marker_dir).glob("attempt-*"))) + 1
        (Path(marker_dir) / f"attempt-{attempts}").write_text(str(os.getpid()), encoding="ascii")
        if attempts == 1:
            Path(pid_file).write_text(str(os.getpid()), encoding="ascii")
            time.sleep(30)
        return f"attempt-{attempts}"


class MeasureCompute(Task):
    """I3: reine Rechenzeit im Kind, der Loop-Prozess bleibt frei."""

    task_info = TaskInfo(name="measure_compute", queues=[QUEUE], max_retries=0)

    async def run(self, seconds: float) -> int:
        deadline = time.monotonic() + seconds
        iterations = 0
        acc = 0.0
        while time.monotonic() < deadline:
            for i in range(50_000):
                acc += (i * 0.5) ** 0.5
            iterations += 1
        return iterations


class MeasureFail(Task):
    """I5: typerhaltende Exception, drei Versuche, dann DLQ."""

    task_info = TaskInfo(
        name="measure_fail",
        queues=[QUEUE],
        max_retries=2,
        retry_backoff="fixed",
        retry_base_delay=0.3,
    )

    async def run(self) -> None:
        raise ValueError("typed failure from the compute child")
