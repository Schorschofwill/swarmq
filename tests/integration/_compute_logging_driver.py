"""Treiber fuer ``test_compute_child_logging``: echter Forkserver und SlotPool, ohne Broker.

Aufruf: ``python -m tests.integration._compute_logging_driver <preload-modul> <spec-json>``.
Eigener Prozess, weil der Forkserver ein Singleton je Prozess ist und die Kinder
sein stderr erben. Der Elternprozess richtet sich eingebaut mit JSON ein, wie ein
Worker; auf stdout steht am Ende eine JSON-Zeile mit Thread-Zahl und Ergebnissen.
"""

from __future__ import annotations

import asyncio
import json
import sys


def main() -> None:
    from swarmq.logging import configure_logging, shutdown_logging
    from swarmq.logging_settings import LoggingSettings
    from swarmq.worker.compute.envelope import ComputeJob
    from swarmq.worker.compute.forkserver import start_forkserver
    from swarmq.worker.compute.pool import SlotPool

    module, spec = sys.argv[1], json.loads(sys.argv[2])
    configure_logging(LoggingSettings(format="json"))
    handle = start_forkserver(["swarmq.logging", "swarmq.worker.compute.child", module])

    async def run() -> list[object]:
        pool = SlotPool(handle.context, 1, recycle_rss_mb=None, recycle_max_tasks=None, end_grace_s=5.0)
        await pool.start()
        values: list[object] = []
        try:
            for i, use_di in enumerate(spec["use_di"]):
                job = ComputeJob(
                    task_id=f"j{i}", task_name="child_logging_task", queue="q",
                    args=(f"j{i}",), kwargs={}, **spec["job"],
                )
                values.append((await pool.run(job, use_di=use_di)).value)
        finally:
            await pool.aclose()
        return values

    values = asyncio.run(run())
    shutdown_logging()
    print(json.dumps({"threads": handle.threads, "results": values}), flush=True)


if __name__ == "__main__":
    main()
