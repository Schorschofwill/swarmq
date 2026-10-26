"""Worker-Parallelität von swarmq stufenweise — swarmq-eigenes Architektur-Szenario."""

from __future__ import annotations

import time

from scenarios.base import (
    BenchmarkResult,
    make_result,
    run_timed_batch_concurrent,
    run_warm_up,
)

STUFEN = (1, 5, 10, 25, 50, 100, 200)


async def run_semaphore_scale(
    system_cls: type,
    n: int = 5000,
    sleep_ms: int = 10,
) -> BenchmarkResult:
    """Run SwarmQ at various worker concurrency levels to find the saturation point.

    Jede Stufe startet den Worker mit ``--concurrency`` = Stufe (``parallelitaet.py``).
    Die Ergebnisabfrage im Erzeuger läuft immer mit der höchsten Stufe, damit sie
    die Messung nicht begrenzt. Prints a progression table for each level.
    Returns the result for the default concurrency (10).
    """
    abfrage = max(STUFEN)
    default_result = None

    for stufe in STUFEN:
        system = system_cls(parallelitaet=str(stufe))

        await system.setup()
        await system.start_worker()

        try:
            await run_warm_up(system, "io_task")

            payloads = [{"sleep_ms": sleep_ms}] * n
            t_start = time.perf_counter()
            latencies = await run_timed_batch_concurrent(system, "io_task", payloads, concurrency=abfrage)
            t_end = time.perf_counter()

            result = make_result(
                system_name=f"swarmq-c{stufe}",
                scenario_name="semaphore_scale",
                params={"n": n, "sleep_ms": sleep_ms, "concurrency": stufe, "abfrage": abfrage},
                latencies=latencies,
                total_duration=t_end - t_start,
            )
            print(
                f"    c={stufe:>3}: {result.throughput:>8.0f} tasks/s, "
                f"P50={result.p50 * 1000:>6.1f}ms, P99={result.p99 * 1000:>6.1f}ms"
            )

            if stufe == 10:
                default_result = result
        finally:
            await system.stop_worker()
            await system.teardown()

    return default_result or result
