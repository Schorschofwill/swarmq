"""Semaphore-concurrency scaling — SwarmQ-only architecture scenario."""

from __future__ import annotations

import time

from scenarios.base import (
    BenchmarkResult,
    make_result,
    run_timed_batch_concurrent,
    run_warm_up,
)


async def run_semaphore_scale(
    system_cls: type,
    n: int = 5000,
    sleep_ms: int = 10,
) -> BenchmarkResult:
    """Run SwarmQ at various concurrency levels to find the saturation point.

    Tests concurrency: 1, 5, 10, 25, 50, 100, 200.
    Prints a progression table for each level.
    Returns the result for the default concurrency (10).
    """
    from systems.swarmq_sys import SwarmQSystem

    default_result = None
    levels = [1, 5, 10, 25, 50, 100, 200]

    for concurrency in levels:
        system = SwarmQSystem()
        system.name = f"swarmq-c{concurrency}"

        await system.setup()
        await system.start_worker()

        try:
            await run_warm_up(system, "io_task")

            payloads = [{"sleep_ms": sleep_ms}] * n
            t_start = time.perf_counter()
            latencies = await run_timed_batch_concurrent(
                system, "io_task", payloads, concurrency=concurrency
            )
            t_end = time.perf_counter()

            result = make_result(
                system_name=f"swarmq-c{concurrency}",
                scenario_name="semaphore_scale",
                params={"n": n, "sleep_ms": sleep_ms, "concurrency": concurrency},
                latencies=latencies,
                total_duration=t_end - t_start,
            )
            print(f"    c={concurrency:>3}: {result.throughput:>8.0f} tasks/s, "
                  f"P50={result.p50*1000:>6.1f}ms, P99={result.p99*1000:>6.1f}ms")

            if concurrency == 10:
                default_result = result
        finally:
            await system.stop_worker()
            await system.teardown()

    return default_result or result
