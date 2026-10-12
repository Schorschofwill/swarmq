"""Concurrency scaling scenario — measures throughput vs. worker concurrency."""

from __future__ import annotations

import time

from scenarios.base import (
    BenchmarkResult,
    make_result,
    run_timed_batch_concurrent,
    run_warm_up,
)


async def run_scaling(
    system_cls: type,
    n: int = 1000,
    sleep_ms: int = 10,
    concurrency: int = 10,
) -> BenchmarkResult:
    """Run N tasks at a given concurrency level, measure throughput.

    Parameters:
        system_cls: TaskQueueSystem class to instantiate
        n: Number of tasks
        sleep_ms: Simulated work per task (ms)
        concurrency: Worker concurrency level
    """
    system = system_cls()

    await system.setup()
    await system.start_worker()

    try:
        await run_warm_up(system, "io_task")

        payloads = [{"sleep_ms": sleep_ms}] * n
        t_start = time.perf_counter()
        latencies = await run_timed_batch_concurrent(system, "io_task", payloads, concurrency=concurrency)
        t_end = time.perf_counter()

        return make_result(
            system_name=system.name,
            scenario_name="scaling",
            params={"n": n, "sleep_ms": sleep_ms, "concurrency": concurrency},
            latencies=latencies,
            total_duration=t_end - t_start,
        )
    finally:
        await system.stop_worker()
        await system.teardown()
