"""I/O-bound scenario — measures concurrency efficiency under simulated I/O load."""

from __future__ import annotations

import time

from scenarios.base import (
    BenchmarkResult,
    make_result,
    run_timed_batch_concurrent,
    run_warm_up,
)


async def run_io_bound(
    system_cls: type,
    n: int = 1000,
    sleep_ms: int = 10,
    concurrency: int = 10,
) -> BenchmarkResult:
    """Enqueue N I/O tasks, gather results with bounded concurrency.

    Parameters:
        system_cls: TaskQueueSystem class to instantiate
        n: Number of tasks
        sleep_ms: Simulated I/O duration per task (ms)
        concurrency: Max concurrent result fetches
    """
    system = system_cls()

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

        return make_result(
            system_name=system.name,
            scenario_name="io_bound",
            params={"n": n, "sleep_ms": sleep_ms, "concurrency": concurrency},
            latencies=latencies,
            total_duration=t_end - t_start,
        )
    finally:
        await system.stop_worker()
        await system.teardown()
