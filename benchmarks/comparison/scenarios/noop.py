"""No-op throughput scenario — measures pure framework overhead."""

from __future__ import annotations

import time

from scenarios.base import (
    BenchmarkResult,
    make_result,
    run_timed_batch,
    run_warm_up,
)


async def run_noop(system_cls: type, n: int = 1000) -> BenchmarkResult:
    """Enqueue N no-op tasks sequentially, measure throughput and latency.

    Parameters:
        system_cls: TaskQueueSystem class to instantiate
        n: Number of tasks (default 1000)
    """
    system = system_cls()

    await system.setup()
    await system.start_worker()

    try:
        # Warm-up
        await run_warm_up(system, "noop")

        # Measurement
        payloads = [{}] * n
        t_start = time.perf_counter()
        latencies = await run_timed_batch(system, "noop", payloads)
        t_end = time.perf_counter()

        return make_result(
            system_name=system.name,
            scenario_name="noop",
            params={"n": n},
            latencies=latencies,
            total_duration=t_end - t_start,
        )
    finally:
        await system.stop_worker()
        await system.teardown()
