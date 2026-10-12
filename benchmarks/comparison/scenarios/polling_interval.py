"""Polling interval analysis — SwarmQ-only architecture scenario."""

from __future__ import annotations

import time

from scenarios.base import (
    BenchmarkResult,
    make_result,
    run_timed_batch,
    run_warm_up,
)


async def run_polling_interval(
    system_cls: type,
    n: int = 1000,
    sleep_ms: int = 10,
) -> BenchmarkResult:
    """Test SwarmQ result latency at various polling intervals.

    Intervals: 1ms, 10ms, 50ms, 200ms.
    Prints a comparison table.
    Returns the result for the 10ms interval (the benchmark default).
    """
    from systems.swarmq_sys import SwarmQSystem

    default_result = None
    intervals = [0.001, 0.01, 0.05, 0.2]

    for poll_interval in intervals:
        system = SwarmQSystem()
        system.name = f"swarmq-poll{int(poll_interval * 1000)}ms"

        await system.setup()
        await system.start_worker()

        try:
            await run_warm_up(system, "io_task", poll_interval=poll_interval)

            payloads = [{"sleep_ms": sleep_ms}] * n
            t_start = time.perf_counter()
            latencies = await run_timed_batch(system, "io_task", payloads, poll_interval=poll_interval)
            t_end = time.perf_counter()

            result = make_result(
                system_name=f"swarmq-poll{int(poll_interval * 1000)}ms",
                scenario_name="polling_interval",
                params={"n": n, "sleep_ms": sleep_ms, "poll_interval_ms": poll_interval * 1000},
                latencies=latencies,
                total_duration=t_end - t_start,
            )
            print(
                f"    poll={poll_interval * 1000:>5.0f}ms: {result.throughput:>8.0f} tasks/s, "
                f"P50={result.p50 * 1000:>6.1f}ms, P99={result.p99 * 1000:>6.1f}ms"
            )

            if poll_interval == 0.01:
                default_result = result
        finally:
            await system.stop_worker()
            await system.teardown()

    return default_result or result
