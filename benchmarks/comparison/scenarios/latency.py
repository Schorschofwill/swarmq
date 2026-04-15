"""Sustained-load latency scenario — measures tail latency under constant enqueue rate."""

from __future__ import annotations

import asyncio
import time

from scenarios.base import (
    BenchmarkResult,
    make_result,
    run_warm_up,
)


async def run_latency(
    system_cls: type,
    rate: int = 100,
    duration_s: int = 30,
    sleep_ms: int = 10,
    poll_interval: float = 0.01,
) -> BenchmarkResult:
    """Sustain a constant enqueue rate, measure per-task E2E latency.

    Parameters:
        system_cls: TaskQueueSystem class to instantiate
        rate: Tasks per second to enqueue
        duration_s: How long to sustain the rate
        sleep_ms: Simulated work per task (ms)
        poll_interval: Result polling interval (seconds)
    """
    system = system_cls()

    await system.setup()
    await system.start_worker()

    try:
        await run_warm_up(system, "io_task", poll_interval=poll_interval)

        interval = 1.0 / rate
        total_tasks = rate * duration_s
        latencies: list[float] = []
        pending: list[tuple[str, float]] = []  # (task_id, t_start)
        collected = 0

        # Background result collector
        async def collect_results():
            nonlocal collected
            while collected < total_tasks:
                # Check pending results
                still_pending = []
                for task_id, t_start in pending:
                    try:
                        await system.get_result(task_id, timeout=0.0, poll_interval=0)
                        latencies.append(time.perf_counter() - t_start)
                        collected += 1
                    except (TimeoutError, Exception):
                        still_pending.append((task_id, t_start))
                pending.clear()
                pending.extend(still_pending)
                await asyncio.sleep(poll_interval)

        collector_task = asyncio.create_task(collect_results())

        # Enqueue at constant rate
        t_scenario_start = time.perf_counter()
        for i in range(total_tasks):
            t_start = time.perf_counter()
            task_id = await system.enqueue("io_task", {"sleep_ms": sleep_ms})
            pending.append((task_id, t_start))

            # Rate limit
            elapsed = time.perf_counter() - t_scenario_start
            expected = (i + 1) * interval
            if elapsed < expected:
                await asyncio.sleep(expected - elapsed)

        # Wait for all results
        try:
            await asyncio.wait_for(collector_task, timeout=60.0)
        except asyncio.TimeoutError:
            collector_task.cancel()

        t_scenario_end = time.perf_counter()

        return make_result(
            system_name=system.name,
            scenario_name="latency",
            params={"rate": rate, "duration_s": duration_s, "sleep_ms": sleep_ms},
            latencies=latencies,
            total_duration=t_scenario_end - t_scenario_start,
        )
    finally:
        await system.stop_worker()
        await system.teardown()
