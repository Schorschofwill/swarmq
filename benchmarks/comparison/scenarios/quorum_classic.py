"""Quorum vs Classic queue overhead — SwarmQ-only architecture scenario."""

from __future__ import annotations

import time

from scenarios.base import (
    BenchmarkResult,
    make_result,
    run_timed_batch,
    run_warm_up,
)


async def run_quorum_classic(
    system_cls: type,
    n: int = 10000,
) -> BenchmarkResult:
    """Compare quorum vs classic queue throughput using SwarmQ.

    Creates two SwarmQ adapters — one with quorum queues, one with classic.
    Returns a combined result; the runner should call this once.
    The scenario internally runs both variants and prints comparison.
    """
    from garantieprofile import STARK
    from systems.swarmq_sys import SwarmQSystem

    results = []
    for queue_type, label in [("quorum", "swarmq-quorum"), ("classic", "swarmq-classic")]:
        system = SwarmQSystem(profil=STARK, queue_type=queue_type)
        system.name = label

        await system.setup()
        await system.start_worker()

        try:
            await run_warm_up(system, "noop")

            payloads = [{}] * n
            t_start = time.perf_counter()
            latencies = await run_timed_batch(system, "noop", payloads)
            t_end = time.perf_counter()

            result = make_result(
                system_name=label,
                scenario_name="quorum_classic",
                params={"n": n, "queue_type": queue_type},
                latencies=latencies,
                total_duration=t_end - t_start,
            )
            results.append(result)
            print(f"    {label}: {result.throughput:.0f} tasks/s, P50={result.p50 * 1000:.1f}ms")
        finally:
            await system.stop_worker()
            await system.teardown()

    # Return the quorum result as the primary (first) result
    return results[0]
