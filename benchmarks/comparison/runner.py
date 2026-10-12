#!/usr/bin/env python3
"""Benchmark runner — orchestrates scenario execution across systems.

Usage:
    python runner.py --scenario noop,io_bound --systems swarmq,taskiq --runs 3
    python runner.py --scenario all --systems all
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path

from scenarios.base import BenchmarkResult, save_result

RESULTS_DIR = Path(__file__).parent / "results"


def get_scenarios() -> dict[str, object]:
    """Static registry of available scenarios."""
    from scenarios.noop import run_noop
    from scenarios.io_bound import run_io_bound
    from scenarios.latency import run_latency
    from scenarios.scaling import run_scaling

    return {
        "noop": run_noop,
        "io_bound": run_io_bound,
        "latency": run_latency,
        "scaling": run_scaling,
    }


def get_swarmq_scenarios() -> dict[str, object]:
    """Static registry of SwarmQ-specific scenarios."""
    from scenarios.quorum_classic import run_quorum_classic
    from scenarios.semaphore_scale import run_semaphore_scale
    from scenarios.polling_interval import run_polling_interval

    return {
        "quorum_classic": run_quorum_classic,
        "semaphore_scale": run_semaphore_scale,
        "polling_interval": run_polling_interval,
    }


def get_systems() -> dict[str, type]:
    """Static registry of available systems."""
    from systems.swarmq_sys import SwarmQSystem
    from systems.taskiq_sys import TaskIQSystem
    from systems.taskiq_rmq_sys import TaskIQRabbitMQSystem

    return {
        "swarmq": SwarmQSystem,
        "taskiq": TaskIQSystem,
        "taskiq-rmq": TaskIQRabbitMQSystem,
    }


async def run_scenario(
    scenario_fn,
    system_cls: type,
    runs: int,
    results_dir: Path,
) -> list[BenchmarkResult]:
    """Run a scenario N times against a system, saving each result."""
    all_results = []
    for run_idx in range(runs):
        print(f"  Run {run_idx + 1}/{runs}...", end=" ", flush=True)
        t0 = time.perf_counter()

        try:
            result = await scenario_fn(system_cls)
            save_result(result, results_dir)
            all_results.append(result)
            elapsed = time.perf_counter() - t0
            print(f"done ({elapsed:.1f}s, {result.throughput:.0f} tasks/s)")
        except Exception as exc:
            elapsed = time.perf_counter() - t0
            print(f"FAILED ({elapsed:.1f}s): {exc}")

    return all_results


async def main(args: argparse.Namespace) -> None:
    scenarios = get_scenarios()
    swarmq_scenarios = get_swarmq_scenarios()
    systems = get_systems()

    # Resolve "all"
    selected_scenarios = (
        list(scenarios.keys()) if args.scenario == "all" else [s.strip() for s in args.scenario.split(",")]
    )
    selected_systems = list(systems.keys()) if args.systems == "all" else [s.strip() for s in args.systems.split(",")]

    print(f"Benchmark: {selected_scenarios} × {selected_systems} ({args.runs} runs each)")
    print(f"Results → {RESULTS_DIR}")
    print()

    for scenario_name in selected_scenarios:
        # Check if it's a SwarmQ-specific scenario
        if scenario_name in swarmq_scenarios:
            scenario_fn = swarmq_scenarios[scenario_name]
            print(f"[{scenario_name}] (SwarmQ-only)")
            await run_scenario(scenario_fn, systems["swarmq"], args.runs, RESULTS_DIR)
            print()
            continue

        if scenario_name not in scenarios:
            print(f"[{scenario_name}] SKIPPED — unknown scenario")
            continue

        scenario_fn = scenarios[scenario_name]

        for system_name in selected_systems:
            if system_name not in systems:
                print(f"[{scenario_name}/{system_name}] SKIPPED — unknown system")
                continue

            system_cls = systems[system_name]
            print(f"[{scenario_name}/{system_name}]")
            await run_scenario(scenario_fn, system_cls, args.runs, RESULTS_DIR)

        print()

    print("Done. Run `python report.py` to see results.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark comparison runner")
    parser.add_argument(
        "--scenario",
        default="all",
        help="Comma-separated scenario names or 'all' (default: all)",
    )
    parser.add_argument(
        "--systems",
        default="all",
        help="Comma-separated system names or 'all' (default: all)",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=3,
        help="Number of runs per scenario/system combo (default: 3)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    try:
        import uvloop

        uvloop.install()
    except ImportError:
        pass

    args = parse_args()
    asyncio.run(main(args))
