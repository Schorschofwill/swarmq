#!/usr/bin/env python3
"""Benchmark report generator — reads results/ and prints comparison tables.

Usage:
    python report.py [results_dir]
"""

from __future__ import annotations

import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

RESULTS_DIR = Path(__file__).parent / "results"

# Systems using RabbitMQ as broker
RABBITMQ_SYSTEMS = {"swarmq", "swarmq-quorum", "swarmq-classic", "taskiq-rmq"}
# Systems using Redis/Valkey as broker
REDIS_SYSTEMS = {"taskiq", "celery", "dramatiq"}


def load_results(results_dir: Path) -> list[dict]:
    results = []
    if not results_dir.exists():
        return results
    for f in sorted(results_dir.glob("*.json")):
        try:
            results.append(json.loads(f.read_text()))
        except (json.JSONDecodeError, OSError) as exc:
            print(f"Warning: skipping {f.name}: {exc}", file=sys.stderr)
    return results


def group_results(
    results: list[dict],
) -> dict[str, dict[str, list[dict]]]:
    """Group by scenario_name -> system_name -> [results]."""
    grouped: dict[str, dict[str, list[dict]]] = defaultdict(lambda: defaultdict(list))
    for r in results:
        grouped[r["scenario_name"]][r["system_name"]].append(r)
    return grouped


def median(values: list[float]) -> float:
    return statistics.median(values) if values else 0.0


def format_table(headers: list[str], rows: list[list[str]], col_widths: list[int] | None = None) -> str:
    if col_widths is None:
        col_widths = [max(len(h), *(len(str(r[i])) for r in rows)) for i, h in enumerate(headers)]

    lines = []
    header_line = " | ".join(h.ljust(w) for h, w in zip(headers, col_widths))
    lines.append(header_line)
    lines.append("-+-".join("-" * w for w in col_widths))
    for row in rows:
        lines.append(" | ".join(str(v).ljust(w) for v, w in zip(row, col_widths)))
    return "\n".join(lines)


def is_cross_broker(systems: set[str]) -> bool:
    """Check if comparison involves systems with different brokers."""
    has_rmq = bool(systems & RABBITMQ_SYSTEMS)
    has_redis = bool(systems & REDIS_SYSTEMS)
    return has_rmq and has_redis


def print_scenario_report(scenario: str, system_results: dict[str, list[dict]]) -> None:
    print(f"## {scenario}")

    systems_in_scenario = set(system_results.keys())
    if is_cross_broker(systems_in_scenario):
        print("  ⚠ Cross-Broker-Vergleich: RabbitMQ vs Redis — nicht direkt vergleichbar")

    headers = ["System", "Throughput", "P50 (ms)", "P95 (ms)", "P99 (ms)", "Duration (s)", "Runs"]
    rows = []

    for system_name in sorted(system_results.keys()):
        results = system_results[system_name]
        throughputs = [r["throughput"] for r in results]
        p50s = [r["latency_p50_ms"] for r in results]
        p95s = [r["latency_p95_ms"] for r in results]
        p99s = [r["latency_p99_ms"] for r in results]
        durations = [r["total_duration"] for r in results]

        rows.append(
            [
                system_name,
                f"{median(throughputs):>8.1f}",
                f"{median(p50s):>8.3f}",
                f"{median(p95s):>8.3f}",
                f"{median(p99s):>8.3f}",
                f"{median(durations):>8.2f}",
                str(len(results)),
            ]
        )

    print(format_table(headers, rows))
    print()


def check_decision_rules(grouped: dict[str, dict[str, list[dict]]]) -> None:
    """Check Phase 1 decision rules against results."""
    print("## Decision Rules Check (Phase 1)")
    print()

    # SwarmQ vs TaskIQ throughput (noop scenario)
    if "noop" in grouped:
        noop = grouped["noop"]
        swarmq_tp = median([r["throughput"] for r in noop.get("swarmq", [])])
        taskiq_tp = median([r["throughput"] for r in noop.get("taskiq", [])])

        if swarmq_tp > 0 and taskiq_tp > 0:
            ratio = swarmq_tp / taskiq_tp
            status = "⚠ ACTION NEEDED" if ratio < 0.5 else "✓ OK"
            print(f"  Throughput ratio (SwarmQ/TaskIQ): {ratio:.2f} — {status}")
            if ratio < 0.5:
                print("    → SwarmQ < 50% of TaskIQ → Profiling-Session empfohlen")
        else:
            print("  Throughput ratio: insufficient data")

    # P99 latency comparison
    if "latency" in grouped:
        lat = grouped["latency"]
        swarmq_p99 = median([r["latency_p99_ms"] for r in lat.get("swarmq", [])])
        taskiq_p99 = median([r["latency_p99_ms"] for r in lat.get("taskiq", [])])

        if swarmq_p99 > 0 and taskiq_p99 > 0:
            ratio = swarmq_p99 / taskiq_p99
            status = "⚠ ACTION NEEDED" if ratio > 5 else "✓ OK"
            print(f"  P99 latency ratio (SwarmQ/TaskIQ): {ratio:.2f} — {status}")
            if ratio > 5:
                print("    → SwarmQ P99 > 5× TaskIQ → Result-Polling überdenken")
        else:
            print("  P99 latency ratio: insufficient data")

    print()


def main() -> None:
    results_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else RESULTS_DIR

    results = load_results(results_dir)
    if not results:
        print(f"No results found in {results_dir}/")
        print("Run benchmarks first: python runner.py --scenario all --systems all")
        sys.exit(1)

    grouped = group_results(results)

    print(f"# Benchmark Report ({len(results)} results from {results_dir})")
    print()

    for scenario in sorted(grouped.keys()):
        print_scenario_report(scenario, grouped[scenario])

    check_decision_rules(grouped)


if __name__ == "__main__":
    main()
