"""Shared benchmark data structures and helper functions."""

from __future__ import annotations

import asyncio
import json
import statistics
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from systems.base import TaskQueueSystem


# ---------------------------------------------------------------------------
# Result data structures
# ---------------------------------------------------------------------------


@dataclass
class BenchmarkResult:
    """Result of a single benchmark run."""

    system_name: str
    scenario_name: str
    params: dict[str, Any]
    throughput: float  # tasks/s
    latencies: list[float] = field(default_factory=list)  # seconds
    total_duration: float = 0.0  # seconds
    memory_peak_mb: float = 0.0

    @property
    def p50(self) -> float:
        if not self.latencies:
            return 0.0
        s = sorted(self.latencies)
        return s[len(s) // 2]

    @property
    def p95(self) -> float:
        if not self.latencies:
            return 0.0
        s = sorted(self.latencies)
        return s[int(len(s) * 0.95)]

    @property
    def p99(self) -> float:
        if not self.latencies:
            return 0.0
        s = sorted(self.latencies)
        return s[int(len(s) * 0.99)]

    def to_dict(self) -> dict[str, Any]:
        return {
            "system_name": self.system_name,
            "scenario_name": self.scenario_name,
            "params": self.params,
            "throughput": round(self.throughput, 2),
            "total_duration": round(self.total_duration, 4),
            "memory_peak_mb": round(self.memory_peak_mb, 2),
            "latency_p50_ms": round(self.p50 * 1000, 3),
            "latency_p95_ms": round(self.p95 * 1000, 3),
            "latency_p99_ms": round(self.p99 * 1000, 3),
            "latency_count": len(self.latencies),
        }


@dataclass
class AggregatedResult:
    """Aggregation of multiple runs (median + min/max)."""

    system_name: str
    scenario_name: str
    params: dict[str, Any]
    runs: int
    throughput_median: float
    throughput_min: float
    throughput_max: float
    p50_median_ms: float
    p95_median_ms: float
    p99_median_ms: float
    duration_median: float
    memory_peak_median_mb: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "system_name": self.system_name,
            "scenario_name": self.scenario_name,
            "params": self.params,
            "runs": self.runs,
            "throughput_median": round(self.throughput_median, 2),
            "throughput_min": round(self.throughput_min, 2),
            "throughput_max": round(self.throughput_max, 2),
            "latency_p50_median_ms": round(self.p50_median_ms, 3),
            "latency_p95_median_ms": round(self.p95_median_ms, 3),
            "latency_p99_median_ms": round(self.p99_median_ms, 3),
            "duration_median": round(self.duration_median, 4),
            "memory_peak_median_mb": round(self.memory_peak_median_mb, 2),
        }


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


async def run_warm_up(
    system: TaskQueueSystem,
    task_name: str,
    n: int = 100,
    poll_interval: float = 0.01,
) -> None:
    """Enqueue and await n warm-up tasks. Fully completes before returning."""
    task_ids = []
    for _ in range(n):
        tid = await system.enqueue(task_name, {})
        task_ids.append(tid)

    # Await all results to ensure warm-up is fully processed
    await asyncio.gather(*(system.get_result(tid, timeout=30.0, poll_interval=poll_interval) for tid in task_ids))


async def run_timed_batch(
    system: TaskQueueSystem,
    task_name: str,
    payloads: list[dict],
    poll_interval: float = 0.01,
) -> list[float]:
    """Enqueue tasks one by one, measure enqueue-to-result latency for each.

    Returns list of latencies in seconds.
    """
    latencies = []
    for payload in payloads:
        t_start = time.perf_counter()
        task_id = await system.enqueue(task_name, payload)
        await system.get_result(task_id, timeout=60.0, poll_interval=poll_interval)
        t_end = time.perf_counter()
        latencies.append(t_end - t_start)
    return latencies


async def run_timed_batch_concurrent(
    system: TaskQueueSystem,
    task_name: str,
    payloads: list[dict],
    concurrency: int,
    poll_interval: float = 0.01,
) -> list[float]:
    """Enqueue all tasks, then gather results concurrently.

    Returns list of latencies in seconds (enqueue-to-result per task).
    """
    # Enqueue all tasks, recording start times
    enqueue_data: list[tuple[str, float]] = []
    for payload in payloads:
        t_start = time.perf_counter()
        task_id = await system.enqueue(task_name, payload)
        enqueue_data.append((task_id, t_start))

    # Gather results with bounded concurrency
    sem = asyncio.Semaphore(concurrency)
    latencies: list[float] = [0.0] * len(enqueue_data)

    async def _collect(idx: int, task_id: str, t_start: float) -> None:
        async with sem:
            await system.get_result(task_id, timeout=60.0, poll_interval=poll_interval)
            latencies[idx] = time.perf_counter() - t_start

    await asyncio.gather(*(_collect(i, tid, ts) for i, (tid, ts) in enumerate(enqueue_data)))
    return latencies


def make_result(
    system_name: str,
    scenario_name: str,
    params: dict[str, Any],
    latencies: list[float],
    total_duration: float,
) -> BenchmarkResult:
    """Build a BenchmarkResult from raw latency data."""
    throughput = len(latencies) / total_duration if total_duration > 0 else 0.0
    return BenchmarkResult(
        system_name=system_name,
        scenario_name=scenario_name,
        params=params,
        throughput=throughput,
        latencies=latencies,
        total_duration=total_duration,
    )


def aggregate_runs(results: list[BenchmarkResult]) -> AggregatedResult:
    """Compute median + min/max across multiple runs."""
    if not results:
        raise ValueError("Cannot aggregate empty results list")

    throughputs = [r.throughput for r in results]
    p50s = [r.p50 * 1000 for r in results]  # convert to ms
    p95s = [r.p95 * 1000 for r in results]
    p99s = [r.p99 * 1000 for r in results]
    durations = [r.total_duration for r in results]
    memories = [r.memory_peak_mb for r in results]

    return AggregatedResult(
        system_name=results[0].system_name,
        scenario_name=results[0].scenario_name,
        params=results[0].params,
        runs=len(results),
        throughput_median=statistics.median(throughputs),
        throughput_min=min(throughputs),
        throughput_max=max(throughputs),
        p50_median_ms=statistics.median(p50s),
        p95_median_ms=statistics.median(p95s),
        p99_median_ms=statistics.median(p99s),
        duration_median=statistics.median(durations),
        memory_peak_median_mb=statistics.median(memories),
    )


def save_result(result: BenchmarkResult, results_dir: Path) -> Path:
    """Save a BenchmarkResult as JSON to the results directory."""
    results_dir.mkdir(parents=True, exist_ok=True)
    timestamp = int(time.time())
    filename = f"{result.scenario_name}_{result.system_name}_{timestamp}.json"
    path = results_dir / filename
    path.write_text(json.dumps(result.to_dict(), indent=2))
    return path


def load_results(results_dir: Path) -> list[dict[str, Any]]:
    """Load all JSON result files from a directory."""
    results = []
    if not results_dir.exists():
        return results
    for f in sorted(results_dir.glob("*.json")):
        results.append(json.loads(f.read_text()))
    return results
