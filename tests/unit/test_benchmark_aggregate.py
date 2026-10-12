"""Unit tests for the repeat/aggregate helpers in benchmarks/bench.py.

These are pure-Python tests: they construct ``BenchResult`` instances by
hand and verify that ``_aggregate_rps`` / ``_aggregate_latencies`` return
the right shape. No RabbitMQ, no Valkey, no Docker.

The aggregate layer exists so that ``--repeat N`` produces a stable
median + min/max over N runs instead of a single single-run number.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
BENCH_PATH = REPO_ROOT / "benchmarks" / "bench.py"


@pytest.fixture(scope="module")
def bench_mod():
    """Load benchmarks/bench.py as a module without executing the CLI.

    The file is not a package and lives outside ``src/``; import by file
    path so these tests do not depend on PYTHONPATH tricks. The aio_pika
    / valkey imports at module top run — that is acceptable here because
    they only fail when the wheel is not installed, which is the same
    precondition as running the benchmark itself.
    """
    spec = importlib.util.spec_from_file_location("bench_for_tests", BENCH_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _make_result(bench_mod, throughput: float, *, latencies: list[float] | None = None):
    return bench_mod.BenchResult(
        broker="Test",
        scenario="unit",
        messages=100,
        concurrency=1,
        duration_s=1.0,
        throughput_msg_s=throughput,
        latencies_ms=list(latencies) if latencies else [],
    )


class TestAggregateRps:
    def test_three_runs_compute_median_min_max(self, bench_mod):
        runs = [
            _make_result(bench_mod, 100),
            _make_result(bench_mod, 200),
            _make_result(bench_mod, 150),
        ]
        stats = bench_mod._aggregate_rps(runs)

        assert stats.count == 3
        assert stats.median == 150
        assert stats.min == 100
        assert stats.max == 200

    def test_single_run_collapses_to_same_value(self, bench_mod):
        """N=1 must not raise. Median == min == max == the single value."""
        stats = bench_mod._aggregate_rps([_make_result(bench_mod, 42)])

        assert stats.count == 1
        assert stats.median == 42
        assert stats.min == 42
        assert stats.max == 42

    def test_even_count_uses_statistics_median(self, bench_mod):
        """statistics.median of an even-length sequence is the mean of the two middle values."""
        runs = [_make_result(bench_mod, v) for v in (10, 20, 30, 40)]
        stats = bench_mod._aggregate_rps(runs)

        assert stats.median == 25  # (20 + 30) / 2
        assert stats.min == 10
        assert stats.max == 40

    def test_preserves_insertion_order_irrelevant(self, bench_mod):
        """Order of input must not affect median/min/max."""
        ordered = [_make_result(bench_mod, v) for v in (100, 200, 150)]
        reverse = [_make_result(bench_mod, v) for v in (200, 150, 100)]

        assert bench_mod._aggregate_rps(ordered) == bench_mod._aggregate_rps(reverse)

    def test_empty_list_raises(self, bench_mod):
        """Defensive guard: the orchestrator never calls with [], but fail
        loudly if it did rather than silently returning garbage."""
        with pytest.raises(ValueError, match="no results"):
            bench_mod._aggregate_rps([])


class TestAggregateLatencies:
    def test_three_runs_each_with_latencies(self, bench_mod):
        runs = [
            _make_result(bench_mod, 100, latencies=[1.0, 2.0, 3.0, 4.0, 5.0]),
            _make_result(bench_mod, 120, latencies=[2.0, 4.0, 6.0, 8.0, 10.0]),
            _make_result(bench_mod, 110, latencies=[0.5, 1.5, 2.5, 3.5, 4.5]),
        ]
        out = bench_mod._aggregate_latencies(runs)
        assert out is not None

        # Per-run P50 is the middle sorted value (index len//2 = 2):
        # run1=3, run2=6, run3=2.5 -> median = 3
        assert out["p50_median"] == 3.0
        # Per-run P95 uses int(len * 0.95) = 4:
        # run1=5, run2=10, run3=4.5 -> median = 5
        assert out["p95_median"] == 5.0
        # Per-run P99 uses int(len * 0.99) = 4 on len=5:
        # run1=5, run2=10, run3=4.5 -> median = 5
        assert out["p99_median"] == 5.0

    def test_returns_none_when_no_latencies(self, bench_mod):
        """Scenario that never measures latency (e.g. enqueue_throughput)."""
        runs = [
            _make_result(bench_mod, 100),
            _make_result(bench_mod, 200),
        ]
        assert bench_mod._aggregate_latencies(runs) is None

    def test_returns_none_when_mixed(self, bench_mod):
        """Conservative: one run without latencies -> do not average."""
        runs = [
            _make_result(bench_mod, 100, latencies=[1.0, 2.0, 3.0]),
            _make_result(bench_mod, 120, latencies=[]),
            _make_result(bench_mod, 110, latencies=[2.0, 3.0, 4.0]),
        ]
        assert bench_mod._aggregate_latencies(runs) is None

    def test_single_run_with_latencies_returns_per_run_percentiles(self, bench_mod):
        """N=1 -> median over [x] is just x; still returns a dict."""
        run = _make_result(bench_mod, 100, latencies=[1.0, 2.0, 3.0, 4.0, 5.0])
        out = bench_mod._aggregate_latencies([run])

        assert out is not None
        # Per-run P50 = sorted([1..5])[2] = 3; median([3]) = 3
        assert out["p50_median"] == 3.0
        assert out["p95_median"] == 5.0
        assert out["p99_median"] == 5.0

    def test_empty_list_raises(self, bench_mod):
        with pytest.raises(ValueError, match="no results"):
            bench_mod._aggregate_latencies([])


class TestPositiveIntArgparseType:
    def test_accepts_positive_int(self, bench_mod):
        assert bench_mod._positive_int("1") == 1
        assert bench_mod._positive_int("3") == 3
        assert bench_mod._positive_int("100") == 100

    def test_rejects_zero(self, bench_mod):
        import argparse

        with pytest.raises(argparse.ArgumentTypeError, match=">= 1"):
            bench_mod._positive_int("0")

    def test_rejects_negative(self, bench_mod):
        import argparse

        with pytest.raises(argparse.ArgumentTypeError, match=">= 1"):
            bench_mod._positive_int("-5")

    def test_rejects_non_integer(self, bench_mod):
        import argparse

        with pytest.raises(argparse.ArgumentTypeError, match="not an integer"):
            bench_mod._positive_int("abc")
