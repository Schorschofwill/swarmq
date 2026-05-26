"""Micro-benchmark for ``import swarmq.cli`` latency.

Runs N warm-runs in fresh subprocesses (so each measurement starts
with empty ``sys.modules``), reports two metrics side by side:

* **wall-clock** — total subprocess time (includes Python interpreter
  startup, ~50 ms on this machine). What users actually feel when they
  run ``swarmq --help``.
* **import-time-cumulative** — sum of all module-import times from
  Python's ``-X importtime`` flag (sysmodule-internal). Isolated from
  interpreter startup; this is the metric Plan 2026-04-23-002 R-4 uses
  for its 60 % reduction target against the 187.6 ms baseline.

Not a pytest gate (subprocess timing is environment-sensitive); use
as a developer feedback tool when changing imports under ``swarmq/``.

Usage:
    uv run python scripts/bench_cli_import.py [N]
"""

from __future__ import annotations

import re
import statistics
import subprocess
import sys
import time

_DEFAULT_RUNS = 20
_BASELINE_IMPORTTIME_MS = 187.6  # measured pre-Plan 2026-04-23-002


def _run_once() -> tuple[float, float]:
    """Return (wall_clock_ms, importtime_cumulative_ms) for one fresh import."""
    start = time.perf_counter_ns()
    result = subprocess.run(
        [sys.executable, "-X", "importtime", "-c", "import swarmq.cli"],
        check=True,
        capture_output=True,
        text=True,
    )
    wall_ms = (time.perf_counter_ns() - start) / 1e6
    # Last line of -X importtime stderr looks like:
    #   "import time:       172 |     187649 | swarmq.cli"
    last = result.stderr.strip().splitlines()[-1]
    match = re.search(r"\|\s+(\d+)\s+\|\s+swarmq\.cli\s*$", last)
    importtime_ms = int(match.group(1)) / 1000.0 if match else float("nan")
    return wall_ms, importtime_ms


def _summarise(label: str, values: list[float], baseline: float | None = None) -> None:
    values = sorted(values)
    n = len(values)
    p95_idx = max(0, int(round(n * 0.95)) - 1)
    print(f"--- {label} ---")
    print(f"min:     {values[0]:7.2f} ms")
    print(f"median:  {statistics.median(values):7.2f} ms")
    print(f"p95:     {values[p95_idx]:7.2f} ms")
    print(f"max:     {values[-1]:7.2f} ms")
    if baseline is not None:
        reduction = (baseline - statistics.median(values)) / baseline * 100
        print(f"baseline:{baseline:7.2f} ms")
        print(f"median reduction vs baseline: {reduction:5.1f} %")


def main(runs: int = _DEFAULT_RUNS) -> int:
    results = [_run_once() for _ in range(runs)]
    wall = [w for w, _ in results]
    imp = [i for _, i in results]
    print(f"runs: {runs}\n")
    _summarise("wall-clock (includes Python startup)", wall)
    print()
    _summarise(
        "importtime-cumulative (Plan 2026-04-23-002 R-4 metric)",
        imp,
        baseline=_BASELINE_IMPORTTIME_MS,
    )
    return 0


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else _DEFAULT_RUNS
    sys.exit(main(n))
