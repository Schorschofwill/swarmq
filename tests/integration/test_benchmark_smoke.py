"""Integration smoke test for benchmarks/bench.py.

Runs the benchmark end-to-end at a tiny message count (n=500) with all
scenarios enabled. The purpose is to lock in two properties that were
previously broken and hard to catch via unit tests:

- the aiormq `_reader` task leak that hung `full_pipeline` after
  Szenario 5, and
- the stale-connection-after-full_pipeline regression that made the
  next scenario's ``connection.channel()`` raise ``RuntimeError:
  Connection closed``.

The test is marked as integration and requires a running RabbitMQ +
Valkey pair (via docker compose) listening on the default localhost
ports. It is skipped when those services are not reachable.
"""

from __future__ import annotations

import os
import socket
import subprocess
from pathlib import Path

import pytest

from tests.support.services import (
    RABBITMQ_HOST,
    RABBITMQ_PORT,
    VALKEY_HOST,
    VALKEY_PORT,
    rabbitmq_amqp_url,
    valkey_db_url,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
BENCH_PATH = REPO_ROOT / "benchmarks" / "bench.py"


def _port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _docker_up() -> bool:
    return _port_open(RABBITMQ_HOST, RABBITMQ_PORT) and _port_open(VALKEY_HOST, VALKEY_PORT)


@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.timeout(240)
@pytest.mark.skipif(
    not _docker_up(),
    reason="RabbitMQ or Valkey not reachable on the configured test ports — start docker compose",
)
def test_benchmark_all_scenarios_small_load() -> None:
    """Full -s all run at n=500 must exit 0 within 3 minutes.

    Regression guard for:
      * aiormq `_reader` task leak causing full_pipeline hang
      * stale RobustConnection surviving into the next scenario
    """
    assert BENCH_PATH.is_file()
    env = dict(os.environ)
    env["PYTHONUNBUFFERED"] = "1"
    # bench.py liest RABBITMQ_URL/VALKEY_URL — auf die Test-Ports zeigen.
    env["RABBITMQ_URL"] = rabbitmq_amqp_url()
    env["VALKEY_URL"] = valkey_db_url()

    result = subprocess.run(
        [
            "uv",
            "run",
            "--with-requirements",
            str(BENCH_PATH.parent / "requirements.txt"),
            "python",
            "-u",
            str(BENCH_PATH),
            "-n",
            "500",
            "-s",
            "all",
        ],
        cwd=BENCH_PATH.parent,
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
    )

    combined = result.stdout + result.stderr

    assert "Connection closed" not in combined, (
        "stale-connection regression: a scenario inherited a dead "
        "RobustConnection from the previous scenario.\n" + combined[-4000:]
    )
    assert "TimeoutError" not in combined, (
        "hang regression: a completion-wait hit the 300s timeout. "
        "Send SIGUSR1 <pid> to a live run to see the parked tasks.\n" + combined[-4000:]
    )
    assert "Traceback" not in combined, "benchmark raised an unexpected exception:\n" + combined[-4000:]
    assert result.returncode == 0, f"benchmark exited {result.returncode}. Last output:\n" + combined[-4000:]
    # Cheap sanity: every scenario header must appear.
    for marker in (
        "Szenario 1:",
        "Szenario 6:",
        "Szenario 8:",
        "Szenario 9:",
        "GEWINNER PRO SZENARIO",
    ):
        assert marker in combined, f"benchmark output missing {marker!r}. Last output:\n" + combined[-4000:]

    # Default (--repeat omitted) must not trigger the aggregate path.
    assert "AGGREGATE [" not in combined, (
        "default run unexpectedly emitted AGGREGATE — the --repeat 1 legacy "
        "path should stay silent.\n" + combined[-4000:]
    )


@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.timeout(240)
@pytest.mark.skipif(
    not _docker_up(),
    reason="RabbitMQ or Valkey not reachable on the configured test ports — start docker compose",
)
def test_benchmark_repeat_mode_emits_aggregate() -> None:
    """`--repeat 2` must print two per-run results plus one AGGREGATE line.

    Locks in that the repeat loop actually runs the measurement twice
    (not once, not in parallel, not silently merged) and that
    print_aggregate fires exactly when repeat > 1.
    """
    assert BENCH_PATH.is_file()
    env = dict(os.environ)
    env["PYTHONUNBUFFERED"] = "1"
    # bench.py liest RABBITMQ_URL/VALKEY_URL — auf die Test-Ports zeigen.
    env["RABBITMQ_URL"] = rabbitmq_amqp_url()
    env["VALKEY_URL"] = valkey_db_url()

    result = subprocess.run(
        [
            "uv",
            "run",
            "--with-requirements",
            str(BENCH_PATH.parent / "requirements.txt"),
            "python",
            "-u",
            str(BENCH_PATH),
            "-n",
            "300",
            "-s",
            "enqueue_throughput",
            "--repeat",
            "2",
        ],
        cwd=BENCH_PATH.parent,
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
    )

    combined = result.stdout + result.stderr

    assert "Connection closed" not in combined, combined[-4000:]
    assert "TimeoutError" not in combined, combined[-4000:]
    assert "Traceback" not in combined, combined[-4000:]
    assert result.returncode == 0, f"benchmark exited {result.returncode}. Last output:\n" + combined[-4000:]

    # The enqueue_throughput scenario runs three variants: RabbitMQ +
    # Valkey (LIST) + Valkey/Stream. --repeat 2 means six per-run prints
    # and three AGGREGATE lines (one per variant).
    throughput_count = combined.count("Throughput:")
    assert throughput_count == 6, f"expected 6 per-run Throughput lines, got {throughput_count}.\n" + combined[-4000:]
    aggregate_count = combined.count("AGGREGATE [2 runs]")
    assert aggregate_count == 3, f"expected 3 AGGREGATE lines, got {aggregate_count}.\n" + combined[-4000:]
