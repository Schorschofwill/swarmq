"""Long-soak leak-detection test (E2E-Hardening Phase 3 U3.9).

Schedules ~100 tasks/s continuously and samples resource metrics every
30s to detect drift in RSS, FD-count, Valkey connections, and RabbitMQ
channels.

Default duration is 60s (``SWARMQ_SOAK_HOURS=0.0167``) for CI sanity;
set ``SWARMQ_SOAK_HOURS=1`` to run the full 1h soak locally.

Metrics & thresholds (soft, drift-based):
    * RSS drift < 20 %
    * FD-count drift < 20 %
    * Valkey CLIENT LIST drift < 20 %
    * RabbitMQ channel-count drift < 20 %
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import time
import uuid

import httpx
import psutil
import pytest
import valkey.asyncio

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.task import Task, TaskInfo


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

_SOAK_HOURS = float(os.environ.get("SWARMQ_SOAK_HOURS", "0.0167"))
_DURATION_S = _SOAK_HOURS * 3600.0
_RATE_PER_S = int(os.environ.get("SWARMQ_SOAK_RATE", "100"))
_SAMPLE_INTERVAL_S = float(os.environ.get("SWARMQ_SOAK_SAMPLE_S", "30"))
_DRIFT_THRESHOLD = 0.20  # 20 % drift allowed on any metric.


def _unique_queue() -> str:
    return f"soak-{uuid.uuid4().hex[:12]}"


# ---------------------------------------------------------------------------
# Metric collection
# ---------------------------------------------------------------------------


async def _valkey_client_count(valkey_url: str) -> int:
    """Count active Valkey clients via CLIENT LIST."""
    client = valkey.asyncio.Valkey.from_url(valkey_url)
    try:
        raw = await client.execute_command("CLIENT", "LIST")
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        # Each client yields one line; strip empties.
        return sum(1 for line in raw.splitlines() if line.strip())
    finally:
        await client.aclose()


async def _rabbit_channel_count(mgmt_url: str) -> int:
    """Count open RabbitMQ channels via Management API."""
    async with httpx.AsyncClient(
        auth=("guest", "guest"), timeout=5.0,
    ) as http:
        try:
            resp = await http.get(f"{mgmt_url}/api/channels")
            resp.raise_for_status()
        except (httpx.HTTPError, httpx.HTTPStatusError):
            return -1  # sentinel: mgmt API unavailable
        return len(resp.json())


def _process_metrics() -> tuple[int, int]:
    """Return (rss_bytes, fd_count) for the current process."""
    proc = psutil.Process()
    rss = proc.memory_info().rss
    try:
        fds = proc.num_fds()
    except AttributeError:
        # num_fds() is POSIX-only; fallback to open_files() length.
        fds = len(proc.open_files())
    return rss, fds


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------


@pytest.mark.soak
@pytest.mark.timeout(int(_DURATION_S) + 600)
async def test_1h_continuous_load_no_leaks(
    rabbitmq_url: str, valkey_url: str, rabbitmq_mgmt_url: str,
) -> None:
    """Sustain ~100 tasks/s for the configured duration; assert no leaks."""
    queue = _unique_queue()
    task_name = f"soak_noop_{uuid.uuid4().hex[:8]}"

    class SoakTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue])

        async def run(self) -> int:
            return 1

    broker = RabbitMQBroker(rabbitmq_url)
    backend = ValkeyBackend(valkey_url)
    app = SwarmQ(broker, backend, queues=[queue])

    worker_broker = RabbitMQBroker(rabbitmq_url)
    worker_backend = ValkeyBackend(valkey_url)
    worker_app = SwarmQ(worker_broker, worker_backend, queues=[queue])

    samples: list[dict[str, float]] = []

    async with app:
        await broker.declare_queue(queue)

        worker_task = asyncio.create_task(worker_app.start_worker())

        # Let the worker settle before baseline to avoid counting churn.
        await asyncio.sleep(2.0)

        async def _sample(label: str) -> dict[str, float]:
            rss, fds = _process_metrics()
            valkey_clients = await _valkey_client_count(valkey_url)
            rabbit_channels = await _rabbit_channel_count(rabbitmq_mgmt_url)
            snap = {
                "t": time.monotonic(),
                "label": label,
                "rss": float(rss),
                "fds": float(fds),
                "valkey_clients": float(valkey_clients),
                "rabbit_channels": float(rabbit_channels),
            }
            print(
                f"[soak] {label:>8s} rss={rss / 1024 / 1024:.1f}MiB "
                f"fds={fds} valkey={valkey_clients} "
                f"rabbit_ch={rabbit_channels}",
                flush=True,
            )
            return snap

        baseline = await _sample("baseline")
        samples.append(baseline)

        # ------------------------------------------------------------------
        # Schedule loop — produce ~rate_per_s tasks/s, sample every 30s.
        # ------------------------------------------------------------------
        stop_at = time.monotonic() + _DURATION_S
        next_sample_at = time.monotonic() + _SAMPLE_INTERVAL_S
        tick_interval = 1.0 / max(1, _RATE_PER_S)
        scheduled = 0
        try:
            while time.monotonic() < stop_at:
                tick_start = time.monotonic()
                # Fire-and-forget schedule; we don't await results.
                try:
                    await app.schedule(task_name)
                except Exception as exc:  # pragma: no cover - stability guard
                    print(f"[soak] schedule error: {exc!r}", flush=True)
                scheduled += 1

                if time.monotonic() >= next_sample_at:
                    samples.append(await _sample(f"t+{int(time.monotonic() - baseline['t'])}s"))
                    next_sample_at += _SAMPLE_INTERVAL_S

                elapsed = time.monotonic() - tick_start
                if elapsed < tick_interval:
                    await asyncio.sleep(tick_interval - elapsed)
        finally:
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker_task
            await worker_broker.close()
            await worker_backend.close()

        # Final sample after teardown-free grace period.
        final = await _sample("final")
        samples.append(final)

    # ------------------------------------------------------------------
    # Assertions — drift < _DRIFT_THRESHOLD across the whole run.
    # ------------------------------------------------------------------
    print(f"[soak] scheduled={scheduled} samples={len(samples)}", flush=True)

    # Need at least baseline + one mid-run or final sample to compute drift.
    assert len(samples) >= 2, "need baseline + final sample to detect drift"

    def _drift(metric: str) -> float:
        values = [s[metric] for s in samples if s[metric] >= 0]
        if len(values) < 2 or values[0] <= 0:
            return 0.0
        peak = max(values)
        return abs(peak - values[0]) / values[0]

    rss_drift = _drift("rss")
    fd_drift = _drift("fds")
    valkey_drift = _drift("valkey_clients")
    rabbit_drift = _drift("rabbit_channels")

    print(
        f"[soak] drift rss={rss_drift:.1%} fds={fd_drift:.1%} "
        f"valkey={valkey_drift:.1%} rabbit={rabbit_drift:.1%}",
        flush=True,
    )

    assert rss_drift < _DRIFT_THRESHOLD, (
        f"RSS drift {rss_drift:.1%} >= {_DRIFT_THRESHOLD:.0%} "
        f"(samples={[s['rss'] for s in samples]})"
    )
    assert fd_drift < _DRIFT_THRESHOLD, (
        f"FD drift {fd_drift:.1%} >= {_DRIFT_THRESHOLD:.0%} "
        f"(samples={[s['fds'] for s in samples]})"
    )
    assert valkey_drift < _DRIFT_THRESHOLD, (
        f"Valkey-clients drift {valkey_drift:.1%} >= {_DRIFT_THRESHOLD:.0%} "
        f"(samples={[s['valkey_clients'] for s in samples]})"
    )
    # Rabbit channel-count may report -1 if mgmt API is offline; skip in
    # that case rather than failing.
    if all(s["rabbit_channels"] >= 0 for s in samples):
        assert rabbit_drift < _DRIFT_THRESHOLD, (
            f"Rabbit-channels drift {rabbit_drift:.1%} "
            f">= {_DRIFT_THRESHOLD:.0%} "
            f"(samples={[s['rabbit_channels'] for s in samples]})"
        )
