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
# Warmup window: connections + pools stabilise in the first ~30 s of
# load. We measure drift *from the first post-warmup sample*, not from
# the pre-load baseline, to avoid counting warmup churn as a leak.
_WARMUP_S = float(os.environ.get("SWARMQ_SOAK_WARMUP_S", "30"))
# Small-absolute guard: if the baseline value is tiny (e.g. 3 connections),
# a +/- 1 wobble blows past 20 %. Require at least this many absolute
# units of drift before percentage drift is enforced.
_MIN_ABS_DELTA = {
    "rss": 50 * 1024 * 1024,  # 50 MiB absolute slack
    "fds": 20,                # 20 FDs absolute slack
    "valkey_clients": 10,     # 10 client connections slack
    "rabbit_channels": 10,    # 10 channels slack
}


def _unique_queue() -> str:
    return f"soak-{uuid.uuid4().hex[:12]}"


# ---------------------------------------------------------------------------
# Metric collection
# ---------------------------------------------------------------------------


async def _valkey_client_count(monitor: valkey.asyncio.Valkey) -> int:
    """Count active Valkey clients via CLIENT LIST (via a shared monitor)."""
    raw = await monitor.execute_command("CLIENT", "LIST")
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    # Each client yields one line; strip empties.
    return sum(1 for line in raw.splitlines() if line.strip())


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

    # Persistent monitor client so CLIENT LIST doesn't open a fresh
    # connection each sample (which would self-pollute the metric).
    monitor = valkey.asyncio.Valkey.from_url(valkey_url)
    try:
        await monitor.ping()
    except Exception:  # pragma: no cover - fixture should have caught this
        await monitor.aclose()
        raise

    async with app:
        await broker.declare_queue(queue)

        worker_task = asyncio.create_task(worker_app.start_worker())

        # Let the worker settle before baseline to avoid counting churn.
        await asyncio.sleep(2.0)

        async def _sample(label: str) -> dict[str, float]:
            rss, fds = _process_metrics()
            valkey_clients = await _valkey_client_count(monitor)
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
                f"[soak] {label:>10s} rss={rss / 1024 / 1024:.1f}MiB "
                f"fds={fds} valkey={valkey_clients} "
                f"rabbit_ch={rabbit_channels}",
                flush=True,
            )
            return snap

        samples.append(await _sample("pre-load"))

        # ------------------------------------------------------------------
        # Schedule loop — produce ~rate_per_s tasks/s, sample every 30s.
        # ------------------------------------------------------------------
        start = time.monotonic()
        stop_at = start + _DURATION_S
        next_sample_at = start + _SAMPLE_INTERVAL_S
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
                    label = f"t+{int(time.monotonic() - start)}s"
                    samples.append(await _sample(label))
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

        # Final under-load sample before tearing everything down.
        samples.append(await _sample("post-load"))
    await monitor.aclose()

    # ------------------------------------------------------------------
    # Assertions — drift < _DRIFT_THRESHOLD from the first post-warmup
    # sample (not pre-load), so we don't count connection-pool warmup as
    # a leak.
    # ------------------------------------------------------------------
    print(f"[soak] scheduled={scheduled} samples={len(samples)}", flush=True)
    assert len(samples) >= 2, "need at least 2 samples to detect drift"

    # Use the last sample under load whose timestamp is >= warmup as
    # baseline. For the 60s default duration this is the t+30s sample;
    # for the 1h run it's also t+30s. Falls back to pre-load if none.
    t0 = samples[0]["t"]
    post_warmup = [
        s for s in samples
        if s["label"].startswith("t+")
        and (s["t"] - t0) >= _WARMUP_S
    ]
    baseline_sample = post_warmup[0] if post_warmup else samples[0]
    compare = (
        [baseline_sample, *samples[samples.index(baseline_sample) + 1:]]
    )
    print(
        f"[soak] drift baseline = {baseline_sample['label']}"
        f" (t+{int(baseline_sample['t'] - t0)}s)",
        flush=True,
    )

    def _drift(metric: str) -> tuple[float, float]:
        """Return (relative_drift, absolute_peak_delta) for metric."""
        values = [s[metric] for s in compare if s[metric] >= 0]
        if len(values) < 2 or values[0] <= 0:
            return 0.0, 0.0
        base = values[0]
        peak = max(values)
        return abs(peak - base) / base, abs(peak - base)

    def _check(metric: str, label: str) -> None:
        rel, abs_ = _drift(metric)
        slack = _MIN_ABS_DELTA[metric]
        # If absolute change is within slack, accept regardless of %.
        ok = abs_ <= slack or rel < _DRIFT_THRESHOLD
        print(
            f"[soak] drift {label:>16s} "
            f"rel={rel:.1%} abs={abs_:.0f} slack={slack:.0f} "
            f"ok={ok}",
            flush=True,
        )
        assert ok, (
            f"{label} drift {rel:.1%} (abs {abs_:.0f} > slack {slack:.0f}) "
            f">= {_DRIFT_THRESHOLD:.0%} "
            f"(samples={[s[metric] for s in compare]})"
        )

    _check("rss", "rss_bytes")
    _check("fds", "fd_count")
    _check("valkey_clients", "valkey_clients")
    # Rabbit channel-count may report -1 if mgmt API is offline; skip.
    if all(s["rabbit_channels"] >= 0 for s in compare):
        _check("rabbit_channels", "rabbit_channels")
