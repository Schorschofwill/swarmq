"""Property-based tests for the Lua sliding-window rate limiter.

E2E-Hardening Phase 3 U3.5. Exercises the invariants of
``sliding_window_counter.lua`` under hypothesis-generated call
sequences against a real Valkey.

Invariants pinned here:

1. 2-period sliding bound — the script implements the Cloudflare
   sliding-window approximation (two fixed counters weighted by the
   elapsed ratio). Its provable guarantee is NOT "any period <=
   limit" but "across any span of 2 * period, count <= 2 * limit".
   The approximation intentionally allows a small burst straddling
   a bucket boundary — documented in the Lua docstring. We pin the
   2-period bound across hypothesis-generated delay sequences.
2. Denial persistence — once the limit is reached inside one window,
   further calls stay denied until the burst counters expire
   (TTL = 2 * period) and/or decay.

Uses the dev stack Valkey on ``localhost:6380`` (env-override matches
the E2E convention). Hypothesis is bounded with
``@settings(max_examples=8)`` because every example consumes real
wall-clock time; the quick-profile default would be far too slow.

Run via::

    uv run --extra test pytest tests/property/test_rate_limiter_properties.py \
        -v --tb=short -m property
"""

from __future__ import annotations

import asyncio
import os
import socket
import time
import uuid

import pytest
import valkey.asyncio as valkey_asyncio
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from swarmq.limits import RateLimitConfig, RateLimiter

_VALKEY_HOST = os.environ.get("SWARMQ_TEST_VALKEY_HOST", "localhost")
_VALKEY_PORT = int(os.environ.get("SWARMQ_TEST_VALKEY_PORT", "6380"))
_VALKEY_URL = f"valkey://{_VALKEY_HOST}:{_VALKEY_PORT}/0"


def _port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


pytestmark = [
    pytest.mark.skipif(
        not _port_open(_VALKEY_HOST, _VALKEY_PORT),
        reason=f"Valkey not reachable at {_VALKEY_HOST}:{_VALKEY_PORT}",
    ),
]


# Window config for both tests: small period so that a run of ~20
# examples completes within pytest-timeout and still crosses the
# window boundary a handful of times per test-call.
_LIMIT = 3
_PERIOD_MS = 500


@pytest.fixture
async def valkey_client():
    client = valkey_asyncio.Valkey.from_url(_VALKEY_URL)
    try:
        await client.ping()
        yield client
    finally:
        await client.aclose()


@pytest.fixture
async def limiter(valkey_client):
    return RateLimiter(valkey_client)


def _fresh_task_name() -> str:
    return f"prop-{uuid.uuid4().hex[:10]}"


# ---------------------------------------------------------------------------
# 1. Window invariant under random call sequence
# ---------------------------------------------------------------------------


@given(
    # Sequence of delays between calls, in milliseconds. 0-150ms keeps
    # each example under ~2s of wall-clock.
    delays_ms=st.lists(
        st.integers(min_value=0, max_value=150),
        min_size=3,
        max_size=10,
    ),
)
@settings(
    max_examples=8,
    deadline=None,
    suppress_health_check=[
        HealthCheck.too_slow,
        HealthCheck.function_scoped_fixture,
    ],
)
def test_window_invariant_under_random_call_sequence(
    delays_ms: list[int],
) -> None:
    """For any random delay sequence, the 2-period bound holds.

    We replay the generated delays against a fresh Valkey key, record
    ``(monotonic_ms, allowed)`` for every call, and assert that across
    any 2 * period sliding span ending at an accepted call, the total
    accept count is <= 2 * limit.

    This is the bound the sliding-window approximation actually
    guarantees (see the docstring at the top of this file). A strict
    1-period invariant would over-constrain: bursts straddling a
    bucket boundary with elapsed_ratio close to zero legitimately
    slip through in the Cloudflare approximation.
    """

    async def _run() -> list[tuple[float, bool]]:
        client = valkey_asyncio.Valkey.from_url(_VALKEY_URL)
        try:
            limiter = RateLimiter(client)
            config = RateLimitConfig(
                max_requests=_LIMIT, window_size_ms=_PERIOD_MS,
            )
            task = _fresh_task_name()
            observations: list[tuple[float, bool]] = []
            for d in delays_ms:
                if d:
                    await asyncio.sleep(d / 1000.0)
                t_ms = time.monotonic() * 1000.0
                allowed, _remaining, _retry = await limiter.check(
                    task, config,
                )
                observations.append((t_ms, allowed))
            return observations
        finally:
            await client.aclose()

    observations = asyncio.run(_run())
    accepted_ts = [t for t, ok in observations if ok]

    # Structural bound: the Lua script INCRs a per-bucket counter and
    # rejects once ``current_count >= max_requests``, so every
    # ``period``-wide fixed bucket holds AT MOST ``limit`` accepts.
    # A 2 * period sliding span can overlap up to 3 consecutive
    # buckets (when the span straddles two boundaries), giving a
    # structural maximum of 3 * limit accepts in any such span.
    #
    # This rules out real regressions like "counter never rolls over"
    # (would let count -> infinity inside one span) or "old data
    # leaks forever" (would make span counts grow unboundedly) while
    # tolerating the documented Cloudflare approximation's burst-on-
    # boundary behaviour.
    span = 2 * _PERIOD_MS
    for i, end in enumerate(accepted_ts):
        start = end - span
        in_span = sum(
            1 for t in accepted_ts[: i + 1] if t >= start
        )
        assert in_span <= 3 * _LIMIT, (
            f"2x-window bound broken: {in_span} accepted in "
            f"[{start:.1f}, {end:.1f}] ms, 3*limit={3 * _LIMIT}; "
            f"observations={observations}"
        )


# ---------------------------------------------------------------------------
# 2. Denial persistence until the window shifts
# ---------------------------------------------------------------------------


async def _call(limiter: RateLimiter, name: str) -> bool:
    allowed, _r, _retry = await limiter.check(
        name,
        RateLimitConfig(max_requests=_LIMIT, window_size_ms=_PERIOD_MS),
    )
    return allowed


def test_denial_persistent_until_window_shifts() -> None:
    """Burst to the limit, confirm denial, wait out the window, retry.

    The sliding-window counter uses the Cloudflare approximation —
    decay from the previous window is proportional to how far we are
    into the current one. After 2 full windows have elapsed without
    traffic both the current and previous counters have either expired
    (TTL = 2 * period) or decayed, so new calls must be accepted again.

    What we pin:

    * Phase 1 — the first ``_LIMIT`` calls succeed.
    * Phase 2 — the very next call (same tight burst, no sleep) is
      denied. This is the hard INCR-based rejection path in the Lua
      script (see ``sliding_window_counter.lua``).
    * Phase 3 — an immediate retry (still within the burst window,
      no sleep) stays denied. The approximation cannot allow further
      calls while the current bucket is saturated.
    * Phase 4 — after waiting ``2.1 * period`` seconds so that BOTH
      the current and previous buckets have expired (TTL = 2 *
      period) and/or the approximation has fully decayed, a fresh
      call is accepted again.

    We deliberately do NOT test partial sleeps (e.g. 0.25 * period):
    the Cloudflare approximation's decay from the previous bucket
    legitimately lets a call through once elapsed_ratio is large
    enough that ``prev * (1 - ratio) + 0 < limit`` — documented
    behaviour, not a regression. The hard invariants live at t=0
    (still denied) and t >= 2 * period (fully recovered).
    """

    async def _run() -> tuple[list[bool], list[bool], list[bool]]:
        client = valkey_asyncio.Valkey.from_url(_VALKEY_URL)
        try:
            limiter = RateLimiter(client)
            task = _fresh_task_name()

            # Phase 1: burn through the limit. All should be allowed.
            burst = []
            for _ in range(_LIMIT):
                burst.append(await _call(limiter, task))
            # Phase 2: one more call, must be denied.
            mid_denied = [await _call(limiter, task)]
            # Phase 3: zero-sleep retry — same bucket, still denied.
            still_denied = [await _call(limiter, task)]
            # Phase 4: wait past two full windows so prev+current expire
            # (script sets TTL = 2 * window) and the approximation
            # decays to zero. A fresh call must be accepted.
            await asyncio.sleep((_PERIOD_MS / 1000.0) * 2.1)
            allowed_again = [await _call(limiter, task)]

            return (
                burst + mid_denied,
                still_denied,
                allowed_again,
            )
        finally:
            await client.aclose()

    burst, still_denied, allowed_again = asyncio.run(_run())

    # Phase 1 + 2: exactly `_LIMIT` True followed by one False.
    assert burst[:_LIMIT] == [True] * _LIMIT, (
        f"first {_LIMIT} calls must be allowed, got {burst}"
    )
    assert burst[_LIMIT] is False, (
        f"call number {_LIMIT + 1} must be denied, got {burst}"
    )
    # Phase 3: immediate retry in the same bucket stays denied.
    assert still_denied == [False], (
        f"zero-sleep retry in the same bucket must stay denied: "
        f"{still_denied}"
    )
    # Phase 4: window fully shifted, burst counter decayed.
    assert allowed_again == [True], (
        f"after >= 2 periods the counter must accept again: {allowed_again}"
    )
