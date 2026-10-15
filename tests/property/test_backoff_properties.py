"""Property-based tests for swarmq.backoff.calculate_backoff.

Plan: docs/plans/2026-04-17-007-feat-e2e-hardening-plan.md U3.4

The backoff API in ``src/swarmq/backoff.py`` is a single function
``calculate_backoff(attempt, strategy, base_delay, max_delay)`` with
three strategies:

- ``fixed``: ``min(base_delay, max_delay)`` regardless of attempt (no
  jitter; E-023, F3-16).
- ``exponential``: ``min(base * 2**attempt * U(1, 2), max_delay)``.
- ``linear``: ``min(base * (attempt + 1) * U(1, 2), max_delay)``.

Jitter is baked in at the fixed multiplier range ``[1.0, 2.0]``, not a
caller-supplied parameter. These properties follow the docstring exactly.
"""

from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from swarmq.backoff import calculate_backoff

# Bounded domains — astronomical base * 2**attempt trivially overflows
# to inf on large attempt counts; the function's intent is real-world
# retry delays, so cap attempt at something sensible.
_ATTEMPT = st.integers(min_value=0, max_value=30)
_BASE = st.floats(min_value=0.0, max_value=1e3, allow_nan=False, allow_infinity=False)
_MAX = st.floats(min_value=0.0, max_value=1e6, allow_nan=False, allow_infinity=False)


# ---------------------------------------------------------------------------
# Monotonicity (expected-value sense — jitter is random, so compare ranges)
# ---------------------------------------------------------------------------


@given(attempt=_ATTEMPT, base=_BASE)
def test_exponential_monotonicity_lower_bound(attempt: int, base: float) -> None:
    """Without the max_delay cap, exp backoff's MIN possible delay grows.

    ``calculate_backoff`` uses ``U(1, 2)`` jitter, so the lower envelope
    for attempt n is ``base * 2**n * 1.0``. Pick max_delay high enough
    to disable the cap, then assert the observed sample at n+1 is
    >= the observed sample at n when jitter effects cancel — easier to
    pin the envelope directly.
    """
    large_cap = 1e18
    # Envelope test: sampled value is always within [base*2^n, base*2^n*2].
    d_n = calculate_backoff(attempt=attempt, strategy="exponential", base_delay=base, max_delay=large_cap)
    assert d_n >= base * (2**attempt) * 1.0 - 1e-9
    assert d_n <= base * (2**attempt) * 2.0 + 1e-9

    d_n1 = calculate_backoff(
        attempt=attempt + 1,
        strategy="exponential",
        base_delay=base,
        max_delay=large_cap,
    )
    # Envelope(n+1).lower == 2 * Envelope(n).lower, and
    # Envelope(n+1).lower == Envelope(n).upper, so d_{n+1} >= d_n / 2
    # worst-case. A strict-ish monotonicity bound:
    assert d_n1 >= base * (2 ** (attempt + 1)) * 1.0 - 1e-9


@given(attempt=_ATTEMPT, base=_BASE)
def test_linear_monotonicity_lower_bound(attempt: int, base: float) -> None:
    """Linear envelope: ``base * (n+1) * [1, 2]``."""
    large_cap = 1e18
    d_n = calculate_backoff(attempt=attempt, strategy="linear", base_delay=base, max_delay=large_cap)
    assert d_n >= base * (attempt + 1) * 1.0 - 1e-9
    assert d_n <= base * (attempt + 1) * 2.0 + 1e-9

    d_n1 = calculate_backoff(
        attempt=attempt + 1,
        strategy="linear",
        base_delay=base,
        max_delay=large_cap,
    )
    assert d_n1 >= base * (attempt + 2) * 1.0 - 1e-9


# ---------------------------------------------------------------------------
# Jitter envelope bounds
# ---------------------------------------------------------------------------


@given(attempt=_ATTEMPT, base=_BASE)
def test_exponential_jitter_within_documented_bounds(attempt: int, base: float) -> None:
    """``delay`` always in ``[base * 2**n, base * 2**n * 2]`` before the cap."""
    large_cap = 1e18
    d = calculate_backoff(attempt=attempt, strategy="exponential", base_delay=base, max_delay=large_cap)
    low = base * (2**attempt) * 1.0
    high = base * (2**attempt) * 2.0
    assert low - 1e-9 <= d <= high + 1e-9


@given(attempt=_ATTEMPT, base=_BASE)
def test_linear_jitter_within_documented_bounds(attempt: int, base: float) -> None:
    """``delay`` always in ``[base * (n+1), base * (n+1) * 2]`` before the cap."""
    large_cap = 1e18
    d = calculate_backoff(attempt=attempt, strategy="linear", base_delay=base, max_delay=large_cap)
    low = base * (attempt + 1) * 1.0
    high = base * (attempt + 1) * 2.0
    assert low - 1e-9 <= d <= high + 1e-9


# ---------------------------------------------------------------------------
# Upper-bound cap
# ---------------------------------------------------------------------------


@given(attempt=_ATTEMPT, base=_BASE, max_delay=_MAX)
def test_max_delay_upper_bound_respected_exponential(attempt: int, base: float, max_delay: float) -> None:
    """Exponential strategy never returns > max_delay."""
    d = calculate_backoff(
        attempt=attempt,
        strategy="exponential",
        base_delay=base,
        max_delay=max_delay,
    )
    assert d <= max_delay + 1e-9


@given(attempt=_ATTEMPT, base=_BASE, max_delay=_MAX)
def test_max_delay_upper_bound_respected_linear(attempt: int, base: float, max_delay: float) -> None:
    """Linear strategy never returns > max_delay."""
    d = calculate_backoff(attempt=attempt, strategy="linear", base_delay=base, max_delay=max_delay)
    assert d <= max_delay + 1e-9


@given(attempt=_ATTEMPT, base=_BASE, max_delay=_MAX)
def test_fixed_ignores_attempt_and_respects_cap(attempt: int, base: float, max_delay: float) -> None:
    """Fixed strategy returns base_delay, at most max_delay, for any attempt.

    E-023 (Nachtrag 2026-10-15, F3-16): ``fixed`` waits exactly
    ``base_delay``, capped at ``max_delay``, without jitter.
    """
    d = calculate_backoff(attempt=attempt, strategy="fixed", base_delay=base, max_delay=max_delay)
    assert d == min(base, max_delay)
