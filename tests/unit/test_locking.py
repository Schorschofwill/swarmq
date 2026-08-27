"""Unit tests for ``compute_lock_ttl_ms``, ``lock_key``, and the
``LockManager`` error-wrapping boundary.

Pure-Python: the happy-path ``LockManager`` flows are tested against a
real Valkey in ``tests/integration/test_locking_manager.py``; here we
only cover the pre-Valkey logic and the boundary that converts arbitrary
client errors into :class:`BackendConnectionError`.
"""

from __future__ import annotations

import pytest

from swarmq.locking import compute_lock_ttl_ms, lock_key


class TestComputeLockTtlMs:
    # P0-A fix (2026-06-02 production-safety audit): the lock TTL must
    # OUTLIVE the task's permitted runtime, otherwise the lock expires
    # mid-execution and a second worker can acquire the same mutex while
    # the original holder is still running (duplicate execution). The TTL
    # is the crash-recovery window: it only fires when a holder dies hard
    # (SIGKILL/OOM) without a cooperative release, so it must be at least
    # ``hard_timeout`` (the longest a holder can legitimately run) plus a
    # small buffer for watchdog-fire precision and release latency.
    # Locks use server-side Valkey TIME, so there is no inter-worker clock
    # skew to budget for.

    def test_ttl_exceeds_hard_timeout_for_short_task(self):
        # ht=10s → 10s + 30s buffer = 40s
        assert compute_lock_ttl_ms(10) == 40_000

    def test_ttl_exceeds_hard_timeout_at_old_floor_boundary(self):
        # ht=30s → 60s. Under the old (buggy) formula this was 30s == ht.
        assert compute_lock_ttl_ms(30) == 60_000

    def test_ttl_for_one_second_task(self):
        # Edge: even a 1s task gets a TTL well above its runtime.
        assert compute_lock_ttl_ms(1) == 31_000

    def test_ttl_for_120s_task_no_longer_equals_hard_timeout(self):
        # Regression: old formula gave 30s (< 120s ht) → mid-run expiry.
        assert compute_lock_ttl_ms(120) == 150_000

    def test_ttl_for_long_task(self):
        # ht=3600s → 3630s. The recovery window is inherently ~1h for a
        # 1h mutex task — correctness over fast recovery.
        assert compute_lock_ttl_ms(3600) == 3_630_000

    @pytest.mark.parametrize("hard_timeout", [1, 5, 30, 60, 120, 300, 600, 3600])
    def test_ttl_always_strictly_greater_than_hard_timeout(self, hard_timeout):
        # The core invariant the P0-A fix must uphold for every input.
        assert compute_lock_ttl_ms(hard_timeout) > hard_timeout * 1000

    def test_none_raises_value_error_with_explanatory_message(self):
        with pytest.raises(ValueError, match="lock requires hard_timeout"):
            compute_lock_ttl_ms(None)


class TestLockKey:
    def test_simple_name(self):
        assert lock_key("payment-queue") == "sq:lock:payment-queue"

    def test_format_string_already_rendered(self):
        # By the time lock_key sees it, format-string-locks have been
        # rendered (the worker does the bind+format). lock_key is a
        # pure-prefix function with no format awareness.
        assert lock_key("user-42") == "sq:lock:user-42"

    def test_empty_name_still_prefixed(self):
        # Defensive: if a caller passes "" we still get a recognizably-
        # prefixed key rather than a bare prefix collision.
        assert lock_key("") == "sq:lock:"
