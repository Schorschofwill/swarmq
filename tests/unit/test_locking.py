"""Unit tests for ``compute_lock_ttl_ms``, ``lock_key``, and the
``LockManager`` error-wrapping boundary.

Pure-Python: the happy-path ``LockManager`` flows are tested against a
real Valkey in ``tests/integration/test_locking_manager.py``; here we
only cover the pre-Valkey logic and the boundary that converts arbitrary
client errors into :class:`BackendConnectionError`.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.exceptions import BackendConnectionError
from swarmq.locking import LockManager, compute_lock_ttl_ms, lock_key


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


def _make_lock_manager_with_scripts(acquire_returns=None, release_returns=None,
                                    renew_returns=None,
                                    time_returns=(1, 0)):
    """Build a LockManager whose register_script returns mockable scripts."""
    client = MagicMock()
    client.time = AsyncMock(return_value=time_returns)
    acquire_script = AsyncMock(return_value=acquire_returns)
    release_script = AsyncMock(return_value=release_returns)
    renew_script = AsyncMock(return_value=renew_returns)
    # register_script is sync and called 3x in __init__ (acquire, release, renew)
    client.register_script = MagicMock(
        side_effect=[acquire_script, release_script, renew_script]
    )
    return LockManager(client), acquire_script, release_script, renew_script


class TestLockManagerErrorWrapping:
    """Boundary tests: any non-BackendConnectionError raised by the
    Valkey client must surface as :class:`BackendConnectionError` so
    the worker pipeline sees one error class.
    """

    @pytest.mark.asyncio
    async def test_acquire_wraps_connection_error(self):
        manager, acquire_script, _, _ = _make_lock_manager_with_scripts()
        acquire_script.side_effect = ConnectionError("kapow")

        with pytest.raises(BackendConnectionError, match="lock acquire failed"):
            await manager.acquire("name", "tok", 60_000, 1)

    @pytest.mark.asyncio
    async def test_release_wraps_runtime_error(self):
        manager, _, release_script, _ = _make_lock_manager_with_scripts()
        release_script.side_effect = RuntimeError("client closed")

        with pytest.raises(BackendConnectionError, match="lock release failed"):
            await manager.release("name", "tok", 1)

    @pytest.mark.asyncio
    async def test_acquire_passes_through_existing_backend_connection_error(self):
        # If something nested already raised BackendConnectionError, we
        # should not re-wrap (would double-nest the message).
        manager, acquire_script, _, _ = _make_lock_manager_with_scripts()
        original = BackendConnectionError("inner")
        acquire_script.side_effect = original

        with pytest.raises(BackendConnectionError) as exc_info:
            await manager.acquire("name", "tok", 60_000, 1)
        assert exc_info.value is original

    @pytest.mark.asyncio
    async def test_release_passes_through_existing_backend_connection_error(self):
        manager, _, release_script, _ = _make_lock_manager_with_scripts()
        original = BackendConnectionError("inner")
        release_script.side_effect = original

        with pytest.raises(BackendConnectionError) as exc_info:
            await manager.release("name", "tok", 1)
        assert exc_info.value is original


class TestLockManagerReturnSemantics:
    """The Lua scripts return small ints; the Python wrapper translates
    to bools. Edge: ``2`` (force-release) must count as released."""

    @pytest.mark.asyncio
    async def test_acquire_returns_true_on_one(self):
        manager, _, _, _ = _make_lock_manager_with_scripts(acquire_returns=1)
        assert await manager.acquire("name", "tok", 60_000, 1) is True

    @pytest.mark.asyncio
    async def test_acquire_returns_false_on_zero(self):
        manager, _, _, _ = _make_lock_manager_with_scripts(acquire_returns=0)
        assert await manager.acquire("name", "tok", 60_000, 1) is False

    @pytest.mark.asyncio
    async def test_release_returns_true_on_one(self):
        manager, _, _, _ = _make_lock_manager_with_scripts(release_returns=1)
        assert await manager.release("name", "tok", 1) is True

    @pytest.mark.asyncio
    async def test_release_returns_true_on_force_two(self):
        manager, _, _, _ = _make_lock_manager_with_scripts(release_returns=2)
        assert await manager.release("name", "tok", 1) is True

    @pytest.mark.asyncio
    async def test_release_returns_false_on_zero(self):
        manager, _, _, _ = _make_lock_manager_with_scripts(release_returns=0)
        assert await manager.release("name", "tok", 1) is False


class TestLockManagerRenew:
    """``renew`` extends the TTL of a mutex lock we still own.

    Mutex-only (P2-8): raising NotImplementedError on max_concurrency>1
    prevents silent WRONGTYPE failures when a caller points renew() at a
    semaphore SortedSet.
    """

    @pytest.mark.asyncio
    async def test_renew_returns_true_on_one(self):
        manager, _, _, renew_script = _make_lock_manager_with_scripts(
            renew_returns=1
        )
        assert await manager.renew("name", "tok", 30_000) is True
        renew_script.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_renew_returns_false_on_zero(self):
        manager, _, _, renew_script = _make_lock_manager_with_scripts(
            renew_returns=0
        )
        # Foreign owner OR key absent → script returns 0 → renew returns False
        assert await manager.renew("name", "tok", 30_000) is False

    @pytest.mark.asyncio
    async def test_renew_passes_lock_key_and_args(self):
        manager, _, _, renew_script = _make_lock_manager_with_scripts(
            renew_returns=1
        )
        await manager.renew("leader", "owner-42", 5_000)
        renew_script.assert_awaited_once_with(
            keys=["sq:lock:leader"],
            args=["owner-42", 5_000],
        )

    @pytest.mark.asyncio
    async def test_renew_wraps_runtime_error(self):
        manager, _, _, renew_script = _make_lock_manager_with_scripts(
            renew_returns=1
        )
        renew_script.side_effect = RuntimeError("connection reset")
        with pytest.raises(BackendConnectionError, match="lock renew failed"):
            await manager.renew("name", "tok", 30_000)

    @pytest.mark.asyncio
    async def test_renew_passes_through_existing_backend_connection_error(self):
        manager, _, _, renew_script = _make_lock_manager_with_scripts(
            renew_returns=1
        )
        original = BackendConnectionError("inner")
        renew_script.side_effect = original
        with pytest.raises(BackendConnectionError) as exc_info:
            await manager.renew("name", "tok", 30_000)
        assert exc_info.value is original

    @pytest.mark.asyncio
    async def test_renew_rejects_semaphore_mode(self):
        """max_concurrency>1 is mutex-only for v1 (P2-8)."""
        manager, _, _, _ = _make_lock_manager_with_scripts(renew_returns=1)
        with pytest.raises(NotImplementedError, match="mutex-only"):
            await manager.renew("name", "tok", 30_000, max_concurrency=2)

    @pytest.mark.asyncio
    async def test_renew_accepts_explicit_mutex_max_concurrency(self):
        manager, _, _, _ = _make_lock_manager_with_scripts(renew_returns=1)
        # Explicit max_concurrency=1 is a no-op; equivalent to default
        assert await manager.renew("name", "tok", 30_000, max_concurrency=1) is True
