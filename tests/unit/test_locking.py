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
    def test_hard_timeout_300s_yields_75s_ttl(self):
        # 0.25 * 300 = 75, max(30, 75) = 75 → 75_000 ms
        assert compute_lock_ttl_ms(300) == 75_000

    def test_hard_timeout_below_floor_clamped_to_30s(self):
        # 0.25 * 60 = 15, max(30, 15) = 30 → 30_000 ms
        assert compute_lock_ttl_ms(60) == 30_000

    def test_hard_timeout_one_second_clamped_to_30s(self):
        # Edge: very short timeout still gets the 30s floor
        assert compute_lock_ttl_ms(1) == 30_000

    def test_hard_timeout_120s_yields_30s_ttl(self):
        # Boundary: 0.25 * 120 = 30, max(30, 30) = 30 → 30_000 ms
        assert compute_lock_ttl_ms(120) == 30_000

    def test_hard_timeout_121s_yields_30250ms_ttl(self):
        # Boundary+1: 0.25 * 121 = 30.25 wins over the floor
        assert compute_lock_ttl_ms(121) == 30_250

    def test_hard_timeout_3600s_yields_900s_ttl(self):
        # Long task: 0.25 * 3600 = 900 seconds → 900_000 ms
        assert compute_lock_ttl_ms(3600) == 900_000

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
