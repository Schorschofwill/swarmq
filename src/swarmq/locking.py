"""Task-locking primitives for SwarmQ.

Backed by ``lock_acquire.lua`` + ``lock_release.lua`` in
``src/swarmq/lua/``. The Lua scripts handle both Mutex (max_concurrency=1
via ``SET NX PX``) and Semaphore (max_concurrency>1 via SortedSet)
modes; this module provides the Python surface (TTL helper, async
``acquire`` / ``release``).

Pattern mirrors ``RateLimiter`` in ``swarmq.limits``: ``register_script``
returns a ``valkey.AsyncScript`` whose own NOSCRIPT-recovery loop
handles cache eviction.
"""

from __future__ import annotations

import valkey.asyncio as valkey_asyncio

from swarmq.exceptions import BackendConnectionError
from swarmq.lua import load_script

# Minimum lock TTL in seconds. Caps the lower bound of ``compute_lock_ttl_ms``
# so that a very small ``hard_timeout`` (e.g. 1s) still gives the lock enough
# room to outlast scheduler jitter, GC pauses, and one round-trip to Valkey.
# The 30s figure comes from the brief — wide enough to absorb realistic
# stalls, narrow enough that worker-crash recovery isn't unreasonably long.
_MIN_LOCK_TTL_SECONDS = 30
_LOCK_TTL_HARD_TIMEOUT_FRACTION = 0.25

_LOCK_KEY_PREFIX = "sq:lock:"


def lock_key(lock_name: str) -> str:
    """Map a user-facing lock name to its Valkey key. Single source of
    truth so producers and consumers agree on the namespace."""
    return f"{_LOCK_KEY_PREFIX}{lock_name}"


def compute_lock_ttl_ms(hard_timeout: int | None) -> int:
    """Lock TTL in ms = max(30s, 0.25 * hard_timeout).

    Couples the lock's automatic-recovery window to the task's own
    runtime ceiling. A task that exceeds ``hard_timeout`` is cancelled
    by the worker watchdog before the lock TTL expires, so the lock
    always releases via cooperative ``LockManager.release`` under
    normal operation. The TTL only matters when the worker dies hard
    (SIGKILL, OOM) — then the next worker waits at most ``ttl_ms`` to
    re-acquire.

    Raises:
        ValueError: when ``hard_timeout`` is ``None``. Locks require a
            hard timeout for safe TTL derivation; a task that may run
            forever cannot have a sound automatic-recovery deadline.
            Caught at ``TaskInfo.__post_init__`` for fail-fast at
            module import, not at first schedule.
    """
    if hard_timeout is None:
        raise ValueError(
            "lock requires hard_timeout to be set; "
            "see brief 'Brief-Decision-4' — locks need a finite "
            "ceiling for TTL-recovery to be sound"
        )
    seconds = max(_MIN_LOCK_TTL_SECONDS, _LOCK_TTL_HARD_TIMEOUT_FRACTION * hard_timeout)
    return int(seconds * 1000)


class LockManager:
    """Async wrapper around ``lock_acquire.lua`` + ``lock_release.lua``.

    One instance per worker, constructed after the backend connects
    (mirrors ``RateLimiter`` lifecycle in ``Worker.start``). Holds two
    registered ``AsyncScript`` instances so the NOSCRIPT-recovery loop
    inside ``valkey.asyncio.AsyncScript`` is the recovery path — we do
    not reimplement it.

    All Valkey-facing methods catch arbitrary exceptions and re-raise
    them as :class:`BackendConnectionError`. The boundary is drawn here
    so the worker's pre-execute pipeline (``_try_apply_lock``) sees one
    error class regardless of whether the underlying transport raised
    ``ConnectionError``, ``TimeoutError``, or a Valkey-specific error.
    """

    def __init__(self, client: valkey_asyncio.Valkey) -> None:
        self._client = client
        self._acquire_script = client.register_script(load_script("lock_acquire"))
        self._release_script = client.register_script(load_script("lock_release"))

    async def acquire(
        self,
        lock_name: str,
        owner_token: str,
        ttl_ms: int,
        max_concurrency: int,
    ) -> bool:
        """Try to claim the lock. Returns ``True`` on success, ``False``
        on conflict (mutex held by someone else, or semaphore full).

        Pulls server-side ``TIME`` once so the script's expiry
        arithmetic uses the same wall clock the SortedSet was scored
        with — same pattern as ``RateLimiter.check``.
        """
        try:
            seconds, microseconds = await self._client.time()
            now_ms = int(seconds) * 1000 + int(microseconds) // 1000
            result = await self._acquire_script(
                keys=[lock_key(lock_name)],
                args=[owner_token, ttl_ms, max_concurrency, now_ms],
            )
        except BackendConnectionError:
            raise
        except Exception as exc:
            raise BackendConnectionError(
                f"lock acquire failed for {lock_name!r}: {exc}"
            ) from exc
        return int(result) == 1

    async def release(
        self,
        lock_name: str,
        owner_token: str,
        max_concurrency: int,
    ) -> bool:
        """Release a lock previously acquired by this token.

        Returns ``True`` when the script reported a release (owned-release
        or admin force-release), ``False`` when nothing was released
        (foreign owner, key gone, or token not in the semaphore set).
        Pass ``owner_token="__force__"`` to bypass the owner check —
        reserved for cancellation / admin paths.
        """
        try:
            result = await self._release_script(
                keys=[lock_key(lock_name)],
                args=[owner_token, max_concurrency],
            )
        except BackendConnectionError:
            raise
        except Exception as exc:
            raise BackendConnectionError(
                f"lock release failed for {lock_name!r}: {exc}"
            ) from exc
        # Lua returns 1 (owned release), 2 (force release), 0 (no-op).
        # Both 1 and 2 count as "released" from the caller's perspective.
        return int(result) >= 1
