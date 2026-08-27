"""Task-locking primitives for SwarmQ.

Backed by ``lock_acquire.lua`` + ``lock_release.lua`` in
``src/swarmq/lua/``. The Lua scripts handle both Mutex (max_concurrency=1
via ``SET NX PX``) and Semaphore (max_concurrency>1 via SortedSet)
modes; this module provides the Python surface (TTL helper, async
``acquire`` / ``release``).

Seit Plan U18 lebt ``LockManager`` in ``swarmq.backend.valkey.locks``; dieses
Modul hält die reinen Ableitungen (``lock_key``, ``compute_lock_ttl_ms``,
``SCHEDULER_LOCK_NAME``/``SCHEDULER_LOCK_KEY``) und den Import-Alias
``swarmq.locking.LockManager`` (KTD10, entfällt in U15).
"""

from __future__ import annotations

from typing import Any

# Buffer added on top of ``hard_timeout`` to derive the lock TTL.
# The lock TTL is purely a
# crash-recovery deadline: under normal operation the holder releases the
# lock cooperatively (the watchdog cancels the task at ``hard_timeout``), so
# the TTL never matters. It only fires when a holder dies hard (SIGKILL/OOM)
# without releasing. Therefore the TTL must OUTLIVE the longest a holder can
# legitimately run — ``hard_timeout`` — by enough to cover watchdog-fire
# precision and the acquire→start / cancel→release latencies. A TTL shorter
# than ``hard_timeout`` (the previous ``0.25 * hard_timeout`` formula) lets
# the lock expire mid-execution, so a second worker can acquire the same
# mutex while the first still runs → duplicate execution of serialized work.
# Locks pull server-side Valkey TIME, so there is no inter-worker clock skew
# to budget for; the buffer only covers single-worker timing slack.
_LOCK_TTL_BUFFER_SECONDS = 30

_LOCK_KEY_PREFIX = "sq:lock:"


def lock_key(lock_name: str) -> str:
    """Map a user-facing lock name to its Valkey key. Single source of
    truth so producers and consumers agree on the namespace."""
    return f"{_LOCK_KEY_PREFIX}{lock_name}"


def compute_lock_ttl_ms(hard_timeout: int | None) -> int:
    """Lock TTL in ms = ``hard_timeout + _LOCK_TTL_BUFFER_SECONDS``.

    The TTL is the crash-recovery window, and it MUST outlive the
    longest a holder can legitimately run (``hard_timeout``). Under
    normal operation the watchdog cancels the task at ``hard_timeout``
    and the lock is released cooperatively via ``LockManager.release``,
    so the TTL is never reached. It only fires when a holder dies hard
    (SIGKILL/OOM) without releasing — then the next worker waits at most
    ``ttl_ms`` to re-acquire.

    The previous formula ``max(30s, 0.25 * hard_timeout)`` produced a
    TTL *shorter* than ``hard_timeout`` for every ``hard_timeout > 30s``,
    so the lock could expire while the holder was still legitimately
    running and a second worker could acquire the same mutex — duplicate
    execution of work that ``@task(lock=...)`` is meant to serialize.
    The same TTL governs
    semaphore-slot expiry, so this also stops a still-running semaphore
    holder's slot from being swept and over-admitting concurrency.

    Trade-off: for a long ``hard_timeout`` the recovery window is
    correspondingly long (a 1h mutex task → ~1h recovery after a hard
    crash). That is the correct cost of mutual exclusion without a
    renewal heartbeat; choose a tighter ``hard_timeout`` for faster
    recovery.

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
    seconds = hard_timeout + _LOCK_TTL_BUFFER_SECONDS
    return int(seconds * 1000)

SCHEDULER_LOCK_NAME = "{sched}:leader"
"""Logical lock name — LockManager's automatic ``sq:lock:`` prefix
yields the final Valkey key ``sq:lock:{sched}:leader``.

The ``{sched}`` hash tag co-locates the leader lock
with ``sq:{sched}:zset`` / ``sq:{sched}:defs`` in one Valkey-Cluster
hash slot — ``sched_commit.lua`` touches all three keys in one EVAL
and would otherwise fail with CROSSSLOT on a sharded cluster.

BREAKING (pre-1.0): deployments upgrading from the un-tagged
``sq:lock:scheduler:leader`` / ``sq:sched:*`` keys must re-create
their schedules (or RENAME the keys manually) — swarmq does not
migrate them."""

SCHEDULER_LOCK_KEY = lock_key(SCHEDULER_LOCK_NAME)
"""Final Valkey key for the leader lock. Derived live from
``lock_key()`` so the invariant ``SCHEDULER_LOCK_KEY ==
_LOCK_KEY_PREFIX + SCHEDULER_LOCK_NAME`` holds by construction — if
the prefix ever changes, ``sched_commit.lua``'s owner-check and the
CLI status view stay in sync automatically (Unit 10 / Fork B1)."""


def __getattr__(name: str) -> Any:
    """Import-Alias (KTD10, bis U15): ``swarmq.locking.LockManager`` ist
    ``swarmq.backend.valkey.locks.LockManager``. Lazy, damit dieses reine
    Modul die Valkey-Implementierung nicht eager zieht."""
    if name == "LockManager":
        from swarmq.backend.valkey.locks import LockManager

        return LockManager
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
