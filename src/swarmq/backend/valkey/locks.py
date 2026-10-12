"""Valkey-Locks (Plan U18, KTD4): ``lock_acquire/release/renew.lua``.

``LockManager`` ist die heutige Klasse (verbatim aus ``swarmq.locking``,
Importpfad bleibt als Alias gültig bis U15). ``ValkeyLockStore`` erfüllt den
Vertrag ``LockStore`` (Token-Typen) über denselben Skripten.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, cast

import valkey.asyncio as valkey_asyncio

from swarmq.backend.valkey.connection import as_backend_error
from swarmq.contracts import LockObservation, LockToken
from swarmq.exceptions import BackendConnectionError
from swarmq.keyspace import KeySpace
from swarmq.lua import load_script

_FORCE_OWNER = "__force__"


class LockManager:
    """Async wrapper around ``lock_acquire.lua`` + ``lock_release.lua``.

    One instance per worker, constructed after the backend connects
    (mirrors ``RateLimiter`` lifecycle in ``Worker.start``). Holds two
    registered ``AsyncScript`` instances so the NOSCRIPT-recovery loop
    inside ``valkey.asyncio.AsyncScript`` is the recovery path — we do
    not reimplement it.

    All Valkey-facing methods catch arbitrary exceptions and re-raise
    them as :class:`BackendConnectionError`: transport failures
    (``ConnectionError``, ``TimeoutError``, ``OSError``) as the plain class
    (an outage -- the lock gate defers), everything else (an error reply
    such as ``WRONGTYPE``, a script error) as the subclass
    :class:`BackendResponseError` (no outage -- the lock gate requeues).
    """

    def __init__(self, client: valkey_asyncio.Valkey, *, keys: KeySpace | None = None) -> None:
        self._client = client
        # Ohne ``keys`` nur ohne Namensraum (Review MR 15, P2-3); das Backend
        # reicht seinen KeySpace durch.
        self._keys = keys if keys is not None else KeySpace()
        self._acquire_script = client.register_script(load_script("lock_acquire"))
        self._release_script = client.register_script(load_script("lock_release"))
        self._renew_script = client.register_script(load_script("lock_renew"))
        # ``lock_extend`` (Plan 0.12.0, MR 13, BZ-25) wird bei erster Nutzung
        # registriert: nur der Lock-Keeper ruft es.
        self._extend_script: Any = None

    async def acquire(self, lock_name: str, owner_token: str, ttl_ms: int, max_concurrency: int) -> bool:
        """Try to claim the lock. Returns ``True`` on success, ``False``
        on conflict (mutex held by someone else, or semaphore full).

        Pulls server-side ``TIME`` once so the script's expiry
        arithmetic uses the same wall clock the SortedSet was scored
        with — same pattern as ``RateLimiter.check``.
        """
        try:
            seconds, microseconds = await self._client.time()
            now_ms = int(seconds) * 1000 + int(microseconds) // 1000
            result = cast(
                "int",
                await self._acquire_script(
                    keys=[self._keys.lock(lock_name)],
                    args=[owner_token, ttl_ms, max_concurrency, now_ms],
                ),
            )
        except BackendConnectionError:
            raise
        except Exception as exc:
            raise as_backend_error(f"lock acquire failed for {lock_name!r}: {exc}", exc) from exc
        return int(result) == 1

    async def release(self, lock_name: str, owner_token: str, max_concurrency: int) -> bool:
        """Release a lock previously acquired by this token.

        Returns ``True`` when the script reported a release (owned-release
        or admin force-release), ``False`` when nothing was released
        (foreign owner, key gone, or token not in the semaphore set).
        Pass ``owner_token="__force__"`` to bypass the owner check —
        reserved for cancellation / admin paths.
        """
        try:
            result = cast(
                "int",
                await self._release_script(keys=[self._keys.lock(lock_name)], args=[owner_token, max_concurrency]),
            )
        except BackendConnectionError:
            raise
        except Exception as exc:
            raise as_backend_error(f"lock release failed for {lock_name!r}: {exc}", exc) from exc
        # Lua returns 1 (owned release), 2 (force release), 0 (no-op).
        # Both 1 and 2 count as "released" from the caller's perspective.
        return int(result) >= 1

    async def renew(self, lock_name: str, owner_token: str, ttl_ms: int, max_concurrency: Literal[1] = 1) -> bool:
        """Extend the TTL of a mutex lock we still own (heartbeat path).

        Returns ``True`` when the script confirms the owner matches and the
        PEXPIRE succeeded, ``False`` when the lock is absent, expired, or
        held by a different owner. Mutex-only for v1 (``Literal[1]``,
        Unit 6); the runtime check is a safety net for untyped callers.
        """
        if max_concurrency != 1:
            raise NotImplementedError(
                f"LockManager.renew is mutex-only in this release; got max_concurrency={max_concurrency}"
            )
        try:
            result = cast(
                "int", await self._renew_script(keys=[self._keys.lock(lock_name)], args=[owner_token, ttl_ms])
            )
        except BackendConnectionError:
            raise
        except Exception as exc:
            raise as_backend_error(f"lock renew failed for {lock_name!r}: {exc}", exc) from exc
        return int(result) == 1

    async def extend(self, lock_name: str, owner_token: str, ttl_ms: int, max_concurrency: int) -> bool:
        """Frist eines gehaltenen Locks auf mindestens ``ttl_ms`` ab jetzt
        setzen, nie verkuerzen (``lock_extend.lua``; Plan 0.12.0, MR 13,
        BZ-25). Mutex und Semaphor-Slot; ``False`` ohne eigenen Halter."""
        if self._extend_script is None:
            self._extend_script = self._client.register_script(load_script("lock_extend"))
        try:
            now_ms = 0  # Mutex: das Skript liest ``now_ms`` nicht -- ein Roundtrip
            if max_concurrency != 1:
                seconds, microseconds = await self._client.time()
                now_ms = int(seconds) * 1000 + int(microseconds) // 1000
            result = await self._extend_script(
                keys=[self._keys.lock(lock_name)],
                args=[owner_token, ttl_ms, max_concurrency, now_ms],
            )
        except BackendConnectionError:
            raise
        except Exception as exc:
            raise as_backend_error(f"lock extend failed for {lock_name!r}: {exc}", exc) from exc
        return int(result) == 1


class ValkeyLockStore:
    """``LockStore``-Vertrag über ``LockManager``: nur ``acquire`` gibt
    ``LockToken`` aus; ``release``/``renew`` nehmen ausschließlich Tokens."""

    def __init__(self, client: valkey_asyncio.Valkey, *, keys: KeySpace | None = None) -> None:
        self._client = client
        # Ohne ``keys`` nur ohne Namensraum (Review MR 15, P2-3); das Backend
        # reicht seinen KeySpace durch.
        self._keys = keys if keys is not None else KeySpace()
        self._manager = LockManager(client, keys=self._keys)

    @property
    def manager(self) -> LockManager:
        """Die heutige API (bis U7/U11 vom Worker/Scheduler genutzt)."""
        return self._manager

    async def acquire(self, name: str, *, owner: str, ttl_ms: int, max_concurrency: int) -> LockToken | None:
        if await self._manager.acquire(name, owner, ttl_ms, max_concurrency):
            return LockToken(name=name, owner=owner, max_concurrency=max_concurrency)
        return None

    async def release(self, token: LockToken) -> bool:
        return await self._manager.release(token.name, token.owner, token.max_concurrency)

    async def renew(self, token: LockToken, ttl_ms: int) -> bool:
        if token.max_concurrency != 1:
            raise NotImplementedError("renew is only defined for mutex locks")
        return await self._manager.renew(token.name, token.owner, ttl_ms)

    async def extend(self, token: LockToken, ttl_ms: int) -> bool:
        return await self._manager.extend(token.name, token.owner, ttl_ms, token.max_concurrency)

    async def force_release(self, name: str, *, max_concurrency: int) -> bool:
        """Operator-Pfad ohne Owner-Check. ``lock_release.lua`` meldet für
        ``__force__`` immer „released" (2), auch ohne Halter — der Vertrag
        verlangt ``False`` ohne Halter, daher der EXISTS davor."""
        try:
            existed = bool(await self._client.exists(self._keys.lock(name)))
        except Exception as exc:
            raise BackendConnectionError(f"lock exists failed for {name!r}: {exc}") from exc
        await self._manager.release(name, _FORCE_OWNER, max_concurrency)
        return existed

    async def current_owner(self, name: str) -> str | None:
        """Halter eines Mutex-Locks (``None`` ohne Halter). Treiberfehler
        propagieren (Decision E4: „kein Leader" vs. „Backend unerreichbar"
        bleibt unterscheidbar). Für Semaphoren nicht definiert → ``None``."""
        raw = await self._client.get(self._keys.lock(name))
        if raw is None:
            return None
        # ``errors='replace'`` keeps ``schedule status`` working even if a
        # rogue writer stored non-UTF8 bytes under the lock key.
        result = raw.decode(errors="replace") if isinstance(raw, bytes) else str(raw)
        # Treat empty string the same as "no owner" — defensive against an
        # upstream writer that SET an empty value.
        return result or None

    async def observe(self, name: str, *, max_concurrency: int) -> LockObservation:
        """Halter und Rest-TTL eines Locks in einem Roundtrip (MULTI/EXEC;
        Plan 0.12.0, MR 13, BZ-05). Nur auf dem Konfliktpfad (Q7)."""
        key = self._keys.lock(name)
        try:
            async with self._client.pipeline(transaction=True) as pipe:
                if max_concurrency == 1:
                    pipe.get(key)
                else:
                    pipe.zrange(key, 0, -1)
                pipe.pttl(key)
                raw, pttl = cast("list[Any]", await pipe.execute())
        except BackendConnectionError:
            raise
        except Exception as exc:
            raise as_backend_error(f"lock observe failed for {name!r}: {exc}", exc) from exc
        values = [raw] if max_concurrency == 1 else list(raw or [])
        owners = frozenset(_as_text(value) for value in values if value is not None and _as_text(value))
        return LockObservation(owners, int(pttl))


def _as_text(value: object) -> str:
    return value.decode(errors="replace") if isinstance(value, bytes) else str(value)


if TYPE_CHECKING:  # Konformitäts-Anker (KTD1)
    from swarmq.contracts import LockStore

    _conforms_locks: type[LockStore] = ValkeyLockStore
