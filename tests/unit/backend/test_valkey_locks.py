"""``LockManager`` (verbatim aus ``swarmq.locking``, Plan U18) und der
``LockStore``-Adapter ``ValkeyLockStore`` — gegen gemockte Skripte.

Die Fehler-Wrapping- und Rückgabe-Tests stammen aus ``tests/unit/test_locking.py``;
dort bleiben die reinen Ableitungen (TTL-Formel, ``lock_key``).

Test-Liste 0.9.0 (Ausfall vs. Fehlerantwort):
1. Antwortet Valkey mit einem Fehler (``ResponseError``, z. B. WRONGTYPE),
   wirft ``acquire`` ``BackendResponseError`` -- kein Ausfall, das Gate
   stellt dafuer nicht zurueck.
2. Ein Transportfehler bleibt ein einfacher ``BackendConnectionError``.
3. Dasselbe fuer ``release`` und ``renew``.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from valkey.exceptions import ConnectionError as ValkeyConnectionError
from valkey.exceptions import TimeoutError as ValkeyTimeoutError

from swarmq.backend.valkey.locks import LockManager, ValkeyLockStore
from swarmq.contracts import LockToken
from swarmq.exceptions import BackendConnectionError, BackendResponseError


def _make_lock_manager_with_scripts(
    acquire_returns=None, release_returns=None, renew_returns=None, time_returns=(1, 0)
):
    """Build a LockManager whose register_script returns mockable scripts."""
    client = MagicMock()
    client.time = AsyncMock(return_value=time_returns)
    acquire_script = AsyncMock(return_value=acquire_returns)
    release_script = AsyncMock(return_value=release_returns)
    renew_script = AsyncMock(return_value=renew_returns)
    # register_script is sync and called 3x in __init__ (acquire, release, renew)
    client.register_script = MagicMock(side_effect=[acquire_script, release_script, renew_script])
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


class TestLockManagerOutageVersusErrorReply:
    async def test_acquire_error_reply_is_a_response_error(self):
        """Liste 1."""
        from valkey.exceptions import ResponseError

        manager, acquire_script, _, _ = _make_lock_manager_with_scripts()
        acquire_script.side_effect = ResponseError("WRONGTYPE Operation against a key")

        with pytest.raises(BackendResponseError, match="lock acquire failed"):
            await manager.acquire("name", "tok", 60_000, 1)

    async def test_acquire_transport_error_is_a_plain_outage(self):
        """Liste 2."""
        from valkey.exceptions import ConnectionError as ValkeyConnectionError

        manager, acquire_script, _, _ = _make_lock_manager_with_scripts()
        acquire_script.side_effect = ValkeyConnectionError("refused")

        with pytest.raises(BackendConnectionError) as exc_info:
            await manager.acquire("name", "tok", 60_000, 1)
        assert not isinstance(exc_info.value, BackendResponseError)

    @pytest.mark.parametrize("op", ["release", "renew"])
    async def test_release_and_renew_error_replies_are_response_errors(self, op):
        """Liste 3."""
        from valkey.exceptions import ResponseError

        manager, _, release_script, renew_script = _make_lock_manager_with_scripts()
        (release_script if op == "release" else renew_script).side_effect = ResponseError("WRONGTYPE")

        call = manager.release("name", "tok", 1) if op == "release" else manager.renew("name", "tok", 60_000)
        with pytest.raises(BackendResponseError, match=f"lock {op} failed"):
            await call


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
        manager, _, _, renew_script = _make_lock_manager_with_scripts(renew_returns=1)
        assert await manager.renew("name", "tok", 30_000) is True
        renew_script.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_renew_returns_false_on_zero(self):
        manager, _, _, _renew_script = _make_lock_manager_with_scripts(renew_returns=0)
        # Foreign owner OR key absent → script returns 0 → renew returns False
        assert await manager.renew("name", "tok", 30_000) is False

    @pytest.mark.asyncio
    async def test_renew_passes_lock_key_and_args(self):
        manager, _, _, renew_script = _make_lock_manager_with_scripts(renew_returns=1)
        await manager.renew("leader", "owner-42", 5_000)
        renew_script.assert_awaited_once_with(
            keys=["sq:lock:leader"],
            args=["owner-42", 5_000],
        )

    @pytest.mark.asyncio
    async def test_renew_wraps_runtime_error(self):
        manager, _, _, renew_script = _make_lock_manager_with_scripts(renew_returns=1)
        renew_script.side_effect = RuntimeError("connection reset")
        with pytest.raises(BackendConnectionError, match="lock renew failed"):
            await manager.renew("name", "tok", 30_000)

    @pytest.mark.asyncio
    async def test_renew_passes_through_existing_backend_connection_error(self):
        manager, _, _, renew_script = _make_lock_manager_with_scripts(renew_returns=1)
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


class TestValkeyLockStoreTokens:
    def _store(self, acquire=1, release=1, renew=1):
        client = MagicMock()
        client.time = AsyncMock(return_value=(1, 0))
        client.get = AsyncMock(return_value=b"owner-a")
        client.exists = AsyncMock(return_value=1)
        scripts = [AsyncMock(return_value=acquire), AsyncMock(return_value=release), AsyncMock(return_value=renew)]
        client.register_script = MagicMock(side_effect=scripts)
        return ValkeyLockStore(client), scripts

    async def test_acquire_returns_token_only_on_success(self):
        store, _ = self._store(acquire=1)
        assert await store.acquire("l", owner="owner-a", ttl_ms=1000, max_concurrency=1) == LockToken("l", "owner-a", 1)
        store2, _ = self._store(acquire=0)
        assert await store2.acquire("l", owner="owner-a", ttl_ms=1000, max_concurrency=1) is None

    async def test_release_and_renew_pass_token_fields_to_scripts(self):
        store, (_, release_script, renew_script) = self._store()
        token = LockToken("l", "owner-a", 1)
        assert await store.release(token) is True
        assert release_script.await_args.kwargs["args"] == ["owner-a", 1]
        assert await store.renew(token, ttl_ms=5000) is True
        assert renew_script.await_args.kwargs["args"] == ["owner-a", 5000]

    async def test_renew_rejects_semaphore_tokens(self):
        store, _ = self._store()
        with pytest.raises(NotImplementedError):
            await store.renew(LockToken("s", "owner-a", 2), ttl_ms=1000)

    async def test_force_release_is_false_when_the_lock_expired_before_the_delete(self):
        """Plan 0.14, U1 (KTD1): Der Lock läuft zwischen Prüfen und Freigeben
        ab; ``True`` hieße „entfernt“, obwohl nichts mehr da war."""
        store, _ = self._store()
        store._client.delete = AsyncMock(return_value=0)

        assert await store.force_release("l", max_concurrency=1) is False

    async def test_force_release_is_true_when_the_delete_removed_the_lock(self):
        """Gegenstück: ``DEL`` hat den Lock-Schlüssel entfernt (Plan 0.14, U1)."""
        store, _ = self._store()
        store._client.delete = AsyncMock(return_value=1)

        assert await store.force_release("l", max_concurrency=1) is True
        store._client.delete.assert_awaited_once()

    async def test_force_release_passes_through_existing_backend_connection_error(self):
        """Ein schon eingeordneter Ausfall (z. B. aus einem Client-Wrapper)
        bleibt ein Ausfall und wird nicht zur Fehlerantwort umgedeutet."""
        store, _ = self._store()
        original = BackendConnectionError("inner")
        store._client.delete = AsyncMock(side_effect=original)

        with pytest.raises(BackendConnectionError) as exc_info:
            await store.force_release("l", max_concurrency=1)

        assert exc_info.value is original

    async def test_current_owner_decodes_and_maps_empty_to_none(self):
        store, _ = self._store()
        assert await store.current_owner("l") == "owner-a"
        store._client.get = AsyncMock(return_value=b"")
        assert await store.current_owner("l") is None

    @pytest.mark.entscheidung("E-081")
    @pytest.mark.parametrize("error", [ValkeyConnectionError("down"), ValkeyTimeoutError("slow")])
    async def test_current_owner_propagates_raw_driver_errors(self, error):
        """E-081: anders als ``force_release`` und ``observe`` übersetzt
        ``current_owner`` Treiberfehler nicht in ``BackendConnectionError``;
        „kein Halter“ (``None``) bleibt von „Valkey weg“ unterscheidbar."""
        store, _ = self._store()
        store._client.get = AsyncMock(side_effect=error)

        with pytest.raises(type(error)) as caught:
            await store.current_owner("l")

        assert caught.value is error
