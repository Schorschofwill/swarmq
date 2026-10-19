"""Unit tests for ValkeyBackend."""

import asyncio
from unittest.mock import AsyncMock, Mock, patch

import pytest
from valkey.exceptions import ConnectionError as ValkeyConnectionError
from valkey.exceptions import TimeoutError as ValkeyTimeoutError
from valkey.exceptions import ValkeyError

from swarmq.backend.valkey import ValkeyBackend
from swarmq.exceptions import BackendConnectionError, ConfigurationError
from tests.unit.backend._pipeline_fakes import FakeScript, fake_client


class TestValkeyBackendInit:
    def test_stores_url(self):
        url = "valkey://localhost:6379/0"
        backend = ValkeyBackend(url)
        assert backend._url == url

    def test_url_path_owns_client(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        assert backend._owns_client is True


class TestValkeyBackendInjection:
    """Producer-path injection: borrow an existing client or pool instead
    of building (and owning) a new one. Ownership truth table — a borrowed
    resource is never closed by swarmq."""

    def test_injected_client_is_stored_and_not_owned(self):
        mock_client = AsyncMock()
        mock_client.register_script = Mock()
        backend = ValkeyBackend(client=mock_client)

        assert backend._client is mock_client
        assert backend._owns_client is False
        # Credential-free placeholder, never None and never the caller's URL.
        assert backend._url == "<injected>"

    def test_injected_pool_builds_client_and_not_owned(self):
        mock_pool = Mock()
        mock_client = AsyncMock()
        mock_client.register_script = Mock()
        with patch("swarmq.backend.valkey.connection.valkey.asyncio.Valkey", return_value=mock_client) as mock_valkey:
            backend = ValkeyBackend(pool=mock_pool)

        mock_valkey.assert_called_once_with(connection_pool=mock_pool)
        assert backend._client is mock_client
        assert backend._owns_client is False
        assert backend._url == "<injected>"

    def test_no_source_raises_configuration_error(self):
        with pytest.raises(ConfigurationError, match="exactly one"):
            ValkeyBackend()

    def test_url_and_client_together_raises_configuration_error(self):
        with pytest.raises(ConfigurationError, match="exactly one"):
            ValkeyBackend("valkey://localhost:6379/0", client=AsyncMock())

    def test_url_and_pool_together_raises_configuration_error(self):
        with pytest.raises(ConfigurationError, match="exactly one"):
            ValkeyBackend("valkey://localhost:6379/0", pool=Mock())

    async def test_connect_is_noop_for_injected_client(self):
        """connect() must not build a second client when one was injected."""
        mock_client = AsyncMock()
        mock_client.register_script = Mock()
        backend = ValkeyBackend(client=mock_client)

        with patch("swarmq.backend.valkey.connection.valkey.asyncio.Valkey.from_url") as mock_from_url:
            await backend.connect()

        mock_from_url.assert_not_called()
        assert backend._client is mock_client

    async def test_close_does_not_close_injected_client(self):
        """AE1: a borrowed client survives the swarmq teardown."""
        mock_client = AsyncMock()
        mock_client.register_script = Mock()
        backend = ValkeyBackend(client=mock_client)

        await backend.close()

        mock_client.aclose.assert_not_awaited()

    async def test_close_does_not_close_injected_pool_client(self):
        mock_pool = Mock()
        mock_client = AsyncMock()
        mock_client.register_script = Mock()
        with patch("swarmq.backend.valkey.connection.valkey.asyncio.Valkey", return_value=mock_client):
            backend = ValkeyBackend(pool=mock_pool)

        await backend.close()

        mock_client.aclose.assert_not_awaited()

    async def test_url_path_still_closes_owned_client(self):
        """The default (URL) path keeps closing its self-built client."""
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        mock_client.register_script = Mock()
        backend._client = mock_client  # simulate post-connect state

        await backend.close()

        mock_client.aclose.assert_awaited_once()
        assert backend._client is None

    async def test_injected_client_used_for_operations(self):
        """Borrowed client serves backend operations normally (AE1)."""
        mock_client = AsyncMock()
        mock_client.register_script = Mock()
        mock_client.get.return_value = b'{"result": 1}'
        backend = ValkeyBackend(client=mock_client)

        result = await backend.get_result("abc-123")

        mock_client.get.assert_awaited_once_with("sq:result:abc-123")
        assert result == b'{"result": 1}'


class TestValkeyBackendConnect:
    async def test_connect_creates_client_and_pings(self):
        url = "valkey://localhost:6379/0"
        backend = ValkeyBackend(url)
        mock_client = AsyncMock()
        mock_client.register_script = Mock()

        with patch(
            "swarmq.backend.valkey.connection.valkey.asyncio.Valkey.from_url", return_value=mock_client
        ) as mock_from_url:
            await backend.connect()

        # Reliability hardening (2026-04-17 spike): retry policy +
        # health_check_interval are now passed — verify url + presence
        # of retry kwargs without binding the test to the exact Retry
        # implementation.
        mock_from_url.assert_called_once()
        call = mock_from_url.call_args
        assert call.args == (url,)
        assert "retry" in call.kwargs
        assert call.kwargs["health_check_interval"] == 30
        assert OSError in call.kwargs["retry_on_error"]
        mock_client.ping.assert_awaited_once()

    async def test_connect_sets_socket_timeouts(self):
        """Härtung R2 P1-2: ohne ``socket_timeout`` blockiert ein
        bereits laufender ``recv`` auf einer half-open Connection
        unbegrenzt — ``health_check_interval`` pingt nur VOR einem
        Kommando nach Idle, es bricht keinen laufenden Read ab.
        ``socket_connect_timeout`` begrenzt zusätzlich den
        TCP-Handshake gegen einen blackholed Host."""
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        mock_client.register_script = Mock()

        with patch(
            "swarmq.backend.valkey.connection.valkey.asyncio.Valkey.from_url",
            return_value=mock_client,
        ) as mock_from_url:
            await backend.connect()

        kwargs = mock_from_url.call_args.kwargs
        assert kwargs.get("socket_timeout"), "socket_timeout must be set"
        assert kwargs["socket_timeout"] > 0
        assert kwargs.get("socket_connect_timeout"), "socket_connect_timeout must be set"
        assert kwargs["socket_connect_timeout"] > 0


class TestValkeyBackendClose:
    async def test_close_closes_client(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        mock_client.register_script = Mock()
        backend._client = mock_client

        await backend.close()

        mock_client.aclose.assert_awaited_once()
        assert backend._client is None

    async def test_a_store_fetched_before_close_opens_no_connection_after_it(self):
        """Ein Worker hält seine Stores (Locks, Ausführungsschutz, …) über
        ``close()`` hinaus. Ein Handler, der nach dem Abbruch des Workers noch
        abschließt, darf über den geschlossenen Pool keine neue Verbindung
        öffnen (sie schlösse niemand mehr); er scheitert wie jede Operation
        nach ``close()``. Hier am Ausführungsschutz, dessen Pipeline sich die
        Verbindung direkt beim Pool holt."""
        client, connection, pool = fake_client(lambda _argv: [b"claimed", 0])
        client.register_script = lambda _source: FakeScript()
        client.aclose = AsyncMock()
        backend = ValkeyBackend("valkey://localhost:6379/0")
        backend._client = client
        guard = backend.execution_guard

        await backend.close()

        with pytest.raises(BackendConnectionError, match="closed"):
            await guard.claim("t-1", owner="o", retry_count=0, ttl_ms=1, state_ttl_ms=1, max_orphans=3)
        assert pool.in_use == 0
        assert connection.sent == []

    async def test_close_is_idempotent(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        assert backend._client is None

        # Should not raise when no client exists
        await backend.close()


class TestValkeyBackendStoreResult:
    async def test_store_result_calls_set_with_px(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        mock_client.register_script = Mock()
        backend._client = mock_client

        task_id = "abc-123"
        data = b'{"result": 42}'
        ttl_ms = 3600000

        await backend.store_result(task_id, data, ttl_ms)

        mock_client.set.assert_awaited_once_with("sq:result:abc-123", data, px=ttl_ms)


class TestValkeyBackendGetResult:
    async def test_get_result_returns_data(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        mock_client.register_script = Mock()
        backend._client = mock_client
        expected = b'{"result": 42}'
        mock_client.get.return_value = expected

        result = await backend.get_result("abc-123")

        mock_client.get.assert_awaited_once_with("sq:result:abc-123")
        assert result == expected

    async def test_get_result_returns_none_for_missing_key(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        mock_client.register_script = Mock()
        backend._client = mock_client
        mock_client.get.return_value = None

        result = await backend.get_result("nonexistent")

        mock_client.get.assert_awaited_once_with("sq:result:nonexistent")
        assert result is None


class TestValkeyBackendDeleteResult:
    async def test_delete_result_calls_del(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        mock_client.register_script = Mock()
        backend._client = mock_client

        await backend.delete_result("abc-123")

        mock_client.delete.assert_awaited_once_with("sq:result:abc-123")


class TestValkeyBackendStoreResultError:
    async def test_store_result_wraps_valkey_error(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        mock_client.register_script = Mock()
        backend._client = mock_client
        mock_client.set.side_effect = ValkeyError("write failed")

        with pytest.raises(BackendConnectionError):
            await backend.store_result("abc-123", b'{"result": 42}', 3600000)


class TestValkeyBackendGetResultError:
    async def test_get_result_wraps_valkey_error(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        mock_client.register_script = Mock()
        backend._client = mock_client
        mock_client.get.side_effect = ValkeyError("read failed")

        with pytest.raises(BackendConnectionError):
            await backend.get_result("abc-123")


class TestValkeyBackendDeleteResultError:
    async def test_delete_result_wraps_valkey_error(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        mock_client.register_script = Mock()
        backend._client = mock_client
        mock_client.delete.side_effect = ValkeyError("delete failed")

        with pytest.raises(BackendConnectionError):
            await backend.delete_result("abc-123")


class TestValkeyBackendConnectGuard:
    async def test_connect_twice_only_calls_from_url_once(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        mock_client.register_script = Mock()

        with patch(
            "swarmq.backend.valkey.connection.valkey.asyncio.Valkey.from_url", return_value=mock_client
        ) as mock_from_url:
            await backend.connect()
            await backend.connect()

        mock_from_url.assert_called_once()


class TestValkeyBackendConnectError:
    async def test_connect_wraps_connection_error(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        mock_client.register_script = Mock()
        mock_client.ping.side_effect = ValkeyConnectionError("Connection refused")

        with patch("swarmq.backend.valkey.connection.valkey.asyncio.Valkey.from_url", return_value=mock_client):  # noqa: SIM117
            with pytest.raises(BackendConnectionError):
                await backend.connect()

    async def test_connect_error_names_the_step_and_the_cause(self):
        """F4-12: Die Meldung sagt, dass der Verbindungsaufbau zu Valkey
        scheiterte, und nennt die Ursache des Treibers."""
        backend = ValkeyBackend("valkey://localhost:6379/0", eviction_check="off")
        mock_client = AsyncMock()
        mock_client.register_script = Mock()
        mock_client.ping.side_effect = ValkeyTimeoutError("Timeout reading from localhost:6379")

        with (
            patch("swarmq.backend.valkey.connection.valkey.asyncio.Valkey.from_url", return_value=mock_client),
            pytest.raises(BackendConnectionError, match=r"^Valkey connect failed: Timeout reading from"),
        ):
            await backend.connect()

    async def test_failed_connect_closes_its_client_and_stays_unconnected(self):
        """F4-13: Nach gescheitertem PING bleibt kein halber Client stehen;
        der eigene Client wird geschlossen."""
        backend = ValkeyBackend("valkey://localhost:6379/0", eviction_check="off")
        mock_client = AsyncMock()
        mock_client.register_script = Mock()
        mock_client.ping.side_effect = ValkeyConnectionError("Connection refused")

        with (
            patch("swarmq.backend.valkey.connection.valkey.asyncio.Valkey.from_url", return_value=mock_client),
            pytest.raises(BackendConnectionError),
        ):
            await backend.connect()

        mock_client.aclose.assert_awaited_once()
        with pytest.raises(BackendConnectionError, match="not connected"):
            _ = backend.client

    @pytest.mark.timeout(5)
    async def test_cancelled_connect_closes_its_client(self):
        """F4-13: Auch ein Abbruch waehrend des PING (``BaseException``)
        schliesst den eigenen Client."""
        backend = ValkeyBackend("valkey://localhost:6379/0", eviction_check="off")
        mock_client = AsyncMock()
        mock_client.register_script = Mock()
        mock_client.ping.side_effect = asyncio.CancelledError

        with (
            patch("swarmq.backend.valkey.connection.valkey.asyncio.Valkey.from_url", return_value=mock_client),
            pytest.raises(asyncio.CancelledError),
        ):
            await backend.connect()

        mock_client.aclose.assert_awaited_once()
        assert backend._client is None

    @pytest.mark.timeout(5)
    async def test_cancel_during_the_eviction_check_closes_its_client(self):
        """F4-13: Ein Abbruch mitten in der Eviction-Prüfung (nach
        erfolgreichem PING) schließt den eigenen Client genau einmal; der
        Abbruch geht unverändert nach außen, und ``connect()`` bleibt
        wiederholbar."""
        backend = ValkeyBackend("valkey://localhost:6379/0", eviction_check="warn")
        mock_client = AsyncMock()
        mock_client.register_script = Mock()
        probing = asyncio.Event()

        async def hanging_probe(*_args: object, **_kwargs: object) -> None:
            probing.set()
            await asyncio.Event().wait()

        with (
            patch("swarmq.backend.valkey.connection.valkey.asyncio.Valkey.from_url", return_value=mock_client),
            patch("swarmq.backend.valkey.connection.probe_eviction", side_effect=hanging_probe),
        ):
            connecting = asyncio.create_task(backend.connect())
            await probing.wait()
            connecting.cancel()
            with pytest.raises(asyncio.CancelledError):
                await connecting

        mock_client.ping.assert_awaited_once()
        mock_client.aclose.assert_awaited_once()
        assert backend._client is None
        assert backend.eviction_check is None

    @pytest.mark.timeout(5)
    async def test_error_while_closing_does_not_hide_the_connect_error(self):
        """F4-13: Wirft ``aclose()`` beim Aufräumen, kommt trotzdem die
        ursprüngliche ``BackendConnectionError`` nach außen, nicht der
        Folgefehler; das Backend bleibt unverbunden."""
        backend = ValkeyBackend("valkey://localhost:6379/0", eviction_check="off")
        mock_client = AsyncMock()
        mock_client.register_script = Mock()
        mock_client.ping.side_effect = ValkeyConnectionError("Connection refused")
        mock_client.aclose.side_effect = OSError("already gone")

        with (
            patch("swarmq.backend.valkey.connection.valkey.asyncio.Valkey.from_url", return_value=mock_client),
            pytest.raises(BackendConnectionError, match="Connection refused") as raised,
        ):
            await backend.connect()

        assert isinstance(raised.value.__cause__, ValkeyConnectionError)
        mock_client.aclose.assert_awaited_once()
        assert backend._client is None

    @pytest.mark.timeout(5)
    async def test_error_while_closing_does_not_hide_a_cancel(self):
        """F4-13: Auch ein Abbruch bleibt ein Abbruch, wenn ``aclose()``
        beim Aufräumen wirft."""
        backend = ValkeyBackend("valkey://localhost:6379/0", eviction_check="off")
        mock_client = AsyncMock()
        mock_client.register_script = Mock()
        mock_client.ping.side_effect = asyncio.CancelledError
        mock_client.aclose.side_effect = OSError("already gone")

        with (
            patch("swarmq.backend.valkey.connection.valkey.asyncio.Valkey.from_url", return_value=mock_client),
            pytest.raises(asyncio.CancelledError),
        ):
            await backend.connect()

        mock_client.aclose.assert_awaited_once()
        assert backend._client is None


class TestValkeyBackendKeyFormat:
    def test_key_format(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        assert backend._key("task-abc-123") == "sq:result:task-abc-123"

    def test_key_format_with_uuid(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        task_id = "550e8400-e29b-41d4-a716-446655440000"
        assert backend._key(task_id) == f"sq:result:{task_id}"


class TestValkeyBackendNotifyResult:
    async def test_notify_result_calls_publish_with_correct_channel(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        mock_client.register_script = Mock()
        backend._client = mock_client

        await backend.notify_result("task-123")

        mock_client.publish.assert_awaited_once_with("sq:notify:task-123", "done")


class TestValkeyBackendGetResults:
    async def test_get_results_calls_mget_with_correct_keys(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        mock_client.register_script = Mock()
        backend._client = mock_client
        mock_client.mget.return_value = [b"r1", b"r2", b"r3"]

        result = await backend.get_results(["a", "b", "c"])

        mock_client.mget.assert_awaited_once_with("sq:result:a", "sq:result:b", "sq:result:c")
        assert result == [b"r1", b"r2", b"r3"]

    async def test_get_results_empty_list_returns_empty(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        mock_client.register_script = Mock()
        backend._client = mock_client

        result = await backend.get_results([])

        mock_client.mget.assert_not_awaited()
        assert result == []


class TestValkeyBackendNotifyResultError:
    async def test_notify_result_swallows_valkey_error(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        mock_client.register_script = Mock()
        backend._client = mock_client
        mock_client.publish.side_effect = ValkeyError("publish failed")

        # Should NOT raise -- fire-and-forget
        await backend.notify_result("task-123")


class TestValkeyBackendGetResultsError:
    async def test_get_results_wraps_valkey_error(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        mock_client.register_script = Mock()
        backend._client = mock_client
        mock_client.mget.side_effect = ValkeyError("mget failed")

        with pytest.raises(BackendConnectionError):
            await backend.get_results(["a", "b"])


class TestValkeyBackendGetResultsPartial:
    async def test_get_results_returns_none_for_missing_keys(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        mock_client.register_script = Mock()
        backend._client = mock_client
        mock_client.mget.return_value = [b"data-a", None, b"data-c"]

        result = await backend.get_results(["a", "missing", "c"])

        mock_client.mget.assert_awaited_once_with("sq:result:a", "sq:result:missing", "sq:result:c")
        assert result == [b"data-a", None, b"data-c"]


# ---------------------------------------------------------------------------
# Cancel-flag operations (Phase 3b.3 / Unit 3)
# ---------------------------------------------------------------------------


class TestValkeyBackendCancelKey:
    def test_cancel_key_format(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        assert backend._cancel_key("task-abc") == "sq:cancel:task-abc"


class TestValkeyBackendSetCancelFlag:
    async def test_set_cancel_flag_calls_set_with_nx_px(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        mock_client.register_script = Mock()
        mock_client.set.return_value = True  # valkey-py with nx=True returns True on success
        backend._client = mock_client

        result = await backend.set_cancel_flag("task-abc", 60_000)

        mock_client.set.assert_awaited_once_with("sq:cancel:task-abc", "1", nx=True, px=60_000)
        assert result is True

    async def test_set_cancel_flag_returns_false_when_already_set(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        mock_client.register_script = Mock()
        mock_client.set.return_value = None  # nx=True returns None when key already exists
        backend._client = mock_client

        result = await backend.set_cancel_flag("task-abc", 60_000)

        assert result is False

    async def test_set_cancel_flag_wraps_valkey_error(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        mock_client.register_script = Mock()
        mock_client.set.side_effect = ValkeyError("write failed")
        backend._client = mock_client

        with pytest.raises(BackendConnectionError):
            await backend.set_cancel_flag("task-abc", 60_000)


class TestValkeyBackendIsCancelled:
    async def test_is_cancelled_returns_true_when_key_exists(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        mock_client.register_script = Mock()
        mock_client.exists.return_value = 1
        backend._client = mock_client

        result = await backend.is_cancelled("task-abc")

        mock_client.exists.assert_awaited_once_with("sq:cancel:task-abc")
        assert result is True

    async def test_is_cancelled_returns_false_when_key_missing(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        mock_client.register_script = Mock()
        mock_client.exists.return_value = 0
        backend._client = mock_client

        result = await backend.is_cancelled("task-abc")
        assert result is False

    async def test_is_cancelled_wraps_valkey_error(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        mock_client.register_script = Mock()
        mock_client.exists.side_effect = ValkeyError("read failed")
        backend._client = mock_client

        with pytest.raises(BackendConnectionError):
            await backend.is_cancelled("task-abc")


class TestValkeyBackendClearCancelFlag:
    @pytest.mark.entscheidung("E-020")
    async def test_clear_cancel_flag_shortens_to_one_hour(self):
        # BZ-23 (Plan 0.12.0, MR 5): PEXPIRE … LT statt DEL.
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        mock_client.register_script = Mock()
        backend._client = mock_client

        await backend.clear_cancel_flag("task-abc")

        mock_client.pexpire.assert_awaited_once_with("sq:cancel:task-abc", 3_600_000, lt=True)
        mock_client.delete.assert_not_awaited()

    async def test_clear_cancel_flag_swallows_valkey_error(self):
        """Flag-DEL failures must not propagate — orphan self-heals via TTL.

        See plan Unit 3 Approach: clear_cancel_flag fängt
        BackendConnectionError intern, loggt, gibt None zurück.
        """
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        mock_client.register_script = Mock()
        mock_client.pexpire.side_effect = ValkeyError("pexpire failed")
        backend._client = mock_client

        # Must not raise
        result = await backend.clear_cancel_flag("task-abc")  # type: ignore[func-returns-value]  # Laufzeit-Rueckgabe trotz -> None pruefen
        assert result is None


# ---------------------------------------------------------------------------
# Schedule operations (Unit 5)
# ---------------------------------------------------------------------------


def _async_iter(items):
    """Helper: async generator from a list, for mocking scan_iter."""

    async def gen():
        for item in items:
            yield item

    return gen()


class TestSchedulerLeader:
    """Plan 2026-04-23-001 CLI-02 / KP-5 — Unit 4a.

    ``get_scheduler_leader`` is a thin, read-only wrapper around
    ``_client.get(SCHEDULER_LOCK_KEY)`` that the CLI can consume
    without poking at backend private state. Per Decision E4,
    connection errors propagate so callers can distinguish
    "no leader" (``None``) from "backend unreachable" (exception).
    """

    async def test_get_scheduler_leader_returns_holder_when_locked(self):
        from swarmq.locking import SCHEDULER_LOCK_KEY

        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        mock_client.register_script = Mock()
        mock_client.get.return_value = b"worker-42"
        backend._client = mock_client

        result = await backend.get_scheduler_leader()

        mock_client.get.assert_awaited_once_with(SCHEDULER_LOCK_KEY)
        assert result == "worker-42"

    async def test_get_scheduler_leader_decodes_bytes_to_str(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        mock_client.register_script = Mock()
        mock_client.get.return_value = b"node-7.region-us-east-1"
        backend._client = mock_client

        result = await backend.get_scheduler_leader()

        assert isinstance(result, str)
        assert result == "node-7.region-us-east-1"

    async def test_get_scheduler_leader_returns_none_when_unlocked(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        mock_client.register_script = Mock()
        mock_client.get.return_value = None
        backend._client = mock_client

        result = await backend.get_scheduler_leader()

        assert result is None

    @pytest.mark.entscheidung("E-081")
    async def test_get_scheduler_leader_propagates_connection_error(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        mock_client.register_script = Mock()
        mock_client.get.side_effect = ValkeyConnectionError("backend down")
        backend._client = mock_client

        with pytest.raises(ValkeyConnectionError, match="backend down"):
            await backend.get_scheduler_leader()

    @pytest.mark.entscheidung("E-081")
    async def test_get_scheduler_leader_propagates_timeout_error(self):
        from valkey.exceptions import TimeoutError as ValkeyTimeoutError

        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        mock_client.register_script = Mock()
        mock_client.get.side_effect = ValkeyTimeoutError("took too long")
        backend._client = mock_client

        with pytest.raises(ValkeyTimeoutError, match="took too long"):
            await backend.get_scheduler_leader()

    async def test_get_scheduler_leader_handles_non_utf8_bytes(self):
        """A rogue writer storing non-UTF8 bytes must not crash the CLI.

        The contract surfaces a (possibly mojibake) string via the
        ``errors='replace'`` decode rather than a ``UnicodeDecodeError``
        traceback — ``schedule status`` would otherwise refuse to render.
        """
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        mock_client.register_script = Mock()
        # 0xff is invalid UTF-8 start byte
        mock_client.get.return_value = b"node-\xff-bad"
        backend._client = mock_client

        result = await backend.get_scheduler_leader()

        assert isinstance(result, str)
        # No exception; placeholder char present.
        assert "node-" in result

    async def test_get_scheduler_leader_treats_empty_bytes_as_none(self):
        """Empty bytes (``b''``) must surface as ``None``, not ``""``.

        The lock contract only ever writes the holder ID; an empty
        value is malformed state. Treating it as "no leader" keeps the
        ``leader is None`` callers from spuriously branching into the
        "leader elected" path on garbage state.
        """
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        mock_client.register_script = Mock()
        mock_client.get.return_value = b""
        backend._client = mock_client

        result = await backend.get_scheduler_leader()

        assert result is None
