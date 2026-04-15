"""Unit tests for ValkeyBackend."""

from unittest.mock import AsyncMock, patch

import pytest
from valkey.exceptions import ConnectionError as ValkeyConnectionError
from valkey.exceptions import ValkeyError

from swarmq.backend.valkey import ValkeyBackend
from swarmq.exceptions import BackendConnectionError


class TestValkeyBackendInit:
    def test_stores_url(self):
        url = "valkey://localhost:6379/0"
        backend = ValkeyBackend(url)
        assert backend._url == url


class TestValkeyBackendConnect:
    async def test_connect_creates_client_and_pings(self):
        url = "valkey://localhost:6379/0"
        backend = ValkeyBackend(url)
        mock_client = AsyncMock()

        with patch("swarmq.backend.valkey.valkey.asyncio.Valkey.from_url", return_value=mock_client) as mock_from_url:
            await backend.connect()

        mock_from_url.assert_called_once_with(url)
        mock_client.ping.assert_awaited_once()


class TestValkeyBackendClose:
    async def test_close_closes_client(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        backend._client = mock_client

        await backend.close()

        mock_client.aclose.assert_awaited_once()
        assert backend._client is None

    async def test_close_is_idempotent(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        assert backend._client is None

        # Should not raise when no client exists
        await backend.close()


class TestValkeyBackendStoreResult:
    async def test_store_result_calls_set_with_px(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        backend._client = mock_client

        task_id = "abc-123"
        data = b'{"result": 42}'
        ttl_ms = 3600000

        await backend.store_result(task_id, data, ttl_ms)

        mock_client.set.assert_awaited_once_with(
            "sq:result:abc-123", data, px=ttl_ms
        )


class TestValkeyBackendGetResult:
    async def test_get_result_returns_data(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        backend._client = mock_client
        expected = b'{"result": 42}'
        mock_client.get.return_value = expected

        result = await backend.get_result("abc-123")

        mock_client.get.assert_awaited_once_with("sq:result:abc-123")
        assert result == expected

    async def test_get_result_returns_none_for_missing_key(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        backend._client = mock_client
        mock_client.get.return_value = None

        result = await backend.get_result("nonexistent")

        mock_client.get.assert_awaited_once_with("sq:result:nonexistent")
        assert result is None


class TestValkeyBackendDeleteResult:
    async def test_delete_result_calls_del(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        backend._client = mock_client

        await backend.delete_result("abc-123")

        mock_client.delete.assert_awaited_once_with("sq:result:abc-123")


class TestValkeyBackendStoreResultError:
    async def test_store_result_wraps_valkey_error(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        backend._client = mock_client
        mock_client.set.side_effect = ValkeyError("write failed")

        with pytest.raises(BackendConnectionError):
            await backend.store_result("abc-123", b'{"result": 42}', 3600000)


class TestValkeyBackendGetResultError:
    async def test_get_result_wraps_valkey_error(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        backend._client = mock_client
        mock_client.get.side_effect = ValkeyError("read failed")

        with pytest.raises(BackendConnectionError):
            await backend.get_result("abc-123")


class TestValkeyBackendDeleteResultError:
    async def test_delete_result_wraps_valkey_error(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        backend._client = mock_client
        mock_client.delete.side_effect = ValkeyError("delete failed")

        with pytest.raises(BackendConnectionError):
            await backend.delete_result("abc-123")


class TestValkeyBackendConnectGuard:
    async def test_connect_twice_only_calls_from_url_once(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()

        with patch("swarmq.backend.valkey.valkey.asyncio.Valkey.from_url", return_value=mock_client) as mock_from_url:
            await backend.connect()
            await backend.connect()

        mock_from_url.assert_called_once()


class TestValkeyBackendConnectError:
    async def test_connect_wraps_connection_error(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        mock_client.ping.side_effect = ValkeyConnectionError("Connection refused")

        with patch("swarmq.backend.valkey.valkey.asyncio.Valkey.from_url", return_value=mock_client):
            with pytest.raises(BackendConnectionError):
                await backend.connect()


class TestValkeyBackendKeyFormat:
    def test_key_format(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        assert backend._key("task-abc-123") == "sq:result:task-abc-123"

    def test_key_format_with_uuid(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        task_id = "550e8400-e29b-41d4-a716-446655440000"
        assert backend._key(task_id) == f"sq:result:{task_id}"
