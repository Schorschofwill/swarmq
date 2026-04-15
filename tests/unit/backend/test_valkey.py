"""Unit tests for ValkeyBackend."""

import asyncio
from unittest.mock import AsyncMock, Mock, patch

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


class TestValkeyBackendPublishNotification:
    async def test_publish_notification_calls_publish_with_correct_channel(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        backend._client = mock_client

        await backend.publish_notification("task-123")

        mock_client.publish.assert_awaited_once_with("sq:notify:task-123", "done")


class TestValkeyBackendGetResultsBatch:
    async def test_get_results_batch_calls_mget_with_correct_keys(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        backend._client = mock_client
        mock_client.mget.return_value = [b"r1", b"r2", b"r3"]

        result = await backend.get_results_batch(["a", "b", "c"])

        mock_client.mget.assert_awaited_once_with(
            "sq:result:a", "sq:result:b", "sq:result:c"
        )
        assert result == [b"r1", b"r2", b"r3"]

    async def test_get_results_batch_empty_list_returns_empty(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        backend._client = mock_client

        result = await backend.get_results_batch([])

        mock_client.mget.assert_not_awaited()
        assert result == []


class TestValkeyBackendPublishNotificationError:
    async def test_publish_notification_swallows_valkey_error(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        backend._client = mock_client
        mock_client.publish.side_effect = ValkeyError("publish failed")

        # Should NOT raise -- fire-and-forget
        await backend.publish_notification("task-123")


class TestValkeyBackendGetResultsBatchError:
    async def test_get_results_batch_wraps_valkey_error(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        backend._client = mock_client
        mock_client.mget.side_effect = ValkeyError("mget failed")

        with pytest.raises(BackendConnectionError):
            await backend.get_results_batch(["a", "b"])


class TestValkeyBackendGetResultsBatchPartial:
    async def test_get_results_batch_returns_none_for_missing_keys(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        backend._client = mock_client
        mock_client.mget.return_value = [b"data-a", None, b"data-c"]

        result = await backend.get_results_batch(["a", "missing", "c"])

        mock_client.mget.assert_awaited_once_with(
            "sq:result:a", "sq:result:missing", "sq:result:c"
        )
        assert result == [b"data-a", None, b"data-c"]


class TestValkeyBackendCreatePubsub:
    def test_create_pubsub_returns_pubsub_from_client(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        mock_pubsub = AsyncMock()
        mock_client.pubsub = Mock(return_value=mock_pubsub)
        backend._client = mock_client

        result = backend.create_pubsub()

        mock_client.pubsub.assert_called_once()
        assert result is mock_pubsub


class TestValkeyBackendSubscribeAndWaitMessage:
    async def test_subscribe_and_wait_returns_true_on_message(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        mock_pubsub = AsyncMock()
        mock_client.pubsub = Mock(return_value=mock_pubsub)
        mock_pubsub.get_message.return_value = {"type": "message", "data": b"done"}
        backend._client = mock_client

        result = await backend.subscribe_and_wait("sq:notify:task-1", timeout=5.0)

        assert result is True
        mock_pubsub.subscribe.assert_awaited_once_with("sq:notify:task-1")
        mock_pubsub.get_message.assert_awaited_once_with(
            ignore_subscribe_messages=True, timeout=5.0
        )


class TestValkeyBackendSubscribeAndWaitTimeout:
    async def test_subscribe_and_wait_returns_false_on_timeout(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        mock_pubsub = AsyncMock()
        mock_client.pubsub = Mock(return_value=mock_pubsub)
        mock_pubsub.get_message.return_value = None
        backend._client = mock_client

        result = await backend.subscribe_and_wait("sq:notify:task-1", timeout=5.0)

        assert result is False


class TestValkeyBackendSubscribeAndWaitValkeyError:
    async def test_subscribe_and_wait_returns_false_on_valkey_error(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        mock_pubsub = AsyncMock()
        mock_client.pubsub = Mock(return_value=mock_pubsub)
        mock_pubsub.subscribe.side_effect = ValkeyError("connection lost")
        backend._client = mock_client

        result = await backend.subscribe_and_wait("sq:notify:task-1", timeout=5.0)

        assert result is False


class TestValkeyBackendSubscribeAndWaitCleanup:
    async def test_subscribe_and_wait_always_calls_unsubscribe_and_aclose(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        mock_pubsub = AsyncMock()
        mock_client.pubsub = Mock(return_value=mock_pubsub)
        mock_pubsub.get_message.side_effect = ValkeyError("read error")
        backend._client = mock_client

        await backend.subscribe_and_wait("sq:notify:task-1", timeout=5.0)

        mock_pubsub.unsubscribe.assert_awaited_once_with("sq:notify:task-1")
        mock_pubsub.aclose.assert_awaited_once()


class TestValkeyBackendSubscribeAndWaitCancelledError:
    async def test_subscribe_and_wait_propagates_cancelled_error_after_cleanup(self):
        backend = ValkeyBackend("valkey://localhost:6379/0")
        mock_client = AsyncMock()
        mock_pubsub = AsyncMock()
        mock_client.pubsub = Mock(return_value=mock_pubsub)
        mock_pubsub.get_message.side_effect = asyncio.CancelledError()
        backend._client = mock_client

        with pytest.raises(asyncio.CancelledError):
            await backend.subscribe_and_wait("sq:notify:task-1", timeout=5.0)

        mock_pubsub.unsubscribe.assert_awaited_once_with("sq:notify:task-1")
        mock_pubsub.aclose.assert_awaited_once()
