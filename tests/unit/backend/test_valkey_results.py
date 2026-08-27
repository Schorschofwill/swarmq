"""``ValkeyResults``: „auf Ergebnis warten" und „Fortschritt beobachten" (Plan U17).

Die Pub/Sub-Wartelogik wanderte aus ``Client.get_result``/``watch_progress``
hierher; diese Tests sind die Portierung der früheren Client-Tests
(``tests/unit/test_client.py`` #14–#21 und ``TestClientProgress``) gegen einen
gemockten valkey-Client. Verhalten unverändert (R9).
"""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, Mock, patch

import pytest

from swarmq.backend.valkey import ValkeyBackend
from swarmq.exceptions import BackendConnectionError


def _backend(get_side_effect: object) -> tuple[ValkeyBackend, AsyncMock, AsyncMock]:
    client = AsyncMock()
    client.get = AsyncMock(side_effect=get_side_effect) if isinstance(get_side_effect, list) else AsyncMock(
        return_value=get_side_effect
    )
    pubsub = AsyncMock()
    pubsub.get_message = AsyncMock(return_value=None)  # no notification by default
    client.pubsub = Mock(return_value=pubsub)
    backend = ValkeyBackend(client=client)
    return backend, client, pubsub


class TestWaitForResult:
    async def test_notification_fetches_result(self) -> None:
        backend, client, pubsub = _backend([None, b"pubsub-ok"])
        pubsub.get_message = AsyncMock(return_value={"type": "message", "data": b"done"})

        assert await backend.wait_for_result("task-pub", timeout=5) == b"pubsub-ok"
        client.pubsub.assert_called_once()
        pubsub.subscribe.assert_awaited_once_with("sq:notify:task-pub")
        assert client.get.await_count == 2  # Erst-GET, dann nach Benachrichtigung

    async def test_initial_check_finds_result_before_waiting(self) -> None:
        backend, client, _ = _backend(b"fast")
        assert await backend.wait_for_result("task-fast", timeout=5) == b"fast"
        client.get.assert_awaited_once()

    async def test_pubsub_miss_falls_back_to_polling(self) -> None:
        backend, client, _ = _backend([None] * 50 + [b"polled"])
        assert await backend.wait_for_result("task-poll", timeout=5) == b"polled"
        assert client.get.await_count >= 2

    async def test_pubsub_subscribe_failure_falls_back_to_polling(self) -> None:
        backend, _, pubsub = _backend([None, b"recovered"])
        pubsub.subscribe = AsyncMock(side_effect=Exception("pubsub unavailable"))
        assert await backend.wait_for_result("task-recover", timeout=5) == b"recovered"

    async def test_returns_none_when_timeout_exhausted(self) -> None:
        backend, _, _ = _backend(None)
        assert await backend.wait_for_result("task-timeout", timeout=0.05) is None

    async def test_cancelled_error_propagates(self) -> None:
        backend, _, pubsub = _backend(None)
        pubsub.get_message = AsyncMock(side_effect=asyncio.CancelledError)
        with pytest.raises(asyncio.CancelledError):
            await backend.wait_for_result("task-cancel", timeout=5)

    async def test_timeout_honored_when_backend_hangs(self) -> None:
        """Härtung R2 P1-2: jeder GET ist an die Rest-Deadline gebunden — ein
        hängender GET (half-open Valkey) darf den Aufrufer nicht über
        ``timeout`` hinaus blockieren."""

        async def _hang(*args: object, **kwargs: object) -> None:
            await asyncio.sleep(60)

        backend, _, _ = _backend(None)
        backend._client.get = AsyncMock(side_effect=_hang)  # type: ignore[union-attr]

        started = asyncio.get_running_loop().time()
        result = await asyncio.wait_for(backend.wait_for_result("task-id", timeout=0.2), timeout=5)
        assert result is None
        assert asyncio.get_running_loop().time() - started < 4

    async def test_cleans_up_pubsub_in_finally(self) -> None:
        backend, _, pubsub = _backend(b"x")
        await backend.wait_for_result("t", timeout=1)
        pubsub.unsubscribe.assert_awaited_once_with("sq:notify:t")
        pubsub.aclose.assert_awaited_once()


def _hash(current: int, total: int) -> dict[bytes, bytes]:
    return {
        b"current": str(current).encode(),
        b"total": str(total).encode(),
        b"percent": f"{current / total * 100:.2f}".encode(),
        b"message": b"x",
        b"updated_at": b"2026-04-17T10:30Z",
        b"started_at": b"2026-04-17T10:29Z",
        b"eta_seconds": b"7",
    }


def _payload(current: int, total: int) -> bytes:
    return json.dumps(
        {
            "current": str(current),
            "total": str(total),
            "percent": f"{current / total * 100:.2f}",
            "message": "live",
            "updated_at": "2026-04-17T10:31Z",
            "started_at": "2026-04-17T10:29Z",
            "eta_seconds": "3",
        }
    ).encode()


def _current(payload: bytes) -> int:
    return int(json.loads(payload)["current"])


async def _collect(backend: ValkeyBackend, *, until_current: int) -> list[bytes]:
    observed: list[bytes] = []
    gen = backend.watch_progress("task-x")
    try:
        async for payload in gen:
            observed.append(payload)
            if _current(payload) >= until_current:
                break
    finally:
        await gen.aclose()
    return observed


@pytest.mark.timeout(5)
class TestWatchProgress:
    def _progress_backend(self, hgetall: object) -> tuple[ValkeyBackend, AsyncMock]:
        backend, client, pubsub = _backend(None)
        client.hgetall = (
            AsyncMock(side_effect=hgetall) if isinstance(hgetall, list) else AsyncMock(return_value=hgetall)
        )
        return backend, pubsub

    async def test_yields_initial_snapshot_then_streams_updates(self) -> None:
        backend, pubsub = self._progress_backend(_hash(3, 10))
        pubsub.get_message = AsyncMock(
            side_effect=[
                None,  # drain-confirmation (after subscribe)
                {"type": "message", "data": _payload(5, 10)},
                {"type": "message", "data": _payload(10, 10)},
            ]
        )
        observed = await _collect(backend, until_current=10)
        assert [_current(p) for p in observed] == [3, 5, 10]
        pubsub.subscribe.assert_awaited_once_with("sq:progress:channel:task-x")
        pubsub.unsubscribe.assert_awaited_once()
        pubsub.aclose.assert_awaited_once()

    async def test_fallback_to_hash_on_pubsub_error_and_resubscribe(self) -> None:
        """Spike 2026-04-17: valkey-py resubscribed nach Socket-Reset nicht —
        der Iterator liefert den HASH-Schnappschuss und abonniert aktiv neu."""
        backend, pubsub = self._progress_backend([{}, _hash(7, 10), _hash(10, 10)])
        pubsub.get_message = AsyncMock(
            side_effect=[
                None,  # initial drain
                ConnectionError("pubsub flap"),
                None,  # resubscribe drain
                {"type": "message", "data": _payload(10, 10)},
            ]
        )
        with patch("swarmq.backend.valkey.results.asyncio.sleep", new=AsyncMock()):
            observed = await _collect(backend, until_current=10)
        currents = [_current(p) for p in observed]
        assert 7 in currents and currents[-1] == 10
        assert pubsub.subscribe.await_count == 2

    async def test_resubscribe_also_failing_keeps_trying(self) -> None:
        backend, pubsub = self._progress_backend([{}, _hash(3, 10), _hash(10, 10)])
        pubsub.subscribe = AsyncMock(side_effect=[None, ConnectionError("still down"), None])
        pubsub.get_message = AsyncMock(
            side_effect=[
                None,
                ConnectionError("flap 1"),
                ConnectionError("flap 2"),
                None,
                {"type": "message", "data": _payload(10, 10)},
            ]
        )
        with patch("swarmq.backend.valkey.results.asyncio.sleep", new=AsyncMock()):
            observed = await _collect(backend, until_current=10)
        currents = [_current(p) for p in observed]
        assert 3 in currents and 10 in currents

    async def test_finally_cleans_up_on_break(self) -> None:
        backend, pubsub = self._progress_backend(_hash(1, 10))
        pubsub.get_message = AsyncMock(return_value=None)
        observed = await _collect(backend, until_current=1)
        assert [_current(p) for p in observed] == [1]
        pubsub.unsubscribe.assert_awaited_once()
        pubsub.aclose.assert_awaited_once()

    async def test_setup_failure_yields_hash_and_exits(self) -> None:
        backend, pubsub = self._progress_backend(_hash(4, 10))
        pubsub.subscribe = AsyncMock(side_effect=ConnectionError("down"))
        observed = [p async for p in backend.watch_progress("task-x")]
        assert [_current(p) for p in observed] == [4]
        pubsub.aclose.assert_awaited_once()

    async def test_non_bytes_payloads_are_dropped(self) -> None:
        backend, pubsub = self._progress_backend({})
        pubsub.get_message = AsyncMock(
            side_effect=[
                None,
                {"type": "subscribe", "data": 1},
                {"type": "message", "data": "not-bytes"},
                {"type": "message", "data": _payload(10, 10)},
            ]
        )
        observed = await _collect(backend, until_current=10)
        assert [_current(p) for p in observed] == [10]


class TestPingAndTimeouts:
    async def test_ping_before_connect_raises(self) -> None:
        with pytest.raises(BackendConnectionError):
            await ValkeyBackend("valkey://localhost:6379/0").ping()

    async def test_op_timeout_is_wrapped(self) -> None:
        async def _slow(*args: object, **kwargs: object) -> None:
            await asyncio.sleep(60)

        backend, client, _ = _backend(None)
        client.set = AsyncMock(side_effect=_slow)
        with pytest.raises(BackendConnectionError, match="store_result timed out after 0.01s"):
            await backend.store_result("t", b"x", 1000, op_timeout=0.01)
