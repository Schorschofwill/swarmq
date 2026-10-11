"""``ValkeyResults``: „auf Ergebnis warten" und „Fortschritt beobachten" (Plan U17).

Die Pub/Sub-Wartelogik wanderte aus ``Client.get_result``/``watch_progress``
hierher; diese Tests sind die Portierung der früheren Client-Tests
(``tests/unit/test_client.py`` #14–#21 und ``TestClientProgress``) gegen einen
gemockten valkey-Client. Verhalten unverändert (R9).

Test-Liste 0.9.0 (Ausfall vs. Fehlerantwort): eine Fehlerantwort von Valkey
(``ResponseError``) wird ``BackendResponseError``, ein Transportfehler bleibt
einfacher ``BackendConnectionError`` (Cancel-Gate: nur Ausfaelle werden
zurueckgestellt).
"""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, Mock, patch

import pytest
from valkey.exceptions import ConnectionError as ValkeyConnectionError
from valkey.exceptions import ValkeyError

from swarmq.backend.valkey import ValkeyBackend
from swarmq.exceptions import BackendConnectionError, BackendResponseError


def _backend(get_side_effect: object) -> tuple[ValkeyBackend, AsyncMock, AsyncMock]:
    client = AsyncMock()
    client.get = (
        AsyncMock(side_effect=get_side_effect)
        if isinstance(get_side_effect, list)
        else AsyncMock(return_value=get_side_effect)
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
        assert 7 in currents
        assert currents[-1] == 10
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
        assert 3 in currents
        assert 10 in currents

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


class TestCompareAndStoreWorkflowState:
    """KTD11 (Plan 2026-09-08-001, U9): ``workflow_state_cas.lua`` lazy
    registriert, Aufruf mit Schluessel und (Version, Payload, TTL)."""

    def _backend(self, results: list[int]) -> tuple[ValkeyBackend, Mock, AsyncMock]:
        client = AsyncMock()
        script = AsyncMock(side_effect=results)
        client.register_script = Mock(return_value=script)
        return ValkeyBackend(client=client), client, script

    async def test_calls_script_with_key_version_payload_ttl(self) -> None:
        backend, _client, script = self._backend([1])
        stored = await backend.compare_and_store_workflow_state(
            "wf-1", b'{"version": 3}', ttl_ms=5_000, expected_version=2
        )
        assert stored is True
        script.assert_awaited_once_with(keys=["sq:wf:wf-1"], args=[2, b'{"version": 3}', 5_000])

    async def test_zero_means_conflict_and_script_is_registered_once(self) -> None:
        backend, client, _script = self._backend([1, 0])
        assert await backend.compare_and_store_workflow_state("wf-1", b"{}", ttl_ms=5_000, expected_version=0) is True
        assert await backend.compare_and_store_workflow_state("wf-1", b"{}", ttl_ms=5_000, expected_version=0) is False
        client.register_script.assert_called_once()

    async def test_before_connect_raises_backend_error(self) -> None:
        backend = ValkeyBackend("valkey://127.0.0.1:1/0")
        with pytest.raises(BackendConnectionError):
            await backend.compare_and_store_workflow_state("wf-1", b"{}", ttl_ms=5_000, expected_version=0)


# ---------------------------------------------------------------------------
# Logging (Plan 2026-10-06-0831, U9: R2, R17, R18)
# ---------------------------------------------------------------------------
# Test list:
# 1. Scheitert ``notify_result``, loggt das ``publish_notification_failed``
#    (vorher ``"publish_notification failed"``) auf WARNING ueber den Logger
#    ``swarmq.backend.valkey.results`` mit ``task_id``, ``error_type``, ``error``.
# 2. Scheitert ``notify_workflow``, loggt das
#    ``publish_workflow_notification_failed`` mit ``workflow_id`` (vorher
#    ``wf_id``) und Fehlerfeldern.
# 3. Scheitert ``publish_progress``, loggt ``publish_progress_failed`` die
#    Fehlerfelder.
# 4. Scheitert ``clear_cancel_flag``, loggt ``cancel_flag_delete_failed`` die
#    Fehlerfelder.
# 5. Scheitert ``release_unique``, loggt ``unique_release_backend_error`` die
#    Fehlerfelder.


def _results_entry(entries: list[dict], event: str) -> dict:
    [entry] = [e for e in entries if e["event"] == event and e["logger"] == "swarmq.backend.valkey.results"]
    return entry


class TestResultsLogging:
    @pytest.mark.timeout(5)
    async def test_failed_notify_result_logs_publish_notification_failed(self, swarmq_logs) -> None:
        backend, client, _ = _backend(None)
        client.publish = AsyncMock(side_effect=ValkeyError("publish down"))

        await backend.notify_result("task-1")

        entry = _results_entry(swarmq_logs, "publish_notification_failed")
        assert entry["log_level"] == "warning"
        assert entry["task_id"] == "task-1"
        assert entry["error_type"] == "ValkeyError"
        assert entry["error"] == "publish down"

    @pytest.mark.timeout(5)
    async def test_failed_notify_workflow_logs_publish_workflow_notification_failed(self, swarmq_logs) -> None:
        backend, client, _ = _backend(None)
        client.publish = AsyncMock(side_effect=OSError("socket closed"))

        await backend.notify_workflow("wf-1")

        entry = _results_entry(swarmq_logs, "publish_workflow_notification_failed")
        assert entry["log_level"] == "warning"
        assert entry["workflow_id"] == "wf-1"
        assert "wf_id" not in entry
        assert entry["error_type"] == "OSError"
        assert entry["error"] == "socket closed"

    @pytest.mark.timeout(5)
    async def test_failed_publish_progress_logs_error_fields(self, swarmq_logs) -> None:
        backend, client, _ = _backend(None)
        client.publish = AsyncMock(side_effect=ValkeyError("publish down"))

        await backend.publish_progress("task-1", b"{}")

        entry = _results_entry(swarmq_logs, "publish_progress_failed")
        assert entry["log_level"] == "warning"
        assert entry["task_id"] == "task-1"
        assert entry["error_type"] == "ValkeyError"
        assert entry["error"] == "publish down"

    @pytest.mark.timeout(5)
    async def test_failed_clear_cancel_flag_logs_error_fields(self, swarmq_logs) -> None:
        backend, client, _ = _backend(None)
        client.pexpire = AsyncMock(side_effect=ValkeyConnectionError("delete down"))

        await backend.clear_cancel_flag("task-1")

        entry = _results_entry(swarmq_logs, "cancel_flag_delete_failed")
        assert entry["log_level"] == "warning"
        assert entry["task_id"] == "task-1"
        assert entry["error_type"] == "BackendConnectionError"
        assert "delete down" in entry["error"]

    @pytest.mark.timeout(5)
    async def test_failed_release_unique_logs_error_fields(self, swarmq_logs) -> None:
        backend, client, _ = _backend(None)
        client.register_script = Mock(return_value=AsyncMock(side_effect=ValkeyConnectionError("script down")))

        assert await backend.release_unique("hash-1", "task-1") is False

        entry = _results_entry(swarmq_logs, "unique_release_backend_error")
        assert entry["log_level"] == "warning"
        assert entry["dedup_hash"] == "hash-1"
        assert entry["task_id"] == "task-1"
        assert entry["error_type"] == "BackendConnectionError"
        assert "script down" in entry["error"]


class TestOutageVersusErrorReply:
    async def test_error_reply_is_a_response_error(self) -> None:
        from valkey.exceptions import ResponseError

        backend, client, _ = _backend(None)
        client.get = AsyncMock(side_effect=ResponseError("WRONGTYPE"))
        with pytest.raises(BackendResponseError):
            await backend.get_result("t")

    async def test_transport_error_is_a_plain_outage(self) -> None:
        backend, client, _ = _backend(None)
        client.exists = AsyncMock(side_effect=ValkeyConnectionError("refused"))
        with pytest.raises(BackendConnectionError) as exc_info:
            await backend.is_cancelled("t", op_timeout=1)
        assert not isinstance(exc_info.value, BackendResponseError)
