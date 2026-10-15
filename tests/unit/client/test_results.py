"""Client-Ergebnis-Tests (Plan U10) — verbatim: get_result(s), run,
cancel, Fortschritt.

Test-Liste P9 (Plan 2026-10-12-1105, MR 3, Q2):

- Fehlerergebnis mit ``error_class`` -> ``TaskError.error_class`` gesetzt,
  Text unveraendert; ohne Schluessel (0.12.0rc1-/0.11.1-Worker) -> ``None``.

Test-Liste F3-09 (Plan 2026-10-15-1138, U4; Nachweise in
``test_befund_f3_09_get_results_timeout_zero.py``):

- [x] ``get_results(timeout=0)``, Ergebnis fehlt -> ein MGET, dann
  ``ResultNotReadyError`` mit den fehlenden IDs.
- [x] ``timeout=0.05``, ``poll_interval=1.0`` -> Schlaf ``0.05``, nicht ``1.0``.
- [x] Teilergebnis ``[x, None]`` -> Meldung nennt nur die fehlende ID.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any, override
from unittest.mock import AsyncMock, Mock

import pytest

from swarmq.client import Client
from swarmq.exceptions import (
    ResultNotReadyError,
    TaskCancelledError,
    TaskError,
)
from swarmq.serialization import encode_result

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _no_pending_oneshot(backend):
    """BZ-23 (Plan 0.12.0, MR 5): ``cancel()`` liest den Oneshot-Eintrag der
    task_id; der Mock-Backend kennt keinen."""
    backend.schedules.get = AsyncMock(return_value=None)


@pytest.mark.asyncio
async def test_get_result_returns_deserialized_value(client, backend):
    result_data = encode_result({"result": 42, "is_error": False})
    backend.get_result = AsyncMock(return_value=result_data)

    value = await client.get_result("some-task-id", timeout=1)
    assert value == 42


@pytest.mark.asyncio
async def test_run_combines_schedule_and_get_result(client, broker, backend):
    result_data = encode_result({"result": 3, "is_error": False})
    backend.get_result = AsyncMock(return_value=result_data)

    value = await client.run("add", 1, 2, queue="math", timeout=1)
    assert value == 3
    broker.publish.assert_awaited_once()
    backend.get_result.assert_awaited()


@pytest.mark.asyncio
async def test_get_result_timeout_raises(client, backend):
    backend.get_result = AsyncMock(return_value=None)

    with pytest.raises(ResultNotReadyError):
        await client.get_result("missing-task", timeout=0.1, polling=0.05)


@pytest.mark.asyncio
async def test_get_result_error_raises_task_error(client, backend):
    result_data = encode_result(
        {
            "result": None,
            "is_error": True,
            "error_message": "division by zero",
        }
    )
    backend.get_result = AsyncMock(return_value=result_data)

    with pytest.raises(TaskError, match="division by zero"):
        await client.get_result("err-task", timeout=1)


@pytest.mark.asyncio
@pytest.mark.timeout(5)
async def test_get_result_error_exposes_error_class_when_present(client, backend):
    base = {"task": {"id": "e", "queue": "q", "name": "n"}, "result": None, "is_error": True, "is_cancelled": False}
    backend.get_result = AsyncMock(
        side_effect=[
            encode_result({**base, "error_message": "", "error_class": "builtins.ValueError"}),
            encode_result({**base, "error_message": "boom"}),
        ]
    )

    with pytest.raises(TaskError) as with_class:
        await client.get_result("e", timeout=1)
    with pytest.raises(TaskError) as without_class:
        await client.get_result("e", timeout=1)

    assert (str(with_class.value), with_class.value.error_class) == ("", "builtins.ValueError")
    assert (str(without_class.value), without_class.value.error_class) == ("boom", None)


@pytest.mark.asyncio
@pytest.mark.entscheidung("E-068")
async def test_get_result_cancelled_raises_task_cancelled_error(client, backend):
    result_data = encode_result(
        {
            "task": {"id": "cancel-task", "queue": "default", "name": "t"},
            "result": None,
            "is_error": False,
            "is_cancelled": True,
            "error_message": "",
        }
    )
    backend.get_result = AsyncMock(return_value=result_data)

    with pytest.raises(TaskCancelledError) as excinfo:
        await client.get_result("cancel-task", timeout=1)

    assert "cancel-task" in str(excinfo.value)
    assert isinstance(excinfo.value, TaskError)


@pytest.mark.asyncio
async def test_get_result_cancelled_takes_priority_over_error(client, backend):
    """When both is_cancelled=True and is_error=True, TaskCancelledError wins."""
    result_data = encode_result(
        {
            "task": {"id": "dual", "queue": "default", "name": "t"},
            "result": None,
            "is_error": True,
            "is_cancelled": True,
            "error_message": "some error",
        }
    )
    backend.get_result = AsyncMock(return_value=result_data)

    with pytest.raises(TaskCancelledError):
        await client.get_result("dual", timeout=1)


@pytest.mark.asyncio
async def test_get_result_legacy_result_without_is_cancelled_key(client, backend):
    """Backward-compat: results stored before R3.6 have no is_cancelled key."""
    result_data = encode_result(
        {
            "task": {"id": "legacy", "queue": "default", "name": "t"},
            "result": 99,
            "is_error": False,
            # no is_cancelled key
        }
    )
    backend.get_result = AsyncMock(return_value=result_data)

    result = await client.get_result("legacy", timeout=1)
    assert result == 99


@pytest.mark.asyncio
async def test_get_result_polls_multiple_times(client, backend):
    result_data = encode_result({"result": "ok", "is_error": False})
    backend.get_result = AsyncMock(
        side_effect=[None, None, result_data],
    )

    # Großzügiges Timeout: der Test prüft das Polling (3 GETs), nicht den
    # Ablauf — mit timeout=2 lief die Deadline unter CI-Last gelegentlich
    # vor dem dritten Poll ab (Flake).
    value = await client.get_result("task-id", timeout=30, polling=0.05)
    assert value == "ok"
    assert backend.get_result.await_count == 3


@pytest.mark.asyncio
async def test_get_result_timeout_zero_checks_once(client, backend):
    backend.get_result = AsyncMock(return_value=None)

    with pytest.raises(ResultNotReadyError):
        await client.get_result("task-id", timeout=0)

    backend.get_result.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_result_timeout_zero_returns_if_found(client, backend):
    result_data = encode_result({"result": 99, "is_error": False})
    backend.get_result = AsyncMock(return_value=result_data)

    value = await client.get_result("task-id", timeout=0)
    assert value == 99
    backend.get_result.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_result_initial_check_finds_result(client, backend):
    """Result already in backend -> returned after one read."""
    result_data = encode_result({"result": "fast", "is_error": False})
    backend.get_result = AsyncMock(return_value=result_data)

    value = await client.get_result("task-fast", timeout=5)
    assert value == "fast"
    backend.get_result.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_result_timeout_zero_no_pubsub(client, backend):
    """timeout=0 does a single GET without entering the wait path."""
    backend.get_result = AsyncMock(return_value=None)

    with pytest.raises(ResultNotReadyError):
        await client.get_result("task-id", timeout=0)

    backend.get_result.assert_awaited_once()
    backend.wait_for_result.assert_not_called()


@pytest.mark.asyncio
async def test_get_result_overall_timeout_raises(client, backend):
    """Full timeout exhausted -> ResultNotReadyError."""
    backend.get_result = AsyncMock(return_value=None)

    with pytest.raises(ResultNotReadyError):
        await client.get_result("task-timeout", timeout=0.05)


@pytest.mark.asyncio
async def test_get_results_returns_all_in_order(client, backend):
    data_a = encode_result({"result": "alpha", "is_error": False})
    data_b = encode_result({"result": "beta", "is_error": False})
    data_c = encode_result({"result": "gamma", "is_error": False})

    # First poll: none ready; second poll: all ready
    backend.get_results = AsyncMock(
        side_effect=[
            [None, None, None],
            [data_a, data_b, data_c],
        ]
    )

    results = await client.get_results(["a", "b", "c"], timeout=2, poll_interval=0.01)
    assert results == ["alpha", "beta", "gamma"]


@pytest.mark.asyncio
async def test_get_results_timeout_raises(client, backend):
    backend.get_results = AsyncMock(return_value=[None, None])

    with pytest.raises(ResultNotReadyError):
        await client.get_results(["x", "y"], timeout=0.05, poll_interval=0.01)


class _FakeClock:
    """Gesteuerte Uhr fuer ``results.asyncio``: ``sleep`` stellt die Zeit
    vor und protokolliert die Schlafdauer (Uhr ist eine Grenze, F3-09)."""

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def time(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> _FakeClock:
    from swarmq.client import results as results_mod

    fake = _FakeClock()
    namespace: Any = SimpleNamespace(get_running_loop=lambda: fake, sleep=fake.sleep)
    monkeypatch.setattr(results_mod, "asyncio", namespace)
    return fake


@pytest.mark.asyncio
@pytest.mark.timeout(5)
@pytest.mark.usefixtures("clock")
async def test_get_results_with_zero_timeout_and_missing_result_reads_once_then_raises(client, backend):
    backend.get_results = AsyncMock(return_value=[None, None])

    with pytest.raises(ResultNotReadyError, match=r"\['x', 'y'\]"):
        await client.get_results(["x", "y"], timeout=0)

    backend.get_results.assert_awaited_once_with(["x", "y"])


@pytest.mark.asyncio
@pytest.mark.timeout(5)
async def test_get_results_wait_does_not_overshoot_the_deadline(client, backend, clock):
    ready = encode_result({"result": "alpha", "is_error": False})
    backend.get_results = AsyncMock(side_effect=[[None], [ready]])

    results = await client.get_results(["a"], timeout=0.05, poll_interval=1.0)

    assert results == ["alpha"]
    assert clock.sleeps == [0.05]


@pytest.mark.asyncio
@pytest.mark.timeout(5)
@pytest.mark.usefixtures("clock")
async def test_get_results_partial_result_names_only_the_missing_id(client, backend):
    ready = encode_result({"result": "alpha", "is_error": False})
    backend.get_results = AsyncMock(return_value=[ready, None])

    with pytest.raises(ResultNotReadyError) as exc_info:
        await client.get_results(["done", "pending"], timeout=0)

    assert "'pending'" in str(exc_info.value)
    assert "'done'" not in str(exc_info.value)


@pytest.mark.asyncio
async def test_cancel_calls_backend_set_cancel_flag_and_returns_bool(
    client,
    backend,
):
    backend.set_cancel_flag = AsyncMock(return_value=True)

    result = await client.cancel("task-abc", ttl_ms=60_000)

    backend.set_cancel_flag.assert_awaited_once_with("task-abc", 60_000)
    assert result is True


@pytest.mark.asyncio
async def test_cancel_returns_false_when_already_cancelled(client, backend):
    backend.set_cancel_flag = AsyncMock(return_value=False)

    result = await client.cancel("task-abc")

    assert result is False


@pytest.mark.asyncio
async def test_cancel_default_ttl_is_24h(client, backend):
    backend.set_cancel_flag = AsyncMock(return_value=True)

    await client.cancel("task-abc")

    backend.set_cancel_flag.assert_awaited_once_with("task-abc", 86_400_000)


@pytest.mark.asyncio
async def test_cancel_rejects_empty_task_id(client):
    with pytest.raises(ValueError, match="task_id must be a non-empty string"):
        await client.cancel("")


@pytest.mark.asyncio
async def test_cancel_propagates_backend_errors(client, backend):
    from swarmq.exceptions import BackendConnectionError

    backend.set_cancel_flag = AsyncMock(side_effect=BackendConnectionError("valkey down"))

    with pytest.raises(BackendConnectionError):
        await client.cancel("task-abc")


@pytest.mark.timeout(5)
class TestClientProgress:
    @pytest.fixture
    def progress_backend(self, backend):
        backend.get_progress = AsyncMock(return_value=None)
        backend.progress_channel_name = Mock(
            side_effect=lambda tid: f"sq:progress:channel:{tid}",
        )
        return backend

    @staticmethod
    def _valid_hash(current: int = 3, total: int = 10) -> dict[bytes, bytes]:
        return {
            b"current": str(current).encode(),
            b"total": str(total).encode(),
            b"percent": f"{current / total * 100:.2f}".encode(),
            b"message": b"x",
            b"updated_at": b"2026-04-17T10:30Z",
            b"started_at": b"2026-04-17T10:29Z",
            b"eta_seconds": b"7",
        }

    @staticmethod
    def _valid_payload(current: int = 5, total: int = 10) -> bytes:
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
        ).encode("utf-8")

    async def test_get_progress_returns_none_on_empty_hash(
        self,
        progress_backend,
        broker,
        registry,
    ):
        progress_backend.get_progress = AsyncMock(return_value=None)
        client = Client(
            broker=broker,
            backend=progress_backend,
            registry=registry,
        )
        assert await client.get_progress("task-x") is None

    async def test_get_progress_decodes_populated_hash(
        self,
        progress_backend,
        broker,
        registry,
    ):
        progress_backend.get_progress = AsyncMock(
            return_value={
                b"current": b"3",
                b"total": b"10",
                b"percent": b"30.00",
                b"message": b"three",
                b"updated_at": b"2026-04-17T10:30Z",
                b"started_at": b"2026-04-17T10:29Z",
                b"eta_seconds": b"7",
            }
        )
        client = Client(
            broker=broker,
            backend=progress_backend,
            registry=registry,
        )
        info = await client.get_progress("task-x")
        assert info is not None
        assert info.current == 3
        assert info.total == 10
        assert info.percent == 30.00
        assert info.message == "three"
        assert info.eta_seconds == 7

    async def test_get_progress_returns_none_on_malformed_hash(
        self,
        progress_backend,
        broker,
        registry,
    ):
        # Missing required fields — from_hash would raise ValueError,
        # but the client layer treats it as "nothing observable".
        progress_backend.get_progress = AsyncMock(
            return_value={
                b"message": b"partial",
            }
        )
        client = Client(
            broker=broker,
            backend=progress_backend,
            registry=registry,
        )
        assert await client.get_progress("task-x") is None

    @staticmethod
    def _streaming_backend(backend, payloads: list[bytes]):
        """U17: das Backend liefert die Payloads (JSON der HASH-Felder);
        Pub/Sub, Schnappschuss und Resubscribe sind dort getestet
        (``tests/unit/backend/test_valkey_results.py``)."""

        async def _watch(task_id: str):
            for payload in payloads:
                yield payload

        backend.watch_progress = Mock(side_effect=_watch)
        return backend

    async def test_watch_progress_decodes_backend_payloads(
        self,
        progress_backend,
        broker,
        registry,
    ):
        self._streaming_backend(
            progress_backend,
            [self._valid_payload(3, 10), self._valid_payload(10, 10)],
        )
        client = Client(broker=broker, backend=progress_backend, registry=registry)
        observed = [info async for info in client.watch_progress("task-x")]
        assert [i.current for i in observed] == [3, 10]
        progress_backend.watch_progress.assert_called_once_with("task-x")

    async def test_watch_progress_drops_malformed_payloads(
        self,
        progress_backend,
        broker,
        registry,
    ):
        # A malformed publish must not crash the iterator — drop and
        # continue until the next valid message arrives.
        bad_payload = json.dumps({"current": "oops", "total": "1"}).encode()
        self._streaming_backend(
            progress_backend,
            [b"not-json-at-all", bad_payload, self._valid_payload(10, 10)],
        )
        client = Client(broker=broker, backend=progress_backend, registry=registry)
        observed = [info async for info in client.watch_progress("task-x")]
        assert [i.current for i in observed] == [10]


# ---------------------------------------------------------------------------
# BZ-23 (Plan 0.12.0, MR 5): Cancel-Flag überdauert Wartezeit und max_age
# Test-Liste:
# - [ ] verzögerter Task: Flag-TTL = Basis + (fire_ts - now)
# - [ ] bekanntes max_age des verzögerten Tasks: + max_age + 300 s
# - [ ] unlesbarer Oneshot-Eintrag: Basis-TTL
# - [ ] Verbindungsfehler beim Lesen des Eintrags propagiert
# ---------------------------------------------------------------------------


async def _memory_backend():
    from tests.support.inmemory import InMemoryBackend
    from tests.support.inmemory.clock import FakeClock

    memory = InMemoryBackend(FakeClock())
    await memory.connect()
    memory.set_cancel_flag = AsyncMock(wraps=memory.set_cancel_flag)  # type: ignore[method-assign]  # Spion um echte Methode
    return memory


@pytest.mark.entscheidung("E-020")
@pytest.mark.timeout(5)
async def test_cancel_of_deferred_task_outlives_its_fire_time():
    memory = await _memory_backend()
    client = Client(broker=AsyncMock(), backend=memory)
    tid = await client.schedule("ping", queue="default", delay=48 * 3600)

    await client.cancel(tid, ttl_ms=60_000)

    ((_, ttl_ms), _) = memory.set_cancel_flag.await_args
    # Basis 60 s plus rund 48 h bis zum Fire (Toleranz für die Laufzeit).
    assert 48 * 3_600_000 + 50_000 <= ttl_ms <= 48 * 3_600_000 + 60_000


@pytest.mark.entscheidung("E-020")
@pytest.mark.timeout(5)
async def test_cancel_of_deferred_task_adds_known_max_age_plus_buffer():
    from swarmq.registry import TaskRegistry
    from swarmq.task import Task, TaskInfo

    class Expiring(Task):
        task_info = TaskInfo(name="bz23_expiring", queues=["default"], max_age=600)

        @override
        async def run(self) -> None:
            return None

    registry = TaskRegistry()
    registry.register(Expiring)
    memory = await _memory_backend()
    client = Client(broker=AsyncMock(), backend=memory, registry=registry)
    tid = await client.schedule("bz23_expiring", delay=3600)

    await client.cancel(tid, ttl_ms=60_000)

    ((_, ttl_ms), _) = memory.set_cancel_flag.await_args
    # Basis 60 s + rund 1 h bis zum Fire + max_age 600 s + Puffer 300 s.
    expected = 60_000 + 3_600_000 + 900_000
    assert expected - 10_000 <= ttl_ms <= expected


@pytest.mark.timeout(5)
async def test_cancel_with_unreadable_oneshot_entry_uses_base_ttl():
    memory = await _memory_backend()
    await memory.schedules.put("oneshot:broken-id", b"not json", None)
    client = Client(broker=AsyncMock(), backend=memory)

    assert await client.cancel("broken-id", ttl_ms=60_000) is True

    memory.set_cancel_flag.assert_awaited_once_with("broken-id", 60_000)


@pytest.mark.timeout(5)
async def test_cancel_propagates_connection_error_of_the_oneshot_lookup(client, backend):
    from swarmq.exceptions import BackendConnectionError

    backend.schedules.get = AsyncMock(side_effect=BackendConnectionError("valkey down"))
    backend.set_cancel_flag = AsyncMock(return_value=True)

    with pytest.raises(BackendConnectionError):
        await client.cancel("task-abc")
    backend.set_cancel_flag.assert_not_awaited()
