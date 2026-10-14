"""Nachweis FS-07 (Client): Ein Timeout nach erfolgreichem Unique-Claim des
Producers gibt das Flag nicht frei.

Register: docs/befunde/fehlersuche-runde-2-2026-10.md (FS-07; Quelle K-4).
Plan 2026-10-14-0748, U12, KTD8.

``schedule``, ``_schedule_deferred`` und ``schedule_many`` claimen vor ihrem
Rollback-``try``. Laeuft ``claim_unique`` in ``op_timeout``, nachdem Valkey das
Flag schon gesetzt hat, haelt das Flag eine nie publizierte ``task_id``; die
Wiederholung des Aufrufers gilt bis ``unique_for`` als Duplikat und publiziert
nichts. ``schedule_many`` gab beim Rollback einen Eintrag nicht frei, dessen
Transport-Wiederholung die eigene ``task_id`` meldete (FS-06b); Eintraege mit
Claim-Fehler gab es schon vor dem Fix frei (der Test dazu ist ein
Charakterisierungstest). Erwartet: owner-gepruefte Freigabe inline, dann
propagiert der Fehler; die Wiederholung publiziert. Ein Abbruch
(``CancelledError``) waehrend des Claims bleibt Restluecke (Flag bis TTL).

Der Lock-Teil steht in
``tests/unit/worker/gates/test_befund_fs07_lock_claim_after_timeout.py``, der
Leader-Teil in ``tests/unit/scheduler/test_befund_fs07_unique_claim_after_timeout.py``.

Testliste:
# [x] schedule: Flag gesetzt, Antwort im Timeout verloren -> Fehler propagiert,
#     Flag frei, die Wiederholung publiziert
# [x] _schedule_deferred (delay): dasselbe, die Wiederholung speichert
# [x] schedule_many: Eintrag mit Claim-Fehler wird freigegeben, die
#     Wiederholung publiziert alle Elemente (Charakterisierung, vor dem Fix gruen)
# [x] schedule_many: Eintrag, dessen Transport-Wiederholung die eigene task_id
#     meldet (FS-06b), wird beim Rollback ebenfalls freigegeben
# [x] Freigabe trifft nie ein fremdes Flag (owner-geprueft)
# [x] Freigabe scheitert am Ausfall (echtes ``release_unique``: ``False`` und
#     ``unique_release_backend_error``) -> der Claim-Fehler bleibt sichtbar,
#     keine zweite Zeile ``dedup_rollback_failed``
# [x] Freigabe wirft unerwartet -> ``dedup_rollback_failed``, der Claim-Fehler
#     bleibt sichtbar
# [x] Fehlerantwort beim Claim (``BackendResponseError``, kein Ausfall) ->
#     keine Freigabe, wie im Lock-Gate (Review U12)
# [x] schedule_many: Eintrag mit Fehlerantwort beim Claim wird beim Rollback
#     nicht freigegeben, ein erfolgreicher Claim desselben Batches schon
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest
from valkey.exceptions import ConnectionError as ValkeyConnectionError

from swarmq.backend.valkey import ValkeyBackend
from swarmq.client import Client
from swarmq.exceptions import BackendConnectionError, BackendResponseError
from swarmq.serialization import compute_dedup_hash

pytestmark = pytest.mark.timeout(5)


class _FlagStore:
    """``SET NX GET`` mit owner-gepruefter Freigabe; die Claims in
    ``lose_reply_for`` landen, ihre Antwort geht aber im Timeout verloren."""

    def __init__(self, lose_reply_for: int = 1) -> None:
        self.flags: dict[str, str] = {}
        self._lose = lose_reply_for

    async def claim_unique(
        self, dedup_hash: str, owner: str, ttl_ms: int, op_timeout: float | None = None
    ) -> str | None:
        existing = self.flags.get(dedup_hash)
        if existing is None:
            self.flags[dedup_hash] = owner
        if self._lose > 0:
            self._lose -= 1
            raise BackendConnectionError(f"setnx_unique timed out after {op_timeout}s")
        return existing

    async def release_unique(self, dedup_hash: str, owner: str, op_timeout: float | None = None) -> bool:
        if self.flags.get(dedup_hash) != owner:
            return False
        del self.flags[dedup_hash]
        return True


def _client(broker: AsyncMock, backend: AsyncMock, registry: MagicMock, flags: _FlagStore) -> Client:
    backend.claim_unique = AsyncMock(side_effect=flags.claim_unique)
    backend.release_unique = AsyncMock(side_effect=flags.release_unique)
    return Client(broker=broker, backend=backend, registry=registry)


@pytest.fixture
def schedule_store(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    from swarmq.client import schedule as client_mod

    store = MagicMock()
    store.add_schedule = AsyncMock()
    monkeypatch.setattr(client_mod, "ScheduleStore", lambda _backend: store)
    return store


@pytest.mark.befund("FS-07")
async def test_schedule_releases_a_claim_whose_reply_timed_out(
    broker: AsyncMock, backend: AsyncMock, registry: MagicMock
) -> None:
    flags = _FlagStore()
    client = _client(broker, backend, registry, flags)

    with pytest.raises(BackendConnectionError):
        await client.schedule("add", 1, 2, queue="math", unique_for=60)
    task_id = await client.schedule("add", 1, 2, queue="math", unique_for=60)

    broker.publish.assert_awaited_once()
    assert flags.flags == {backend.claim_unique.await_args.args[0]: task_id}


@pytest.mark.befund("FS-07")
async def test_deferred_schedule_releases_a_claim_whose_reply_timed_out(
    broker: AsyncMock, backend: AsyncMock, registry: MagicMock, schedule_store: MagicMock
) -> None:
    flags = _FlagStore()
    client = _client(broker, backend, registry, flags)

    with pytest.raises(BackendConnectionError):
        await client.schedule("add", 1, queue="math", delay=60.0, unique_for=300)
    task_id = await client.schedule("add", 1, queue="math", delay=60.0, unique_for=300)

    definition = schedule_store.add_schedule.await_args.args[0]
    assert definition.task_id == task_id
    assert definition.source == "cli_unique"


@pytest.mark.befund("FS-07")
async def test_schedule_many_releases_a_claim_whose_reply_timed_out(
    broker: AsyncMock, backend: AsyncMock, registry: MagicMock
) -> None:
    flags = _FlagStore()
    client = _client(broker, backend, registry, flags)
    tasks: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = [("add", (1,), {}), ("add", (2,), {})]

    with pytest.raises(BackendConnectionError):
        await client.schedule_many(tasks, queue="math", unique_for=60)
    ids = await client.schedule_many(tasks, queue="math", unique_for=60)

    assert broker.publish.await_count == 2
    assert sorted(flags.flags.values()) == sorted(ids)


@pytest.mark.befund("FS-07")
async def test_schedule_many_rollback_releases_an_own_claim_reported_by_the_retry(
    broker: AsyncMock, backend: AsyncMock, registry: MagicMock
) -> None:
    """FS-06b + FS-07: Meldet die Transport-Wiederholung eines Elements die
    eigene task_id, ist das Flag eigen; scheitert ein anderes Element, gibt der
    Rollback auch dieses frei."""
    flags = _FlagStore(lose_reply_for=0)

    async def _claim(dedup_hash: str, owner: str, ttl_ms: int, op_timeout: float | None = None) -> str | None:
        await flags.claim_unique(dedup_hash, owner, ttl_ms, op_timeout)
        if len(flags.flags) == 1:
            return owner  # Wiederholung nach verlorener Antwort meldet den eigenen Claim
        raise BackendConnectionError("setnx_unique timed out")

    client = _client(broker, backend, registry, flags)
    backend.claim_unique = AsyncMock(side_effect=_claim)
    tasks: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = [("add", (1,), {}), ("add", (2,), {})]

    with pytest.raises(BackendConnectionError):
        await client.schedule_many(tasks, queue="math", unique_for=60)

    assert flags.flags == {}
    broker.publish.assert_not_awaited()


@pytest.mark.befund("FS-07")
async def test_release_after_timeout_never_touches_a_foreign_flag(
    broker: AsyncMock, backend: AsyncMock, registry: MagicMock
) -> None:
    """Hat ein anderer Producer das Flag, bevor der eigene Claim ankam, bleibt
    es stehen: die Freigabe ist owner-geprueft."""
    flags = _FlagStore()
    client = _client(broker, backend, registry, flags)
    dedup_hash = compute_dedup_hash("add", (1, 2), {})
    flags.flags[dedup_hash] = "foreign-task"

    with pytest.raises(BackendConnectionError):
        await client.schedule("add", 1, 2, queue="math", unique_for=60)

    backend.release_unique.assert_awaited_once()
    assert flags.flags == {dedup_hash: "foreign-task"}


def _release_unique_failing_on_an_outage() -> Callable[..., Awaitable[bool]]:
    """Das echte ``ValkeyResults.release_unique`` ueber einem gestoerten Client:
    es faengt den Ausfall selbst, loggt ``unique_release_backend_error`` und
    liefert ``False``."""
    valkey = AsyncMock()
    valkey.register_script = Mock(return_value=AsyncMock(side_effect=ValkeyConnectionError("valkey down")))
    return ValkeyBackend(client=valkey).release_unique


@pytest.mark.befund("FS-07")
async def test_release_failing_on_the_outage_keeps_the_claim_error_visible(
    broker: AsyncMock, backend: AsyncMock, registry: MagicMock, swarmq_logs: list[dict[str, Any]]
) -> None:
    """Das Backend meldet den Ausfall der Freigabe selbst (mit ``dedup_hash``
    und ``task_id``); das Flag laeuft ueber seine TTL ab."""
    client = _client(broker, backend, registry, _FlagStore())
    backend.release_unique = AsyncMock(side_effect=_release_unique_failing_on_an_outage())

    with pytest.raises(BackendConnectionError, match="setnx_unique timed out"):
        await client.schedule("add", 1, 2, queue="math", unique_for=60)

    [entry] = [e for e in swarmq_logs if e["event"] == "unique_release_backend_error"]
    assert entry["task_id"] == backend.claim_unique.await_args.args[1]
    assert entry["dedup_hash"] == backend.claim_unique.await_args.args[0]
    assert [e for e in swarmq_logs if e["event"] == "dedup_rollback_failed"] == []
    broker.publish.assert_not_awaited()


@pytest.mark.befund("FS-07")
async def test_unexpected_release_error_keeps_the_claim_error_visible(
    broker: AsyncMock, backend: AsyncMock, registry: MagicMock, swarmq_logs: list[dict[str, Any]]
) -> None:
    client = _client(broker, backend, registry, _FlagStore())
    backend.release_unique = AsyncMock(side_effect=RuntimeError("bug"))

    with pytest.raises(BackendConnectionError, match="setnx_unique timed out"):
        await client.schedule("add", 1, 2, queue="math", unique_for=60)

    [entry] = [e for e in swarmq_logs if e["event"] == "dedup_rollback_failed"]
    assert entry["task_id"] == backend.claim_unique.await_args.args[1]
    assert entry["error_type"] == "RuntimeError"
    broker.publish.assert_not_awaited()


@pytest.mark.befund("FS-07")
async def test_claim_error_reply_releases_nothing(broker: AsyncMock, backend: AsyncMock, registry: MagicMock) -> None:
    """Eine Fehlerantwort (WRONGTYPE) heisst: der Claim lief nicht; wie im
    Lock-Gate wird nur nach einem Ausfall freigegeben (Review U12)."""
    client = _client(broker, backend, registry, _FlagStore(lose_reply_for=0))
    backend.claim_unique = AsyncMock(side_effect=BackendResponseError("WRONGTYPE Operation against a key"))

    with pytest.raises(BackendResponseError):
        await client.schedule("add", 1, 2, queue="math", unique_for=60)

    backend.release_unique.assert_not_awaited()
    broker.publish.assert_not_awaited()


@pytest.mark.befund("FS-07")
async def test_schedule_many_rollback_skips_an_entry_whose_claim_got_an_error_reply(
    broker: AsyncMock, backend: AsyncMock, registry: MagicMock
) -> None:
    flags = _FlagStore(lose_reply_for=0)
    bad_hash = compute_dedup_hash("add", (2,), {})

    async def _claim(dedup_hash: str, owner: str, ttl_ms: int, op_timeout: float | None = None) -> str | None:
        if dedup_hash == bad_hash:
            raise BackendResponseError("WRONGTYPE Operation against a key")
        return await flags.claim_unique(dedup_hash, owner, ttl_ms, op_timeout)

    client = _client(broker, backend, registry, flags)
    backend.claim_unique = AsyncMock(side_effect=_claim)
    tasks: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = [("add", (1,), {}), ("add", (2,), {})]

    with pytest.raises(BackendResponseError):
        await client.schedule_many(tasks, queue="math", unique_for=60)

    assert flags.flags == {}
    assert [c.args[0] for c in backend.release_unique.await_args_list] == [compute_dedup_hash("add", (1,), {})]
    broker.publish.assert_not_awaited()
