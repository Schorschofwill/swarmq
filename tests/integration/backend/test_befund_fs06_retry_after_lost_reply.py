"""Nachweis FS-06 (Teil 1): Der Valkey-Client sendet nach verlorener Antwort
nicht idempotente Befehle erneut; der zweite Lauf haelt den eigenen Claim fuer
fremd.

Register: docs/befunde/fehlersuche-runde-2-2026-10.md (FS-06; Quelle K-3).
Teil 2 (Leader-Renew ohne ``op_timeout``) steht in
``tests/unit/scheduler/test_befund_fs06_leader_renew_timeout.py``.

``ValkeyConnection.connect`` (``backend/valkey/connection.py``) baut den
Client mit ``Retry(ExponentialBackoff, retries=5)`` und
``retry_on_error=[ConnectionError, TimeoutError, OSError]``. valkey-py
(``execute_command`` -> ``call_with_retry`` -> ``_send_command_parse_response``)
sendet den Befehl erneut, wenn die Antwort verloren geht, obwohl der Server ihn
schon ausgefuehrt hat (Valkey-Neustart, Failover, Proxy-Reset,
``CLIENT KILL``). Der zweite Lauf trifft auf den eigenen Claim:

- ``claim_unique`` (``SET NX GET``) liefert die eigene ``task_id`` als
  "existing"; ``Client.schedule(unique_for=...)`` gibt sie zurueck und
  publiziert nie.
- ``claim_fire`` (``sched_fire_claim.lua``) liefert 0; das Fire wird als
  Duplikat bestaetigt.
- ``LockStore.acquire`` (``lock_acquire.lua``, Mutex) liefert ``None``; der
  eigene verwaiste Lock blockiert bis ``hard_timeout + 30 s``.

Erwartet: ein Claim, den derselbe Owner schon haelt, gilt als gewonnen.
Beim Unique-Claim entscheidet das die Aufrufstelle, nicht das Backend (KTD7):
nur wo die ``task_id`` in genau diesem Aufruf frisch erzeugt wurde (Client,
Leader-Cron); ``swarmq schedule add --unique-for`` und der Leader-Oneshot
halten eine stabile ID und bleiben beim Selbsttreffer Duplikat (E-011).

Integration (echter Valkey auf dem Testport): Die Antwort wird nach dem
vollstaendigen Lesen verworfen (``Valkey.parse_response`` wirft einmal
``ConnectionError``), der Befehl ist also sicher auf dem Server gelaufen; den
Rest (Trennen, Wiederholen) macht der echte Client. Ein Unit-Fake kann die
Lua-Skripte nicht ausfuehren.

Testliste:
# [x] Client.schedule(unique_for) nach verlorener SET-Antwort -> publiziert
#     (eigener Claim; behoben U11)
# [x] claim_fire nach verlorener Antwort -> True (eigener Claim; behoben U6)
# [x] Mutex-Acquire nach verlorener Antwort -> Token (eigener Lock; behoben U11)
# [x] Semaphor voll inklusive eigenem Token, verlorene Antwort -> Token (U11)
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any, cast
from unittest.mock import AsyncMock

import msgspec
import pytest
import valkey.asyncio.client
from valkey.exceptions import ConnectionError as ValkeyConnectionError

from swarmq.backend.valkey import ValkeyBackend
from swarmq.client import Client

pytestmark = [pytest.mark.integration, pytest.mark.timeout(10)]


class _LostReply:
    """Verwirft die Antwort des naechsten Befehls *command* einmal, nachdem sie
    vollstaendig gelesen wurde."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, command: str) -> None:
        self.injected = False
        # valkey-py typisiert ``**options`` nicht; der cast legt die Signatur fest.
        original = cast("Callable[..., Awaitable[Any]]", valkey.asyncio.client.Valkey.parse_response)
        outer = self

        async def _parse_response(self: Any, connection: Any, command_name: Any, **options: Any) -> Any:
            response = await original(self, connection, command_name, **options)
            if not outer.injected and command_name == command:
                outer.injected = True
                raise ValkeyConnectionError("Connection reset by peer (Antwort nach Ausfuehrung verloren)")
            return response

        monkeypatch.setattr(valkey.asyncio.client.Valkey, "parse_response", _parse_response)


@pytest.fixture
async def backend(valkey_url: str) -> AsyncIterator[ValkeyBackend]:
    backend = ValkeyBackend(valkey_url)
    await backend.connect()
    try:
        yield backend
    finally:
        await backend.close()


@pytest.mark.befund("FS-06")
async def test_claim_unique_after_lost_reply_counts_as_own_claim(
    backend: ValkeyBackend, monkeypatch: pytest.MonkeyPatch
) -> None:
    broker = AsyncMock()
    client = Client(broker=broker, backend=backend)
    lost = _LostReply(monkeypatch, "SET")

    task_id = await client.schedule("fs06.unique", uuid.uuid4().hex, queue="fs06", unique_for=60)

    assert lost.injected, "Voraussetzung: Antwort des SET verloren"
    broker.publish.assert_awaited_once()
    published = msgspec.json.decode(broker.publish.await_args.args[1])["task"]["id"]
    assert published == task_id, "eigener Unique-Claim nach Transport-Retry als Duplikat gewertet"


@pytest.mark.befund("FS-06")
async def test_claim_fire_after_lost_reply_counts_as_own_claim(
    backend: ValkeyBackend, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Skript vorab laden: der Zielbefehl ist dann genau ein EVALSHA.
    assert await backend.schedules.claim_fire("fs06:warmup:1", 60_000, owner="fs06-warmup")
    lost = _LostReply(monkeypatch, "EVALSHA")

    claimed = await backend.schedules.claim_fire("fs06:cron:1", 60_000, owner="fs06-owner")

    assert lost.injected, "Voraussetzung: Antwort des EVALSHA verloren"
    assert claimed is True, "eigener Fire-Claim nach Transport-Retry als Duplikat gewertet"


@pytest.mark.befund("FS-06")
async def test_mutex_acquire_after_lost_reply_counts_as_own_lock(
    backend: ValkeyBackend, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert await backend.locks.acquire("fs06-warmup", owner="fs06-warmup", ttl_ms=60_000, max_concurrency=1)
    lost = _LostReply(monkeypatch, "EVALSHA")

    token = await backend.locks.acquire("fs06-mutex", owner="fs06-owner", ttl_ms=60_000, max_concurrency=1)

    assert lost.injected, "Voraussetzung: Antwort des EVALSHA verloren"
    assert token is not None, "eigener Mutex nach Transport-Retry als fremd gewertet (Konflikt mit sich selbst)"


async def test_full_semaphore_acquire_after_lost_reply_counts_as_own_slot(
    backend: ValkeyBackend, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Der eigene Slot fuellt den Semaphor; ohne Selbsttreffer zaehlte die
    Wiederholung ihn als belegt und wiese sich selbst ab."""
    name = f"fs06-sem-{uuid.uuid4().hex[:8]}"
    assert await backend.locks.acquire(name, owner="fs06-other", ttl_ms=60_000, max_concurrency=2)
    lost = _LostReply(monkeypatch, "EVALSHA")

    token = await backend.locks.acquire(name, owner="fs06-owner", ttl_ms=60_000, max_concurrency=2)

    assert lost.injected, "Voraussetzung: Antwort des EVALSHA verloren"
    assert token is not None, "eigener Semaphor-Slot nach Transport-Retry als fremd gewertet"
