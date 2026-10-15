"""Nachweis F3-01: Geht die Antwort auf das HSETNX in ``ScheduleStore.add``
verloren, fehlt der Oneshot im Fire-Index.

Register: docs/befunde/fehlersuche-runde-3-2026-10.md (F3-01; Quelle R3B-01).
Muster: ``tests/integration/backend/test_befund_fs06_retry_after_lost_reply.py``
(FS-06: alle NX-Befehle gegen Transport-Wiederholung pruefen).

``ValkeyConnection`` baut den Client mit Wiederholung bei
``ConnectionError``/``TimeoutError``. Geht die Antwort auf das HSETNX
verloren, obwohl Valkey den Eintrag schon geschrieben hat, sendet valkey-py
den Befehl erneut; die Wiederholung meldet ``False``. ``add``
(``backend/valkey/schedules.py``) kehrt dann vor dem ZADD zurueck,
``_schedule_deferred`` ignoriert den Rueckgabewert und liefert die ID. Der
Oneshot aus ``schedule(delay=…)`` steht in ``sq:sched:defs``, aber nicht im
Fire-Index, und laeuft nie (mit ``unique_for`` sperrt er zudem gleiche
Aufrufe).

Erwartet laut Entscheidung 4 (Georg, 2026-10-15): Oneshots (frische UUID im
Namen) sind beim Speichern idempotent; ein byte-gleicher bestehender Eintrag
zaehlt als eigener Insert, der Fire-Index wird ergaenzt. ``swarmq schedule
add`` mit gleichem Namen meldet weiter „already exists“ (nicht Teil dieses
Nachweises).

Integration (echter Valkey auf dem Testport): Die Antwort wird nach dem
vollstaendigen Lesen verworfen, den Rest (Trennen, Wiederholen) macht der
echte Client.

Testliste:
# [x] schedule(delay=60) nach verlorener HSETNX-Antwort -> Eintrag im Fire-Index
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest
import valkey.asyncio.client
from valkey.exceptions import ConnectionError as ValkeyConnectionError

from swarmq.backend.valkey import ValkeyBackend
from swarmq.client import Client

pytestmark = [pytest.mark.integration, pytest.mark.timeout(10)]


class _F301LostReply:
    """Verwirft die Antwort des naechsten Befehls *command* einmal, nachdem sie
    vollstaendig gelesen wurde (der Befehl ist auf dem Server gelaufen)."""

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


@pytest.mark.befund("F3-01")
@pytest.mark.xfail(strict=True, reason="F3-01: offen - Oneshot fehlt nach verlorener HSETNX-Antwort im Fire-Index")
async def test_deferred_oneshot_is_indexed_after_lost_hsetnx_reply(
    backend: ValkeyBackend, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = Client(broker=AsyncMock(), backend=backend)
    lost = _F301LostReply(monkeypatch, "HSETNX")

    task_id = await client.schedule("f301.oneshot", uuid.uuid4().hex, queue="f301", delay=60)
    name = f"oneshot:{task_id}"
    try:
        assert lost.injected, "Voraussetzung: Antwort des HSETNX verloren"
        assert await backend.schedules.get(name) is not None, "Voraussetzung: Definition gespeichert"
        assert await backend.schedules.fire_ts(name) is not None, (
            "Oneshot ohne Eintrag im Fire-Index: schedule() lieferte eine ID, die nie feuert"
        )
    finally:
        await backend.schedules.remove(name)
