"""Nachweis F3-01: Geht die Antwort auf das Speichern in ``ScheduleStore.add``
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

Seit 0.12.3 schreibt ``sched_add.lua`` Definition und Fire-Index in einem
EVAL (KTD4); verloren geht also die Antwort auf das ``EVALSHA``. Die
Wiederholung meldet 0, und die Definitionsschicht erkennt den eigenen Insert.

Erwartet laut E-058 (Georg, 2026-10-15): Oneshots (frische UUID im
Namen) sind beim Speichern idempotent; ein byte-gleicher bestehender Eintrag
zaehlt als eigener Insert, der Fire-Index wird ergaenzt. ``swarmq schedule
add`` mit gleichem Namen meldet weiter „already exists“; seit dem atomaren
Skript ist der Eintrag dabei vollständig (zweiter Test).

Integration (echter Valkey auf dem Testport): Die Antwort wird nach dem
vollstaendigen Lesen verworfen, den Rest (Trennen, Wiederholen) macht der
echte Client.

Testliste:
# [x] schedule(delay=60) nach verlorener EVALSHA-Antwort -> Eintrag im Fire-Index
# [x] CLI-Cron nach verlorener EVALSHA-Antwort -> Score vorhanden,
#     ``add_schedule`` meldet ``False`` („already exists“, E-058)
# [x] Altzustand eines 0.12.x-Clients (Definition per HSET ohne ZADD):
#     ``add_schedule(oneshot)`` -> True, Fire-Index ergänzt (Review U3,
#     echter Byte-Vergleich msgspec gegen HGET)
"""

from __future__ import annotations

import hashlib
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
from swarmq.lua import load_script
from swarmq.scheduler import ScheduleDefinition, ScheduleStore

pytestmark = [pytest.mark.integration, pytest.mark.timeout(10)]


class _F301LostReply:
    """Verwirft die Antwort des naechsten ``EVALSHA`` auf ``sched_add`` einmal,
    nachdem sie vollstaendig gelesen wurde (das Skript ist auf dem Server
    gelaufen). Der SHA wird geprueft, damit ein vorgelagertes anderes Skript
    die Injektion nicht verbraucht und der Test leer gruen wird (Review U3)."""

    SCHED_ADD_SHA = hashlib.sha1(load_script("sched_add").encode(), usedforsecurity=False).hexdigest()

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.injected = False
        self._args: tuple[Any, ...] = ()
        # valkey-py typisiert ``**options`` nicht; der cast legt die Signatur fest.
        original_execute = cast("Callable[..., Awaitable[Any]]", valkey.asyncio.client.Valkey.execute_command)
        original_parse = cast("Callable[..., Awaitable[Any]]", valkey.asyncio.client.Valkey.parse_response)
        outer = self

        async def _execute_command(self: Any, *args: Any, **options: Any) -> Any:
            outer._args = args
            return await original_execute(self, *args, **options)

        async def _parse_response(self: Any, connection: Any, command_name: Any, **options: Any) -> Any:
            response = await original_parse(self, connection, command_name, **options)
            if not outer.injected and outer._is_sched_add(command_name):
                outer.injected = True
                raise ValkeyConnectionError("Connection reset by peer (Antwort nach Ausfuehrung verloren)")
            return response

        monkeypatch.setattr(valkey.asyncio.client.Valkey, "execute_command", _execute_command)
        monkeypatch.setattr(valkey.asyncio.client.Valkey, "parse_response", _parse_response)

    def _is_sched_add(self, command_name: Any) -> bool:
        sha = self._args[1] if len(self._args) > 1 else None
        if isinstance(sha, bytes):
            sha = sha.decode()
        return command_name == "EVALSHA" and sha == self.SCHED_ADD_SHA


@pytest.fixture
async def backend(valkey_url: str) -> AsyncIterator[ValkeyBackend]:
    backend = ValkeyBackend(valkey_url)
    await backend.connect()
    try:
        yield backend
    finally:
        await backend.close()


@pytest.mark.befund("F3-01")
@pytest.mark.entscheidung("E-058")
async def test_deferred_oneshot_is_indexed_after_lost_hsetnx_reply(
    backend: ValkeyBackend, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = Client(broker=AsyncMock(), backend=backend)
    lost = _F301LostReply(monkeypatch)

    task_id = await client.schedule("f301.oneshot", uuid.uuid4().hex, queue="f301", delay=60)
    name = f"oneshot:{task_id}"
    try:
        assert lost.injected, "Voraussetzung: Antwort des sched_add-EVALSHA verloren"
        assert await backend.schedules.get(name) is not None, "Voraussetzung: Definition gespeichert"
        assert await backend.schedules.fire_ts(name) is not None, (
            "Oneshot ohne Eintrag im Fire-Index: schedule() lieferte eine ID, die nie feuert"
        )
    finally:
        await backend.schedules.remove(name)


@pytest.mark.befund("F3-01")
@pytest.mark.entscheidung("E-058")
async def test_cli_cron_is_indexed_after_lost_reply_and_reports_already_exists(
    backend: ValkeyBackend, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bewusste Folge von E-058: „already exists“ trotz eigenem Insert --
    harmlos, weil der Eintrag vollständig ist und feuert."""
    name = f"f301-cli-{uuid.uuid4().hex[:12]}"
    definition = ScheduleDefinition(
        schedule_name=name,
        task_name="f301.cron",
        args=(),
        kwargs={},
        queue="f301",
        cron="0 3 * * *",
        fire_ts_ms=1_700_000_000_000,
        source="cli",
    )
    lost = _F301LostReply(monkeypatch)
    try:
        assert await ScheduleStore(backend).add_schedule(definition) is False
        assert lost.injected, "Voraussetzung: Antwort des sched_add-EVALSHA verloren"
        assert await backend.schedules.fire_ts(name) == 1_700_000_000_000
    finally:
        await backend.schedules.remove(name)


@pytest.mark.entscheidung("E-058")
async def test_identical_oneshot_without_fire_index_gets_indexed(backend: ValkeyBackend) -> None:
    """Reparaturzweig der Definitionsschicht gegen echtes Valkey: ein 0.12.x-
    Client hat HSETNX geschrieben, das ZADD fehlt. Das ``add`` desselben
    Oneshots gilt als eigener Insert und ergänzt den Fire-Index."""
    task_id = str(uuid.uuid7())
    name = f"oneshot:{task_id}"
    definition = ScheduleDefinition(
        schedule_name=name,
        task_name="f301.oneshot",
        args=("x",),
        kwargs={"k": 1},
        queue="f301",
        cron=None,
        fire_ts_ms=1_700_000_060_000,
        source="cli",
        task_id=task_id,
    )
    await backend.schedules.hset_schedule_def(name, msgspec.json.encode(definition))
    try:
        assert await backend.schedules.fire_ts(name) is None, "Voraussetzung: kein Fire-Index"
        assert await ScheduleStore(backend).add_schedule(definition) is True
        assert await backend.schedules.fire_ts(name) == 1_700_000_060_000
    finally:
        await backend.schedules.remove(name)
