"""Nachweis F4-11 gegen echtes Valkey: ``OOM`` unter ``noeviction`` kommt aus
dem Chord-Store roh heraus.

Register: docs/befunde/fehlersuche-runde-4-2026-10.md (F4-11; Verdacht
R4M-D-01; Gegenprüfung ``6-gegenpruefung-M.md``, „Kein Zeitfenster nötig:
OOM unter ``noeviction``“). Unit-Nachweise:
``tests/unit/backend/test_befund_f4_11_chord_errors_wrapped.py`` (Grenze) und
``tests/unit/worker/test_befund_f4_11_advance_redelivers_on_chord_outage.py``
(Folge im Worker: Fail-Mark statt Redelivery).

E-005 verlangt ``maxmemory-policy noeviction``. Erreicht Valkey ``maxmemory``,
lehnt es Schreibbefehle mit ``OOM`` ab; ``chord_complete.lua`` (``SADD``)
scheitert sofort. Der Lock-Store wandelt dieselbe Antwort in
``BackendConnectionError`` (Gegenprobe), der Chord-Store reicht sie roh durch.

Erwartet: Plan 2026-05-30-001 ``:89`` (Chord-Zähler wrappt Treiberfehler in
``BackendConnectionError``), ``as_backend_error`` (``OOM`` ist ein Ausfall).

``exclusive``: ``CONFIG SET maxmemory`` gilt für den ganzen Test-Valkey (alle
Datenbanken). Der Test läuft deshalb nach dem parallelen Durchgang allein
(``scripts/run-tests.sh``, Testsperre), setzt ``maxmemory`` im ``finally`` auf
den vorher gelesenen Wert zurück und prüft die Rückstellung. Er verändert den
Server nur unter ``noeviction``; unter einer verdrängenden Policy würde ein
kleines ``maxmemory`` fremde Schlüssel löschen, dann scheitert er laut, ohne
etwas zu setzen.

Testliste:
# [x] ``maxmemory`` 1 Byte unter ``noeviction``: Lock-Store meldet ``BackendConnectionError`` (Gegenprobe),
#     Chord-Store muss ebenfalls ``BackendConnectionError`` melden
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest

from swarmq.backend.valkey import ValkeyBackend
from swarmq.exceptions import BackendConnectionError, BackendResponseError

pytestmark = [pytest.mark.integration, pytest.mark.timeout(15)]


@pytest.fixture
async def backend(valkey_url: str) -> AsyncIterator[ValkeyBackend]:
    backend = ValkeyBackend(valkey_url, eviction_check="off")
    await backend.connect()
    yield backend
    await backend.close()


def _value(raw: dict[Any, Any], name: str) -> str:
    for key, value in raw.items():
        text = key.decode() if isinstance(key, bytes) else str(key)
        if text == name:
            return value.decode() if isinstance(value, bytes) else str(value)
    pytest.fail(f"CONFIG GET {name} lieferte keinen Wert: {raw!r}")


async def _store_error(call: Any) -> BaseException | None:
    try:
        await call
    except Exception as exc:
        return exc
    return None


@pytest.mark.befund("F4-11")
@pytest.mark.exclusive
async def test_chord_store_reports_oom_under_noeviction_as_an_outage(backend: ValkeyBackend) -> None:
    client = backend.client
    policy = _value(await client.config_get("maxmemory-policy"), "maxmemory-policy")
    if policy != "noeviction":
        pytest.fail(f"Test-Valkey läuft mit maxmemory-policy={policy}; ein kleines maxmemory würde Schlüssel löschen")
    original = _value(await client.config_get("maxmemory"), "maxmemory")
    suffix = uuid.uuid4().hex
    try:
        await client.config_set("maxmemory", "1")
        lock_error = await _store_error(
            backend.locks.acquire(f"f411-{suffix}", owner="o", ttl_ms=60_000, max_concurrency=1)
        )
        chord_error = await _store_error(
            backend.chords.record_member(f"f411-{suffix}", "/h", "m1", expected=2, ttl_ms=60_000)
        )
    finally:
        await client.config_set("maxmemory", original)
        restored = _value(await client.config_get("maxmemory"), "maxmemory")
    if restored != original:
        pytest.fail(f"maxmemory nicht zurückgesetzt: {restored} != {original}")
    if not isinstance(lock_error, BackendConnectionError) or isinstance(lock_error, BackendResponseError):
        pytest.fail(f"Vorbedingung: Lock-Store meldet OOM nicht als Ausfall, sondern {lock_error!r}")

    assert isinstance(chord_error, BackendConnectionError), (
        f"Chord-Store liefert {type(chord_error).__name__} für OOM unter noeviction; "
        "erwartet BackendConnectionError wie der Lock-Store (Plan 2026-05-30-001 :89)"
    )
    assert not isinstance(chord_error, BackendResponseError), "OOM ist ein Ausfall, keine Fehlerantwort"
