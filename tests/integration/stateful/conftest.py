"""Store-Factory Valkey der Zustandsmaschinen (Plan 2026-10-17-0941, KTD2).

Gleiche Fixture-Signatur wie ``tests/property/stateful/conftest.py``
(``open_env``), aber gegen die Valkey-Datenbank dieses xdist-Prozesses
(``tests/support/services.py``: DB N+1 für ``gwN``). Der Broker der Maschinen
bleibt ``InMemoryBroker``.

Je Hypothesis-Beispiel:

- beim Öffnen wird die Datenbank geleert (``flush_valkey_db``), das Backend
  bekommt einen eigenen Namensraum;
- ``advance`` stellt die ``FakeClock`` vor und kürzt in Valkey jede Frist um
  dieselbe Zeit (Schlüssel, deren Rest-TTL nicht reicht, werden gelöscht).
  So laufen TTLs, ``PEXPIRE GT`` und die Owner-Prüfungen der Lua-Skripte
  echt, ohne dass die Tests real warten. Dazu kommt die echte Laufzeit der
  Befehle: Eine Frist in Valkey liegt höchstens um die seit dem Öffnen real
  vergangene Zeit unter dem Modell. ``ttl_tolerance_ms`` misst genau diese
  Zeit (``time.monotonic``) plus 50 ms, auch unter CPU-Last;
- ``close`` schließt das Backend und leert die Datenbank über
  ``flush_valkey_db`` (die autouse-Fixture flusht nur je Testfunktion).

Grenze des Zeitsprungs: Werte, die Serverzeit enthalten (Scores der
Semaphor-ZSETs aus ``lock_acquire.lua``, Fenster von
``sliding_window_counter.lua``), verschiebt er nicht. Eine Maschine, die
solche Skripte fährt, muss das selbst lösen.
"""

from __future__ import annotations

import itertools
import time
import uuid
from collections.abc import Callable, Coroutine
from typing import Any

import pytest
import valkey.asyncio as valkey_asyncio

from swarmq.backend.valkey import ValkeyBackend
from swarmq.keyspace import KeySpace
from tests.support.inmemory import FakeClock
from tests.support.services import flush_valkey_db

# Zuschlag auf die gemessene Zeit: Rundung von PTTL und Uhrauflösung.
TOLERANCE_SLACK_MS = 50


async def age_keys(client: valkey_asyncio.Valkey, keys: KeySpace, delta_ms: int) -> None:
    """Zeitsprung: jede Frist im Namensraum ``keys`` um ``delta_ms`` kürzer."""
    async for key in client.scan_iter(match=keys.stem_pattern()):
        pttl = int(await client.pttl(key))
        if pttl < 0:  # ohne Frist (-1) oder inzwischen weg (-2)
            continue
        if pttl <= delta_ms:
            await client.delete(key)
        else:
            await client.pexpire(key, pttl - delta_ms)


class ValkeyEnv:
    """Store-Factory Valkey: ``ValkeyBackend`` samt Stores, ``FakeClock`` des
    Modells, Zeitsprung in Valkey."""

    def __init__(
        self, url: str, raw: valkey_asyncio.Valkey, backend: ValkeyBackend, keys: KeySpace, opened_at: float
    ) -> None:
        self.clock = FakeClock()
        self._opened_at = opened_at
        self.backend = backend
        self._url = url
        self._raw = raw
        self._keys = keys

    @property
    def ttl_tolerance_ms(self) -> int:
        """Real vergangene Zeit seit dem Öffnen plus Zuschlag: um höchstens so
        viel liegt eine Frist in Valkey unter dem Modell."""
        return int((time.monotonic() - self._opened_at) * 1000) + TOLERANCE_SLACK_MS

    async def advance(self, seconds: float) -> None:
        await self.clock.advance(seconds)
        await age_keys(self._raw, self._keys, round(seconds * 1000))

    async def close(self) -> None:
        try:
            await self.backend.close()
        finally:
            try:
                await flush_valkey_db(self._url)
            finally:
                await self._raw.aclose()


def valkey_env_factory(url: str) -> Callable[[], Coroutine[Any, Any, ValkeyEnv]]:
    """Coroutine-Factory: je Aufruf eine frische Umgebung auf geleerter DB."""
    examples = itertools.count()

    async def open_env() -> ValkeyEnv:
        opened_at = time.monotonic()
        await flush_valkey_db(url)  # prüft vorher gegen fremde Stacks
        raw = valkey_asyncio.Valkey.from_url(url)
        namespace = f"sm{next(examples)}-{uuid.uuid4().hex[:8]}"
        backend = ValkeyBackend(url, namespace=namespace)
        try:
            await backend.connect()
        except BaseException:
            try:
                await backend.close()
            finally:
                await raw.aclose()
            raise
        return ValkeyEnv(url, raw, backend, KeySpace(namespace), opened_at)

    return open_env


@pytest.fixture
def open_env(valkey_url: str) -> Callable[[], Coroutine[Any, Any, ValkeyEnv]]:
    return valkey_env_factory(valkey_url)
