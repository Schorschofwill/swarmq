"""Nachweis DP-01: ``sliding_window_counter.lua`` lässt Abgewiesene bis zum
Ende des festen Fensters warten statt bis wieder Platz im gleitenden ist.

Register: docs/befunde/doku-pruefung-2026-10.md (DP-01, Verdacht V1).

Das Skript lieferte als ``retry_after_ms`` die Restzeit des festen Fensters.
Bei Rückstand kommen alle Abgewiesenen gesammelt an der Grenze zurück, wo das
Vorfenster noch voll zählt; der Durchsatz sinkt auf etwa die Hälfte (Probe
mit dem echten Skript: ``100/m`` ergibt Ø 48–56 je Minute).

Erwartet (Doku auf ``main``, ``guides/rate-limiting.md``): Wartende werden
frei, „when enough headroom reappears. There is no hard edge at the minute
mark.“ Daraus: Ohne weitere Aufrufe dazwischen geht ein Aufruf zur Zeit
``now + retry_after_ms`` durch, einer kurz davor nicht.

Die Zeit ist als ``now_ms`` Argument des Skripts gesetzt (wie
``RateLimiter.check`` die Serverzeit übergibt); es wird nicht gewartet.

Testliste:
# [x] Vorfenster voll, aktuelles zu 25 %, 25 % ins Fenster: Platz nach ~1 ms statt 45 s
# [x] aktuelles Fenster voll: Platz 1 ms nach Fensterbeginn statt erst eines Fensters später
# [x] Vorfenster und aktuelles teilweise voll: genau ab retry_after wieder Platz
# [x] ganzzahlige Grenze (100/45, 0/125): keine Abweisung durch Gleitkomma-Rundung
"""

from __future__ import annotations

import uuid

import pytest
import valkey.asyncio as valkey_asyncio

from swarmq.lua import load_script
from tests.support.services import valkey_db_url

pytestmark = [pytest.mark.integration, pytest.mark.timeout(10), pytest.mark.befund("DP-01")]

_WINDOW_MS = 60_000
_LIMIT = 100
# Ein Fensterbeginn (Vielfaches von 60 000 ms).
_WINDOW_START = 1_700_000_040_000


@pytest.fixture
async def valkey_client():
    client = valkey_asyncio.Valkey.from_url(valkey_db_url())
    try:
        yield client
    finally:
        await client.aclose()


class _Limiter:
    def __init__(self, client: valkey_asyncio.Valkey) -> None:
        self._client = client
        self._script = client.register_script(load_script("sliding_window_counter"))
        self._name = f"dp01-{uuid.uuid4().hex[:12]}"

    def key(self, window_start: int) -> str:
        return f"sq:rl:{self._name}:{window_start}"

    async def preset(self, window_start: int, count: int) -> None:
        await self._client.set(self.key(window_start), count, px=4 * _WINDOW_MS)

    async def check(self, now_ms: int) -> tuple[int, int]:
        window_start = now_ms - now_ms % _WINDOW_MS
        keys = [self.key(window_start), self.key(window_start - _WINDOW_MS)]
        allowed, _remaining, retry = await self._script(keys=keys, args=[_LIMIT, _WINDOW_MS, now_ms])
        return int(allowed), int(retry)

    async def assert_headroom_exactly_after(self, now_ms: int) -> None:
        allowed, retry = await self.check(now_ms)
        assert allowed == 0
        if retry > 1:
            # Kurz davor noch kein Platz (abgewiesen zählt nicht).
            assert (await self.check(now_ms + retry - 1))[0] == 0
        assert (await self.check(now_ms + retry))[0] == 1


async def test_previous_window_full_frees_headroom_while_the_window_runs(valkey_client) -> None:
    limiter = _Limiter(valkey_client)
    await limiter.preset(_WINDOW_START - _WINDOW_MS, _LIMIT)
    await limiter.preset(_WINDOW_START, _LIMIT // 4)
    # 25 % ins Fenster: Schätzung 100 · 0,75 + 25 = 100, gerade am Limit.
    now = _WINDOW_START + _WINDOW_MS // 4

    _, retry = await limiter.check(now)

    assert retry <= 1_000
    await limiter.assert_headroom_exactly_after(now)


async def test_current_window_full_frees_headroom_right_after_the_boundary(valkey_client) -> None:
    limiter = _Limiter(valkey_client)
    await limiter.preset(_WINDOW_START, _LIMIT)
    now = _WINDOW_START + _WINDOW_MS // 2

    _, retry = await limiter.check(now)

    assert retry <= _WINDOW_MS // 2 + 1_000
    await limiter.assert_headroom_exactly_after(now)


async def test_partly_filled_windows_free_headroom_exactly_at_retry_after(valkey_client) -> None:
    limiter = _Limiter(valkey_client)
    await limiter.preset(_WINDOW_START - _WINDOW_MS, 80)
    await limiter.preset(_WINDOW_START, 40)

    await limiter.assert_headroom_exactly_after(_WINDOW_START + 6_000)


@pytest.mark.parametrize(("previous", "current"), [(100, 45), (0, 125)])
async def test_an_integer_boundary_is_not_lost_to_float_rounding(valkey_client, previous: int, current: int) -> None:
    """Review DP-01: ``1 - 55/100`` ist in double ``0.44999…``; mit
    ``floor(r · W) + 1`` zeigte retry_after auf den Zeitpunkt, an dem die
    Schätzung genau das Limit erreicht (abgewiesen). Ebenso ``1 - 100/125``
    im Zweig „erst im nächsten Fenster“."""
    limiter = _Limiter(valkey_client)
    if previous:
        await limiter.preset(_WINDOW_START - _WINDOW_MS, previous)
    await limiter.preset(_WINDOW_START, current)

    await limiter.assert_headroom_exactly_after(_WINDOW_START)
