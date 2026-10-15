"""Nachweis F3-09: ``get_results`` macht bei ``timeout=0`` keinen MGET und
nach einer im ``sleep`` abgelaufenen Frist keinen letzten.

Register: docs/befunde/fehlersuche-runde-3-2026-10.md (F3-09; Quelle R3D-11).

``ClientResults.get_results`` (``client/results.py``) pollt nur, solange
``loop.time() < deadline``. Bei ``timeout=0`` ist die Bedingung sofort falsch:
kein einziger MGET, alle IDs gelten als nicht fertig, obwohl die Ergebnisse
laengst da sind. Dieselbe Ursache: Laeuft die Frist im ``sleep`` ab, fehlt am
Ende der letzte MGET.

Erwartet (``api.md``: „Batch-poll version of ``get_result``“, dort
``timeout=0`` → ein einzelnes GET): mindestens ein MGET, und nach Ablauf der
Frist ein letzter MGET, bevor ``ResultNotReadyError`` fliegt.

Die Uhr ist eine Grenze und wird gefakt: ``asyncio`` im Modul ``results``
bekommt eine Schleife mit gesteuerter ``time()`` und ein ``sleep``, das diese
Zeit vorstellt.

Testliste:
# [x] timeout=0, Ergebnisse vorhanden -> Ergebnisse, genau ein MGET
# [x] Frist laeuft im sleep ab, Ergebnis kommt in der Zwischenzeit -> letzter
#     MGET liefert es
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from swarmq.client import Client
from swarmq.serialization import encode_result

pytestmark = pytest.mark.timeout(5)


class _FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def time(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> _FakeClock:
    from swarmq.client import results as results_mod

    fake = _FakeClock()
    namespace: Any = SimpleNamespace(get_running_loop=lambda: fake, sleep=fake.sleep)
    monkeypatch.setattr(results_mod, "asyncio", namespace)
    return fake


@pytest.mark.befund("F3-09")
@pytest.mark.xfail(strict=True, reason="F3-09: offen - get_results(timeout=0) macht keinen MGET")
@pytest.mark.usefixtures("clock")
async def test_get_results_with_zero_timeout_reads_once(client: Client, backend: AsyncMock) -> None:
    ready = [
        encode_result({"result": "alpha", "is_error": False}),
        encode_result({"result": "beta", "is_error": False}),
    ]
    backend.get_results = AsyncMock(return_value=ready)

    results = await client.get_results(["a", "b"], timeout=0)

    assert results == ["alpha", "beta"]
    backend.get_results.assert_awaited_once_with(["a", "b"])


@pytest.mark.befund("F3-09")
@pytest.mark.xfail(strict=True, reason="F3-09: offen - kein letzter MGET nach Ablauf der Frist im sleep")
@pytest.mark.usefixtures("clock")
async def test_get_results_reads_once_more_when_deadline_passes_during_sleep(
    client: Client, backend: AsyncMock
) -> None:
    ready = encode_result({"result": "alpha", "is_error": False})
    backend.get_results = AsyncMock(side_effect=[[None], [ready]])

    results = await client.get_results(["a"], timeout=0.05, poll_interval=0.1)

    assert results == ["alpha"], "Ergebnis lag nach dem sleep vor, wurde aber nicht mehr gelesen"
    assert backend.get_results.await_count == 2
