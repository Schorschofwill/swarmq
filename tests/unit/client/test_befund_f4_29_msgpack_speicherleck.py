"""Nachweis F4-29: Ein abgelehntes Ergebnis aus dem Backend hält keinen
Speicher fest.

Register: docs/befunde/fehlersuche-runde-4-2026-10.md (F4-29; Signal S-007,
``uv audit``: GHSA-j586-36cw-2gc2).

Die C-Erweiterung von msgpack bis 1.2.2 behält den schon dekodierten Wert
einer Map, wenn ``unpackb`` deren Schlüssel ablehnt (weder str noch bytes,
``strict_map_key``). ``decode_result`` reicht das als
``ResultSerializationError`` weiter (Zusage im Docstring), der Wert aber
bleibt für immer im Speicher. Auslöser braucht keinen Angreifer: Gibt ein
Task ein Dict mit int-Schlüsseln zurück, speichert der Worker es mit
``encode_result``, und jedes ``get_result`` darauf verliert den ganzen
Objektgraphen des Ergebnisses.

Gemessen wird mit ``tracemalloc``: Der Wert ist ein ``bytes``-Objekt, das
über den Python-Allokator entsteht. Vier abgelehnte Ergebnisse mit je 1 MiB
lassen unter 1.2.2 rund 4 MiB liegen; die Grenze von 256 KiB lässt Platz
für Rauschen und liegt weit unter einem einzigen Wert.

Testliste:
# [x] Task-Ergebnis mit int-Schlüssel und 1-MiB-Wert, viermal gelesen ->
#     ResultSerializationError, kein Speicherzuwachs
"""

from __future__ import annotations

import tracemalloc
from unittest.mock import AsyncMock

import pytest

from swarmq.client import Client
from swarmq.exceptions import ResultSerializationError
from swarmq.serialization import encode_result

pytestmark = pytest.mark.timeout(5)

_VALUE_BYTES = 1024 * 1024
_READS = 4
_ALLOWED_GROWTH_BYTES = 256 * 1024


@pytest.mark.befund("F4-29")
async def test_rejected_result_from_backend_retains_no_memory(client: Client, backend: AsyncMock) -> None:
    rejected = encode_result({"result": {42: b"A" * _VALUE_BYTES}, "is_error": False})
    backend.get_result = AsyncMock(return_value=rejected)

    tracemalloc.start()
    try:
        before = tracemalloc.get_traced_memory()[0]
        for _ in range(_READS):
            with pytest.raises(ResultSerializationError):
                await client.get_result("task-1", timeout=0)
        growth = tracemalloc.get_traced_memory()[0] - before
    finally:
        tracemalloc.stop()

    assert growth < _ALLOWED_GROWTH_BYTES, f"{growth} Bytes nach {_READS} abgelehnten Ergebnissen festgehalten"
