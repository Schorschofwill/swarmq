"""Nachweis F4-11: ``ChordManager`` wandelt Valkey-Treiberfehler nicht in
``BackendConnectionError``.

Register: docs/befunde/fehlersuche-runde-4-2026-10.md (F4-11; Verdacht
R4M-D-01; Sichtung ``3c-sichtung-D.md``, Gegenprüfung ``6-gegenpruefung-M.md``).

``ChordManager.record_and_check_complete`` (``backend/valkey/chords.py``) ruft
``chord_complete.lua`` ohne ``try``; alle anderen Koordinations-Stores
(``LockManager``, Guard, Rate-Limit, Sollwerte) wandeln an derselben Grenze
mit ``as_backend_error``. Ein Transportfehler, ein Zeitablauf oder eine
heilende Fehlerantwort (``OOM`` unter ``noeviction``, E-005) kommt roh beim
Worker an, landet dort im ``except Exception`` des Advance und markiert einen
Workflow, dessen Schritte alle gelungen sind, als ``failed``
(Nachweis auf Worker-Ebene: ``tests/unit/worker/test_befund_f4_11_advance_redelivers_on_chord_outage.py``).

Erwartet: Plan 2026-05-30-001 ``:89`` („jede neue Backend-Methode wrappt
``(ValkeyError, OSError)`` → ``BackendConnectionError``, sonst entkommt der
Counter-Fehler dem Worker-Handler“), ``:451``; ``logging.md:666-667``;
Einordnung ``as_backend_error`` (Ausfall = Transportfehler oder heilende
Fehlerantwort).

Grenze gemockt: ein Fake-Client, dessen registriertes Skript den Treiberfehler
wirft (wie valkey-py nach erschöpfter Retry-Politik bzw. bei einer
``OOM``-Antwort).

Testliste:
# [x] ``record_member``: ``ConnectionError`` -> ``BackendConnectionError`` (Ausfall)
# [x] ``record_member``: ``TimeoutError`` -> ``BackendConnectionError`` (Ausfall)
# [x] ``record_member``: ``OutOfMemoryError`` (OOM, noeviction) -> Ausfall, nicht ``BackendResponseError``
# [x] ``record_member``: ``ResponseError`` mit Präfix ``OOM`` -> Ausfall
# [x] ``record_and_check_complete`` (Altname) wandelt ebenso
"""

from __future__ import annotations

from collections.abc import Awaitable
from unittest.mock import AsyncMock, MagicMock

import pytest
from valkey.exceptions import ConnectionError as ValkeyConnectionError
from valkey.exceptions import OutOfMemoryError, ResponseError
from valkey.exceptions import TimeoutError as ValkeyTimeoutError

from swarmq.backend.valkey.chords import ChordManager
from swarmq.exceptions import BackendConnectionError, BackendResponseError

pytestmark = pytest.mark.timeout(5)

_OUTAGES = [
    pytest.param(ValkeyConnectionError("Error 111 connecting to valkey:6379. Connection refused."), id="connection"),
    pytest.param(ValkeyTimeoutError("Timeout reading from socket"), id="timeout"),
    pytest.param(OutOfMemoryError("command not allowed when used memory > 'maxmemory'."), id="oom-class"),
    pytest.param(ResponseError("OOM command not allowed when used memory > 'maxmemory'."), id="oom-prefix"),
]


def _chords(error: BaseException) -> ChordManager:
    client = MagicMock()
    client.register_script = MagicMock(return_value=AsyncMock(side_effect=error))
    return ChordManager(client)


async def _error_of(call: Awaitable[object]) -> BaseException | None:
    try:
        await call
    except Exception as exc:
        return exc
    return None


def _assert_outage(error: BaseException | None, cause: BaseException) -> None:
    assert isinstance(error, BackendConnectionError), (
        f"ChordManager liefert {type(error).__name__} für {type(cause).__name__} aus dem Skript; "
        "erwartet BackendConnectionError (Plan 2026-05-30-001 :89, as_backend_error)"
    )
    assert not isinstance(error, BackendResponseError), "ein heilender Fehler ist ein Ausfall, keine Fehlerantwort"


@pytest.mark.befund("F4-11")
@pytest.mark.parametrize("cause", _OUTAGES)
async def test_record_member_reports_a_valkey_outage_as_backend_connection_error(cause: BaseException) -> None:
    error = await _error_of(_chords(cause).record_member("wf", "/h", "m1", expected=2, ttl_ms=60_000))

    _assert_outage(error, cause)


@pytest.mark.befund("F4-11")
async def test_record_and_check_complete_reports_a_valkey_outage_as_backend_connection_error() -> None:
    cause = ValkeyConnectionError("Error 111 connecting to valkey:6379. Connection refused.")

    error = await _error_of(_chords(cause).record_and_check_complete("wf", "/h", "m1", 2, 60_000))

    _assert_outage(error, cause)
