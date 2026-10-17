"""Nachweis F4-12: ``ValkeyBackend.connect()`` reicht einen Zeitablauf beim
Verbinden roh als ``valkey.exceptions.TimeoutError`` durch.

Register: docs/befunde/fehlersuche-runde-4-2026-10.md (F4-12; Verdacht
R4M-D-02; Gegenprüfung ``6-gegenpruefung-M.md``).

``connect()`` (``backend/valkey/connection.py``) fängt nur
``(ValkeyConnectionError, OSError)``. ``valkey.exceptions.TimeoutError`` erbt
nicht von ``ConnectionError``; valkey-py wirft ihn bei
``socket_connect_timeout`` („Timeout connecting to server“) und bei
``socket_timeout`` auf dem ``PING`` (Host verwirft Pakete, angehaltener
Server). Ebenso entkommt eine Fehlerantwort auf ``PING`` (ACL ``NOPERM``).
Eingebettete Nutzer, die nach Doku ``except BackendConnectionError`` schreiben,
fangen beides nicht.

Erwartet: Vertrag ``ResultBackend.connect`` „Idempotent; Treiberfehler →
``BackendConnectionError``“ (``contracts/backend.py:17``); Kommentar
``connection.py:81-83`` („a timed-out op surfaces as ValkeyTimeoutError →
retried (transient) → wrapped to BackendConnectionError“);
``errors.md:192-193``; Fehlerkatalog (``BackendConnectionError`` in der Phase
„startup“). Die Dauer bis zur Ausnahme (rund 3 min) ist die Produktfrage
PF-16, nicht Teil dieses Nachweises.

Grenze gemockt: ``valkey.asyncio.Valkey.from_url`` liefert einen Fake-Client,
dessen ``ping`` den Treiberfehler wirft (wie valkey-py nach erschöpfter
Retry-Politik).

Testliste:
# [x] Zeitablauf beim Verbinden (``TimeoutError``) -> ``BackendConnectionError`` (Ausfall)
# [x] Fehlerantwort auf ``PING`` (``NoPermissionError``) -> ``BackendConnectionError``
"""

from __future__ import annotations

from unittest.mock import AsyncMock, Mock, patch

import pytest
from valkey.exceptions import NoPermissionError
from valkey.exceptions import TimeoutError as ValkeyTimeoutError

from swarmq.backend.valkey import ValkeyBackend
from swarmq.exceptions import BackendConnectionError, BackendResponseError

pytestmark = pytest.mark.timeout(5)


async def _connect_error(cause: BaseException) -> BaseException | None:
    backend = ValkeyBackend("valkey://valkey.invalid:6379/0", eviction_check="off")
    client = AsyncMock()
    client.register_script = Mock()
    client.ping.side_effect = cause
    with patch("swarmq.backend.valkey.connection.valkey.asyncio.Valkey.from_url", return_value=client):
        try:
            await backend.connect()
        except Exception as exc:
            return exc
    return None


@pytest.mark.befund("F4-12")
@pytest.mark.xfail(strict=True, raises=AssertionError, reason="F4-12: offen - connect() reicht TimeoutError roh durch")
async def test_connect_reports_a_timeout_as_backend_connection_error() -> None:
    error = await _connect_error(ValkeyTimeoutError("Timeout connecting to server"))

    assert isinstance(error, BackendConnectionError), (
        f"connect() liefert {type(error).__name__} für einen Zeitablauf beim Verbinden; "
        "erwartet BackendConnectionError (contracts/backend.py:17, connection.py:81-83)"
    )
    assert not isinstance(error, BackendResponseError), "ein Zeitablauf ist ein Ausfall, keine Fehlerantwort"


@pytest.mark.befund("F4-12")
@pytest.mark.xfail(
    strict=True, raises=AssertionError, reason="F4-12: offen - connect() reicht Fehlerantworten roh durch"
)
async def test_connect_reports_an_error_reply_to_ping_as_backend_connection_error() -> None:
    error = await _connect_error(NoPermissionError("this user has no permissions to run the 'ping' command"))

    assert isinstance(error, BackendConnectionError), (
        f"connect() liefert {type(error).__name__} für eine Fehlerantwort auf PING; "
        "erwartet BackendConnectionError bzw. BackendResponseError (contracts/backend.py:17)"
    )
