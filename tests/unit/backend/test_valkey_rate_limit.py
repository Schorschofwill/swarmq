"""``ValkeyRateLimitStore``: Transportfehler werden an der Backend-Grenze zu
``BackendConnectionError`` -- das Rate-Limit-Gate stellt die Nachricht dann
zurueck, statt sie per Requeue das Zustellbudget verbrauchen zu lassen.

Test-Liste:
- [x] Valkey-Verbindungsfehler -> BackendConnectionError
- [x] Valkey-Timeout und OS-Fehler -> BackendConnectionError
- [x] Logikfehler (ResponseError) bleibt, wie er ist
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
import valkey.exceptions

from swarmq.backend.valkey.rate_limit import ValkeyRateLimitStore
from swarmq.exceptions import BackendConnectionError

pytestmark = pytest.mark.timeout(5)


def _store(script_error: BaseException) -> ValkeyRateLimitStore:
    client = MagicMock()
    client.time = AsyncMock(return_value=(1, 0))
    client.register_script = MagicMock(return_value=AsyncMock(side_effect=script_error))
    return ValkeyRateLimitStore(client)


async def test_valkey_connection_error_becomes_backend_connection_error():
    store = _store(valkey.exceptions.ConnectionError("refused"))

    with pytest.raises(BackendConnectionError, match="refused"):
        await store.check("t", max_requests=1, window_ms=1000)


@pytest.mark.parametrize(
    "error", [valkey.exceptions.TimeoutError("slow"), TimeoutError("slow"), ConnectionRefusedError("slow")],
)
async def test_timeouts_and_os_errors_become_backend_connection_error(error):
    store = _store(error)

    with pytest.raises(BackendConnectionError, match="slow"):
        await store.check("t", max_requests=1, window_ms=1000)


async def test_logic_error_is_not_wrapped():
    """Ein Skriptfehler ist kein Backend-Ausfall: er soll nicht als
    voruebergehend behandelt (und zurueckgestellt) werden."""
    store = _store(valkey.exceptions.ResponseError("bad script"))

    with pytest.raises(valkey.exceptions.ResponseError):
        await store.check("t", max_requests=1, window_ms=1000)
