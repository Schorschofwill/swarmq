"""``ValkeySetpointStore`` (Plan Kontrollkanal, U1): Sollwert lesen, Rückmeldung
schreiben, Fehlerwandlung — gegen einen gemockten Client.

Test-Liste (U1):
- Lesen ohne hinterlegten Sollwert liefert ``None``.
- Lesen eines geschriebenen Sollwerts liefert Unter- und Obergrenze.
- Unlesbarer Inhalt liefert ``SetpointUnreadable`` statt einer Ausnahme.
- Gültiges JSON mit Grenzen, die keine ganzen Zahlen sind (Text, ``true``),
  ist ebenfalls unlesbar (Testlücke D-5 der Runde 4).
- Verbindungsfehler beim Lesen kommt als ``BackendConnectionError``.
- Zweimaliges ``connect()`` öffnet keine zweite Verbindung.
- Schreiben der Rückmeldung setzt das Verfallsdatum je Aufruf.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from valkey.exceptions import ConnectionError as ValkeyConnectionError

from swarmq.backend.valkey.control import ValkeySetpointStore
from swarmq.contracts.control import Setpoint, SetpointUnreadable
from swarmq.exceptions import BackendConnectionError


def _store(**client_attrs):
    client = MagicMock()
    for name, value in client_attrs.items():
        setattr(client, name, value)
    return ValkeySetpointStore(client=client), client


class TestReadSetpoint:
    async def test_group_without_setpoint_reads_as_none(self):
        store, client = _store(get=AsyncMock(return_value=None))
        assert await store.read_setpoint("orders") is None
        client.get.assert_awaited_once_with("sq:control:setpoint:orders")

    async def test_written_setpoint_reads_back_with_both_bounds(self):
        store, _ = _store(get=AsyncMock(return_value=b'{"min": 2, "max": 8}'))
        assert await store.read_setpoint("orders") == Setpoint(low=2, high=8)

    async def test_undecodable_content_reads_as_unreadable_not_exception(self):
        store, _ = _store(get=AsyncMock(return_value=b"vier bis acht"))
        result = await store.read_setpoint("orders")
        assert isinstance(result, SetpointUnreadable)
        assert result.reason

    @pytest.mark.parametrize(
        "raw",
        [
            pytest.param(b'{"min": "1", "max": 3}', id="min-text"),
            pytest.param(b'{"min": 1, "max": true}', id="max-bool"),
            pytest.param(b'{"min": true, "max": 3}', id="min-bool"),
            pytest.param(b'{"min": 1, "max": "3"}', id="max-text"),
        ],
    )
    async def test_bounds_that_are_not_integers_read_as_unreadable(self, raw):
        """Testlücke D-5 (Runde 4): Bisher war nur Nicht-JSON geprüft. Eine
        Grenze als Text oder als JSON-Wahrheitswert (in Python ``bool``, also
        auch ``int``) ist kein Sollwert; der Supervisor behandelt ihn wie eine
        Ablehnung.

        Gegenprobe-Mutanten: ``control.py`` in ``_decode_setpoint`` das erste
        ``or`` → ``and``; alternativ ``or not isinstance(high, int)`` streichen."""
        store, _ = _store(get=AsyncMock(return_value=raw))

        result = await store.read_setpoint("orders")

        assert isinstance(result, SetpointUnreadable)
        assert raw.decode() in result.reason

    async def test_connection_failure_surfaces_as_backend_connection_error(self):
        store, _ = _store(get=AsyncMock(side_effect=ValkeyConnectionError("kapow")))
        with pytest.raises(BackendConnectionError):
            await store.read_setpoint("orders")

    async def test_driver_runtime_error_surfaces_as_backend_connection_error(self):
        store, _ = _store(get=AsyncMock(side_effect=RuntimeError("client closed")))
        with pytest.raises(BackendConnectionError):
            await store.read_setpoint("orders")


class TestConnect:
    async def test_second_connect_opens_no_second_connection(self):
        client = MagicMock()
        client.ping = AsyncMock(return_value=True)
        with patch("swarmq.backend.valkey.connection.valkey.asyncio.Valkey.from_url", return_value=client) as from_url:
            store = ValkeySetpointStore("valkey://localhost:6379/0")
            await store.connect()
            await store.connect()
        assert from_url.call_count == 1


class TestWriteStatus:
    async def test_status_is_written_per_supervisor_with_expiry(self):
        store, client = _store(set=AsyncMock(return_value=True))
        await store.write_status("orders", "host:p42", {"effective": [2, 8]}, ttl_s=47.0)
        client.set.assert_awaited_once()
        args, kwargs = client.set.call_args
        assert args[0] == "sq:control:status:orders:host:p42"
        assert json.loads(args[1]) == {"effective": [2, 8]}
        assert kwargs["px"] == 47_000


class TestSteeringSide:
    """Der steuernde Dienst (A3) schreibt und löscht den Sollwert und liest die Rückmeldung."""

    async def test_write_clear_setpoint_and_read_status(self):
        store, client = _store(
            set=AsyncMock(return_value=True),
            delete=AsyncMock(return_value=1),
            get=AsyncMock(return_value=b'{"effective": [4, 4]}'),
        )
        await store.write_setpoint("orders", Setpoint(low=2, high=8))
        args, _ = client.set.call_args
        assert args[0] == "sq:control:setpoint:orders"
        assert json.loads(args[1]) == {"min": 2, "max": 8}

        await store.clear_setpoint("orders")
        client.delete.assert_awaited_once_with("sq:control:setpoint:orders")

        assert await store.read_status("orders", "host:p42") == {"effective": [4, 4]}
        client.get.assert_awaited_once_with("sq:control:status:orders:host:p42")
