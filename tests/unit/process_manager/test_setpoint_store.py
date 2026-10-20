"""Sync-Hüllen um den Sollwert-Speicher (Plan Kontrollkanal, U2): Fabrik-Seam,
Zeitgrenzen, Schließen, kein Backend-Import.

Test-Liste (U2):
- Erfolgreiches Lesen gibt den Sollwert zurück und schließt die Verbindung.
- Gelesen wird der Sollwert der übergebenen Gruppe (T-F-22).
- Überschrittene Zeitgrenze liefert „nicht gelesen" und schließt trotzdem.
- Ausnahme beim Öffnen liefert „nicht gelesen" und protokolliert einmal.
- Fehler beim Schließen verhindert nicht den gelesenen Wert.
- Zeitgrenzen für Lesen und Schreiben liegen zusammen unter der Hälfte des Intervalls.
- Das Modul importiert nichts aus ``swarmq.backend``.
- Schreiben der Rückmeldung schließt die Verbindung; ein Fehler liefert False.
- Plan 2026-10-06-0831, U9 (R17): Lese- und Schreibfehler loggen
  ``error_type`` und ein bereinigtes ``error``.
"""

from __future__ import annotations

import asyncio
from typing import Any

from swarmq.contracts.control import Setpoint
from swarmq.process_manager.setpoint_store import SetpointUnread, read_setpoint_sync, write_status_sync


class FakeStore:
    def __init__(self, setpoint=None, *, hang=False, connect_error=None, close_error=None):
        self.setpoint = setpoint
        self.hang = hang
        self.connect_error = connect_error
        self.close_error = close_error
        self.closed = False
        self.read_groups: list[str] = []
        self.status_writes: list[tuple[str, str, dict[str, Any], float]] = []

    async def connect(self):
        if self.connect_error is not None:
            raise self.connect_error
        if self.hang:
            await asyncio.sleep(3600)

    async def close(self):
        self.closed = True
        if self.close_error is not None:
            raise self.close_error

    async def read_setpoint(self, group):
        self.read_groups.append(group)
        return self.setpoint

    async def write_status(self, group, supervisor_id, status, *, ttl_s):
        self.status_writes.append((group, supervisor_id, dict(status), ttl_s))


class TestReadSetpointSync:
    def test_successful_read_returns_setpoint_and_closes(self):
        store = FakeStore(Setpoint(low=2, high=8))
        assert read_setpoint_sync(lambda: store, "orders") == Setpoint(low=2, high=8)
        assert store.closed is True

    def test_reads_the_setpoint_of_the_given_group(self):
        """T-F-22, Mutant ``setpoint_store.py:77`` ``read_setpoint(group)`` -> ``read_setpoint(None)``."""
        store = FakeStore(Setpoint(low=2, high=8))
        read_setpoint_sync(lambda: store, "orders")
        assert store.read_groups == ["orders"]

    def test_timeout_yields_unread_and_still_closes(self, monkeypatch):
        monkeypatch.setattr("swarmq.process_manager.setpoint_store.READ_TIMEOUT_S", 0.1)
        store = FakeStore(hang=True)
        result = read_setpoint_sync(lambda: store, "orders")
        assert isinstance(result, SetpointUnread)
        assert store.closed is True

    def test_connect_failure_yields_unread_and_logs_once(self, swarmq_logs):
        store = FakeStore(connect_error=ConnectionRefusedError("no route"))
        result = read_setpoint_sync(lambda: store, "orders")
        assert isinstance(result, SetpointUnread)
        assert "no route" in result.reason
        [entry] = [e for e in swarmq_logs if e["log_level"] == "warning"]
        assert entry["event"] == "setpoint_read_failed"
        assert entry["group"] == "orders"
        assert entry["error_type"] == "ConnectionRefusedError"
        assert entry["error"] == "no route"

    def test_read_failure_logs_sanitized_error(self, swarmq_logs):
        store = FakeStore(connect_error=ConnectionError("valkey://:s3cret@cache:6379"))
        read_setpoint_sync(lambda: store, "orders")
        [entry] = [e for e in swarmq_logs if e["event"] == "setpoint_read_failed"]
        assert entry["error"] == "valkey://:***@cache:6379"

    def test_close_failure_does_not_lose_the_read_value(self):
        store = FakeStore(Setpoint(low=1, high=1), close_error=RuntimeError("socket gone"))
        assert read_setpoint_sync(lambda: store, "orders") == Setpoint(low=1, high=1)


class TestBudgets:
    def test_read_and_write_timeouts_stay_under_half_the_sample_interval(self):
        from swarmq.process_manager import setpoint_store as mod

        assert mod.READ_TIMEOUT_S + mod.WRITE_TIMEOUT_S < mod.DEFAULT_SAMPLE_INTERVAL_S / 2

    def test_module_imports_nothing_from_the_backend(self):
        import inspect

        from swarmq.process_manager import setpoint_store as mod

        source = inspect.getsource(mod)
        assert "swarmq.backend" not in source
        assert "import valkey" not in source


class TestWriteStatusSync:
    def test_writes_status_with_ttl_and_closes(self):
        store = FakeStore()
        ok = write_status_sync(lambda: store, "orders", "host:p1", {"effective": [2, 8]}, ttl_s=40.0)
        assert ok is True
        assert store.status_writes == [("orders", "host:p1", {"effective": [2, 8]}, 40.0)]
        assert store.closed is True

    def test_failure_returns_false_and_logs_once(self, swarmq_logs):
        store = FakeStore(connect_error=OSError("valkey://:s3cret@cache:6379"))
        ok = write_status_sync(lambda: store, "orders", "host:p1", {}, ttl_s=40.0)
        assert ok is False
        [entry] = [e for e in swarmq_logs if e["log_level"] == "warning"]
        assert entry["event"] == "setpoint_status_write_failed"
        assert entry["group"] == "orders"
        assert entry["error_type"] == "OSError"
        assert entry["error"] == "valkey://:***@cache:6379"
        assert store.closed is True


class TestFactoryFailure:
    def test_factory_raising_yields_unread_instead_of_propagating(self, swarmq_logs):
        """Kein Pfad laesst einen Sollwert den Start scheitern (R10): auch die Fabrik nicht."""

        def broken_factory():
            raise RuntimeError("no backend url")

        result = read_setpoint_sync(broken_factory, "orders")
        assert isinstance(result, SetpointUnread)
        assert write_status_sync(broken_factory, "orders", "host:p1", {}, ttl_s=1.0) is False
