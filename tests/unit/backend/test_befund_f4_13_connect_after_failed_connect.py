"""Nachweis F4-13: Ein zweites ``connect()`` nach gescheitertem ersten meldet
Erfolg, ohne Valkey zu erreichen; die Eviction-Prüfung (E-005) entfällt.

Register: docs/befunde/fehlersuche-runde-4-2026-10.md (F4-13; Verdacht
R4M-D-03; Gegenprüfung ``6-gegenpruefung-M.md``).

``connect()`` (``backend/valkey/connection.py``) setzt ``self._client`` vor dem
``PING`` und lässt ihn nach einem Fehlschlag stehen. Der nächste Aufruf hält
das Backend für verbunden (``if self._client is not None: return``) und kehrt
ohne ``PING`` und ohne ``probe_eviction`` zurück. Eine eingebettete
Wiederholschleife (Fehlerkatalog: „Back off, retry“; ``async with app`` in
einer Schleife) läuft so durch, obwohl Valkey fehlt; kommt Valkey mit
verdrängender Policy zurück, bleibt ``eviction_check`` ``None``, und der Worker
startet im Modus ``strict`` ohne Sperre und ohne Warnung.

Erwartet: Vertrag ``ResultBackend.connect`` „Idempotent; Treiberfehler →
``BackendConnectionError``“ und ``ping`` „Wirft ``BackendConnectionError`` vor
``connect``“ (``contracts/backend.py:17, 23``); Lösungsdoku
``swarmq-async-semaphore-deadlock-and-error-wrapping-2026-04-15.md:158``
(„Reconnect-Logik ``connect()`` defensiv aufrufen können muss“); E-005
(verdrängende Policy blockiert den Start; nicht prüfbar → WARNING).

Grenze gemockt: ``valkey.asyncio.Valkey.from_url`` liefert je Aufruf den
nächsten Fake-Client (erst einen, dessen ``PING`` scheitert, dann einen
gesunden mit ``allkeys-lru``). Im heutigen Code wird der zweite nie gebaut.

Testliste:
# [x] Valkey weiter weg: zweites ``connect()`` wirft wieder ``BackendConnectionError``
# [x] Valkey zurück mit ``allkeys-lru``: zweites ``connect()`` pingt und prüft die Policy (``unsafe``)
# [x] dasselbe mit ``strict``: die Start-Sperre greift beim zweiten Versuch (Exit 78)
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from valkey.exceptions import ConnectionError as ValkeyConnectionError

from swarmq.backend.valkey import ValkeyBackend
from swarmq.exceptions import BackendConnectionError, ConfigurationError
from swarmq.worker.eviction_gate import enforce_eviction_policy

pytestmark = pytest.mark.timeout(5)


class _FakeValkey:
    """Valkey-Client mit ``ping`` und den Wegen der Eviction-Prüfung."""

    def __init__(self, *, reachable: bool, policy: str = "allkeys-lru") -> None:
        self._reachable = reachable
        self._policy = policy
        self.pings = 0
        self.connection_pool = MagicMock()  # close() sperrt den Pool

    async def ping(self) -> bool:
        self.pings += 1
        if not self._reachable:
            raise ValkeyConnectionError("Error 111 connecting to valkey:6379. Connection refused.")
        return True

    async def config_get(self, pattern: str = "*", *args: Any, **kwargs: Any) -> dict[bytes, bytes]:
        return {b"maxmemory-policy": self._policy.encode(), b"maxmemory": b"1048576"}

    async def info(self, section: str | None = None, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return {"maxmemory_policy": self._policy, "maxmemory": 1_048_576}

    def register_script(self, script: str) -> Any:
        raise NotImplementedError(script)

    async def aclose(self) -> None:
        return None


async def _second_connect_error(backend: ValkeyBackend, *clients: _FakeValkey) -> BaseException | None:
    with patch("swarmq.backend.valkey.connection.valkey.asyncio.Valkey.from_url", side_effect=list(clients)):
        try:
            await backend.connect()
        except BackendConnectionError:
            pass
        else:
            pytest.fail("Vorbedingung: der erste connect() muss scheitern")
        try:
            await backend.connect()
        except Exception as exc:
            return exc
    return None


@pytest.mark.befund("F4-13")
async def test_second_connect_after_a_failed_one_still_reports_the_outage() -> None:
    backend = ValkeyBackend("valkey://valkey.invalid:6379/0", eviction_check="warn")

    try:
        error = await _second_connect_error(backend, _FakeValkey(reachable=False), _FakeValkey(reachable=False))

        assert isinstance(error, BackendConnectionError), (
            "zweites connect() meldet Erfolg, obwohl Valkey weiter nicht erreichbar ist "
            "(contracts/backend.py:17, Fehlerkatalog „Back off, retry“)"
        )
    finally:
        await backend.close()


@pytest.mark.befund("F4-13")
async def test_second_connect_after_valkey_returns_checks_the_eviction_policy() -> None:
    backend = ValkeyBackend("valkey://valkey.invalid:6379/0", eviction_check="warn")
    returned = _FakeValkey(reachable=True, policy="allkeys-lru")

    try:
        error = await _second_connect_error(backend, _FakeValkey(reachable=False), returned)

        assert error is None, f"zweites connect() gegen das zurückgekehrte Valkey scheitert: {error!r}"
        assert returned.pings == 1, "zweites connect() hat das zurückgekehrte Valkey nicht erreicht (kein PING)"
        assert backend.eviction_check is not None, "Eviction-Prüfung entfällt beim zweiten connect() (E-005)"
        assert backend.eviction_check.status == "unsafe"
    finally:
        await backend.close()


@pytest.mark.befund("F4-13")
async def test_second_connect_after_valkey_returns_blocks_a_strict_start_on_an_evicting_policy() -> None:
    backend = ValkeyBackend("valkey://valkey.invalid:6379/0", eviction_check="strict")

    try:
        error = await _second_connect_error(
            backend, _FakeValkey(reachable=False), _FakeValkey(reachable=True, policy="allkeys-lru")
        )
        assert error is None, f"zweites connect() gegen das zurückgekehrte Valkey scheitert: {error!r}"

        # ConfigurationError endet an den Einstiegspunkten mit Exit 78.
        with pytest.raises(ConfigurationError, match="allkeys-lru"):
            enforce_eviction_policy(backend, "strict")
    finally:
        await backend.close()
