"""Pruefung der Valkey-Eviction-Policy beim Verbinden (Plan 0.12.0, MR 14, BZ-04).

Die Nachweise stehen in ``test_befund_bz04_eviction_policy.py``; hier die
Faelle drumherum. Grenze gemockt: ``valkey.asyncio.Valkey.from_url`` liefert
einen Fake-Client.

Test-Liste:
- [x] weder ``CONFIG`` noch ``INFO`` beantwortet -> WARNING „nicht pruefbar“,
      ``connect`` wirft nicht, Ergebnis ``unknown`` am Backend
- [x] Ergebnis (Status, Policy, ``maxmemory``) liegt am Backend
- [x] ``maxmemory`` 0 -> sicher, auch mit verdraengender Policy
- [x] haengende Abfrage -> nach dem Zeitlimit ``unknown``, ``connect`` kehrt zurueck
- [x] ``eviction_check="off"`` (Parameter oder Umgebung) -> keine Abfrage, keine Warnung
- [x] injizierter Client -> keine Pruefung (kein Ergebnis)
- [x] erneute Pruefung (Leader, stuendlich): Ergebnis mit ``used_memory``, Warnung bei Befund
- [x] erneute Pruefung mit ``off`` -> ``None``, keine Abfrage
"""

from __future__ import annotations

import asyncio
from typing import Any, override

import pytest
import valkey.asyncio
from valkey.exceptions import NoPermissionError

from swarmq.backend.valkey import ValkeyBackend

pytestmark = pytest.mark.timeout(5)


class _FakeValkey:
    def __init__(
        self,
        config: dict[bytes, bytes] | None = None,
        info: dict[str, Any] | None = None,
    ) -> None:
        self._config = config
        self._info = info
        self.calls: list[str] = []

    async def ping(self) -> bool:
        return True

    async def config_get(self, pattern: str = "*", *args: Any, **kwargs: Any) -> dict[bytes, bytes]:
        self.calls.append("config_get")
        if self._config is None:
            raise NoPermissionError("this user has no permissions to run the 'config|get' command")
        return self._config

    async def info(self, section: str | None = None, *args: Any, **kwargs: Any) -> dict[str, Any]:
        self.calls.append("info")
        if self._info is None:
            raise NoPermissionError("this user has no permissions to run the 'info' command")
        return self._info

    async def aclose(self) -> None:
        return None


def _install(monkeypatch: pytest.MonkeyPatch, fake: Any) -> None:
    monkeypatch.setattr(valkey.asyncio.Valkey, "from_url", classmethod(lambda cls, *a, **kw: fake))


def _events(entries: list[dict[str, Any]], level: str = "warning") -> list[str]:
    return [e["event"] for e in entries if e["log_level"] == level]


async def test_unanswered_policy_query_warns_and_connect_succeeds(
    monkeypatch: pytest.MonkeyPatch, swarmq_logs: list[dict[str, Any]]
) -> None:
    _install(monkeypatch, _FakeValkey())
    backend = ValkeyBackend("valkey://fake:6399/0")

    await backend.connect()

    assert backend.eviction_check is not None
    assert backend.eviction_check.status == "unknown"
    assert "valkey_eviction_policy_unknown" in _events(swarmq_logs)


async def test_result_of_the_check_lies_on_the_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch, _FakeValkey(config={b"maxmemory-policy": b"allkeys-lru", b"maxmemory": b"1048576"}))
    backend = ValkeyBackend("valkey://fake:6399/0")
    before_connect = backend.eviction_check
    assert before_connect is None

    await backend.connect()

    assert backend.eviction_check is not None
    assert (backend.eviction_check.status, backend.eviction_check.policy, backend.eviction_check.maxmemory) == (
        "unsafe",
        "allkeys-lru",
        1048576,
    )


async def test_without_memory_limit_no_policy_evicts(
    monkeypatch: pytest.MonkeyPatch, swarmq_logs: list[dict[str, Any]]
) -> None:
    """``maxmemory`` 0: Valkey verdraengt nie, die Policy greift nicht."""
    _install(monkeypatch, _FakeValkey(config={b"maxmemory-policy": b"allkeys-lru", b"maxmemory": b"0"}))
    backend = ValkeyBackend("valkey://fake:6399/0")

    await backend.connect()

    assert backend.eviction_check is not None
    assert backend.eviction_check.status == "safe"
    assert _events(swarmq_logs) == []


class _HangingValkey(_FakeValkey):
    def __init__(self) -> None:
        super().__init__()
        self.never = asyncio.Event()

    @override
    async def config_get(self, pattern: str = "*", *args: Any, **kwargs: Any) -> dict[bytes, bytes]:
        await self.never.wait()
        return {}

    @override
    async def info(self, section: str | None = None, *args: Any, **kwargs: Any) -> dict[str, Any]:
        await self.never.wait()
        return {}


async def test_hanging_policy_query_ends_after_the_time_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("swarmq.backend.valkey.environment._PROBE_TIMEOUT_S", 0.05)
    _install(monkeypatch, _HangingValkey())
    backend = ValkeyBackend("valkey://fake:6399/0")

    await backend.connect()

    assert backend.eviction_check is not None
    assert backend.eviction_check.status == "unknown"


async def test_check_off_queries_nothing_and_stays_silent(
    monkeypatch: pytest.MonkeyPatch, swarmq_logs: list[dict[str, Any]]
) -> None:
    fake = _FakeValkey(config={b"maxmemory-policy": b"allkeys-lru", b"maxmemory": b"1"})
    _install(monkeypatch, fake)
    backend = ValkeyBackend("valkey://fake:6399/0", eviction_check="off")

    await backend.connect()

    assert fake.calls == []
    assert backend.eviction_check is None
    assert _events(swarmq_logs) == []


async def test_check_off_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ohne Parameter gilt ``SWARMQ_VALKEY_EVICTION_CHECK`` (Q11: gelesen ueber ``config.py``);
    auch CLI-Befehle bauen ihr Backend ohne ``SwarmQConfig``."""
    monkeypatch.setenv("SWARMQ_VALKEY_EVICTION_CHECK", "OFF")
    fake = _FakeValkey(config={b"maxmemory-policy": b"allkeys-lru", b"maxmemory": b"1"})
    _install(monkeypatch, fake)
    backend = ValkeyBackend("valkey://fake:6399/0")

    await backend.connect()

    assert fake.calls == []


async def test_injected_client_is_not_checked(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeValkey(config={b"maxmemory-policy": b"allkeys-lru", b"maxmemory": b"1"})
    backend = ValkeyBackend(client=fake)  # type: ignore[arg-type]

    await backend.connect()

    assert fake.calls == []
    assert backend.eviction_check is None


async def test_recheck_reports_memory_and_warns(
    monkeypatch: pytest.MonkeyPatch, swarmq_logs: list[dict[str, Any]]
) -> None:
    fake = _FakeValkey(config={b"maxmemory-policy": b"noeviction", b"maxmemory": b"1000"}, info={"used_memory": 250})
    _install(monkeypatch, fake)
    backend = ValkeyBackend("valkey://fake:6399/0")
    await backend.connect()
    fake._config = {b"maxmemory-policy": b"volatile-lru", b"maxmemory": b"1000"}

    check = await backend.recheck_eviction()

    assert check is not None
    assert (check.status, check.policy, check.maxmemory, check.used_memory) == ("unsafe", "volatile-lru", 1000, 250)
    assert backend.eviction_check == check
    assert "valkey_eviction_policy_unsafe" in _events(swarmq_logs)


async def test_recheck_with_check_off_queries_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeValkey(config={b"maxmemory-policy": b"allkeys-lru", b"maxmemory": b"1"})
    _install(monkeypatch, fake)
    backend = ValkeyBackend("valkey://fake:6399/0", eviction_check="off")
    await backend.connect()

    assert await backend.recheck_eviction() is None
    assert fake.calls == []
