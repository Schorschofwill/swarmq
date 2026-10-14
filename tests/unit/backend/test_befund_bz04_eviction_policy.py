"""Befund BZ-04: Valkey-Eviction-Policy wird beim Verbinden nicht geprüft.

Register: docs/befunde/betrieb-ueber-zeit-2026-10.md (BZ-04; E-3, F-5, Z-3, L-5).

swarmq hängt an TTL-Keys, die nie verdrängt werden dürfen: Mutex-Locks
(``sq:lock:*``), Leader-Lock, Ausführungsschutz (``sq:guard:*``),
Unique-Flags und Fired-Marker. Unter ``volatile-*``/``allkeys-*`` (Managed-
Valkey, geteilte Cache-Instanz; ElastiCache Serverless ist fest auf
``volatile-lru``) verdrängt Valkey genau diese Keys still -- Folge ist
Doppelausführung. Erwartet: Wer sich mit Valkey verbindet, prüft
``maxmemory-policy`` und warnt laut, wenn sie nicht ``noeviction`` ist. Ein
verbotenes ``CONFIG GET`` (ACL bei Managed-Diensten) wird toleriert.

Grenze gemockt: ``valkey.asyncio.Valkey.from_url`` liefert einen Fake-Client.

Test-Liste:
- [x] ``volatile-lru`` -> WARNING beim Verbinden (behoben, MR 14)
- [x] ``allkeys-lru`` -> WARNING beim Verbinden (behoben, MR 14)
- [x] ``noeviction`` -> keine Eviction-Warnung (Schutz gegen Fehlalarm, grün)
- [x] ``CONFIG`` per ACL verboten -> Verbinden gelingt trotzdem und
      hinterlässt einen Hinweis, dass die Policy nicht prüfbar war (behoben, MR 14)
"""

from __future__ import annotations

from typing import Any

import pytest
import valkey.asyncio
from valkey.exceptions import NoPermissionError

from swarmq.backend.valkey import ValkeyBackend

pytestmark = pytest.mark.timeout(5)


class _FakeValkey:
    """Minimaler Valkey-Client: ``ping`` und die üblichen Wege zur Policy."""

    def __init__(self, policy: str | None, *, config_forbidden: bool = False) -> None:
        self._policy = policy
        self._forbidden = config_forbidden
        self.config_calls = 0

    async def ping(self) -> bool:
        return True

    def _check(self) -> None:
        self.config_calls += 1
        if self._forbidden:
            raise NoPermissionError("this user has no permissions to run the 'config|get' command")

    async def config_get(self, pattern: str = "*", *args: Any, **kwargs: Any) -> dict[bytes, bytes]:
        self._check()
        return {b"maxmemory-policy": str(self._policy).encode()}

    async def execute_command(self, *args: Any, **kwargs: Any) -> Any:
        if args and str(args[0]).upper() == "CONFIG":
            self._check()
            return [b"maxmemory-policy", str(self._policy).encode()]
        raise NotImplementedError(args)

    async def info(self, section: str | None = None, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return {"maxmemory_policy": self._policy, "maxmemory": 1024 * 1024}

    async def aclose(self) -> None:
        return None


def _install(monkeypatch: pytest.MonkeyPatch, fake: _FakeValkey) -> None:
    monkeypatch.setattr(valkey.asyncio.Valkey, "from_url", classmethod(lambda cls, *a, **kw: fake))


def _eviction_warnings(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """WARNING/ERROR, die die Eviction-Policy betreffen (Event oder Feld)."""
    hits = []
    for entry in entries:
        if entry["log_level"] not in ("warning", "error", "critical"):
            continue
        text = " ".join(str(v) for v in entry.values()).lower()
        if "evict" in text or "maxmemory" in text:
            hits.append(entry)
    return hits


@pytest.mark.entscheidung("E-006")
@pytest.mark.parametrize("policy", ["volatile-lru", "allkeys-lru"])
async def test_connect_warns_when_valkey_may_evict_swarmq_keys(
    monkeypatch: pytest.MonkeyPatch, swarmq_logs: list[dict[str, Any]], policy: str
) -> None:
    _install(monkeypatch, _FakeValkey(policy))
    backend = ValkeyBackend("valkey://fake:6399/0")

    await backend.connect()

    assert _eviction_warnings(swarmq_logs), (
        f"Valkey laeuft mit maxmemory-policy={policy}, Locks/Guard-/Unique-/Fired-"
        "Marker koennen still verdraengt werden -- swarmq hat beim Verbinden nicht "
        f"gewarnt. Logs: {[e['event'] for e in swarmq_logs]}"
    )


async def test_connect_does_not_warn_under_noeviction(
    monkeypatch: pytest.MonkeyPatch, swarmq_logs: list[dict[str, Any]]
) -> None:
    """Schutz gegen Fehlalarm: unter der sicheren Policy keine Warnung."""
    _install(monkeypatch, _FakeValkey("noeviction"))
    backend = ValkeyBackend("valkey://fake:6399/0")

    await backend.connect()

    assert _eviction_warnings(swarmq_logs) == []


async def test_connect_tolerates_forbidden_config_get_but_says_so(
    monkeypatch: pytest.MonkeyPatch, swarmq_logs: list[dict[str, Any]]
) -> None:
    fake = _FakeValkey("volatile-lru", config_forbidden=True)
    _install(monkeypatch, fake)
    backend = ValkeyBackend("valkey://fake:6399/0")

    await backend.connect()  # darf nicht werfen

    assert fake.config_calls >= 1, "die Policy wurde nicht einmal abgefragt"
    hints = [
        e
        for e in swarmq_logs
        if "maxmemory" in " ".join(str(v) for v in e.values()).lower()
        or "evict" in " ".join(str(v) for v in e.values()).lower()
    ]
    assert hints, "kein Hinweis, dass die Eviction-Policy nicht pruefbar war"
