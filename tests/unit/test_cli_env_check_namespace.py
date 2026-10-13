"""``swarmq check``: Namensraum und Keys ohne Praefix (Plan 0.12.0, MR 15, BZ-10).

Grenze gemockt: ``valkey.asyncio.Valkey.from_url`` liefert einen Fake-Client
mit ``SCAN``.

Test-Liste:
- [x] mit ``SWARMQ_NAMESPACE``: Abschnitt ``valkey`` nennt den Namensraum und
      zaehlt Keys ohne Praefix (``sq:*``); > 0 -> ``warning`` ``unprefixed_keys``, Exit 0
- [x] keine Keys ohne Praefix -> kein Befund
- [x] Zaehlung gedeckelt: ab der Obergrenze "mindestens N"
- [x] SCAN scheitert (ACL) -> ``unprefixed_keys`` null, Befund ``warning``, Exit 0
- [x] ungueltiger Namensraum -> Exit 78 ohne Verbindung
"""

from __future__ import annotations

import fnmatch
import json
from types import SimpleNamespace
from typing import Any

import pytest
import valkey.asyncio
from valkey.exceptions import NoPermissionError

from swarmq.cli import main

pytestmark = pytest.mark.timeout(5)


class _FakeValkey:
    def __init__(self, keys: list[str]) -> None:
        self.keys = keys
        self.scan_forbidden = False
        # Pool, den ValkeyBackend.close() vor dem Trennen sperrt.
        self.connection_pool = SimpleNamespace(get_connection=None, release=None)

    async def ping(self) -> bool:
        return True

    async def config_get(self, *args: Any, **kwargs: Any) -> dict[bytes, bytes]:
        return {b"maxmemory-policy": b"noeviction", b"maxmemory": b"0"}

    async def info(self, section: str | None = None, *args: Any, **kwargs: Any) -> dict[str, Any]:
        sections: dict[str, dict[str, Any]] = {
            "memory": {"used_memory": 1, "maxmemory": 0, "maxmemory_policy": "noeviction"},
            "server": {"valkey_version": "8.1.10"},
            "persistence": {"aof_enabled": 1},
            "cluster": {"cluster_enabled": 0},
        }
        return sections.get(str(section), {})

    async def scan(
        self, cursor: int = 0, match: str | None = None, count: int | None = None
    ) -> tuple[int, list[bytes]]:
        if self.scan_forbidden:
            raise NoPermissionError("no permissions to run the 'scan' command")
        # Zwei Seiten, damit der Cursor-Lauf geprueft wird.
        page = self.keys[cursor : cursor + 2]
        nxt = cursor + 2 if cursor + 2 < len(self.keys) else 0
        return nxt, [k.encode() for k in page if match is None or fnmatch.fnmatchcase(k, match)]

    async def aclose(self) -> None:
        pass


def _install(monkeypatch: pytest.MonkeyPatch, keys: list[str], namespace: str = "app1") -> _FakeValkey:
    client = _FakeValkey(keys)
    monkeypatch.setattr(valkey.asyncio.Valkey, "from_url", classmethod(lambda cls, *a, **kw: client))
    monkeypatch.setenv("SWARMQ_BACKEND_URL", "valkey://fake:6399/0")
    monkeypatch.setenv("SWARMQ_NAMESPACE", namespace)
    for name in ("SWARMQ_VALKEY_EVICTION_CHECK", "SWARMQ_SHUTDOWN_TIMEOUT"):
        monkeypatch.delenv(name, raising=False)
    return client


def _run_json(capsys: pytest.CaptureFixture[str]) -> tuple[int, dict[str, Any]]:
    code = main(["check", "--format", "json"])
    return code, json.loads(capsys.readouterr().out)


def test_namespace_and_unprefixed_keys_are_reported(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _install(monkeypatch, ["sq:result:a", "app1:sq:result:c", "sq:lock:b", "other:x", "app1:sq:{sched}:zset"])

    code, report = _run_json(capsys)

    assert code == 0
    assert report["valkey"]["namespace"] == "app1"
    assert report["valkey"]["unprefixed_keys"] == 2
    [finding] = [f for f in report["findings"] if f["code"] == "unprefixed_keys"]
    assert finding["level"] == "warning"
    assert "2 keys" in finding["message"]


def test_no_unprefixed_keys_no_finding(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    _install(monkeypatch, ["app1:sq:result:c"])

    code, report = _run_json(capsys)

    assert (code, report["valkey"]["unprefixed_keys"], report["findings"]) == (0, 0, [])


def test_count_is_capped(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    from swarmq.backend.valkey.environment import COUNT_KEYS_LIMIT

    _install(monkeypatch, [f"sq:result:{i}" for i in range(COUNT_KEYS_LIMIT + 10)])

    _code, report = _run_json(capsys)

    assert report["valkey"]["unprefixed_keys"] == COUNT_KEYS_LIMIT
    [finding] = report["findings"]
    assert finding["message"].startswith(f"at least {COUNT_KEYS_LIMIT} keys")


def test_scan_failure_is_a_warning(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    _install(monkeypatch, ["sq:result:a"]).scan_forbidden = True

    code, report = _run_json(capsys)

    assert (code, report["valkey"]["unprefixed_keys"]) == (0, None)
    assert [f["code"] for f in report["findings"]] == ["unprefixed_keys_unknown"]


def test_invalid_namespace_exits_78(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    _install(monkeypatch, [], namespace="a:b")

    assert main(["check", "--format", "json"]) == 78
    assert "invalid namespace" in capsys.readouterr().err
