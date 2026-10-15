"""``swarmq check``: Umgebung vor dem Upgrade pruefen (Plan 0.12.0, MR 14, BZ-04).

Nur lesend: ``CONFIG GET`` und ``INFO`` gegen Valkey. Grenze gemockt:
``valkey.asyncio.Valkey.from_url`` liefert einen Fake-Client.

Test-Liste:
- [x] sichere Umgebung -> Exit 0, JSON mit Abschnitt ``valkey`` (Policy, maxmemory, Speicher, AOF, Version, Cluster)
- [x] verdraengende Policy unter ``strict`` -> Exit 78 (der Worker verweigerte den Start), Befund ``error``
- [x] dieselbe Policy unter ``warn`` -> Exit 0, Befund ``warning``
- [x] nicht pruefbar (ACL) -> Exit 0, Befund ``warning``, Werte ``null``
- [x] AOF aus, Version < 8, Cluster -> je ein ``warning``
- [x] Abschnitt ``shutdown``: ``shutdown_timeout`` aus der Umgebung und Mindest-Gnadenfrist (+15 s)
- [x] Namensraum: ``null``; Keys ohne Praefix: ``null`` (erst mit Namensraum, MR 15)
- [x] Tabellenausgabe nennt jeden Befund
- [x] ohne ``SWARMQ_BACKEND_URL`` -> Exit 1 mit Meldung
- [x] ``swarmq check`` gilt nicht als ``worker --check`` (kein JSON-Fehlerpfad des Workers)
- [x] Verbindung zum Backend wird geschlossen
- Plan 2026-10-12-1105, MR 5 (P1, T4): Abschnitt ``scheduler``:
  - [x] ``SWARMQ_SCHEDULER=off`` -> ``{"scheduler": "off"}``, Befund
    ``scheduler_disabled`` (``warning``), ``ok`` bleibt ``True``
  - [x] ohne Variable -> ``{"scheduler": "on"}``, kein Befund
  - [x] Tabellenausgabe enthaelt ``scheduler  scheduler  off``
  - [x] ``SWARMQ_SCHEDULER=no`` -> Befund ``scheduler_invalid`` (``error``),
    ``ok: false``, Exit 78
- F3-18 (E-006): ungueltige Konfiguration endet wie beim Worker mit 78:
  - [x] stderr nennt das Feld, kein Valkey-Client wird gebaut (vor jeder Verbindung)
  - [x] ``SWARMQ_VALKEY_EVICTION_CHECK=bogus`` bei unerreichbarem Valkey -> 78, nicht 1
  - [x] ``SWARMQ_VALKEY_EVICTION_CHECK=WARN`` (Grossschreibung) -> 0, Befund ``warning``
  - [x] ``env_valkey_eviction_check()`` wertet ``bogus`` fuer Producer weiter als ``strict``
- Review U9: die Logging-Env loest ``check`` wie der Start ohne Flags auf:
  - [x] ``SWARMQ_LOG_LEVEL=info`` / ``SWARMQ_LOG_LIFECYCLE_LEVEL=info`` -> 0 (der Start akzeptiert sie)
  - [x] ``SWARMQ_LOG_CONFIG`` mit ``SWARMQ_LOG_FORMAT`` -> 78 vor jeder Verbindung (der Start lehnt ab)
  - [x] ``check`` kennt keine Worker-Flags: ``SWARMQ_LOG_FORMAT=xml`` -> 78 (E-006)
  - [x] Gleichlauf: ``check`` und Worker-Start entscheiden bei Logging-Env gleich
"""

from __future__ import annotations

import json
import sys
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import valkey.asyncio
from valkey.exceptions import NoPermissionError

from swarmq.cli import main

pytestmark = pytest.mark.timeout(5)

_SAFE_INFO: dict[str, dict[str, Any]] = {
    "memory": {"used_memory": 1024, "maxmemory": 0, "maxmemory_policy": "noeviction"},
    "server": {"valkey_version": "8.1.10", "redis_version": "7.2.4"},
    "persistence": {"aof_enabled": 1},
    "cluster": {"cluster_enabled": 0},
}


class _FakeValkey:
    def __init__(
        self,
        config: dict[bytes, bytes] | None = None,
        info: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        self.config = config if config is not None else {b"maxmemory-policy": b"noeviction", b"maxmemory": b"0"}
        self.sections = info if info is not None else {k: dict(v) for k, v in _SAFE_INFO.items()}
        self.forbidden = False
        self.closed = False
        # Pool, den ValkeyBackend.close() vor dem Trennen sperrt.
        self.connection_pool = SimpleNamespace(get_connection=None, release=None)

    async def ping(self) -> bool:
        return True

    async def config_get(self, pattern: str = "*", *args: Any, **kwargs: Any) -> dict[bytes, bytes]:
        if self.forbidden:
            raise NoPermissionError("no permissions to run the 'config|get' command")
        return self.config

    async def info(self, section: str | None = None, *args: Any, **kwargs: Any) -> dict[str, Any]:
        if self.forbidden:
            raise NoPermissionError("no permissions to run the 'info' command")
        return self.sections.get(str(section), {})

    async def aclose(self) -> None:
        self.closed = True


@pytest.fixture
def fake(monkeypatch: pytest.MonkeyPatch) -> _FakeValkey:
    client = _FakeValkey()
    monkeypatch.setattr(valkey.asyncio.Valkey, "from_url", classmethod(lambda cls, *a, **kw: client))
    monkeypatch.setenv("SWARMQ_BACKEND_URL", "valkey://user:secret@fake:6399/0")
    for name in ("SWARMQ_VALKEY_EVICTION_CHECK", "SWARMQ_SHUTDOWN_TIMEOUT"):
        monkeypatch.delenv(name, raising=False)
    return client


def _run_json(capsys: pytest.CaptureFixture[str]) -> tuple[int, dict[str, Any]]:
    code = main(["check", "--format", "json"])
    return code, json.loads(capsys.readouterr().out)


def test_safe_environment_passes_and_reports_valkey(fake: _FakeValkey, capsys: pytest.CaptureFixture[str]) -> None:
    code, report = _run_json(capsys)

    assert code == 0
    assert report["ok"] is True
    valkey_section = report["valkey"]
    assert valkey_section["maxmemory_policy"] == "noeviction"
    assert valkey_section["maxmemory"] == 0
    assert valkey_section["used_memory"] == 1024
    assert valkey_section["eviction"] == "safe"
    assert valkey_section["aof_enabled"] is True
    assert valkey_section["version"] == "8.1.10"
    assert valkey_section["cluster_enabled"] is False
    assert "secret" not in valkey_section["url"]
    assert report["findings"] == []


def _codes(report: dict[str, Any], level: str) -> list[str]:
    return [f["code"] for f in report["findings"] if f["level"] == level]


def _evicting(fake: _FakeValkey) -> None:
    fake.config = {b"maxmemory-policy": b"volatile-lru", b"maxmemory": b"1048576"}


@pytest.mark.entscheidung("E-006")
def test_evicting_policy_under_strict_fails_like_the_worker(
    fake: _FakeValkey, capsys: pytest.CaptureFixture[str]
) -> None:
    _evicting(fake)

    code, report = _run_json(capsys)

    assert code == 78
    assert report["ok"] is False
    assert report["valkey"]["eviction"] == "unsafe"
    assert _codes(report, "error") == ["eviction_policy"]


@pytest.mark.entscheidung("E-006")
@pytest.mark.parametrize("mode", ["warn", "off"])
def test_evicting_policy_without_strict_only_warns(
    fake: _FakeValkey, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    _evicting(fake)
    monkeypatch.setenv("SWARMQ_VALKEY_EVICTION_CHECK", mode)

    code, report = _run_json(capsys)

    assert code == 0
    assert report["valkey"]["eviction_check"] == mode
    assert _codes(report, "warning") == ["eviction_policy"]


def test_forbidden_queries_warn_and_leave_values_unknown(fake: _FakeValkey, capsys: pytest.CaptureFixture[str]) -> None:
    fake.forbidden = True

    code, report = _run_json(capsys)

    assert code == 0
    valkey_section = report["valkey"]
    assert valkey_section["eviction"] == "unknown"
    assert valkey_section["maxmemory_policy"] is None
    assert valkey_section["aof_enabled"] is None
    assert "eviction_policy_unknown" in _codes(report, "warning")


@pytest.mark.entscheidung("E-006")
def test_without_aof_warns(fake: _FakeValkey, capsys: pytest.CaptureFixture[str]) -> None:
    fake.sections["persistence"]["aof_enabled"] = 0

    code, report = _run_json(capsys)

    assert code == 0
    assert report["valkey"]["aof_enabled"] is False
    assert _codes(report, "warning") == ["aof_disabled"]


@pytest.mark.entscheidung("E-006")
@pytest.mark.parametrize(
    ("server", "version"),
    [({"redis_version": "7.2.4"}, "7.2.4"), ({"valkey_version": "7.2.11", "redis_version": "7.2.4"}, "7.2.11")],
    ids=["redis", "valkey-7"],
)
def test_valkey_before_8_warns(
    fake: _FakeValkey, capsys: pytest.CaptureFixture[str], server: dict[str, str], version: str
) -> None:
    fake.sections["server"] = server

    code, report = _run_json(capsys)

    assert code == 0
    assert report["valkey"]["version"] == version
    assert _codes(report, "warning") == ["version"]


@pytest.mark.entscheidung("E-006")
def test_cluster_mode_warns(fake: _FakeValkey, capsys: pytest.CaptureFixture[str]) -> None:
    fake.sections["cluster"]["cluster_enabled"] = 1

    code, report = _run_json(capsys)

    assert code == 0
    assert report["valkey"]["cluster_enabled"] is True
    assert _codes(report, "warning") == ["cluster"]


def test_shutdown_section_names_the_minimum_grace_period(
    fake: _FakeValkey, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SWARMQ_SHUTDOWN_TIMEOUT", "60")

    _code, report = _run_json(capsys)

    assert report["shutdown"] == {"shutdown_timeout": 60, "min_grace_period_s": 75}


def test_namespace_fields_stay_empty_without_a_namespace(fake: _FakeValkey, capsys: pytest.CaptureFixture[str]) -> None:
    _code, report = _run_json(capsys)

    assert report["valkey"]["namespace"] is None
    assert report["valkey"]["unprefixed_keys"] is None


def test_table_names_every_finding(fake: _FakeValkey, capsys: pytest.CaptureFixture[str]) -> None:
    _evicting(fake)
    fake.sections["persistence"]["aof_enabled"] = 0

    code = main(["check", "--format", "table"])

    out = capsys.readouterr().out
    assert code == 78
    assert "ERROR  valkey: maxmemory-policy volatile-lru" in out
    assert "WARNING  valkey: AOF is off" in out
    assert out.rstrip().endswith("FAILED")


def test_without_backend_url_exits_1(
    fake: _FakeValkey, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("SWARMQ_BACKEND_URL")

    code = main(["check", "--format", "json"])

    captured = capsys.readouterr()
    assert code == 1
    assert captured.out == ""
    assert "swarmq check: SWARMQ_BACKEND_URL is not set" in captured.err


@pytest.mark.parametrize("argv", [["check"], ["check", "--format", "json"], ["check", "--check"]])
def test_check_is_not_the_worker_dry_run(argv: list[str]) -> None:
    from swarmq.cli.check_cmd import _is_check_invocation

    assert _is_check_invocation(argv) is False


def test_backend_connection_is_closed(fake: _FakeValkey, capsys: pytest.CaptureFixture[str]) -> None:
    _run_json(capsys)

    assert fake.closed is True


def test_cli_spec_documents_exit_78_of_check() -> None:
    from swarmq.agent_spec import build_cli_spec

    [entry] = [e for e in build_cli_spec()["exit_codes"] if e["code"] == 78]
    assert "`swarmq check`" in entry["meaning"]


def test_scheduler_off_warns_but_passes(
    fake: _FakeValkey, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SWARMQ_SCHEDULER", "off")

    code, report = _run_json(capsys)

    assert code == 0
    assert report["ok"] is True
    assert report["scheduler"] == {"scheduler": "off"}
    assert _codes(report, "warning") == ["scheduler_disabled"]
    [finding] = report["findings"]
    assert finding["section"] == "scheduler"


def test_scheduler_defaults_to_on_without_finding(
    fake: _FakeValkey, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("SWARMQ_SCHEDULER", raising=False)

    code, report = _run_json(capsys)

    assert code == 0
    assert report["scheduler"] == {"scheduler": "on"}
    assert report["findings"] == []


def test_table_shows_the_scheduler_section(
    fake: _FakeValkey, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SWARMQ_SCHEDULER", "off")

    code = main(["check", "--format", "table"])

    out = capsys.readouterr().out
    assert code == 0
    assert "scheduler  scheduler  off" in out.splitlines()


def test_invalid_scheduler_value_fails_like_the_worker(
    fake: _FakeValkey, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SWARMQ_SCHEDULER", "no")

    code, report = _run_json(capsys)

    assert code == 78
    assert report["ok"] is False
    assert report["scheduler"] == {"scheduler": "no"}
    assert _codes(report, "error") == ["scheduler_invalid"]
    assert _codes(report, "warning") == []


@pytest.mark.entscheidung("E-006")
@pytest.mark.parametrize(
    ("variable", "value", "named"),
    [
        ("SWARMQ_VALKEY_EVICTION_CHECK", "bogus", "valkey_eviction_check"),
        ("SWARMQ_SHUTDOWN_TIMEOUT", "abc", "SWARMQ_SHUTDOWN_TIMEOUT"),
    ],
)
def test_invalid_configuration_fails_before_any_connection(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch, variable: str, value: str, named: str
) -> None:
    built: list[object] = []

    def _from_url(_cls: object, /, *args: object, **kwargs: object) -> _FakeValkey:
        built.append(args)
        return _FakeValkey()

    monkeypatch.setattr(valkey.asyncio.Valkey, "from_url", classmethod(_from_url))
    monkeypatch.setenv("SWARMQ_BACKEND_URL", "valkey://fake:6399/0")
    monkeypatch.setenv(variable, value)

    code = main(["check", "--format", "json"])

    captured = capsys.readouterr()
    assert code == 78
    assert captured.out == ""
    assert captured.err.startswith("swarmq check: ")
    assert named in captured.err
    assert built == []


@pytest.mark.entscheidung("E-006")
def test_invalid_eviction_mode_with_unreachable_valkey_exits_78(
    fake: _FakeValkey, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _unreachable() -> bool:
        raise ConnectionError("f3_18 valkey unreachable")

    monkeypatch.setattr(fake, "ping", _unreachable)
    monkeypatch.setenv("SWARMQ_VALKEY_EVICTION_CHECK", "bogus")

    code = main(["check", "--format", "json"])

    capsys.readouterr()
    assert code == 78


@pytest.mark.entscheidung("E-006")
def test_eviction_mode_is_case_insensitive(
    fake: _FakeValkey, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    _evicting(fake)
    monkeypatch.setenv("SWARMQ_VALKEY_EVICTION_CHECK", "WARN")

    code, report = _run_json(capsys)

    assert code == 0
    assert report["valkey"]["eviction_check"] == "warn"
    assert _codes(report, "warning") == ["eviction_policy"]


@pytest.mark.entscheidung("E-006")
def test_producers_treat_an_invalid_eviction_mode_as_strict(monkeypatch: pytest.MonkeyPatch) -> None:
    from swarmq.config import env_valkey_eviction_check

    monkeypatch.setenv("SWARMQ_VALKEY_EVICTION_CHECK", "bogus")

    assert env_valkey_eviction_check() == "strict"


#: Logging-Variablen, die die Tests setzen oder deren Fremdwert sie verfälschen könnte.
_LOGGING_ENV = (
    "SWARMQ_LOG_LEVEL",
    "SWARMQ_LOG_LIFECYCLE_LEVEL",
    "SWARMQ_LOG_FORMAT",
    "SWARMQ_LOG_JSON",
    "SWARMQ_LOG_CONFIG",
    "SWARMQ_LOG_CONFIGURE",
)


def _set_logging_env(monkeypatch: pytest.MonkeyPatch, values: dict[str, str]) -> None:
    for name in _LOGGING_ENV:
        monkeypatch.delenv(name, raising=False)
    for name, value in values.items():
        monkeypatch.setenv(name, value)


@pytest.mark.parametrize("variable", ["SWARMQ_LOG_LEVEL", "SWARMQ_LOG_LIFECYCLE_LEVEL"])
def test_lowercase_log_level_passes_like_the_worker_start(
    fake: _FakeValkey, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch, variable: str
) -> None:
    _set_logging_env(monkeypatch, {variable: "info"})

    code = main(["check", "--format", "json"])

    captured = capsys.readouterr()
    assert code == 0, f"{variable}=info: der Worker startet, check meldet {code}: {captured.err}"
    assert json.loads(captured.out)["ok"] is True


def _record_valkey_clients(monkeypatch: pytest.MonkeyPatch) -> list[object]:
    """Jeder gebaute Valkey-Client landet in der Liste (Grenze gemockt)."""
    built: list[object] = []

    def _from_url(_cls: object, /, *args: object, **kwargs: object) -> _FakeValkey:
        built.append(args)
        return _FakeValkey()

    monkeypatch.setattr(valkey.asyncio.Valkey, "from_url", classmethod(_from_url))
    monkeypatch.setenv("SWARMQ_BACKEND_URL", "valkey://fake:6399/0")
    return built


def test_log_config_with_log_format_fails_before_any_connection(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    built = _record_valkey_clients(monkeypatch)
    _set_logging_env(monkeypatch, {"SWARMQ_LOG_CONFIG": str(tmp_path / "logging.json"), "SWARMQ_LOG_FORMAT": "json"})

    code = main(["check", "--format", "json"])

    captured = capsys.readouterr()
    assert code == 78
    assert captured.out == ""
    assert "SWARMQ_LOG_CONFIG cannot be combined with SWARMQ_LOG_FORMAT" in captured.err
    assert built == []


@pytest.mark.entscheidung("E-006")
def test_check_knows_no_worker_flags_and_rejects_an_overridable_variable(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """``swarmq check`` kennt keine Worker-Flags: ``SWARMQ_LOG_FORMAT=xml``,
    das ``swarmq worker --log-format human`` überstimmen würde, ergibt 78."""
    built = _record_valkey_clients(monkeypatch)
    _set_logging_env(monkeypatch, {"SWARMQ_LOG_FORMAT": "xml"})

    code = main(["check", "--format", "json"])

    captured = capsys.readouterr()
    assert code == 78
    assert captured.out == ""
    assert "SWARMQ_LOG_FORMAT" in captured.err
    assert built == []


_GLEICHLAUF_MODULE = "env_check_gleichlauf_tasks"

_GLEICHLAUF_TASK_SOURCE = """\
from swarmq import Task, TaskInfo


class EnvCheckGleichlaufTask(Task):
    task_info = TaskInfo(name="env_check_gleichlauf_task", queues=["default"])

    async def run(self) -> None:
        return None
"""


class _StubProcessManager:
    """Ersetzt den Supervisor: nach dem Pre-Flight startet nichts."""

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        self.reloader: object = None

    def run(self) -> int:
        return 0


@pytest.fixture
def gleichlauf_module(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    """Gültiges Task-Modul auf ``sys.path``; räumt ``sys.modules`` auf."""
    (tmp_path / f"{_GLEICHLAUF_MODULE}.py").write_text(_GLEICHLAUF_TASK_SOURCE, encoding="utf-8")
    monkeypatch.setattr(sys, "path", [str(tmp_path), *sys.path])
    yield _GLEICHLAUF_MODULE
    sys.modules.pop(_GLEICHLAUF_MODULE, None)


@pytest.mark.parametrize(
    "logging_env",
    [
        {"SWARMQ_LOG_LEVEL": "info"},
        {"SWARMQ_LOG_LIFECYCLE_LEVEL": "debug"},
        {"SWARMQ_LOG_FORMAT": "xml"},
        {"SWARMQ_LOG_CONFIG": "logging.json", "SWARMQ_LOG_FORMAT": "json"},
    ],
    ids=["level-klein", "lifecycle-klein", "format-ungueltig", "config-und-format"],
)
def test_check_and_worker_start_decide_alike_on_logging_env(
    fake: _FakeValkey,
    gleichlauf_module: str,
    logging_env: dict[str, str],
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Gleichlauf (E-006): ``swarmq check`` sagt für die Logging-Env genau den
    Exit 78 voraus, den der Worker-Start ohne Flags liefert."""
    from swarmq.process_manager import run as run_module
    from swarmq.supervision import CONFIG_ERROR_EXIT_CODE

    monkeypatch.setattr(run_module, "ProcessManager", _StubProcessManager)
    monkeypatch.setenv("SWARMQ_BROKER_URL", "amqp://guest:guest@env-check.invalid:5699/")
    for name in ("SWARMQ_SCHEDULER", "SWARMQ_NAMESPACE", "SWARMQ_CONCURRENCY", "SWARMQ_QUEUE_TYPE"):
        monkeypatch.delenv(name, raising=False)
    _set_logging_env(monkeypatch, logging_env)

    check_code = main(["check", "--format", "json"])
    capsys.readouterr()
    start_code = main(["worker", f"--module={gleichlauf_module}"])
    capsys.readouterr()

    check_rejects = check_code == CONFIG_ERROR_EXIT_CODE
    start_rejects = start_code == CONFIG_ERROR_EXIT_CODE
    assert check_rejects == start_rejects, f"{logging_env}: check {check_code}, Start {start_code}"
